#!/usr/bin/env python3
"""
dbus state host -- the durable half of the transparent-projection split.

A GENERIC durable vedbus host. It owns the dbus connection and holds every hosted
service + value in memory, and it exposes a private control interface the logic daemon
drives. It knows NOTHING about MQTT or services.yml -- all shape/config is passed in
over the control interface. Because state is in-memory, this process is meant to be
small and stable and almost never restart; its own crash is an accepted total loss
(== GX reboot), so it needs no disk persistence.

See docs/transparent-projection.md (sections 3-4).

Control interface: bus name `com.hypnos.dbusstate`, object `/`, iface `com.hypnos.DbusState1`.
"""

import os
import sys
import json
import uuid
import signal
import logging
import argparse

import dbus
import dbus.service
from gi.repository import GLib
from dbus.mainloop.glib import DBusGMainLoop

# velib_python (vedbus.py) -- provided on PYTHONPATH by the nix dev shell (VELIB_PYTHON),
# or ext/velib_python on the GX.
from vedbus import VeDbusService

CONTROL_BUS_NAME = "com.hypnos.dbusstate"
CONTROL_IFACE = "com.hypnos.DbusState1"
CONTROL_PATH = "/"

DRIVER_PROCESS = "dbus-state-host"
DRIVER_VERSION = "0.1.0"


def _native(value):
    """Coerce a dbus value (or python value) to a JSON-serialisable native type."""
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return float(value)
    try:
        return str(value)
    except Exception:
        return None


def _make_gettext(fmt):
    if not fmt:
        return None

    def cb(_path, value):
        if value is None:
            return ""
        try:
            return fmt.format(value)
        except Exception:
            return str(value)

    return cb


def _abspath(path):
    return path if path.startswith("/") else "/" + path


class MemoryAllocator:
    """Bench: in-memory DeviceInstance allocation (NOT persisted -> instances can change
    across a state-daemon restart; the GX uses LocalSettingsAllocator for stability)."""

    def __init__(self, base=200):
        self._base = base
        self._map = {}

    def allocate(self, service_id, service_type, requested=None):
        if service_id in self._map:
            return self._map[service_id]
        used = set(self._map.values())
        inst = requested if (requested is not None and requested not in used) else self._base
        while inst in used:
            inst += 1
        self._map[service_id] = inst
        return inst

    def release(self, service_id):
        self._map.pop(service_id, None)


class LocalSettingsAllocator:
    """GX: allocate a stable VRM instance via com.victronenergy.settings (localsettings),
    keyed by service_id -- exactly like freakent's device_service. The instance persists
    across a state-daemon restart / GX reboot so the device keeps its VRM identity."""

    def __init__(self, bus):
        self._bus = bus
        self._settings = {}  # service_id -> SettingsDevice (kept alive)

    def allocate(self, service_id, service_type, requested=None):
        from settingsdevice import SettingsDevice
        sd = SettingsDevice(bus=self._bus, supportedSettings={}, eventCallback=None)
        path = "/Settings/Devices/{}/ClassAndVrmInstance".format(service_id)
        r = sd.addSetting(path, "{}:{}".format(service_type, requested or 1), "", "")
        self._settings[service_id] = sd
        _cls, _inst = str(r.get_value()).split(":")
        return int(_inst)

    def release(self, service_id):
        # Keep the localsettings record so the VRM instance is stable; just drop our ref.
        self._settings.pop(service_id, None)


class HostedService:
    """One projected com.victronenergy.<type>.<service_id> service."""

    def __init__(self, conn, spec, on_gx_write):
        self.service_id = spec["service_id"]            # e.g. mqtt_hypnosinv_v1
        self.type = spec["type"]                        # e.g. vebus
        self.instance = int(spec["instance"])
        self.name = "com.victronenergy.%s.%s" % (self.type, self.service_id)
        self.paths = spec.get("paths", {}) or {}        # path -> meta (format/writeable/min/max/persist)
        self.meta = spec.get("meta") or {}              # opaque logic-daemon metadata (e.g. lwt), durable
        self._on_gx_write = on_gx_write
        self.connected = True
        # Each hosted service owns a PRIVATE dbus connection: object path '/' (the vedbus
        # root export) is per-connection, so multiple services -- and the control
        # interface -- cannot share one. (This is how freakent hosts many devices.)
        self.conn = conn

        init = spec.get("init", {}) or {}
        # Normalise init keys to abs paths.
        init = {_abspath(k): v for k, v in init.items()}

        svc = VeDbusService(self.name, bus=conn, register=False)
        self.svc = svc

        # Driver-identity: synthesized constants describing the PROJECTOR, not device
        # state -- never stale, so the daemon owns them.
        svc.add_path("/Mgmt/ProcessName", DRIVER_PROCESS)
        svc.add_path("/Mgmt/ProcessVersion", DRIVER_VERSION)
        svc.add_path("/Mgmt/Connection", spec.get("connection", "MQTT"))
        svc.add_path("/DeviceInstance", self.instance)
        svc.add_path("/Connected", 1)

        # Device-identity the board supplied in the spec (optional).
        for ident in ("ProductName", "ProductId", "FirmwareVersion"):
            if ident in spec:
                svc.add_path("/" + ident, spec[ident])

        # Device paths from the SHAPE; value = board-authored init, or None if the board
        # omitted it (ownership, not a guess -- e.g. genset /Start).
        for path, meta in self.paths.items():
            dpath = _abspath(path)
            meta = meta or {}
            writeable = bool(meta.get("writeable", False))
            svc.add_path(
                dpath,
                value=init.get(dpath, None),
                description=meta.get("description", ""),
                writeable=writeable,
                onchangecallback=self._gx_write if writeable else None,
                gettextcallback=_make_gettext(meta.get("format")),
            )

        svc.register()
        logging.info("hosting %s (instance %d, %d paths)", self.name, self.instance, len(self.paths))

    def _gx_write(self, path, newvalue):
        # The GX wrote a writeable path. Accept it and notify the logic daemon. For our
        # control paths this is informational (board reads commands via N/), but the
        # channel exists for settings-persist / future write-forwarding.
        try:
            self._on_gx_write(self.service_id, path, _native(newvalue))
        except Exception:
            logging.exception("on_gx_write failed for %s %s", self.name, path)
        return True  # accept the change

    def set_values(self, values):
        for path, value in values.items():
            dpath = _abspath(path)
            if dpath in self.svc:
                self.svc[dpath] = value
            else:
                logging.warning("set_values: %s has no path %s", self.name, dpath)

    def set_connected(self, connected):
        self.connected = bool(connected)
        self.svc["/Connected"] = 1 if connected else 0
        if not connected:
            # Invalidate live (non-setting) device paths so a stale value can't keep
            # counting on the GX (the stuck-meter fix), settings/persist untouched.
            for path, meta in self.paths.items():
                if (meta or {}).get("persist") or (meta or {}).get("setting"):
                    continue
                dpath = _abspath(path)
                if dpath in self.svc:
                    self.svc[dpath] = None

    def state(self):
        values = {}
        for path in self.paths:
            dpath = _abspath(path)
            if dpath in self.svc:
                values[dpath] = _native(self.svc[dpath])
        return {
            "service_id": self.service_id,
            "type": self.type,
            "instance": self.instance,
            "connected": self.connected,
            "meta": self.meta,
            "values": values,
        }

    def remove(self):
        # Force immediate deregister of the service + all object paths (releases the
        # bus name -> the GX sees the device disappear), then drop the private connection.
        self.svc.__del__()
        try:
            self.conn.close()
        except Exception:
            pass


class StateHost(dbus.service.Object):
    def __init__(self, bus, make_bus, allocator):
        self._bus = bus
        self._make_bus = make_bus  # () -> a fresh private connection for a hosted service
        self._alloc = allocator
        # In-memory incarnation cookie: new per process, stable for its life -> changes
        # iff this daemon lost its state. The logic daemon mirrors it to MQTT (retained)
        # so boards re-announce on a change. (Nonce, not counter: boards only test
        # "different from last".) Set BEFORE claiming the name, so a GetCookie racing the
        # NameOwnerChanged can't arrive before the attribute exists.
        self.cookie = uuid.uuid4().hex
        self.services = {}  # service_id -> HostedService
        self._busname = dbus.service.BusName(CONTROL_BUS_NAME, bus, do_not_queue=True)
        super().__init__(self._busname, CONTROL_PATH)
        logging.info("state host up on %s, cookie=%s", CONTROL_BUS_NAME, self.cookie)
        self.Started(self.cookie)

    # -- control interface --------------------------------------------------------

    @dbus.service.method(CONTROL_IFACE, in_signature="s", out_signature="s")
    def EnsureService(self, spec_json):
        spec = json.loads(spec_json)
        sid = spec["service_id"]
        if sid in self.services:
            # Idempotent adopt: existing service is already correct -> DO NOT re-apply
            # init (that would stomp live state / fight a command). Reconcile is a no-op.
            hs = self.services[sid]
            return json.dumps({"instance": hs.instance, "created": False})
        # The state daemon owns instance allocation (stable identity across a logic
        # restart, and -- on the GX -- across its own restart via localsettings).
        requested = spec.get("instance", spec.get("instance_hint"))
        spec["instance"] = self._alloc.allocate(sid, spec["type"], requested)
        hs = HostedService(self._make_bus(), spec, on_gx_write=self._emit_gx_write)
        self.services[sid] = hs
        return json.dumps({"instance": hs.instance, "created": True})

    @dbus.service.method(CONTROL_IFACE, in_signature="ss", out_signature="b")
    def SetValues(self, service_id, values_json):
        hs = self.services.get(service_id)
        if hs is None:
            return False
        hs.set_values(json.loads(values_json))
        return True

    @dbus.service.method(CONTROL_IFACE, in_signature="sb", out_signature="b")
    def SetConnected(self, service_id, connected):
        hs = self.services.get(service_id)
        if hs is None:
            return False
        hs.set_connected(bool(connected))
        return True

    @dbus.service.method(CONTROL_IFACE, in_signature="s", out_signature="b")
    def RemoveService(self, service_id):
        hs = self.services.pop(service_id, None)
        if hs is None:
            return False
        hs.remove()
        self._alloc.release(service_id)
        return True

    @dbus.service.method(CONTROL_IFACE, in_signature="", out_signature="s")
    def ListServices(self):
        return json.dumps([
            {"service_id": hs.service_id, "type": hs.type, "instance": hs.instance, "meta": hs.meta}
            for hs in self.services.values()
        ])

    @dbus.service.method(CONTROL_IFACE, in_signature="s", out_signature="s")
    def GetService(self, service_id):
        hs = self.services.get(service_id)
        return json.dumps(hs.state() if hs else None)

    @dbus.service.method(CONTROL_IFACE, in_signature="", out_signature="s")
    def GetCookie(self):
        return self.cookie

    @dbus.service.signal(CONTROL_IFACE, signature="s")
    def Started(self, cookie):
        pass

    @dbus.service.signal(CONTROL_IFACE, signature="sss")
    def GxWrite(self, service_id, path, value_json):
        pass

    def _emit_gx_write(self, service_id, path, value):
        self.GxWrite(service_id, path, json.dumps(value))


def main():
    ap = argparse.ArgumentParser(description="dbus state host (durable vedbus projector)")
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--system", action="store_true", help="use the system bus (GX); default session if available")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    DBusGMainLoop(set_as_default=True)

    use_session = (not args.system) and ("DBUS_SESSION_BUS_ADDRESS" in os.environ)

    def make_bus(private=True):
        if use_session:
            return dbus.SessionBus(private=private)
        return dbus.SystemBus(private=private)

    control_bus = make_bus(private=False)  # control endpoint; shared connection is fine

    # Pick the instance allocator: localsettings on the GX, in-memory on the bench.
    if control_bus.name_has_owner("com.victronenergy.settings"):
        allocator = LocalSettingsAllocator(control_bus)
        logging.info("instance allocator: localsettings")
    else:
        allocator = MemoryAllocator()
        logging.info("instance allocator: in-memory (no com.victronenergy.settings)")

    host = StateHost(control_bus, make_bus, allocator)  # keep a ref for the lifetime of the loop

    loop = GLib.MainLoop()
    signal.signal(signal.SIGINT, lambda *_: loop.quit())
    signal.signal(signal.SIGTERM, lambda *_: loop.quit())
    loop.run()


if __name__ == "__main__":
    main()
