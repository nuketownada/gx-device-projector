#!/usr/bin/env python3
"""
v2 logic daemon -- the restartable MQTT<->state-daemon bridge (full-fork, v2-only).

Job (and ONLY this -- values never flow through here; the board writes telemetry via
W/<portal>/<type>/<inst>/<path> straight into flashmq -> the state daemon's writeable
paths):
  - device/+/Status (non-retained v2 registration w/ init) -> ProjectionClient.ensure
    -> reply device/<clientId>/DBus {portalId, deviceInstance, topicPath}
  - connected==0 / LWT -> set_connected(False);  empty payload -> remove
  - reconcile on startup (adopt from the host; v2 registrations are non-retained so a
    logic-only restart gets no re-announce)
  - watch_host -> publish device/_host/cookie RETAINED (boards re-announce on a change)

MQTT/GLib socket integration is modelled on lib/dbus-mqtt MqttGObjectBridge but made
configurable (host/port/tls) for the fork + the bench.
See docs/transparent-projection.md.
"""

import os
import sys
import ssl
import json
import signal
import logging
import argparse

import dbus
import yaml
import paho.mqtt.client as mqtt
from gi.repository import GLib
from dbus.mainloop.glib import DBusGMainLoop

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from projection import ProjectionClient

COOKIE_TOPIC = "device/_host/cookie"

# Grace before a will / connected:0 actually marks a board disconnected. rust-mqtt 0.3 has no
# MQTT5 Will Delay and its clean session fires the non-retained connected:0 will on EVERY
# ungraceful drop -- including the board's own 8s registration-timeout retry while logicd is
# slow/redeploying. Must exceed the board's TCP-retry (3s) + reg-timeout (8s) so a single
# reconnect cycle can't trip it. See _disconnect.
DISCONNECT_GRACE_S = 15


class V2LogicDaemon:
    def __init__(self, bus, services_yml, portal_id,
                 mqtt_host, mqtt_port, ca_cert=None, user=None, passwd=None, ns="device",
                 disconnect_grace=DISCONNECT_GRACE_S):
        self.bus = bus
        self.portal_id = portal_id
        self.shapes = self._load_shapes(services_yml)   # type -> {abspath: meta}
        self.proj = ProjectionClient(bus)
        self.last_cookie = None
        self._lwt = {}                                  # lwt_topic -> (client_id, lwt_value)
        self._pending_disc = {}                         # client_id -> GLib timer id (debounce)
        self._disc_grace = disconnect_grace
        # Registration namespace. Default "device" = production. A distinct ns (e.g.
        # "hstest") isolates a GX integration test from the running freakent, which only
        # listens on device/+/Status.
        self.ns = ns
        self.status_sub = "{}/+/Status".format(ns)
        self.proxy_sub = "{}/+/Proxy".format(ns)
        self.cookie_topic = "{}/_host/cookie".format(ns)

        self.proj.watch_host(self._on_host_up, self._on_host_down)

        self._mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dbus_state_logic_" + ns)
        if user:
            self._mqtt.username_pw_set(user, passwd)
        if ca_cert:
            self._mqtt.tls_set(ca_cert, cert_reqs=ssl.CERT_REQUIRED)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.on_disconnect = self._on_disconnect
        self._sock_watch = None
        self._mqtt.connect(mqtt_host, mqtt_port, 60)
        self._init_socket()
        # loop_misc/writes on a 1s tick (added once; survives reconnects).
        self._sock_timer = GLib.timeout_add_seconds(1, self._on_sock_timer)

    # -- services.yml = SHAPE only (no value defaults in the v2 model) ---------
    def _load_shapes(self, path):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        shapes = {}
        self._v1_defaults = {}  # type -> {path: services.yml default} (the v1 transition shim)
        for typ, paths in cfg.items():
            if not isinstance(paths, dict):
                continue
            d = {}
            defaults = {}
            for pk, meta in paths.items():
                meta = meta or {}
                d["/" + pk] = {
                    "writeable": True,  # all device paths writeable (flashmq W/ + GX control)
                    "format": meta.get("format"),
                    "persist": bool(meta.get("persist", False)),
                    "setting": bool(meta.get("setting", False)),
                    # GX-OWNED command path (/Mode, /Start): the GX authors its value, not the
                    # board. Drives two things in the state daemon -- forward its onchange (a GX
                    # write is a command) and EXEMPT it from disconnect-invalidation (its value
                    # is standing GX intent, not board telemetry to expire on a flap).
                    "gx_owned": bool(meta.get("gx_owned", False)),
                    "description": meta.get("description", ""),
                }
                if meta.get("default") is not None:
                    defaults["/" + pk] = meta["default"]
            shapes[typ] = d
            self._v1_defaults[typ] = defaults
        return shapes

    # -- MQTT via the GLib mainloop (single-threaded, like MqttGObjectBridge) --
    def _init_socket(self):
        # (Re)watch the MQTT socket for reads; the fd changes on every reconnect.
        if self._sock_watch:
            GLib.source_remove(self._sock_watch)
        self._sock_watch = GLib.io_add_watch(self._mqtt.socket().fileno(), GLib.IO_IN, self._on_sock_in)

    def _on_disconnect(self, client, userdata, *args):
        # Broker restart / GX reboot: drop the (now-dead) socket watch and reconnect. The
        # state daemon keeps hosting the devices; on reconnect on_connect reconciles + re-subs.
        logging.warning("[mqtt] disconnected; scheduling reconnect")
        if self._sock_watch:
            GLib.source_remove(self._sock_watch)
            self._sock_watch = None
        GLib.timeout_add_seconds(3, self._reconnect)

    def _reconnect(self):
        try:
            self._mqtt.reconnect()
            self._init_socket()
            logging.info("[mqtt] reconnected")
            return False  # stop the retry timer
        except Exception as e:
            logging.warning("[mqtt] reconnect failed (%s); retry in 3s", e)
            return True   # keep retrying

    def _flush(self):
        # Send any queued packets NOW (don't wait for the 1s misc timer) -- otherwise a
        # SUBSCRIBE/PUBLISH can lag behind a board's non-retained announce and miss it.
        while self._mqtt.want_write():
            if self._mqtt.loop_write() != mqtt.MQTT_ERR_SUCCESS:
                break

    def _on_sock_in(self, src, cond):
        self._mqtt.loop_read()
        self._flush()
        return True

    def _on_sock_timer(self):
        self._mqtt.loop_misc()
        self._flush()
        return True

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        logging.info("[mqtt] connected rc=%s", reason_code)
        client.subscribe(self.status_sub)
        client.subscribe(self.proxy_sub)  # value-relay clients (patroclus, tanks)
        self._flush()  # push the SUBSCRIBEs out before any board announces
        # Reconcile now if the host is already up: adopt devices, rebuild LWT subs, and
        # publish the current cookie. (watch_host handles a host restart while we're up.)
        try:
            self._on_host_up(self.proj.get_cookie())
        except dbus.DBusException:
            logging.info("[mqtt] state host not up yet; will reconcile on its appearance")

    def _on_message(self, client, userdata, msg):
        try:
            self._handle(msg)
        except Exception:
            logging.exception("[mqtt] handling %s failed", msg.topic)

    # -- registration handshake ----------------------------------------------
    def _handle(self, msg):
        parts = msg.topic.split("/")
        if len(parts) == 3 and parts[0] == self.ns and parts[2] == "Status":
            client_id = parts[1]
            if not msg.payload:
                self._remove(client_id)
                return
            status = json.loads(msg.payload)
            connected = status.get("connected")
            if connected == 1:
                self._register(client_id, status)
            elif connected == 0:
                self._disconnect(client_id)
        elif len(parts) == 3 and parts[0] == self.ns and parts[2] == "Proxy":
            self._handle_proxy(msg.payload)
        elif msg.topic in self._lwt:
            client_id, lwt_value = self._lwt[msg.topic]
            if msg.payload.decode("utf-8", "ignore") == lwt_value:
                logging.info("[lwt] %s fired -> disconnect %s", msg.topic, client_id)
                self._disconnect(client_id)

    def _handle_proxy(self, payload):
        # freakent's device/<id>/Proxy value relay (ported from device_proxy.py): a client
        # batches values as {topicPath:"W/<portal>/<svc>/<inst>", values:{Key:val,...}};
        # fan them out to individual W/ writes (-> flashmq -> the state daemon's dbus paths).
        # patroclus + the tanks publish this way; the hypnos boards write W/ directly.
        try:
            msg = json.loads(payload)
        except Exception:
            return
        tp = msg.get("topicPath")
        values = msg.get("values")
        if not tp or not isinstance(values, dict):
            return
        for k, v in values.items():
            self._mqtt.publish(tp + "/" + k, json.dumps({"value": v}))
        self._flush()

    def _register(self, client_id, status):
        # A (re)announce proves the board is alive -> cancel any armed disconnect (§1 debounce)
        # so a flap that re-announced within the grace window is literally zero dbus change.
        self._cancel_pending_disc(client_id)
        version = status.get("version", "")
        lwt_topic = status.get("lwt_topic")
        lwt_value = str(status.get("lwt_value", "0"))
        # Durable per-service meta so LWT survives a logic restart (registrations are
        # non-retained; reconcile recovers this from the state daemon).
        meta = {"client_id": client_id}
        if lwt_topic:
            meta["lwt_topic"] = lwt_topic
            meta["lwt_value"] = lwt_value
        ensured = {}
        for tag, sdef in (status.get("services") or {}).items():
            # sdef is "type" (v1) or {type, init} (v2).
            if isinstance(sdef, str):
                # v1 client (no init): seed init from the services.yml defaults so it
                # behaves exactly as under the old freakent. Transition shim only -- v2
                # clients author their own init and get NO defaults (transparent projection).
                typ = sdef
                init = dict(self._v1_defaults.get(typ, {}))
            else:
                typ, init = sdef.get("type"), (sdef.get("init") or {})
            shape = self.shapes.get(typ)
            if shape is None:
                logging.warning("[reg] unknown service type %r (client %s tag %s)", typ, client_id, tag)
                continue
            service_id = "mqtt_{}_{}".format(client_id, tag)
            spec = {
                "service_id": service_id,
                "type": typ,
                "connection": "MQTT:" + client_id,
                "FirmwareVersion": version,
                "paths": shape,
                "init": init,        # board-authored; omitted paths -> None
                "meta": meta,
            }
            prev = self.proj.get_service(service_id)
            online = bool(prev and prev.get("connected"))
            res = self.proj.ensure(spec)  # the state daemon owns instance allocation
            # Re-apply the board's declared init UNLESS the device was already online (a
            # redundant re-announce -> don't stomp live values). A reconnect leaves the
            # device Connected=0 with its live values invalidated by the LWT, and
            # EnsureService ADOPTS an existing device without re-applying init -- so a v2
            # board (whose identity/capability values live only in init, not W/) would come
            # back blank. Re-applying restores it.
            #
            # ORDER: apply init BEFORE flipping Connected=1. If Connected goes high first,
            # the GX briefly sees a connected device whose LWT-invalidated paths are still
            # None -- e.g. a running genset reads StatusCode=None as a not-running edge (the
            # exact section-1.1 hazard, self-inflicted at reconnect).
            #
            # Safe because v2 boards deliberately keep GX-command paths OUT of init. The
            # inverter authors *actual* status in /State (continuous telemetry) and leaves
            # /Mode -- the Victron switch-position/control input -- out of init entirely
            # (invbus seeds it ONCE from hardware after the first status, then honours GX
            # writes); the genset omits /Start. So init carries only board-owned identity +
            # telemetry and can never stomp a GX command. (The bench stub + doc historically
            # put /Mode in the vebus init, which is what mis-suggests a race -- fix those, not
            # this: /Mode is desired-value-owned-by-GX, /State is actual-owned-by-board.)
            if init and not online:
                self.proj.set_values(service_id, init)
            # A registration means the client is connected -- flip this LAST (see ORDER above).
            self.proj.set_connected(service_id, True)
            ensured[tag] = {"type": typ, "instance": res["instance"]}
            logging.info("[reg] %s -> instance %d (%s%s)", service_id, res["instance"],
                         "created" if res["created"] else "adopted",
                         "" if online else "; init applied")
        # Subscribe the device's LWT so an ungraceful drop marks it disconnected.
        if lwt_topic:
            self._lwt[lwt_topic] = (client_id, lwt_value)
            self._mqtt.subscribe(lwt_topic)
            self._flush()
        if ensured:
            self._reply(client_id, ensured)

    def _reply(self, client_id, ensured):
        deviceInstance, topicPath = {}, {}
        for tag, info in ensured.items():
            t, inst = info["type"], info["instance"]
            deviceInstance[tag] = inst
            topicPath[tag] = {
                "N": "N/{}/{}/{}".format(self.portal_id, t, inst),
                "R": "R/{}/{}/{}".format(self.portal_id, t, inst),
                "W": "W/{}/{}/{}".format(self.portal_id, t, inst),
            }
        payload = {"portalId": self.portal_id, "deviceInstance": deviceInstance, "topicPath": topicPath}
        self._mqtt.publish("{}/{}/DBus".format(self.ns, client_id), json.dumps(payload))
        self._flush()
        logging.info("[reg] replied device/%s/DBus %s", client_id, deviceInstance)

    def _service_ids_for(self, client_id):
        # Match on the durable, EXACT meta client_id -- not a service_id string prefix, which
        # would also match a client named e.g. "foo_extra" when disconnecting "foo".
        return [s["service_id"] for s in self.proj.list_services()
                if (s.get("meta") or {}).get("client_id") == client_id]

    def _disconnect(self, client_id):
        # DEBOUNCE, don't act immediately. A board's connected:0 will fires on every ungraceful
        # rust-mqtt drop (see DISCONNECT_GRACE_S) -- acting at once would invalidate a live
        # board's values on a transient flap (StatusCode 8->None -> patroclus handoff-off,
        # systemcalc drops the device), relocating the section-1.1 blip from rare operator
        # restarts to frequent board reconnects. Arm a grace timer instead; a re-announce
        # cancels it, a genuine death commits after the window. NB this debounces logicd's OWN
        # action, not a Victron internal -- legitimate under the design doctrine.
        if client_id in self._pending_disc:
            return  # already armed
        self._pending_disc[client_id] = GLib.timeout_add_seconds(
            self._disc_grace, self._disconnect_commit, client_id)
        logging.info("[reg] %s will/disc -> armed %ds grace", client_id, self._disc_grace)

    def _disconnect_commit(self, client_id):
        self._pending_disc.pop(client_id, None)
        for sid in self._service_ids_for(client_id):
            self.proj.set_connected(sid, False)
            logging.info("[reg] %s -> Connected=0 (grace elapsed)", sid)
        return False  # one-shot timer

    def _cancel_pending_disc(self, client_id):
        t = self._pending_disc.pop(client_id, None)
        if t is not None:
            GLib.source_remove(t)
            logging.info("[reg] %s alive -> cancelled pending disconnect", client_id)

    def _remove(self, client_id):
        self._cancel_pending_disc(client_id)  # explicit removal supersedes any pending disc
        for sid in self._service_ids_for(client_id):
            self.proj.remove(sid)
            logging.info("[reg] removed %s", sid)

    # -- host (re)appearance: reconcile + cookie mirror -----------------------
    def _on_host_up(self, cookie):
        adopted = self.proj.reconcile()
        # Rebuild LWT subscriptions from durable per-service meta (registrations are
        # non-retained, so a logic-only restart would otherwise lose LWT monitoring).
        self._lwt = {}
        for _sid, d in adopted.items():
            m = d.get("meta") or {}
            lt = m.get("lwt_topic")
            if lt and lt not in self._lwt:
                self._lwt[lt] = (m.get("client_id"), m.get("lwt_value", "0"))
                self._mqtt.subscribe(lt)
        self._flush()
        logging.info("[host] up cookie=%s adopted=%d lwt=%d", cookie[:8], len(adopted), len(self._lwt))
        self.last_cookie = cookie
        self._publish_cookie(cookie)

    def _on_host_down(self):
        logging.info("[host] down")

    def _publish_cookie(self, cookie):
        self._mqtt.publish(self.cookie_topic, cookie, retain=True)
        self._flush()
        logging.info("[host] published %s = %s (retained)", self.cookie_topic, cookie[:8])

    def _lookup_portal(self):
        return self.portal_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--services", default=os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "services.yml"))
    ap.add_argument("--portal", default=None, help="portalId; default = look up com.victronenergy.system/Serial")
    ap.add_argument("--mqtt-host", default="127.0.0.1")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--ca-cert", default=None)
    ap.add_argument("--mqtt-user", default=None)
    ap.add_argument("--mqtt-pass", default=None)
    ap.add_argument("--ns", default="device", help="registration namespace (default device; use e.g. hstest to isolate a GX test)")
    ap.add_argument("--disconnect-grace", type=int, default=DISCONNECT_GRACE_S,
                    help="seconds a will/connected:0 is debounced before marking a board disconnected (bench uses a short value)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s [logicd] %(message)s")
    DBusGMainLoop(set_as_default=True)
    bus = dbus.SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else dbus.SystemBus()

    portal = args.portal
    if portal is None:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "ext", "velib_python"))
        try:
            from vedbus import VeDbusItemImport
            portal = str(VeDbusItemImport(bus, "com.victronenergy.system", "/Serial").get_value())
        except Exception:
            portal = "unknownportal"
    logging.info("[logicd] portalId=%s broker=%s:%d", portal, args.mqtt_host, args.mqtt_port)

    daemon = V2LogicDaemon(bus, args.services, portal, args.mqtt_host, args.mqtt_port,
                           ca_cert=args.ca_cert, user=args.mqtt_user, passwd=args.mqtt_pass, ns=args.ns,
                           disconnect_grace=args.disconnect_grace)

    loop = GLib.MainLoop()
    signal.signal(signal.SIGINT, lambda *_: loop.quit())
    signal.signal(signal.SIGTERM, lambda *_: loop.quit())
    loop.run()


if __name__ == "__main__":
    main()
