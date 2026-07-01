#!/usr/bin/env python3
"""
ProjectionClient -- the logic-daemon's view of the durable state host.

This is the reusable piece that replaces "freakent owns VeDbusService": the logic
daemon drives the host through it (ensure/set/remove), recovers its routing from the
host after its OWN restart (reconcile, since v2 registrations are non-retained and
boards do NOT re-announce on a logic-only restart), and reacts to a host restart via
the incarnation cookie.

See docs/transparent-projection.md sections 4-5, 7.
"""

import json
import logging

import dbus

CONTROL_BUS_NAME = "com.hypnos.dbusstate"
CONTROL_IFACE = "com.hypnos.DbusState1"
CONTROL_PATH = "/"


class ProjectionClient:
    def __init__(self, bus):
        self._bus = bus
        self._iface = None

    def _control(self):
        if self._iface is None:
            obj = self._bus.get_object(CONTROL_BUS_NAME, CONTROL_PATH)
            self._iface = dbus.Interface(obj, CONTROL_IFACE)
        return self._iface

    # -- drive ----------------------------------------------------------------
    def ensure(self, spec):
        """Create-or-adopt. Returns {'instance': int, 'created': bool}."""
        return json.loads(self._control().EnsureService(json.dumps(spec)))

    def set_values(self, service_id, values):
        return bool(self._control().SetValues(service_id, json.dumps(values)))

    def set_connected(self, service_id, connected):
        return bool(self._control().SetConnected(service_id, bool(connected)))

    def remove(self, service_id):
        return bool(self._control().RemoveService(service_id))

    # -- recover --------------------------------------------------------------
    def list_services(self):
        return json.loads(self._control().ListServices())

    def get_service(self, service_id):
        return json.loads(self._control().GetService(service_id))

    def get_cookie(self):
        return str(self._control().GetCookie())

    def reconcile(self):
        """Adopt whatever the host currently holds -> {service_id: {instance, type}}.

        The logic daemon calls this on startup (and on a host (re)appearance) to rebuild
        its routing table WITHOUT recreating services. Adoption is a no-op on the host,
        so it never blips a hosted name.
        """
        return {s["service_id"]: {"instance": s["instance"], "type": s["type"],
                                  "meta": s.get("meta", {})}
                for s in self.list_services()}

    # -- react ----------------------------------------------------------------
    def watch_host(self, on_up, on_down=None):
        """Fire on_up(cookie) when the host's control name (re)appears, on_down() when it
        vanishes. The caller reconciles + (re)publishes the cookie in on_up."""
        def handler(name, old, new):
            old, new = str(old), str(new)
            if new and not old:
                self._iface = None  # new owner -> drop the cached proxy
                try:
                    on_up(self.get_cookie())
                except Exception:
                    logging.exception("watch_host on_up failed")
            elif old and not new:
                self._iface = None
                if on_down:
                    try:
                        on_down()
                    except Exception:
                        logging.exception("watch_host on_down failed")

        self._bus.add_signal_receiver(
            handler,
            signal_name="NameOwnerChanged",
            dbus_interface="org.freedesktop.DBus",
            arg0=CONTROL_BUS_NAME,
        )
