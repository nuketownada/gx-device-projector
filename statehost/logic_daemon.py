#!/usr/bin/env python3
"""
Bench logic daemon -- exercises ProjectionClient (reconcile + cookie-watch) through a
REAL restartable process, the way freakent's device_manager eventually will.

In production the north side is MQTT: registrations arrive on device/+/Status and the
cookie is mirrored RETAINED to device/_host/cookie. Here that side is a small dbus bench
interface (com.hypnos.LogicBench1) so the harness can inject registrations and inspect
the adopted view + the cookie. The state-host-facing behaviour is the real thing.

Reconcile model (docs sections 5, 7):
  - host (re)appears with the SAME cookie  -> logic-only restart; adopt via reconcile.
  - host (re)appears with a NEW cookie      -> host lost state; adopt (now empty) and
    (would) publish the cookie retained so boards re-announce with current values. We do
    NOT re-ensure from cached specs -- that would project stale init.
"""

import os
import sys
import json
import signal
import logging

import dbus
import dbus.service
from gi.repository import GLib
from dbus.mainloop.glib import DBusGMainLoop

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from projection import ProjectionClient

BENCH_BUS_NAME = "com.hypnos.logicbench"
BENCH_IFACE = "com.hypnos.LogicBench1"
BENCH_PATH = "/"


class LogicDaemon(dbus.service.Object):
    def __init__(self, bus):
        self._bus = bus
        self._busname = dbus.service.BusName(BENCH_BUS_NAME, bus, do_not_queue=True)
        super().__init__(self._busname, BENCH_PATH)

        self.proj = ProjectionClient(bus)
        self.specs = {}        # service_id -> spec (registrations we've seen this life)
        self.devices = {}      # service_id -> {"instance", "state": created|adopted}
        self.last_cookie = None
        self.cookie_published = None

        self.proj.watch_host(self._on_host_up, self._on_host_down)
        # Reconcile immediately if the host is already up (logic started after it).
        try:
            self._on_host_up(self.proj.get_cookie())
        except dbus.DBusException:
            logging.info("state host not up yet; will reconcile on its appearance")

    def _on_host_up(self, cookie):
        changed = (cookie != self.last_cookie) and (self.last_cookie is not None)
        adopted = self.proj.reconcile()
        self.devices = {sid: {"instance": d["instance"], "state": "adopted"}
                        for sid, d in adopted.items()}
        logging.info("host up cookie=%s changed=%s adopted=%d",
                     cookie[:8], changed, len(self.devices))
        if changed:
            # New incarnation: trigger re-announce (boards republish current values).
            logging.info("host incarnation changed -> publish cookie retained (re-announce)")
        self.last_cookie = cookie
        self._publish_cookie(cookie)

    def _on_host_down(self):
        logging.info("state host down; clearing adopted view")
        self.devices = {}

    def _publish_cookie(self, cookie):
        # Production: MQTT publish device/_host/cookie retained. Bench: record it.
        self.cookie_published = cookie

    # -- bench/north interface (stands in for MQTT device/+/Status) -----------
    @dbus.service.method(BENCH_IFACE, in_signature="s", out_signature="s")
    def Register(self, spec_json):
        spec = json.loads(spec_json)
        sid = spec["service_id"]
        self.specs[sid] = spec
        res = self.proj.ensure(spec)
        self.devices[sid] = {
            "instance": res["instance"],
            "state": "created" if res["created"] else "adopted",
        }
        return json.dumps(res)

    @dbus.service.method(BENCH_IFACE, in_signature="", out_signature="s")
    def ListAdopted(self):
        return json.dumps(self.devices)

    @dbus.service.method(BENCH_IFACE, in_signature="", out_signature="s")
    def LastCookie(self):
        return self.last_cookie or ""

    @dbus.service.method(BENCH_IFACE, in_signature="", out_signature="s")
    def CookiePublished(self):
        return self.cookie_published or ""


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--debug", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s [logic] %(message)s")

    DBusGMainLoop(set_as_default=True)
    bus = dbus.SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else dbus.SystemBus()
    daemon = LogicDaemon(bus)  # keep ref

    loop = GLib.MainLoop()
    signal.signal(signal.SIGINT, lambda *_: loop.quit())
    signal.signal(signal.SIGTERM, lambda *_: loop.quit())
    loop.run()


if __name__ == "__main__":
    main()
