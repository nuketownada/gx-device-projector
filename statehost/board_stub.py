#!/usr/bin/env python3
"""
Bench / GX-integration board stub -- an MQTT v2 client, the way a hypnos board will
behave after milestone 4. Publishes a NON-RETAINED v2 registration carrying
board-authored init, learns its instances from the <ns>/<clientId>/DBus reply, optionally
writes a telemetry value over W/ (to exercise the GX's flashmq -> dbus path), and
RE-ANNOUNCES (jittered) whenever the retained <ns>/_host/cookie changes.

Logs events as JSONL to --log so a harness can assert.
"""

import os
import sys
import json
import time
import random
import signal
import argparse
import threading

import paho.mqtt.client as mqtt

# Ownership partition mirrored per profile. `full` = the bench (vebus fully board-authored
# incl. /Mode; genset omits GX-owned /Start). `temp` = a harmless device for the live-GX
# integration test (a phantom vebus could perturb ESS/DVCC; a temperature sensor cannot).
PROFILES = {
    "full": {
        "v1": {"type": "vebus", "init": {"/Mode": 3, "/ModeIsAdjustable": 1, "/State": 9,
                                         "/Dc/0/Voltage": 13.5, "/Dc/0/Current": -22.0}},
        "g1": {"type": "genset", "init": {"/RemoteStartModeEnabled": 1, "/StatusCode": 8}},
    },
    "temp": {
        "t1": {"type": "temperature", "init": {"/Temperature": 21.5}},
    },
}
# Telemetry pushed over W/ after registration -> flashmq -> the state daemon's dbus path.
WRITES = {
    "full": {},
    "temp": {"t1": {"/Temperature": 12.34}},
}


class BoardStub:
    def __init__(self, host, port, client_id, logpath, ns="device", profile="full",
                 do_write=False, ca_cert=None):
        self.client_id = client_id
        self.ns = ns
        self.services = PROFILES[profile]
        self.writes = WRITES.get(profile, {})
        self.do_write = do_write
        self.log = open(logpath, "a", buffering=1)
        self.last_cookie = None
        self.dbus_topic = "{}/{}/DBus".format(ns, client_id)
        self.status_topic = "{}/{}/Status".format(ns, client_id)
        self.cookie_topic = "{}/_host/cookie".format(ns)
        self.lwt_topic = "{}/{}/LWT".format(ns, client_id)
        self.lwt_value = "offline"

        self.m = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="board_" + client_id)
        if ca_cert:
            import ssl
            self.m.tls_set(ca_cert, cert_reqs=ssl.CERT_REQUIRED)
        self.m.on_connect = self._on_connect
        self.m.on_message = self._on_message
        self.m.will_set(self.lwt_topic, self.lwt_value, retain=False)
        self.m.connect(host, port, 60)
        self.m.loop_start()  # background thread is fine -- the stub touches no dbus

    def _emit(self, kind, data=""):
        self.log.write(json.dumps({"t": kind, "d": data}) + "\n")

    def _on_connect(self, c, u, flags, rc, props=None):
        c.subscribe(self.dbus_topic)
        c.subscribe(self.cookie_topic)
        self._announce()

    def _announce(self):
        payload = {"clientId": self.client_id, "connected": 1, "version": "bench-1.0",
                   "proto": 2, "services": self.services,
                   "lwt_topic": self.lwt_topic, "lwt_value": self.lwt_value}
        self.m.publish(self.status_topic, json.dumps(payload), retain=False)  # NON-RETAINED
        self._emit("announce")

    def _write_telemetry(self, reply):
        # reply.topicPath[tag]["W"] == "W/<portal>/<type>/<inst>"; append the path.
        tp = reply.get("topicPath", {})
        for tag, paths in self.writes.items():
            wbase = tp.get(tag, {}).get("W")
            if not wbase:
                continue
            for path, val in paths.items():
                self.m.publish(wbase + path, json.dumps({"value": val}))
                self._emit("write", {"topic": wbase + path, "value": val})

    def _on_message(self, c, u, msg):
        if msg.topic == self.dbus_topic:
            reply = json.loads(msg.payload)
            self._emit("dbus_reply", reply)
            if self.do_write:
                self._write_telemetry(reply)
        elif msg.topic == self.cookie_topic:
            cookie = msg.payload.decode()
            if not cookie:
                return
            if self.last_cookie is None:
                self.last_cookie = cookie
                self._emit("cookie_init", cookie)
            elif cookie != self.last_cookie:
                self.last_cookie = cookie
                self._emit("cookie_change", cookie)
                time.sleep(random.uniform(0.05, 0.25))  # jitter -> avoid a synchronized FPC flood
                self._announce()
                self._emit("reannounce", cookie)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client-id", default="benchboard")
    ap.add_argument("--mqtt-host", default="127.0.0.1")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--ca-cert", default=None)
    ap.add_argument("--ns", default="device")
    ap.add_argument("--profile", default="full", choices=list(PROFILES.keys()))
    ap.add_argument("--write", action="store_true", help="push a W/ telemetry value after registration")
    ap.add_argument("--log", required=True)
    args = ap.parse_args()

    BoardStub(args.mqtt_host, args.mqtt_port, args.client_id, args.log,
              ns=args.ns, profile=args.profile, do_write=args.write, ca_cert=args.ca_cert)
    ev = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: ev.set())
    signal.signal(signal.SIGINT, lambda *_: ev.set())
    ev.wait()


if __name__ == "__main__":
    main()
