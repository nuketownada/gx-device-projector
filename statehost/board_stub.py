#!/usr/bin/env python3
"""
Bench board stub -- an MQTT v2 client, the way a hypnos board will behave after
milestone 4. Publishes a NON-RETAINED v2 registration carrying board-authored init,
learns its instances from the device/<clientId>/DBus reply, and RE-ANNOUNCES (with
jitter) whenever the retained device/_host/cookie changes (the state-host-lost signal).

Logs events as JSONL to --log so the bench can assert.
"""

import os
import sys
import json
import time
import random
import signal
import argparse

import paho.mqtt.client as mqtt

COOKIE_TOPIC = "device/_host/cookie"

# Mirrors the ownership partition: vebus fully board-authored incl. /Mode;
# genset authors RemoteStartModeEnabled + StatusCode and OMITS /Start (GX-owned).
SERVICES = {
    "v1": {"type": "vebus", "init": {"/Mode": 3, "/ModeIsAdjustable": 1, "/State": 9,
                                     "/Dc/0/Voltage": 13.5, "/Dc/0/Current": -22.0}},
    "g1": {"type": "genset", "init": {"/RemoteStartModeEnabled": 1, "/StatusCode": 8}},
}


class BoardStub:
    def __init__(self, host, port, client_id, logpath):
        self.client_id = client_id
        self.log = open(logpath, "a", buffering=1)
        self.last_cookie = None
        self.dbus_topic = "device/{}/DBus".format(client_id)
        self.status_topic = "device/{}/Status".format(client_id)
        self.lwt_topic = "device/{}/LWT".format(client_id)
        self.lwt_value = "offline"

        self.m = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="board_" + client_id)
        self.m.on_connect = self._on_connect
        self.m.on_message = self._on_message
        # Last will: the broker publishes this if we drop ungracefully -> logicd disconnects us.
        self.m.will_set(self.lwt_topic, self.lwt_value, retain=False)
        self.m.connect(host, port, 60)
        self.m.loop_start()  # background thread is fine -- the stub touches no dbus

    def _emit(self, kind, data=""):
        self.log.write(json.dumps({"t": kind, "d": data}) + "\n")

    def _on_connect(self, c, u, flags, rc, props=None):
        c.subscribe(self.dbus_topic)
        c.subscribe(COOKIE_TOPIC)
        self._announce()

    def _announce(self):
        payload = {"clientId": self.client_id, "connected": 1, "version": "bench-1.0",
                   "proto": 2, "services": SERVICES,
                   "lwt_topic": self.lwt_topic, "lwt_value": self.lwt_value}
        # NON-RETAINED: the device exists only while we are announcing it.
        self.m.publish(self.status_topic, json.dumps(payload), retain=False)
        self._emit("announce")

    def _on_message(self, c, u, msg):
        if msg.topic == self.dbus_topic:
            self._emit("dbus_reply", json.loads(msg.payload))
        elif msg.topic == COOKIE_TOPIC:
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
    ap.add_argument("--log", required=True)
    args = ap.parse_args()

    BoardStub(args.mqtt_host, args.mqtt_port, args.client_id, args.log)
    loop = __import__("threading").Event()
    signal.signal(signal.SIGTERM, lambda *_: loop.set())
    signal.signal(signal.SIGINT, lambda *_: loop.set())
    loop.wait()


if __name__ == "__main__":
    main()
