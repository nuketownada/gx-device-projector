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
INSTANCE_BASE = 200  # bench fallback; GX uses the localsettings allocator (TODO)


class V2LogicDaemon:
    def __init__(self, bus, services_yml, portal_id,
                 mqtt_host, mqtt_port, ca_cert=None, user=None, passwd=None):
        self.bus = bus
        self.portal_id = portal_id
        self.shapes = self._load_shapes(services_yml)   # type -> {abspath: meta}
        self.proj = ProjectionClient(bus)
        self.instance_map = {}                          # service_id -> instance
        self.last_cookie = None

        self.proj.watch_host(self._on_host_up, self._on_host_down)

        self._mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dbus_state_logic")
        if user:
            self._mqtt.username_pw_set(user, passwd)
        if ca_cert:
            self._mqtt.tls_set(ca_cert, cert_reqs=ssl.CERT_REQUIRED)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.connect(mqtt_host, mqtt_port, 60)
        self._init_socket()

    # -- services.yml = SHAPE only (no value defaults in the v2 model) ---------
    def _load_shapes(self, path):
        with open(path) as f:
            cfg = yaml.safe_load(f)
        shapes = {}
        for typ, paths in cfg.items():
            if not isinstance(paths, dict):
                continue
            d = {}
            for pk, meta in paths.items():
                meta = meta or {}
                d["/" + pk] = {
                    "writeable": True,  # all device paths writeable (flashmq W/ + GX control)
                    "format": meta.get("format"),
                    "persist": bool(meta.get("persist", False)),
                    "setting": bool(meta.get("setting", False)),
                    "description": meta.get("description", ""),
                }
            shapes[typ] = d
        return shapes

    # -- MQTT via the GLib mainloop (single-threaded, like MqttGObjectBridge) --
    def _init_socket(self):
        self._sock_watch = GLib.io_add_watch(self._mqtt.socket().fileno(), GLib.IO_IN, self._on_sock_in)
        self._sock_timer = GLib.timeout_add_seconds(1, self._on_sock_timer)

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
        client.subscribe("device/+/Status")
        self._flush()  # push the SUBSCRIBE out before any board announces
        try:
            self._publish_cookie(self.proj.get_cookie())
        except dbus.DBusException:
            logging.info("[mqtt] state host not up yet; cookie publish deferred to host-up")

    def _on_message(self, client, userdata, msg):
        try:
            self._handle(msg)
        except Exception:
            logging.exception("[mqtt] handling %s failed", msg.topic)

    # -- registration handshake ----------------------------------------------
    def _handle(self, msg):
        parts = msg.topic.split("/")
        if len(parts) == 3 and parts[0] == "device" and parts[2] == "Status":
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

    def _register(self, client_id, status):
        version = status.get("version", "")
        ensured = {}
        for tag, sdef in (status.get("services") or {}).items():
            # Lenient: sdef may be "type" (string) or {type, init}. Not v1 dual-support,
            # just tolerant parsing.
            if isinstance(sdef, str):
                typ, init = sdef, {}
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
                "instance": self._instance_for(service_id),
                "connection": "MQTT:" + client_id,
                "FirmwareVersion": version,
                "paths": shape,
                "init": init,        # board-authored; omitted paths -> None
            }
            res = self.proj.ensure(spec)
            ensured[tag] = {"type": typ, "instance": res["instance"]}
            logging.info("[reg] %s -> instance %d (%s)", service_id, res["instance"],
                         "created" if res["created"] else "adopted")
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
        self._mqtt.publish("device/{}/DBus".format(client_id), json.dumps(payload))
        self._flush()
        logging.info("[reg] replied device/%s/DBus %s", client_id, deviceInstance)

    def _service_ids_for(self, client_id):
        prefix = "mqtt_{}_".format(client_id)
        return [s["service_id"] for s in self.proj.list_services()
                if s["service_id"].startswith(prefix)]

    def _disconnect(self, client_id):
        for sid in self._service_ids_for(client_id):
            self.proj.set_connected(sid, False)
            logging.info("[reg] %s -> Connected=0", sid)

    def _remove(self, client_id):
        for sid in self._service_ids_for(client_id):
            self.proj.remove(sid)
            self.instance_map.pop(sid, None)
            logging.info("[reg] removed %s", sid)

    def _instance_for(self, service_id):
        if service_id in self.instance_map:
            return self.instance_map[service_id]
        used = set(self.instance_map.values())
        inst = INSTANCE_BASE
        while inst in used:
            inst += 1
        self.instance_map[service_id] = inst
        return inst

    # -- host (re)appearance: reconcile + cookie mirror -----------------------
    def _on_host_up(self, cookie):
        adopted = self.proj.reconcile()
        for sid, d in adopted.items():
            self.instance_map[sid] = d["instance"]
        logging.info("[host] up cookie=%s adopted=%d", cookie[:8], len(adopted))
        self.last_cookie = cookie
        self._publish_cookie(cookie)

    def _on_host_down(self):
        logging.info("[host] down")

    def _publish_cookie(self, cookie):
        self._mqtt.publish(COOKIE_TOPIC, cookie, retain=True)
        self._flush()
        logging.info("[host] published %s = %s (retained)", COOKIE_TOPIC, cookie[:8])

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
                           ca_cert=args.ca_cert, user=args.mqtt_user, passwd=args.mqtt_pass)

    loop = GLib.MainLoop()
    signal.signal(signal.SIGINT, lambda *_: loop.quit())
    signal.signal(signal.SIGTERM, lambda *_: loop.quit())
    loop.run()


if __name__ == "__main__":
    main()
