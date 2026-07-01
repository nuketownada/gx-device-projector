#!/usr/bin/env python3
"""
Bench stand-in for the logic daemon: a thin CLI client of the state host's control
interface. Used to prove reconcile/idempotency before the real freakent refactor.

  ensure              create (or idempotently adopt) the bench vebus + genset services
  cookie              print the host's incarnation cookie
  get <service_id>    print GetService(service_id) JSON
  set <service_id> <json>   SetValues(service_id, json)
"""

import sys
import json
import dbus

CONTROL_BUS_NAME = "com.hypnos.dbusstate"
CONTROL_IFACE = "com.hypnos.DbusState1"
CONTROL_PATH = "/"

# Bench services mirror the real ownership partition (docs section 6.3):
#  - vebus (inverter): board authors actual status in /State + identity; /Mode is the
#    GX-OWNED switch position, OMITTED from init (invbus seeds it once from hardware over
#    W/ -- a flashmq path the mosquitto bench has no bridge for, so here /Mode stays None,
#    which is exactly how /Start behaves for the genset).
#  - genset: board authors RemoteStartModeEnabled + StatusCode; OMITS /Start (GX-owned).
BENCH_SERVICES = [
    {
        "service_id": "mqtt_benchinv_v1",
        "type": "vebus",
        "instance": 260,
        "ProductName": "Bench Magnum",
        "FirmwareVersion": "3.7",
        "paths": {
            "/Mode": {"writeable": True, "gx_owned": True, "description": "1=ChgOnly 2=InvOnly 3=On 4=Off (GX-owned switch position)"},
            "/ModeIsAdjustable": {},
            "/State": {},
            "/Dc/0/Voltage": {"format": "{:.2f} V"},
            "/Dc/0/Current": {"format": "{:.1f} A"},
        },
        # NOTE: no /Mode -- GX-owned, seeded from actual over W/ on the real GX; None here.
        "init": {"/ModeIsAdjustable": 1, "/State": 9,
                 "/Dc/0/Voltage": 13.5, "/Dc/0/Current": -22.0},
    },
    {
        "service_id": "mqtt_benchgen_v1",
        "type": "genset",
        "instance": 261,
        "ProductName": "Bench Genset",
        "paths": {
            "/RemoteStartModeEnabled": {},
            "/StatusCode": {"format": "{}"},
            "/Start": {"writeable": True, "description": "WRITTEN BY THE GX"},
        },
        # NOTE: no /Start -- the board does not own it, so it is created invalid.
        "init": {"/RemoteStartModeEnabled": 1, "/StatusCode": 8},
    },
]


def control():
    bus = dbus.SessionBus() if _has_session() else dbus.SystemBus()
    obj = bus.get_object(CONTROL_BUS_NAME, CONTROL_PATH)
    return dbus.Interface(obj, CONTROL_IFACE)


def _has_session():
    import os
    return "DBUS_SESSION_BUS_ADDRESS" in os.environ


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    cmd = argv[0]
    iface = control()

    if cmd == "ensure":
        for spec in BENCH_SERVICES:
            res = json.loads(iface.EnsureService(json.dumps(spec)))
            print("ensure %s -> %s" % (spec["service_id"], res))
        return 0

    if cmd == "cookie":
        print(str(iface.GetCookie()))
        return 0

    if cmd == "get":
        print(str(iface.GetService(argv[1])))
        return 0

    if cmd == "set":
        ok = iface.SetValues(argv[1], argv[2])
        print("set %s -> %s" % (argv[1], bool(ok)))
        return 0

    print("unknown command: %s" % cmd)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
