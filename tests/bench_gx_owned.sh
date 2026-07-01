#!/usr/bin/env bash
# gx_owned + additive-shape-evolution bench (the PR-1 state-daemon semantics):
#   - GxWrite GATING: a dbus write to a gx_owned path (/Start -- what startstop.py
#     does) emits GxWrite; a telemetry-style write to a plain writeable path
#     (/StatusCode -- what a flashmq W/ sample does) must NOT. Every W/ sample lands
#     on the same onchange; ungated, each one is pointless bus traffic.
#   - INVALIDATION EXEMPTION: a committed disconnect nulls live telemetry
#     (/StatusCode -> None, the stuck-meter fix) but must NOT null standing GX
#     intent (/Start) -- otherwise every board flap erases a scheduler command.
#   - ADDITIVE SHAPE EVOLUTION: EnsureService on an ADOPTED service creates a
#     newly-declared path (with its init value, visible on the actual bus) with
#     ZERO NameOwnerChanged -- so the durable daemon never restarts to ship a
#     board feature. A further re-ensure must NOT re-apply init to the
#     now-existing path (init-only-on-creation, extended to path granularity).
# Runs against a private session bus so it can't touch the real bus.
#
# Run under the dev shell:  nix develop --command tests/bench_gx_owned.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"

# Re-exec under a private session bus if not already inside one.
if [ "${BENCH_PRIVATE_BUS:-}" != "1" ]; then
  exec env BENCH_PRIVATE_BUS=1 dbus-run-session -- "$0" "$@"
fi

PY=python3
echo "private bus: $DBUS_SESSION_BUS_ADDRESS"

NOC="$(mktemp)"
wait_name() {  # wait until well-known name has an owner
  for _ in $(seq 1 60); do
    if $PY -c "import dbus,sys; sys.exit(0 if dbus.SessionBus().name_has_owner('$1') else 1)" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

cleanup() { kill -TERM "${DAEMON:-}" "${WATCHER:-}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

$PY tests/noc_watcher.py "$NOC" & WATCHER=$!
sleep 0.5

echo "--- start state daemon ---"
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "daemon never claimed control name"; exit 1; }

echo "--- exercise gx_owned + shape evolution ---"
$PY - "$NOC" <<'PYEOF'
import sys, json, time
import dbus, dbus.mainloop.glib
from gi.repository import GLib

dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
noc_path = sys.argv[1]
bus = dbus.SessionBus()
ctl = dbus.Interface(bus.get_object("com.hypnos.dbusstate", "/"), "com.hypnos.DbusState1")
SVC = "com.victronenergy.genset.mqtt_gxbench_g1"

fails = []
def check(cond, msg):
    print(("PASS" if cond else "FAIL") + ": " + msg)
    if not cond:
        fails.append(msg)

def item(path):
    return dbus.Interface(bus.get_object(SVC, path), "com.victronenergy.BusItem")

def getsvc():
    return json.loads(ctl.GetService("mqtt_gxbench_g1"))

def pump(ms=400):
    loop = GLib.MainLoop()
    GLib.timeout_add(ms, loop.quit)
    loop.run()

# The real ownership partition (docs section 6.3): /Start is GX-owned and
# OMITTED from init; /StatusCode + /RemoteStartModeEnabled are board-authored.
spec = {
    "service_id": "mqtt_gxbench_g1",
    "type": "genset",
    "instance": 270,
    "paths": {
        "/Start":                  {"writeable": True, "gx_owned": True},
        "/StatusCode":             {"writeable": True},
        "/RemoteStartModeEnabled": {"writeable": True},
    },
    "init": {"/RemoteStartModeEnabled": 1, "/StatusCode": 8},
}
res = json.loads(ctl.EnsureService(json.dumps(spec)))
check(res["created"] is True, "genset created")

# --- 1) GxWrite gating -------------------------------------------------------
signals = []
bus.add_signal_receiver(
    lambda sid, path, val: signals.append((str(sid), str(path), str(val))),
    signal_name="GxWrite", dbus_interface="com.hypnos.DbusState1")

item("/Start").SetValue(1)       # GX command write (startstop.py)
item("/StatusCode").SetValue(8)  # telemetry-style write (flashmq W/ sample)
pump()

check(any(p == "/Start" for _, p, _ in signals),
      "GxWrite emitted for gx_owned /Start")
check(not any("StatusCode" in p for _, p, _ in signals),
      "GxWrite NOT emitted for telemetry /StatusCode")

# --- 2) committed-disconnect invalidation exemption --------------------------
ctl.SetConnected("mqtt_gxbench_g1", False)
v = getsvc()["values"]
check(v.get("/Start") == 1,
      "gx_owned /Start SURVIVES a committed disconnect (standing GX intent)")
check(v.get("/StatusCode") is None,
      "telemetry /StatusCode invalidated on disconnect (stuck-meter fix)")
ctl.SetConnected("mqtt_gxbench_g1", True)

# --- 3) additive shape evolution on the ADOPTED service ----------------------
spec["paths"]["/Engine/OperatingHours"] = {"writeable": True, "format": "{} s"}
spec["init"]["/Engine/OperatingHours"] = 1234
res = json.loads(ctl.EnsureService(json.dumps(spec)))
check(res["created"] is False, "re-ensure ADOPTS (created=False)")
check(getsvc()["values"].get("/Engine/OperatingHours") == 1234,
      "new path created on the live service with its init value")
check(int(item("/Engine/OperatingHours").GetValue()) == 1234,
      "new path readable on the actual bus (post-register add_path)")

# init-only-on-creation, at path granularity: mutate the live value, re-ensure
# with the SAME spec -- the (now-existing) path must NOT be re-initialized, and
# a standing /Start must not be touched either.
item("/Engine/OperatingHours").SetValue(5678)
pump(150)
ctl.EnsureService(json.dumps(spec))
v = getsvc()["values"]
check(v.get("/Engine/OperatingHours") == 5678,
      "re-ensure does NOT re-apply init to an existing path")
check(v.get("/Start") == 1,
      "re-ensure leaves standing GX intent untouched")

# --- 4) all of the above with ZERO blip --------------------------------------
time.sleep(0.4)
noc = [json.loads(l) for l in open(noc_path) if l.strip()]
loss = [e for e in noc if e.get("name") == SVC and e.get("old") and not e.get("new")]
check(len(loss) == 0, "zero NameOwnerChanged loss on the hosted name throughout")

sys.exit(1 if fails else 0)
PYEOF
RC=$?

echo "--- gx_owned rc=$RC ---"
exit $RC
