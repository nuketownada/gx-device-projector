#!/usr/bin/env bash
# Milestone-2b end-to-end bench: real broker + v2 logic daemon + state daemon + board.
#   1) board registers v2 (non-retained, init) -> device appears with board values
#   2) LOGIC restart  -> reconcile, ZERO blip, board does NOT re-announce (cookie same)
#   3) STATE-DAEMON restart -> cookie flips -> board re-announces -> device rebuilt
# nix develop --command tests/bench_e2e_mqtt.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"; cd "$HERE"
if [ "${BENCH_PRIVATE_BUS:-}" != "1" ]; then
  exec env BENCH_PRIVATE_BUS=1 dbus-run-session -- "$0" "$@"
fi

PY=python3
PORT=11883
BLOG="$(mktemp)"; NOC="$(mktemp)"
export DSLOG="$DBUS_SESSION_BUS_ADDRESS"
echo "private bus: $DBUS_SESSION_BUS_ADDRESS  broker port: $PORT"

wait_name(){ for _ in $(seq 1 80); do $PY -c "import dbus,sys;sys.exit(0 if dbus.SessionBus().name_has_owner('$1') else 1)" 2>/dev/null && return 0; sleep 0.1; done; return 1; }
getsvc(){ $PY -c "import dbus,sys;print(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetService(sys.argv[1]))" "$1"; }
hostcookie(){ $PY -c "import dbus;print(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetCookie())"; }

cleanup(){ kill -TERM "${BOARD:-}" "${LOGICD:-}" "${DAEMON:-}" "${WATCHER:-}" "${MOQ:-}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

mosquitto -p $PORT >/tmp/moq_e2e.log 2>&1 & MOQ=$!
sleep 0.6
$PY tests/noc_watcher.py "$NOC" & WATCHER=$!
sleep 0.3
echo "--- state daemon + logicd + board ---"
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "no state host"; exit 1; }
$PY statehost/logicd.py --mqtt-port $PORT --portal benchportal --disconnect-grace 3 & LOGICD=$!
sleep 0.6
$PY statehost/board_stub.py --mqtt-port $PORT --client-id benchboard --log "$BLOG" & BOARD=$!

# wait for the device to appear on dbus
wait_name com.victronenergy.vebus.mqtt_benchboard_v1 || { echo "device never appeared"; cat "$BLOG"; exit 1; }
sleep 0.6
VINV1="$(getsvc mqtt_benchboard_v1)"
VGEN1="$(getsvc mqtt_benchboard_g1)"
HC1="$(hostcookie)"
cp "$BLOG" "${BLOG}.afterreg"
sleep 0.3

echo "--- LOGIC RESTART (reconcile; board must NOT re-announce) ---"
: > "${BLOG}"   # truncate to observe only post-restart board activity
kill -TERM "$LOGICD"; wait "$LOGICD" 2>/dev/null
sleep 0.4
$PY statehost/logicd.py --mqtt-port $PORT --portal benchportal --disconnect-grace 3 & LOGICD=$!
sleep 1.2
cp "$NOC" "${NOC}.afterlogic"
BOARD_AFTER_LOGIC="$(cat "$BLOG")"
VINV2="$(getsvc mqtt_benchboard_v1)"

echo "--- STATE-DAEMON RESTART (cookie flips; board re-announces) ---"
: > "${BLOG}"
kill -TERM "$DAEMON"; wait "$DAEMON" 2>/dev/null
sleep 0.4
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "state host gone"; exit 1; }
# logicd should see host-up, publish new cookie; board should re-announce; device rebuilt
wait_name com.victronenergy.vebus.mqtt_benchboard_v1 || { echo "device not rebuilt after re-announce"; echo "board log:"; cat "$BLOG"; exit 1; }
sleep 0.6
HC2="$(hostcookie)"
VINV3="$(getsvc mqtt_benchboard_v1)"
BOARD_AFTER_STATE="$(cat "$BLOG")"

echo "--- LWT FLAP (drop + re-announce within grace -> ZERO change, stays Connected) ---"
# The connected:0 will fires on the ungraceful drop, but logicd DEBOUNCES it (--disconnect-grace
# 3). A board that re-announces within the window must produce zero dbus change -- no
# Connected=0, no value invalidation. This is the section-1 fix: board MQTT flaps (frequent,
# uncontrolled) no longer blip the genset/meter chain the way freakent restarts used to.
kill -KILL "$BOARD"; BOARD=""
sleep 0.5   # let the broker fire the will + logicd arm the grace timer
$PY statehost/board_stub.py --mqtt-port $PORT --client-id benchboard --log "$BLOG" & BOARD=$!
sleep 1.5   # reconnect + re-announce, still well within the 3s grace -> cancels the pending disc
VINV_FLAP="$(getsvc mqtt_benchboard_v1)"

echo "--- LWT SUSTAINED death (drop, no re-announce -> commit Connected=0 after grace) ---"
kill -KILL "$BOARD"; BOARD=""
sleep 4.5   # > grace (3s): will fires, grace elapses with no re-announce -> commit disconnect
VINV4="$(getsvc mqtt_benchboard_v1)"

echo "--- assertions ---"
$PY - "${NOC}.afterlogic" "${BLOG}.afterreg" "$VINV1" "$VGEN1" "$VINV2" "$VINV3" \
      "$HC1" "$HC2" "$BOARD_AFTER_LOGIC" "$BOARD_AFTER_STATE" "$VINV4" "$VINV_FLAP" <<'PYEOF'
import sys, json
noc, blog_reg, vinv1, vgen1, vinv2, vinv3, hc1, hc2, after_logic, after_state, vinv4, vinv_flap = sys.argv[1:13]
def jl(s): return [json.loads(l) for l in s.splitlines() if l.strip()]
def jlf(p): return [json.loads(l) for l in open(p) if l.strip()]
noc_ev = jlf(noc)
def acq(p): return [e for e in noc_ev if e["name"].startswith(p) and e["new"] and not e["old"]]
def lost(p): return [e for e in noc_ev if e["name"].startswith(p) and e["old"] and not e["new"]]
reg = jl(open(blog_reg).read())
al, as_ = jl(after_logic), jl(after_state)
INV1,GEN1,INV2,INV3 = map(json.loads,(vinv1,vgen1,vinv2,vinv3))

ok=True
def check(c,m):
    global ok; ok=ok and c
    print(("PASS" if c else "FAIL")+": "+m)

# 1) registration: device populated from board init, /Start omitted
check(INV1 and INV1["values"].get("/Mode")==3, "vebus /Mode=3 from board init")
check(INV1["values"].get("/State")==9, "vebus /State=9 from board init")
check(GEN1["values"].get("/StatusCode")==8, "genset /StatusCode=8 from board init")
check(GEN1["values"].get("/Start", "X") is None, "genset /Start invalid (board omitted)")
check(any(e["t"]=="dbus_reply" for e in reg), "board received device/<id>/DBus reply")
rep = [e for e in reg if e["t"]=="dbus_reply"][-1]["d"]
check(rep.get("portalId")=="benchportal" and "v1" in rep.get("deviceInstance",{}),
      "DBus reply carries portalId + instances (%s)"%rep.get("deviceInstance"))
check(any(e["t"]=="cookie_init" for e in reg), "board received initial retained cookie")

# 2) LOGIC restart: zero blip + board silent (no re-announce) + values intact
check(len(acq("com.victronenergy.vebus."))==1 and len(lost("com.victronenergy.vebus."))==0,
      "vebus: 1 acquire / 0 loss across logic restart")
check(len(acq("com.victronenergy.genset."))==1 and len(lost("com.victronenergy.genset."))==0,
      "genset: 1 acquire / 0 loss across logic restart")
check(not any(e["t"] in ("announce","reannounce") for e in al),
      "board did NOT re-announce on logic restart (events=%s)"%[e["t"] for e in al])
check(INV2==INV1, "vebus values unchanged across logic restart")

# 3) STATE-daemon restart: cookie flips, board re-announces, device rebuilt with init
check(hc2 and hc2!=hc1, "host cookie flipped across state-daemon restart")
check(any(e["t"]=="cookie_change" for e in as_), "board saw cookie_change")
check(any(e["t"]=="reannounce" for e in as_), "board re-announced after the flip")
check(INV3 and INV3["values"].get("/Mode")==3, "device rebuilt with board init (/Mode=3)")

# 4a) LWT FLAP: drop + re-announce within the grace -> debounced, ZERO change
INV_FLAP=json.loads(vinv_flap)
check(INV_FLAP and INV_FLAP.get("connected") is True,
      "LWT flap: vebus STAYS Connected across a drop+re-announce within grace")
check(INV_FLAP["values"].get("/State")==9,
      "LWT flap: live value NOT invalidated (/State still 9)")

# 4b) LWT SUSTAINED: drop with no re-announce -> commit Connected=0 + invalidate after grace
INV4=json.loads(vinv4)
check(INV4 and INV4.get("connected") is False,
      "LWT sustained: vebus Connected=0 after grace elapses")
check(INV4["values"].get("/State") is None,
      "LWT sustained: live value invalidated (/State -> None)")
sys.exit(0 if ok else 1)
PYEOF
RC=$?; echo "--- e2e rc=$RC ---"; exit $RC
