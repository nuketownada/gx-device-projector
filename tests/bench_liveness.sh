#!/usr/bin/env bash
# Retained-online liveness bench -- proves the stuck-Connected=1 hole is SHUT.
#
# The hole (docs section 5.2, before this change): a board's connected:0 will is non-retained,
# so if it fires while logicd is DOWN it's lost; logicd restarts, reconcile adopts the service
# with its stale Connected=1, and nothing ever invalidates it -- a dead board counts as alive,
# telemetry frozen, the exact stuck-meter failure SetConnected invalidation exists to prevent.
#
# The fix: the board's will is a RETAINED device/<id>/online=0. On restart logicd subscribes
# <ns>/+/online and the broker delivers that retained 0 immediately -> it arms the disconnect
# grace (single-writer: online never sets Connected=1) -> commit lands grace-after-reconcile.
#
# This test kills the board WHILE LOGICD IS DOWN, so the ONLY surviving signal is the retained
# online=0; if Connected commits to 0, it did so through the retained topic. Run under the dev
# shell:  nix develop --command tests/bench_liveness.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"; cd "$HERE"
if [ "${BENCH_PRIVATE_BUS:-}" != "1" ]; then
  exec env BENCH_PRIVATE_BUS=1 dbus-run-session -- "$0" "$@"
fi

PY=python3
PORT=11884
GRACE=3
BLOG="$(mktemp)"
echo "private bus: $DBUS_SESSION_BUS_ADDRESS  broker port: $PORT"

wait_name(){ for _ in $(seq 1 80); do $PY -c "import dbus,sys;sys.exit(0 if dbus.SessionBus().name_has_owner('$1') else 1)" 2>/dev/null && return 0; sleep 0.1; done; return 1; }
getconn(){ $PY -c "import dbus,sys,json;print(json.loads(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetService('mqtt_livebench_g1')).get('connected'))"; }
getstate(){ $PY -c "import dbus,sys,json;print(json.loads(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetService('mqtt_livebench_g1'))['values'].get('/StatusCode'))"; }

cleanup(){ kill -TERM "${BOARD:-}" "${LOGICD:-}" "${DAEMON:-}" "${MOQ:-}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

mosquitto -p $PORT >/tmp/moq_live.log 2>&1 & MOQ=$!
sleep 0.6
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "no state host"; exit 1; }
$PY statehost/logicd.py --mqtt-port $PORT --portal livebench --disconnect-grace $GRACE & LOGICD=$!
sleep 0.6
$PY statehost/board_stub.py --mqtt-port $PORT --client-id livebench --log "$BLOG" & BOARD=$!

wait_name com.victronenergy.genset.mqtt_livebench_g1 || { echo "device never appeared"; cat "$BLOG"; exit 1; }
sleep 0.6
echo "--- registered: connected=$(getconn) statuscode=$(getstate) ---"

echo "--- stop logicd, THEN kill the board (will fires while logicd is DOWN) ---"
kill -TERM "$LOGICD"; wait "$LOGICD" 2>/dev/null
kill -KILL "$BOARD"; BOARD=""
sleep 1.0   # broker detects the dropped socket, publishes the RETAINED online=0 will

echo "--- restart logicd: it must learn liveness from the retained online, not a live will ---"
$PY statehost/logicd.py --mqtt-port $PORT --portal livebench --disconnect-grace $GRACE & LOGICD=$!
wait_name com.hypnos.dbusstate >/dev/null   # (already up; just a sync point)
sleep 1.0   # logicd reconnects, reconciles (adopts stale Connected=1), gets retained online=0 -> arms grace
CONN_EARLY="$(getconn)"
echo "shortly after restart: connected=$CONN_EARLY (expect True -- adopted stale, grace armed not committed)"
sleep $((GRACE + 2))
CONN_LATE="$(getconn)"; STATE_LATE="$(getstate)"
echo "after grace: connected=$CONN_LATE statuscode=$STATE_LATE"

echo "--- SINGLE-WRITER: a bare retained online=1 (no registration) must NOT resurrect the device ---"
# This pins the rule the design states: online never sets Connected=1 -- that comes exclusively
# from a processed registration (which applies init FIRST). If online=1 could flip Connected,
# the GX would see a connected device whose values are still invalidated (the exact ordering
# hazard the init-before-Connected fix removed), through a topic the board publishes BEFORE its
# announce. A mutation adding set_connected(True) to the "1" branch passes every other bench;
# only these assertions catch it.
mosquitto_pub -p $PORT -r -t device/livebench/online -m 1
sleep 1.0
CONN_BARE="$(getconn)"; STATE_BARE="$(getstate)"
echo "after bare online=1: connected=$CONN_BARE statuscode=$STATE_BARE"

echo "--- recovery: the board itself returns -> registration restores init THEN Connected ---"
$PY statehost/board_stub.py --mqtt-port $PORT --client-id livebench --log "$BLOG" & BOARD=$!
sleep 1.5
CONN_BACK="$(getconn)"; STATE_BACK="$(getstate)"
echo "after re-announce: connected=$CONN_BACK statuscode=$STATE_BACK"

echo "--- STALE TAKEOVER-WILL: a late retained 0 on a LIVE board must self-heal to 1 ---"
# FlashMQ fires the OLD connection's retained will cross-thread on session takeover --
# unordered vs the new connection's online=1, and deferrable to the keepalive reaper
# (~1.5x keepalive) when the dead socket's buffers are full. Simulate the late will
# landing on the live board's topic; the board's own-topic subscription must re-assert 1.
mosquitto_pub -p $PORT -r -t device/livebench/online -m 0
sleep 1.0
RETAINED_NOW="$(timeout 3 mosquitto_sub -p $PORT -t device/livebench/online -C 1 2>/dev/null)"
echo "retained after stale 0 + heal window: $RETAINED_NOW"

# and the projector consequence the race actually threatens: a LATER logicd restart reading
# the retained store must NOT falsely disconnect the live board.
kill -TERM $LOGICD 2>/dev/null; wait $LOGICD 2>/dev/null
$PY statehost/logicd.py --mqtt-port $PORT --portal livebench --disconnect-grace $GRACE & LOGICD=$!
sleep $((GRACE + 3))
CONN_HEALED="$(getconn)"; STATE_HEALED="$(getstate)"
echo "after logicd restart over healed topic: connected=$CONN_HEALED statuscode=$STATE_HEALED"

echo "--- assertions ---"
RC=0
[ "$CONN_EARLY" = "True" ] && echo "PASS: adopted the stale Connected=1 on restart (grace not yet elapsed)" || { echo "FAIL: expected Connected=1 right after restart, got $CONN_EARLY"; RC=1; }
[ "$CONN_LATE" = "False" ] && echo "PASS: retained online=0 committed Connected=0 within grace (hole SHUT)" || { echo "FAIL: dead board stuck Connected=$CONN_LATE -- retained online did not commit"; RC=1; }
[ "$STATE_LATE" = "None" ] && echo "PASS: live values invalidated on the commit (stuck-meter fix applied)" || { echo "FAIL: StatusCode not invalidated (got $STATE_LATE)"; RC=1; }
[ "$CONN_BARE" = "False" ] && echo "PASS: single-writer -- bare online=1 did NOT set Connected=1" || { echo "FAIL: online=1 resurrected the device without a registration (Connected=$CONN_BARE)"; RC=1; }
[ "$STATE_BARE" = "None" ] && echo "PASS: single-writer -- values still invalidated after bare online=1" || { echo "FAIL: values changed on a bare online=1 (StatusCode=$STATE_BARE)"; RC=1; }
[ "$CONN_BACK" = "True" ] && [ "$STATE_BACK" = "8" ] && echo "PASS: recovery via registration -- init reapplied, then Connected=1" || { echo "FAIL: recovery wrong (connected=$CONN_BACK statuscode=$STATE_BACK)"; RC=1; }
[ "$RETAINED_NOW" = "1" ] && echo "PASS: stale takeover-will healed -- live board re-asserted retained online=1" || { echo "FAIL: retained online stuck at '$RETAINED_NOW' after stale 0 on a live board"; RC=1; }
[ "$CONN_HEALED" = "True" ] && [ "$STATE_HEALED" = "8" ] && echo "PASS: logicd restart over the healed topic keeps the live board Connected=1" || { echo "FAIL: live board falsely disconnected by stale will (connected=$CONN_HEALED statuscode=$STATE_HEALED)"; RC=1; }
echo "--- liveness rc=$RC ---"; exit $RC
