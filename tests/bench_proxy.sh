#!/usr/bin/env bash
# Verify logicd's device/<id>/Proxy fan-out (the patroclus/tanks value-relay path).
# The bench has no flashmq, so we assert logicd re-publishes the batched values as the
# correct individual W/ writes (which flashmq routes to dbus on the real GX).
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"; cd "$HERE"
if [ "${BENCH_PRIVATE_BUS:-}" != "1" ]; then exec env BENCH_PRIVATE_BUS=1 dbus-run-session -- "$0" "$@"; fi
PY=python3; PORT=11884; BLOG="$(mktemp)"; WCAP="$(mktemp)"
wait_name(){ for _ in $(seq 1 60); do $PY -c "import dbus,sys;sys.exit(0 if dbus.SessionBus().name_has_owner('$1') else 1)" 2>/dev/null && return 0; sleep 0.1; done; return 1; }
cleanup(){ kill ${SUB:-} ${BOARD:-} ${LOGICD:-} ${SD:-} ${MOQ:-} 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

mosquitto -p $PORT >/tmp/moqx.log 2>&1 & MOQ=$!; sleep 0.6
$PY statehost/state_daemon.py & SD=$!; wait_name com.hypnos.dbusstate || { echo "no state host"; exit 1; }
$PY statehost/logicd.py --mqtt-port $PORT --portal benchportal >/tmp/logicdx.log 2>&1 & LOGICD=$!; sleep 1
$PY statehost/board_stub.py --profile temp --client-id benchboard --mqtt-port $PORT --log "$BLOG" & BOARD=$!
wait_name com.victronenergy.temperature.mqtt_benchboard_t1 || { echo "device never registered"; exit 1; }
sleep 1
INST="$($PY -c "import json,dbus;print(json.loads(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetService('mqtt_benchboard_t1'))['instance'])")"
echo "temperature instance: $INST"

echo "=== capture W/ fan-out, then send a Proxy message ==="
mosquitto_sub -p $PORT -v -t "W/benchportal/temperature/$INST/#" > "$WCAP" 2>&1 & SUB=$!
sleep 0.5
mosquitto_pub -p $PORT -t device/benchboard/Proxy \
  -m "{\"topicPath\":\"W/benchportal/temperature/$INST\",\"values\":{\"Temperature\":55.5,\"Status\":0}}"
sleep 2
echo "--- W/ topics logicd fanned out: ---"; sed 's/^/  /' "$WCAP"

if grep -q "W/benchportal/temperature/$INST/Temperature .*55.5" "$WCAP"; then
  echo "=== OK PROXY: logicd fanned out /Temperature=55.5 as a W/ write ==="
  exit 0
else
  echo "=== FAIL PROXY: fan-out missing ==="; cat /tmp/logicdx.log | tail -5; exit 1
fi
