#!/bin/sh
# Guarded GX integration test for the transparent-projection daemons.
# Isolated on the `hstest` MQTT namespace + a harmless temperature device, so the
# running freakent and the energy system are untouched. Self-cleaning.
set -u
export PYTHONPATH=/data/drivers/dbus-mqtt-devices-0.9.0/ext/velib_python
unset DBUS_SESSION_BUS_ADDRESS
cd /data/hstest
PY=python3
CID=hstestboard
SVC=com.victronenergy.temperature.mqtt_${CID}_t1
SETTING=/Settings/Devices/mqtt_${CID}_t1/ClassAndVrmInstance

cleanup() {
  kill ${BD:-} ${LD:-} ${SD:-} 2>/dev/null
  sleep 1
  $PY - <<PYEOF 2>/dev/null
import dbus
b=dbus.SystemBus(); s=b.get_object("com.victronenergy.settings","/")
try:
    print("RemoveSettings ->", s.RemoveSettings(dbus.Array(["$SETTING"], signature="s"),
          dbus_interface="com.victronenergy.Settings"))
except Exception as e:
    print("RemoveSettings failed (harmless leftover):", e)
PYEOF
}
trap cleanup EXIT

echo "=== baseline: real freakent-hosted devices ==="
dbus -y 2>/dev/null | grep -oE "com\.victronenergy\.[a-z]+\.mqtt_[A-Za-z0-9_]+" | sort -u > /tmp/hs_base.txt
cat /tmp/hs_base.txt

poll() {  # poll <file> <pattern> <max_halfsecs>
  i=0; while [ $i -lt $3 ]; do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.5; i=$((i+1)); done; return 1
}

echo "=== start state daemon (--system) ==="
$PY statehost/state_daemon.py --system >/tmp/hs_state.log 2>&1 & SD=$!
poll /tmp/hs_state.log "state host up" 24 && echo "  state daemon up" || { echo "  STATE DAEMON FAILED"; tail -15 /tmp/hs_state.log; exit 1; }

echo "=== start logicd (ns=hstest) ==="
$PY statehost/logicd.py --ns hstest --mqtt-host 127.0.0.1 --mqtt-port 1883 >/tmp/hs_logicd.log 2>&1 & LD=$!
poll /tmp/hs_logicd.log "connected rc" 30 && echo "  logicd connected" || { echo "  LOGICD FAILED"; tail -15 /tmp/hs_logicd.log; exit 1; }

echo "=== start board_stub (temperature + W/ write) ==="
$PY statehost/board_stub.py --ns hstest --profile temp --write --client-id $CID \
    --mqtt-host 127.0.0.1 --mqtt-port 1883 --log /tmp/hs_board.log >/tmp/hs_board_out.log 2>&1 & BD=$!
# poll for the device to appear on dbus, then give flashmq a moment to route the W/ write
i=0; while [ $i -lt 30 ]; do dbus -y 2>/dev/null | grep -q "$SVC" && break; sleep 0.5; i=$((i+1)); done
sleep 2

echo "=== RESULTS ============================================"
echo "-- device on dbus? --"
if dbus -y 2>/dev/null | grep -q "$SVC"; then echo "  PRESENT: $SVC"; else echo "  MISSING: $SVC"; fi
echo "-- /DeviceInstance (localsettings-allocated) --"
echo "  $(dbus -y $SVC /DeviceInstance GetValue 2>&1)"
echo "-- /Temperature (expect 12.34 via flashmq W/ -> dbus) --"
echo "  $(dbus -y $SVC /Temperature GetValue 2>&1)"
echo "-- /Connected --"
echo "  $(dbus -y $SVC /Connected GetValue 2>&1)"
echo "-- /Mgmt/ProcessName (should be our daemon) --"
echo "  $(dbus -y $SVC /Mgmt/ProcessName GetValue 2>&1)"
echo "-- localsettings ClassAndVrmInstance --"
echo "  $(dbus -y com.victronenergy.settings $SETTING GetValue 2>&1)"
echo "-- cookie --"
echo "  $(dbus -y com.hypnos.dbusstate / GetCookie 2>&1)"

echo "-- real devices unchanged? --"
dbus -y 2>/dev/null | grep -oE "com\.victronenergy\.[a-z]+\.mqtt_[A-Za-z0-9_]+" | grep -v "mqtt_${CID}_t1" | sort -u > /tmp/hs_now.txt
if diff /tmp/hs_base.txt /tmp/hs_now.txt >/dev/null; then echo "  REAL DEVICES UNCHANGED"; else echo "  !!! DIFF:"; diff /tmp/hs_base.txt /tmp/hs_now.txt; fi

echo "-- board event log --"
sed 's/^/  /' /tmp/hs_board.log
echo "=== logicd tail ==="; tail -6 /tmp/hs_logicd.log | sed 's/^/  /'
echo "=== END RESULTS ========================================"
