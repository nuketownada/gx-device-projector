#!/usr/bin/env bash
# Milestone-2 bench (docs/transparent-projection.md sections 5, 7):
#   - LOGIC restart: the logic daemon RECONCILES (adopts services from the host) with
#     ZERO blip on the hosted names + a stable host cookie  -> redeploys are invisible.
#   - STATE-DAEMON restart: the logic daemon detects it (cookie watch) and mirrors the
#     NEW cookie  -> the re-announce trigger fires.
# Runs against a private session bus.  nix develop --command tests/bench_logic_reconcile.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"

if [ "${BENCH_PRIVATE_BUS:-}" != "1" ]; then
  exec env BENCH_PRIVATE_BUS=1 dbus-run-session -- "$0" "$@"
fi

PY=python3
echo "private bus: $DBUS_SESSION_BUS_ADDRESS"
NOC="$(mktemp)"

wait_name() {
  for _ in $(seq 1 60); do
    if $PY -c "import dbus,sys; sys.exit(0 if dbus.SessionBus().name_has_owner('$1') else 1)" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  return 1
}
lb()         { $PY -c "import dbus,sys; print(getattr(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.logicbench','/'),'com.hypnos.LogicBench1'),sys.argv[1])())" "$1"; }
host_cookie(){ $PY -c "import dbus; print(dbus.Interface(dbus.SessionBus().get_object('com.hypnos.dbusstate','/'),'com.hypnos.DbusState1').GetCookie())"; }
register_all() {
$PY - <<'PY'
import sys, dbus, json
sys.path.insert(0, "statehost")
from stub_logic import BENCH_SERVICES
i = dbus.Interface(dbus.SessionBus().get_object("com.hypnos.logicbench","/"), "com.hypnos.LogicBench1")
for s in BENCH_SERVICES:
    print("  register", s["service_id"], "->", str(i.Register(json.dumps(s))))
PY
}

cleanup(){ kill -TERM "${LOGIC:-}" "${DAEMON:-}" "${WATCHER:-}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

$PY tests/noc_watcher.py "$NOC" & WATCHER=$!
sleep 0.4
echo "--- start state daemon + logic daemon ---"
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "no state host"; exit 1; }
$PY statehost/logic_daemon.py & LOGIC=$!
wait_name com.hypnos.logicbench || { echo "no logic daemon"; exit 1; }

echo "--- register devices (north side) ---"
register_all
ADOPTED1="$(lb ListAdopted)"
HCOOKIE1="$(host_cookie)"
LCOOKIE1="$(lb LastCookie)"
sleep 0.4

echo "--- LOGIC RESTART (must reconcile, zero blip) ---"
kill -TERM "$LOGIC"; wait "$LOGIC" 2>/dev/null
sleep 0.3
$PY statehost/logic_daemon.py & LOGIC=$!
wait_name com.hypnos.logicbench || { echo "logic did not come back"; exit 1; }
sleep 0.3
ADOPTED2="$(lb ListAdopted)"
LCOOKIE2="$(lb LastCookie)"
cp "$NOC" "${NOC}.afterlogic"   # snapshot before any state-daemon kill

echo "--- STATE-DAEMON RESTART (cookie must flip; logic must mirror) ---"
kill -TERM "$DAEMON"; wait "$DAEMON" 2>/dev/null
sleep 0.4
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "state host did not come back"; exit 1; }
sleep 0.8   # let the logic daemon process NameOwnerChanged
HCOOKIE2="$(host_cookie)"
LCOOKIE3="$(lb LastCookie)"
PUBLISHED="$(lb CookiePublished)"
ADOPTED3="$(lb ListAdopted)"

echo "--- simulate board re-announce after the flip ---"
register_all
ADOPTED4="$(lb ListAdopted)"

echo "--- assertions ---"
$PY - "${NOC}.afterlogic" "$ADOPTED1" "$ADOPTED2" "$HCOOKIE1" "$LCOOKIE1" "$LCOOKIE2" \
       "$HCOOKIE2" "$LCOOKIE3" "$PUBLISHED" "$ADOPTED3" "$ADOPTED4" <<'PYEOF'
import sys, json
noc, a1, a2, hc1, lc1, lc2, hc2, lc3, pub, a3, a4 = sys.argv[1:12]
events = [json.loads(l) for l in open(noc) if l.strip()]
def acq(p): return [e for e in events if e["name"].startswith(p) and e["new"] and not e["old"]]
def lost(p): return [e for e in events if e["name"].startswith(p) and e["old"] and not e["new"]]
A1,A2,A3,A4 = map(json.loads, (a1,a2,a3,a4))

ok=True
def check(c,m):
    global ok; ok = ok and c
    print(("PASS" if c else "FAIL")+": "+m)

# zero blip across the LOGIC restart
check(len(acq("com.victronenergy.vebus."))==1 and len(lost("com.victronenergy.vebus."))==0,
      "vebus: 1 acquire / 0 loss across logic restart (acq=%d loss=%d)"%(len(acq("com.victronenergy.vebus.")),len(lost("com.victronenergy.vebus."))))
check(len(acq("com.victronenergy.genset."))==1 and len(lost("com.victronenergy.genset."))==0,
      "genset: 1 acquire / 0 loss across logic restart (acq=%d loss=%d)"%(len(acq("com.victronenergy.genset.")),len(lost("com.victronenergy.genset."))))

# reconcile: logic recovered both devices as ADOPTED after its restart
check(set(A1)=={"mqtt_benchinv_v1","mqtt_benchgen_v1"}, "pre-restart adopted both (created)")
check(set(A2)==set(A1), "post-restart logic reconciled the SAME devices (%s)"%sorted(A2))
check(all(v["state"]=="adopted" for v in A2.values()), "post-restart devices marked ADOPTED, not recreated")
check(A1["mqtt_benchinv_v1"]["instance"]==A2["mqtt_benchinv_v1"]["instance"], "instance stable across logic restart")

# cookie: stable across logic restart, flipped + mirrored across state-daemon restart
check(bool(hc1) and lc1==hc1 and lc2==hc1, "logic tracked the host cookie, stable across logic restart")
check(bool(hc2) and hc2!=hc1, "host cookie FLIPPED across state-daemon restart")
check(lc3==hc2, "logic MIRRORED the new cookie (LastCookie=%s host=%s)"%(lc3[:8],hc2[:8]))
check(pub==hc2, "logic (would) PUBLISH the new cookie retained (=%s)"%pub[:8])
check(A3=={}, "after the flip the host is empty -> logic adopted nothing (board re-announce pending)")
check(set(A4)==set(A1) and all(v["state"]=="created" for v in A4.values()),
      "board re-announce re-created both devices on the fresh host")

sys.exit(0 if ok else 1)
PYEOF
RC=$?
echo "--- bench rc=$RC ---"
exit $RC
