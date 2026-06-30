#!/usr/bin/env bash
# Milestone-1 bench (docs/transparent-projection.md section 9):
#   - a LOGIC restart (re-run the stub) causes ZERO NameOwnerChanged + a STABLE cookie
#     + unchanged values  -> zero blip on deploys
#   - a STATE-DAEMON restart FLIPS the cookie  -> the resync signal
# Runs against a private session bus so it can't touch the real bus.
#
# Run under the dev shell:  nix develop --command tests/bench_state_daemon.sh
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

echo "--- logic creates services ---"
$PY statehost/stub_logic.py ensure
COOKIE1="$($PY statehost/stub_logic.py cookie)"
VINV1="$($PY statehost/stub_logic.py get mqtt_benchinv_v1)"
VGEN1="$($PY statehost/stub_logic.py get mqtt_benchgen_v1)"
sleep 0.5

echo "--- LOGIC RESTART x2 (idempotent adopt) ---"
$PY statehost/stub_logic.py ensure
$PY statehost/stub_logic.py ensure
COOKIE2="$($PY statehost/stub_logic.py cookie)"
VINV2="$($PY statehost/stub_logic.py get mqtt_benchinv_v1)"
sleep 0.5
cp "$NOC" "${NOC}.afterlogic"   # snapshot before the daemon kill adds loss events

echo "--- STATE-DAEMON RESTART (cookie must flip) ---"
kill -TERM "$DAEMON"; wait "$DAEMON" 2>/dev/null
sleep 0.5
$PY statehost/state_daemon.py & DAEMON=$!
wait_name com.hypnos.dbusstate || { echo "daemon never came back"; exit 1; }
COOKIE3="$($PY statehost/stub_logic.py cookie)"

echo "--- assertions ---"
$PY - "${NOC}.afterlogic" "$COOKIE1" "$COOKIE2" "$COOKIE3" "$VINV1" "$VINV2" "$VGEN1" <<'PYEOF'
import sys, json
noc, c1, c2, c3, vinv1, vinv2, vgen1 = sys.argv[1:8]

def load(p):
    rows=[]
    for line in open(p):
        line=line.strip()
        if line: rows.append(json.loads(line))
    return rows

events = load(noc)
def acq(prefix): return [e for e in events if e["name"].startswith(prefix) and e["new"] and not e["old"]]
def lost(prefix): return [e for e in events if e["name"].startswith(prefix) and e["old"] and not e["new"]]

ok=True
def check(cond,msg):
    global ok; ok = ok and cond
    print(("PASS" if cond else "FAIL")+": "+msg)

check(len(acq("com.victronenergy.vebus."))==1, "vebus acquired exactly once across create+logic-restart (got %d)"%len(acq("com.victronenergy.vebus.")))
check(len(lost("com.victronenergy.vebus."))==0, "vebus never lost across logic restart (got %d)"%len(lost("com.victronenergy.vebus.")))
check(len(acq("com.victronenergy.genset."))==1, "genset acquired exactly once (got %d)"%len(acq("com.victronenergy.genset.")))
check(len(lost("com.victronenergy.genset."))==0, "genset never lost across logic restart (got %d)"%len(lost("com.victronenergy.genset.")))

check(bool(c1) and c1==c2, "cookie STABLE across logic restart (%s == %s)"%(c1[:8],c2[:8]))
check(bool(c3) and c3!=c1, "cookie FLIPPED across daemon restart (%s -> %s)"%(c1[:8],c3[:8]))

j1=json.loads(vinv1); j2=json.loads(vinv2)
check(j1==j2, "vebus values unchanged across logic restart")
check(j1 and j1["values"].get("/Mode")==3, "vebus /Mode = 3 from board init (got %r)"%(j1 and j1["values"].get("/Mode")))

g=json.loads(vgen1)
check(g["values"].get("/StatusCode")==8, "genset /StatusCode = 8 from board init (got %r)"%g["values"].get("/StatusCode"))
check(g["values"].get("/Start", "MISSING") is None, "genset /Start is INVALID/None (board omitted it) (got %r)"%g["values"].get("/Start","MISSING"))

print("\nvebus values:", json.dumps(j1["values"]))
print("genset values:", json.dumps(g["values"]))
sys.exit(0 if ok else 1)
PYEOF
RC=$?
echo "--- bench rc=$RC ---"
exit $RC
