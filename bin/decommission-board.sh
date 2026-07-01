#!/bin/sh
# decommission-board.sh -- permanently remove a board: clear BOTH of its retained topics so
# nothing lingers on the broker after the hardware is gone.
#
# WHY (see docs/transparent-projection.md §5.2):
#   A v2 board's liveness will is a RETAINED device/<id>/online=0, and a v1 board may still
#   have a RETAINED device/<id>/Status. If you simply power the board off for good, that
#   retained 0 (or stale Status) sits on the broker forever. It's harmless to a running logicd
#   (unknown client_id -> no services -> the online consumer / registration handler no-op), but
#   it's litter, and a future board that reuses the client_id would inherit a stale retained 0
#   until it published its own online=1. Clear both on decommission.
#
#   This is the DELIBERATE removal path. Do NOT clear these topics for a board that is merely
#   offline/rebooting -- a live board re-asserts online=1 on connect, and clearing Status would
#   break a v1 client's restart durability.
#
# DEVICE-DESTROYING: against a LIVE logicd this is not just litter cleanup. logicd subscribes
# device/+/Status, and an empty retained Status payload is its "definition removed" signal ->
# it runs _remove -> the dbus services DISAPPEAR from the GX immediately (RemoveService, not a
# disconnect). That's the intent here -- decommission -- but run it only when you mean to erase
# the device, not to quiet a board that's coming back.
#
# Usage (run on the GX, or point -h at the broker):
#   bin/decommission-board.sh <client_id> [broker_host]
#   e.g.  bin/decommission-board.sh hypnosgen
set -eu

CID=${1:?usage: decommission-board.sh <client_id> [broker_host]   (e.g. hypnosgen)}
HOST=${2:-127.0.0.1}

for TOPIC in "device/${CID}/online" "device/${CID}/Status"; do
  echo "Clearing retained topic on ${HOST}: ${TOPIC}"
  mosquitto_pub -h "$HOST" -r -t "$TOPIC" -m ""
done

# Verify both are gone (a surviving retained message is delivered within ~1s of subscribing).
FAIL=0
for TOPIC in "device/${CID}/online" "device/${CID}/Status"; do
  LEFT=$(mosquitto_sub -h "$HOST" -t "$TOPIC" -W 2 2>/dev/null || true)
  if [ -n "$LEFT" ]; then
    echo "WARNING: ${TOPIC} still has a retained message: $LEFT" >&2
    FAIL=1
  fi
done
[ "$FAIL" = 0 ] || exit 1
echo "OK: device/${CID}/{online,Status} cleared."
