#!/bin/sh
# cutover-board-to-v2.sh -- one-time per-board step when migrating a hypnos board from the
# v1 (freakent) registration protocol to the v2 transparent-projection protocol.
#
# WHY (see docs/transparent-projection.md §8.1):
#   v1 boards register with a RETAINED device/<id>/Status, so a logicd/state-daemon restart
#   rebuilds them from that retained value. v2 boards publish Status NON-retained and
#   re-announce on the state cookie instead. When you flash a board v1->v2, its OLD v1
#   retained Status -- a connected:1 registration, or once the v1 link dropped, the
#   connected:0 WILL -- LINGERS on the broker (v2 firmware never overwrites it: it's
#   non-retained). The next logicd restart re-reads that stale retained connected:0 and
#   strands the now-v2 board Connected=0 (tell: live telemetry, but Connected=0).
#
#   Do NOT "fix" this in logicd by ignoring retained Status -- v1 clients DEPEND on retained
#   registration to survive a restart, so that breaks every live v1 client. It's a one-time
#   per-board artifact: clear the board's retained Status topic after flashing it to v2.
#
# Usage (run on the GX, or point -h at the broker):
#   bin/cutover-board-to-v2.sh <client_id> [broker_host]
#   e.g.  bin/cutover-board-to-v2.sh hypnosinv
#         bin/cutover-board-to-v2.sh hypnosgen
set -eu

CID=${1:?usage: cutover-board-to-v2.sh <client_id> [broker_host]   (e.g. hypnosinv)}
HOST=${2:-127.0.0.1}
TOPIC="device/${CID}/Status"

echo "Clearing stale v1 retained registration on ${HOST}: ${TOPIC}"
mosquitto_pub -h "$HOST" -r -t "$TOPIC" -m ""

# A retained message, if any survived, is delivered within ~1s of subscribing.
LEFT=$(mosquitto_sub -h "$HOST" -t "$TOPIC" -W 2 2>/dev/null || true)
if [ -n "$LEFT" ]; then
  echo "WARNING: ${TOPIC} still has a retained message:" >&2
  echo "  $LEFT" >&2
  exit 1
fi
echo "OK: ${TOPIC} cleared -- the v2 board will re-announce (non-retained) on connect."
