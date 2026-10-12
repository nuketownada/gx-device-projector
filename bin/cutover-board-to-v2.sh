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
# ORDERING (learned the hard way on patroclus01): the clearing publish below is delivered
# LIVE to logicd as an empty device/<id>/Status -- which is exactly the v1 "definition
# removed" deregistration signal, so logicd immediately REMOVES the board's dbus services.
# A v2 board doesn't notice (nothing prompts it to re-announce), so it keeps publishing
# Proxy into the void until its next reconnect. Therefore, after running this script you
# MUST bounce the board (power-cycle, OTA re-upload, or drop its MQTT connection) so it
# reconnects and re-registers. Full per-board sequence:
#   1. flash the board to v2 firmware
#   2. wait for its v2 registration in logicd.log
#   3. run this script (clears the stale v1 retained Status; logicd removes the devices)
#   4. bounce the board -> fresh connect re-registers, devices re-created with init applied
#
# Usage (run on the GX, or point -h at the broker):
#   bin/cutover-board-to-v2.sh <client_id> [broker_host]
#   e.g.  bin/cutover-board-to-v2.sh hypnosinv
#         bin/cutover-board-to-v2.sh hypnosgen
set -eu

CID=${1:?usage: cutover-board-to-v2.sh <client_id> [broker_host]   (e.g. hypnosinv)}
HOST=${2:-127.0.0.1}
TOPIC="device/${CID}/Status"

# Credentials (optional): set MQTT_USER + MQTT_PASSWORD for an authenticated connection.
AUTH=""
[ -n "${MQTT_USER:-}" ] && AUTH="-u ${MQTT_USER} -P ${MQTT_PASSWORD:-}"

# ACL CANARY: the verify read below is SILENT on an ACL-denied subscribe (anonymous localhost
# is ACL-blind to device/# on the Victron broker), so a failed clear would look like success.
# Prove we can read device/# via device/_host/cookie (logicd republishes it retained on every
# startup) and abort loudly if empty.
CANARY=$(mosquitto_sub -h "$HOST" $AUTH -t "device/_host/cookie" -C 1 -W 3 2>/dev/null || true)
[ -n "$CANARY" ] || { echo "ERROR: cannot read device/_host/cookie on ${HOST} -- ACL-denied (set MQTT_USER/MQTT_PASSWORD) or broker down. Refusing to decide blind." >&2; exit 1; }

echo "Clearing stale v1 retained registration on ${HOST}: ${TOPIC}"
mosquitto_pub -h "$HOST" $AUTH -r -t "$TOPIC" -m ""

# A retained message, if any survived, is delivered within ~1s of subscribing.
LEFT=$(mosquitto_sub -h "$HOST" $AUTH -t "$TOPIC" -W 2 2>/dev/null || true)
if [ -n "$LEFT" ]; then
  echo "WARNING: ${TOPIC} still has a retained message:" >&2
  echo "  $LEFT" >&2
  exit 1
fi
echo "OK: ${TOPIC} cleared -- the v2 board will re-announce (non-retained) on connect."

# If logicd is live it just deregistered the board (empty Status == v1 definition removal).
# When running locally on the GX, show whether the dbus services are gone so the operator
# knows a bounce is required.
if [ "$HOST" = "127.0.0.1" ] && command -v dbus >/dev/null 2>&1; then
  LIVE=$(dbus -y 2>/dev/null | grep -c "mqtt_${CID}_" || true)
  if [ "$LIVE" -eq 0 ]; then
    echo "NOTE: no com.victronenergy.*.mqtt_${CID}_* services on dbus -- logicd deregistered"
    echo "      the board on the clear (expected). BOUNCE THE BOARD NOW so it re-registers."
  else
    echo "NOTE: ${LIVE} mqtt_${CID}_* service(s) still on dbus. If the board was online when"
    echo "      you ran this, logicd may remove them momentarily -- bounce the board anyway."
  fi
else
  echo "NOTE: if the board was online during the clear, logicd deregistered it -- bounce the"
  echo "      board so it reconnects and re-registers."
fi
