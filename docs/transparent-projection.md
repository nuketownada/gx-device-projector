# Transparent Projection — dbus state daemon + board-authored state

**Status:** design draft (2026-06-30). Not yet implemented.
**Authors:** Ada, Joshua Perry.
**Scope:** restructure dbus-mqtt-devices (freakent) so it never authors device
state — it only *projects* state authored by the MQTT client. Companion firmware
changes land in the hypnos boards (and later patroclus).

---

## 1. Problem

freakent currently **authors values** for the dbus paths it hosts. On every device
(re)build (`device_service.py:130-142`) each path is created with either:

- the services.yml `default` (e.g. `vebus/Mode = 4` Off, `genset/Start = 0` Stop), or
- a persisted snapshot (`persist:`/`setting:` paths, from localsettings).

Any value freakent holds is **structurally stale**, because freakent is never the
owner of that value. The owner is:

- the **hardware**, when the truth is hardware-recoverable (a Magnum inverter's on/off,
  readable from its status LED — the board can always reconstruct it), or
- the **GX scheduler**, when the truth is GX-only (whether the generator *should* run —
  not recoverable from any wire the board can read).

`persist:` does not fix this; it swaps "always wrong (Off)" for "wrong whenever the
state changed while freakent wasn't looking" — lower probability, identical failure
mode.

### 1.1 Why a stale value is dangerous (not cosmetic)

The GX is a **reactive control system**: it acts on dbus *edges*, and the action
**outlives the transient** and lands on systems that never blipped. Concrete, already
wired in this install:

```
hypnos genset StatusCode:   8 ───▶ 0 (freakent default) ───▶ 8 (board repopulates)
patroclus (watches N/.../genset/+/StatusCode == 8):
                            running    handoff OFF              handoff ON
                                       (meter→grid, un-zero)    (meter→genset, zero grid)
GX systemcalc:              re-buckets AC consumption + re-fires the start-edge double-count
```

…on a generator that **never stopped**. The genset path blipped; the *meter accounting*
took the damage. And the GX's own ESS/DVCC/systemcalc reactions to a `vebus/State` blip
are Victron code we cannot debounce. So a blip must be **prevented at the source**, not
absorbed downstream. The requirement is **zero blip**, not small blip.

### 1.2 Goal

Restart **anything except the GX** — redeploy freakent logic, crash a board, reflash
firmware — with **zero dbus blip** and no stale values. State loss is acceptable *only*
on a GX reboot or an equivalent total loss, and even then values come back correct.

---

## 2. The invariant

> **services.yml authors the _shape_. The board authors the _values_. freakent authors
> _neither_.**

freakent becomes a pure cache/projector of board-authored state. The only `None` on the
bus is a path the **board chose to omit** (e.g. `genset/Start`, which the GX owns) —
ownership, not a guess.

Corollaries:

- No services.yml value `default`s. services.yml keeps **shape** only: which paths a
  service type has, and each path's format/min/max/writeable.
- No `persist:`-of-device-state. (localsettings is still used — for the stable
  DeviceInstance allocation only, never for a value. See §6.4.)
- The inverter/generator asymmetry stops being special-cased: it falls out of "the board
  announces what it owns." The inverter announces its full state (so it survives even a
  total reset); the genset omits `/Start`, so `/Start` resets only on a GX-class loss —
  matching the accepted policy.

---

## 3. Architecture

Split the single freakent process into two:

```
            ┌─────────────────────────── GX (venus.local) ───────────────────────────┐
            │                                                                          │
  ESP32     │   ┌──────────────┐   private dbus    ┌───────────────────────────────┐   │
  boards ───┼──▶│ logic daemon │◀── control IF ───▶│ state daemon                  │   │
  (MQTT)    │   │ (MQTT⇄dbus)  │                   │ (generic durable vedbus host) │──▶│── system dbus
            │   │  RESTARTABLE │   reads Cookie    │  STABLE / rarely restarts     │   │   (GX consumers)
            │   └──────┬───────┘                   └───────────────────────────────┘   │
            │          │ publishes Cookie (retained)                                    │
            └──────────┼───────────────────────────────────────────────────────────────┘
                       ▼
                 device/_host/cookie   ◀── boards subscribe; re-announce on change
```

- **state daemon** — owns the dbus connection and *all* hosted values, in memory. It is
  **generic**: it knows nothing about MQTT or services.yml. It just hosts whatever
  services/paths/values the logic daemon tells it to, and exposes an **incarnation
  cookie**. Because state is in-memory, its own crash is an accepted total loss (§5.3) —
  so it needs **no disk persistence**, only to be small and stable enough that it almost
  never restarts.
- **logic daemon** — the churning half: speaks MQTT (the board handshake + the Victron
  `N/W/R` bridge), parses services.yml, and drives the state daemon via the control
  interface. Restarted freely on every deploy. On startup it **reconciles, not
  recreates** (§5.1) — it adopts the services the state daemon already holds, so a logic
  redeploy causes **zero** `NameOwnerChanged` and zero value change.
- **state cookie** — the channel by which the GX-resident half tells the ESP32-resident
  half "I lost my state, re-push everything" (§7).

This split is the whole point: **dbus state lifetime is decoupled from the logic process
lifetime.** Today they're welded — restart the process for any reason and every device
+ value is destroyed. After the split, the durable thing (bus presence + values) lives
in the daemon that essentially never changes, and we iterate the volatile half behind it.

---

## 4. Component: state daemon (generic durable vedbus host)

A standalone process. Owns one private well-known bus name for its control interface, and
claims one `com.victronenergy.<type>.mqtt_<clientId>_<tag>` name per hosted service (via
`velib_python` `VeDbusService`, same as freakent does today).

It holds NO knowledge of MQTT or services.yml. All schema/config is passed in by the
logic daemon. It may synthesize its own **driver-identity** paths (`/Mgmt/ProcessName`,
`/Mgmt/ProcessVersion`, `/Mgmt/Connection`) since those describe the projector, not the
device, and are constant — never device state.

### 4.1 Control interface — `com.hypnos.DbusState1` (private name `com.hypnos.dbusstate`)

| Member | Sig | Purpose |
|---|---|---|
| `EnsureService(service_id, paths, init)` | `s a{sv} a{sv} → (i,b)` | Idempotent. If the service exists → adopt, return `(instance, false)`, **ignore `init`**. If absent → create, allocate/lookup instance via localsettings, apply `init`, return `(instance, true)`. `paths` = per-path metadata (format string, min, max, writeable, persist-setting flag). |
| `SetValue(service_id, path, value)` | `s s v → b` | Update one live value. |
| `SetValues(service_id, values)` | `s a{sv} → b` | Bulk update (telemetry batches). |
| `SetConnected(service_id, connected)` | `s b → b` | Set `/Connected`; on `false` apply the live-value invalidation (the stuck-meter fix) per the path flags. |
| `RemoveService(service_id)` | `s → b` | Tear down the dbus service (board removed its definition). |
| `ListServices()` | `→ a(si)` | `(service_id, instance)` for every hosted service — the reconcile input. |
| `GetService(service_id)` | `s a{sv}` | Current paths+values, for reconcile diffing. |
| `Cookie` (property) | `s` | The incarnation nonce (§7). Read-only. |
| `Started` (signal) | `s` | Emitted once at startup, carrying the new cookie. |

Notes:

- **No callables over IPC.** The current `gettextcallback` is a Python callable; it
  becomes a **format descriptor** in `paths` (e.g. `"{:.1f} V"`, or an enum→text map for
  status paths). The state daemon builds the gettextcallback from that. services.yml
  parsing — and thus the descriptor — stays entirely in the logic daemon.
- **GX writes** to a writeable path fire `VeDbusService`'s onchange *inside the state
  daemon*. It emits `ValueChanged(service_id, path, value)` for the logic daemon. For our
  control paths this is nearly unused (GX `/Mode`,`/Start` reach the board over `N/`, not
  through this channel); it exists for completeness + any future write-forwarding.

### 4.2 Lifecycle

- **Start:** generate a fresh in-memory cookie (random per-boot UUID), claim the control
  name, emit `Started(cookie)`. Hosts nothing until the logic daemon populates it.
- **Run:** serve control calls; hold values; serve GX reads/writes on the system bus.
- **Crash/restart:** all hosted `com.victronenergy.*` names drop → GX sees devices vanish
  → accepted total loss (§5.3). New process → new cookie.

---

## 5. Component: logic daemon (reconcile + MQTT)

Everything freakent does today *except* owning the dbus values: services.yml parsing, the
board handshake, the Victron `N/W/R` bridge — plus the new reconcile + cookie publishing.

### 5.1 Reconcile (the correctness-critical path)

On startup (logic restarted, **state daemon alive**):

1. Connect to the state daemon; `ListServices()`.
2. For each existing service, **adopt** it: reconstruct MQTT routing from the
   `service_id` (which encodes `clientId`+`tag`) and `instance` — subscribe the relevant
   `N/<portal>/<type>/<inst>/<control-path>` topics, resume the `W/` publish plumbing. **No
   `EnsureService`, no value writes** — the service and its values are already correct.
3. Re-subscribe the board handshake topics (`device/<clientId>/Status`) in case boards
   re-announce, and re-subscribe/keep the Victron republish keepalives.
4. Read `Cookie` and publish it retained (§7) — same value, so boards no-op.

**Invariant:** logic-daemon startup creates and destroys **nothing**. It only makes its
own view match the state daemon's reality. This is what delivers zero-blip on deploys.

### 5.2 Failure modes to engineer against

| Hazard | Rule |
|---|---|
| Logic recreates services on its own restart → blip | Adopt from `ListServices()`; only `EnsureService` for a service that is genuinely absent. |
| `EnsureService` re-applies `init` to a live service → stomps state / fights a command | `init` is applied **only on creation**. On an existing service `EnsureService` ignores `init` entirely. |
| New instance allocated on re-announce → GX sees a "new device" | Instance comes from localsettings keyed by `service_id`; stable across everything. |
| Board announces while state daemon transiently down | Logic retries `EnsureService` until the daemon is up. (Cookie is published only *after* the daemon is up, so cookie-driven announces already arrive late; this covers an independent board reconnect.) |

### 5.3 Restart taxonomy

| Event | state daemon | cookie | board action | dbus result |
|---|---|---|---|---|
| **Logic deploy/crash** | untouched | unchanged | none | **zero blip** (adopt) |
| **State-daemon crash** | new, empty | **changes** | re-announce | total loss → rebuilt from board init (§7); accepted |
| **GX reboot** | new, empty | changes | reconnect→announce | total loss; boards re-announce on MQTT reconnect (cookie redundant) |
| **Board restart** | untouched | unchanged | LWT → `/Connected=0`; on return, announce | board's own device flaps disconnected/connected; others untouched |

---

## 6. Registration v2 (non-retained, board-authored init)

### 6.1 Today (v1)

Board publishes **retained** `device/<clientId>/Status`:

```json
{ "clientId": "...", "connected": 1, "version": "...", "services": { "v1": "vebus" } }
```

freakent replies `device/<clientId>/DBus` `{portalId, deviceInstance, topicPath}`. The
retained-ness is what lets freakent rebuild devices after *its own* restart without the
board — the exact mechanism we are removing.

### 6.2 v2

Publish **non-retained**, carrying init values per service:

```json
{
  "clientId": "hypnosinv",
  "connected": 1,
  "version": "...",
  "proto": 2,
  "services": {
    "v1": {
      "type": "vebus",
      "init": { "Mode": 3, "ModeIsAdjustable": 1, "CustomName": "Magnum Inverter" }
    }
  }
}
```

- **Non-retained** → a device exists only because a *live* board is announcing it.
  Registration lifetime == bus/device lifetime. Safe **only because** the state daemon now
  provides the durability retention used to (so this must not ship before the daemon).
- **`init`** = the board's values at creation. The board includes what it owns and
  **omits what it doesn't** (the genset omits `/Start`). Omitted paths are created `None`.
- `proto: 2` lets the logic daemon support v1 and v2 clients **concurrently** (§8), so we
  migrate boards one at a time.

### 6.3 Ownership partition (this install)

| Service | Board includes in `init` | Board omits (→ `None`, GX-owned) |
|---|---|---|
| `vebus` (inverter) | `Mode` (actual on/off), `ModeIsAdjustable`, `CustomName`, identity | — (fully board-recoverable) |
| `genset` | `RemoteStartModeEnabled=1`, `StatusCode`, `CustomName`, identity | `Start` (GX scheduler intent) |

### 6.4 Instance stability

DeviceInstance is allocated and held in localsettings
(`/Settings/Devices/mqtt_<clientId>_<tag>/ClassAndVrmInstance`), keyed by `service_id`,
owned by the **state daemon**. This is the *only* use of localsettings — a stable
identity number, never a value — so it cannot reintroduce value staleness. Stable across
logic restarts, state-daemon restarts, and re-announces.

---

## 7. State cookie (resync signal)

The one new mechanism non-retention forces: after a state-daemon loss, a board that stayed
connected (its MQTT never dropped) has no other way to learn it must re-push.

- **state daemon** holds an in-memory **incarnation nonce** — random per-boot UUID, stable
  for the process's life, so it changes **iff** the daemon lost its state. Exposed only as
  the `Cookie` dbus property. (Nonce, not counter: boards only test "different from last.")
- **logic daemon** watches `NameOwnerChanged` on the state daemon's control name → on a
  (re)appearance re-reads `Cookie` → publishes it **retained** to `device/_host/cookie`.
  (`_host` is a reserved id that cannot collide with a `clientId`.)
- **boards** subscribe `device/_host/cookie`. On connect they store the retained value as
  baseline and do their normal announce. On a **change** they invoke their existing
  on-connect announce/repopulate path, after a small random **jitter** (a few hundred ms)
  to avoid a synchronized re-announce burst flooding `full_publish_completed`.

Why retained wins over the alternatives:

- vs **soft-state periodic announce** — retained gives the same self-healing (an offline
  board picks up the current cookie the instant it reconnects and compares) **without**
  steady-state traffic.
- vs **one-shot nudge** — retained is **not missable**; it persists in the broker for
  any board that shows up later.

Cookie does not depend on broker disk-persistence of retained messages: on a GX reboot the
broker may lose it, but boards reconnect and announce anyway, and the logic daemon
republishes it on startup.

---

## 8. Migration & sequencing

The deployed single-process freakent stays as rollback until cutover. Build the new path
in parallel; it doesn't disturb the running driver until we switch the service over.

1. **State daemon, standalone.** Generic vedbus host + control interface + cookie. Bench
   test in isolation (§9) — no freakent, no MQTT.
2. **Logic daemon refactor.** Point freakent's `device_manager`/`device_service` at the
   control interface instead of owning `VeDbusService`. Add reconcile-on-startup (§5.1) and
   cookie mirroring. Keep v1 (retained) handling intact.
3. **Registration v2 in the logic daemon.** Parse `proto:2` + `init`; treat v2 `Status` as
   non-retained. **Dual support:** v1 clients keep retained behaviour (rebuilt from their
   retained `Status` on a state-daemon restart, `init`-less → `None`/telemetry-repopulated);
   v2 clients use the cookie. Lets hypnos migrate before patroclus.
4. **Firmware (hypnos).** Board: include `init` in the registration; publish it
   **non-retained**; subscribe `device/_host/cookie`; re-announce-on-change with jitter.
   **Delete** the genbus/invbus debounce (the `synced`/`mode_seeded`/`mode_ready` ladder,
   pre-sync `/Start` STOP ignore, dbus-republish re-arm). Keep only the create-handshake
   repopulate (write current values when told the device exists), which is now the
   announce path.
5. **Cutover** on the bench / off-hours on venus.local: stop the old driver, start state +
   logic daemons. Verify all devices, then a logic redeploy → zero blip.
6. **Cleanup.** Remove services.yml value `default`s and the `persist:true` on `vebus/Mode`.
7. **patroclus** (later): port the same v2 client behaviour (C++/PlatformIO). Until then it
   runs as a v1 client. Independently, consider debouncing its own `StatusCode==8` handoff
   (a few seconds of stable not-running before handoff-off) as defense-in-depth — it can't
   protect the GX's internal reactions, but it hardens the chain we own.

### 8.1 Per-board v1→v2 cutover — clear the stale retained `Status`

A v1 board registers with a **retained** `device/<id>/Status` (so a logicd restart rebuilds
it from the retained value). A v2 board publishes `Status` **non-retained** and re-announces
on the cookie instead. So when you flash a board v1→v2, its old v1 retained `Status` —
either the `connected:1` registration or, once the v1 connection has dropped, the
`connected:0` **will** — **lingers on the broker**. The v2 firmware never overwrites it (it
publishes non-retained). On the next logicd restart, `_handle` re-reads that stale retained
`connected:0` and marks the now-v2 board **Connected=0**, stranding it offline until it
happens to re-announce (tell: a live board with telemetry flowing but Connected=0).

**Do not mitigate this in logicd** by ignoring retained `Status`. v1 clients *depend* on
retained registration — that's exactly how they survive a logicd/state-daemon restart — so
ignoring retained `Status` breaks every live v1 client (patroclus, the tanks). The stale
will is a one-time per-board artifact; clear it as part of the flash:

```sh
hypnos-ota <ip> <board>              # flash the board to v2 (run from the hypnos repo)
bin/cutover-board-to-v2.sh <client_id>   # then clear its stale v1 retained Status
```

`cutover-board-to-v2.sh` just does `mosquitto_pub -r -t device/<id>/Status -m ""` (an empty
retained payload clears the topic) and verifies it's gone. The v2 board then re-announces
(non-retained) on connect and registers cleanly, and no future logicd restart can strand it.

---

## 9. Bench validation (before venus.local)

State daemon, in isolation:

- Host a `vebus` + a `genset` service via a stub driver.
- **Restart the stub ("logic") repeatedly** → assert **zero** `NameOwnerChanged` on the
  hosted names and **zero** value change (watch with `dbus-monitor` / a `GetValue` poll),
  and a **stable** `Cookie`.
- **Restart the state daemon** → assert `Cookie` **flips** and the hosted names drop +
  reappear only after re-announce.

End-to-end (state + logic + a board):

- Board registers v2 (non-retained) → device appears with board `init` values; `/Start`
  shows `---` (genset), `/Mode` shows actual (vebus).
- **Logic redeploy** → device + values unchanged, no GX reaction; patroclus sees no
  `StatusCode` edge.
- **State-daemon restart** → cookie flips → board re-announces (jittered) → device rebuilt
  with current board values.
- **GX `/Mode` write** → reaches the board over `N/`, toggles the inverter — unchanged from
  today.

---

## 10. Open / optional

- **GX-side UI rename of CustomName.** Treated here as board-authored (matches current
  invbus, which re-asserts its name each connect). If a persistent GX-side rename is ever
  wanted, that single path can be an opt-in localsettings `setting:` — a deliberate,
  per-path exception to the invariant, not the default.
- **Upstreamability.** The generic state-daemon split is a large fork and unlikely to land
  upstream as-is; registration v2 (`proto`/`init`) and non-retained handling are smaller and
  more plausibly upstreamable. The cookie is a clean, self-contained protocol addition.
- **Re-announce jitter window** and the cookie topic name are tunable; values above are
  starting points.
