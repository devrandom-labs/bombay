# ADR-0015 — `ActorId` is a process-local pure name; restart mints a new incarnation

**Status:** Accepted (2026-07-26) — decided under card #206

## Context

`ActorId` was a kameo-shaped scaffold: `pub struct ActorId(u64)` with a public
raw constructor (`ActorId::new`), minted by a process-local counter. With the
Zenoh remote layer (#2/#3) and KERI identity (#121) ahead, the handle risked
leaking as a global identity:

- **Forgeable** — a public `new(raw: u64)` lets any `u64` become an `ActorId`,
  so a remote layer wrapping a wire value would fabricate a foreign "local"
  handle (cross-node collision / impersonation).
- **Silently globalizable** — nothing stopped the process-local number entering
  a serialized envelope or a persisted nexus event, where it aliases across
  nodes and process incarnations. This is the Erlang `creation`-field bug class:
  BEAM pids cross the wire, so BEAM needs a per-incarnation `creation` tag
  (widened 2→32 bits in OTP 23 because 2 bits was a real bug), and pids still
  leaked into replicated CRDTs (the Lasp/Partisan corruption).

Design verified against primary sources (see the #206 spec): Baker & Hewitt's
locality laws (addresses acquired only by creation or communication), SALSA Lite
(global naming must be opt-in), UIA / E / CapTP / Pony (unforgeable local ref +
separate durable designator), KERI OOBI (AID↔locator pairing lives outside the
identifier). Every mature actor/dataspace system separates a stable logical
identity from a scope-local routing handle; bombay's split is that pattern.

## Decision

1. **Process-local pure name.** `ActorId` is the in-process routing key only.
   - **Unforgeable** outside the crate: the raw wrap is `pub(crate) from_raw`;
     the only external fabrication path is `from_raw_for_test`, gated
     `#[cfg(any(test, feature = "test-support"))]`. Outside the crate an
     `ActorId` is obtainable solely from a spawned actor (the locality laws as a
     compiler guarantee). This is a *safe-Rust* guarantee — `unsafe` transmute
     can forge anything; the clippy restriction suite polices `unsafe`.
   - **Non-serializable**, pinned by
     `assert_not_impl_any!(ActorId: Serialize, DeserializeOwned)` (dev-build).
     Three structural layers: the orphan rule blocks downstream impls; serde
     being a dev-only dependency means `#[derive(Serialize)]` cannot resolve in
     the production lib; the pin catches the residual (serde promoted to a
     normal dependency, or a hand-written impl). Field poisoning then protects
     every container transitively.
   - **Pure name**: the raw `u64` is unreadable outside the crate — no getter,
     no `From`/`Into<u64>`, no `Display`. An `ActorId` supports exactly copy,
     comparison, and `Debug`.
2. **Designation ≠ authority.** A bare `ActorId` never converts into
   send-authority: no `lookup(id) -> ActorRef`-shaped API, ever; the registry
   stays name-keyed (#119). Authority is exclusively `ActorRef`/`Recipient`
   (they hold the channel). One such API would be Hardy's confused deputy and
   would collapse the unforgeability argument.
3. **Restart mints a new incarnation.** Supervision restart (#196/#199)
   re-spawns and mints a fresh `ActorId`; watchers' held ids go permanently
   stale by design (they receive the death notice; ids never resurrect — never
   reused within a process). The KERI AID (#121), once earned, is what survives
   restart.

## Consequences

- The global identity coordinate is #121's separate `Aid` type, paired with the
  handle at the remote boundary — **pair, never replace**; the core never routes
  by AID (in-process delivery is a `flume` mailbox, unrelated to crypto).
- "Restart transparent to watchers" is **permanently forbidden at the handle
  layer** — it would require a stable id across restart, which never-reuse
  forbids. Any such feature must be built at the AID layer, with the KEL
  sequence number as the incarnation datum (the KERI-native analog of Akka's
  path `#uid` / Erlang's `creation`).
- `Mailbox::bounded(capacity, id)` stays `pub` with an unchanged signature but
  is externally inert: outside `test-support`, ids exist only for spawned
  actors. Spawn (`PreparedActor`) is the front door.
- The mint's overflow/wrap policy (2⁶⁴, unreachable in practice) is deliberately
  out of scope here — it is the counter-hygiene follow-up card, kept separate so
  an unreachable-error API tax does not ride on the identity-shape change.
