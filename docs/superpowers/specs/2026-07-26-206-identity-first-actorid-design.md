# #206 — Identity-first `ActorId`: an unforgeable, process-local handle

**Card:** [#206](https://github.com/devrandom-labs/bombay/issues/206) — core(identity),
M1, finishes epic #122's core rebuild. Split from #121 (KERI substance, M4).
**Status:** design — awaiting review.
**Date:** 2026-07-26.

## The problem

`ActorId` names an actor so other things can refer to it: mailbox routing,
death-watch, registry lookup, and — eventually — dataspace addressing.

kameo's `ActorId` is a process-first, identity-*incidental* opaque counter
("the Nth actor spawned in this process"); global identity is bolted on later
behind a `remote` feature flag. bombay inverts this: identity is **first** — a
dataspace actor should be able to *prove who it is* (KERI: self-certifying,
no registry).

Today's `ActorId` (`bombay-core/src/mailbox.rs`) is the kameo shape carried over
verbatim:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorId(u64);
impl ActorId { pub const fn new(raw: u64) -> Self { Self(raw) } }
```

This handle is **process-local**: `u64` values are unique only *within one
process*. Node A's 5th actor and node B's 5th actor are both `ActorId(5)`. The
concrete danger — not a 2⁶⁴ fantasy, a real bug the moment the Zenoh remote
layer (#2/#3) lands:

- **Forgeable.** `pub const fn new(raw: u64)` lets *any* `u64` become an
  `ActorId`. When the remote layer receives a peer's number off the wire and
  does `ActorId::new(wire_u64)`, it fabricates a "local" handle that is
  actually **foreign** → cross-node collision / impersonation.
- **Silently globalizable.** Nothing structural stops a future author adding
  `#[derive(Serialize)]` "for convenience" so the process-local number crosses
  nodes and means the wrong thing on the other side.

**#206 solves exactly this: make the local handle honestly process-local — so
it cannot be forged or leak as a global identity — and leave a clean seam where
#121 pairs it with a global KERI identity for dataspace addressing.**

This is *not* a counter-correctness card. See "Scope" for what is deliberately
split out.

## The two identifiers and their relationship

An actor has, over its life, up to two identifiers with different lifetimes and
jobs:

| | Local handle (`ActorId`, this card) | KERI AID (#121, M4) |
|---|---|---|
| Exists from | spawn (always) | "when it joins the dataspace" (optional; a purely-local actor never earns one) |
| Job | in-process routing: mailbox, watch, registry | dataspace *addressing*: key-expr contact tree, self-certifying authenticity |
| Shape | dense process-local `u64` | ~256-bit self-certifying crypto value (CESR/Base64URL) |
| Scope | one process | global |

The load-bearing design question: **when an actor earns an AID, does the core
start routing by the AID, or does the local handle stay the routing key and the
AID become its network address?**

Reasoned out:

1. In-process delivery is a `flume` mailbox — a channel, unrelated to crypto.
   It has no reason to switch to a 256-bit AID; that would be slower and
   pointless.
2. A purely-local actor never earns an AID, so the core **must** be able to
   route without one. The local handle is unavoidable and permanent.
3. An id that *changes* mid-life (handle → AID) breaks every watcher, registry
   entry, and queued message holding the old id. Identifiers must be stable, so
   "promote the handle into an AID" is off the table.

**Conclusion:** the AID does not replace the handle and the core does not
re-route by it. The handle stays the in-process routing key; the AID is a
**second coordinate** — the dataspace address — that maps *to* the handle at the
Zenoh remote boundary. #121's own scope agrees: the AID lives in a key-expr
"contact tree," truth lives in the KEL — that is *addressing*, not mailbox
routing.

So there are two types across the two cards, non-overlapping: local `ActorId`
(this card) and global `Aid` (#121). "Identity-first" manifests at the remote
boundary, where the *only* global address is the AID — never the bare handle.

## Design

Four moves. Note that the two leak vectors are closed **structurally — by the
compiler**, which is stronger than any runtime test.

### 1. Unforgeable: mint-only construction

The only legitimate way to obtain an `ActorId` is the crate minting one at
spawn.

- Drop the public raw constructor. The raw wrap becomes `pub(crate)` (the mint,
  `next_actor_id`, is in-crate and keeps working unchanged).
- Tests that fabricate ids (`ActorId::new(0)`, ~40 call sites across
  `mailbox.rs`, `restart.rs`, `registry.rs`, `spawn.rs`, `test_support.rs`) get
  a constructor gated `#[cfg(any(test, feature = "test-support"))]` — the same
  seam `test_support` already uses so external integration tests (which link the
  lib and cannot reach `pub(crate)`) can still build ids.

After this, no wire value, no library user, and no future remote-layer author
can fabricate a handle. The forgery path is closed by visibility, not by
convention.

**Naming:** the test constructor is named to shout its restriction, e.g.
`ActorId::from_raw_for_test(u64)` (not `new`) — `new` implies a blessed public
constructor, which is exactly what we are removing.

### 2. Non-serializable, by design and documented

`ActorId` keeps **no** `serde` derives. The invariant is written on the type:

> `ActorId` is a **process-local routing key**. It is deliberately not
> serializable: the dataspace address of an actor is its KERI AID (#121), and
> the local handle never crosses the wire. Do not add `Serialize`/`Deserialize`.

This turns the "just derive Serialize" regression into a deliberate,
review-visible act rather than a convenient accident. (We do not add a
`compile_fail` guard: per #170 those doctests never run in the gate, so they
would be false assurance. The guard is the absence of the derive plus the
documented invariant.)

### 3. Process-local meaning made explicit

Give `ActorId` its own module — `bombay-core/src/id.rs` — re-exported at the
crate root (`pub use id::ActorId`). Today it lives in `mailbox.rs` only because
the death-watch types needed *a* concrete shape to carry; identity is core, not
a mailbox detail, and an identity-first crate should say so in its module
layout. `mailbox.rs` imports it like any other module. This also gives #121 a
natural home to add the KERI `Aid` alongside (or in `actor/id.rs`, its call).

The module doc states the process-local scope and the handle/AID relationship
above, so the invariant is discoverable at the definition site.

### 4. The #121 pairing seam

#206 builds no AID and no map. Its seam *is* the guarantee that the core never
exposes the bare handle as a global address:

- `ActorId` is unforgeable and non-serializable (moves 1–2), so #121/#2/#3
  physically cannot address an actor by its local handle across the wire — they
  must introduce the AID.
- The documented relationship (two coordinates, handle routes / AID addresses,
  handle stable for life) fixes the contract #121 builds against: pair, never
  replace.

That is the whole seam. It is a *negative* guarantee enforced by the type
system, which is the right size for an M1 foundation card.

## Scope

**In #206 (identity-first shape):**

- [ ] `ActorId` is unforgeable outside the crate — no public raw constructor;
      mint is `pub(crate)`; test-only constructor behind `test-support`.
- [ ] `ActorId` is not serializable, with the process-local invariant
      documented on the type.
- [ ] `ActorId` lives in its own `id` module, re-exported at the crate root,
      carrying the handle/AID relationship doc.
- [ ] The #121 pairing seam is the above structural guarantees + the documented
      contract; no AID, no map built here.

**Split OUT to a named counter-hygiene follow-up card (filed before #206's PR;
landed explicitly, never silently dropped — cf. the #149 regression in
CLAUDE.md):**

- Overflow: refuse-at-ceiling (`fetch_add` returns `u64::MAX` → `Err`), the
  fallible-`Prepared::new` API decision it forces, and the documented wrap
  policy.
- loom model over the concurrent mint (bombay owns `NEXT_ACTOR_ID`; loom *can*
  see it — unlike ADR-0005's ref-model case, where flume ships no
  instrumentation).
- `cargo-mutants` zero-survivors + boundary tests at `0/1/MAX-1/MAX`.

Rationale for the split: the counter's platform-width + no-unwrap concern (#88)
is **already satisfied** — the code is `AtomicU64` with no `try_into().unwrap()`.
What remains (overflow) is 2⁶⁴-unreachable and its only honest fix makes
`Prepared::new` fallible, taxing every spawn with an error that can never fire —
disproportionate, and orthogonal to the leak this card is about. #206 does not
touch the `fetch_add` logic at all; it only changes `ActorId`'s construction
visibility and module home.

## Testing

The two leak vectors are closed **structurally**, so the primary evidence is
that the crate compiles with the tightened visibility and the untouched derive
set — not a runtime assertion (a test that "passes" over a compiler-enforced
invariant is theatre).

- **Unforgeability:** enforced by `pub(crate)` + feature-gated test
  constructor. Evidence: the lib builds *without* `test-support` and the public
  surface no longer exposes a raw `ActorId` constructor (verified by the doc
  build / public-API review). No positive runtime test can meaningfully assert
  "you cannot call a private function."
- **Non-serializability:** enforced by absence of the derive; documented
  invariant. No gate-visible `compile_fail` (per #170).
- **Mint still produces distinct handles:** one focused unit test — concurrent
  mints via `tokio::spawn` + `Barrier` yield pairwise-distinct ids. (Deeper
  concurrency/boundary proof is the follow-up card's loom/mutation work.)
- The existing suite (mailbox/watch/restart/registry/spawn) must stay green
  through the constructor rename — it exercises `ActorId` end to end and is the
  real regression net for the move.

## API changes & README

Public-API change (README "public API at a glance" case):

- **Removed:** `pub const fn ActorId::new(raw: u64)`.
- **Added (test-only):** `ActorId::from_raw_for_test(u64)` behind
  `#[cfg(any(test, feature = "test-support"))]` — not part of the public API.
- **Moved:** `ActorId` now at the crate root (`bombay_core::ActorId`) via
  `pub use id::ActorId`; `mailbox::ActorId` path removed (or kept as a
  re-export if churn warrants — decided in the plan).

README: update the `ActorId` bullet to state it is a process-local, unforgeable
routing key, distinct from the future dataspace AID (#121). No coverage number
(that lives in `docs/testing/coverage-baseline.md`).

## Follow-up card to file (counter hygiene)

Title (draft): *core(identity): id-counter hygiene — overflow-refuse, loom mint
model, mutation zero-survivors*. Milestone M1. Carries the three split-out
bullets verbatim, plus the `Prepared::new` fallibility decision. Referenced from
#206's PR body so the deferral is landed, not silent.
