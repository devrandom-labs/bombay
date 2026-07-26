# #206 — Identity-first `ActorId`: an unforgeable, process-local handle

**Card:** [#206](https://github.com/devrandom-labs/bombay/issues/206) — core(identity),
M1, finishes epic #122's core rebuild. Split from #121 (KERI substance, M4).
**Status:** design — literature-verified, awaiting review.
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
process incarnation*. Node A's 5th actor and node B's 5th actor are both
`ActorId(5)`; a restarted process re-mints 1, 2, 3… The concrete danger — not a
2⁶⁴ fantasy, a real bug class the moment the Zenoh remote layer (#2/#3) lands:

- **Forgeable.** `pub const fn new(raw: u64)` lets *any* `u64` become an
  `ActorId`. A remote layer that does `ActorId::new(wire_u64)` fabricates a
  "local" handle that is actually **foreign** → cross-node collision /
  impersonation.
- **Silently globalizable.** Nothing structural stops the process-local number
  leaking into a serialized envelope, a persisted nexus event, or a replicated
  structure, where it aliases across nodes and process incarnations.

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
| Shape | dense process-local `u64` | self-certifying crypto identifier (`keri-events::Identifier`: key prefix or SAID, qb64 wire form) |
| Scope | one process incarnation | global |

The load-bearing design question: **when an actor earns an AID, does the core
start routing by the AID, or does the local handle stay the routing key and the
AID become its network address?**

Reasoned out:

1. In-process delivery is a `flume` mailbox — a channel, unrelated to crypto.
   It has no reason to switch to a wide crypto AID; that would be slower and
   pointless.
2. A purely-local actor never earns an AID, so the core **must** be able to
   route without one. The local handle is unavoidable and permanent.
3. An id that *changes* mid-life (handle → AID) breaks every watcher, registry
   entry, and queued message holding the old id. Identifiers must be stable, so
   "promote the handle into an AID" is off the table.

**Conclusion:** the AID does not replace the handle and the core does not
re-route by it. The handle stays the in-process routing key; the AID is a
**second coordinate** — the dataspace address — that maps *to* the handle at the
Zenoh remote boundary. Two types across two cards, non-overlapping:
local `ActorId` (this card) and global `Aid` (#121). "Identity-first" manifests
at the remote boundary, where the *only* global address is the AID — never the
bare handle.

## Verification against the literature and the sibling crates

Reviewed 2026-07-26 against primary sources (three research sweeps: actor-system
practice, academic papers, the sibling `cesr`/`nexus` code). Full agent reports
in the session; the durable conclusions:

**The split is the field's convergent design, independently re-derived:**

- **Baker & Hewitt's locality laws** (*Laws for Communicating Parallel
  Processes*, MIT WP-134A, 1977) — an actor address is acquired **only** by
  creation or by communication, never minted or guessed. Unforgeable
  construction is the original actor-theory law; §"Design" implements it in the
  type system. Agha (MIT AI-TR-844, 1986) makes the mail address the sole
  permanent component of actor identity — behavior changes, the address never
  does (our stability argument, from the source).
- **SALSA** (Varela & Agha, OOPSLA'01) ran the same two-coordinate split as
  UAN (stable universal name) vs UAL (current locator) — and **SALSA Lite**
  (Desell & Varela, 2014) is the empirical clincher: a decade of experience
  showed universal naming taxes purely-local actors so heavily they rebuilt the
  runtime so global names are **opt-in**. bombay's "AID is earned, possibly
  never" is that finding, adopted up front.
- **UIA** (Ford et al., OSDI'06) pairs a stable self-certifying id with
  transient local names and a separate resolution layer — the exact
  AID/handle/Zenoh stack.
- **E's live vs sturdy refs** (Miller/Tribble/Shapiro, TGC'05), **CapTP/OCapN**
  (wire references are session-scoped table indices or cryptographic
  designators, never raw local tokens), and **Pony's deny capabilities**
  (Clebsch et al., AGERE'15 — unforgeable, non-serializable references enforced
  purely by the type system at zero runtime cost) are the capability lineage
  this design lands in.
- **Erlang is the validating counterexample**: BEAM lets pids cross the wire
  and pays with the `creation` incarnation field (widened 2→32 bits in OTP 23
  because 2 bits was a real bug class), `list_to_pid` restrictions — and still
  got the Lasp/Partisan corruption (pids embedded in replicated CRDTs, rewritten
  in flight, unbounded growth). Erlang patches what bombay dissolves: the
  colliding values across process incarnations are unobservable if the handle
  never leaves the process — **the invariant, not the counter, does the work**,
  which is why §"Design" enforces it mechanically.
- **KERI's own doctrine agrees** (Smith, arXiv:1907.02143; OOBI draft): AIDs
  are address-independent; pairing an AID with a transport locator happens
  *outside* the identifier (OOBI is exactly an AID→locator map). Nothing in
  KERI wants the AID as a routing key.
- **Sibling crates agree structurally**: the user's `cesr` workspace
  (`cesr-rs`, `keri-events`, `keri-codec`, `keri-rs`) ships the real AID type
  (`Identifier<'a>` = `Basic(Prefixer)` | `SelfAddressing(Saider)`) with **no
  serde anywhere — deliberately** (wire form is qb64/CESR framing), Clone-not-
  Copy, `no_std`. Even the *global* identity is never serde-serialized; the
  local handle certainly must not be. qb64 (URL-safe base64) contains no
  Zenoh-reserved chars, so an AID is a valid key-expr chunk by construction.
- **Waldo et al.** (*A Note on Distributed Computing*, 1994): keeping local and
  remote reference types distinct is the position that won.

**Problems the literature surfaced** are folded into this design (compile-time
enforcement, the ocap no-authority rule, the incarnation constraint below) or
recorded in the #121 seam contract; none contradict the split itself.

## Design

Four moves. The two leak vectors are closed **structurally — by the compiler** —
which is stronger than any runtime test or CI grep.

### 1. Unforgeable: mint-only construction

The only legitimate way to obtain an `ActorId` is the crate minting one at
spawn.

- Drop the public raw constructor. The raw wrap becomes `pub(crate)` (the mint,
  `next_actor_id`, is in-crate and keeps working unchanged).
- Tests that fabricate ids (~40 call sites across `mailbox.rs`, `restart.rs`,
  `registry.rs`, `spawn.rs`, `test_support.rs`) get a constructor gated
  `#[cfg(any(test, feature = "test-support"))]` — the same seam `test_support`
  already uses so external integration tests (which link the lib and cannot
  reach `pub(crate)`) can still build ids. Named to shout its restriction:
  `ActorId::from_raw_for_test(u64)`, not `new`.

Outside the crate there is then **no expression in safe Rust** that produces an
`ActorId` except receiving one from the crate — the locality laws as a compiler
guarantee. (Caveat, stated honestly: `unsafe` transmute can forge anything;
unforgeability is a *safe-Rust* guarantee, the same footing Pony's proof stands
on. The clippy restriction suite polices `unsafe`.)

### 2. Non-serializable: three compiler layers

No CI grep, no convention — the type system carries it transitively:

1. **Orphan rule blocks downstream.** No external crate can
   `impl Serialize for ActorId`: neither the trait nor the type is theirs.
   Compiler-enforced, permanently, for every dependent crate.
2. **Trait-bound transitivity blocks the embedding hazard.** The real leak
   vector (the Lasp lesson) is an `ActorId` field inside a struct that derives
   `Serialize` — a nexus event, an envelope, a log record. In Rust that fails
   to compile: the derive requires every field `Serialize`, so the missing impl
   *poisons the container*, in bombay and in every downstream crate.
   Erlang could not stop pids leaking into DETS because `term_to_binary`
   serializes anything; serde is opt-in per type, and the opt-in cannot be
   written.
3. **In-crate regression pin.** The only party who could ever add the impl is
   bombay-core itself, by accident. Pin it with a compile-time negative
   assertion that runs in the gate:

   ```rust
   // serde as a dev-dependency only
   static_assertions::assert_not_impl_any!(ActorId: serde::Serialize, serde::Deserialize);
   ```

   Adding the derive stops the crate compiling. Zero runtime cost, stable Rust.
   (Unlike a `compile_fail` doctest, this is a *positive* compile in the test
   build — it actually runs in the gate; cf. the #170 lesson.)

The invariant is documented on the type:

> `ActorId` is a **process-local routing key**. It is deliberately not
> serializable: the dataspace address of an actor is its KERI AID (#121), and
> the local handle never crosses the wire or gets persisted. Do not add
> `Serialize`/`Deserialize` — the `assert_not_impl_any!` pin will refuse the
> build.

### 3. Pure name: the raw value never escapes

Needham's rule: a pure name "carries no information; only useful for
comparison." Enforce it — keep the `u64` **unreadable**:

- private field (already true), **no getter**, no `From<ActorId> for u64` /
  `Into`, no `Display`.
- Outside the crate an `ActorId` supports exactly compare / copy / `Debug`.
  Nobody can build `u64`-keyed side tables, do arithmetic on ids, or embed the
  number in a URL; `Debug` output cannot round-trip back into an id because no
  constructor accepts it.

**Ocap corollary (designation ≠ authority), stated as a standing invariant:**
a bare `ActorId` must never convert into send-authority. Authority in bombay
*is* `ActorRef<A>`/`Recipient` (they hold the channel); `ActorId` holds
nothing. No API of the form `lookup(id) -> ActorRef` may ever exist — the
registry stays **name-keyed** (`Cow<'static, str>`, #119), and no id-keyed map
to refs is ever built, so the violating function has no data path to build the
ref from. One such API would collapse the entire unforgeability argument
(Hardy's confused deputy; Miller/Yee/Shapiro). Recorded in the module docs.

### 4. Own module + the #121 pairing seam

`ActorId` moves to `bombay-core/src/id.rs`, re-exported at the crate root
(`pub use id::ActorId`). Today it lives in `mailbox.rs` only because the
death-watch types needed *a* concrete shape; identity is core, not a mailbox
detail, and an identity-first crate should say so in its module layout. The
module doc carries the process-local invariant, the handle/AID relationship,
and the ocap rule — discoverable at the definition site. Gives #121 a natural
home for the `Aid` type.

#206 builds no AID and no map. Its seam *is* the structural guarantee that the
core never exposes the bare handle as a global address — a *negative* guarantee
enforced by the type system, the right size for an M1 foundation card.

## Incarnation constraint (ADR-bound)

Recorded now, before #121 freezes the identity shape (Akka's restart-vs-recreate
distinction, E's live-ref discontinuity):

- Supervision restart (#196/#199) re-spawns and **mints a new `ActorId`** —
  every supervised restart is a **new incarnation**. Watchers' held ids go
  permanently stale by design (they receive the death notice; the id never
  resurrects). This is the Erlang-style choice, made explicit.
- The AID (once earned, #121) is what survives restart; the `ActorId` never
  does. A future "restart transparent to watchers" feature would require the
  opposite (stable id across restart), which the never-reuse invariant
  **permanently forbids** — any such feature must be built at the AID layer.

This becomes an ADR in the #206 PR.

## Seam contract for #121 (recorded here, built there)

The literature/sibling review surfaced these as **#121/#2/#3/#67 obligations**;
they are contract items the M1 seam hands over, not #206 work:

1. **Incarnation ambiguity at the AID layer.** A stable AID over per-incarnation
   handles reintroduces, at the AID layer, exactly the ambiguity Akka's wire
   `#uid` and Erlang's `creation` field solve. KERI hands the fix over for
   free: the KEL sequence number/digest is a cryptographic incarnation counter —
   the wire coordinate can be (AID, KEL-anchored incarnation datum). #121 must
   decide **per remote verb**: virtual/Orleans semantics (AID always answers,
   incarnation invisible) or incarnation-precise/Akka semantics.
2. **An AID grants zero authority** (confused deputy at the Zenoh boundary —
   `put` on a key-expr is ambient authority). Remote send requires a
   capability-bearing witness obtained by KERI verification, encoded in types:
   `fn tell_remote(peer: &VerifiedPeer, …)`, never `fn tell_remote(aid: &Aid, …)`.
   The only constructor of `VerifiedPeer` is the KEL-verifying session
   establishment. Compile-time confused-deputy guard.
3. **The AID→key-expr→handle binding is a mapping system** — and RFC 1498's
   warning is that bindings, not names, are the bug farm: who is authoritative
   for the binding (KERI-signed location assertion vs node vs registry),
   staleness windows on restart/migration, first-contact resolution latency
   inside ask-timeout budgets, cache invalidation. LISP/ILNP and SALSA's UANP
   all paid here. Must be designed, not assumed.
4. **Remote death-watch is suspicion, not death** (Chandra–Toueg: perfect
   failure detection is unattainable; Zenoh liveliness `Delete` under partition
   is a false positive by construction). The remote watch API must distinguish
   *suspected dead* from *confirmed stopped* — leases are the literature's
   stable answer. Hits the #67 exit gate directly.
5. **AID never rebinds** to a different logical actor, ever (the locality-law
   reuse rule lifted to the dataspace), and **an AID is never a GC root** — a
   dangling AID resolves to a clean "no such actor" failure, and the dataspace
   registry needs a deletion protocol for stopped actors' entries.
6. **Registry equivocation is outside KERI's protection.** Witnesses detect
   key-state duplicity, not dataspace-registry duplicity; a forked AID→key-expr
   view is a SUNDR-style attack needing fork-consistency or KERI-signed
   location records.
7. **Sibling-crate practicalities**: `Matter`/`Identifier` implement neither
   `Hash` nor `Ord` (cannot key maps as-is — add upstream in cesr or wrap the
   owned qb64 form); no owned AID type exists anywhere in the cesr workspace
   (everything lifetime-parameterized — #121 defines the owned canonical form);
   delegation verification is unimplemented in keri-rs
   (`Rejection::DelegationUnsupported`, deferred to K4) — #121's delegation
   bullet has vocabulary, not a verify path; KERI transferability is chosen at
   inception (ephemeral non-transferable vs rotatable — a natural ephemeral-vs-
   durable actor mapping to surface); the card's "Existing crates" list
   (`keriox`/`cesride`) is stale — target the user's own stack. AIDs travel as
   qb64 bytes, never serde.

## Scope

**In #206 (identity-first shape):**

- [ ] `ActorId` is unforgeable outside the crate — no public raw constructor;
      mint is `pub(crate)`; test-only constructor behind `test-support`.
- [ ] `ActorId` is non-serializable with the three-layer compile-time story:
      orphan rule + field-poisoning transitivity (documented), and the
      `assert_not_impl_any!` pin compiled in the gate.
- [ ] `ActorId` is a pure name — raw `u64` unreadable outside the crate: no
      getter, no `From`/`Into<u64>`, no `Display`.
- [ ] The designation-≠-authority invariant (no id→ref API ever; registry stays
      name-keyed) documented in the module docs.
- [ ] `ActorId` lives in its own `id` module, re-exported at the crate root,
      carrying the process-local invariant + handle/AID relationship doc.
- [ ] The incarnation constraint recorded as an ADR (new id per restart; AID is
      what survives; transparent-restart permanently forbidden at handle layer).
- [ ] The #121 seam contract (§ above) posted to card #121.

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
What remains (overflow) is 2⁶⁴-unreachable (~585 years at 10⁹ spawns/s) and its
only honest fix makes `Prepared::new` fallible, taxing every spawn with an error
that can never fire — disproportionate, and orthogonal to the leak this card is
about. #206 does not touch the `fetch_add` logic at all.

## Testing

The leak vectors are closed **structurally**, so the primary evidence is
compilation, not runtime assertions (a runtime test "passing" over a
compiler-enforced invariant is theatre):

- **Unforgeability:** enforced by `pub(crate)` + feature-gated test
  constructor. Evidence: the lib builds *without* `test-support` and the public
  surface exposes no raw constructor, no getter, no `From`/`Into<u64>`
  (public-API review in the PR).
- **Non-serializability:** `assert_not_impl_any!(ActorId: Serialize, Deserialize)`
  compiled in the gate (`static_assertions` + `serde` as dev-dependencies —
  workspace-root declared, latest versions).
- **Mint still produces distinct handles:** one focused unit test — concurrent
  mints via `tokio::spawn` + `Barrier` yield pairwise-distinct ids. (Deeper
  boundary/model proof is the follow-up card's loom/mutation work.)
- The existing suite (mailbox/watch/restart/registry/spawn) must stay green
  through the constructor rename — it exercises `ActorId` end to end and is the
  real regression net for the move.

## API changes & README

Public-API change (README "public API at a glance" case):

- **Removed:** `pub const fn ActorId::new(raw: u64)`.
- **Added (test-only):** `ActorId::from_raw_for_test(u64)` behind
  `#[cfg(any(test, feature = "test-support"))]` — not part of the public API.
- **Moved:** `ActorId` now at the crate root (`bombay_core::ActorId`) via
  `pub use id::ActorId`; whether `mailbox::ActorId` stays as a re-export is
  decided in the plan by call-site churn.

README: update the `ActorId` bullet — process-local, unforgeable,
non-serializable pure-name routing key, distinct from the future dataspace AID
(#121). No coverage number (that lives in `docs/testing/coverage-baseline.md`).

## Follow-up card to file (counter hygiene)

Title (draft): *core(identity): id-counter hygiene — overflow-refuse, loom mint
model, mutation zero-survivors*. Milestone M1. Carries the three split-out
bullets verbatim, plus the `Prepared::new` fallibility decision. Referenced from
#206's PR body so the deferral is landed, not silent.

## Sources (primary)

Baker & Hewitt, *Laws for Communicating Parallel Processes*, MIT WP-134A (1977) ·
Agha, *Actors*, MIT AI-TR-844 (1986) · De Koster et al., *43 Years of Actors*,
AGERE'16 · Saltzer, RFC 1498 (1982/1993) · Needham, *Names* (Mullender,
*Distributed Systems*) · Varela & Agha, SALSA, OOPSLA'01; Desell & Varela,
*SALSA Lite*, LNCS 8665 (2014) · Ford et al., UIA, OSDI'06 · Mazières et al.,
SFS, SOSP'99 · Li et al., SUNDR, OSDI'04 · Miller, *Robust Composition*, JHU
thesis (2006) · Miller/Yee/Shapiro, *Capability Myths Demolished*, SRL2003-02 ·
Hardy, *The Confused Deputy* (1988) · Clebsch et al., *Deny Capabilities*,
AGERE'15 · Miller/Tribble/Shapiro, *Concurrency Among Strangers*, TGC'05 ·
Gonzalez Boix et al., leasing, TOOLS'09 · Chandra & Toueg, JACM 43(2) (1996) ·
Waldo et al., *A Note on Distributed Computing* (1994) · Smith, KERI,
arXiv:1907.02143 · OOBI, draft-ssmith-oobi · Erlang ERTS external-term-format &
distribution docs · Meiklejohn, the disterl/Lasp pid bug (2021) · Akka
*Addressing* docs · Orleans MSR-TR-2014-41 · OCapN/CapTP spec.
