# #206 Identity-first `ActorId` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ActorId` an unforgeable, non-serializable, pure-name process-local handle in its own module, with the #121 KERI seam recorded (spec: `docs/superpowers/specs/2026-07-26-206-identity-first-actorid-design.md`).

**Architecture:** Move `ActorId` + the mint counter from `mailbox.rs`/`spawn.rs` into a new `bombay-core/src/id.rs`; drop the public constructor (`pub(crate)` mint + `test-support`-gated test constructor); pin non-serializability with `assert_not_impl_any!`; record the incarnation constraint as ADR-0015; hand the seam contract to card #121; file the counter-hygiene follow-up card.

**Tech Stack:** stable Rust (edition 2024), `static_assertions` 1.1, `serde` (dev-dep only), tokio test, `nix flake check` gate.

**Working branch:** `feat/206-identity-first-actorid` (already exists, spec committed).

**Facts the executor must know (verified 2026-07-26):**
- Vendored kameo (`src/`, package `bombay`) has its OWN `ActorId` (`bombay::actor::ActorId`); root `tests/core_steps/` targets it. **Never touch `src/` or `tests/`** — only `bombay-core/`, `fuzz/tests/`, docs.
- `bombay-core/Cargo.toml:44` has a self-dev-dep `bombay-core = { path = ".", features = ["test-support"] }` — benches, examples, and `bombay-core/tests/*` all compile with `test-support` on. `fuzz/Cargo.toml:17` also enables `test-support`. So a `#[cfg(any(test, feature = "test-support"))]` constructor is visible to every in-repo `ActorId::new` call site.
- `ActorRef::new` is already `pub(crate)` (`actor_ref.rs:82`) — no cascade.
- `Mailbox::bounded(capacity, id)` stays `pub` with unchanged signature: in-repo surfaces mint test ids; external users obtain ids only from spawned actors. Intent, not accident — documented in Task 1.
- No rustdoc examples use `ActorId::new` (verified); README does not show it in code.
- The flake gate only sees **tracked** files — `git add` new files before `nix flake check`, and commit before the gate (gate is slow; never leave it to a stranded subagent).
- Do NOT run `cargo hakari generate` (aspirational in CLAUDE.md; the command does not exist here).

---

**TDD note for this plan:** every task leads with its test. Where the invariant
*already holds* in production (e.g. distinct mint), a classic red is impossible
without breaking production code — there the **falsifiability probe is the red
step** (temporarily break the SUT, watch the test fail, revert; record the probe
in the commit message). Where the test drives *new* API (`from_raw_for_test`,
the root re-export), the red is genuine: the test fails to compile first.

---

### Task 1: `id.rs` module — unforgeable pure-name `ActorId`

**Files:**
- Test: `bombay-core/tests/invariants.rs` (new test written FIRST)
- Create: `bombay-core/src/id.rs`
- Modify: `bombay-core/src/lib.rs` (module decl + root re-export)
- Modify: `bombay-core/src/mailbox.rs:126-138` (remove struct, re-export)
- Modify: `bombay-core/src/actor/spawn.rs:17,85-93` (remove counter, import mint)
- Modify (mechanical rename): every `ActorId::new(` under `bombay-core/src`, `bombay-core/tests`, `bombay-core/benches`, `bombay-core/examples`, `fuzz/tests`

- [ ] **Step 1 (RED): Write the failing test against the NEW api**

In `bombay-core/tests/invariants.rs`, next to `i17_distinct_ids` (~line 968),
add a test that exercises the new surface — the root re-export and the
test-only constructor — before either exists:

```rust
/// I18 — the identity surface (#206): `ActorId` is re-exported at the crate
/// root, and the only fabrication path is the test-support constructor.
/// Asserts exact `Eq`/`Copy` pure-name semantics on fabricated ids.
#[test]
fn i18_actor_id_root_export_and_test_ctor() {
    let a = bombay_core::ActorId::from_raw_for_test(41);
    let b = bombay_core::ActorId::from_raw_for_test(41);
    let c = bombay_core::ActorId::from_raw_for_test(42);
    assert_eq!(a, b, "same raw value compares equal");
    assert_ne!(a, c, "different raw values compare unequal");
    let copied = a; // Copy: `a` stays usable
    assert_eq!(a, copied, "ActorId is Copy");
}
```

- [ ] **Step 2 (verify RED): Run it — must FAIL to compile**

```bash
nix develop --command cargo nextest run -p bombay-core --test invariants i18
```

Expected: compile error — `no function or associated item named
'from_raw_for_test'` (and no `bombay_core::ActorId` root path yet).

- [ ] **Step 3 (GREEN): Create `bombay-core/src/id.rs`**

```rust
//! Actor identity: the process-local handle (card #206).
//!
//! [`ActorId`] is bombay's **local** identity coordinate. The **global**
//! coordinate — a self-certifying KERI AID earned when an actor joins the
//! dataspace — is a *separate* type owned by card #121: it pairs with this
//! handle at the Zenoh remote boundary and never replaces it. The core
//! routes by this handle; the AID addresses across the dataspace.

use core::sync::atomic::{AtomicU64, Ordering};

/// A process-local, unforgeable actor handle: bombay's in-process routing key
/// (mailbox, death-watch, supervision).
///
/// # Process-local, never global
///
/// Values are unique only within one process incarnation (the counter restarts
/// with the process). The handle is deliberately **not serializable**: the
/// dataspace address of an actor is its KERI AID (#121); the local handle
/// never crosses the wire and is never persisted. Do not add
/// `Serialize`/`Deserialize` — a compile-time pin refuses the build.
///
/// # Pure name
///
/// The raw value is unreadable outside this crate — no getter, no
/// `From`/`Into<u64>`, no `Display`. An `ActorId` supports exactly copy,
/// comparison, and `Debug`.
///
/// # Designation, not authority
///
/// Holding an `ActorId` grants nothing: send-authority lives exclusively in
/// [`ActorRef`](crate::actor::ActorRef)/`Recipient` (they hold the channel).
/// No API may ever convert a bare `ActorId` into a ref — the registry stays
/// name-keyed. See ADR-0015.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorId(u64);

impl ActorId {
    /// Wraps a value minted by [`next_actor_id`]. In-crate only: outside this
    /// crate an `ActorId` is obtainable solely from a spawned actor.
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Test-only fabrication seam (unit suites, benches, fuzz, examples).
    /// Never a production path: production ids come from [`next_actor_id`].
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub const fn from_raw_for_test(raw: u64) -> Self {
        Self(raw)
    }
}

/// Monotonic process-local id source. Overflow/wrap policy is the
/// counter-hygiene follow-up card (see the #206 PR); 2^64 is unreachable in
/// practice (~585 years at 10^9 spawns/s).
static NEXT_ACTOR_ID: AtomicU64 = AtomicU64::new(1);

/// Mints the next process-local id (spawn path).
pub(crate) fn next_actor_id() -> ActorId {
    // Relaxed is sufficient: correctness needs only that each `fetch_add`
    // returns a distinct value. Uniqueness is a property of atomic increment
    // alone and requires no happens-before with any other memory (CLAUDE
    // concurrency rule).
    ActorId::from_raw(NEXT_ACTOR_ID.fetch_add(1, Ordering::Relaxed))
}
```

- [ ] **Step 4: Wire the module in `bombay-core/src/lib.rs`**

Find the module declarations (near `pub mod mailbox;`) and add (private `mod` +
controlled re-export, per the API rules):

```rust
mod id;

pub use id::ActorId;
```

- [ ] **Step 5: Replace the struct in `bombay-core/src/mailbox.rs`**

Delete lines 126-138 (the doc comment, `pub struct ActorId(u64);`, and the
`impl ActorId` block) and replace with:

```rust
/// Re-export: `ActorId` lives in [`crate::id`] (#206); this path stays for the
/// mailbox-composition surface (`Mailbox::bounded` takes it).
pub use crate::id::ActorId;
```

- [ ] **Step 6: Point the spawn path at the new mint in `bombay-core/src/actor/spawn.rs`**

Delete lines 85-93 (`NEXT_ACTOR_ID` static + `fn next_actor_id`). Remove
`AtomicU64` from the `use core::sync::atomic::...` import at line 17 (keep
`Ordering` only if still used in production code — check; test modules have
their own imports). Add to the crate imports at the top:

```rust
use crate::id::next_actor_id;
```

`Prepared::new` (line 149) and `new_linked` (line 197) call sites stay
textually identical.

- [ ] **Step 7 (verify GREEN): Run the new test — must PASS now**

```bash
nix develop --command cargo nextest run -p bombay-core --test invariants i18
```

Expected: PASS (the rest of the suite is still red until the rename below —
that's expected; `ActorId::new` no longer exists).

- [ ] **Step 8: Mechanical rename of test fabrication sites**

```bash
rg -l 'ActorId::new\(' bombay-core/src bombay-core/tests bombay-core/benches bombay-core/examples fuzz/tests | xargs sd -s 'ActorId::new(' 'ActorId::from_raw_for_test('
```

Then verify nothing else remains (docs/specs hits are fine, code hits are not):

```bash
rg -n 'ActorId::new\(' --glob '!docs/**' --glob '!src/**' --glob '!tests/**'
```

Expected: no output.

- [ ] **Step 9: Build both workspaces**

```bash
nix develop --command cargo test -p bombay-core --no-run
nix develop --command bash -c 'cd fuzz && cargo check --tests'
```

Expected: both compile. (If `fuzz/Cargo.lock` changes, stage it.)

- [ ] **Step 10: Run the full bombay-core suite**

```bash
nix develop --command cargo nextest run -p bombay-core
```

Expected: all green — the existing suite is the regression net for the move.

- [ ] **Step 11: Commit**

```bash
git add bombay-core/src/id.rs bombay-core/src/lib.rs bombay-core/src/mailbox.rs bombay-core/src/actor/spawn.rs bombay-core/tests/invariants.rs
git add -u
git commit -m "core(id): unforgeable pure-name ActorId in own module [#206]

ActorId moves to id.rs with the mint counter; public raw constructor removed
(pub(crate) from_raw + test-support-gated from_raw_for_test). Outside the
crate an ActorId is now obtainable only from a spawned actor — the locality
laws (Baker-Hewitt 1977) as a compiler guarantee."
```

---

### Task 2: Non-serializability pin (`assert_not_impl_any!`)

**Files:**
- Modify: root `Cargo.toml` (`[workspace.dependencies]`)
- Modify: `bombay-core/Cargo.toml` (`[dev-dependencies]`)
- Modify: `bombay-core/src/id.rs` (test module)

- [ ] **Step 1: Add workspace dep**

In root `Cargo.toml` `[workspace.dependencies]` (where `serde = "1.0"` already
lives, line ~55), add:

```toml
static_assertions = "1.1"
```

- [ ] **Step 2: Add bombay-core dev-deps**

In `bombay-core/Cargo.toml` `[dev-dependencies]`:

```toml
serde = { workspace = true }
static_assertions = { workspace = true }
```

- [ ] **Step 3: Add the pin to `bombay-core/src/id.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::ActorId;

    // Compile-time proof by contradiction: this generates an impl that
    // conflicts iff the forbidden impl exists. The orphan rule already blocks
    // every downstream crate; this pin blocks in-crate regression. Field
    // poisoning then protects every container: a struct embedding ActorId
    // cannot derive Serialize anywhere (the Lasp/Partisan lesson).
    static_assertions::assert_not_impl_any!(ActorId: serde::Serialize, serde::Deserialize);
}
```

- [ ] **Step 4: Verify it compiles (the pin passing IS the test)**

```bash
nix develop --command cargo test -p bombay-core --lib id
```

Expected: compiles, runs 0-or-more tests, no failures.

- [ ] **Step 5 (RED via probe): prove the pin can fail**

(The desired state — no serde impl — is the starting state, so classic
red-first is impossible; the probe is the red step.)

Temporarily add above `pub struct ActorId`:

```rust
#[derive(serde::Serialize)]
```

(and `serde` won't resolve in non-dev context — expect a compile error either
from the missing dep in the lib build or from the conflicting impl in the test
build). Run:

```bash
nix develop --command cargo test -p bombay-core --lib id 2>&1 | head -20
```

Expected: FAILS to compile. **Revert the derive.** Record the probe in the
commit message.

- [ ] **Step 6: fuzz lock check**

```bash
nix develop --command bash -c 'cd fuzz && cargo check --tests'
git status --short fuzz/Cargo.lock
```

Dev-deps of a path dependency should not enter fuzz's lock; if the lock
changed anyway, stage it (the #185 gotcha).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml bombay-core/Cargo.toml bombay-core/src/id.rs
git add -u
git commit -m "core(id): compile-time non-serializability pin [#206]

assert_not_impl_any!(ActorId: Serialize, Deserialize) in the test build.
Probe verified: a temporary #[derive(serde::Serialize)] on ActorId breaks the
build, then reverted. Orphan rule blocks downstream impls; field poisoning
protects containers transitively."
```

---

### Task 3: Concurrent-mint distinctness test

**Files:**
- Modify: `bombay-core/tests/invariants.rs` (next to `i17_distinct_ids`, ~line 968)

`i17_distinct_ids` already covers sequential mint. Add real-overlap concurrent
mint (test-quality rule: `tokio::spawn` + `Barrier`, not sequential-then-check).

- [ ] **Step 1: Write the test**

Add after `i17_distinct_ids` (reuse the file's existing imports — `Arc`,
`timeout`, `TERMINATE`, `PreparedActor`, `Bank`, `cap` are all in scope; add
`tokio::sync::Barrier` to the imports if not present):

```rust
/// I17b — concurrent mint: 32 tasks release together on a barrier, each
/// building a `PreparedActor`. ASSERT all ids pairwise distinct (`ActorId` is
/// `Eq` but not `Hash`, so distinctness is asserted pairwise).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn i17b_concurrent_mint_distinct_ids() {
    const TASKS: usize = 32;
    let barrier = Arc::new(Barrier::new(TASKS));
    let handles: Vec<_> = (0..TASKS)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                PreparedActor::<Bank>::new(cap(1)).actor_ref().id()
            })
        })
        .collect();
    let mut ids = Vec::with_capacity(TASKS);
    for handle in handles {
        ids.push(
            timeout(TERMINATE, handle)
                .await
                .expect("mint task must finish")
                .expect("mint task must not panic"),
        );
    }
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            assert_ne!(ids[i], ids[j], "concurrent ids at {i} and {j} must be distinct");
        }
    }
}
```

- [ ] **Step 2: Run it**

```bash
nix develop --command cargo nextest run -p bombay-core --test invariants i17b
```

Expected: PASS.

- [ ] **Step 3 (RED via probe):**

(The distinct-mint invariant already holds in production — the probe is the red
step.) Temporarily change `next_actor_id` in `bombay-core/src/id.rs` to:

```rust
ActorId::from_raw(7)
```

Run the same command. Expected: `i17b` (and `i17`) FAIL. **Revert.**

- [ ] **Step 4: Commit**

```bash
git add bombay-core/tests/invariants.rs
git commit -m "test(invariants): I17b concurrent mint distinctness [#206]

32 barrier-released tasks mint through PreparedActor; ids pairwise distinct.
Probe verified: constant-id stub in next_actor_id fails i17/i17b, reverted."
```

---

### Task 4: ADR-0015 — process-local pure name + incarnation constraint

**Files:**
- Create: `docs/adr/0015-actorid-process-local-pure-name.md`
- Modify: `docs/adr/README.md` (index — match its existing line format)

- [ ] **Step 1: Write the ADR**

```markdown
# ADR-0015 — `ActorId` is a process-local pure name; restart mints a new incarnation

**Status:** Accepted (2026-07-26) — decided under card #206

## Context

`ActorId` was a kameo-shaped scaffold: `pub struct ActorId(u64)` with a public
raw constructor, minted by a process-local counter. With the Zenoh remote layer
(#2/#3) and KERI identity (#121) ahead, the handle risked leaking as a global
identity: forgeable from any `u64` (a remote layer wrapping a wire value would
fabricate a foreign "local" handle), and silently globalizable (nothing stopped
it entering a serialized envelope or persisted nexus event, where it aliases
across nodes and process incarnations — the Erlang `creation`-field /
Lasp-Partisan bug class).

Design verified against primary sources (see the #206 spec): Baker & Hewitt's
locality laws (addresses acquired only by creation or communication), SALSA
Lite (global naming must be opt-in), UIA/E/CapTP/Pony (unforgeable local ref +
separate durable designator), KERI OOBI (AID→locator pairing lives outside the
identifier).

## Decision

1. **Process-local pure name.** `ActorId` is the in-process routing key only.
   Unforgeable outside the crate (`pub(crate)` mint; `test-support`-gated test
   constructor). Non-serializable, pinned by
   `assert_not_impl_any!(ActorId: Serialize, Deserialize)` — the orphan rule
   blocks downstream impls, field poisoning protects containers. The raw `u64`
   is unreadable (no getter, no `From`/`Into<u64>`, no `Display`).
2. **Designation ≠ authority.** A bare `ActorId` never converts into
   send-authority: no `lookup(id) -> ActorRef`-shaped API, ever; the registry
   stays name-keyed (#119). Authority is exclusively `ActorRef`/`Recipient`.
3. **Restart mints a new incarnation.** Supervision restart (#196/#199)
   re-spawns and mints a fresh `ActorId`; watchers' held ids go permanently
   stale by design (they receive the death notice; ids never resurrect —
   never-reused within a process). The KERI AID (#121), once earned, is what
   survives restart.

## Consequences

- The global identity coordinate is #121's separate `Aid` type, paired with the
  handle at the remote boundary — pair, never replace; the core never routes by
  AID.
- "Restart transparent to watchers" is **permanently forbidden at the handle
  layer** (it would require stable-id-across-restart, which never-reuse
  forbids). Any such feature must be built at the AID layer, with the KEL
  sequence number as the incarnation datum (the KERI-native analog of Akka's
  path `#uid` / Erlang's `creation`).
- `Mailbox::bounded(capacity, id)` remains `pub` but is externally inert:
  outside test-support, ids exist only for spawned actors. Spawn
  (`PreparedActor`) is the front door.
- Unforgeability is a safe-Rust guarantee (`unsafe` transmute can forge
  anything); the clippy restriction suite polices `unsafe`.
```

- [ ] **Step 2: Add the index line to `docs/adr/README.md`** (follow the file's
existing format; read it first).

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0015-actorid-process-local-pure-name.md docs/adr/README.md
git commit -m "docs(adr): ADR-0015 ActorId process-local pure name + incarnation [#206]"
```

---

### Task 5: README + coverage baseline

**Files:**
- Modify: `README.md` ("public API at a glance")
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: README** — public API changed (constructor removed, root
re-export added). In the "public API at a glance" section, update/add the
`ActorId` bullet (adapt to surrounding style):

```markdown
- **`ActorId`** — a process-local, unforgeable pure-name routing key
  (`bombay_core::ActorId`): obtainable only from a spawned actor, deliberately
  non-serializable and unreadable as a number — the dataspace identity of an
  actor is its future KERI AID (#121), never this handle (ADR-0015).
```

Also scan the README for any text implying users construct `ActorId` directly;
fix if found.

- [ ] **Step 2: coverage-baseline.md** — append the I17b entry following the
file's existing per-test format (read it first; record `invariants.rs::i17b_concurrent_mint_distinct_ids`).

- [ ] **Step 3: Commit**

```bash
git add README.md docs/testing/coverage-baseline.md
git commit -m "docs(206): README ActorId bullet + coverage baseline I17b"
```

---

### Task 6: File follow-up card + post the #121 seam contract

**Files:** none (GitHub only). Account must be `joeldsouzax` (`gh auth status`).

- [ ] **Step 1: File the counter-hygiene follow-up card**

```bash
gh issue create --repo devrandom-labs/bombay \
  --title "core(identity): id-counter hygiene — overflow-refuse, loom mint model, mutation zero-survivors" \
  --milestone "M1 · Foundation: Zenoh remote layer" \
  --label foundation \
  --body "$(cat <<'EOF'
Split out of **#206** (see the deferral recorded in its PR): the counter-arithmetic
invariants of the id mint, deliberately separated from the identity-shape work.
`bombay-core/src/id.rs` owns the counter (`NEXT_ACTOR_ID` + `next_actor_id`).

## Scope — one invariant per bullet
- [ ] Overflow never silently wraps: refuse-at-ceiling (`fetch_add` returning `u64::MAX` => `Err`), with the wrap policy documented on the mint. This forces `Prepared::new`/`new_linked` to become fallible — the API decision is part of this card (the 2^64 boundary is ~585 years at 10^9 spawns/s; weigh the error-that-cannot-fire tax deliberately).
- [ ] **loom** model over the concurrent mint. bombay owns this atomic, so loom CAN instrument it — unlike the ADR-0005 ref-model case (flume ships no loom instrumentation); this does not contradict that ADR.
- [ ] `cargo-mutants` zero survivors over `id.rs`, with boundary tests seeding the counter at `0`, `1`, `u64::MAX-1`, `u64::MAX` (reach the boundary via a test-seeded counter — mint as a pure fn over `&AtomicU64`, or a test-support seed seam).

## Context
#206 made `ActorId` unforgeable/non-serializable (the identity-first shape); #88's
original concern (platform width + unwrap) was already satisfied (`AtomicU64`, no
unwrap). What remains here is arithmetic hygiene per the house rules.
EOF
)"
```

- [ ] **Step 2: Add it to project board #4**

```bash
gh project item-add 4 --owner devrandom-labs --url <URL-from-step-1>
```

- [ ] **Step 3: Post the seam contract on #121**

```bash
gh issue comment 121 --repo devrandom-labs/bombay --body "$(cat <<'EOF'
**Seam contract from #206** (M1 local handle — spec: `docs/superpowers/specs/2026-07-26-206-identity-first-actorid-design.md`, ADR-0015). Literature-verified obligations this card inherits:

1. **Incarnation ambiguity at the AID layer.** A stable AID over per-incarnation handles reintroduces the ambiguity Akka's wire `#uid` / Erlang's `creation` solve. KERI hands over the fix: the KEL sequence number/digest is a cryptographic incarnation counter — wire coordinate can be (AID, KEL-anchored datum). Decide **per remote verb**: virtual/Orleans semantics vs incarnation-precise/Akka semantics.
2. **An AID grants zero authority** (confused deputy at the Zenoh boundary — `put` on a key-expr is ambient authority). Encode in types: remote send takes a verified-session witness (`fn tell_remote(peer: &VerifiedPeer, …)`), never a bare `&Aid`; `VerifiedPeer`'s only constructor is KEL-verifying session establishment.
3. **AID→key-expr→handle is a mapping system** (RFC 1498: the bindings are the bug farm): binding authority (KERI-signed location assertion vs node vs registry), staleness on restart/migration, first-contact latency inside ask budgets, invalidation. Design it, don't assume it.
4. **Remote death-watch is suspicion, not death** (Chandra–Toueg; Zenoh liveliness `Delete` under partition is a false positive by construction). Distinguish *suspected* from *confirmed* stop; leases are the literature's answer. Hits the #67 exit gate.
5. **AID never rebinds** to a different logical actor; **an AID is never a GC root** — dangling AID resolves to a clean "no such actor"; the dataspace registry needs a deletion protocol.
6. **Registry equivocation is outside KERI's protection** (witnesses detect key-state duplicity, not registry duplicity) — a forked AID→key-expr view is a SUNDR-style attack; needs fork-consistency or KERI-signed location records.
7. **Sibling-crate practicalities** (verified in-tree): `cesr` workspace `Identifier`/`Matter` implement neither `Hash` nor `Ord` (cannot key maps — add upstream or wrap owned qb64); **no owned AID type exists** (all lifetime-parameterized — this card defines the owned canonical form); delegation verification is unimplemented in keri-rs (`Rejection::DelegationUnsupported`, deferred to K4) — the delegation bullet has vocabulary, not a verify path; transferability is fixed at inception (ephemeral vs rotatable — natural ephemeral/durable actor mapping); the "Existing crates" section is stale (`keriox`/`cesride`) — target `cesr-rs`/`keri-events`/`keri-codec`/`keri-rs`; AIDs travel as qb64 bytes, never serde (the stack has no serde, deliberately; qb64 is key-expr-safe by construction).
EOF
)"
```

- [ ] **Step 4:** No commit (no files changed). Note both URLs for the PR body.

---

### Task 7: Gate, push, PR

- [ ] **Step 1: Format + everything tracked**

```bash
nix develop --command cargo fmt
git status --short   # everything staged/committed, nothing untracked
git add -u && git diff --cached --quiet || git commit -m "style: cargo fmt [#206]"
```

- [ ] **Step 2: Mutants accounting.** The gate ratchet (`nix build .#mutants`)
must account for the new/renamed fns (`id::from_raw`, `id::next_actor_id`;
`from_raw_for_test` is cfg-test-gated, not mutated). Run the gate build; if new
mutants land as Unaccounted, add `mutants-baseline.json` entries per the
existing pattern (`ActorId` has no `Default`, so `Default::default()` mutants
are unviable → `known_zero_viable`-style entries; the `i17`/`i17b` tests must
catch every *viable* mint mutant — a survivor there is a test gap, fix the
test, never the baseline).

```bash
nix build .#mutants 2>&1 | tail -20
```

Expected: success, 0 missed. Commit any baseline change:

```bash
git add mutants-baseline.json && git commit -m "chore(mutants): account id.rs fns [#206]"
```

(If the baseline file lives elsewhere, locate with `fd mutants-baseline`.)

- [ ] **Step 3: The single gate** (commit first — the gate is slow):

```bash
nix flake check 2>&1 | tail -5
```

Expected: success. A ~10-20s failure on flake-input eval is the GitHub-503
pattern — rerun, don't debug the diff.

- [ ] **Step 4: Push + PR**

```bash
git push -u origin feat/206-identity-first-actorid
gh pr create --repo devrandom-labs/bombay --title "core(id): identity-first ActorId — unforgeable process-local pure name [#206]" --body "$(cat <<'EOF'
Closes #206.

## Shipped (per scope bullet)
- Unforgeable: `pub(crate)` mint + `test-support`-gated `from_raw_for_test` — `bombay-core/src/id.rs`
- Non-serializable: orphan rule + field-poisoning (documented) + `assert_not_impl_any!` pin compiled in the gate (`id.rs` tests; probe-verified)
- Pure name: no getter / `From` / `Into<u64>` / `Display`
- Designation ≠ authority documented (module docs + ADR-0015); registry stays name-keyed
- Own `id` module, root re-export `bombay_core::ActorId` (mailbox path kept as re-export)
- Incarnation constraint: ADR-0015
- #121 seam contract posted: <link to comment>
- Test: `invariants.rs::i17b_concurrent_mint_distinct_ids` (barrier overlap, probe-verified)

## Deferred (landed, not silent)
- Counter hygiene (overflow refuse-at-ceiling, loom mint model, mutants boundary sweep) → <follow-up issue URL>. #88's width+unwrap concern is already satisfied in-tree (`AtomicU64`, no unwrap).

Design: `docs/superpowers/specs/2026-07-26-206-identity-first-actorid-design.md` (literature-verified).
EOF
)"
```

---

## Self-review (done at plan-writing time)

- **Spec coverage:** all 7 scope bullets map — unforgeable (T1), pin (T2), pure name (T1: no getter exists today, none added; `Display` never existed), designation≠authority docs (T1 module docs + T4 ADR), module+re-export (T1), ADR (T4), #121 post (T6). README case (T5). Follow-up card filed before PR (T6 before T7). Concurrent-mint test (T3).
- **Type consistency:** `from_raw` / `from_raw_for_test` / `next_actor_id` used consistently across tasks; `mailbox::ActorId` kept as re-export (25 files import that path — churn not worth it, and `Mailbox::bounded` legitimately references it).
- **No placeholders:** every code step carries the code; gh bodies inline.
