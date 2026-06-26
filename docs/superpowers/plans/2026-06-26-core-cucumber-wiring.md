# Core cucumber wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire all 24 `tests/features/core/*.feature` files (309 scenarios across 12 modules) to the live `kameo` core so every non-`@bug` scenario is GREEN under `nix flake check`, with the 3 `actor_id` `@bug` probes kept RED via `#[should_panic]` demonstrators.

**Architecture:** Per-module World + step file under `tests/core_steps/`; one standard-libtest cucumber runner per feature file (each runner `#[path]`-includes only its own module file). Real spawned tokio actors for behaviour; `proptest!` for pure laws; `tokio::spawn` + `Barrier` + cited oracles for concurrency; deterministic boundary-loops for async/global laws. Source is not modified except a one-line `@bug` tag correction in `actor_id.properties.feature` and (per module, only where needed) gated `testing` re-exports of private items.

**Tech Stack:** Rust edition 2024, `cucumber = "0.23"` (libtest), `proptest = "1.11"`, tokio multi-thread, crane/nextest gate. Deps already present from card #76.

**Design doc:** `docs/superpowers/specs/2026-06-26-core-cucumber-wiring-design.md`

---

## File structure

```
tests/
  core_steps/
    actor_id.rs              ActorIdWorld + steps + proptest laws + oracles  (Task 1)
    error.rs                 ErrorWorld + steps + laws                       (Task 3)
    message.rs               MessageWorld + steps + laws                     (Task 4)
    reply.rs                 ReplyWorld + steps + laws                       (Task 5)
    actor_ref.rs             ActorRefWorld + steps + laws                    (Task 6)
    actor_lifecycle.rs       LifecycleWorld + steps + laws                   (Task 7)
    request_ask.rs           AskWorld + steps + laws                         (Task 8)
    request_tell.rs          TellWorld + steps + laws                        (Task 9)
    mailbox.rs               MailboxWorld + steps + laws                     (Task 10)
    registry.rs              RegistryWorld + steps + laws                    (Task 11)
    links.rs                 LinksWorld + steps + laws                       (Task 12)
    supervision.rs           SupervisionWorld + steps + laws                 (Task 13)
  core_actor_id_bdd.rs       runner -> actor_id.feature           + 3 #[should_panic] probes
  core_actor_id_props_bdd.rs runner -> actor_id.properties.feature
  core_error_bdd.rs / core_error_props_bdd.rs
  … one example runner + one props runner per module (24 runners total)

Cargo.toml                   MODIFY  one [[test]] block per runner (required-features=["testing"])
src/lib.rs                   MODIFY  (only if a module needs it) crate-root #[cfg] pub mod testing
src/<module>.rs              MODIFY  (only where needed) flip a private item to pub for re-export
tests/features/core/actor_id.properties.feature  MODIFY  add @bug:id.rs:140-143 to one scenario
README.md                    MODIFY  every commit
```

Each runner `#[path = "core_steps/<module>.rs"] mod <module>;` includes ONLY its module file, so a module that needs the `testing` feature does not force it on unrelated runners, and a compile error stays local. The example runner and the props runner for a module both include the same module file (cucumber registers steps per test binary).

## Conventions for every task

- **Toolchain:** no local cargo. Run everything through the flake dev shell: `nix develop -c cargo test --test <runner_name>`. The authoritative gate is `nix develop -c nix flake check` (or just `nix flake check`).
- **README:** the pre-commit hook (`.githooks/pre-commit`) blocks any commit that doesn't stage a `README.md` change. Every commit step stages a one-line truthful README update (e.g. bump the per-module progress note added in Task 1).
- **Facts only (CLAUDE.md rule 0):** every asserted value comes from the scenario's own `# Confirmed:` / `# ORACLE:` note (already written in each `.feature`) and the cited source line. If a scenario's confirmed behaviour cannot be reproduced green, **stop and surface it** — never assert buggy behaviour to force green (design §3).
- **Methodology (design §4):** pure `@property` → real `proptest!` with boundary-biased generators hitting the `# GEN:` values; `@linearizability`/`@model` → `tokio::spawn` + `Arc<Barrier>` real overlap + an independent cited oracle; async/global `@property` → documented deterministic boundary-loop + seeded LCG; `@timing`/`@unstable-clock` → `tokio::time::pause()`/`advance()`, never assert `SystemTime` ordering; actor-stop → condition-based polling, never `wait_for_shutdown()` as the settle barrier.
- **Runner template (used verbatim in every runner, substituting `<World>`, `<file>`, and the bug predicate):**
  ```rust
  #[tokio::test(flavor = "multi_thread")]
  async fn <module>_features() {
      <World>::cucumber()
          .fail_on_skipped()
          .with_default_cli()
          .filter_run_and_exit(
              concat!(env!("CARGO_MANIFEST_DIR"), "/tests/features/core/<file>.feature"),
              // green run excludes @bug scenarios; only actor_id has any.
              |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
          )
          .await;
  }
  ```
  Add `.max_concurrent_scenarios(1)` ONLY for modules that touch process-global state (registry's `ACTOR_REGISTRY`, the ActorId counter laws) — noted per task.

---

# Task 1: Bootstrap skeleton + actor_id (the exemplar)

This task establishes the whole pattern on the simplest module. `ActorId::{new, generate, to_bytes, from_bytes, sequence_id}` and `ActorIdFromBytesError::MissingSequenceID` are already `pub` (verified: `src/actor/id.rs:39,72,89,110,138,242`), so **no `testing` surface is needed** — use `kameo::actor::ActorId` directly. Once green, every other module is this shape with a domain-specific World.

**Files:**
- Create: `tests/core_steps/actor_id.rs`
- Create: `tests/core_actor_id_bdd.rs`
- Create: `tests/core_actor_id_props_bdd.rs`
- Modify: `tests/features/core/actor_id.properties.feature` (one tag line)
- Modify: `Cargo.toml` (two `[[test]]` blocks)
- Modify: `README.md`

- [ ] **Step 1: Register the two runner targets**

Append to root `Cargo.toml` (after the existing `console_wire_props_bdd` block):

```toml
[[test]]
name = "core_actor_id_bdd"
required-features = ["testing"]

[[test]]
name = "core_actor_id_props_bdd"
required-features = ["testing"]
```

- [ ] **Step 2: Write the example runner + the 3 `#[should_panic]` `@bug` probes**

Create `tests/core_actor_id_bdd.rs`:

```rust
//! Cucumber runner for core/actor_id.feature, plus the @bug:id.rs probes.
//!
//! The 3 @bug scenarios in actor_id.feature assert the DESIRED behaviour
//! (from_bytes returns Err(MissingSequenceID) on a short slice). The source
//! panics on `bytes[0..8]` (id.rs:140) BEFORE the map_err can run, so those
//! scenarios are excluded from the green cucumber run (the filter predicate
//! drops any @bug tag) and the live defect is pinned by the #[should_panic]
//! tests below instead. They pass GREEN while the bug lives and flip RED the
//! moment fix(actor_id) lands — at which point delete them and remove the @bug
//! tags so the real Err-asserting scenarios run green.

#[path = "core_steps/actor_id.rs"]
mod actor_id;

use actor_id::ActorIdWorld;
use cucumber::World;
use kameo::actor::ActorId;

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_features() {
    ActorIdWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/features/core/actor_id.feature"),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}

// --- @bug:id.rs:140-143 — from_bytes panics on a too-short slice -------------

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_140_from_bytes_panics_on_short_slice() {
    let _ = ActorId::from_bytes(&[0u8; 4]);
}

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_140_from_bytes_panics_on_empty_slice() {
    let _ = ActorId::from_bytes(&[]);
}

// --- @bug:id.rs:218-221 — Deserialize of a too-short buffer panics -----------

#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_218_deserialize_panics_on_short_buffer() {
    // serde's visit_bytes runs from_bytes, which panics before the
    // invalid_length mapping (id.rs:218-221) can run. rmp-encode a 4-byte
    // bytes payload and decode it as ActorId to hit the visitor.
    let buf = rmp_serde::to_vec(&serde_bytes::Bytes::new(&[0u8; 4])).unwrap();
    let _: ActorId = rmp_serde::from_slice(&buf).unwrap();
}
```

NOTE on the third probe: confirm during implementation that `rmp_serde` routes the payload to `ActorIdVisitor::visit_bytes` (not `visit_seq`). If `serde_bytes` is not already a dev-dep, either add it or construct the byte-payload another way that reaches `visit_bytes` (e.g. `rmp_serde::to_vec(&ByteBuf::from(vec![0u8;4]))`). If neither reaches the panic, fall back to asserting the panic on the direct `ActorId::from_bytes(&[0u8;4])` path only and document that the serde path shares the same root cause (id.rs:140) — but prefer the genuine visitor path.

- [ ] **Step 3: Run the probes — verify they pass (the bug is live)**

Run: `nix develop -c cargo test --test core_actor_id_bdd -- bug_id`
Expected: 3 `bug_id_*` tests PASS (each panics with "out of range for slice", satisfying `should_panic`). The `actor_id_features` test will fail until Step 4 wires the World — that is expected at this step; filter to the `bug_id` tests as shown.

- [ ] **Step 4: Write `ActorIdWorld` + step definitions for actor_id.feature**

Create `tests/core_steps/actor_id.rs`. Wire every NON-`@bug` scenario in `tests/features/core/actor_id.feature` (read it as the source of truth; assertions are in each scenario's `# Confirmed:` note). The World and the key step shapes:

```rust
use std::collections::HashSet;
use std::sync::Arc;

use cucumber::{World, given, then, when};
use kameo::actor::ActorId;
use tokio::sync::Barrier;

#[derive(Debug, Default, World)]
pub struct ActorIdWorld {
    ids: Vec<ActorId>,         // generated/collected ids
    a: Option<ActorId>,        // primary subject
    b: Option<ActorId>,        // comparison subject
    bytes: Vec<u8>,            // encode/decode buffer
    decoded: Option<ActorId>,  // from_bytes/round-trip result
    last_string: String,       // Display/Debug output
    counter_before: u64,       // recorded counter baseline for range laws
}

// @sequence: sequential generate() -> +1
#[given("two ActorIds are generated one after another on a single thread")]
async fn given_two_sequential(world: &mut ActorIdWorld) {
    world.a = Some(ActorId::generate());
    world.b = Some(ActorId::generate());
}

#[when("their sequence ids are compared")]
async fn when_compare_seq(_world: &mut ActorIdWorld) {}

#[then("the second sequence id is exactly the first plus one")]
async fn then_second_is_first_plus_one(world: &mut ActorIdWorld) {
    let a = world.a.unwrap().sequence_id();
    let b = world.b.unwrap().sequence_id();
    assert_eq!(b, a + 1, "generate() must hand out consecutive ids");
}
```

Cover, from the feature (use `#[given/when/then(regex = ...)]`, never `expr`):
- **@sequence** consecutive generate (+1); batch of 1000 distinct (`HashSet` len == 1000).
- **@lifecycle** byte round-trips: `to_bytes`→`from_bytes` equals original; `new(7)` encodes to exactly 8 bytes; `new(123456789)` round-trip preserves `sequence_id`; serde round-trip (`rmp_serde::to_vec`/`from_slice`) equals original.
- **@boundary** Display outline (`"#{seq}"`), Debug outline (`"ActorId({seq})"`), the 8-byte decode success (`from_bytes(&1u64.to_le_bytes())` → `sequence_id == 1`), ordering follows seq, `generate()` never panics + unique.
- **@linearizability** concurrent generate no-dup; contiguous-range law (record `counter_before` via a baseline `generate()`, spawn N tasks each generating, assert the set == `[base+1 .. base+1+k]` exactly — oracle is integer contiguity, NOT a SUT call). Use `Arc<Barrier>` so the tasks overlap. Equal/unequal hash+eq.

For the contiguous-range oracle:
```rust
#[then(regex = r"^the set equals exactly the integers N through N\+99$")]
async fn then_contiguous_range(world: &mut ActorIdWorld) {
    let got: HashSet<u64> = world.ids.iter().map(|i| i.sequence_id()).collect();
    let want: HashSet<u64> = (world.counter_before..world.counter_before + 100).collect();
    assert_eq!(got, want, "fetch_add must cover [N, N+100) with no gaps/repeats");
}
```
(Record `counter_before` as the `sequence_id()` of one baseline `generate()` **plus 1** — read the scenario's `Given the counter value before spawning is recorded as N` to align the exact baseline; the baseline id IS N-1 if you generate one to probe, so derive N precisely and assert the off-by-one against the feature text.)

- [ ] **Step 5: Run the example feature — expect green (minus @bug, which is filtered)**

Run: `nix develop -c cargo test --test core_actor_id_bdd -- actor_id_features`
Expected: PASS. `.fail_on_skipped()` guarantees every non-`@bug` scenario is wired; the 3 `@bug` scenarios are filtered out. If any scenario is reported skipped/undefined, wire it before continuing (no false green).

- [ ] **Step 6: Commit the example slice**

```bash
git add tests/core_steps/actor_id.rs tests/core_actor_id_bdd.rs Cargo.toml README.md
git commit -m "test(core): card #77 wire actor_id.feature + @bug should_panic probes"
```
(README: add a "Core wiring progress" line listing actor_id example done.)

- [ ] **Step 7: Add the `@bug` tag correction + the props runner**

In `tests/features/core/actor_id.properties.feature`, the `@property @boundary` scenario *"from_bytes rejects any byte string shorter than eight bytes"* asserts `Err(MissingSequenceID)` but lacks a `@bug` tag, so it would panic. Change its tag line from:
```
  @property @boundary
```
to:
```
  @property @boundary @bug:id.rs:140-143
```
(Only that one scenario — the other props scenarios stay as-is.) This matches `docs/testing/properties.md:47-48`.

Create `tests/core_actor_id_props_bdd.rs`:
```rust
#[path = "core_steps/actor_id.rs"]
mod actor_id;

use actor_id::ActorIdWorld;
use cucumber::World;

#[tokio::test(flavor = "multi_thread")]
async fn actor_id_props_features() {
    ActorIdWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/features/core/actor_id.properties.feature"),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
```

- [ ] **Step 8: Wire the property laws (in `tests/core_steps/actor_id.rs`)**

Add `proptest!`-backed `Then` steps for the `@property` laws (real sync proptest, boundary-biased per each `# GEN:`):
```rust
use proptest::prelude::*;

#[then("the decoded ActorId equals the original")]
async fn law_roundtrip_identity(_world: &mut ActorIdWorld) {
    // GEN: sequence_id ∈ {0,1,MAX-1,MAX} ∪ uniform. ORACLE: from_bytes ∘ to_bytes = id.
    let boundaries = [0u64, 1, u64::MAX - 1, u64::MAX];
    for v in boundaries {
        let id = ActorId::new(v);
        assert_eq!(ActorId::from_bytes(&id.to_bytes()).unwrap(), id);
    }
    proptest!(|(v in any::<u64>())| {
        let id = ActorId::new(v);
        prop_assert_eq!(ActorId::from_bytes(&id.to_bytes()).unwrap(), id);
    });
}
```
Wire the remaining `@property` laws the same way: 8-byte accept/recover (`from_le_bytes` inverse), sequential +1 over n ∈ {1,2,1000}, Eq/Hash/Ord agree with the integer oracle. The `@model` concurrent-contiguity law: real `tokio::spawn` + `Arc<Barrier>` with P ∈ {2,16}, k ∈ {1,2,100,1000}, asserting the set == contiguous `[N0, N0+k)` (the integer oracle), as a deterministic loop over those boundary (P,k) pairs (async + the global counter make `proptest!` unsuitable — document this in-step). The `@bug:id.rs:140-143` property is filtered out (covered by the should_panic probes); leave it in the feature as the desired-behaviour spec.

- [ ] **Step 9: Run the props feature**

Run: `nix develop -c cargo test --test core_actor_id_props_bdd`
Expected: PASS (all non-`@bug` property scenarios green).

- [ ] **Step 10: Mutation-review gate (independent reviewer)**

Dispatch an independent reviewer subagent: for each assertion in `actor_id.rs`, mutate the expected value (e.g. `a + 1` → `a + 2`, the contiguous range bounds, the Display string) and confirm the test FAILS; confirm the 3 `@bug` probes still pass and that flipping the source to a non-panicking stub would make them fail (reason about it — do not edit source). Fix any assertion that passes under mutation. Confirm no `@bug` scenario leaks into the green run (scenario count sanity: example run reports `17 - 3 = 14` scenarios; props reports `6 - 1 = 5`).

- [ ] **Step 11: Commit the props slice**

```bash
git add tests/core_steps/actor_id.rs tests/core_actor_id_props_bdd.rs tests/features/core/actor_id.properties.feature README.md
git commit -m "test(core): card #77 wire actor_id property laws + @bug tag correction"
```

---

# Tasks 3–13: one module per task

Each module task follows Task 1's shape exactly, substituting the World name, the SUT surface, and the feature files. **Per-task checklist (identical structure every time):**

1. Read `tests/features/core/<module>.feature` and `<module>.properties.feature` — the scenarios' `# Confirmed:`/`# ORACLE:` notes are the assertions.
2. Audit the SUT surface (below). If an item under test is private, add a gated re-export: in `src/lib.rs` add (once, if not present) `#[cfg(any(test, feature = "testing"))] pub mod testing { … }` and re-export the needed items, flipping the owning item to `pub` with its module kept private (design §2; CLAUDE.md rule 4). Most-public-first: try `kameo::<module>::*` directly before adding any surface.
3. Register two `[[test]]` blocks in `Cargo.toml` (`core_<module>_bdd`, `core_<module>_props_bdd`, both `required-features = ["testing"]`).
4. Create `tests/core_steps/<module>.rs` (World + steps + laws) and the two runner files (Task 1 Step 2 template; no `#[should_panic]` block — only actor_id has `@bug`). Apply `.max_concurrent_scenarios(1)` where flagged.
5. Wire example scenarios → run `nix develop -c cargo test --test core_<module>_bdd` → green. Then wire property/model laws → run the props runner → green.
6. Mutation-review gate (independent reviewer; break each assertion, prove it fails; verify real overlap for `@linearizability`; verify no silent narrowing of any law).
7. Commit (one or two commits per module, README staged): `test(core): card #77 wire <module> scenarios`.

The per-module specifics:

### Task 3: error (`error.feature` 20 + `.properties` 4)

**SUT (`src/error.rs`, all pure, in-process):** `SendError<M,E>` variants (`ActorNotRunning`/`ActorStopped`/`MailboxFull`/`HandlerError`/`Timeout`) and its combinators `map_msg`/`map_err`/`boxed`/`msg`/`err`/`flatten`; `BoxSendError` downcast/`try_downcast` round-trip; `PanicError`; `Infallible`. Likely all `pub` via `kameo::error::*` — verify; no actors needed. **World:** holds a `SendError<TestMsg, TestErr>` and downcast results. **Methodology:** mostly `@sequence`/`@boundary`/`@lifecycle` over pure values; `@property` laws (6) via real `proptest!`; one `@model`. Pure module — no `max_concurrent_scenarios`. Watch the `@phase2` props (still in scope).

### Task 4: message (`message.feature` 17 + `.properties` 5)

**SUT (`src/message.rs`):** `Message` handler, `Context<A,R>` (actor_ref/reply channel/stop flag), `reply_sender`/`reply`/`forward`/`try_forward`/`blocking_forward`, `StreamMessage`, `DynMessage::handle_dyn` dispatch (reply routes to ask caller; error routes to panic hook on tell). **World:** real spawned actor(s) + a captured reply/forward result. **Methodology:** `@sequence`/`@lifecycle` need real actors (ask/tell, forward); `@linearizability` (5) via `tokio::spawn`+`Barrier`; `@timing` (1) via `tokio::time`. No `@bug` tags (the comment header mention is not a tag). Confirm the `@review-semantics` notes are pinned, not asserted as guarantees.

### Task 5: reply (`reply.feature` 22 + `.properties` 5)

**SUT (`src/reply.rs`):** `Reply` trait (`to_result`/`into_any_err`/`into_value`/`downcast_ok`/`downcast_err`), `Result`/`impl_infallible_reply!` impls, `DelegatedReply`, `ReplySender` single-use send, `ForwardedReply` (Forwarded vs Direct; `from_ok`/`from_err`/`from_result`; downcast paths). **World:** holds reply values + a `ReplySender`. **Methodology:** mostly pure/`@sequence`/`@boundary`; `ReplySender` single-use is a `@lifecycle` (send once, second send is the error path); `@linearizability` (4) real overlap; `@property` (6) proptest. Mostly no actors except where a reply channel is exercised end-to-end.

### Task 6: actor_ref (`actor_ref.feature` 22 + `.properties` 5)

**SUT (`src/actor/actor_ref.rs`):** ask/tell, alive/dead state machine, strong/weak refcounts, downgrade/upgrade, `is_current`, identity (id/eq/hash/ord), startup/shutdown waiters, `Recipient`/`ReplyRecipient` type-erasure, self link/unlink no-ops. **World:** spawned actors + `WeakActorRef`s + recipients. **Methodology:** heavy `@boundary`/`@lifecycle`/`@linearizability` (8, refcount races — real overlap); `@timing` (1) via `tokio::time`; `@model` (5). **Use condition-based polling for any alive/dead assertion** (not `wait_for_shutdown` as settle barrier).

### Task 7: actor_lifecycle (`actor_lifecycle.feature` 23 + `.properties` 6)

**SUT (`src/actor.rs`, `src/actor/spawn.rs`, `src/actor/kind.rs`):** lifecycle hooks (`on_start`/`on_panic`/`on_link_died`/`on_stop`), `run_actor_lifecycle`, `ActorBehaviour` startup-buffer replay, `Spawn` trait variants. **World:** actors whose hooks record observable side-effects (e.g. an `Arc<Mutex<Vec<&str>>>` hook log). **Methodology:** dominant `@lifecycle` (18); `@linearizability` (7); `@timing` (1) via `tokio::time`; `@model` (4). The header-comment `@bug:` is NOT a tag (verified — no real `@bug` here). Startup-buffer replay is a `@sequence`: messages sent before `on_start` finishes must replay in order — assert the recorded order.

### Task 8: request_ask (`request_ask.feature` 23 + `.properties` 4)

**SUT (`src/request/ask.rs` + `src/request.rs`):** `AskRequest` builder — `ask(M)` → `mailbox_timeout`/`reply_timeout` → `send`/`try_send`/`blocking_send`/`enqueue`/`try_enqueue`/`blocking_enqueue`/`forward`/`try_forward`/`blocking_forward`/`IntoFuture`. **World:** spawned actor + the request outcome. **Methodology:** `@timing` is dominant (13) — use `tokio::time::pause()`/`advance()` to drive mailbox/reply timeouts deterministically (a slow handler + `advance` past the timeout, asserting `Err(SendError::Timeout)`). `@linearizability` (6) real overlap. `blocking_*` variants run on a blocking thread (`tokio::task::spawn_blocking`).

### Task 9: request_tell (`request_tell.feature` 16 + `.properties` 4)

**SUT (`src/request/tell.rs` + `src/request.rs`):** `TellRequest` — `tell(M)` → `mailbox_timeout` → `send`/`try_send`/`blocking_send`/`send_after`/`IntoFuture`. **World:** spawned actor + delivery observation. **Methodology:** `@timing` (10) via `tokio::time` (esp. `send_after` — `advance` the clock and assert delivery; never sleep). `@lifecycle`/`@boundary` for full-mailbox `try_send` (`MailboxFull`) and dead-actor (`ActorNotRunning`). `@linearizability` (4) real overlap. One `@phase2` prop.

### Task 10: mailbox (`mailbox.feature` 35 + `.properties` 7) — largest

**SUT (`src/mailbox.rs`):** `Mailbox` MPSC signal channel (bounded `mpsc::channel` / unbounded `mpsc::unbounded_channel`) + the `front: VecDeque<Signal<A>>` push-back buffer for restart re-queue. Likely needs a **`testing` re-export** of `Mailbox` constructors/`front` access — audit and expose minimally. **World:** a constructed mailbox + signals. **Methodology:** `@sequence` (15) FIFO + front-buffer ordering (push-back then drain — assert exact order); `@boundary` (16) bounded-full / closed-channel / capacity edges; `@linearizability` (8) multi-producer single-consumer real overlap; `@property` (7) proptest over orderings; `@model` (4). The header `@bug:` mention is not a tag (verified none). Split into 2 commits (example, then props) given size.

### Task 11: registry (`registry.feature` 20 + `.properties` 3)

**SUT (`src/registry.rs`):** local `ActorRegistry` behind the global `ACTOR_REGISTRY` Mutex — register/lookup/unregister by name; local only (remote out of scope). **Process-global state → `.max_concurrent_scenarios(1)` AND reset between scenarios** (unregister all test names in the World `Default`/a per-scenario cleanup, or use unique names per scenario and assert deltas). **World:** registered names + lookups. **Methodology:** `@sequence`/`@boundary`/`@lifecycle` (dead-actor lookup returns None / is pruned); `@linearizability` (5) concurrent register/lookup real overlap; `@model` (4). One `@phase2` prop. If a re-export of a reset hook is needed, add it gated like console's `reset_for_test`.

### Task 12: links (`links.feature` 17 + `.properties` 4)

**SUT (`src/links.rs`):** `Links` per-actor registry — `parent` (notified with `mailbox_rx`, can restart), `sibblings` [sic] (notified without `mailbox_rx`, no restart), `children` (`ErasedChildSpec`); link/unlink/notify machinery under supervision. Likely needs **`testing` re-exports** of `Links` internals — audit/expose minimally. **World:** linked actor sets + notification observations. **Methodology:** `@lifecycle` (9) link→die→notify; `@sequence` (7) notification order; `@linearizability` (7) concurrent link/unlink real overlap; `@model` (4). One `@phase2` prop. Use condition-based polling for death-notification assertions.

### Task 13: supervision (`supervision.feature` 21 + `.properties` 3) — hardest, last

**SUT (`src/supervision.rs` + `should_restart` on `ErasedChildSpec` at `src/links.rs:226-265`):** `RestartPolicy` (Permanent/Transient/Never), `SupervisionStrategy` (OneForOne/OneForAll/RestForOne), restart-intensity sliding-window limits, the `should_restart` decision. **World:** a supervisor + children with recorded restart events. **Methodology:** `@lifecycle` (12) restart strategies (kill a child, assert which siblings restart per strategy — OneForOne restarts one, OneForAll restarts all, RestForOne restarts the rest); `@unstable-clock` (6) + `@timing` (6) the sliding-window intensity limit — drive with `tokio::time::pause()`/`advance()` so the window is deterministic, assert "more than N restarts within the window → escalate/stop". `@linearizability` (5) concurrent child failures real overlap; `@model` (3). One `@phase2` prop. Largest behavioural surface — budget extra review.

---

# Task 14: Whole-suite green gate

**Files:** none (verification only) — plus a final README line and the PR.

- [ ] **Step 1: Full gate**

Run: `nix develop -c nix flake check -L`
Expected: GREEN — build + clippy + fmt + audit + deny + nextest (all 24 core runners + the 3 `@bug` probes + the pre-existing console suite) + doctest + actionlint all pass. The 3 `bug_id_*` `#[should_panic]` tests pass (bug live); every non-`@bug` core scenario passes.

- [ ] **Step 2: If a scenario flakes (CI-only)**

Get the exact failing scenario from the `-L` logs (never guess by elimination). Reproduce locally with an exit-code loop:
```bash
for i in $(seq 1 50); do nix develop -c cargo test --test core_<module>_bdd >/dev/null 2>&1 || { echo "FAIL on iter $i"; break; }; done
```
Fix determinism (barrier-based overlap, `tokio::time` instead of real sleeps, condition-based settle, monotonic-only assertions) — never `#[ignore]` a non-`@bug` scenario.

- [ ] **Step 3: Move the card + open the PR**

```bash
git push -u origin test/77-core-wiring
nix run nixpkgs#gh -- pr create --repo devrandom-labs/bombay --base main \
  --title "test(core): card #77 wire core feature scenarios to the SUT" \
  --body "Wires all 24 tests/features/core/*.feature files (309 scenarios / 12 modules) to the live kameo core on the #76 cucumber harness. Non-@bug scenarios green under nix flake check; the 3 actor_id from_bytes @bug probes are #[should_panic] demonstrators (green while the defect lives, red on fix). One @bug tag correction on the too-short property law. Design: docs/superpowers/specs/2026-06-26-core-cucumber-wiring-design.md."
```
Set project #4 item #77 Status → Done (or leave In Progress until merge, per board convention). No Claude/Anthropic attribution.

---

## Self-review notes

- **Spec coverage:** §2 layout → File structure + Task 1 Steps 1–2, 7; §3 `@bug` + latent-red → Task 1 Steps 2, 7, 8 + filter predicate in every runner; §4 methodology → Conventions block + per-task "Methodology" lines; §5 execution order → Tasks 1, 3–13 in the design's wave order (actor_id → error/message/reply → actor_ref/lifecycle/ask/tell → mailbox/registry/links → supervision); §6 risks → per-task mutation gate + Task 14 flake procedure.
- **Module numbering:** Tasks are 1 (actor_id), 3–13 (the other 11 modules), 14 (gate). There is intentionally no Task 2 — the 11 follow-on modules are Tasks 3–13 so each module's number is stable if one is split. (If preferred, renumber 3–13 → 2–12 at execution; the content is unaffected.)
- **`@bug` scope:** verified only `actor_id.feature` carries real `@bug` tags (3 scenarios); all other "@bug:" occurrences are authoring-rule comment headers, so only Task 1 has `#[should_panic]` probes.
- **Placeholder scan:** SUT surfaces and methodology are concrete per module; per-scenario step text is delegated to the feature files by design (they are executable specs with confirmed assertions), not left as TODO.
