# #149 — Reusable bolero fuzz workspace + property/replay in the flake gate

**Status:** Implemented (2026-07-13). Sub-task of #117.
**Backbone for:** #150 (DST), #151 (MIRI + counting allocator), #152 (nightly deep-fuzz).

## Purpose

Stand up the **reusable verification backbone** for the actor core: one isolated
`bolero` workspace whose `check!` targets run **two ways from the same code** —
deterministic corpus-replay + bounded-random on **stable, inside `nix flake
check`** (this card), and sanitized deep-fuzz on **nightly, in a scheduled
workflow** (#152). This mirrors the pattern proven in `cesr` (`fuzz/` workspace +
`cesr-fuzz-replay` flake check) and `mnesis` (MIRI over existing tests).

The first real target fuzzes the **mailbox / `Signal` state machine** against a
reference model, turning fuzzing into a *correctness* differential (FIFO,
exactly-once, capacity backpressure), not just a panic-hunt.

## Key decision: sync-only surface

The mailbox splits cleanly:
- **sync** — `try_send`, `drain`, `is_closed`
- **async** — `send`, `send_message`, `recv`

The fuzz target drives the **sync surface only**. Rationale:
1. **No runtime in the closure** → fast, deterministic replay.
2. **MIRI-runnable** (#151): MIRI cannot drive the tokio runtime, so a sync
   target is the *same* surface MIRI's leak/UB job runs. An async target would
   be lost to #151.
3. **Clean reuse boundary:** async send/recv *concurrency* is #150's DST
   (loom/shuttle) job. Fuzzing the sync state machine and DST-ing the async
   concurrency partition the space without overlap.

The sync surface still exercises the load-bearing properties: `try_send`
enqueues a `Signal::Message { msg, self_sender }` (the self-pin), `drain` bulk-
dequeues in FIFO, capacity gives `Full` backpressure, and dropping the receiver
with a backlog is the leak cycle #151 measures.

## Layout (directory `fuzz/`, crate `bombay-fuzz`)

```
fuzz/
  Cargo.toml          # own [workspace] (non-member); [profile.fuzz]; deps below
  Cargo.lock          # committed; vendored separately by the flake
  tests/
    smoke.rs          # bolero wiring proof (cesr smoke target, verbatim)
    mailbox.rs        # the model-based mailbox target (below)
    __fuzz__/         # committed corpus seeds (extension-less)
```

`fuzz/Cargo.toml`:
- `[workspace]` empty table → own root, so the fuzz dependency tree never enters
  the main crate's audit/deny/dev surface.
- deps: `bolero` (latest — verify the flake check is green before trusting the
  version), `bolero-generator` (matching), `bombay-core = { path = ".." }`.
- `[profile.fuzz]` inheriting `dev` with `opt-level = 3`, `codegen-units = 1`,
  `incremental = false` (bolero requires a `fuzz` profile to exist).
- **No `fuzz/rust-toolchain.toml`** — nightly stays quarantined to #152's CI env,
  so a rustup user's `cd fuzz && cargo test` replay stays on stable.

## The mailbox target — model-based differential

```rust
#[derive(Debug, TypeGenerator)]
enum Op {
    TrySend(u64),   // enqueue Signal::Message { msg, self_sender: tx.clone() }
    Drain,          // rx.drain() -> assert exact FIFO equality vs model
    CloneTx,        // add a sender clone (exercises sender-count)
    DropTx,         // drop one sender clone (last drop => is_closed)
    IsClosed,       // assert is_closed() == (no live senders)
}

#[test]  // bolero::check! runs under `cargo test`
fn mailbox_state_machine() {
    bolero::check!()
        .with_type::<(u16, Vec<Op>)>()   // u16 seed -> Capacity (see below)
        .for_each(|(cap_seed, ops)| {
            let cap = capacity_from_seed(*cap_seed);   // boundary-biased map
            let (tx, mut rx) = Mailbox::<Probe>::bounded(cap);
            let mut senders = vec![tx];
            let mut model: VecDeque<u64> = VecDeque::new();
            for op in ops { /* drive real mailbox + model in lockstep, assert */ }
        });
}
```

`Capacity` wraps `NonZeroUsize` and is foreign to `bolero-generator`, so we do
**not** generate it directly. Instead generate a `u16` seed and map it to a
`Capacity` in a small helper (`capacity_from_seed`) that biases toward the
boundaries (`1`, `MAX-1`, `MAX`) — keeping the fuzzed capacities bounded and
reproducible without an orphan `TypeGenerator` impl.

**Oracle = a `VecDeque<u64>` reference model.** Every op runs against both the
real mailbox and the model, asserting exact equivalence (`assert_eq!`, never
`contains`):
- `TrySend(m)` → `Ok` iff `model.len() < cap`; on `Ok`, `model.push_back(m)`; on
  `Full`, model unchanged and the returned message equals `m`.
- `Drain` → the drained payloads equal `model.drain(..)` **in FIFO order**
  (proves FIFO + exactly-once).
- `CloneTx`/`DropTx` → track live sender count; `IsClosed` asserts
  `is_closed() == senders.is_empty()`.
- **No sequence panics.**

`Probe`'s `Msg = u64` (the existing mailbox test scaffold type).

**Capacity is fuzzed** with boundary emphasis (`1`, `MAX-1`, `MAX`). `0` is an
invalid `NonZeroUsize`, so it is a rejected-input assertion, not a queue op.

**Deliberately out of scope for #149:** the exact-memory / leak assertion. It
needs #151's counting global allocator, which plugs into *this same target*
later (drop `rx` mid-backlog → assert live bytes return to baseline). #149 builds
the target; #151 adds the allocator; #152 runs it under nightly sancov. That is
the reuse.

## In-gate flake wiring

Add a `bombay-fuzz-replay` check to `flake.nix`:
- Vendor the fuzz workspace's own lock: `craneLib.vendorCargoDeps { cargoLock =
  ./fuzz/Cargo.lock; }`.
- `craneLib.mkCargoDerivation` (template: the existing `mutants` derivation) with
  `buildPhaseCargoCommand = "(cd fuzz && cargo test --no-fail-fast)"` on the
  pinned **stable** toolchain.
- bombay's flake builds `src` via `lib.fileset.toSource`; extend the fileset to
  include `fuzz/tests/__fuzz__/**` (extension-less corpus seeds crane would
  otherwise strip).

## Scope of "done"

- [ ] `fuzz/` workspace with committed `Cargo.lock` and `[profile.fuzz]`.
- [ ] `smoke.rs` target green under `cd fuzz && cargo test`.
- [ ] `mailbox.rs` model-based target green (TDD: a seeded failing case first).
- [ ] `bombay-fuzz-replay` wired into `nix flake check` (stable, hermetic).
- [ ] a small committed corpus under `fuzz/tests/__fuzz__/`.
- [ ] coverage-baseline note; no README change (internal test infra).

## Non-goals (owned by sibling cards)

- Nightly sancov / libFuzzer / AFL run — **#152**.
- MIRI leak/UB + counting-allocator exact-memory test — **#151**.
- loom/shuttle DST of the async run-loop concurrency — **#150**.
