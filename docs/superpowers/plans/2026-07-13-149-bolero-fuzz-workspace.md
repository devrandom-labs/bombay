# #149 — Reusable bolero fuzz workspace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up an isolated `bolero` fuzz workspace whose `check!` targets run on stable inside `nix flake check` (deterministic replay + bounded-random), starting with a model-based differential over the sync mailbox state machine.

**Architecture:** A non-member `fuzz/` cargo workspace (own `Cargo.lock`, so its dep tree never touches the main crate's audit/deny surface) depends on `bombay-core` via a path dep. Targets live in `fuzz/tests/*.rs` and run under `cargo test`. A `bombay-fuzz-replay` crane check runs `(cd fuzz && cargo test)` on the pinned stable toolchain, vendoring the fuzz lock separately. Nightly sanitized fuzzing (#152) reuses the same targets via a CI env toolchain — deliberately no `fuzz/rust-toolchain.toml`.

**Tech Stack:** Rust (edition 2024, stable), `bolero` + `bolero-generator` (latest), crane/fenix Nix flake, `bombay-core::mailbox` public API.

**Spec:** `docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md`

**Facts locked (verified):**
- Root workspace members are explicit (`[".", "actors", "bombay-core", "console", "macros"]`) — a nested `fuzz/` with its own `[workspace]` is auto-excluded; no collision.
- Public API the target uses: `bombay_core::mailbox::{Capacity, Mailbox, Mailboxed, Signal, MailboxSender}`. `Capacity::MAX` (usize const), `Capacity::get`, `TryFrom<usize> for Capacity`. `bounded(MAX)` is safe (existing max-capacity test passes — flume does not preallocate).
- `Probe` in `mailbox.rs` is `#[cfg(test)]`, so the fuzz crate defines its own `Mailboxed` impl.
- No local cargo — all cargo runs via `nix develop --command`.

---

### Task 1: Scaffold the `fuzz/` workspace + smoke target

Proves the bolero-under-`cargo test` wiring before the real target. (The smoke target is a wiring proof, not a TDD subject — it cannot meaningfully fail once the crate compiles.)

**Files:**
- Create: `fuzz/Cargo.toml`
- Create: `fuzz/tests/smoke.rs`
- Create (generated): `fuzz/Cargo.lock`

- [ ] **Step 1: Create `fuzz/Cargo.toml`**

```toml
# Isolated fuzzing workspace. The empty [workspace] table makes this crate its
# OWN workspace root so the parent workspace (root Cargo.toml members) stays
# unchanged and the fuzzing dependency tree never enters the main crate's
# audit/deny/dev surface.
[workspace]

[package]
name = "bombay-fuzz"
version = "0.0.0"
edition = "2024"
publish = false
license = "MIT OR Apache-2.0"

[dependencies]
bombay-core = { path = "../bombay-core" }
# bolero + bolero-generator versions are filled by `cargo add` in Step 2
# (per the "use latest deps" rule); do not hand-pin here.

# bolero requires a `fuzz` profile to exist.
[profile.fuzz]
inherits = "dev"
opt-level = 3
incremental = false
codegen-units = 1
```

- [ ] **Step 2: Add latest bolero and generate the lock**

Run:
```bash
nix develop --command bash -c 'cd fuzz && cargo add bolero bolero-generator && cargo generate-lockfile'
```
Expected: `fuzz/Cargo.toml` now lists `bolero`/`bolero-generator` at their latest versions; `fuzz/Cargo.lock` is created. (If `cargo add` reorders the `[dependencies]` table, that is fine.)

- [ ] **Step 3: Create `fuzz/tests/smoke.rs`**

```rust
//! Smoke target: proves the bolero harness builds and runs under `cargo test`.

#[test]
fn smoke() {
    bolero::check!().for_each(|input: &[u8]| {
        // Trivial total function: can never panic. Confirms wiring only.
        let _ = input.len();
    });
}
```

- [ ] **Step 4: Run the smoke target**

Run:
```bash
nix develop --command bash -c 'cd fuzz && cargo test --test smoke'
```
Expected: PASS (`test smoke ... ok`). This confirms bolero's DefaultEngine runs under `cargo test` on stable.

- [ ] **Step 5: Verify the main gate is unaffected**

Run:
```bash
nix develop --command cargo metadata --format-version 1 --no-deps
```
Expected: the `packages` list contains `bombay-core`, `actors`, `console`, `macros`, and the root — but **NOT** `bombay-fuzz` (it is a separate workspace). Confirms no collision with the root workspace.

- [ ] **Step 6: Commit**

```bash
git add fuzz/Cargo.toml fuzz/Cargo.lock fuzz/tests/smoke.rs
git commit -m "test(fuzz): scaffold isolated bolero fuzz workspace + smoke target (#149)"
```

---

### Task 2: Model-based mailbox target

Differential fuzz of the **sync** mailbox surface (`try_send`/`drain`/clone/drop) against a `VecDeque` oracle. TDD rigor here is a falsifiability check: prove the target catches a discrepancy (temporarily break an assertion → bolero reports a counterexample), then restore.

**Files:**
- Create: `fuzz/tests/mailbox.rs`

- [ ] **Step 1: Write the target**

```rust
//! Model-based differential fuzz of the synchronous mailbox state machine.
//! Drives `try_send` / `drain` / clone / drop against a `VecDeque` oracle and
//! asserts FIFO + exactly-once + capacity backpressure. Sync-only, so it is
//! also the surface #151's MIRI job can run (MIRI cannot drive tokio).

use std::collections::VecDeque;

use bolero::{check, TypeGenerator};
use bombay_core::mailbox::{Capacity, Mailbox, MailboxSender, Mailboxed, Signal};

/// Fuzz-local actor. The mailbox is domain-agnostic, so a `u64` message is
/// enough (`Probe` in `mailbox.rs` is `#[cfg(test)]` and unreachable here).
struct Probe;
impl Mailboxed for Probe {
    type Msg = u64;
}

#[derive(Debug, TypeGenerator)]
enum Op {
    TrySend(u64),
    Drain,
    CloneTx,
    DropTx,
    IsClosed,
}

/// Map a fuzzer seed to a valid, mostly-small capacity so `try_send` actually
/// exercises the `Full` path, while keeping the `MAX`/`MAX-1` boundaries
/// reachable. `Capacity` rejects `0`, so the floor is `1`.
fn capacity_from_seed(seed: u16) -> Capacity {
    let value = match seed {
        0 => Capacity::MAX,            // upper boundary
        1 => Capacity::MAX - 1,        // MAX-1 boundary
        n => (usize::from(n) % 8) + 1, // 1..=8: small caps exercise `Full`
    };
    Capacity::try_from(value).expect("seed maps to a valid capacity")
}

fn message(msg: u64, tx: &MailboxSender<Probe>) -> Signal<Probe> {
    Signal::Message {
        msg,
        self_sender: tx.clone(),
    }
}

#[test]
fn mailbox_state_machine() {
    check!()
        .with_type::<(u16, Vec<Op>)>()
        .for_each(|(cap_seed, ops)| {
            let cap = capacity_from_seed(*cap_seed);
            let cap_n = cap.get();
            let (tx, mut rx) = Mailbox::<Probe>::bounded(cap);
            let mut senders: Vec<MailboxSender<Probe>> = vec![tx];
            let mut model: VecDeque<u64> = VecDeque::new();

            for op in ops {
                match op {
                    // rx is never dropped in this loop, so try_send can only
                    // fail with `Full` — never `Closed`.
                    Op::TrySend(m) => {
                        let Some(sender) = senders.first() else {
                            continue;
                        };
                        match sender.try_send(message(*m, sender)) {
                            Ok(()) => {
                                assert!(model.len() < cap_n, "accepted past capacity");
                                model.push_back(*m);
                            }
                            Err(_) => {
                                assert_eq!(model.len(), cap_n, "rejected below capacity");
                            }
                        }
                    }
                    Op::Drain => {
                        let drained: Vec<u64> = rx
                            .drain()
                            .map(|s| match s {
                                Signal::Message { msg, .. } => msg,
                                other => unreachable!("only Message enqueued, got {other:?}"),
                            })
                            .collect();
                        let expected: Vec<u64> = model.drain(..).collect();
                        assert_eq!(drained, expected, "drain must be FIFO + exactly-once");
                    }
                    Op::CloneTx => {
                        if let Some(sender) = senders.first() {
                            senders.push(sender.clone());
                        }
                    }
                    Op::DropTx => {
                        senders.pop();
                    }
                    Op::IsClosed => {
                        // Only the "open while a sender lives" direction is
                        // observable — once every sender is dropped there is no
                        // handle left to query.
                        if let Some(sender) = senders.first() {
                            assert!(!sender.is_closed(), "open while a sender lives");
                        }
                    }
                }
            }
        });
}
```

Note: `Signal` must be `Debug` for the `unreachable!("...{other:?}")` arm. It is (`#[derive(Debug)]` on the enum in `mailbox.rs`); if a future change removes that, replace the arm with `_ => unreachable!("only Message enqueued")`.

- [ ] **Step 2: Run the target — expect PASS**

Run:
```bash
nix develop --command bash -c 'cd fuzz && cargo test --test mailbox'
```
Expected: PASS (`test mailbox_state_machine ... ok`). The real mailbox satisfies the oracle across bolero's bounded-random inputs.

- [ ] **Step 3: Falsifiability check — break an assertion, watch bolero find a counterexample**

Temporarily change the `Drain` assertion to a wrong expectation:
```rust
// TEMP: reverse expected order to prove the target catches FIFO violations.
let expected: Vec<u64> = model.drain(..).rev().collect();
```
Run:
```bash
nix develop --command bash -c 'cd fuzz && cargo test --test mailbox'
```
Expected: FAIL — bolero reports a failing input (a sequence where drained ≠ reversed model). This proves the target actually discriminates. **Then revert the `.rev()`** and re-run Step 2 to confirm PASS.

- [ ] **Step 4: Commit**

```bash
git add fuzz/tests/mailbox.rs
git commit -m "test(fuzz): model-based differential target for the sync mailbox (#149)"
```

---

### Task 3: Commit a corpus seed directory

bolero stores per-target corpus under `fuzz/tests/__fuzz__/<target>/`. Committing seeds makes replay deterministic and gives the flake check a stable input set; the extension-less files must reach the Nix sandbox (Task 4 keeps them).

**Files:**
- Create: `fuzz/tests/__fuzz__/.gitkeep` (plus any seeds bolero writes)

- [ ] **Step 1: Generate corpus by running the targets once**

Run:
```bash
nix develop --command bash -c 'cd fuzz && cargo test'
```
Expected: PASS. bolero may create `fuzz/tests/__fuzz__/...` corpus/entries.

- [ ] **Step 2: Ensure the corpus directory is tracked even if empty**

Run:
```bash
mkdir -p fuzz/tests/__fuzz__ && touch fuzz/tests/__fuzz__/.gitkeep
```

- [ ] **Step 3: Confirm the fuzz workspace ignores build artifacts, not corpus**

Create `fuzz/.gitignore`:
```gitignore
/target
```

- [ ] **Step 4: Commit**

```bash
git add fuzz/tests/__fuzz__ fuzz/.gitignore
git commit -m "test(fuzz): commit corpus seed directory + fuzz .gitignore (#149)"
```

---

### Task 4: Wire `bombay-fuzz-replay` into the flake gate

Add a stable, hermetic crane check that runs `(cd fuzz && cargo test)`. The fuzz workspace's own lock is vendored separately; the corpus seeds are added to the sandboxed source.

**Files:**
- Modify: `flake.nix` (src fileset + a `let`-bound `fuzzCargoArtifacts` + a `checks.bombay-fuzz-replay` entry)

- [ ] **Step 1: Extend the `src` fileset to keep the corpus seeds**

In `flake.nix`, the `src = lib.fileset.toSource { ... }` block currently unions `commonCargoSources ./.`, `./README.md`, `./tests/features`. Add the corpus dir (extension-less files `commonCargoSources` would strip):
```nix
        src = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            (craneLib.fileset.commonCargoSources ./.)
            ./README.md
            ./tests/features
            ./fuzz/tests/__fuzz__
          ];
        };
```

- [ ] **Step 2: Vendor the fuzz workspace lock (in the same `let` block as `cargoArtifacts`)**

Immediately after the `cargoArtifacts = craneLib.buildDepsOnly commonArgs;` line, add:
```nix
        # The fuzz workspace has its OWN Cargo.lock (bolero + bombay-core path
        # dep). Vendor it separately so the replay check builds offline without
        # touching the root workspace's vendored deps.
        fuzzCargoArtifacts = craneLib.vendorCargoDeps { cargoLock = ./fuzz/Cargo.lock; };
```

- [ ] **Step 3: Add the `bombay-fuzz-replay` check**

Inside the `checks = { ... }` attrset (e.g. next to `bombay-nextest`), add:
```nix
          # Deterministic corpus-replay + bounded-random fuzz gate. Runs the
          # bolero `check!` targets in the isolated `fuzz/` workspace via plain
          # `cargo test` on the pinned STABLE toolchain (bolero's DefaultEngine
          # needs no nightly — sanitizers, which do, live in the #152 scheduled
          # workflow). `src` already carries the whole tree (parent crate + fuzz
          # sources + corpus); `fuzzCargoArtifacts` vendors the fuzz lock so the
          # build is fully offline/hermetic.
          bombay-fuzz-replay = craneLib.mkCargoDerivation (
            commonArgs
            // {
              cargoVendorDir = fuzzCargoArtifacts;
              cargoArtifacts = null;
              pnameSuffix = "-fuzz-replay";
              buildPhaseCargoCommand = ''
                (cd fuzz && cargo test --no-fail-fast)
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );
```

- [ ] **Step 4: Run just the new check**

Run:
```bash
nix build -L '.#checks.aarch64-darwin.bombay-fuzz-replay'
```
Expected: builds successfully (the bolero targets pass under the sandboxed stable toolchain). If it fails with a missing-corpus path error, confirm `fuzz/tests/__fuzz__` exists and Step 1 was applied.

- [ ] **Step 5: Commit**

```bash
git add flake.nix
git commit -m "ci(fuzz): gate bolero replay on stable via bombay-fuzz-replay (#149)"
```

---

### Task 5: Docs — coverage baseline note

No README change (internal test infra; matches #113–#117 precedent). Record the new workspace in the coverage baseline and mark the spec implemented.

**Files:**
- Modify: `docs/testing/coverage-baseline.md`
- Modify: `docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md:3` (status line)

- [ ] **Step 1: Append a `#149` section to `docs/testing/coverage-baseline.md`**

Add, after the `actor-ref` (#117) section:
```markdown
### `fuzz` — bolero workspace (#149) — done
Isolated non-member `fuzz/` workspace (crate `bombay-fuzz`, own `Cargo.lock`) —
the reusable verification backbone (#150/#151/#152 build on it). `bolero::check!`
targets run on **stable** via the `bombay-fuzz-replay` flake check
(`cd fuzz && cargo test`, DefaultEngine = deterministic corpus-replay +
bounded-random); nightly sanitized fuzzing is #152, quarantined to CI env (no
`fuzz/rust-toolchain.toml`).

Targets: `smoke` (wiring proof) and `mailbox_state_machine` — a model-based
differential over the **sync** mailbox surface (`try_send`/`drain`/clone/drop)
against a `VecDeque` oracle, asserting FIFO + exactly-once + capacity
backpressure. Sync-only so #151's MIRI job runs the same surface. Exact-memory /
leak assertion is deferred to #151's counting allocator, which plugs into this
same target.
```

- [ ] **Step 2: Update the spec status line**

Change `docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md` line 3 from `**Status:** Design approved (2026-07-13). Sub-task of #117.` to `**Status:** Implemented (2026-07-13). Sub-task of #117.`

- [ ] **Step 3: Commit**

```bash
git add docs/testing/coverage-baseline.md docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md
git commit -m "docs(testing): record #149 bolero fuzz workspace in coverage baseline (#149)"
```

---

### Task 6: Full gate + PR

**Files:** none (verification + PR)

- [ ] **Step 1: Run the full gate**

Run:
```bash
nix flake check
```
Expected: all checks ✅ including `bombay-fuzz-replay`, `bombay-nextest`, `bombay-clippy`, `bombay-fmt`, `bombay-deny`, `bombay-audit`. If `bombay-deny`/`bombay-audit` flag a bolero transitive dep, note it — those run over the root workspace `src` only, which excludes the fuzz lock, so they should be unaffected; investigate before overriding.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin core/149-bolero-fuzz-workspace
```

- [ ] **Step 3: Open the PR (references #117 via #149; does not close #117)**

```bash
gh pr create --repo devrandom-labs/bombay --base main --head core/149-bolero-fuzz-workspace \
  --title "test(fuzz): reusable bolero fuzz workspace + mailbox replay in the gate (#149)" \
  --body "Implements #149 (sub-task of #117). Isolated \`fuzz/\` bolero workspace; \`bombay-fuzz-replay\` runs the \`check!\` targets on stable inside \`nix flake check\`. First target is a model-based differential over the sync mailbox state machine (FIFO + exactly-once + capacity backpressure). Backbone for #150/#151/#152. No README change (internal test infra). Spec: docs/superpowers/specs/2026-07-13-149-bolero-fuzz-workspace-design.md"
```

---

## Self-Review

**Spec coverage:**
- Isolated `fuzz/` workspace + own lock + `[profile.fuzz]` → Task 1. ✓
- `bolero::check!` targets over the sync mailbox state machine → Task 2. ✓
- Structured generators (not `&[u8]`) → `#[derive(TypeGenerator)] enum Op` + `with_type` → Task 2. ✓
- Model-based oracle (FIFO/exactly-once/capacity) → Task 2. ✓
- Capacity boundaries `1`/`MAX-1`/`MAX` → `capacity_from_seed` → Task 2. ✓
- In-gate `bombay-fuzz-replay` on stable, vendored separately → Task 4. ✓
- Corpus under `fuzz/tests/__fuzz__` kept by the flake fileset → Task 3 + Task 4 Step 1. ✓
- No `fuzz/rust-toolchain.toml` → Task 1 (absent by construction). ✓
- Leak/exact-memory deferred to #151 → noted in Task 5 doc + spec. ✓
- No README change → Task 5. ✓

**Placeholder scan:** No TBD/TODO; all code and commands are concrete. bolero version is intentionally resolved by `cargo add` (Task 1 Step 2) per the "use latest deps" rule — the one deliberately-late binding, verified green in Task 4/6.

**Type consistency:** `Op` variants (`TrySend`/`Drain`/`CloneTx`/`DropTx`/`IsClosed`) are used identically in the match. `capacity_from_seed(u16) -> Capacity`, `message(u64, &MailboxSender<Probe>) -> Signal<Probe>`, and `Mailbox::<Probe>::bounded(cap)` signatures match the `bombay_core::mailbox` public API confirmed above. `with_type::<(u16, Vec<Op>)>()` matches the `|(cap_seed, ops)|` closure binding.
