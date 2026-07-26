# Audit request — mutants-gate fix on `feat/196-restart-supervision`

You are auditing a change another agent just made to the **Bombay** repo
(`~/Code/devrandom/bombay`, PR #200, branch `feat/196-restart-supervision`).
Do **not** trust this brief — verify every claim against the repo, `gh`, and
primary sources. Your job is to find where the fix is wrong, incomplete, or
deviates from project doctrine — not to ratify it.

## Ground rules (per your role)
- Facts only, no opinions. State trade-offs and uncertainty as facts.
- Cite primary sources for every behavioural claim:
  - cargo-mutants timeout/outcome semantics → the cargo-mutants book
    (`mutants.rs` / `github.com/sourcefrog/cargo-mutants`), not memory.
  - tokio timer/`start_paused` virtual-clock behaviour → `docs.rs/tokio`
    (`tokio::time::pause`/`advance`) and the tokio repo.
  - libtest parallelism (`--test-threads`, default = available parallelism)
    → the rustc/libtest source or the Rust book test chapter.
- Read the actual artifacts with `gh` and the working tree before judging.
  Never assume file contents.
- Use Mermaid where structure/flow/causation is clearer as a picture
  (a `flowchart` of the timeout causal chain; a `stateDiagram-v2` of a mutant's
  cargo-mutants outcome classification).

## What to inspect (verify, don't take on faith)
1. The failing run and the fix:
   - `gh pr view 200 --repo devrandom-labs/bombay`
   - `gh run view --repo devrandom-labs/bombay <the failed mutants job> --log`
     (find it via `gh run list --repo devrandom-labs/bombay --workflow mutants`)
   - `git show 644a67c` — the fix commit. Two files: `flake.nix`,
     `mutants-baseline.json`.
   - The gate logic it must satisfy: `mutants-gate/src/gate.rs`
     (`Failure::{Timeout,Unaccounted,Collapse,StaleFloor,InvalidFloor,
     MissingOutcomes,Survivor}`) and `emit_baseline`.
   - The test-timing constants: `bombay-core/src/test_support.rs`
     (`terminate_bound`) and `bombay-core/src/actor/spawn.rs`
     (`ON_STOP_NOTICE_GRACE`).

2. The claimed root cause (challenge it):
   - Claim A — the 5 **Timeout** failures (`stop`, `cancel_token`, `link_tx`,
     `recv`, `Capacity::get`) are NOT infinite hangs but **real-time
     accumulation**: a mutation suppresses graceful stop / death-watch, ~16
     multi_thread/current_thread lifecycle tests each wait `terminate_bound()`
     (15 s) for an effect that never arrives, and on a 4-core runner they
     serialize to ~60 s, grazing the old `--timeout 60`. (Local repro cited:
     `stop -> ()` → lib suite 60.03 s at `--test-threads=4`.)
   - Claim B — all 16 hanging tests already exist on `main`; the sweep on `main`
     was riding the ~60 s cliff and #196's additions tipped it over.
   - **Test these claims yourself.** Reproduce at least one: hand-apply a
     mutation (e.g. `ActorRef::stop` body → `()`), run
     `cargo nextest run -p bombay-core --no-fail-fast` under a short
     `slow-timeout … terminate-after`, and confirm which tests stall and
     whether any stall *forever* (a `start_paused` test with a `yield_now`
     busy-loop pinning virtual time so the wrapping `timeout` never fires) vs
     merely burn 15 s. If any hang is unbounded, raising the cap is the WRONG
     fix and the await must be made fail-fast instead — say so.

3. The two fixes — decide correct / band-aid / wrong:
   - **`--timeout 60 → 180`** in `flake.nix`. Is converting a legitimately-CAUGHT
     -but-slow mutant from Timeout→Caught defensible, or does it mask a real
     test-quality defect? Weigh against the project doctrine in `CLAUDE.md`
     (the #149/#164/#168 "green lanes over the wrong surface" lesson) and the
     recurring memory note "fix = bound the awaits, don't raise the cap".
     Does 180 scale, or just move the cliff? Quantify: worst-case per-mutant
     wall-time vs core count vs cap.
   - The rejected alternative: shrinking `terminate_bound()`. Confirm the stated
     blocker — that it is pinned at **3× `ON_STOP_NOTICE_GRACE`** and
     deliberately unequal to it (read the doc comment), and that
     `ON_STOP_NOTICE_GRACE` is a production constant. Is a **separate shorter
     bound** for prompt in-process effects (notice/idle-stop) the better fix the
     author skipped? Argue it with the actual call sites in `spawn.rs`.
   - **15 baseline entries** in `mutants-baseline.json` (8 floors @1, 7
     known-zero). For the 7 known-zero (`supervise`, `supervise_cloned`,
     `spawn_child`, `watch_reg`, `dispatch_death`, `startup_failed`,
     `watch_installer`): verify each is genuinely **all-mutants-unviable**
     (cargo-mutants viability is compile-based) and NOT a real coverage gap
     being laundered as known-zero (this is exactly the `emit_baseline`
     "operator reviews the diff" responsibility and the #168 audit's concern).
     For the 8 floors @1: is a floor of 1 meaningful, or should these functions
     carry more viable mutants than the sweep generated?

4. Plan vs deviations — reconstruct and report:
   - Implicit plan: diagnose the red sweep, fix to green, push. Enumerate every
     deviation from a disciplined path — e.g. hand-mutating source to reproduce
     (was it fully reverted? check `git status` / `git diff`), transcribing
     viable counts from a garbled interleaved CI log rather than regenerating
     the baseline via `emit_baseline` from a clean local sweep (accuracy risk),
     and NOT running the authoritative `nix build .#mutants` locally before
     pushing (verification deferred to CI).
   - Flag the one genuine bug caught mid-flight: the *staged* `flake.nix` had the
     new comment **inside** the `\`-continued `cargo mutants` command, which
     would have commented out `--output`/`--timeout`. Confirm the committed
     version fixed it (`git show 644a67c -- flake.nix`).

## Deliverable
A findings report, most-severe first. For each finding: the claim, the primary
source or repro that confirms/refutes it, and the concrete failure scenario if
it is wrong. End with a verdict on whether commit `644a67c` is a correct fix, a
correct-but-load-bearing band-aid (name the follow-up card it needs), or masks a
defect that will resurface — and, if the latter, the specific fail-fast change
that should replace the timeout bump. Confirm the CI mutants sweep on `644a67c`
(run id in `gh run list … --workflow mutants`) actually went green before
closing the audit.
