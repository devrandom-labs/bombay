# Reproducible mutation gate that cannot lie (#171 + #165)

**Cards:** [#171](https://github.com/devrandom-labs/bombay/issues/171) (no standing gate; #148's whole-package claim came from an interrupted 141/205 run) + [#165](https://github.com/devrandom-labs/bombay/issues/165) (the gate is vacuously green — nothing reports the viable-mutant ratio, so a collapse is invisible). Shipped as **one PR** closing both.

**Precondition (met):** #179 (PR #180, `193ee4c`) made the whole-package sweep actually exit 0 — it was exiting 3 on 3 missed + 5 timeouts, and every prior "0 survivors" line was false. A standing gate could not have gone legitimately green before that.

## Problem

Two failure modes, one root cause — *a green lane over the wrong surface is indistinguishable from a green lane over the right one*:

1. **Nothing re-runs the sweep.** `packages.mutants` exists under `packages`, never under `checks`; `rg -i mutants .github/` is empty. Every "0 survivors" in the repo is a point-in-time PR-body assertion, not a standing property. `mutants.out/` is gitignored scratch, so no artifact of a *complete* whole-package run exists anywhere.
2. **The metric is silent about its own reach.** cargo-mutants replaces a fn body with `Default::default()`. `Capacity` (rejects 0 — nexus contract), `ActorId`, `RunResult`, `ActorRef` have **no `Default`, correctly** — so most mutants fail to build and are counted `Unviable`, exiting 0. `spawn.rs` is **2 viable / 29**; `WeakActorRef::with_sender` is **0 viable / 4**. "Zero survivors over 4 viable" renders identically to "zero survivors over 200". The type discipline that prevents bugs is exactly what blinds the tool measuring bug-catching.

**Non-goal (explicit):** do not add `Default` impls to inflate viability. That trades real safety for a metric. The fix is to make the metric *honest about its reach*.

## Architecture

One cohesive Rust tool owns the verdict; the flake runs it; the workflow just triggers the flake. Full reproducibility: `nix build .#mutants` is the single entry point that fails on survivor, timeout, incompleteness, or viability collapse.

```
nix build .#mutants                          (nightly workflow: nix build .#mutants -L)
  └─ packages.mutants derivation (flake.nix)
       ├─ cargo mutants --package bombay-core --package bombay_macros
       │      --file 'bombay-core/**' --file 'macros/src/derive_msg.rs'
       │      --no-shuffle --colors never --timeout 60 --output "$out"  || true
       └─ cargo run -p mutants-gate --                 ← owns pass/fail
              --outcomes   "$out/mutants.out/outcomes.json"
              --candidates "$out/mutants.out/mutants.json"
              --baseline   mutants-baseline.json
```

cargo-mutants' own exit is swallowed (`|| true`) so **`mutants-gate` is the single source of truth** and always prints the ratio before deciding. It fails on **any** of:

1. **Incompleteness** — `outcomes.json` count `<` `mutants.json` candidate count. Directly kills #148's "141/205 interrupted, narrated as whole-package": a partial run can no longer pass.
2. **Survivor** — any outcome with `summary == MissedMutant`.
3. **Timeout** — any `summary == Timeout`. (`--timeout 60` per #179's lesson: cargo-mutants' auto-20s yields *false* timeouts under CI core-contention; single-threaded the suite finishes in ~0.2s. #179 already bounded the hang-prone awaits, so the clean sweep had 0 timeouts.)
4. **Viability collapse** — any floored `file::function` whose viable count (`caught + missed + timeout` = `total − unviable`) drops below its recorded floor.

### `mutants-gate` crate

New workspace member (a non-`.`-root crate so it is *not* itself in the sweep's `--package` set). Pure and unit-testable: it reads two JSON files and a baseline, returns a verdict. No cargo-mutants invocation inside it — that stays in the derivation, keeping the tool a deterministic function of its inputs.

- **Ratchet key is `file::function`, not file.** `with_sender`'s 0 viable would otherwise average into `actor_ref.rs`'s 30 mutants and vanish — the exact "averaged away" blindness #168 flagged. Per-function keying makes each of the six #148 functions individually visible and individually floored. `outcomes.json` carries `scenario.function.function_name` and the file path (schema **verified empirically as implementation step 1**, not assumed — see below).
- **Report is always printed, pass or fail:** overall `N viable / M total`, then a per-function table (viable/total/caught/unviable) sorted so 0-viable functions are conspicuous, not buried.

### `mutants-baseline.json` (committed data)

```jsonc
{
  "floors": {                       // the ratchet: file::fn -> min viable count (>=1)
    "bombay-core/src/actor/spawn.rs::<fn>": 2,
    "bombay-core/src/actor/kind.rs::<fn>":  2
    // ... every function with >=1 viable mutant, from the first reproducible sweep
  },
  "known_zero_viable": [            // documented structural blind spots, asserted == 0
    "bombay-core/src/actor/actor_ref.rs::with_sender",   // pure field-copy, ActorRef has no Default
    "bombay-core/src/message.rs::*"                       // trait-const-only: cargo-mutants emits 0 mutants
  ]
}
```

- Floors are populated from the **first reproducible sweep** (implementation), not invented here. Known anchors from measured evidence: `spawn.rs` 2, `kind.rs` 2, overall 64 viable / 210 total (#179 clean sweep).
- **A source function present in `outcomes.json` but absent from both `floors` and `known_zero_viable` is a hard error**, not a silent pass — so a newly-added 0-viable surface cannot sneak in unnoticed. This is what makes the metric honest about its reach: the baseline must *account for* every function the sweep saw.
- `known_zero_viable` entries are asserted to be exactly 0 viable (if one later becomes viable that is fine and ignored; if a *floored* function collapses to 0 that fails). This documents `with_sender`/`message.rs` as *deliberately* unreachable-by-mutation rather than silently uncovered.

### `with_sender` hand-written test (bombay-core)

Mutation structurally cannot reach `with_sender` (0 viable), yet it is the upgrade path behind `WeakActorRef::upgrade` and the self-ref construction in `kind.rs:40`. A wrong-`id` or stale-`cancel`/`abort` copy would be invisible. TDD a test asserting `with_sender` preserves `id`, `cancel`, and `abort` — the compensating control the baseline's `known_zero_viable` entry points at.

### Nightly workflow `.github/workflows/mutants.yml`

Mirrors `miri.yml` / `fuzz.yml`: `schedule` (nightly, staggered off the existing 03:00/04:00 UTC lanes) + `pull_request` + `workflow_dispatch`. Runs `nix build .#mutants -L`, echoes the ratio to `$GITHUB_STEP_SUMMARY` (so "0 survivors" stops being a PR-body assertion), single stable `mutants-gate` check name via an `always()` gate job — same pattern as `miri-gate`/`fuzz-gate`.

### ADR-0006

Records the three decisions so none is folklore: (a) **viable-count ratchet, not unviable-%** — a ratio threshold flags `spawn.rs`'s correct 93%-unviable forever or is too loose to fire; (b) **floor as committed data**, baseline accounts for every function; (c) **quarantined from `nix flake check`** — rebuild-and-test once per mutant is minutes-to-hours, same rationale as MIRI/fuzz; it is a flake *package* run nightly, not a per-push *check*.

## Testing (TDD)

- `mutants-gate` unit tests over **fixture** `outcomes.json` / `mutants.json` / baseline pairs — each verdict path fails when it should and only when it should: clean pass, survivor, timeout, incomplete run (outcomes < candidates), viability collapse (floored fn below floor), unaccounted new function. Fixtures are trimmed real cargo-mutants output.
- `with_sender` preserves `id`/`cancel`/`abort` (bombay-core, written failing first).
- The workflow's gate-collapse bash mirrors the verified `miri-gate` idiom.

## Card-bullet map (close COMPLETED only when each is shipped or deferred to a named card)

**#171:** scheduled workflow fails red on survivor/timeout → `mutants.yml` + `mutants-gate`. Not in `nix flake check`; decision recorded → ADR-0006 + flake comment. mailbox timeouts pass/excluded-with-reason → #179 bounded them (0 timeouts) + `--timeout 60`; `.cargo/mutants.toml` already excludes `log_on_stop_outcome`. Result published → job summary ratio. Correct #157's whole-package claim → comment on #148/PR#157 (#148 stays closed) + fix `coverage-baseline.md`. Pairs with #165 → same PR.

**#165:** post-process `outcomes.json`, fail on viable collapse → `mutants-gate` per-fn ratchet. Report ratio regardless of pass/fail, incl. `coverage-baseline.md` → tool prints; doc updated. Audit #148's claim, record actual viable count → PR body + doc. Non-gating if slow → ADR-0006. Per-fn report covers the six #148 fns individually → per-function keying. `with_sender` field-copy test → bombay-core. `message.rs` zero-by-construction → `known_zero_viable`. `derive_msg.rs` outside sweep → **extended to `bombay_macros` via `--file`** (only `derive_msg.rs`, not the vendored kameo derives).

## Verify-first (implementation step 1, per #165's "verify against the actual binary")

Against **cargo-mutants 27.0.0 in the devshell**, empirically confirm before writing the tool:
1. `outcomes.json` shape — top-level key, per-outcome `summary` variants, and where file + `function_name` live.
2. `mutants.json` candidate-list shape for the completeness count.
3. That `--package X --package Y --file globA --file globB` composes as "build/test both packages, mutate only matching files" (so vendored `bombay_macros` derives are excluded). If it does not, fall back to per-file scoping that achieves the same set.

## Out of scope

- `--package bombay-core` → `--workspace` (would mutate the vendored kameo root `.`, `actors`, `console` — M7-doomed). Explicit two-package scope instead.
- #164 (a loop probe cargo-mutants structurally cannot provide) — separate card.
- #170 (derive compile-fail doctests never run in-gate) — separate; named here only as `derive_msg.rs`'s *other* compensating control alongside the now-added macro mutation.
