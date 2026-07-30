# 257 — injectable on_stop grace, kill the cfg!(miri) production fork

Full design + verbatim code blocks: `@docs/superpowers/plans/2026-07-30-injectable-on-stop-grace.md`
and `@docs/superpowers/specs/2026-07-30-injectable-on-stop-grace-design.md`. This file is the
executable checklist; when a step says "see plan", use the code block there verbatim.

## Context

- `crates/core/src/actor/spawn.rs:57` has `ON_STOP_NOTICE_GRACE: Duration = if cfg!(miri) { 10min } else { 5s }`.
  A `cfg!(miri)` fork in a production `const` — untestable branching, and the bound is un-tunable per actor.
- Card #257: make the grace per-spawn configurable via a new `SpawnConfig`, default 5 s, and DELETE the
  `cfg!(miri)` arm from production entirely.
- Invariants that MUST hold after the change:
  - Behavior at the default is unchanged — every actor still gets a 5 s `on_stop` grace.
  - Production source contains NO `cfg!(miri)` on the grace (grep-guard).
  - The two bounded-teardown sites (`finish_actor`, `teardown_children`) read the per-spawn grace, not a const.
  - `SpawnConfig` is NOT `#[non_exhaustive]` (repo phase rule). Grace is a plain `Duration` field — no Option/sentinel.
  - No bare arithmetic / `saturating_*` added. `thiserror` error model untouched (no new errors here).
- Reference oracle: kameo `v0.22.2` at `~/Code/devrandom/kameo` (read-only) — but this surface is bombay-specific.

## Steps

### Step 1 — SpawnConfig + rename const + PreparedActor field + migrate ALL call sites  [SEQUENTIAL — foundation]

Files: `crates/core/src/actor/spawn.rs`, `crates/core/src/actor/kind.rs`, `crates/core/src/actor/mod.rs`,
`crates/core/src/actor/timer.rs`, `crates/core/src/test_support.rs` (doc only),
`crates/core/tests/{invariants,control_lane,tracing_capture,dst_races,app_job_queue}.rs`,
`crates/core/benches/{registry_vs_kameo.rs,request_vs_kameo.rs}`.

1a. In `spawn.rs`, replace the whole `ON_STOP_NOTICE_GRACE` const (lines 45-71) with
`pub(super) const DEFAULT_ON_STOP_NOTICE_GRACE: Duration = Duration::from_secs(5);` plus the doc comment
(see plan Task 1 Step 1). NO `cfg!(miri)`.

1a-rename. The rename breaks every reference to the OLD name — they MUST all move to
`DEFAULT_ON_STOP_NOTICE_GRACE` in THIS step or nothing compiles (CHECK finding 1). This is a pure rename
here; Step 2 changes the READ, not the name. Rename at:
  - PRODUCTION uses, still reading the renamed const for now: `spawn.rs:486` (`timeout(...)`),
    `spawn.rs:498` (`trace::on_stop_abandoned`), `kind.rs:588` (`sleep(...)`), `kind.rs:594` + `kind.rs:608`
    (`trace::child_teardown_abandoned`), and the import `kind.rs:17`
    (`use crate::actor::spawn::ON_STOP_NOTICE_GRACE;` → `...::DEFAULT_ON_STOP_NOTICE_GRACE;`).
  - TEST-module imports + uses (CHECK finding 5): `spawn.rs:804` (`use super::{DEFAULT_MAILBOX_CAPACITY,
    ON_STOP_NOTICE_GRACE}` → add `SpawnConfig`, rename the grace), `spawn.rs:3536`
    (`use super::super::ON_STOP_NOTICE_GRACE`), and the two uses `spawn.rs:2796` + `spawn.rs:4959`
    (`ON_STOP_NOTICE_GRACE + Duration::from_secs(1)`).

1b. Add the `SpawnConfig` struct with `Default` (capacity = `default_capacity()`, grace =
`DEFAULT_ON_STOP_NOTICE_GRACE`) — see plan Task 1 Step 2. `#[derive(Debug, Clone)]`, both fields `pub`,
NOT `#[non_exhaustive]`.

1c. Add `on_stop_grace: Duration` field to `PreparedActor`. Change `new` and `new_linked` to take
`config: SpawnConfig` (use `config.capacity` for `Mailbox::bounded`, store `config.on_stop_grace`) —
see plan Task 1 Step 3. `finish_actor` / `teardown_children` bodies otherwise UNCHANGED in this step:
they keep reading the (now renamed) `DEFAULT_ON_STOP_NOTICE_GRACE` const, NOT the field. That leaves the
field stored-but-unused so Step 2 can flip const→field TDD-style. (The const's "read only by
`SpawnConfig::default`" is the END state, after Step 2 — not yet true here.)

1d. In `mod.rs`: re-export `SpawnConfig`; rewrite `spawn_with_capacity` → `spawn_with_config`,
`spawn_linked_with_capacity` → `spawn_linked_with_config`, `spawn_supervised_with_capacity` →
`spawn_supervised_with_config`; zero-arg `spawn`/`spawn_linked`/`spawn_supervised` use
`SpawnConfig::default()` — see plan Task 1 Step 4. KEEP `#[must_use]` on ALL SIX methods (CHECK finding 6 —
present today on `mod.rs:181,207,255` and the zero-arg forms; the plan's code blocks dropped it, re-add it).
Drop the now-unused `default_capacity` AND likely-unused `Capacity` imports from `mod.rs` (CHECK finding 9 —
the compiler will flag them).

1e. Migrate EVERY remaining call site. Do NOT trust a narrow regex — the CHECK found `new(default_cap())`
(8+ sites in `tracing_capture.rs`), `new_linked(Capacity::try_from(...))` (`app_job_queue.rs:439`), and the
benches, all missed by `new\(cap`. Prefer compiler-driven: after 1a-1d, run `cargo check -p bombay
--all-targets` and fix each error. The transformation is always: the OLD `Capacity`/`usize`-capacity arg
becomes `SpawnConfig { capacity: <that arg>, ..Default::default() }`. Discovery aid (superset):
`rg -n "::new\(|::new_linked\(|_with_capacity\(" crates/core`.

  DANGER — do NOT blanket-transform (CHECK finding 7): `timer.rs:463`
  `GatedSink::spawn_with_capacity(gate, capacity: usize)` is a LOCAL test helper, NOT the trait method
  (arg order `(gate, usize)`). Leave its NAME and signature alone; only its BODY changes — the `new(cap)`
  at `timer.rs:468` migrates to `new(SpawnConfig { capacity: cap, ..Default::default() })`.

1f. Doc rot from the rename (CHECK finding 8): fix the dangling intra-doc link `` [`ON_STOP_NOTICE_GRACE`] ``
at `spawn.rs:7` (module doc), `spawn.rs:42` ("tune with `spawn_with_capacity`" → `spawn_with_config`),
`test_support.rs:96` (references the old const name), and the test doc comments at `spawn.rs:2774,4511,4931`.

Expected: `cargo check -p bombay --all-targets` clean, no behavior change.

### Step 2 — thread the grace into finish_actor + teardown_children + run_lifecycle*  [SEQUENTIAL — depends on Step 1]

Files: `crates/core/src/actor/spawn.rs`, `crates/core/src/actor/kind.rs`.

2a. Add the honored-grace unit test `spawn_with_explicit_grace_observes_that_bound` to `spawn.rs`'s tests
module — verbatim from plan Task 2 Step 1 (uses a 50 ms `SMALL` grace, `start_paused`, asserts the notice
arrives inside `SMALL + 10ms`). Add `SpawnConfig` to the test module `use super::{...}`.

2b. Give `finish_actor` an `on_stop_grace: Duration` param; replace both `ON_STOP_NOTICE_GRACE` uses
(the `tokio::time::timeout(...)` at ~486 and `trace::on_stop_abandoned(...)` at ~498) with the param —
plan Task 2 Step 3.

2c. Thread `on_stop_grace` through `run_lifecycle`, `run_lifecycle_linked`, `run_lifecycle_supervised`,
and add `on_stop_grace` to the `Self { .. }` destructure in `PreparedActor::run` / `run_linked` /
`run_supervised`, passing it down — plan Task 2 Step 4. In the supervised path, pass it to the graceful
`finish_actor(...)` AND all three `teardown_children(...)` calls.

2d. Give `teardown_children` (kind.rs) an `on_stop_grace: Duration` param; replace the three
`DEFAULT_ON_STOP_NOTICE_GRACE` uses (~588 sleep, ~594 + ~608 trace calls — renamed in Step 1a) with the
param. The import was already renamed in Step 1a; after this step it is unused (both remaining reads become
the param) — remove `use crate::actor::spawn::DEFAULT_ON_STOP_NOTICE_GRACE;` if the compiler flags it.
`use core::time::Duration;` already exists at `kind.rs:4` (CHECK finding 10 — no-op). Update the
doc-comment references (`kind.rs:526,585`) to name the parameter.
  - IN-MODULE TEST CALLER (CHECK finding 3): `kind.rs`'s own test `teardown_children_cancels_and_aborts_live_ones`
    (~1661) calls `teardown_children(&mut children, &link_rx)` — add the third arg
    `DEFAULT_ON_STOP_NOTICE_GRACE` (or a small paused-clock value if the test cares) so it compiles.

Expected: crate compiles; the new test now passes (Claude verifies via the gate).

### Step 3 — source guard test  [SEQUENTIAL — depends on Step 1]

File: `crates/core/src/actor/spawn.rs` (tests module). Add `grace_const_has_no_miri_fork` verbatim from
plan Task 3 Step 1 (asserts the `DEFAULT_ON_STOP_NOTICE_GRACE` definition line has no `cfg!(miri)`).

### Step 4 — README

File: `README.md`. Replace the `spawn_with_capacity` family in the public-API section with the
`SpawnConfig` / `_with_config` surface; update any `PreparedActor::new(capacity)` example to
`PreparedActor::new(SpawnConfig { capacity, ..Default::default() })`; add one line that `on_stop_grace` is
per-spawn tunable — plan Task 6 Step 1.

NOTE: the MIRI-lane reconciliation (plan Task 4) and job-queue wiring (plan Task 5) are driven by CLAUDE
after the gate runs (they need test execution, which you must not do). Do NOT add `test_support::on_stop_grace()`
or touch the job-queue example unless a later fix round tells you to.

## Verification

You (K3) must NOT run tests — the sandbox hangs cargo test/nextest. Run ONLY:

```bash
cd /Users/joel/Code/devrandom/bombay
nix develop --command cargo check -p bombay --all-targets
nix develop --command cargo clippy -p bombay --all-targets
```

Both must be clean (clippy is deny-level; fix every warning on the items you touched, never `#[allow]`
without a `reason`). Run `cargo fmt` before finishing. The authoritative gate — `nix flake check`, the full
test suite, the MIRI lane, fail-first confirmation — is run by CLAUDE, not you.

## Out of scope

- No per-message or post-spawn dynamic grace. Grace is fixed at spawn.
- Do NOT touch a child's per-child `stop_grace` (cancel→abort) — distinct from this inside-out notice bound.
- No `test_support::on_stop_grace()` and no job-queue/example edits in this dispatch (Claude owns those,
  they are test-execution-gated).
- No commits (devrandom no-commit rule; Claude commits).
- No lint-level or `clippy.toml` changes.
