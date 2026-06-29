# Core cucumber wiring — design (card #77)

> **Phase 3 of epic #74, the core slice.** Wire `tests/features/core/*.feature` (+
> `*.properties.feature`) — 309 scenarios across 24 files / 12 modules — to the real kameo
> core SUT, reusing the cucumber harness card #76 (PR #81) established for the console. No
> production source changes: this is additive test plumbing only. The one exception is a
> single one-line `@bug` tag correction in a feature file (§3), to honour the project's own
> properties rule.

**Done when:** every non-`@bug` core scenario is GREEN under `nix flake check`; the
`@bug:id.rs:140-143` / `@bug:id.rs:218-221` probes are RED (compile + fail via the
`from_bytes` panic), captured so the gate stays green.

**Card:** #77 (`test(core): wire core feature scenarios to the SUT`), milestone M1.
**Predecessor:** #76 console wiring — `tests/console_wire_bdd.rs`, `console/tests/steps/`,
and the pattern doc `docs/testing/README.md` "Wiring (Phase 3)".

---

## 1. Scope

| | |
|---|---|
| In scope | Step definitions + a World + runners binding all 24 `core/` feature files to the live `kameo` core; `#[should_panic]` probes for the `from_bytes` defect; one `@bug` tag correction. |
| Out of scope | Fixing the `from_bytes` panic (separate `fix(actor_id)` card). Touching any other `src/`. `loom` instrumentation (deferred follow-up card, §4). The `actors`/`console` slices (#76 done; `actors` is a later card). |

Counts (verified from the tree): actor_id 17+6, actor_lifecycle 23+6, actor_ref 22+5,
error 20+4, links 17+4, mailbox 35+7, message 17+5, registry 20+3, reply 22+5,
request_ask 23+4, request_tell 16+4, supervision 21+3 → **309 scenarios**.

## 2. Architecture & layout

Per-module World, one shared step directory, one runner per feature file — the console
pattern, scaled. Each module is a self-contained unit: its own World type, its own step
module, its own runners; understandable and testable in isolation, and assignable to one
implementer subagent.

```
tests/
  core_steps/
    mod.rs                     pub mod actor_id; pub mod mailbox; … (12 modules)
    actor_id.rs                ActorIdWorld + steps + proptest laws + helpers/oracles
    error.rs   message.rs   reply.rs
    actor_ref.rs   actor_lifecycle.rs   request_ask.rs   request_tell.rs
    mailbox.rs   registry.rs   links.rs
    supervision.rs
  core_actor_id_bdd.rs         runner → core/actor_id.feature  (+ the 3 #[should_panic] probes)
  core_actor_id_props_bdd.rs   runner → core/actor_id.properties.feature
  … one runner file per feature file (~24 total)
```

- **Runner shape (per the #76 lessons, verbatim):** a standard
  `#[tokio::test(flavor = "multi_thread")]` — NOT `harness = false` (cucumber 0.23's
  libtest writer doesn't implement nextest's `--list`, and the gate runs `cargoNextest`).
  Body:
  ```rust
  World::cucumber()
      .fail_on_skipped()                 // any unwired/undefined scenario → hard failure
      .with_default_cli()                // stop cucumber parsing nextest's argv
      .filter_run_and_exit(
          concat!(env!("CARGO_MANIFEST_DIR"), "/tests/features/core/<file>.feature"),
          |_, _, sc| !is_bug(sc),        // exclude @bug scenarios from the green run
      )
      .await;
  ```
  Path anchored to `CARGO_MANIFEST_DIR` (nextest's cwd under the nix sandbox isn't the
  workspace root). Each `[[test]]` target carries `required-features = ["testing"]`.
- **Step modules are shared, not test targets.** Files under `tests/core_steps/` are pulled
  into each runner with `mod core_steps;`. Cargo only treats top-level `tests/*.rs` as test
  binaries, so the step files compile into the runners that include them, exactly like
  `console/tests/steps/`. Each runner includes only the module(s) it needs.
- **One World per module, not one mega-World.** `#[given]`/`#[when]`/`#[then]` bind to the
  World type in their first argument, and cucumber re-`default()`s the World per scenario, so
  there is no state bleed. A per-module World keeps each struct focused (a handful of fields
  for that domain) instead of a 40-field grab-bag mixing unrelated domains.

### SUT access — extend the existing `testing` surface

The root crate already has `#[cfg(any(test, feature = "testing"))] pub mod testing`
(console-only today) and a self dev-dep that auto-activates `testing` for the crate's own
test builds. We **extend** that module to re-export the private core items each module needs
(e.g. `ActorId::{new, generate, from_bytes, to_bytes}`, the `ActorIdFromBytesError` variants,
mailbox/links internals as required). Items being re-exported must be `pub` (not
`pub(crate)`) with their **module kept private** — `pub use` cannot widen `pub(crate)` out of
the crate (the #76 rule). The exact re-export list is discovered per module during
implementation (a private-item audit is the first step of each module's task).

No new bootstrap: `cucumber`/`proptest`/`rstest` are already workspace dev-deps, the
`testing` feature exists, and `flake.nix`'s `src` fileset already unions `tests/features`.

## 3. `@bug` probes and the latent-red scenario

The console slice had **zero** `@bug` scenarios, so the "keep a probe RED while the gate is
GREEN" problem is solved here for the first time.

**Mechanism (chosen): `#[should_panic]` demonstrators.**
cucumber catches panics inside steps and turns them into failed scenarios (not process
panics), so a `#[should_panic]` *cucumber* runner can't work. Instead the 3 `actor_id`
`@bug` scenarios are mechanized as plain Rust tests in `core_actor_id_bdd.rs` that call the
SUT directly:

```rust
#[tokio::test]
#[should_panic(expected = "out of range for slice")]
async fn bug_id_140_from_bytes_panics_on_short_slice() {
    // RED probe: passes ONLY while the from_bytes defect lives (id.rs:140-143).
    // Flips red the moment fix(actor_id) lands → delete this + un-exclude the
    // Err-asserting scenario in actor_id.feature.
    let _ = kameo::actor::ActorId::from_bytes(&[0u8; 4]);
}
```

(One each for the `< 8` slice panic, the empty-slice panic, and the serde
`visit_bytes`/`Deserialize` too-short path → `id.rs:218-221`. Exact panic substrings and the
serde call are confirmed against source during implementation.)

- This runs under the gate, is GREEN today because the panic fires, and flips RED on fix —
  a self-policing probe that exercises the real defect every CI run.
- The cucumber `@bug` scenarios (which assert the *desired* `Err`) are **excluded from the
  green runner** by the filter predicate, so `fail_on_skipped` never flags them. They remain
  in the feature file as the spec of intended behaviour. `is_bug(sc)` matches any tag
  starting `bug` (covers `@bug`, `@bug:id.rs:140-143`, `@bug:id.rs:218-221`).

**Latent-red correction.** `actor_id.properties.feature`'s `@property @boundary` law
*"from_bytes rejects any byte string shorter than eight bytes"* asserts
`Err(MissingSequenceID)` — the universally-quantified form of the same defect — but is **not**
`@bug`-tagged, so wired naïvely it panics and breaks the gate. `docs/testing/properties.md`
(lines 47–48) is explicit: *"A property that exposes a Phase-1 `@bug` … carries the same
`@bug:<file:line>` and must fail today."* **Fix:** add `@bug:id.rs:140-143` to that one
scenario — a one-line `.feature` tag correction matching the project's own rule — so it is
filtered uniformly, and cover its panic with a `#[should_panic]` property demonstrator in the
props runner. This is the only feature-file edit in the card.

**Standing rule for implementers.** If any *non-`@bug`* scenario cannot go green because it
documents desired-but-absent behaviour, **stop and surface it** — never silently rewrite the
assertion to match buggy current behaviour (CLAUDE.md rules 0 + 8). That is how any further
latent-red beyond the one above gets caught. The reviewer's mutation pass is the backstop.

## 4. Testing methodology

- **Pure `@property` laws** (ActorId byte round-trips; `error` `Display`/round-trips; `reply`
  constructors; any pure formatter): real synchronous `proptest!` with boundary-biased
  generators that hit the values named in each `# GEN:` line (`0, 1, MAX-1, MAX`; empty/max
  strings), asserting the `# ORACLE:` predicate. Real proptest gives shrinking; the SUT is
  pure so no async bridge is needed.
- **`@linearizability` / `@model`:** real overlap — `tokio::spawn` + `Arc<Barrier>` so
  operations genuinely interleave — against the actual std-atomic SUT, asserted against an
  **independent** reference oracle cited in-step (e.g. concurrent `generate()` → the set of
  assigned ids must equal the contiguous range `[N0, N0+k)` with no gaps/repeats; a torn
  `fetch_add` fails this on essentially any scheduling). The oracle must NOT call the SUT.
- **`@property` over async / process-global state:** a documented deterministic boundary-loop
  over the `# GEN:` values plus a handful of seeded (LCG) op sequences asserting the same
  oracle each iteration — the console fallback, used because `proptest!` is synchronous and
  the async + global-state bridge is fragile. Stated explicitly in the step, never silently
  narrowed.
- **`@timing` / `@unstable-clock`:** `tokio::time::pause()` / `advance()`. Never assert
  wall-clock (`SystemTime`) ordering — only monotonic `Instant`/uptime. Actor-stop
  assertions use condition-based polling of the observable state, NOT `wait_for_shutdown()`
  (which returns when the mailbox closes, before the stop is observable — the #76 flake).
- **No `loom` in this card.** loom only exercises real interleavings if `src/` is compiled
  against `loom::sync` under `#[cfg(loom)]`; kameo's source has no such instrumentation, and
  adding it is an out-of-scope production change against code M1 will hard-fork. A loom test
  would otherwise exercise a hand-built model, not the SUT (rule 8). **Follow-up:** a small
  separate card instruments just `id.rs` under `#[cfg(loom)]` and adds a loom-gated test kept
  out of the default `nix flake check`.

## 5. Execution plan

One branch `test/77-core-wiring`; one commit per module (`test(core): …`, `README.md` staged
each commit per the pre-commit hook); one PR when the whole core slice is green. The card is
moved to In Progress on project #4 (devrandom-labs), and to Done / PR-opened when green.

**Subagent-driven, one module per implementer, easiest → hardest** (re-prove the pattern on
self-contained modules, end on the most complex):

1. **actor_id** — also lands the shared skeleton (`core_steps/mod.rs`, first runners,
   `testing` re-exports) and the `@bug` mechanism + tag correction.
2. **error**, **message**, **reply** — largely pure / local.
3. **actor_ref**, **actor_lifecycle**, **request_ask**, **request_tell** — real spawned actors.
4. **mailbox**, **registry**, **links** — stateful / concurrent.
5. **supervision** — the hardest (restart strategies, links, panics).

Each module's task: (a) audit the private items it needs and extend `testing`; (b) write the
World + step defs, watch the targeted scenarios fail, then wire to green under
`nix develop -c cargo test --test <runner>`; (c) drop any name/tag filter on the last runner
for that file so `fail_on_skipped` covers the whole file; (d) an **independent reviewer
subagent applies mutation testing** — break each assertion and confirm the test fails — before
the module's commit (this caught real issues on #76). `nix flake check` is the single gate;
no `#[ignore]` on any non-`@bug` scenario; determinism over real sleeps.

The per-module step regexes, World fields, exact `testing` re-exports, and oracle code are
detailed in the implementation plan (writing-plans, the next step).

## 6. Risks

- **Scale (309 scenarios).** Mitigated by per-module isolation + one subagent per module +
  mutation-testing review per module.
- **Further latent-red scenarios** beyond the one found. Mitigated by the standing
  stop-and-surface rule and the mutation pass.
- **Shared edits to `Cargo.toml` / `lib.rs testing`** across modules. Sequential
  subagent-driven execution (current session) avoids merge races; each task appends.
- **CI-only flakes** (sandbox cwd, clock, scheduling). Mitigated by the #76 lessons baked
  into §4 (anchored paths, monotonic-only assertions, condition-based waiting); reproduce any
  flake from the `-L` logs, never guess.
