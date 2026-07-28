# 226-pipe-to-self — pipe_to_self + pipe_ask (card #226)

## Context

Full task-by-task plan WITH COMPLETE CODE lives in
`docs/superpowers/plans/2026-07-28-226-pipe-to-self.md` — READ IT FIRST; this
file only adds dispatch structure (ordering, parallelism, verification,
boundaries). Design spec: `docs/superpowers/specs/2026-07-28-226-pipe-to-self-design.md`.
ADR to write: 0017 (follow 0016's shape).

Invariants that must hold (spec §Semantics, §Mechanism):
- The detached pipe task captures ONLY `WeakActorRef<A>` while pending — a
  strong ref may exist ONLY between upgrade and send-completion.
- Mapper output is `A::Msg` (closed menu); no erased path, no `Box<dyn Fn>`.
- Piped-future panic → `Err(PanicError)` with NEW `PanicReason::PipedFuture`;
  `is_lifecycle_hook()` must be `false` for it (flip the predicate to a
  POSITIVE match — the current `!matches!(HandlerPanic)` silently misclassifies
  any new variant).
- Dead/drain-window target at resolution → result dropped, task exits clean.
- Closed-mailbox tell after upgrade (kill race) → swallowed, never a panic.
- `PipeAskError::flatten` is variant-lossless (each `AskError`/`TellError`
  variant → distinct flat variant); undelivered-msg payload drop is documented,
  not silent.
- Every test await is BOUNDED (`tokio::time::timeout(terminate_bound(), ..)`)
  — an unbounded await is a cargo-mutants timeout factory.
- All `use` imports at top of file; thiserror for the new enum; no lint
  relaxation, no `#[allow]` without `reason`.

## Steps

Numbering = Task N in the detailed plan (each has exact code there).

1. `PanicReason::PipedFuture` + positive-match `is_lifecycle_hook` + repo-wide
   `rg "PanicReason::"` sweep. Files: `crates/core/src/error.rs`.
   **PARALLEL OK** (disjoint from step 2). Verify:
   `cargo nextest run -p bombay panic_reason`
2. Trace breadcrumbs `pipe_mapper_panicked` / `pipe_result_dropped`, BOTH cfg
   halves. Files: `crates/core/src/trace.rs`. **PARALLEL OK** (disjoint from
   step 1). Verify: `cargo check -p bombay && cargo check -p bombay --features tracing`
3. Create `crates/core/src/actor/pipe.rs` (verb `pipe_to_self` + `pub(crate)
   spawn_pipe`) + `mod pipe;` in `actor/mod.rs` + round-trip test.
   **SEQUENTIAL — depends on steps 1, 2.** Verify:
   `cargo nextest run -p bombay piped_future_result`
4. Panic-surfacing test (+ falsifiability probe: mutate reason variant, watch
   fail, revert). **SEQUENTIAL — depends on 3** (same file). Verify:
   `cargo nextest run -p bombay piped_panic_reaches`
5. Non-pinning test (+ probe: strong-capture mutation must fail it).
   **SEQUENTIAL — depends on 4.** Verify:
   `cargo nextest run -p bombay in_flight_pipe_does_not_pin`
6. Dead-before-resolution + kill-race tests (ExitGuard drop-oneshot
   observation). **SEQUENTIAL — depends on 5.** Verify:
   `cargo nextest run -p bombay -E 'test(actor_dead_before) or test(pipe_resolving_into_killed)'`
7. Liveness-overlap test (gated responder; check `DEFAULT_ASK_TIMEOUT` vs
   `terminate_bound()` — use `.no_timeout()` on the inner ask if needed).
   **SEQUENTIAL — depends on 6.** Verify:
   `cargo nextest run -p bombay actor_keeps_processing`
8. `PipeAskError` enum (error.rs) + `flatten` + `pipe_ask` verb + flat-Ok
   round-trip test. Bounds: `E: Send + 'static`, `R: Send + 'static` — nothing
   stricter (`ReplySender`/`ask` impose no `E` bound; pre-flight CHECK verified).
   **SEQUENTIAL — depends on 7** (pipe.rs + error.rs both touched by earlier
   steps). Verify: `cargo nextest run -p bombay pipe_ask_delivers_flat_ok`
9. Flatten-lossless pure-fn test (ALL arms) + e2e dead-target + e2e
   handler-error arms (timeout arms covered by pure-fn test BY DESIGN — do not
   write sleep-based e2e timeout tests). **SEQUENTIAL — depends on 8.**
   Verify: `cargo nextest run -p bombay -E 'test(pipe) or test(flatten) or test(piped)'`
10. Sugar-inherits-non-pinning delegation test. **SEQUENTIAL — depends on 9.**
    Verify: `cargo nextest run -p bombay in_flight_pipe_ask`
11. Ban-text doc pointers in `request.rs` + `actor_ref.rs` ask docs.
    **PARALLEL OK with 12** (disjoint files, both after 10). Verify:
    `cargo doc -p bombay --no-deps` (no broken intra-doc links)
12. ADR `docs/adr/0017-pipe-to-self-not-reentrancy.md` — transcribe spec
    (options A/B/C, full fate table + pipe_ask rows, PipedFuture flip).
    **PARALLEL OK with 11.**
13. mutants-baseline.json floors for new fns (`cargo mutants --list` for exact
    keys; scoped sweep `cargo mutants -p bombay -f crates/core/src/actor/pipe.rs
    --timeout 60` must be 0 missed/0 timeout; also re-check the
    `is_lifecycle_hook` floor). **SEQUENTIAL — depends on 11, 12.**
14. README public-API bullet + `docs/testing/coverage-baseline.md` rows.
    **SEQUENTIAL — depends on 13.**
15. `git add` EVERYTHING (untracked files are invisible to the flake), then
    `nix flake check`. **SEQUENTIAL — last.**

Commit after each step with the message given in the detailed plan (NO
Claude/AI attribution trailers — repo rule).

## Verification

- Per-step commands above (run inside `nix develop --command ...`).
- Final gate: `nix flake check` — must be green with everything tracked.
- `cargo fmt` before every commit (fmt gate is strict).

## Out of scope

- NO changes to `kind.rs` / `spawn.rs` run-loop (rejected design B).
- NO new public items beyond: `pipe_to_self`, `pipe_ask`, `PipeAskError`,
  `PanicReason::PipedFuture`. `spawn_pipe` stays `pub(crate)`.
- NO trait abstraction over the primitive (that's #223's second-use call).
- NO clippy.toml / `[lints]` edits. NO `cargo hakari` (not wired here).
- NO touching `fuzz/` unless the gate demands a lockfile update for a new dep
  (there are no new deps — so no).
