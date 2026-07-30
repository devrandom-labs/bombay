# Card #260 — drain-window handler ref must carry the actor's own link_tx

## Context

Card #260 filed this as a LATENT bug ("no handler-context self-watch API
exists"). That premise is FALSE, proven by experiment on 2026-07-30: `handle()`
receives `ActorRef<Self>` (`crates/core/src/actor/mod.rs:78-83`), and
`watch`/`link` are `pub` on `ActorRef<A: Watch>`
(`crates/core/src/actor/actor_ref.rs:223,254`). A throwaway integration probe
showed:

- steady state (external ref held): handler-context `actor_ref.watch(&t)` → `Ok(())`
- drain window (no external ref; ref minted at `kind.rs:257-265` with
  `link_tx: None`): same call → `Err(ActorNotLinked)` — WRONG for a
  linked-spawned actor.

So this is a LIVE bug reachable through the public API, and the fix the
in-code TODO names ships now: thread the actor's own `link_tx` through
`LoopHandles` into the drain-window mint.

Invariants that must hold:

- I-A: a handler-context `watch` on a **linked-spawned** actor behaves
  identically in steady state and in the drain window (both `Ok`, and the
  death notice is actually delivered to the loop's own link channel).
- I-B: a **plain-spawned** `Watch` actor still gets `Err(ActorNotLinked)`
  (`spawn.rs` test `plain_spawned_watch_actor_watch_errs` must stay green) —
  `LoopHandles.link_tx` is `None` on the plain path.
- I-C: ref-count-driven stop (Collected) is unchanged: `LinkSender` is not a
  mailbox sender and must not pin the strong count. (`start_actor` already
  drops the strong `actor_ref`; the new field is only a flume link-channel
  sender clone.)
- I-D: the `link_open` disable flag in `run_linked_message_loop`
  (`kind.rs:301-314`) STAYS. Rationale: `LinkReceiver` is a public type alias
  (`watch.rs:40`), so `run_linked` can be handed a receiver whose senders are
  not the loop's own — the all-senders-gone `Err` + `biased` spin hazard the
  flag guards is still constructible. Do NOT remove the flag; only update the
  doc comment (see step 3).

Design precedent: the supervised loop already holds a loop-lifetime clone of
the supervisor's own link sender (`kind.rs:363-369`, `kind.rs:414-420`), with
exactly this channel-never-closes reasoning. This change makes the linked
loop's drain-window mint consistent with that.

## Steps (all SEQUENTIAL — every step touches kind.rs and/or spawn.rs)

1. **`LoopHandles` gains the link sender** — `crates/core/src/actor/kind.rs:37-40`:
   add field `pub(super) link_tx: Option<LinkSender>` (import
   `crate::watch::LinkSender` at the top of the file with the existing `use`
   block — no inline paths). Doc-comment the field: the loop's own cold copy
   for drain-window minting (ADR-0010), `None` on the plain-spawn path,
   fixes #260.

2. **Populate it in the prologue** — `crates/core/src/actor/spawn.rs:383-386`
   (`start_actor`): `link_tx: actor_ref.link_tx().cloned()` alongside
   `cancel`/`abort`. (`ActorRef::link_tx()` is `pub(crate)`,
   `actor_ref.rs:102`, returns `Option<&LinkSender>`.)

3. **Use it at the drain-window mint** — `crates/core/src/actor/kind.rs:253-265`:
   replace the `None` argument with `handles.link_tx.clone()`. Rewrite the
   `TODO(#195 Q5)` comment block: the minted ref now carries the loop's own
   cold copy of `link_tx`, so a handler-context `watch`/`link` in the drain
   window behaves exactly as in steady state (#260). Also update the
   `run_linked_message_loop` doc paragraph (`kind.rs:283-289`): the loop's
   `LoopHandles` now holds a sender clone, so on the normal path the channel
   never reaches all-senders-gone while the loop lives; the `link_open` flag
   stays as the guard against a mismatched, externally-constructed
   `LinkReceiver` (public alias) whose senders all drop.

4. **Tests** — in the existing `spawn.rs` test module, next to the
   drain-window guard cluster (`drain_window_handler_ref_stops_the_actor`,
   ~`spawn.rs:1067`). Follow that cluster's exact idioms (`PreparedActor`,
   `bounded()`, `cap()`, enqueue-before-run for the drain window). Three
   tests, each one invariant:

   - T1 `drain_window_handler_watch_succeeds` (#260 regression; FAILED before
     this fix with `Err(ActorNotLinked)`):
     - `Target`: trivial plain actor (unit `Ping` msg), `PreparedActor::new`
       + `.spawn(())`, test holds `t_ref`.
     - `Watcher: Watch` (default hook), state = `ActorRef<Target>` +
       `Arc<Mutex<Option<Result<(), ActorNotLinked>>>>`; `handle` does
       `actor_ref.watch(&self.target).await`, records the result, sets
       `*stop = true`.
     - `PreparedActor::<Watcher>::new_linked`, `tell(Ping)` BEFORE `run_linked`,
       hold NO external watcher ref.
     - Assert recorded result `== Some(Ok(()))` and
       `RunResult::Stopped { reason: ActorStopReason::Normal, .. }` (exact
       variant — never `is_normal`).
   - T2 `drain_window_watch_delivers_death_notice` (the Ok must not be
     vacuous — the registration must carry the SAME channel the loop drains):
     - Watcher spawned via `spawn_linked_task`, messages enqueued before
       spawn, no external watcher ref (drain window).
     - Handler: `watch(&target)` → assert/record `Ok`, signal the test
       (oneshot A), then await oneshot B (bounded).
     - Test: on A, drop `t_ref` and await the target's join handle (target
       reaches `Collected`; its watcher-guard `Drop` fires the notice), then
       fire B.
     - Watcher's `on_link_died` override records `(id, reason, linked)`.
       After the watcher's join handle resolves, assert exactly one notice:
       `id == target id`, `linked == false`, and
       `matches!(reason, ActorStopReason::Collected)`.
     - Every await bounded with `terminate_bound()` (the #148/#179
       discipline). `Option<oneshot::Sender>::take()` for the non-Clone
       senders held in state.
   - T3 `steady_state_handler_watch_succeeds` (control sibling of T1): same
     as T1 but the test HOLDS an external watcher ref while `run_linked`
     drives the loop; assert `Some(Ok(()))`. (Handler-context steady-state
     watch is otherwise untested — existing watch tests register from
     outside a handler.)

5. **Mutants baseline** — run `cargo mutants --list` (list only — NEVER
   `cargo mutants` run, sandbox) and diff mutant paths for
   `start_actor` / `handle_mailbox_step` / the mint closure against
   `mutants-baseline.json`; add entries for any NEW mutants per the baseline
   workflow (new fn shapes must not land Unaccounted). If a new mutant is
   killed by T1/T2, no floor change is needed — verify, don't assume.

6. **Docs** — `docs/testing/coverage-baseline.md`: add the three tests to
   whatever per-module accounting it keeps (read the file's existing format
   first and follow it). README: NO change (no public-API change; bugfix).

## Verification (K3 runs ONLY these — tests are Claude's, via the gate)

```
cargo check -p bombay --all-targets
cargo clippy -p bombay --all-targets
```

Tests (`cargo nextest run -p bombay`) and `nix flake check` are run by the
controller AFTER execution — do not invoke them (sandboxed cargo test hangs).

## Out of scope

- Removing the `link_open` flag (see I-D — keep it).
- `SupervisedState.sup_link_tx` / `SupervisorRef` plumbing — already correct;
  do not unify it with `LoopHandles.link_tx`.
- The job-queue example app (bugfix card, no feature surface — deferral
  recorded in the PR body).
- Any `#[ignore]` probe — the card's original bullet is superseded: the bug
  is live, so the probe ships as the always-on regression test T1.
- Lint levels, `clippy.toml`, error-type changes.
