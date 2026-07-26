# Restart-Set Strategies (#199) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `OneForAll`/`RestForOne` restart-set strategies on the supervisor loop — the widen-coalescing cycle engine designed in [`2026-07-25-199-set-strategies-design.md`](../specs/2026-07-25-199-set-strategies-design.md) (ADR-0014), plus the heterogeneous-children proof and the three DST invariants.

**Architecture:** One `cycling: bool` per child entry (echo absorption + membership) and one loop-owned three-state coordinator (`Idle`/`Tearing`/`Waiting`). Set-teardown reuses the existing cancel + `pending_aborts` machinery; the cycle's rebuild deadline rides the existing `retries: DelayQueue` under a kept `Key`. Mid-cycle triggers **widen** the active cycle (suffix-nesting lemma) and remove any armed deadline. Strategy is a `Supervisor` trait method, default `OneForOne` (2a behavior unchanged).

**Tech Stack:** existing tree only — `tokio_util::time::DelayQueue` (already used), `fastrand`, `smallvec`. No new dependencies (no `fuzz/Cargo.lock` churn).

**Branch:** `feat/199-set-strategies` (spec already committed). Every code step: test-first, `cargo fmt` before each commit. The gate is `nix flake check` — run at the checkpoints marked; remember untracked files are invisible to it (`git add` first).

**Reference oracles when in doubt:** kameo `src/` (vendored), `ractor-supervisor` 0.1.9 `src/supervisor.rs`, OTP `supervisor.erl` (quotes in the spec). The design model `docs/superpowers/specs/2026-07-25-199-cycle-model.rs` is the behavioral oracle — when a test's expected value is unclear, run the model's matching scenario.

---

## File Structure

| File | Change |
|---|---|
| `bombay-core/src/restart.rs` | `SupervisionStrategy` enum (pure policy domain, beside `RestartPolicy`) |
| `bombay-core/src/actor/mod.rs` | `Supervisor::supervision_strategy()` default method |
| `bombay-core/src/actor/supervision.rs` | `Child.cycling`; `CycleState`; `Children::{position, flag_cycle, absorb_cycling_death, cycling_rebuild_ids}` |
| `bombay-core/src/actor/kind.rs` | cycle wiring: absorb path, `start_or_widen_cycle`, retry-arm key match, `rebuild_child` cycling guard, `apply_supervision_op` awaiting adjust |
| `bombay-core/src/actor/spawn.rs` | `SupervisedState.cycle`; behavioral tests in the supervision test module (`struct Sup` at ~3376) |
| `bombay-core/tests/dst_races.rs` | the three `dst_*` invariants |
| `mutants-baseline.json` | entries for every new fn |
| `README.md` | strategy bullet (public API changed) |

---

### Task 1: `SupervisionStrategy` enum

**Files:**
- Modify: `bombay-core/src/restart.rs` (after `RestartPolicy`, ~line 35)

- [ ] **Step 1: Write the failing test** (in `restart.rs` `mod tests`)

```rust
    /// The strategy is an escalation LADDER, not a menu: each variant names the
    /// suffix of the birth order it cycles. Exhaustive match = compile tripwire
    /// for new variants.
    #[test]
    fn strategy_variants_are_the_three_ladder_rungs() {
        for s in [
            SupervisionStrategy::OneForOne,
            SupervisionStrategy::RestForOne,
            SupervisionStrategy::OneForAll,
        ] {
            match s {
                SupervisionStrategy::OneForOne
                | SupervisionStrategy::RestForOne
                | SupervisionStrategy::OneForAll => {}
            }
        }
        assert_ne!(SupervisionStrategy::OneForOne, SupervisionStrategy::OneForAll);
    }
```

Add `SupervisionStrategy` to the test module's `use super::*;` reach (it already glob-imports).

- [ ] **Step 2: Run to verify it fails**

Run: `nix develop --command cargo test -p bombay-core --lib restart::tests::strategy_variants -- --nocapture`
Expected: compile FAIL — `SupervisionStrategy` not found.

- [ ] **Step 3: Implement**

```rust
/// Which SIBLINGS share a failed child's fate — a property of the SUPERVISOR
/// ([`Supervisor::supervision_strategy`](crate::actor::Supervisor::supervision_strategy)),
/// never of a child: mixing per-child strategies would let two children
/// disagree about the set they are in.
///
/// The variants are the middle rungs of the escalation ladder, ordered by
/// containment (microreboot's "progressively larger subsets"): each names the
/// suffix of the supervisor's birth-ordered child table it cycles —
/// `OneForOne` the failed child alone, `RestForOne` the failed child and every
/// younger sibling, `OneForAll` the whole set. Because every subset is a
/// suffix, any two are nested — the property the widen rule rests on
/// (ADR-0014).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SupervisionStrategy {
    /// Rebuild the failed child only (the 2a behavior, and the default).
    OneForOne,
    /// Cycle the failed child and every YOUNGER sibling (later birth); older
    /// siblings are untouched. For pipelines: juniors depend on elders.
    RestForOne,
    /// Cycle the whole child set. For siblings sharing fragile state that a
    /// half-fresh set would corrupt (microreboot's recovery group).
    OneForAll,
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `nix develop --command cargo test -p bombay-core --lib restart::tests::strategy_variants`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add bombay-core/src/restart.rs && git commit -m "core(supervision): SupervisionStrategy enum — the ladder's three rungs [#199]"
```

---

### Task 2: `supervision_strategy()` on the `Supervisor` trait

**Files:**
- Modify: `bombay-core/src/actor/mod.rs:214-224` (the `Supervisor` marker)

- [ ] **Step 1: Failing test** (in `mod.rs`'s `watch_trait_tests` module — add a sibling test module `supervisor_trait_tests` after it)

```rust
#[cfg(test)]
mod supervisor_trait_tests {
    use super::*;
    use crate::restart::SupervisionStrategy;

    struct DefaultSup;
    struct AllSup;
    #[derive(Debug)]
    struct M2;
    impl crate::message::Msg for M2 {}
    macro_rules! actor_boilerplate {
        ($t:ty) => {
            impl crate::mailbox::Mailboxed for $t {
                type Msg = M2;
            }
            impl Actor for $t {
                type Args = ();
                type Error = core::convert::Infallible;
                async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                    unimplemented!("trait-surface test: never spawned")
                }
                async fn handle(
                    &mut self,
                    _: M2,
                    _: ActorRef<Self>,
                    _: &mut bool,
                ) -> Result<(), Self::Error> {
                    Ok(())
                }
            }
            impl Watch for $t {}
        };
    }
    actor_boilerplate!(DefaultSup);
    actor_boilerplate!(AllSup);
    impl Supervisor for DefaultSup {}
    impl Supervisor for AllSup {
        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::OneForAll
        }
    }

    /// The strategy seat #196 reserved: a supervisor property with the 2a
    /// default, overridable per supervisor TYPE. `RestartConfig` (per-child)
    /// carries no strategy field — the card's compile-visible invariant
    /// `strategy_is_supervisor_property_not_child` is held structurally by
    /// this being the only strategy surface in the crate.
    #[test]
    fn strategy_is_supervisor_property_with_one_for_one_default() {
        assert_eq!(
            DefaultSup::supervision_strategy(),
            SupervisionStrategy::OneForOne,
            "default preserves 2a behavior",
        );
        assert_eq!(AllSup::supervision_strategy(), SupervisionStrategy::OneForAll);
    }
}
```

- [ ] **Step 2: Run — expect compile FAIL** (`supervision_strategy` not a member): `nix develop --command cargo test -p bombay-core --lib supervisor_trait_tests`

- [ ] **Step 3: Implement** — replace `pub trait Supervisor: Watch {}` and its doc's "No methods in this slice…" paragraph:

```rust
/// Authority marker: an [`Actor`] cannot watch, a [`Watch`] actor observes a
/// peer's death, a `Supervisor` **rebuilds** dead children under a restart
/// policy.
///
/// Restart *policy* (whether a given child comes back) stays per-child,
/// supplied at `supervise` time. The *strategy* (which siblings share the
/// failed child's fate) is a property of the supervisor — the seat #196
/// reserved (card #199, ADR-0014).
pub trait Supervisor: Watch {
    /// The restart-set strategy for this supervisor's children.
    ///
    /// Defaults to [`OneForOne`](SupervisionStrategy::OneForOne) — the 2a
    /// behavior: a failed child is rebuilt alone and siblings never observe
    /// it. Override to cycle sets ([`RestForOne`](SupervisionStrategy::RestForOne)
    /// / [`OneForAll`](SupervisionStrategy::OneForAll)).
    #[must_use]
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
    }
}
```

Add `use crate::restart::SupervisionStrategy;` to `mod.rs`'s imports.

- [ ] **Step 4: Run to verify PASS**, plus the whole lib suite for regressions: `nix develop --command cargo test -p bombay-core --lib`

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add bombay-core/src/actor/mod.rs && git commit -m "core(supervision): supervision_strategy() lands on the Supervisor seat [#199]"
```

---

### Task 3: `Children` cycle surface — `cycling` flag, `CycleState`, table ops

**Files:**
- Modify: `bombay-core/src/actor/supervision.rs` (`Child` ~149, `Children` ~214, tests ~295)
- Modify: `bombay-core/src/actor/actor_ref.rs:377-384` (`Child` literal in `supervise` gains `cycling: false`)
- Modify: `bombay-core/src/actor/kind.rs` test helper `child()` (~762) gains `cycling: false`

- [ ] **Step 1: Failing unit tests** (append to `supervision.rs` `mod tests`; helpers `child_entry`/`handle` exist there)

```rust
    /// `flag_cycle(from)` flags the suffix, returns live members' stop edges in
    /// REVERSE birth order (the teardown order) with their graces, and counts
    /// them. Dead members (backoff window / the trigger) are flagged but not
    /// counted — they will send no death.
    #[test]
    fn flag_cycle_flags_suffix_and_returns_live_reverse_order() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));
        let mut dead = child_entry(ActorId::new(2));
        dead.handle = None; // the trigger, or a backoff-window member
        children.insert(ActorId::new(2), dead);
        children.insert(ActorId::new(3), child_entry(ActorId::new(3)));

        let (stops, awaiting) = children.flag_cycle(1); // suffix {2, 3}

        assert_eq!(awaiting, 1, "only the live member is awaited");
        assert_eq!(stops.len(), 1);
        assert_eq!(stops[0].0.id(), ActorId::new(3));
        assert!(!children.get_mut(ActorId::new(1)).unwrap().cycling, "elder untouched");
        assert!(children.get_mut(ActorId::new(2)).unwrap().cycling);
        assert!(children.get_mut(ActorId::new(3)).unwrap().cycling);
    }

    /// Widening re-flags idempotently: already-cycling members are NOT returned
    /// again (their cancel is already in flight), only newly flagged live
    /// members are.
    #[test]
    fn flag_cycle_widen_returns_only_newly_flagged() {
        let mut children = Children::new();
        for i in 1..=3 {
            children.insert(ActorId::new(i), child_entry(ActorId::new(i)));
        }
        let (first, awaiting1) = children.flag_cycle(2); // {3}
        assert_eq!((first.len(), awaiting1), (1, 1));

        let (widened, awaiting2) = children.flag_cycle(0); // widen to {1,2,3}
        let ids: Vec<ActorId> = widened.iter().map(|(h, _)| h.id()).collect();
        assert_eq!(ids, [ActorId::new(2), ActorId::new(1)], "new members only, reverse birth");
        assert_eq!(awaiting2, 2, "counts only the additions");
    }

    /// The absorb primitive: a cycling member's death reports whether it was
    /// still live (= it was being awaited); a non-cycling id reports `None`.
    /// Either way a dead member keeps its entry (factory + accounting persist).
    #[test]
    fn absorb_cycling_death_distinguishes_awaited_from_not() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));
        children.insert(ActorId::new(2), child_entry(ActorId::new(2)));
        let (_, _) = children.flag_cycle(1); // {2} cycling, live

        assert_eq!(children.absorb_cycling_death(ActorId::new(1)), None, "not cycling");
        assert_eq!(
            children.absorb_cycling_death(ActorId::new(2)),
            Some(true),
            "cycling and live: this death was awaited",
        );
        assert!(children.get_mut(ActorId::new(2)).unwrap().handle.is_none());
        assert_eq!(
            children.absorb_cycling_death(ActorId::new(2)),
            Some(false),
            "already dead: absorbed but not awaited",
        );
    }

    /// The rebuild sweep: returns non-`Never` cycling ids in BIRTH order and
    /// clears every cycling flag (Never members are left dead, entry retained).
    #[test]
    fn cycling_rebuild_ids_returns_non_never_in_birth_order_and_clears_flags() {
        let mut children = Children::new();
        children.insert(ActorId::new(1), child_entry(ActorId::new(1)));
        let mut never = child_entry(ActorId::new(2));
        never.config = RestartConfig::new(RestartPolicy::Never);
        children.insert(ActorId::new(2), never);
        children.insert(ActorId::new(3), child_entry(ActorId::new(3)));
        let (_, _) = children.flag_cycle(0);

        let ids: Vec<ActorId> = children.cycling_rebuild_ids().into_iter().collect();

        assert_eq!(ids, [ActorId::new(1), ActorId::new(3)], "birth order, Never excluded");
        for i in 1..=3 {
            assert!(!children.get_mut(ActorId::new(i)).unwrap().cycling, "flag {i} cleared");
        }
    }

    /// `position` is the strategy's subset anchor: birth index by CURRENT key.
    #[test]
    fn position_finds_current_key() {
        let mut children = Children::new();
        children.insert(ActorId::new(5), child_entry(ActorId::new(5)));
        children.insert(ActorId::new(7), child_entry(ActorId::new(7)));
        assert_eq!(children.position(ActorId::new(7)), Some(1));
        assert_eq!(children.position(ActorId::new(9)), None);
    }
```

- [ ] **Step 2: Run — expect compile FAIL** (`cycling`, `flag_cycle`, … missing): `nix develop --command cargo test -p bombay-core --lib actor::supervision::tests`

- [ ] **Step 3: Implement.** In `Child` (after `tracker`):

```rust
    /// Member of the ACTIVE set-cycle (card #199): its death is expected —
    /// absorbed by the cycle, never fed to the restart policy (ADR-0014's echo
    /// suppression). Set by `flag_cycle`, cleared by `cycling_rebuild_ids`.
    pub(crate) cycling: bool,
```

`CycleState` after `SupervisionOp`:

```rust
/// The supervisor's set-cycle coordinator (card #199, ADR-0014). Loop-owned,
/// beside `Children` — at most ONE cycle is ever active, because a mid-cycle
/// trigger *widens* the active cycle instead of starting a second (every
/// restart subset is a suffix of the birth order, so any two are nested).
#[derive(Debug)]
pub(crate) enum CycleState {
    /// No set-cycle in flight (also the only state `OneForOne` ever sees).
    Idle,
    /// Teardown in flight: `awaiting` live members' deaths are pending;
    /// `backoff` is the trigger's jittered delay, applied once teardown ends.
    Tearing { awaiting: u32, backoff: Duration },
    /// Torn down; the rebuild deadline is armed in the loop's `retries` queue
    /// under `key` — kept so a widen can REMOVE it (the stale-deadline hazard:
    /// left armed, it would rebuild a half-torn set).
    Waiting { key: delay_queue::Key },
}
```

with `use tokio_util::time::delay_queue;` added to the imports. `Children` methods (after `rekey`):

```rust
    /// Birth index of the entry currently keyed by `id` — the anchor
    /// `RestForOne` computes its suffix from.
    pub(crate) fn position(&self, id: ActorId) -> Option<usize> {
        self.entries.iter().position(|(key, _)| *key == id)
    }

    /// Flags `[from..]` as cycling and returns the stop edges of members that
    /// were NOT already cycling and are live — in REVERSE birth order (the
    /// teardown order) with each member's own grace — plus their count (the
    /// `awaiting` delta). Idempotent under widening: re-flagging is a no-op and
    /// already-cycling members are never returned twice (their cancel is
    /// already in flight).
    pub(crate) fn flag_cycle(
        &mut self,
        from: usize,
    ) -> (SmallVec<[(ChildHandle, Duration); 4]>, u32) {
        let mut stops: SmallVec<[(ChildHandle, Duration); 4]> = SmallVec::new();
        for (_, child) in self.entries[from..].iter_mut().rev() {
            if child.cycling {
                continue;
            }
            child.cycling = true;
            if let Some(handle) = &child.handle {
                stops.push((handle.clone(), child.config.stop_grace));
            }
        }
        let awaiting = u32::try_from(stops.len()).unwrap_or(u32::MAX);
        (stops, awaiting)
    }

    /// Absorbs one death IF `id` names a cycling member: records the
    /// incarnation gone and reports whether it was still live — i.e. whether
    /// this death was one the teardown is awaiting. `None`: not a cycling
    /// member (route to the ordinary paths). The entry always survives.
    pub(crate) fn absorb_cycling_death(&mut self, id: ActorId) -> Option<bool> {
        self.entries
            .iter_mut()
            .find(|(key, _)| *key == id)
            .filter(|(_, child)| child.cycling)
            .map(|(_, child)| child.handle.take().is_some())
    }

    /// Ends the cycle's teardown phase: clears EVERY cycling flag and returns
    /// the non-`Never` members' ids in BIRTH order — the rebuild worklist.
    /// `Never` members are left dead with entries retained (stopped with the
    /// set, never rebuilt — OTP's temporary-children rule).
    pub(crate) fn cycling_rebuild_ids(&mut self) -> SmallVec<[ActorId; 4]> {
        self.entries
            .iter_mut()
            .filter(|(_, child)| child.cycling)
            .filter_map(|(id, child)| {
                child.cycling = false;
                (child.config.policy != RestartPolicy::Never).then_some(*id)
            })
            .collect()
    }
```

`use crate::restart::RestartPolicy;` may need adding. Update the `Child` literal in `actor_ref.rs::supervise` (add `cycling: false,`) and `supervision.rs`/`kind.rs` test helpers (`child_entry`, `child`) likewise.

- [ ] **Step 4: Run to verify PASS + no regressions**: `nix develop --command cargo test -p bombay-core --lib`

- [ ] **Step 5: Commit**

```bash
cargo fmt && git add -A bombay-core/src && git commit -m "core(supervision): cycling flag + CycleState + Children cycle ops [#199]"
```

---

### Task 4: kind.rs decision layer — absorb, start/widen, guards

**Files:**
- Modify: `bombay-core/src/actor/kind.rs` (`handle_child_death` ~486, `restart_or_give_up` ~511, `rebuild_child` ~546, `apply_supervision_op` ~602; unit tests at the bottom use the existing `supervisor()`/`child()`/`handle()` helpers)

All functions stay **sync and non-generic** (strategy passed by value) — the mutation-testability discipline.

- [ ] **Step 1: Failing unit tests** (append to `kind.rs` `mod tests`; follow the existing helper idioms — `child(config, started)` builds a `Child`, `supervisor(id)` a `SupervisorRef` + `LinkReceiver`, `notice(id, reason)` may need adding as below)

```rust
    fn notice(id: ActorId, reason: ActorStopReason) -> LinkDied {
        LinkDied {
            id,
            reason,
            linked: false,
            cleanup_failed: false,
        }
    }

    fn table_of(n: u32) -> Children {
        let mut children = Children::new();
        for i in 1..=n {
            children.insert(
                ActorId::new(i as usize),
                child(RestartConfig::new(RestartPolicy::Permanent), Instant::now()),
            );
        }
        children
    }

    /// OneForAll trigger: the whole table is flagged, live siblings' cancels
    /// fire, the trigger's counters advance ONCE, siblings' never (card:
    /// `set_restart_counts_once_against_budget` at the decision layer).
    #[tokio::test(start_paused = true)]
    async fn set_trigger_flags_set_and_counts_trigger_once() {
        let mut children = table_of(3);
        children.get_mut(ActorId::new(2)).unwrap().handle = None; // the trigger died
        let (sup, _link_rx) = supervisor(ActorId::new(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);

        let flow = handle_child_death(
            &mut children,
            &mut retries,
            &mut pending_aborts,
            &mut cycle,
            SupervisionStrategy::OneForAll,
            &mut rng,
            &notice(ActorId::new(2), ActorStopReason::Killed),
        );

        assert!(matches!(flow, Some(ControlFlow::Continue(()))));
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 2, .. }), "{cycle:?}");
        for i in [1_usize, 3] {
            let sibling = children.get_mut(ActorId::new(i)).unwrap();
            assert!(sibling.cycling);
            assert!(
                sibling.handle.as_ref().unwrap().cancel.is_cancelled(),
                "sibling {i} cancelled",
            );
        }
        assert_eq!(pending_aborts.len(), 2, "deferred hard-kills armed");
        assert_eq!(retries.len(), 0, "no rebuild while teardown pending");
        let _ = sup;
    }

    /// Absorb: a cycling member's death decrements awaiting; the LAST one arms
    /// the single cycle-rebuild deadline (Waiting) instead of a policy verdict —
    /// even when the death reason is a lifecycle-hook panic (an `on_stop` panic
    /// during deliberate teardown is not crash-loop evidence; the reason is
    /// diagnostic only on this path).
    #[tokio::test(start_paused = true)]
    async fn absorbed_deaths_count_down_and_arm_rebuild() {
        let mut children = table_of(3);
        children.get_mut(ActorId::new(2)).unwrap().handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::new(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::OneForAll, &mut rng,
            &notice(ActorId::new(2), ActorStopReason::Killed),
        );

        let hook_panic = ActorStopReason::Panicked(PanicError::new(
            Box::new("on_stop blew up during teardown"),
            PanicReason::OnStop,
        ));
        let first = handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::OneForAll, &mut rng,
            &notice(ActorId::new(3), hook_panic),
        );
        assert!(matches!(first, Some(ControlFlow::Continue(()))), "absorbed, not escalated");
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        let last = handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::OneForAll, &mut rng,
            &notice(ActorId::new(1), ActorStopReason::Killed),
        );
        assert!(matches!(last, Some(ControlFlow::Continue(()))));
        assert!(matches!(cycle, CycleState::Waiting { .. }), "teardown complete: rebuild armed");
        assert_eq!(retries.len(), 1, "exactly one cycle deadline");
        let trigger = children.get_mut(ActorId::new(2)).unwrap();
        assert_eq!(trigger.tracker, {
            let mut t = RestartTracker::new(Instant::now());
            t.record_failure(&trigger.config, Instant::now());
            t
        }, "trigger charged once; absorbs charged nothing");
    }

    /// Widen: an elder Supervised death mid-Tearing folds into the active cycle
    /// (RestForOne: its suffix is a superset), recomputing awaiting and NOT
    /// double-cancelling already-cycling members.
    #[tokio::test(start_paused = true)]
    async fn elder_death_mid_tearing_widens_the_cycle() {
        let mut children = table_of(3);
        children.get_mut(ActorId::new(2)).unwrap().handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::new(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        // Trigger: child 2 → cycle {2,3} (RestForOne suffix), awaiting {3}.
        handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::RestForOne, &mut rng,
            &notice(ActorId::new(2), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        // Elder child 1 dies spontaneously mid-cycle → widen to {1,2,3}.
        let flow = handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::RestForOne, &mut rng,
            &notice(ActorId::new(1), ActorStopReason::Killed),
        );
        assert!(matches!(flow, Some(ControlFlow::Continue(()))));
        // 1 was live? No — it just DIED (its death is the trigger); nothing new
        // to await: still awaiting only {3}.
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }), "{cycle:?}");
        assert!(children.get_mut(ActorId::new(1)).unwrap().cycling, "elder folded in");
        assert_eq!(pending_aborts.len(), 1, "no double-cancel of member 3");
    }

    /// Widen during Waiting: the armed rebuild deadline is REMOVED and re-armed
    /// (the stale-deadline/half-alive hazard, ADR-0014's counterexample table).
    #[tokio::test(start_paused = true)]
    async fn widen_during_waiting_replaces_the_armed_deadline() {
        let mut children = table_of(2);
        children.get_mut(ActorId::new(2)).unwrap().handle = None;
        let (_sup, _link_rx) = supervisor(ActorId::new(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        // Child 2 is the LAST child: RestForOne suffix = {2} alone, all dead →
        // straight to Waiting.
        handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::RestForOne, &mut rng,
            &notice(ActorId::new(2), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Waiting { .. }));
        assert_eq!(retries.len(), 1);

        // Elder child 1 dies in the Waiting window → widen to {1,2}: the stale
        // deadline is removed, one fresh deadline armed.
        children.get_mut(ActorId::new(1)).unwrap().handle = None; // it died
        handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::RestForOne, &mut rng,
            &notice(ActorId::new(1), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Waiting { .. }));
        assert_eq!(retries.len(), 1, "stale deadline removed, exactly one armed");
        assert!(children.get_mut(ActorId::new(1)).unwrap().cycling);
    }

    /// `rebuild_child` on a cycling entry is a no-op: a pre-cycle solo backoff
    /// deadline firing mid-cycle must not rebuild one member of a set mid-
    /// teardown (drop-at-fire supersession — 2a discards solo `Key`s).
    #[tokio::test(start_paused = true)]
    async fn rebuild_child_is_superseded_for_cycling_entries() {
        let mut children = table_of(1);
        children.get_mut(ActorId::new(1)).unwrap().handle = None;
        children.get_mut(ActorId::new(1)).unwrap().cycling = true;
        let (sup, _link_rx) = supervisor(ActorId::new(9));

        rebuild_child(&mut children, &sup, ActorId::new(1));

        let entry = children.get_mut(ActorId::new(1)).expect("entry retained");
        assert!(entry.handle.is_none(), "no rebuild while cycling");
        assert!(entry.cycling, "flag untouched — the cycle still owns it");
    }

    /// Removal mid-cycle (`unsupervise`/`stop_child` of an awaited member) must
    /// count the teardown down — else the cycle waits forever for a death that
    /// will land as a table-miss (the wedge counterexample).
    #[tokio::test(start_paused = true)]
    async fn removing_an_awaited_member_counts_the_teardown_down() {
        let mut children = table_of(2);
        children.get_mut(ActorId::new(1)).unwrap().handle = None;
        let (sup, _link_rx) = supervisor(ActorId::new(9));
        let mut retries = DelayQueue::new();
        let mut pending_aborts = DelayQueue::new();
        let mut cycle = CycleState::Idle;
        let mut rng = Rng::with_seed(7);
        handle_child_death(
            &mut children, &mut retries, &mut pending_aborts, &mut cycle,
            SupervisionStrategy::OneForAll, &mut rng,
            &notice(ActorId::new(1), ActorStopReason::Killed),
        );
        assert!(matches!(cycle, CycleState::Tearing { awaiting: 1, .. }));

        apply_supervision_op(
            &mut children, &sup, &mut pending_aborts, &mut retries, &mut cycle,
            SupervisionOp::Remove(ActorId::new(2)),
        );

        assert!(matches!(cycle, CycleState::Waiting { .. }), "last awaited member removed ⇒ rebuild armed");
        assert!(children.get_mut(ActorId::new(2)).is_none(), "entry gone, never rebuilt");
    }
```

Add needed imports to the test module (`CycleState`, `SupervisionStrategy`, `PanicError`, `PanicReason`, `RestartTracker`).

- [ ] **Step 2: Run — expect compile FAIL** (new params/fns absent): `nix develop --command cargo test -p bombay-core --lib actor::kind::tests`

- [ ] **Step 3: Implement.** New sigs and bodies in `kind.rs`:

```rust
/// Counts one awaited teardown death (or removal) down; on the LAST, arms the
/// single cycle-rebuild deadline and moves to `Waiting`. The `DelayQueue` value
/// is `id` only because the queue carries `ActorId`s — the cycle path matches
/// on the KEY, never the value.
fn cycle_count_down(cycle: &mut CycleState, retries: &mut DelayQueue<ActorId>, id: ActorId) {
    let CycleState::Tearing { awaiting, backoff } = cycle else {
        return;
    };
    *awaiting = awaiting.saturating_sub(1);
    if *awaiting == 0 {
        let key = retries.insert(id, *backoff);
        *cycle = CycleState::Waiting { key };
    }
}
```

(`saturating_sub` here is NOT a size/limit path — `awaiting` is a teardown
countdown whose floor is a state transition, and the `Tearing` guarantee makes
0-entry unreachable; still, prefer `checked_sub` + `debug`-free explicit branch
if review prefers: `let Some(left) = awaiting.checked_sub(1) else { return };`.)

Use the `checked_sub` form:

```rust
    let Some(left) = awaiting.checked_sub(1) else {
        return; // Tearing{awaiting: 0} is unrepresentable by construction
    };
    *awaiting = left;
    if left == 0 { ... }
```

`handle_child_death` — new signature and set-strategy routing:

```rust
fn handle_child_death(
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    pending_aborts: &mut DelayQueue<ChildHandle>,
    cycle: &mut CycleState,
    strategy: SupervisionStrategy,
    rng: &mut Rng,
    notice: &LinkDied,
) -> Option<ControlFlow<ActorStopReason>> {
    // Echo absorption FIRST (ADR-0014): a cycling member's death is expected —
    // count the teardown down and absorb, whatever the reason says (an on_stop
    // panic during deliberate teardown is not crash-loop evidence; the fresh
    // incarnation runs a fresh on_start).
    if let Some(was_awaited) = children.absorb_cycling_death(notice.id) {
        if was_awaited {
            cycle_count_down(cycle, retries, notice.id);
        }
        return Some(ControlFlow::Continue(()));
    }
    let child = children.get_mut(notice.id)?;
    child.handle = None;
    Some(match should_restart(child.config.policy, &notice.reason) {
        RestartVerdict::LeaveDead => ControlFlow::Continue(()),
        RestartVerdict::Escalate => {
            ControlFlow::Break(ActorStopReason::ChildLifecycleFailed { child: notice.id })
        }
        RestartVerdict::Restart => restart_or_give_up(
            children, retries, pending_aborts, cycle, strategy, rng, notice.id,
        ),
    })
}
```

`restart_or_give_up` — record once, then route by strategy (note it now takes
the table, since the set path needs it; the borrow of `child` is re-taken):

```rust
fn restart_or_give_up(
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    pending_aborts: &mut DelayQueue<ChildHandle>,
    cycle: &mut CycleState,
    strategy: SupervisionStrategy,
    rng: &mut Rng,
    id: ActorId,
) -> ControlFlow<ActorStopReason> {
    let child = children
        .get_mut(id)
        .expect("caller verified membership; single-threaded loop");
    match child.tracker.record_failure(&child.config, Instant::now()) {
        GiveUp::Yes { rebuilds } => ControlFlow::Break(ActorStopReason::RestartLimitExceeded {
            child: id,
            rebuilds,
        }),
        GiveUp::No { attempt } => {
            let delay = jittered_backoff(&child.config, attempt, rng);
            match strategy {
                SupervisionStrategy::OneForOne => {
                    retries.insert(id, delay);
                }
                SupervisionStrategy::RestForOne | SupervisionStrategy::OneForAll => {
                    let from = if strategy == SupervisionStrategy::OneForAll {
                        0
                    } else {
                        children.position(id).expect("membership verified above")
                    };
                    start_or_widen_cycle(children, retries, pending_aborts, cycle, from, delay, id);
                }
            }
            ControlFlow::Continue(())
        }
    }
}
```

Wait — `expect` violates the no-panic discipline? No: this is a programmer-invariant (`get_mut` succeeded lines above in the same synchronous scope), the documented panic-for-bugs case. Keep the `expect` strings as shown.

`start_or_widen_cycle`:

```rust
/// Starts a set-cycle over the suffix `[from..]`, or WIDENS the active one —
/// the same operation, because `flag_cycle` is idempotent and every subset is
/// a suffix (nested; ADR-0014). Cancels newly flagged live members in reverse
/// birth order with deferred hard-kills, then re-derives the cycle state. Any
/// armed rebuild deadline is REMOVED first: left in place it would fire
/// mid-teardown of the widened set and rebuild a half-alive set.
fn start_or_widen_cycle(
    children: &mut Children,
    retries: &mut DelayQueue<ActorId>,
    pending_aborts: &mut DelayQueue<ChildHandle>,
    cycle: &mut CycleState,
    from: usize,
    delay: Duration,
    trigger: ActorId,
) {
    if let CycleState::Waiting { key } = cycle {
        retries.remove(key);
    }
    let (stops, added) = children.flag_cycle(from);
    for (handle, grace) in stops {
        handle.cancel.cancel();
        pending_aborts.insert(handle, grace);
    }
    let pending = match *cycle {
        CycleState::Tearing { awaiting, .. } => awaiting,
        CycleState::Idle | CycleState::Waiting { .. } => 0,
    };
    let awaiting = pending.saturating_add(added); // bounded by fan-out, cannot overflow u32 in practice; saturate = still-correct "await them all"
    if awaiting == 0 {
        let key = retries.insert(trigger, delay);
        *cycle = CycleState::Waiting { key };
    } else {
        *cycle = CycleState::Tearing {
            awaiting,
            backoff: delay,
        };
    }
}
```

`saturating_add` justification comment must stay — or use `checked_add` with the same fallback branch if review prefers the house rule strictly; the checked form:

```rust
    let awaiting = pending.checked_add(added).unwrap_or(u32::MAX);
```

Use the checked form. `rebuild_child` gains the supersession guard as its first
lookup (replace the current `children.get_mut(old_id).map(...)` chain):

```rust
    let Some(Spawned {
        handle,
        install_watch,
    }) = children
        .get_mut(old_id)
        // Drop-at-fire supersession: the cycle owns this entry now; its solo
        // deadline (whose Key 2a discards) must not rebuild one member of a
        // set mid-teardown. The CYCLE's rebuild sweep clears the flag first.
        .filter(|child| !child.cycling)
        .map(|child| (child.factory)())
    else {
        return;
    };
```

`apply_supervision_op` — new params `retries: &mut DelayQueue<ActorId>, cycle: &mut CycleState`, and both removal arms count down:

```rust
        SupervisionOp::Remove(id) => {
            if let Some(child) = children.remove(id)
                && child.cycling
                && child.handle.is_some()
            {
                // An awaited member left the cycle without dying: count the
                // teardown down or the cycle waits forever (its death will land
                // as a table-miss). The cancel already sent cannot be revoked.
                cycle_count_down(cycle, retries, id);
            }
        }
        SupervisionOp::Stop(id) => {
            if let Some(child) = children.remove(id) {
                let was_awaited = child.cycling && child.handle.is_some();
                if let Some(handle) = child.handle {
                    handle.cancel.cancel();
                    pending_aborts.insert(handle, child.config.stop_grace);
                }
                if was_awaited {
                    cycle_count_down(cycle, retries, id);
                }
            }
        }
```

Callers updated in this task compile-only (loop wiring is Task 5): `dispatch_death` threads the new params through; imports gain `CycleState`, `SupervisionStrategy`.

- [ ] **Step 4: Run to verify PASS**: `nix develop --command cargo test -p bombay-core --lib actor::kind`
- [ ] **Step 5: Full lib suite green**: `nix develop --command cargo test -p bombay-core --lib`
- [ ] **Step 6: Commit**

```bash
cargo fmt && git add bombay-core/src/actor && git commit -m "core(supervision): set-cycle decision layer — absorb, start-or-widen, guards [#199]"
```

---

### Task 5: Loop wiring — `SupervisedState.cycle`, strategy plumb, retry-arm key match

**Files:**
- Modify: `bombay-core/src/actor/kind.rs` (`SupervisedState` ~51, `run_supervised_message_loop` ~331, `dispatch_death` ~427)
- Modify: `bombay-core/src/actor/spawn.rs` (`run_lifecycle_supervised` ~525: construct `CycleState::Idle`, pass it)

- [ ] **Step 1: Failing test** — loop-level, in `spawn.rs`'s supervision test module. Uses the existing `Sup`-style harness but with an `OneForAll` supervisor; minimal first probe (full behavioral matrix is Tasks 6–8):

```rust
        struct AllSup;
        impl Mailboxed for AllSup {
            type Msg = SupMsg;
        }
        impl crate::actor::Actor for AllSup {
            type Args = ();
            type Error = Infallible;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
                Ok(Self)
            }
            async fn handle(
                &mut self,
                _: SupMsg,
                _: ActorRef<Self>,
                _: &mut bool,
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }
        impl Watch for AllSup {}
        impl Supervisor for AllSup {
            fn supervision_strategy() -> SupervisionStrategy {
                SupervisionStrategy::OneForAll
            }
        }

        /// End-to-end smoke for the wired cycle: under OneForAll a sibling's
        /// crash rebuilds BOTH children with fresh ids.
        #[tokio::test(start_paused = true)]
        async fn one_for_all_smoke_rebuilds_the_sibling_too() {
            let sup = AllSup::spawn_supervised(());
            let (id_tx, id_rx) = flume::unbounded::<ActorId>();
            let senders: Senders = Arc::new(Mutex::new(Vec::new()));

            let a = tokio::time::timeout(
                terminate_bound(),
                sup.supervise(RestartPolicy::Permanent, worker_factory(id_tx.clone(), Arc::clone(&senders))),
            )
            .await
            .expect("no hang")
            .expect("alive");
            assert_eq!(recv_id(&id_rx).await, a);
            let b = tokio::time::timeout(
                terminate_bound(),
                sup.supervise(RestartPolicy::Permanent, worker_factory(id_tx, Arc::clone(&senders))),
            )
            .await
            .expect("no hang")
            .expect("alive");
            assert_eq!(recv_id(&id_rx).await, b);

            send_cmd(&senders, 1, Cmd::Crash); // b crashes

            let r1 = recv_id(&id_rx).await;
            let r2 = recv_id(&id_rx).await;
            assert_eq!([r1, r2].iter().filter(|id| **id == a || **id == b).count(), 0,
                "both rebuilt with FRESH ids: {r1:?}, {r2:?} vs {a:?}, {b:?}");

            drop(sup);
        }
```

(`SupMsg` = whatever `Sup`'s message type is named in that module — reuse it; `worker_factory` needs `id_tx.clone()` for the two calls as shown.)

- [ ] **Step 2: Run — expect FAIL** (compiles only after wiring; then behaviorally b-only rebuild under the unwired loop): `nix develop --command cargo test -p bombay-core --lib one_for_all_smoke`

- [ ] **Step 3: Wire.** `SupervisedState` gains:

```rust
    /// The set-cycle coordinator (card #199, ADR-0014) — loop-owned like the
    /// table it coordinates.
    pub(super) cycle: &'a mut CycleState,
```

`run_lifecycle_supervised` (spawn.rs) constructs `let mut cycle = CycleState::Idle;` beside `retries`/`pending_aborts` and passes `cycle: &mut cycle`. In `run_supervised_message_loop`: destructure `cycle`, compute once before the loop:

```rust
    let strategy = A::supervision_strategy();
```

Thread `cycle`/`strategy`/`pending_aborts` through `dispatch_death` into `handle_child_death`. The retry arm becomes:

```rust
            next_retry = retries.next(), if !retries.is_empty() => {
                if let Some(expired) = next_retry {
                    // The CYCLE's deadline is matched by KEY (its value is
                    // incidental); everything else is a solo (OneForOne)
                    // backoff for the id it carries.
                    if matches!(cycle, CycleState::Waiting { key } if *key == expired.key()) {
                        *cycle = CycleState::Idle;
                        for id in children.cycling_rebuild_ids() {
                            rebuild_child(children, &supervisor, id);
                        }
                    } else {
                        rebuild_child(children, &supervisor, expired.into_inner());
                    }
                }
            }
```

The mailbox arm's `apply_supervision_op` call gains `retries`/`cycle` args. Note `cycling_rebuild_ids` clears flags BEFORE `rebuild_child` runs, so the supersession guard in `rebuild_child` passes for cycle rebuilds and blocks only solo strays.

- [ ] **Step 4: Run to verify PASS**: `nix develop --command cargo test -p bombay-core --lib one_for_all_smoke`
- [ ] **Step 5: Whole lib suite + clippy** (2a regressions — especially `supervisor_serves_messages_during_backoff`, `escalation_stops_surviving_children`): `nix develop --command bash -c 'cargo test -p bombay-core --lib && cargo clippy -p bombay-core'`
- [ ] **Step 6: Commit**

```bash
cargo fmt && git add bombay-core/src/actor && git commit -m "core(supervision): wire the set-cycle into the supervised loop [#199]"
```

---

### Task 6: Behavioral — OneForAll invariants (card boxes 1, 2, 6)

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` supervision test module

Three tests, each TDD'd individually (write → fail → adjust if needed → pass → next). They need a stop-order-recording worker: extend the module with

```rust
        /// A worker that records its stop on a shared tape — the reverse-birth
        /// teardown assertion reads the tape.
        struct TapeWorker;
        type Tape = Arc<Mutex<Vec<&'static str>>>;
        // Args = (tag, tape, id_tx); on_stop pushes tag. Message type Idle2.
        #[derive(Debug)]
        struct Idle2;
        impl Msg for Idle2 {}
        impl Mailboxed for TapeWorker { type Msg = Idle2; }
        impl crate::actor::Actor for TapeWorker {
            type Args = (&'static str, Tape, flume::Sender<(&'static str, ActorId)>);
            type Error = Infallible;
            async fn on_start(
                (tag, tape, id_tx): Self::Args,
                this: ActorRef<Self>,
            ) -> Result<Self, Self::Error> {
                let _ = id_tx.send((tag, this.id()));
                Ok(Self { tag, tape })
            }
            // struct TapeWorker { tag: &'static str, tape: Tape } — adjust decl above
            async fn handle(&mut self, _: Idle2, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
                Ok(())
            }
            async fn on_stop(
                &mut self,
                _: crate::actor::WeakActorRef<Self>,
                _: crate::error::ActorStopReason,
            ) -> Result<(), Self::Error> {
                self.tape.lock().expect("tape").push(self.tag);
                Ok(())
            }
        }
        impl Watch for TapeWorker {}
```

- [ ] **Step 1: `one_for_all_restarts_all_children`** — supervise three `TapeWorker`s (`"a"`, `"b"`, `"c"`, all `Permanent`) under `AllSup`; kill `b`'s incarnation via a crash-capable variant (give `TapeWorker` a `Boom` message that panics — add `Boom` to `Idle2` as an enum, or reuse `Worker` for the crasher and `TapeWorker` for the siblings: supervise `a: TapeWorker, b: Worker, c: TapeWorker`). Assert: three fresh `(tag, id)` pairs arrive with ids ≠ originals; sup still alive (`sup.is_alive()`).

```rust
        #[tokio::test(start_paused = true)]
        async fn one_for_all_restarts_all_children() {
            let sup = AllSup::spawn_supervised(());
            let tape: Tape = Arc::new(Mutex::new(Vec::new()));
            let (tag_tx, tag_rx) = flume::unbounded::<(&'static str, ActorId)>();
            let (id_tx, id_rx) = flume::unbounded::<ActorId>();
            let senders: Senders = Arc::new(Mutex::new(Vec::new()));

            let sup_ref = &sup;
            let mut tape_factory = |tag: &'static str| {
                let (tape, tag_tx) = (Arc::clone(&tape), tag_tx.clone());
                move || TapeWorker::spawn((tag, Arc::clone(&tape), tag_tx.clone()))
            };
            let a0 = bounded(sup_ref.supervise(RestartPolicy::Permanent, tape_factory("a"))).await.expect("alive");
            let b0 = supervise_worker(sup_ref, RestartPolicy::Permanent, id_tx, &senders).await;
            let c0 = bounded(sup_ref.supervise(RestartPolicy::Permanent, tape_factory("c"))).await.expect("alive");
            let (_, a_id) = recv_tag(&tag_rx).await;
            assert_eq!(a_id, a0);
            assert_eq!(recv_id(&id_rx).await, b0);
            let (_, c_id) = recv_tag(&tag_rx).await;
            assert_eq!(c_id, c0);

            send_cmd(&senders, 0, Cmd::Crash); // b crashes

            // All three come back fresh.
            let b1 = recv_id(&id_rx).await;
            let (tag1, r1) = recv_tag(&tag_rx).await;
            let (tag2, r2) = recv_tag(&tag_rx).await;
            assert_ne!(b1, b0);
            assert!(![a0, c0].contains(&r1) && ![a0, c0].contains(&r2), "fresh ids");
            assert_eq!({ let mut t = [tag1, tag2]; t.sort_unstable(); t }, ["a", "c"]);
            drop(sup);
        }
```

with the tiny helper (put beside `recv_id`):

```rust
        async fn recv_tag(
            rx: &flume::Receiver<(&'static str, ActorId)>,
        ) -> (&'static str, ActorId) {
            tokio::time::timeout(terminate_bound(), rx.recv_async())
                .await
                .expect("a tagged incarnation must be reported")
                .expect("channel open")
        }
```

Run → FAIL first (before Task 5 it would; here it should PASS if Task 5 wired correctly — if it passes immediately, verify the assertion bites by temporarily flipping `AllSup` to default strategy and watching it fail; note that flip in the commit message body as the fail-first evidence). Commit:

```bash
cargo fmt && git add bombay-core/src/actor/spawn.rs && git commit -m "test(supervision): one_for_all_restarts_all_children [#199]"
```

- [ ] **Step 2: `one_for_all_stop_order_reverse_start_order`** — same topology, all three `TapeWorker` (crash `b` via a `Boom` message variant added to `Idle2` → rename the message enum `TapeMsg { Idle, Boom }`, `Boom` panics in `handle`). After the cycle: tape must read `["c", "a"]` — cancel order is reverse birth among the CANCELLED members (`b` crashed, so its `on_stop` runs on the panic path first; assert the SUFFIX of the tape after b's entry is `["c", "a"]`). Under `current_thread` + `start_paused` the wake order is deterministic. If flakiness appears, assert instead on the recorded CANCEL order via a `CancellationToken`-observing `run` loop — but try the tape first. Commit as above with its own message.

- [ ] **Step 3: `set_restart_counts_once_against_budget`** — `AllSup` with `a` on `RestartConfig::new(Permanent).with_max_restarts(1)` and `b` (the repeat-crasher) on default config. Crash `b` twice (two cycles). If a set cycle wrongly charged siblings, `a`'s budget (max 1 consecutive) would trip on the second cycle and kill the supervisor. Assert: after two cycles, `sup.is_alive()`, and both rebuilds arrived. Then crash the CURRENT `a` incarnation once directly and assert the supervisor survives (its counter holds 1 = its own first failure, proving the sibling cycles contributed 0). Commit.

---

### Task 7: Behavioral — RestForOne + Never (card boxes 3, 4, 5)

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` supervision test module (add `RestSup` mirroring `AllSup` with `RestForOne`)

- [ ] **Step 1: `rest_for_one_restarts_failed_plus_younger`** — `RestSup`, children `a`, `b`, `c` (birth order; `b` = crasher `Worker`, `a`/`c` = `TapeWorker`). Crash `b`. Assert: fresh `b'` and `c'` ids arrive; **no** fresh `a` arrives within a 120 s virtual-time window (the `transient_child_that_exits_normally_is_not_rebuilt` idiom); `a`'s tape has no stop entry; `a`'s original id still live (send it an `Idle` and expect no `TellError`). Commit.

- [ ] **Step 2: `rest_for_one_of_last_child_equals_one_for_one`** — crash `c` (the youngest). Assert: exactly one fresh id (`c'`), tape empty (no sibling stopped), 120 s window silent otherwise. Commit.

- [ ] **Step 3: `never_children_excluded_from_set_restarts`** — `AllSup` with `a` `Permanent` (crasher), `n` `Never` (`TapeWorker`). Crash `a`. Assert: `n`'s tape records its stop (stopped with the set); fresh `a'` arrives; NO fresh `n` in the 120 s window. Commit.

---

### Task 8: Behavioral — mid-cycle races (card-derived boxes 9–13)

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` supervision test module

Uncooperative children for real teardown windows: add a `Stubborn` worker whose `run` ignores cancellation (its `handle` loops on a never-resolving future once poked, or simplest: its `on_stop` sleeps past the grace — the existing `stop_child_aborts_a_child_that_ignores_cancellation` test (~3886) has the established idiom; reuse its actor type).

- [ ] **Step 1: `sibling_death_during_teardown_is_absorbed`** (box 9) — `AllSup`; `a` = crasher, `b` = stubborn (grace 5 s), `b` panics *mid-teardown* (send `Boom` racing the cancel — under paused time, send after the crash but before advancing past grace). Assert: cycle still completes with exactly one rebuild wave (two fresh ids), supervisor alive, and — the counter leg — `b`'s budget untouched (crash `b`'s fresh incarnation `max_restarts` times afterward; the supervisor must survive exactly that many, proving the mid-teardown panic charged nothing).
- [ ] **Step 2: `unsupervise_during_cycle_completes_cycle_without_rebuilding_removed`** (box 10) — `AllSup`; `a` crasher, `b` stubborn. Crash `a`, then `unsupervise(b)` while `b`'s grace runs. Assert: fresh `a'` arrives (cycle completed — the wedge counterexample), no fresh `b` in the window.
- [ ] **Step 3: `widen_supersedes_armed_rebuild_deadline`** (box 11) — `RestSup`; `a`, `b`, `c`; crash `c` (suffix `{c}` alone → `Waiting` immediately with its 100 ms deadline); within the Waiting window (advance < 100 ms) crash `a` → widen to `{a,b,c}` (`b` live → real teardown). Assert: exactly THREE fresh ids total (one wave, no early partial rebuild of `c`), and no id appears twice.
- [ ] **Step 4: `solo_backoff_deadline_superseded_by_cycle`** (box 12) — needs a solo deadline coexisting with a cycle, which `OneForOne` arms: use `RestSup` with children `a`, `b`: crash `b` → suffix `{b}` = solo-ish cycle... NOT distinguishing. Correct construction: `RestSup`, children `a`, `b`, `c`. Crash `c` → cycle `{c}` `Waiting` (this IS the set path). For a genuine SOLO deadline under a set-strategy supervisor there is none — solo deadlines only arise under `OneForOne`. The unit test in Task 4 (`rebuild_child_is_superseded_for_cycling_entries`) plus this structural fact cover box 12; record that in the test's doc comment on the Task 4 unit test and SKIP a separate behavioral test (note the deferral in the PR body).
- [ ] **Step 5: `supervisor_serves_messages_during_set_teardown`** (box 13) — `AllSup` whose `handle` records receipts; `a` crasher, `b` stubborn (5 s grace). Crash `a`; while `b`'s grace runs (virtual time not yet advanced past it), `tell` the supervisor and assert the reply/receipt arrives BEFORE advancing time past the grace — the anti-OTP-block property, asserted directly.
- [ ] Commit after each test:

```bash
cargo fmt && git add bombay-core/src/actor/spawn.rs && git commit -m "test(supervision): <test_name> [#199]"
```

---

### Task 9: Heterogeneous children (card box 7)

**Files:**
- Modify: `bombay-core/src/actor/spawn.rs` supervision test module

- [ ] **Step 1: `supervisor_signals_heterogeneous_children`** — `AllSup` supervising one `Worker` AND one `TapeWorker` (two distinct actor types through the same erased `RebuildFactory` edges — the honest scope of #122-#10). Crash the `Worker`. Assert both types rebuilt: fresh `Worker` id on `id_rx`, fresh `TapeWorker` tag+id on `tag_rx`, sup alive. Sequence category: then crash the fresh `TapeWorker` (`Boom`) and assert the second cycle also rebuilds both — the two erased edges survive reuse.
- [ ] **Step 2: Run, verify, commit** as before.

---

### Task 10: DST — the three storm invariants

**Files:**
- Modify: `bombay-core/tests/dst_races.rs` (house idioms: seeded, `start_paused`, `terminate_bound`, current-thread)

All three drive a `RestSup`/`AllSup`-style supervisor (redeclare minimal actor types locally — integration tests can't reach `spawn.rs`'s test module; only public API: `Supervisor`, `SupervisionStrategy`, `supervise`, `RestartConfig`). Recorder = `flume::unbounded<(&'static str, ActorId)>` birth tape + virtual timestamps via `tokio::time::Instant::now()`.

- [ ] **Step 1: `dst_restart_storm_deterministic`** — N=4 children under `OneForAll`, per-seed scripted crash schedule (crash times drawn from `fastrand::Rng::with_seed(seed)`), run two identical passes and one different-seed pass:

```rust
async fn storm_trace(seed: u64) -> Vec<(u64, &'static str)> {
    // spawn AllSup + 4 tagged children; drive `steps` crashes at seeded virtual
    // times against whichever incarnation currently holds each tag; record
    // (virtual_ms_since_start, tag) for every rebuild the tape reports; return
    // the trace once quiescent (bounded by terminate_bound each await).
}

#[tokio::test(start_paused = true)]
async fn dst_restart_storm_deterministic() {
    let a = storm_trace(42).await;
    let b = storm_trace(42).await;
    assert_eq!(a, b, "same seed ⇒ identical rebuild interleaving (times AND order)");
    let c = storm_trace(43).await;
    assert_ne!(a, c, "different seed ⇒ the jitter actually varies the schedule");
    assert!(!a.is_empty(), "the storm actually stormed");
}
```

The two runs must be **separate runtimes** (each `storm_trace` call inside its own `#[tokio::test]`-provided runtime is NOT separate — build each trace inside `tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true)` manually, the established pattern for multi-run DST comparisons; if none exists in the file yet, this is the first — follow the model comment in the spec).
- [ ] **Step 2: `dst_concurrent_link_unlink_die`** — seeded op tape over `RestSup`: at seeded times pick from {crash current incarnation of tag, `unsupervise(current id of tag)`, `supervise(fresh tagged child)`}; after quiescence assert (a) **no rebuild for any id that was unsupervised before its death** (track removed ids; the birth tape must contain no successor born from a removed tag after removal unless re-supervised) and (b) supervisor alive (no wedge — every await bounded). Multiple seeds in one test (loop 8 seeds).
- [ ] **Step 3: `dst_backoff_distribution_measured`** — single `Permanent` child under `AllSup` with jitter 20%: crash it k=20 times across seeds {1,2,3}, record delay between each death and its rebuild (virtual time makes this exact); assert every delay ∈ `[base(n), base(n)*1.2]` for its attempt number n and that delays across seeds differ (jitter live). Print the collected distribution with `eprintln!` (visible via `--nocapture`) — the numbers that resolve the spec's "expected to move" note.
- [ ] **Step 4: Run all three**: `nix develop --command cargo test -p bombay-core --test dst_races dst_ -- --nocapture`
- [ ] **Step 5: Commit**

```bash
cargo fmt && git add bombay-core/tests/dst_races.rs && git commit -m "test(dst): restart storm, link/unlink races, backoff distribution [#199]"
```

- [ ] **Step 6:** Post the measured distribution as a comment on #199 with `gh issue comment 199 --repo devrandom-labs/bombay --body "..."` (tuning-defaults resolution — the card bullet requires the numbers land on the card).

---

### Task 11: Wiring — mutants baseline, README, gate, PR

**Files:**
- Modify: `mutants-baseline.json`, `README.md`, `docs/testing/coverage-baseline.md`

- [ ] **Step 1: mutants baseline entries.** Every new fn gets a floor entry (Unaccounted otherwise — the house rule). New fns:

```
bombay-core/src/actor/supervision.rs::Children::position
bombay-core/src/actor/supervision.rs::Children::flag_cycle
bombay-core/src/actor/supervision.rs::Children::absorb_cycling_death
bombay-core/src/actor/supervision.rs::Children::cycling_rebuild_ids
bombay-core/src/actor/kind.rs::cycle_count_down
bombay-core/src/actor/kind.rs::start_or_widen_cycle
bombay-core/src/actor/mod.rs::Supervisor::supervision_strategy
```

Count viable mutants per fn from a scoped run:

```bash
nix develop --command cargo mutants --package bombay-core -f bombay-core/src/actor/supervision.rs -f bombay-core/src/actor/kind.rs --timeout 60 --list
```

then run the sweep and set each floor to the caught count:

```bash
nix develop --command cargo mutants --package bombay-core -f bombay-core/src/actor/supervision.rs -f bombay-core/src/actor/kind.rs --timeout 60
```

Expected: `0 missed` (survivors mean a test gap — fix the test, not the baseline). Timeouts on unbounded awaits are OUR bug (memory: #148/#179) — bound them.
- [ ] **Step 2: Verify the ratchet gate**: `nix build .#mutants` (must log `building '...drv'` — silent = cached = didn't run).
- [ ] **Step 3: README** — public API changed: add the strategy bullet to the supervision section ("per-supervisor `supervision_strategy()`: `OneForOne` (default) / `RestForOne` / `OneForAll` — set restarts per ADR-0014") + one usage line in the example if the supervision example exists. Update `docs/testing/coverage-baseline.md` for the new test surface.
- [ ] **Step 4: MIRI check** — new tests are all `#[tokio::test]` (MIRI lane drives them; any proptest added must be `prop_`-prefixed — none planned). Confirm no new proptests violate the naming: `rg 'proptest|prop_' bombay-core/src/actor/supervision.rs bombay-core/src/actor/kind.rs`.
- [ ] **Step 5: The gate**: `git add -A && nix flake check` (tracked-files rule). Expected: all checks pass.
- [ ] **Step 6: Commit + PR**

```bash
git add -A && git commit -m "core(supervision): baseline, README, coverage for set strategies [#199]"
git push -u origin feat/199-set-strategies
gh pr create --repo devrandom-labs/bombay --title "core(supervision): restart-set strategies — OneForAll/RestForOne, widen-coalesce cycle, DST storm [#120 slice 3] (#199)" --body "<summary: strategies + ADR-0014 + invariant checklist mapping each card box to its test/file; deferrals: box 12 covered at unit level (structural: solo deadlines cannot coexist with set strategies), noted per card rule>"
```

PR body must map **every card checkbox** to a named test/file or an explicit deferral (the #149 lesson). No Claude attribution.

---

## Self-Review Notes

- **Spec coverage:** boxes 1–8 → Tasks 6/7/9/2; 9–13 → Tasks 8/4; DST ×3 → Task 10; wiring ×4 → Tasks 5/11. Box 12 is downgraded to unit-level with a structural argument — flagged for the PR body as an explicit deferral decision.
- **Type consistency:** `flag_cycle(from) -> (SmallVec<[(ChildHandle, Duration); 4]>, u32)`, `absorb_cycling_death -> Option<bool>`, `cycling_rebuild_ids -> SmallVec<[ActorId; 4]>`, `CycleState::{Idle, Tearing{awaiting, backoff}, Waiting{key}}` used identically in Tasks 3–5.
- **Known risk:** the reverse-stop-order tape assertion (Task 6 Step 2) rides current-thread wake determinism; fallback documented inline (assert cancel issuance instead). The Task 5 smoke test may pass immediately after wiring — the strategy-flip fail-first check compensates.
