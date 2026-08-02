# Behavior Algebra Prototype (card #295, pass 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship ADR-0030 (the Behavior algebra, sealed-first) plus the synchronous essence-fold prototype — every capability as a layer, proven by per-brick tests, a new-brick closure proof, and a model-vs-real trace-equality oracle.

**Architecture:** One sync `Behavior` trait (the algebra's one object) + a ~12-line fold in a directory-style integration-test crate (`crates/core/tests/behavior_algebra/`), using only bombay's public API (`Step`, `Never`, `Disposition`, `Deferred`) so #298 can lift it verbatim as `bombay-matrix`'s frozen reference. Five model layers mirror the five capabilities; the oracle drives model and real machinery with one script and compares probe sequences.

**Tech Stack:** Rust stable (repo toolchain), tokio (oracle only, `start_paused`), proptest (dev-dep, `prop_` prefix rule). All commands via `nix develop --command`. `nix flake check` is the gate; **`git add` before checking — untracked files are invisible to the flake.**

**Conventions binding every task:** god-level clippy applies to tests (workspace-wide flake clippy): fns ≤ 80 lines, cognitive complexity ≤ 9, no `unwrap` (use `expect`), doc comments on `pub` items. Commits: conventional with scope + `[#295]`, **no Claude/Anthropic attribution lines**.

---

## File structure

- Create: `docs/adr/0030-behavior-algebra.md` — the ADR (Task 1)
- Create: `crates/core/tests/behavior_algebra/main.rs` — test-crate root, module decls
- Create: `crates/core/tests/behavior_algebra/model.rs` — the frozen-reference algebra: `Exit`, `Behavior`, `run`, five layers (Tasks 2–8)
- Create: `crates/core/tests/behavior_algebra/new_brick.rs` — out-of-module rate-limit layer, closure proof (Task 9)
- Create: `crates/core/tests/behavior_algebra/oracle.rs` — model-vs-real trace equality (Task 10)
- Modify: `docs/testing/coverage-baseline.md` — new test surface (Task 11)

Layer names in `model.rs` deliberately mirror the real capabilities (`Phased`, `Watching`, …); consumers qualify through `model::` so there is no ambiguity.

---

### Task 1: ADR-0030

**Files:**
- Create: `docs/adr/0030-behavior-algebra.md`

- [ ] **Step 1: Write the ADR** with exactly this content:

```markdown
# ADR-0030: The Behavior algebra — one object, capabilities as layers

Date: 2026-08-02 · Status: accepted (algebra; encodings deferred, see Open
doors) · Card: #295 · Spec: `docs/superpowers/specs/2026-08-02-behavior-algebra-design.md`
· Predecessors: ADR-0026 (constraint 2), ADR-0028 (two-layer law), ADR-0029
(one verdict family — precondition)

## Context

The capability layer is three sealed hooks (`Admission`/`Replay`,
`DeadlineHook`) projected onto three hand-written select loops
(`PlainRun`/`LinkedRun`/`SupervisedRun`). The logic and the scheduling are
welded: interleavings are testable only through a spawned runtime, the arm
order is a comment maintained in three places, and every new event source
is a new hand-written loop. ADR-0028 named this card as the successor that
implements the algebra those hooks project.

Directives recorded from the design session: everything is a layer;
extreme testability everywhere; third-party openness is NOT a goal — the
algebra ships sealed, unsealing stays a one-line decision on the card.

## Decision

### The one object: `Behavior`

Renamed from the cards' "machine" (Joel, design session): the object is
Agha's *behavior* plus its event alphabet (precision caveat: Agha's
behavior is the one-communication function alone; ours bundles the
sources).

    trait Behavior {
        type Event;
        type Ph;      // the become-menu still exposed upward (Never when erased)
        type Error;
        async fn step(&mut self, ev: Self::Event)
            -> Result<Step<Self::Ph, ActorStopReason>, Self::Error>;
    }

State lives in `&mut self` — the fold accumulator. Events, not calls: a
layer that adds a source extends the alphabet as a sum type; the step is
total over its alphabet; there is no second hook surface. Only a fully
erased behavior (`Ph = Never`) is runnable.

### The transformer: a capability is a layer

The tower pair, literally: each capability is a type constructor
`C<B>: Behavior where B: Behavior` (as `Timeout<S>: Service`), plus a thin
assembly trait for #286's builder (tower `Layer`):

    trait Capability<B: Behavior> { type Out: Behavior; fn apply(self, inner: B) -> Self::Out; }

Closure under composition is structural (`Out: Behavior`). Both traits are
SEALED — the laws bind only the five core layers.

### The five capabilities as layers

Base = handler floor · `Stashing`/`Deadlined`/`Watching` = source-adding
layers routing their own events · `Phased` = both planes (gate wraps the
step; its seats add events) · `Supervising` = source-adding layer whose
reactions restart CHILDREN — the outer fold over child folds, still ONE
kind of thing (demonstrated by the prototype's model, not asserted).

### Laws in signatures

1. Commit-after-Ok: a layer is the caller of `inner.step(ev).await?` —
   commit is syntactically after the `?`.
2. One deadline arm as min-fold over source wakes (encoding deferred).
3. No-silent-drop: total classification; `Defer` without a seat stays
   uncompilable (ADR-0028, unchanged).
4. Priority = stack order: outer layers' events outrank inner ones;
   `Supervising<Watching<Deadlined<Base>>>` derives today's arm order
   (encoding deferred with the select work).
5. Agha floor as upper bound: anything not derivable from typed-become +
   merged sources is accidental structure.

### Typed-become audit — findings (prototype pass)

- The pending-goto side channel (`Ctx::goto` + `commit()` + the D3
  code-order proof) is accidental structure: with the verdict carrying
  `Goto` (ADR-0029's family used as designed), committing a phase on `Err`
  is UNREPRESENTABLE — D3's test shrinks from a code-order proof to a
  tautology. The real-surface migration of this finding rides the
  implementation pass (and reshapes with #286's handler API).
- The two-queue stash (held vs released) re-derives itself in the model's
  `Phased` drain — it is essential structure, not an implementation
  artifact.
- Replay-batch atomicity is preserved BY CONSTRUCTION: the layer drains
  its batch inside its own `step`, so no outer event interleaves —
  resolving the drain-as-source question in favor of current behavior.

## Alternatives rejected

- Contribution record over a fixed spine (today's CapSet with more seats):
  openness per-seat, not structural; fails "everything is a layer".
- Effect commands (free-monad): alloc/`dyn` on the hot path; laws demoted
  from signatures to interpreter convention.
- Unsealed-first: forces every law to bind strangers' code for openness
  nobody needs yet.

## Open doors (each gated, none silent)

- The async merged-select encoding over an open source set: gated on
  #298's monomorphization-slope measurements (required pre-reading, card
  comment). Stays on #295.
- Unsealing `Behavior`/`Capability`/`DeadlineCx` + the out-of-crate
  capability proof: contingent on Joel's unseal decision; model-grade
  closure proof ships in this pass.
- The full oracle-over-derived-loop (#266 6-scenario, 24-point lattice):
  rides the implementation pass; this pass ships model-vs-real equality
  for plain/phased/deadline scenarios.

## Consequences

- The prototype (`crates/core/tests/behavior_algebra/`) is the executable
  spec and doubles as bombay-matrix's frozen reference (#298).
- No public API change in this pass; the run loop is untouched.
- The implementation pass derives the three loops from the algebra and
  must keep the #266-family oracles green unchanged.
```

- [ ] **Step 2: Commit**

```bash
git add docs/adr/0030-behavior-algebra.md
git commit -m "docs(adr): ADR-0030 behavior algebra — one object, capabilities as layers, sealed-first [#295]"
```

---

### Task 2: Test-crate scaffold + `Exit` + `Behavior` + the fold (**USER CONTRIBUTION**)

**Files:**
- Create: `crates/core/tests/behavior_algebra/main.rs`
- Create: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the scaffold and the failing fold test**

`crates/core/tests/behavior_algebra/main.rs`:

```rust
//! Card #295 executable prototype: the Behavior algebra as a synchronous
//! fold (ADR-0030). This crate is the frozen-reference candidate for
//! bombay-matrix (#298) — public bombay API only.

mod model;
mod new_brick;
mod oracle;
```

(Comment out `mod new_brick;` and `mod oracle;` until their tasks — an
empty module file also works; prefer creating empty files now so `main.rs`
never changes again.)

`crates/core/tests/behavior_algebra/model.rs` (trait + Exit + tests; NO
`run` body yet):

```rust
//! The frozen-reference algebra: one object ([`Behavior`]), one fold
//! ([`run`]), capabilities as layers (ADR-0030).

use std::collections::VecDeque;

use bombay::capability::{Deferred, Disposition, Never, Step};

/// The model's exit vocabulary — the `R` parameter of [`Step`]
/// (ADR-0029 used as designed). The oracle maps it onto
/// `ActorStopReason` kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// Clean self-stop (`Flow::Stop(Normal)`'s model image).
    Normal,
    /// Sources exhausted — the mailbox-closed / ref-count-collection image.
    Collected,
    /// A watch layer propagated a linked peer's death.
    LinkDied(u64),
}

/// The one object (sync projection — spec §prototype): state in
/// `&mut self`, one total `step` over the event alphabet.
pub trait Behavior {
    /// The event alphabet this behavior folds over.
    type Event;
    /// The become-menu still exposed upward (`Never` once erased).
    type Ph;
    /// The controlled-crash type.
    type Error;
    /// One fold step: typed become — continue, switch, or stop.
    fn step(&mut self, ev: Self::Event) -> Result<Step<Self::Ph, Exit>, Self::Error>;
}

#[cfg(test)]
mod fold_tests {
    use super::{Behavior, Exit, run};
    use bombay::capability::{Never, Step};

    /// Counts events; stops normally at the third.
    struct Countdown(u32);

    impl Behavior for Countdown {
        type Event = ();
        type Ph = Never;
        type Error = &'static str;
        fn step(&mut self, (): ()) -> Result<Step<Never, Exit>, &'static str> {
            self.0 += 1;
            match self.0 {
                3 => Ok(Step::Stop(Exit::Normal)),
                9 => Err("boom"),
                _ => Ok(Step::Continue),
            }
        }
    }

    #[test]
    fn fold_stops_at_the_verdict_and_consumes_no_further_events() {
        let mut b = Countdown(0);
        let out = run(&mut b, std::iter::repeat_n((), 10));
        assert_eq!(out, Ok(Exit::Normal), "Stop's exit rides out");
        assert_eq!(b.0, 3, "no event after the Stop verdict is folded");
    }

    #[test]
    fn fold_reports_collected_when_events_run_dry() {
        let mut b = Countdown(0);
        let out = run(&mut b, std::iter::repeat_n((), 2));
        assert_eq!(out, Ok(Exit::Collected), "exhausted sources = collection");
    }

    #[test]
    fn fold_surfaces_a_controlled_crash_unchanged() {
        let mut b = Countdown(8);
        let out = run(&mut b, std::iter::repeat_n((), 3));
        assert_eq!(out, Err("boom"), "Err short-circuits the fold");
        assert_eq!(b.0, 9, "the crashing step was the last one folded");
    }
}
```

- [ ] **Step 2: Run to verify red**

Run: `git add crates/core/tests/behavior_algebra && nix develop --command cargo nextest run -p bombay --test behavior_algebra`
Expected: COMPILE ERROR — `cannot find function 'run' in this scope` (this is the red: the fold does not exist).

- [ ] **Step 3: USER CONTRIBUTION — Joel writes the fold.** Prepare this in `model.rs` above the tests, then hand over:

```rust
/// The essence-fold (~ADR-0028's "async fold of one step shape", sync
/// projection): drive a fully-erased behavior over a trace.
///
/// Laws to carry (the tests above pin them):
/// - `Stop(exit)` ends the fold immediately — later events are never seen.
/// - `Goto` is unconstructible at `Ph = Never` — discharge with an empty match.
/// - `Err` short-circuits (controlled crash rides out unchanged).
/// - Trace exhaustion is collection, not success: `Exit::Collected`.
pub fn run<B: Behavior<Ph = Never>>(
    b: &mut B,
    events: impl IntoIterator<Item = B::Event>,
) -> Result<Exit, B::Error> {
    // TODO(joel): the fold body — the ~10 most design-bearing lines of #295.
    unimplemented!()
}
```

Ask Joel to replace the body. Guidance to give him: it is a `for` loop, a
`match` on `b.step(ev)?` with three arms, and one line after the loop; the
`Goto` arm is `match never {}`.

- [ ] **Step 4: Run to verify green**

Run: `nix develop --command cargo nextest run -p bombay --test behavior_algebra`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra
git commit -m "test(algebra): Behavior object + essence-fold — the #295 executable prototype floor [#295]"
```

---

### Task 3: `Base` layer (the handler floor)

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing tests** (append to `model.rs`):

```rust
#[cfg(test)]
mod base_tests {
    use super::{Base, Exit, run};
    use bombay::capability::{Never, Step};

    #[derive(Debug, PartialEq, Eq)]
    enum Msg {
        Work(u64),
        Quit,
    }

    fn floor() -> Base<Vec<u64>, Msg, Never, &'static str> {
        Base {
            state: Vec::new(),
            handle: |seen, msg| match msg {
                Msg::Work(id) => {
                    seen.push(id);
                    Ok(Step::Continue)
                }
                Msg::Quit => Ok(Step::Stop(Exit::Normal)),
            },
        }
    }

    #[test]
    fn base_runs_the_handler_over_its_own_state() {
        let mut b = floor();
        let out = run(&mut b, vec![Msg::Work(7), Msg::Work(8), Msg::Quit]);
        assert_eq!(out, Ok(Exit::Normal));
        assert_eq!(b.state, vec![7, 8], "the handler folded over &mut state");
    }
}
```

- [ ] **Step 2: Run to verify red**

Run: `nix develop --command cargo nextest run -p bombay --test behavior_algebra`
Expected: COMPILE ERROR — `cannot find type 'Base'`.

- [ ] **Step 3: Implement** (append to `model.rs`, above the test mods):

```rust
/// The floor layer: a plain actor = state + handler. `P` is the become
/// menu the handler exposes upward (`Never` for a one-phase actor).
pub struct Base<S, M, P, E> {
    /// The user state the fold accumulates into.
    pub state: S,
    /// The handler — fn pointer, not a closure: the model stays nameable.
    pub handle: fn(&mut S, M) -> Result<Step<P, Exit>, E>,
}

impl<S, M, P, E> Behavior for Base<S, M, P, E> {
    type Event = M;
    type Ph = P;
    type Error = E;
    fn step(&mut self, ev: M) -> Result<Step<P, Exit>, E> {
        (self.handle)(&mut self.state, ev)
    }
}
```

- [ ] **Step 4: Run to verify green** — same command, expected PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Base layer — the handler floor of the model [#295]"
```

---

### Task 4: `Deadlined` layer

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing test** (append):

```rust
#[cfg(test)]
mod deadlined_tests {
    use super::{Base, Deadlined, Exit, Timed, run};
    use bombay::capability::{Never, Step};

    #[test]
    fn deadline_events_route_to_the_reaction_and_inner_events_pass_through() {
        let mut b = Deadlined {
            inner: Base::<Vec<&'static str>, &'static str, Never, &'static str> {
                state: Vec::new(),
                handle: |seen, m| {
                    seen.push(m);
                    Ok(Step::Continue)
                },
            },
            on_deadline: |inner| {
                inner.state.push("timeout");
                Ok(Step::Stop(Exit::Normal))
            },
        };
        let out = run(&mut b, vec![Timed::Event("a"), Timed::Deadline, Timed::Event("b")]);
        assert_eq!(out, Ok(Exit::Normal), "the reaction's verdict rides out");
        assert_eq!(
            b.inner.state,
            vec!["a", "timeout"],
            "expiry routed to the reaction; the post-stop event never folded",
        );
    }
}
```

- [ ] **Step 2: Run to verify red** — compile error: `cannot find type 'Deadlined'`.

- [ ] **Step 3: Implement**:

```rust
/// A time-armed source's alphabet extension: expiry or the inner event.
#[derive(Debug, PartialEq, Eq)]
pub enum Timed<E> {
    /// The deadline source fired.
    Deadline,
    /// A pass-through inner event.
    Event(E),
}

/// The deadline capability as a layer: adds the expiry event, routes it
/// to the reaction, forwards everything else.
pub struct Deadlined<B: Behavior> {
    /// The wrapped behavior.
    pub inner: B,
    /// The expiry reaction — reads/writes the inner behavior.
    pub on_deadline: fn(&mut B) -> Result<Step<Never, Exit>, B::Error>,
}

impl<B: Behavior> Behavior for Deadlined<B> {
    type Event = Timed<B::Event>;
    type Ph = B::Ph;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<B::Ph, Exit>, B::Error> {
        match ev {
            Timed::Event(inner_ev) => self.inner.step(inner_ev),
            Timed::Deadline => Ok(match (self.on_deadline)(&mut self.inner)? {
                Step::Continue => Step::Continue,
                Step::Goto(never) => match never {},
                Step::Stop(exit) => Step::Stop(exit),
            }),
        }
    }
}
```

- [ ] **Step 4: Run to verify green.**

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Deadlined layer — expiry as an alphabet extension [#295]"
```

---

### Task 5: `Watching` layer

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing tests** (append):

```rust
#[cfg(test)]
mod watching_tests {
    use super::{Base, Exit, Linked, Watching, otp_propagation, run};
    use bombay::capability::{Never, Step};

    fn watcher() -> Watching<Base<Vec<u64>, u64, Never, &'static str>> {
        Watching {
            inner: Base {
                state: Vec::new(),
                handle: |seen, id| {
                    seen.push(id);
                    Ok(Step::Continue)
                },
            },
            on_link_died: otp_propagation,
        }
    }

    #[test]
    fn an_abnormal_linked_death_propagates_with_the_carried_reason() {
        let mut b = watcher();
        let out = run(
            &mut b,
            vec![
                Linked::Event(1),
                Linked::LinkDied { peer: 42, abnormal: true },
            ],
        );
        assert_eq!(out, Ok(Exit::LinkDied(42)), "the watch corner CARRIES the reason");
    }

    #[test]
    fn a_normal_death_notice_is_absorbed() {
        let mut b = watcher();
        let out = run(
            &mut b,
            vec![Linked::LinkDied { peer: 42, abnormal: false }, Linked::Event(2)],
        );
        assert_eq!(out, Ok(Exit::Collected), "normal death absorbed; fold continues");
        assert_eq!(b.inner.state, vec![2]);
    }
}
```

- [ ] **Step 2: Run to verify red** — compile error on `Watching`/`Linked`/`otp_propagation`.

- [ ] **Step 3: Implement**:

```rust
/// The link source's alphabet extension: a death notice or the inner event.
#[derive(Debug, PartialEq, Eq)]
pub enum Linked<E> {
    /// A watched/linked peer stopped.
    LinkDied {
        /// The dead peer's model id.
        peer: u64,
        /// Whether the stop was abnormal (the OTP propagation trigger).
        abnormal: bool,
    },
    /// A pass-through inner event.
    Event(E),
}

/// The watch capability as a layer: adds the death-notice event and
/// routes it to the policy.
pub struct Watching<B: Behavior> {
    /// The wrapped behavior.
    pub inner: B,
    /// The death reaction (the model image of `WatchPolicy`).
    pub on_link_died: fn(&mut B, u64, bool) -> Result<Step<Never, Exit>, B::Error>,
}

/// The default policy's model image: propagate an abnormal linked death,
/// absorb everything else (`OtpPropagation`).
pub fn otp_propagation<B: Behavior>(
    _: &mut B,
    peer: u64,
    abnormal: bool,
) -> Result<Step<Never, Exit>, B::Error> {
    if abnormal {
        Ok(Step::Stop(Exit::LinkDied(peer)))
    } else {
        Ok(Step::Continue)
    }
}

impl<B: Behavior> Behavior for Watching<B> {
    type Event = Linked<B::Event>;
    type Ph = B::Ph;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<B::Ph, Exit>, B::Error> {
        match ev {
            Linked::Event(inner_ev) => self.inner.step(inner_ev),
            Linked::LinkDied { peer, abnormal } => {
                Ok(match (self.on_link_died)(&mut self.inner, peer, abnormal)? {
                    Step::Continue => Step::Continue,
                    Step::Goto(never) => match never {},
                    Step::Stop(exit) => Step::Stop(exit),
                })
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify green.**

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Watching layer — death notices as alphabet events [#295]"
```

---

### Task 6: `Stashing` layer

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing tests** (append):

```rust
#[cfg(test)]
mod stashing_tests {
    use super::{Base, Exit, StashRoute, Stashing, run};
    use bombay::capability::{Never, Step};

    /// Routes odd ids to the stash until the 0 sentinel releases them.
    fn stasher() -> Stashing<Base<Vec<u64>, u64, Never, &'static str>> {
        Stashing::new(
            Base {
                state: Vec::new(),
                handle: |seen, id| {
                    seen.push(id);
                    Ok(Step::Continue)
                },
            },
            |&id| match id {
                0 => StashRoute::Release,
                n if n % 2 == 1 => StashRoute::Stash,
                _ => StashRoute::Deliver,
            },
        )
    }

    #[test]
    fn released_batch_replays_fifo_within_one_step() {
        let mut b = stasher();
        let out = run(&mut b, vec![1, 2, 3, 0, 4]);
        assert_eq!(out, Ok(Exit::Collected));
        assert_eq!(
            b.inner.state,
            vec![2, 0, 1, 3, 4],
            "release delivers its trigger, then the batch FIFO, atomically before 4",
        );
    }

    #[test]
    fn a_re_stashed_replay_goes_to_held_never_back_into_the_batch() {
        // The route is stable (odd = stash), so a released odd id re-stashes:
        // the batch must terminate — snapshot bound, no livelock.
        let mut b = stasher();
        let out = run(&mut b, vec![1, 0]);
        assert_eq!(out, Ok(Exit::Collected));
        assert_eq!(b.inner.state, vec![0], "1 re-stashed to held, not redelivered");
        assert_eq!(b.held(), 1, "the re-stashed message is retained");
    }
}
```

- [ ] **Step 2: Run to verify red** — compile error on `Stashing`/`StashRoute`.

- [ ] **Step 3: Implement**:

```rust
/// The stash routing verdict (the model image of user-driven stashing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StashRoute {
    /// Hold the event for a later release.
    Stash,
    /// Deliver now.
    Deliver,
    /// Deliver now, then replay the whole held batch in this same step.
    Release,
}

/// The stash capability as a layer: two queues (held vs the draining
/// batch — the shape the real Stashing arrived at) and an in-step drain.
pub struct Stashing<B: Behavior> {
    /// The wrapped behavior.
    pub inner: B,
    route: fn(&B::Event) -> StashRoute,
    held: VecDeque<B::Event>,
}

impl<B: Behavior<Ph = Never>> Stashing<B> {
    /// Builds an empty-stash layer over `inner` with the given routing.
    pub fn new(inner: B, route: fn(&B::Event) -> StashRoute) -> Self {
        Self { inner, route, held: VecDeque::new() }
    }

    /// How many events are currently held (test observability).
    pub fn held(&self) -> usize {
        self.held.len()
    }

    /// Drains a SNAPSHOT of the held queue through the route: re-stashed
    /// events return to `held`, never to the draining batch (the bound
    /// that terminates the drain); `Stop` abandons the rest of the batch.
    fn drain(&mut self) -> Result<Step<Never, Exit>, B::Error> {
        let mut batch: VecDeque<B::Event> = self.held.drain(..).collect();
        while let Some(ev) = batch.pop_front() {
            match (self.route)(&ev) {
                StashRoute::Stash => self.held.push_back(ev),
                StashRoute::Deliver | StashRoute::Release => {
                    if let Step::Stop(exit) = self.inner.step(ev)? {
                        self.held.extend(batch);
                        return Ok(Step::Stop(exit));
                    }
                }
            }
        }
        Ok(Step::Continue)
    }
}

impl<B: Behavior<Ph = Never>> Behavior for Stashing<B> {
    type Event = B::Event;
    type Ph = Never;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<Never, Exit>, B::Error> {
        match (self.route)(&ev) {
            StashRoute::Stash => {
                self.held.push_back(ev);
                Ok(Step::Continue)
            }
            StashRoute::Deliver => self.inner.step(ev),
            StashRoute::Release => {
                if let Step::Stop(exit) = self.inner.step(ev)? {
                    return Ok(Step::Stop(exit));
                }
                self.drain()
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify green.**

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Stashing layer — two-queue in-step drain, batch atomicity by construction [#295]"
```

---

### Task 7: `Phased` layer (both planes; goto in the verdict)

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing tests** (append):

```rust
#[cfg(test)]
mod phased_tests {
    use super::{Base, Exit, Phased, run};
    use bombay::capability::{Deferred, Disposition, Step};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ph {
        Loading,
        Ready,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Msg {
        Work(u64),
        Promote,
        PromoteThenFail,
        Quit,
    }

    /// Work defers in Loading; Promote gotos Ready (verdict-carried become).
    fn machine() -> Phased<Base<Vec<u64>, Msg, Ph, &'static str>> {
        Phased::new(
            Base {
                state: Vec::new(),
                handle: |seen, msg| match msg {
                    Msg::Work(id) => {
                        seen.push(id);
                        Ok(Step::Continue)
                    }
                    Msg::Promote => Ok(Step::Goto(Ph::Ready)),
                    Msg::PromoteThenFail => Err("bang"),
                    Msg::Quit => Ok(Step::Stop(Exit::Normal)),
                },
            },
            Ph::Loading,
            |phase, msg| match (phase, msg) {
                (Ph::Loading, Msg::Work(_)) => Disposition::Defer(Deferred),
                _ => Disposition::Deliver,
            },
        )
    }

    #[test]
    fn goto_releases_the_deferred_batch_fifo_within_one_step() {
        let mut b = machine();
        let out = run(
            &mut b,
            vec![Msg::Work(1), Msg::Work(2), Msg::Promote, Msg::Work(3), Msg::Quit],
        );
        assert_eq!(out, Ok(Exit::Normal));
        assert_eq!(
            b.inner.state,
            vec![1, 2, 3],
            "the batch replays FIFO inside the Promote step, ahead of 3",
        );
        assert_eq!(b.phase(), Ph::Ready);
    }

    #[test]
    fn an_err_step_cannot_commit_a_phase_change() {
        // ADR-0030 audit finding: with the verdict carrying Goto, this is
        // a tautology — an Err has no verdict to carry a phase in. The
        // assertion is the D3 witness, kept as the migration oracle.
        let mut b = machine();
        let out = run(&mut b, vec![Msg::PromoteThenFail]);
        assert_eq!(out, Err("bang"));
        assert_eq!(b.phase(), Ph::Loading, "no half-switched phase after Err (D3)");
    }

    #[test]
    fn stop_mid_batch_abandons_the_rest() {
        let mut b = Phased::new(
            Base {
                state: Vec::new(),
                handle: |seen: &mut Vec<u64>, msg: Msg| match msg {
                    Msg::Work(1) => {
                        seen.push(1);
                        Ok(Step::Stop(Exit::Normal))
                    }
                    Msg::Work(id) => {
                        seen.push(id);
                        Ok(Step::Continue)
                    }
                    Msg::Promote => Ok(Step::Goto(Ph::Ready)),
                    Msg::PromoteThenFail => Err("bang"),
                    Msg::Quit => Ok(Step::Stop(Exit::Normal)),
                },
            },
            Ph::Loading,
            |phase, msg| match (phase, msg) {
                (Ph::Loading, Msg::Work(_)) => Disposition::Defer(Deferred),
                _ => Disposition::Deliver,
            },
        );
        let out = run(&mut b, vec![Msg::Work(1), Msg::Work(2), Msg::Promote]);
        assert_eq!(out, Ok(Exit::Normal), "the replayed 1 stopped the actor");
        assert_eq!(b.inner.state, vec![1], "2 was abandoned with the batch");
    }
}
```

- [ ] **Step 2: Run to verify red** — compile error on `Phased`.

- [ ] **Step 3: Implement**:

```rust
/// The phase capability as a layer — BOTH planes: the gate wraps the
/// step (message plane) and the deferral seat's replay extends behavior
/// within the step (event plane). Erases the inner become-menu:
/// `Ph = Never` upward — the menu is consumed here.
pub struct Phased<B: Behavior> {
    /// The wrapped behavior (its `Ph` is this layer's phase menu).
    pub inner: B,
    phase: B::Ph,
    gate: fn(B::Ph, &B::Event) -> Disposition<Deferred>,
    held: VecDeque<B::Event>,
    batch: VecDeque<B::Event>,
}

impl<B> Phased<B>
where
    B: Behavior,
    B::Ph: Copy + PartialEq,
{
    /// Builds the layer in `initial`, empty queues.
    pub fn new(
        inner: B,
        initial: B::Ph,
        gate: fn(B::Ph, &B::Event) -> Disposition<Deferred>,
    ) -> Self {
        Self { inner, phase: initial, gate, held: VecDeque::new(), batch: VecDeque::new() }
    }

    /// The committed phase (test observability).
    pub fn phase(&self) -> B::Ph {
        self.phase
    }

    /// One delivered inner step. A `Goto` verdict commits HERE — inside
    /// the `Ok`, so an `Err` cannot half-switch (D3 is structural) — and
    /// a phase CHANGE releases the held queue into the draining batch.
    fn deliver(&mut self, ev: B::Event) -> Result<Step<Never, Exit>, B::Error> {
        match self.inner.step(ev)? {
            Step::Continue => Ok(Step::Continue),
            Step::Stop(exit) => Ok(Step::Stop(exit)),
            Step::Goto(next) => {
                if next != self.phase {
                    self.phase = next;
                    self.batch.extend(self.held.drain(..));
                }
                Ok(Step::Continue)
            }
        }
    }

    /// The in-step replay drain: every replayed event RE-GATES in the
    /// current phase; a re-deferred event returns to `held`, never to
    /// the batch (snapshot bound); `Stop` abandons the rest.
    fn drain(&mut self) -> Result<Step<Never, Exit>, B::Error> {
        while let Some(ev) = self.batch.pop_front() {
            match (self.gate)(self.phase, &ev) {
                Disposition::Ignore => {}
                Disposition::Defer(Deferred) => self.held.push_back(ev),
                Disposition::Deliver => {
                    if let Step::Stop(exit) = self.deliver(ev)? {
                        self.batch.clear();
                        return Ok(Step::Stop(exit));
                    }
                }
            }
        }
        Ok(Step::Continue)
    }
}

impl<B> Behavior for Phased<B>
where
    B: Behavior,
    B::Ph: Copy + PartialEq,
{
    type Event = B::Event;
    type Ph = Never;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<Never, Exit>, B::Error> {
        match (self.gate)(self.phase, &ev) {
            Disposition::Ignore => Ok(Step::Continue),
            Disposition::Defer(Deferred) => {
                self.held.push_back(ev);
                Ok(Step::Continue)
            }
            Disposition::Deliver => {
                if let Step::Stop(exit) = self.deliver(ev)? {
                    return Ok(Step::Stop(exit));
                }
                self.drain()
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify green.**

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Phased layer — verdict-carried become; D3 dissolves into the type [#295]"
```

---

### Task 8: `Supervising` layer (outer fold over child folds) + proptest

**Files:**
- Modify: `crates/core/tests/behavior_algebra/model.rs`

- [ ] **Step 1: Write the failing tests** (append):

```rust
#[cfg(test)]
mod supervising_tests {
    use super::{Base, Child, Exit, Sup, Supervising, run};
    use bombay::capability::{Never, Step};

    type Kid = Base<u32, u32, Never, &'static str>;

    fn kid() -> Kid {
        Base {
            state: 0,
            handle: |count, n| {
                *count += n;
                Ok(Step::Continue)
            },
        }
    }

    fn supervisor(budget: u32) -> Supervising<Base<Vec<u64>, u64, Never, &'static str>, Kid> {
        Supervising {
            inner: Base {
                state: Vec::new(),
                handle: |seen, id| {
                    seen.push(id);
                    Ok(Step::Continue)
                },
            },
            children: vec![Child { behavior: kid(), alive: true }],
            build: |_| kid(),
            restarts_left: budget,
        }
    }

    #[test]
    fn an_abnormal_child_stop_rebuilds_the_child_fold() {
        let mut sup = supervisor(1);
        sup.children[0].behavior.state = 41; // the inner fold has progressed
        let out = run(&mut sup, vec![Sup::ChildStopped { idx: 0, abnormal: true }]);
        assert_eq!(out, Ok(Exit::Collected));
        assert_eq!(sup.children[0].behavior.state, 0, "the child is a FRESH fold");
        assert!(sup.children[0].alive);
        assert_eq!(sup.restarts_left, 0);
    }

    #[test]
    fn budget_exhaustion_leaves_the_child_dead() {
        let mut sup = supervisor(0);
        let out = run(&mut sup, vec![Sup::ChildStopped { idx: 0, abnormal: true }]);
        assert_eq!(out, Ok(Exit::Collected));
        assert!(!sup.children[0].alive, "give-up: no restart budget");
    }

    #[test]
    fn a_normal_child_stop_never_restarts() {
        let mut sup = supervisor(5);
        let out = run(&mut sup, vec![Sup::ChildStopped { idx: 0, abnormal: false }]);
        assert_eq!(out, Ok(Exit::Collected));
        assert!(!sup.children[0].alive, "normal stop is final under every policy");
        assert_eq!(sup.restarts_left, 5, "no budget spent on a normal stop");
    }
}

#[cfg(test)]
mod law_tests {
    use super::{Base, Exit, Phased, run};
    use bombay::capability::{Deferred, Disposition, Step};
    use proptest::prelude::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ph {
        A,
        B,
    }

    proptest! {
        /// No-silent-drop + FIFO exactly-once: under an arbitrary prefix
        /// of deferred work and one promotion, every id is delivered
        /// exactly once, deferred ids in arrival order.
        #[test]
        fn prop_phased_replay_is_fifo_exactly_once(deferred in proptest::collection::vec(0_u64..64, 0..=8)) {
            let mut b = Phased::new(
                Base::<Vec<u64>, _, Ph, &'static str> {
                    state: Vec::new(),
                    handle: |seen, msg: Result<u64, ()>| match msg {
                        Ok(id) => {
                            seen.push(id);
                            Ok(Step::Continue)
                        }
                        Err(()) => Ok(Step::Goto(Ph::B)),
                    },
                },
                Ph::A,
                |phase, msg| match (phase, msg) {
                    (Ph::A, Ok(_)) => Disposition::Defer(Deferred),
                    _ => Disposition::Deliver,
                },
            );
            let script: Vec<Result<u64, ()>> =
                deferred.iter().copied().map(Ok).chain([Err(())]).collect();
            let out = run(&mut b, script);
            prop_assert_eq!(out, Ok(Exit::Collected));
            prop_assert_eq!(&b.inner.state, &deferred, "FIFO, exactly once, none lost");
        }
    }
}
```

- [ ] **Step 2: Run to verify red** — compile error on `Supervising`/`Sup`/`Child`.

- [ ] **Step 3: Implement** (the supervising layer; the proptest needs no new code):

```rust
/// The supervision source's alphabet extension.
#[derive(Debug, PartialEq, Eq)]
pub enum Sup<E> {
    /// A child fold ended (the link-notice image on the supervisor side).
    ChildStopped {
        /// Index into the child table.
        idx: usize,
        /// Whether the child's stop was abnormal (restart-eligible).
        abnormal: bool,
    },
    /// A pass-through inner event.
    Event(E),
}

/// One supervised child: an inner fold and its liveness.
pub struct Child<C> {
    /// The child's behavior — a whole fold instance.
    pub behavior: C,
    /// False once stopped-for-good (normal stop or budget exhausted).
    pub alive: bool,
}

/// The supervision capability as a layer: the OUTER fold restarting
/// inner folds. One-for-one, budget-bounded — model grade.
pub struct Supervising<B: Behavior, C: Behavior<Ph = Never>> {
    /// The supervisor's own behavior.
    pub inner: B,
    /// The child table (each child an inner fold).
    pub children: Vec<Child<C>>,
    /// The restart factory: a restart is a FRESH fold, never resumed state.
    pub build: fn(usize) -> C,
    /// Remaining restart budget (the two-counter accounting, collapsed).
    pub restarts_left: u32,
}

impl<B: Behavior, C: Behavior<Ph = Never>> Supervising<B, C> {
    fn on_child_stopped(&mut self, idx: usize, abnormal: bool) {
        let Some(child) = self.children.get_mut(idx) else {
            return;
        };
        if abnormal && self.restarts_left > 0 {
            self.restarts_left -= 1;
            *child = Child { behavior: (self.build)(idx), alive: true };
        } else {
            child.alive = false;
        }
    }
}

impl<B: Behavior, C: Behavior<Ph = Never>> Behavior for Supervising<B, C> {
    type Event = Sup<B::Event>;
    type Ph = B::Ph;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<B::Ph, Exit>, B::Error> {
        match ev {
            Sup::Event(inner_ev) => self.inner.step(inner_ev),
            Sup::ChildStopped { idx, abnormal } => {
                self.on_child_stopped(idx, abnormal);
                Ok(Step::Continue)
            }
        }
    }
}
```

- [ ] **Step 4: Run to verify green** (all model tests + the proptest).

- [ ] **Step 5: Commit**

```bash
git add crates/core/tests/behavior_algebra/model.rs
git commit -m "test(algebra): Supervising layer + replay FIFO/exactly-once property — model complete [#295]"
```

---

### Task 9: New-brick closure proof (out-of-module rate limiter)

**Files:**
- Create/replace: `crates/core/tests/behavior_algebra/new_brick.rs`

- [ ] **Step 1: Write the failing test AND the brick together** (the brick IS the test — the proof is that it compiles and behaves using only `model`'s public items, with zero edits to `model.rs`):

```rust
//! The closure proof (ADR-0030): a capability the model has never heard
//! of — written here, against `model`'s public surface only. It BOTH
//! wraps the step (budget check) and adds a source (the refill tick):
//! the algebra's two operations, exercised out-of-module.

use bombay::capability::Step;

use super::model::{Base, Behavior, Exit, run};

/// The refill source's alphabet extension.
#[derive(Debug, PartialEq, Eq)]
enum Limited<E> {
    /// The token-bucket refill tick.
    Refill,
    /// A pass-through inner event.
    Event(E),
}

/// A token-bucket admission layer: refused events are counted, never
/// silently dropped (the no-silent-drop law holds for strangers too).
struct RateLimited<B: Behavior> {
    inner: B,
    budget: u32,
    refill_to: u32,
    refused: u32,
}

impl<B: Behavior> Behavior for RateLimited<B> {
    type Event = Limited<B::Event>;
    type Ph = B::Ph;
    type Error = B::Error;
    fn step(&mut self, ev: Self::Event) -> Result<Step<B::Ph, Exit>, B::Error> {
        match ev {
            Limited::Refill => {
                self.budget = self.refill_to;
                Ok(Step::Continue)
            }
            Limited::Event(inner_ev) => {
                if self.budget == 0 {
                    self.refused += 1;
                    return Ok(Step::Continue);
                }
                self.budget -= 1;
                self.inner.step(inner_ev)
            }
        }
    }
}

#[test]
fn a_foreign_brick_composes_with_zero_model_changes() {
    let mut b = RateLimited {
        inner: Base::<Vec<u64>, u64, bombay::capability::Never, &'static str> {
            state: Vec::new(),
            handle: |seen, id| {
                seen.push(id);
                Ok(Step::Continue)
            },
        },
        budget: 2,
        refill_to: 2,
        refused: 0,
    };
    let out = run(
        &mut b,
        vec![
            Limited::Event(1),
            Limited::Event(2),
            Limited::Event(3),
            Limited::Refill,
            Limited::Event(4),
        ],
    );
    assert_eq!(out, Ok(Exit::Collected));
    assert_eq!(b.inner.state, vec![1, 2, 4], "3 refused at empty budget; refill re-admits");
    assert_eq!(b.refused, 1, "refusal recorded, not silent (no-silent-drop)");
}
```

Also uncomment/add `mod new_brick;` in `main.rs` if it was left out in Task 2.

- [ ] **Step 2: Run to verify** — first run should already be green if the brick is correct; force the red discipline by first running with the assertion `vec![1, 2, 3, 4]` (deliberately wrong), confirming FAIL, then restoring `vec![1, 2, 4]` and confirming PASS. (The falsifiability probe pattern from #207.)

Run: `nix develop --command cargo nextest run -p bombay --test behavior_algebra new_brick`

- [ ] **Step 3: Commit**

```bash
git add crates/core/tests/behavior_algebra
git commit -m "test(algebra): new-brick closure proof — foreign rate-limit layer, zero model changes [#295]"
```

---

### Task 10: Trace-equality oracle (model vs real machinery)

**Files:**
- Create/replace: `crates/core/tests/behavior_algebra/oracle.rs`

Three scenarios, each: ONE abstract script → model events AND real
actor driving → identical probe sequences + stop kind. Probe vocabulary
is only what USER code can observe on both sides (handler + deadline
reaction + `on_stop`): deferral is observable through ORDER, not named.

- [ ] **Step 1: Write the shared vocabulary + scenario 1 (plain), failing first**

```rust
//! Model-vs-real trace equality (#266 oracle discipline): the same
//! abstract script drives the sync fold AND a spawned capability actor;
//! probe sequences and stop kinds must be identical. Scenarios: plain,
//! phased (defer + release), deadline. Watching/supervising equality
//! rides the implementation pass (named deferral on card #295).

use core::num::NonZeroUsize;
use core::time::Duration;

use bombay::{
    actor::{Flow, Normal},
    capability::{
        Actor, Bounded, ByState, CapSet, Ctx, DeadlineInstant, DeadlinePolicy, Deferred,
        Disposition, NoTimeout, PhasePolicy, Phased, StashPolicy, Step, spawn,
    },
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed},
};
use tokio::sync::mpsc;
// (Import list grows with scenarios 2–3; keep it warning-clean per step —
// the deny-warnings gate treats an unused import as a failure.)

use super::model;

/// The mode-blind observable vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Probe {
    /// The user handler ran for this id.
    Applied(u64),
    /// A deadline reaction ran.
    TimedOut,
}

/// The stop-kind comparison (Exit ↔ ActorStopReason, kinds only).
fn same_stop(model_exit: model::Exit, real: &ActorStopReason) -> bool {
    matches!(
        (model_exit, real),
        (model::Exit::Normal, ActorStopReason::Normal)
            | (model::Exit::Collected, ActorStopReason::Collected)
    )
}
```

Scenario 1 — plain. Model side:

```rust
/// Scenario 1 model: the base floor alone.
fn plain_model(script: &[u64]) -> (Vec<Probe>, model::Exit) {
    let mut b = model::Base::<Vec<Probe>, u64, bombay::capability::Never, &'static str> {
        state: Vec::new(),
        handle: |probes, id| {
            if id == 0 {
                return Ok(Step::Stop(model::Exit::Normal));
            }
            probes.push(Probe::Applied(id));
            Ok(Step::Continue)
        },
    };
    let exit = model::run(&mut b, script.iter().copied()).expect("infallible scenario");
    (b.state, exit)
}
```

Real side — a `Caps = ()` capability actor mirroring the same handler
(probes over an unbounded mpsc, stop reason over a second channel exactly
as `capability_stage1.rs` does):

```rust
#[derive(Debug, bombay_macros::Msg)]
struct PlainMsg(u64);

struct PlainActor {
    probes: mpsc::UnboundedSender<Probe>,
    stopped: mpsc::UnboundedSender<ActorStopReason>,
}

impl Mailboxed for PlainActor {
    type Msg = PlainMsg;
}

impl Actor for PlainActor {
    type Msg = PlainMsg;
    type Args = (mpsc::UnboundedSender<Probe>, mpsc::UnboundedSender<ActorStopReason>);
    type Error = core::convert::Infallible;
    type Caps = ();

    async fn init((probes, stopped): Self::Args, _: Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { probes, stopped })
    }

    async fn handle(&mut self, PlainMsg(id): PlainMsg, _: Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        if id == 0 {
            return Ok(Flow::Stop(Normal));
        }
        let _ = self.probes.send(Probe::Applied(id));
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        _: bombay::actor::WeakActorRef<bombay::capability::Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        let _ = self.stopped.send(reason);
        Ok(())
    }
}

async fn plain_real(script: &[u64]) -> (Vec<Probe>, ActorStopReason) {
    let (ptx, mut prx) = mpsc::unbounded_channel();
    let (stx, mut srx) = mpsc::unbounded_channel();
    let actor = spawn::<PlainActor>((ptx, stx));
    for &id in script {
        actor.tell(PlainMsg(id)).await.expect("tell");
    }
    let reason = tokio::time::timeout(Duration::from_secs(10), srx.recv())
        .await
        .expect("stop within bound")
        .expect("probe open");
    let mut probes = Vec::new();
    while let Ok(p) = prx.try_recv() {
        probes.push(p);
    }
    (probes, reason)
}

#[tokio::test]
async fn plain_scenario_traces_are_identical() {
    let script = [7, 8, 9, 0];
    let (m_probes, m_exit) = plain_model(&script);
    let (r_probes, r_reason) = plain_real(&script).await;
    assert_eq!(m_probes, r_probes, "probe sequences must be identical");
    assert!(same_stop(m_exit, &r_reason), "stop kinds must agree: {m_exit:?} vs {r_reason:?}");
}
```

- [ ] **Step 2: Run to verify** it compiles and passes; apply the falsifiability probe (temporarily reorder the model's push vs stop check, watch it FAIL, restore).

Run: `nix develop --command cargo nextest run -p bombay --test behavior_algebra oracle`

- [ ] **Step 3: Add scenario 2 (phased: defer → promote → batch-before-backlog).** Script ops:

```rust
/// Scenario 2's abstract ops — one script, two runners.
#[derive(Debug, Clone, Copy)]
enum PhOp {
    Work(u64),
    Promote,
    Quit,
}
```

Model runner: `model::Phased` over `model::Base` with gate = defer `Work`
in `Loading`, deliver everything else; handler pushes `Applied(id)` for
`Work`, returns `Step::Goto(Ready)` for `Promote`, `Stop(Normal)` for
`Quit`. Expected probes for `[Work 1, Work 2, Promote, Work 3, Quit]`:
`[Applied(1), Applied(2), Applied(3)]` with the batch strictly before 3.

Real runner: a `Phased<P>` capability actor —

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggPh {
    Loading,
    Ready,
}

#[derive(Debug, bombay_macros::Msg)]
enum AggMsg {
    Work(u64),
    Promote,
    Quit,
}

struct AggPolicy;

impl PhasePolicy for AggPolicy {
    type Actor = AggActor;
    type Phase = AggPh;
    type Deferral = Bounded<AggStash>;
    type Timeout = NoTimeout;

    fn initial(_: &<AggActor as Actor>::Args) -> AggPh {
        AggPh::Loading
    }

    fn gate(phase: AggPh, msg: &AggMsg) -> Disposition<Deferred> {
        match (phase, msg) {
            (AggPh::Loading, AggMsg::Work(_)) => Disposition::Defer(Deferred),
            _ => Disposition::Deliver,
        }
    }
}

struct AggStash;

impl StashPolicy<AggActor> for AggStash {
    fn capacity(_: &<AggActor as Actor>::Args) -> Capacity {
        Capacity::new(NonZeroUsize::new(16).expect("nonzero")).expect("valid")
    }
}

#[derive(bombay_macros::Provide)]
struct AggCaps {
    phased: Phased<AggPolicy>,
}

impl CapSet<AggActor> for AggCaps {
    fn build(args: &<AggActor as Actor>::Args) -> Self {
        Self { phased: Phased::build(args) }
    }
}
```

`AggActor` mirrors `PlainActor` (probes + stopped channels) with
`Caps = AggCaps`; its handler: `Work(id)` → push `Applied(id)`, Continue;
`Promote` → `cx.cap::<Phased<AggPolicy>>().goto(AggPh::Ready)`, Continue;
`Quit` → `Flow::Stop(Normal)`. The real runner maps `PhOp` to tells; the
model runner maps `PhOp` to model messages. Assert identical probes +
stop kind. **This is the pass's central assertion**: model verdict-carried
goto vs real side-channel goto — observationally identical.

- [ ] **Step 4: Add scenario 3 (deadline).** `#[tokio::test(start_paused = true)]`. Real: a `Deadlined<DP>` actor, `DP: DeadlinePolicy<ByState<Self>>` with `next_deadline` = `actor.due`, `on_deadline` pushes `TimedOut`, clears `due`, returns `Step::Continue` (crib the exact seat spelling from `IdlePolicy` in `crates/core/src/capability/shell.rs` tests and `deadline_plane.rs` — arm `due` at init to `DeadlineInstant::now() + 5s`). Script: tell `Work(1)`, `tokio::time::sleep(Duration::from_secs(6)).await` (paused clock auto-advances; the arm fires), tell `Work(2)`, tell `Quit`. Model: `model::Deadlined` over `model::Base`; events `[Timed::Event(Work 1), Timed::Deadline, Timed::Event(Work 2), Timed::Event(Quit)]`. Expected both sides: `[Applied(1), TimedOut, Applied(2)]`, Normal stop.

- [ ] **Step 5: Run the full test crate**

Run: `nix develop --command cargo nextest run -p bombay --test behavior_algebra`
Expected: PASS (all model + brick + 3 oracle tests).

- [ ] **Step 6: Commit**

```bash
git add crates/core/tests/behavior_algebra
git commit -m "test(algebra): model-vs-real trace oracle — plain/phased/deadline identical [#295]"
```

---

### Task 11: Gate, docs, card, PR

**Files:**
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Update the coverage baseline** — add a `behavior_algebra` entry under the tests section describing the new surface (model bricks, closure proof, 3-scenario oracle), per the README-per-card rule (no README change: no public API change).

- [ ] **Step 2: Run the single gate** (everything already `git add`ed — verify with `git status` first: untracked files make the check vacuous):

Run: `git status --short && nix flake check`
Expected: clean tracked tree, check PASSES (a silent build means cached — look for `building '...drv'` on first run).

- [ ] **Step 3: Commit docs + push + PR**

```bash
git add docs/testing/coverage-baseline.md
git commit -m "docs(testing): coverage baseline — behavior_algebra prototype surface [#295]"
git push -u origin core/295-behavior-algebra
gh pr create --repo devrandom-labs/bombay --title "core(caps): behavior algebra — ADR-0030 + essence-fold prototype [#295]" --body "$(cat <<'EOF'
ADR-0030 (Behavior algebra, sealed-first) + the #295 executable prototype:
sync essence-fold, five capabilities as layers, new-brick closure proof,
model-vs-real trace oracle (plain/phased/deadline).

Card #295 bullet status:
- [x] ADR (algebra only; select ENCODING = open door pending #298 slope data)
- [ ] open source-set select — deferred on-card, gated on #298
- [ ] unseal DeadlineCx — deferred; sealed-first is the recorded decision
- [ ] out-of-crate proof — deferred with unsealing; model-grade proof ships here
- [~] oracles as theorems — prototype-grade (3 scenarios); watching/supervising
  equality + full 6-scenario oracle ride the implementation pass

No public API change → no job-queue app extension this pass (walking-skeleton
rule, stated per CLAUDE.md).
EOF
)"
```

- [ ] **Step 4: Comment the bullet status on card #295** (same status block as the PR body) so the card is cold-start accurate.

---

## Self-review notes (run before execution)

- Spec coverage: ADR (Task 1), fold+object (Task 2), five layers (Tasks 3–8), laws (Task 8 proptest + Task 7 D3 witness + no-silent-drop in Task 9), new-brick proof (Task 9), trace equality (Task 10), scoped deferrals recorded (Task 11).
- The model uses `Exit` (its own `R`) rather than `ActorStopReason` — deliberate, recorded in the ADR text (Task 1) and mapped at the oracle boundary (`same_stop`).
- Oracle real-side API spellings (`ask`/`tell`/`spawn`, `on_stop` probe, derive) follow `capability_stage1.rs`; the `Phased` seat spelling follows `phase_equivalence.rs`; the deadline seat follows `shell.rs`'s `IdlePolicy` — if a signature drifts, those three files are the source of truth, not this plan.
