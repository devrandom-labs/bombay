//! Mock of candidate shape (b): `FsmActor` trait + `Fsm<S>` wrapper.
//!
//! Semantics mirror gen_statem's transition rules (verified against the
//! erlang.org docs, 2026-07-31):
//! - postponed (stashed) events are retried ONLY on a state change
//!   (`Goto(s)` with `s == current` is a no-op transition, like
//!   `{next_state, State, _}` with `NextState =:= State`);
//! - a state timeout is cancelled ONLY on a state change, and the new
//!   state's timeout (if any) is armed then.
//!
//! MOCK COMPROMISES (the real in-crate build would differ; ADR records both):
//! - State timeouts ride a public envelope `FsmMsg<M>` because the mock lives
//!   outside the crate. The real implementation rides the internal control
//!   lane (ADR-0021) or run-loop plumbing, keeping `Fsm<S>::Msg == S::Msg` —
//!   no envelope, no slot growth, no `tell(FsmMsg::User(..))` wart.
//! - The stash is a local re-implementation of `bombay::stash::Stash`
//!   (its constructor is `pub(crate)`); semantics copied: bounded,
//!   snapshot `unstash_all`, refuse-with-handback overflow.

use core::future::Future;
use std::collections::VecDeque;
use std::time::Duration;

use bombay::{
    actor::{Actor, ActorRef, Flow, TimerHandle, WeakActorRef},
    error::{ActorStopReason, PanicError, ReplyError},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
};

/// Per-state message admission (the P trio): what the wrapper does with a
/// message before the handler is consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Hand it to `handle` now.
    Deliver,
    /// Stash it; re-gate on every state change (P `defer`).
    Defer,
    /// Drop it deliberately, by declaration (P `ignore`).
    Ignore,
}

/// The handler's transition decision — `Flow` plus one variant. Plain value:
/// nothing on this path allocates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<St> {
    /// Stay in the current state (gen_statem `keep_state`).
    Stay,
    /// Transition. `Goto(current)` is deliberately a no-op (gen_statem
    /// `next_state` to the same state): no unstash, no timeout reset.
    Goto(St),
    /// Stop cleanly (reason `Normal`), like `Flow::Stop`.
    Stop,
}

/// Bounded deferral buffer — semantics of `bombay::stash::Stash` (ADR-0022).
#[derive(Debug)]
pub struct FsmStash<M> {
    held: VecDeque<M>,
    ready: VecDeque<M>,
    cap: usize,
}

/// Overflow handback, mirroring `bombay::stash::StashFull`.
#[derive(Debug)]
pub struct FsmStashFull<M>(pub M);

impl<M> FsmStash<M> {
    fn bounded(cap: Capacity) -> Self {
        Self {
            held: VecDeque::new(),
            ready: VecDeque::new(),
            cap: cap.get(),
        }
    }

    pub fn len(&self) -> usize {
        self.held.len().checked_add(self.ready.len()).unwrap_or(usize::MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty() && self.ready.is_empty()
    }

    /// Defers `msg`; refuses with handback at capacity.
    pub fn stash(&mut self, msg: M) -> Result<(), FsmStashFull<M>> {
        if self.len() >= self.cap {
            return Err(FsmStashFull(msg));
        }
        self.held.push_back(msg);
        Ok(())
    }

    /// Manual release — still available (the idiomatic StashActor verb).
    /// The wrapper also calls this automatically on every state change.
    pub fn unstash_all(&mut self) {
        self.ready.append(&mut self.held);
    }

    fn pop_ready(&mut self) -> Option<M> {
        self.ready.pop_front()
    }
}

/// The FSM actor shape: `StashActor`'s signature family plus a state tag
/// argument and a `Step` return.
pub trait FsmActor: Mailboxed<Msg: Msg> + Sized + Send + 'static {
    type Args: Send;
    type Error: ReplyError;
    /// The state NAME (gen_statem split): a plain tag enum the framework
    /// observes. State DATA stays in `self`.
    type State: Clone + PartialEq + Send + 'static;

    fn initial_state(args: &Self::Args) -> Self::State;

    fn stash_capacity(args: &Self::Args) -> Capacity;

    /// Declarative per-state deadline. Armed on entering `state`, cancelled
    /// on leaving it (gen_statem state timeout). `None` = no deadline.
    #[must_use]
    fn state_timeout(state: &Self::State) -> Option<Duration> {
        let _ = state;
        None
    }

    /// Declarative per-state admission (P PLDI 2013 `defer`/`ignore`
    /// semantics, made payload-capable): classifies every message BEFORE
    /// `handle` sees it. `Defer`red messages are stashed and re-gated on
    /// transition; `Ignore`d messages are dropped deliberately (declared
    /// intent — P's `ignore`, not a silent loss). Admission stops being a
    /// per-arm imperative step.
    #[must_use]
    fn gate(state: &Self::State, msg: &Self::Msg) -> Disposition {
        let _ = (state, msg);
        Disposition::Deliver
    }

    /// A declaratively-deferred message found the stash at capacity. The
    /// message is handed BACK here (never silently dropped, ADR-0022).
    /// Default: deliver to `handle` after all — visible-but-unrefused
    /// shedding; override for a loud typed refusal.
    fn on_defer_full(
        &mut self,
        state: &Self::State,
        msg: Self::Msg,
        actor_ref: ActorRef<Fsm<Self>>,
        stash: &mut FsmStash<Self::Msg>,
    ) -> impl Future<Output = Result<Step<Self::State>, Self::Error>> + Send {
        self.handle(state, msg, actor_ref, stash)
    }

    fn on_start(
        args: Self::Args,
        actor_ref: ActorRef<Fsm<Self>>,
    ) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    /// Handles one message in `state`. Manual `stash`/`unstash_all` remain
    /// available; a returned `Goto` to a DIFFERENT state additionally
    /// releases the stash and swaps the state timeout automatically.
    fn handle(
        &mut self,
        state: &Self::State,
        msg: Self::Msg,
        actor_ref: ActorRef<Fsm<Self>>,
        stash: &mut FsmStash<Self::Msg>,
    ) -> impl Future<Output = Result<Step<Self::State>, Self::Error>> + Send;

    /// The state timeout fired while still in `state` — staleness is
    /// filtered by the wrapper, so this NEVER fires for a left state.
    fn on_state_timeout(
        &mut self,
        state: &Self::State,
        actor_ref: ActorRef<Fsm<Self>>,
        stash: &mut FsmStash<Self::Msg>,
    ) -> impl Future<Output = Result<Step<Self::State>, Self::Error>> + Send {
        let _ = (state, actor_ref, stash);
        async { Ok(Step::Stay) }
    }

    fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Fsm<Self>>,
        reason: ActorStopReason,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let _ = (actor_ref, reason);
        async { Ok(()) }
    }
}

/// Mock envelope — see module docs: the real build has no envelope.
#[derive(Debug)]
pub enum FsmMsg<M> {
    User(M),
    /// Internal state-timeout event. `epoch` identifies the arming state
    /// incarnation; a mismatch means the timeout raced a transition and is
    /// dropped by the wrapper (unrepresentable staleness).
    StateTimeout { epoch: u64 },
}

impl<M: Send + 'static> Msg for FsmMsg<M> {}

/// Framework-owned composition: an `Fsm<S>` is a plain `Actor` — every verb
/// (spawn, tell, watch, supervise, timers, Recipient) works unchanged.
#[derive(Debug)]
pub struct Fsm<S: FsmActor> {
    data: S,
    state: S::State,
    /// Bumped on every state change; stamps and filters state timeouts.
    epoch: u64,
    timer: Option<TimerHandle>,
    stash: FsmStash<S::Msg>,
}

impl<S: FsmActor> Mailboxed for Fsm<S> {
    type Msg = FsmMsg<S::Msg>;
}

impl<S: FsmActor> Fsm<S> {
    /// Applies a state change: bump epoch, cancel the old state timeout,
    /// switch, release the stash (replay runs in the NEW state, per
    /// gen_statem), arm the new state's timeout.
    fn transition(&mut self, next: S::State, actor_ref: &ActorRef<Self>) {
        self.epoch = self.epoch.wrapping_add(1);
        if let Some(t) = self.timer.take() {
            t.cancel();
        }
        self.state = next;
        self.stash.unstash_all();
        self.arm_state_timeout(actor_ref);
    }

    fn arm_state_timeout(&mut self, actor_ref: &ActorRef<Self>) {
        if let Some(dur) = S::state_timeout(&self.state) {
            let epoch = self.epoch;
            self.timer = Some(actor_ref.send_after(dur, FsmMsg::StateTimeout { epoch }));
        }
    }

    /// Applies one handler decision. Returns `Some(Flow::Stop)` to stop.
    fn apply(&mut self, step: Step<S::State>, actor_ref: &ActorRef<Self>) -> Option<Flow> {
        match step {
            Step::Stay => None,
            Step::Stop => Some(Flow::Stop),
            Step::Goto(next) => {
                if next != self.state {
                    self.transition(next, actor_ref);
                }
                None
            }
        }
    }
}

impl<S: FsmActor> Actor for Fsm<S> {
    type Args = S::Args;
    type Error = S::Error;

    fn name() -> &'static str {
        core::any::type_name::<S>()
    }

    async fn on_start(args: S::Args, actor_ref: ActorRef<Self>) -> Result<Self, S::Error> {
        let state = S::initial_state(&args);
        let cap = S::stash_capacity(&args);
        let data = S::on_start(args, actor_ref.clone()).await?;
        let mut this = Self {
            data,
            state,
            epoch: 0,
            timer: None,
            stash: FsmStash::bounded(cap),
        };
        this.arm_state_timeout(&actor_ref);
        Ok(this)
    }

    async fn handle(
        &mut self,
        msg: FsmMsg<S::Msg>,
        actor_ref: ActorRef<Self>,
    ) -> Result<Flow, S::Error> {
        let step = match msg {
            FsmMsg::User(m) => match S::gate(&self.state, &m) {
                Disposition::Deliver => {
                    S::handle(&mut self.data, &self.state, m, actor_ref.clone(), &mut self.stash)
                        .await?
                }
                Disposition::Defer => match self.stash.stash(m) {
                    Ok(()) => Step::Stay,
                    Err(FsmStashFull(m)) => {
                        S::on_defer_full(
                            &mut self.data,
                            &self.state,
                            m,
                            actor_ref.clone(),
                            &mut self.stash,
                        )
                        .await?
                    }
                },
                Disposition::Ignore => Step::Stay,
            },
            FsmMsg::StateTimeout { epoch } => {
                if epoch != self.epoch {
                    // Raced a transition: armed for a state we since left.
                    // Dropped here — user code cannot observe it.
                    return Ok(Flow::Continue);
                }
                S::on_state_timeout(
                    &mut self.data,
                    &self.state,
                    actor_ref.clone(),
                    &mut self.stash,
                )
                .await?
            }
        };
        if let Some(stop) = self.apply(step, &actor_ref) {
            return Ok(stop);
        }
        // Replay loop (the `Stashed::handle` shape): released messages run
        // ahead of the whole mailbox backlog, in stash-arrival order, in the
        // CURRENT (possibly just-entered) state. A replayed handler may
        // itself transition; the flat loop keeps draining whatever is ready.
        while let Some(m) = self.stash.pop_ready() {
            // Re-gate in the current state (P: an event stays deferred until
            // a state stops deferring it). Snapshot semantics bound the loop:
            // a re-deferred message goes to `held`, never back into `ready`.
            let step = match S::gate(&self.state, &m) {
                Disposition::Deliver => {
                    S::handle(&mut self.data, &self.state, m, actor_ref.clone(), &mut self.stash)
                        .await?
                }
                Disposition::Defer => match self.stash.stash(m) {
                    // Net length unchanged (pop then push): full is unreachable,
                    // but the refusal path stays total, not asserted away.
                    Ok(()) => Step::Stay,
                    Err(FsmStashFull(m)) => {
                        S::on_defer_full(
                            &mut self.data,
                            &self.state,
                            m,
                            actor_ref.clone(),
                            &mut self.stash,
                        )
                        .await?
                    }
                },
                Disposition::Ignore => Step::Stay,
            };
            if let Some(stop) = self.apply(step, &actor_ref) {
                return Ok(stop);
            }
        }
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), S::Error> {
        S::on_stop(&mut self.data, actor_ref, reason).await
    }

    async fn on_panic(&mut self, _: WeakActorRef<Self>, err: PanicError) -> ActorStopReason {
        ActorStopReason::Panicked(err)
    }
}
