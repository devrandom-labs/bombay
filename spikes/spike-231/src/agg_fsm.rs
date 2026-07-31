//! The nexus-shaped aggregate lifecycle via the mock `FsmActor` (shape (b)
//! + P-style declarative admission).
//!
//! Loading: fold replay events; commands deferred BY DECLARATION; 30 s
//!          rehydration deadline.
//! Ready:   execute commands.
//! Draining: refuse commands; stop once the snapshot flush completes.
//!
//! Note what is ABSENT relative to `agg_idiom`: no phase field, no timer
//! field, no manual stash call, no manual unstash, no manual cancel, no
//! stale-deadline guard arms, no deadline message variant in the menu —
//! and `handle` never sees a message its state declared away.

use core::convert::Infallible;
use std::time::Duration;

use bombay::{
    actor::ActorRef,
    mailbox::{Capacity, Mailboxed},
    message::Msg,
};

use crate::{
    Probe, ProbeTx,
    fsm::{Disposition, Fsm, FsmActor, FsmMsg, FsmStash, Step},
};

pub const LOAD_DEADLINE: Duration = Duration::from_secs(30);

/// State NAMES only — data lives in `AggFsm` (the gen_statem split).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Loading,
    Ready,
    Draining,
}

/// The closed menu. No deadline variant: the state timeout is framework-side.
#[derive(Debug)]
pub enum AggMsg {
    Replay { ev: u64, last: bool },
    Cmd { id: u64 },
    Drain,
    FlushDone,
}

impl Msg for AggMsg {}

pub struct AggFsm {
    probe: ProbeTx,
    /// Folded rehydration state (stands in for the aggregate's real state).
    applied: u64,
}

impl Mailboxed for AggFsm {
    type Msg = AggMsg;
}

impl FsmActor for AggFsm {
    type Args = (ProbeTx, Capacity);
    type Error = Infallible;
    type State = Phase;

    fn initial_state(_: &Self::Args) -> Phase {
        Phase::Loading
    }

    fn stash_capacity((_, cap): &Self::Args) -> Capacity {
        *cap
    }

    fn state_timeout(state: &Phase) -> Option<Duration> {
        match state {
            Phase::Loading => Some(LOAD_DEADLINE),
            Phase::Ready | Phase::Draining => None,
        }
    }

    /// The whole admission protocol, in one declarative place.
    fn gate(state: &Phase, msg: &AggMsg) -> Disposition {
        match (state, msg) {
            // Commands cannot be decided before the fold completes.
            (Phase::Loading, AggMsg::Cmd { .. }) => Disposition::Defer,
            // Late/duplicate signals outside their phase: declared noise.
            (Phase::Ready | Phase::Draining, AggMsg::Replay { .. })
            | (Phase::Loading | Phase::Ready, AggMsg::FlushDone)
            | (Phase::Draining, AggMsg::Drain) => Disposition::Ignore,
            _ => Disposition::Deliver,
        }
    }

    async fn on_start((probe, _): Self::Args, _: ActorRef<Fsm<Self>>) -> Result<Self, Infallible> {
        Ok(Self { probe, applied: 0 })
    }

    async fn handle(
        &mut self,
        state: &Phase,
        msg: AggMsg,
        actor_ref: ActorRef<Fsm<Self>>,
        _stash: &mut FsmStash<AggMsg>,
    ) -> Result<Step<Phase>, Infallible> {
        Ok(match (state, msg) {
            (Phase::Loading, AggMsg::Replay { ev, last }) => {
                self.applied = self.applied.wrapping_add(ev);
                let _ = self.probe.send(Probe::Applied(ev));
                if last { Step::Goto(Phase::Ready) } else { Step::Stay }
            }
            (Phase::Ready, AggMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Processed(id));
                Step::Stay
            }
            (Phase::Ready | Phase::Loading, AggMsg::Drain) => {
                let _ = self.probe.send(Probe::DrainStarted);
                // Simulated async snapshot flush completing later.
                // MOCK WART: the envelope leaks into user code here — the
                // real (in-crate, no-envelope) build would be
                // `actor_ref.tell(AggMsg::FlushDone)`.
                let _ = actor_ref.tell(FsmMsg::User(AggMsg::FlushDone)).await;
                Step::Goto(Phase::Draining)
            }
            (Phase::Draining, AggMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Refused(id));
                Step::Stay
            }
            (Phase::Draining, AggMsg::FlushDone) => {
                let _ = self.probe.send(Probe::Snapshotted);
                Step::Stop
            }
            // Unreachable by declaration: every remaining (state, msg) pair
            // is gated `Defer` or `Ignore` above. Rust's exhaustiveness
            // cannot see the gate, so the arm must exist — recorded as a
            // wart of gate-plus-exhaustive-match.
            _ => Step::Stay,
        })
    }

    /// Loud shedding at stash capacity — same typed-refusal behavior as the
    /// idiom variant's overflow branch.
    async fn on_defer_full(
        &mut self,
        _: &Phase,
        msg: AggMsg,
        _: ActorRef<Fsm<Self>>,
        _: &mut FsmStash<AggMsg>,
    ) -> Result<Step<Phase>, Infallible> {
        if let AggMsg::Cmd { id } = msg {
            let _ = self.probe.send(Probe::ShedFull(id));
        }
        Ok(Step::Stay)
    }

    async fn on_state_timeout(
        &mut self,
        state: &Phase,
        _: ActorRef<Fsm<Self>>,
        _: &mut FsmStash<AggMsg>,
    ) -> Result<Step<Phase>, Infallible> {
        Ok(match state {
            Phase::Loading => {
                let _ = self.probe.send(Probe::LoadTimedOut);
                Step::Stop
            }
            // Unreachable: no timeout declared for these states, and stale
            // timeouts are filtered by the wrapper. Kept total anyway.
            Phase::Ready | Phase::Draining => {
                let _ = self.probe.send(Probe::StaleTimeoutLeaked);
                Step::Stay
            }
        })
    }
}
