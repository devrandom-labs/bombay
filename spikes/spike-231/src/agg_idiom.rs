//! The nexus-shaped aggregate lifecycle via the shipped surface (shape (a)):
//! `StashActor`/`Stashed` + `match self.phase` + manual transition
//! bookkeeping.
//!
//! Every site marked `// [F#]` is a FORGETTABLE STEP: correct code the user
//! must remember at that spot, whose omission compiles clean and fails
//! silently (or subtly) at runtime. The equivalence oracle in
//! `tests/equivalence.rs` is what catches an omission here.

use core::convert::Infallible;
use std::time::Duration;

use bombay::{
    actor::{ActorRef, Flow, TimerHandle},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    stash::{Stash, StashActor, Stashed},
};

use crate::{Probe, ProbeTx};

pub const LOAD_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Loading,
    Ready,
    Draining,
}

/// The closed menu — one variant MORE than the fsm variant: the rehydration
/// deadline must be a user-visible message to be schedulable at all.
#[derive(Debug)]
pub enum AggMsg {
    Replay { ev: u64, last: bool },
    Cmd { id: u64 },
    Drain,
    FlushDone,
    LoadDeadline,
}

impl Msg for AggMsg {}

pub struct AggIdiom {
    probe: ProbeTx,
    applied: u64,
    /// State NAME and state DATA fused in one struct — the Rust idiom.
    phase: Phase,
    /// [F1] The deadline timer must be carried by hand to be cancellable.
    load_timer: Option<TimerHandle>,
}

impl Mailboxed for AggIdiom {
    type Msg = AggMsg;
}

impl StashActor for AggIdiom {
    type Args = (ProbeTx, Capacity);
    type Error = Infallible;

    fn stash_capacity((_, cap): &Self::Args) -> Capacity {
        *cap
    }

    async fn on_start(
        (probe, _): Self::Args,
        actor_ref: ActorRef<Stashed<Self>>,
    ) -> Result<Self, Infallible> {
        // [F2] Arm the rehydration deadline by hand.
        let load_timer = Some(actor_ref.send_after(LOAD_DEADLINE, AggMsg::LoadDeadline));
        Ok(Self {
            probe,
            applied: 0,
            phase: Phase::Loading,
            load_timer,
        })
    }

    async fn handle(
        &mut self,
        msg: AggMsg,
        actor_ref: ActorRef<Stashed<Self>>,
        stash: &mut Stash<AggMsg>,
    ) -> Result<Flow, Infallible> {
        match (self.phase, msg) {
            (Phase::Loading, AggMsg::Replay { ev, last }) => {
                self.applied = self.applied.wrapping_add(ev);
                let _ = self.probe.send(Probe::Applied(ev));
                if last {
                    self.phase = Phase::Ready;
                    // [F3] Release deferred commands — forgetting this
                    // strands every stashed command forever, silently.
                    stash.unstash_all();
                    // [F4] Cancel the state-scoped deadline — forgetting
                    // this delivers a bogus LoadDeadline while Ready.
                    if let Some(t) = self.load_timer.take() {
                        t.cancel();
                    }
                }
            }
            (Phase::Loading, cmd @ AggMsg::Cmd { .. }) => {
                if let Err(overflow) = stash.stash(cmd)
                    && let AggMsg::Cmd { id } = overflow.msg()
                {
                    let _ = self.probe.send(Probe::ShedFull(id));
                }
            }
            (Phase::Loading, AggMsg::LoadDeadline) => {
                let _ = self.probe.send(Probe::LoadTimedOut);
                return Ok(Flow::Stop);
            }
            (Phase::Ready, AggMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Processed(id));
            }
            (Phase::Ready | Phase::Loading, AggMsg::Drain) => {
                let _ = self.probe.send(Probe::DrainStarted);
                // [F4-dup] Leaving Loading via THIS edge must also cancel
                // the deadline — per-edge bookkeeping, not per-state.
                if let Some(t) = self.load_timer.take() {
                    t.cancel();
                }
                self.phase = Phase::Draining;
                // [F6] Release on THIS edge too (gen_statem retries
                // postponed events on EVERY state change): deferred
                // commands replay into Draining and get an explicit
                // refusal. Forgetting this silently drops them at stop —
                // the first draft of this very file had that bug.
                stash.unstash_all();
                // Simulated async snapshot flush completing later.
                let _ = actor_ref.tell(AggMsg::FlushDone).await;
            }
            (Phase::Draining, AggMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Refused(id));
            }
            (Phase::Draining, AggMsg::FlushDone) => {
                let _ = self.probe.send(Probe::Snapshotted);
                return Ok(Flow::Stop);
            }
            // [F5] Stale-deadline guards: the timeout can race the
            // transition (fired-and-queued before cancel), so every OTHER
            // phase needs an arm that recognizes and drops it. Exhaustive
            // matching forces AN arm to exist, but not the RIGHT one — a
            // catch-all `_ => {}` would hide the same bug.
            (Phase::Ready | Phase::Draining, AggMsg::LoadDeadline) => {}
            // Late/duplicate signals outside their phase: observed, ignored.
            (_, AggMsg::Replay { .. } | AggMsg::Drain | AggMsg::FlushDone) => {}
        }
        Ok(Flow::Continue)
    }
}
