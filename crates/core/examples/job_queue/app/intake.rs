//! The `Intake` front door: a bounded-stash capability defers `Submit`s
//! during maintenance and replays them on `Resume` (ADR-0022, card #279).

use core::convert::Infallible;

use bombay::{actor::Flow, capability, mailbox::Capacity, reply::ReplySender};

use super::{Dispatcher, DispatcherMsg, Job, SubmitError};

/// Front door for submissions (card #224, migrated to the capability surface at
/// card #279).
///
/// A plain [`capability::Actor`] carrying a [`capability::Stashing`] capability: it defers
/// `Submit`s during maintenance and forwards them on `Resume` — the
/// walking-skeleton demonstration of the bounded stash as a capability.
pub struct Intake {
    dispatcher: capability::Handle<Dispatcher>,
    maintenance: bool,
}

#[allow(
    dead_code,
    reason = "Pause/Resume are exercised by the integration tests"
)]
#[derive(Debug, bombay_macros::Msg)]
pub enum IntakeMsg {
    Submit {
        job: Job,
        reply: ReplySender<(), SubmitError>,
    },
    Pause,
    Resume,
}

/// The intake's capability set: bounded deferral, nothing else. `#[derive]`
/// emits the `Provide` access seam AND the `Replay` loop hook — the stash is
/// serviced automatically, impossible to forget.
#[derive(bombay_macros::Provide)]
pub struct IntakeCaps {
    stash: capability::Stashing<IntakeMsg>,
}

/// Stash capacity, threaded from the spawn args (never a `SpawnConfig` field,
/// ADR-0022 D8).
pub struct IntakePolicy;

impl capability::StashPolicy<Intake> for IntakePolicy {
    fn capacity((_, cap): &<Intake as capability::Actor>::Args) -> Capacity {
        *cap
    }
}

impl capability::CapSet<Intake> for IntakeCaps {
    fn build(args: &<Intake as capability::Actor>::Args) -> Self {
        Self {
            stash: capability::Stashing::bounded(<IntakePolicy as capability::StashPolicy<
                Intake,
            >>::capacity(args)),
        }
    }
}

impl capability::Actor for Intake {
    type Msg = IntakeMsg;
    type Args = (capability::Handle<Dispatcher>, Capacity);
    type Error = Infallible;
    type Caps = IntakeCaps;

    async fn init(
        (dispatcher, _): Self::Args,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            dispatcher,
            maintenance: false,
        })
    }

    async fn handle(
        &mut self,
        msg: IntakeMsg,
        mut cx: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Infallible> {
        match msg {
            IntakeMsg::Pause => self.maintenance = true,
            IntakeMsg::Resume => {
                self.maintenance = false;
                cx.cap::<capability::Stashing<IntakeMsg>>().unstash_all();
            }
            submit @ IntakeMsg::Submit { .. } if self.maintenance => {
                // Full stash = shed load with the same typed refusal the
                // dispatcher uses for a full queue: the asker learns NOW.
                if let Err(overflow) = cx.cap::<capability::Stashing<IntakeMsg>>().stash(submit)
                    && let IntakeMsg::Submit { reply, .. } = overflow.msg()
                {
                    let _ = reply.send_err(SubmitError::QueueFull);
                }
            }
            IntakeMsg::Submit { job, reply } => {
                // Forward: the dispatcher answers the original asker directly.
                if self
                    .dispatcher
                    .tell(DispatcherMsg::Submit { job, reply })
                    .await
                    .is_err()
                {
                    // Dispatcher gone: nothing to answer with — the dropped
                    // reply port surfaces the typed ask-side error.
                }
            }
        }
        Ok(Flow::Continue)
    }
}
