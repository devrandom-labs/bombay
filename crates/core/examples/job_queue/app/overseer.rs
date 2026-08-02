//! The `Overseer`: app-level death-watch consumer with a NAMED recording
//! policy (never propagates).

use core::convert::Infallible;

use bombay::{
    ActorId,
    actor::Flow,
    capability::{self, Never, Step},
    error::ActorStopReason,
    reply::ReplySender,
};

/// Watches the dispatcher from outside — the app-level death-watch consumer.
#[derive(Debug, bombay_macros::Msg)]
pub enum OverseerMsg {
    /// Did the dispatcher die, and was it a normal stop?
    Observed {
        reply: ReplySender<Option<(ActorId, bool)>>,
    },
}

pub struct Overseer {
    seen: Option<(ActorId, bool)>,
}

/// The overseer's death reaction, a NAMED policy (stage 3): record who died
/// and whether it was a normal stop; never propagate.
pub struct RecordDeath;

impl capability::WatchPolicy<Overseer> for RecordDeath {
    async fn on_link_died(
        actor: &mut Overseer,
        id: ActorId,
        reason: ActorStopReason,
        _linked: bool,
    ) -> Result<Step<Never, ActorStopReason>, Infallible> {
        actor.seen = Some((id, reason.is_normal()));
        Ok(Step::Continue)
    }
}

/// The overseer's capability set: watching with the recording policy.
#[derive(bombay_macros::Provide)]
pub struct OverseerCaps {
    watching: capability::Watching<RecordDeath>,
}

impl capability::CapSet<Overseer> for OverseerCaps {
    fn build((): &()) -> Self {
        Self {
            watching: capability::Watching::new(),
        }
    }
}

impl capability::Actor for Overseer {
    type Msg = OverseerMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = OverseerCaps;

    async fn init((): (), _: capability::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Self { seen: None })
    }

    async fn handle(
        &mut self,
        msg: OverseerMsg,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Self::Error> {
        let OverseerMsg::Observed { reply } = msg;
        let _ = reply.send(self.seen);
        Ok(Flow::Continue)
    }
}
