//! The `AuditLog`: append-only acceptance trail — a plain capability
//! actor (`Caps = ()`, card #278).

use core::convert::Infallible;

use bombay::{actor::Flow, capability, reply::ReplySender};

use super::bump;

/// The audit sink's closed menu.
#[allow(
    dead_code,
    reason = "Count and the recorded job id are exercised by the integration tests"
)]
#[derive(Debug, bombay_macros::Msg)]
pub enum AuditMsg {
    /// A submission was accepted by the dispatcher.
    Recorded { job_id: u64 },
    /// How many acceptances have been recorded?
    Count { reply: ReplySender<u64> },
}

/// Append-only audit trail on the distilled capability surface (ADR-0026 stage
/// 1, card #278): ONE trait impl, `Caps = ()`, spawned via
/// [`capability::spawn`] — the walking-skeleton demonstration that a plain
/// capability actor composes with the existing app unchanged.
pub struct AuditLog {
    entries: u64,
}

impl capability::Actor for AuditLog {
    type Msg = AuditMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = ();

    async fn init((): (), _: capability::Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { entries: 0 })
    }

    async fn handle(
        &mut self,
        msg: AuditMsg,
        _: capability::Ctx<'_, Self>,
    ) -> Result<Flow, Infallible> {
        match msg {
            AuditMsg::Recorded { job_id: _ } => bump(&mut self.entries),
            AuditMsg::Count { reply } => {
                let _ = reply.send(self.entries);
            }
        }
        Ok(Flow::Continue)
    }
}
