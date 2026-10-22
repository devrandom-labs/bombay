//! A one-shot application reader; every effect is an ordinary typed Actions send.
use super::requester::{
    CounterOperation, CounterProtectedMeaning, CounterRequests, CounterServiceInput, Scope,
};
use bombay::behavior::{EstablishedDelivery, EstablishedRecipient};
use bombay::prelude::{Actions, BehaviorActed};
use std::fmt;

pub(super) struct Reader {
    pub(super) service: EstablishedRecipient<CounterRequests>,
    pub(super) scope: Scope,
    pub(super) replies: Vec<CounterProtectedMeaning>,
}

pub(super) struct ReaderReply {
    pub(super) scope: Scope,
    pub(super) meaning: CounterProtectedMeaning,
}

#[derive(thiserror::Error)]
#[error("the reply does not belong to this one-shot read")]
pub(super) struct ReaderError(pub(super) Box<ReaderReply>);

impl fmt::Debug for ReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReaderError(..)")
    }
}

#[bombay::actor(
    sends = pub(super) {
        requests: Vec<EstablishedDelivery<CounterRequests>>,
    },
    error = ReaderError,
)]
impl Reader {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the selected Behavior actor expansion uses the same fallible fold contract for initialization and typed receive failures"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().send_requests(EstablishedDelivery::new(
            self.service.clone(),
            CounterServiceInput::Verify(self.scope.identity.sequence),
        )))
    }

    fn receive(&mut self, reply: ReaderReply) -> BehaviorActed<Self> {
        if reply.scope != self.scope
            || self.scope.operation != CounterOperation::Read
            || !matches!(reply.meaning, CounterProtectedMeaning::ReadReply(_))
            || !self.replies.is_empty()
        {
            return Err(ReaderError(Box::new(reply)));
        }
        let purpose = reply.meaning.purpose();
        self.replies.push(reply.meaning);
        Ok(Actions::stop().send_requests(EstablishedDelivery::new(
            self.service.clone(),
            CounterServiceInput::ProtectedConsumed {
                correlation: self.scope.identity.sequence,
                purpose,
            },
        )))
    }
}
