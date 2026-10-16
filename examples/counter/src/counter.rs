use bombay::behavior::MessageProtocol;
use bombay::prelude::*;

pub(crate) struct CounterValue;

impl Protocol for CounterValue {
    type Addr = MailAddr;
    type Msg = u64;
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CounterIncremented {
    pub(crate) request_sequence: u64,
}

pub(crate) type CounterProcessed = MessageProtocol<MailAddr, CounterIncremented>;

pub(crate) enum CounterMessage {
    Increment,
    // Both examples consume the genuine request-correlated processing receipt.
    #[allow(dead_code)]
    IncrementAndReply {
        request_sequence: u64,
        reply_to: EstablishedRecipient<CounterProcessed>,
    },
    Read(EstablishedRecipient<CounterValue>),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CounterError {
    Overflow,
}

pub(crate) struct Counter {
    value: u64,
}

impl Counter {
    pub(crate) const fn new() -> Self {
        Self { value: 0 }
    }

    fn increment(&mut self) -> Result<(), CounterError> {
        self.value = self.value.checked_add(1).ok_or(CounterError::Overflow)?;
        Ok(())
    }
}

#[bombay::actor(
    sends = pub(crate) {
        values: Vec<EstablishedDelivery<CounterValue>>,
        processed_increments: Vec<EstablishedDelivery<CounterProcessed>>,
    },
    error = CounterError,
)]
impl Counter {
    pub(crate) fn receive(&mut self, message: CounterMessage) -> BehaviorActed<Self> {
        match message {
            CounterMessage::Increment => {
                self.increment()?;
                Ok(Actions::cont())
            }
            CounterMessage::IncrementAndReply {
                request_sequence,
                reply_to,
            } => {
                self.increment()?;
                Ok(
                    Actions::cont().send_processed_increments(EstablishedDelivery::new(
                        reply_to,
                        CounterIncremented { request_sequence },
                    )),
                )
            }
            CounterMessage::Read(reply_to) => {
                Ok(Actions::cont().send_values(EstablishedDelivery::new(reply_to, self.value)))
            }
        }
    }
}
