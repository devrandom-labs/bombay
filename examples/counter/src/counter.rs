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

    #[cfg(test)]
    pub(super) const fn at_maximum() -> Self {
        Self { value: u64::MAX }
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

#[cfg(test)]
// This shared owner is also compiled by authorized; main owns the real consumer.
#[allow(dead_code)]
pub(super) fn assert_increment_processing_actions(
    processing_reply_to: EstablishedRecipient<CounterProcessed>,
    values_reply_to: EstablishedRecipient<CounterValue>,
) {
    let initialized = Counter::new().initialize().unwrap();
    let Actions {
        sends: CounterSends {
            values,
            processed_increments,
        },
        creates,
        become_,
    } = initialized.actions;
    assert!(values.is_empty());
    assert!(processed_increments.is_empty());
    assert!(creates.is_empty());
    assert!(matches!(become_, Step::Continue));
    let mut counter = initialized.behavior;
    for request_sequence in [5, 13] {
        let increment = counter
            .receive(
                MailAddr(1),
                CounterMessage::IncrementAndReply {
                    request_sequence,
                    reply_to: processing_reply_to.clone(),
                },
            )
            .unwrap();
        let Actions {
            sends:
                CounterSends {
                    values,
                    processed_increments,
                },
            creates,
            become_,
        } = increment;
        assert!(matches!(become_, Step::Continue));
        assert!(creates.is_empty());
        assert!(values.is_empty());
        assert_eq!(processed_increments.len(), 1);
        assert_eq!(
            processed_increments[0].message,
            CounterIncremented { request_sequence },
        );
    }
    assert_eq!(counter.value, 2);
    let increment = counter
        .receive(MailAddr(1), CounterMessage::Increment)
        .unwrap();
    assert_eq!(counter.value, 3);
    let Actions {
        sends: CounterSends {
            values,
            processed_increments,
        },
        creates,
        become_,
    } = increment;
    assert!(values.is_empty());
    assert!(processed_increments.is_empty());
    assert!(creates.is_empty());
    assert!(matches!(become_, Step::Continue));
    let read = counter
        .receive(MailAddr(1), CounterMessage::Read(values_reply_to))
        .unwrap();
    let Actions {
        sends: CounterSends {
            values,
            processed_increments,
        },
        creates,
        become_,
    } = read;
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].message, 3);
    assert!(processed_increments.is_empty());
    assert!(creates.is_empty());
    assert!(matches!(become_, Step::Continue));
    let mut exhausted = Counter::at_maximum().initialize().unwrap().behavior;
    let overflow = exhausted.receive(
        MailAddr(1),
        CounterMessage::IncrementAndReply {
            request_sequence: 29,
            reply_to: processing_reply_to,
        },
    );
    assert!(matches!(overflow, Err(CounterError::Overflow)));
    assert_eq!(exhausted.value, u64::MAX);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_and_increment_preserve_the_complete_actions() {
        let initialized = Counter::new()
            .initialize()
            .expect("zero initialization succeeds");
        let initialization_actions = initialized.actions;
        let mut counter = initialized.behavior;
        let increment = counter
            .receive(MailAddr(1), CounterMessage::Increment)
            .expect("increment below the limit succeeds");

        assert_eq!(counter.value, 1);
        assert!(matches!(initialization_actions.become_, Step::Continue));
        assert!(initialization_actions.sends.values.is_empty());
        assert!(matches!(increment.become_, Step::Continue));
        assert!(increment.sends.values.is_empty());
    }

    #[test]
    fn overflow_preserves_state_and_returns_the_exact_domain_error() {
        let mut counter = Counter { value: u64::MAX }
            .initialize()
            .expect("counter initialization succeeds")
            .behavior;

        let result = counter.receive(MailAddr(1), CounterMessage::Increment);

        assert!(matches!(result, Err(CounterError::Overflow)));
        assert_eq!(counter.value, u64::MAX);
    }
}
