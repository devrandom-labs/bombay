use bombay::prelude::*;

pub(crate) struct CounterValue;

impl Protocol for CounterValue {
    type Addr = MailAddr;
    type Msg = u64;
}

pub(crate) enum CounterMessage {
    Increment,
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
}

#[bombay::actor(
    sends = { values: Vec<EstablishedDelivery<CounterValue>> },
    error = CounterError,
)]
impl Counter {
    pub(crate) fn receive(&mut self, message: CounterMessage) -> BehaviorActed<Self> {
        match message {
            CounterMessage::Increment => {
                self.value = self.value.checked_add(1).ok_or(CounterError::Overflow)?;
                Ok(Actions::cont())
            }
            CounterMessage::Read(reply_to) => {
                Ok(Actions::cont().send_values(EstablishedDelivery::new(reply_to, self.value)))
            }
        }
    }
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
