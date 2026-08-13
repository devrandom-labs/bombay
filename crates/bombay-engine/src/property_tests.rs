//! Property tests over arbitrary event sequences.
//!
//! Verifies the Driver's run-to-completion law holds for arbitrary
//! event sequences: every event is processed exactly once, in order,
//! and each effect is interpreted before the next event is consumed.

#[cfg(test)]
mod run_to_completion {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, Step, User};
    use proptest::prelude::*;

    use crate::{Driver, Environment, RunExit, RuntimeEffects};

    struct EchoEnv {
        events: VecDeque<Option<u64>>,
        effects: Arc<Mutex<Vec<Vec<u64>>>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for EchoEnv {
        type Event = User<MailAddr, u64>;
        type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
        type Error = std::convert::Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User::new(MailAddr(0), v))
        }

        async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
            self.effects.lock().unwrap().push(effect.sends);
            Ok(())
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    struct Echo {
        value: u64,
    }

    impl Behavior for Echo {
        type Addr = MailAddr;
        type Msg = u64;
        type Event = User<MailAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = std::convert::Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, event: Self::Event) -> BehaviorActed<Self> {
            self.value = event.message;
            Ok(Actions {
                sends: vec![event.message],
                creates: Vec::new(),
                become_: Step::Continue,
            })
        }
    }

    proptest! {
        /// # Property: run-to-completion for arbitrary event sequences.
        ///
        /// For any sequence of events, the driver processes each event
        /// exactly once, in order, producing one effect per event (plus
        /// one empty init effect), then retires exactly once.
        #[test]
        fn every_event_processed_once_in_order(
            events in prop::collection::vec(0u64..1000, 0..50)
        ) {
            let effects = Arc::new(Mutex::new(Vec::new()));
            let retired = Arc::new(Mutex::new(false));

            let mut queued: VecDeque<Option<u64>> =
                events.iter().copied().map(Some).collect();
            queued.push_back(None);

            let env = EchoEnv {
                events: queued,
                effects: effects.clone(),
                retired: retired.clone(),
            };
            let mut driver = Driver::new(Echo { value: 0 }, env);

            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let outcome = rt.block_on(async move { driver.run().await });
            prop_assert!(matches!(outcome, Ok(RunExit::EnvironmentClosed)));

            let log = effects.lock().unwrap();
            prop_assert_eq!(log.len(), events.len() + 1, "init + N events");
            prop_assert!(log[0].is_empty(), "init effect has no sends");
            for (i, event) in events.iter().enumerate() {
                prop_assert_eq!(&log[i + 1], &vec![*event]);
            }
            prop_assert!(*retired.lock().unwrap());
        }
    }
}
