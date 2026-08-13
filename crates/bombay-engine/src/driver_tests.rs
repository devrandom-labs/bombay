//! Engine law tests with falsification oracles.
//!
//! Every test verifies one invariant of the driver orchestration.
//! Each test can fail when the production implementation is deliberately
//! inverted — add oracles that would detect such inversion.

#[cfg(test)]
mod init_order {
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, Behavior, Never, NoBirths, Step, User};

    use crate::{Driver, Environment, RuntimeEffects};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestAddr(u64);

    impl behavior::Address for TestAddr {
        type Nonce = u64;
        fn birth(self, nonce: Self::Nonce) -> Self {
            Self(nonce)
        }
    }

    /// A behavior whose init produces a specific effect.
    struct InitEffect {
        init_value: u64,
    }

    impl Behavior for InitEffect {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = std::convert::Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions {
                sends: vec![self.init_value],
                creates: Vec::new(),
                become_: Step::Continue,
            })
        }

        fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
            self.init_value = event.message;
            Ok(Actions {
                sends: vec![event.message],
                creates: Vec::new(),
                become_: Step::Continue,
            })
        }
    }

    #[derive(Default)]
    struct LogEnv {
        effects: Arc<Mutex<Vec<Vec<u64>>>>,
        events: std::collections::VecDeque<Option<u64>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for LogEnv {
        type Event = User<TestAddr, u64>;
        type Effect = RuntimeEffects<TestAddr, Vec<u64>, NoBirths>;
        type Error = std::convert::Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User {
                from: TestAddr(0),
                message: v,
            })
        }

        async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
            self.effects.lock().unwrap().push(effect.sends);
            Ok(())
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    /// # Falsifier: init effects must appear before any event-driven effect.
    ///
    /// If the driver processes events before init effects, the first
    /// logged effect would be `[1]` instead of `[42]`.
    #[tokio::test]
    async fn init_effects_before_first_event() {
        let effects = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::new(Mutex::new(false));

        let env = LogEnv {
            effects: effects.clone(),
            events: vec![Some(1), Some(2), None].into(),
            retired: retired.clone(),
        };
        let behavior = InitEffect { init_value: 42 };
        let mut driver = Driver::new(behavior, env);

        let _ = driver.run().await;
        let log = effects.lock().unwrap();
        assert_eq!(log.len(), 3, "init + 2 events = 3 effects");
        assert_eq!(log[0], vec![42], "init effect must be first");
        assert_eq!(log[1], vec![1]);
        assert_eq!(log[2], vec![2]);
        assert!(*retired.lock().unwrap());
    }
}

#[cfg(test)]
mod explicit_stop {
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, Behavior, Exit, Never, NoBirths, Step, User};

    use crate::{Driver, Environment, RunExit, RuntimeEffects};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestAddr(u64);

    impl behavior::Address for TestAddr {
        type Nonce = u64;
        fn birth(self, nonce: Self::Nonce) -> Self {
            Self(nonce)
        }
    }

    struct StopAfter {
        count: usize,
        limit: usize,
    }

    impl Behavior for StopAfter {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = std::convert::Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, event: Self::Event) -> behavior::BehaviorActed<Self> {
            self.count += 1;
            if self.count >= self.limit {
                Ok(Actions {
                    sends: vec![event.message],
                    creates: Vec::new(),
                    become_: Step::Stop(Exit::Normal),
                })
            } else {
                Ok(Actions {
                    sends: vec![event.message],
                    creates: Vec::new(),
                    become_: Step::Continue,
                })
            }
        }
    }

    struct SimpleEnv {
        events: std::collections::VecDeque<Option<u64>>,
        log: Arc<Mutex<Vec<u64>>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for SimpleEnv {
        type Event = User<TestAddr, u64>;
        type Effect = RuntimeEffects<TestAddr, Vec<u64>, NoBirths>;
        type Error = std::convert::Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User {
                from: TestAddr(0),
                message: v,
            })
        }

        async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
            self.log.lock().unwrap().extend(effect.sends);
            Ok(())
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    /// # Falsifier: explicit stop must produce `Stopped` exit before
    /// processing further events.
    ///
    /// If the driver ignores the stop, it would process all 3 events
    /// and return `EnvironmentClosed` instead of `Stopped`.
    #[tokio::test]
    async fn stop_produces_stopped_not_closed() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::new(Mutex::new(false));

        let env = SimpleEnv {
            events: vec![Some(1), Some(2), Some(3)].into(),
            log: log.clone(),
            retired: retired.clone(),
        };
        let behavior = StopAfter { count: 0, limit: 2 };
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        match result {
            Ok(RunExit::Stopped(Exit::Normal)) => {}
            other => panic!("expected Stopped(Normal), got {other:?}"),
        }
        assert_eq!(log.lock().unwrap().len(), 2, "only 2 events before stop");
        assert!(*retired.lock().unwrap());
    }
}

#[cfg(test)]
mod error_propagation {
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, Behavior, Never, NoBirths, User};

    use crate::{Driver, Environment, RunError, RuntimeEffects};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestAddr(u64);

    impl behavior::Address for TestAddr {
        type Nonce = u64;
        fn birth(self, nonce: Self::Nonce) -> Self {
            Self(nonce)
        }
    }

    struct FailsInit;

    impl Behavior for FailsInit {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = &'static str;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Err("init failure")
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            unreachable!()
        }
    }

    struct FailsTransition;

    impl Behavior for FailsTransition {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = &'static str;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            Err("transition failure")
        }
    }

    struct NoopEnv {
        events: std::collections::VecDeque<Option<u64>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for NoopEnv {
        type Event = User<TestAddr, u64>;
        type Effect = RuntimeEffects<TestAddr, Vec<u64>, NoBirths>;
        type Error = std::convert::Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User {
                from: TestAddr(0),
                message: v,
            })
        }

        async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    /// # Falsifier: behavior init error must propagate as `RunError::Behavior`.
    ///
    /// If the driver silently ignores init errors, this would return
    /// `Ok(EnvironmentClosed)`.
    #[tokio::test]
    async fn init_error_propagates() {
        let retired = Arc::new(Mutex::new(false));
        let env = NoopEnv {
            events: vec![Some(1)].into(),
            retired: retired.clone(),
        };
        let behavior = FailsInit;
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        match result {
            Err(RunError::Behavior("init failure")) => {}
            other => panic!("expected Behavior(\"init failure\"), got {other:?}"),
        }
        assert!(*retired.lock().unwrap());
    }

    /// # Falsifier: behavior transition error must propagate as `RunError::Behavior`.
    #[tokio::test]
    async fn transition_error_propagates() {
        let retired = Arc::new(Mutex::new(false));
        let env = NoopEnv {
            events: vec![Some(1)].into(),
            retired: retired.clone(),
        };
        let behavior = FailsTransition;
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        match result {
            Err(RunError::Behavior("transition failure")) => {}
            other => panic!("expected Behavior(\"transition failure\"), got {other:?}"),
        }
        assert!(*retired.lock().unwrap());
    }

    /// An environment that fails on every interpretation.
    struct FailingEnv {
        events: std::collections::VecDeque<Option<u64>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for FailingEnv {
        type Event = User<TestAddr, u64>;
        type Effect = RuntimeEffects<TestAddr, Vec<u64>, NoBirths>;
        type Error = &'static str;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User {
                from: TestAddr(0),
                message: v,
            })
        }

        async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Self::Error> {
            Err("environment rejected effect")
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    struct PassingBehavior;

    impl Behavior for PassingBehavior {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = std::convert::Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    /// # Falsifier: environment error must propagate as `RunError::Environment`.
    ///
    /// If the driver ignores environment errors and continues the event loop,
    /// this test would hang or return `EnvironmentClosed` instead of the error.
    #[tokio::test]
    async fn environment_error_propagates() {
        let retired = Arc::new(Mutex::new(false));
        let env = FailingEnv {
            events: vec![Some(1)].into(),
            retired: retired.clone(),
        };
        let behavior = PassingBehavior;
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        match result {
            Err(RunError::Environment("environment rejected effect")) => {}
            other => panic!("expected Environment error, got {other:?}"),
        }
        assert!(*retired.lock().unwrap());
    }
}

#[cfg(test)]
mod retirement_guarantee {
    use std::sync::{Arc, Mutex};

    use behavior::{Actions, Behavior, Exit, Never, NoBirths, Step, User};

    use crate::{Driver, Environment, RunExit, RuntimeEffects};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestAddr(u64);

    impl behavior::Address for TestAddr {
        type Nonce = u64;
        fn birth(self, nonce: Self::Nonce) -> Self {
            Self(nonce)
        }
    }

    struct Counter {
        events: std::collections::VecDeque<Option<u64>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for Counter {
        type Event = User<TestAddr, u64>;
        type Effect = RuntimeEffects<TestAddr, Vec<u64>, NoBirths>;
        type Error = std::convert::Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            self.events.pop_front()?.map(|v| User {
                from: TestAddr(0),
                message: v,
            })
        }

        async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn retire(&mut self) {
            *self.retired.lock().unwrap() = true;
        }
    }

    struct StopAtInit;

    impl Behavior for StopAtInit {
        type Addr = TestAddr;
        type Msg = u64;
        type Event = User<TestAddr, u64>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = std::convert::Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions {
                sends: Vec::new(),
                creates: Vec::new(),
                become_: Step::Stop(Exit::Normal),
            })
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            unreachable!()
        }
    }

    /// # Falsifier: `retire()` must be called on stop.
    #[tokio::test]
    async fn retire_called_on_stop() {
        let retired = Arc::new(Mutex::new(false));
        let env = Counter {
            events: vec![Some(1), None].into(),
            retired: retired.clone(),
        };
        let behavior = StopAtInit;
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        assert!(matches!(result, Ok(RunExit::Stopped(Exit::Normal))));
        assert!(*retired.lock().unwrap());
    }

    #[allow(clippy::items_after_statements)]
    /// # Falsifier: `retire()` must be called on environment closure.
    #[tokio::test]
    async fn retire_called_on_closure() {
        let retired = Arc::new(Mutex::new(false));
        let env = Counter {
            events: vec![None].into(),
            retired: retired.clone(),
        };

        struct Echo;
        impl Behavior for Echo {
            type Addr = TestAddr;
            type Msg = u64;
            type Event = User<TestAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = std::convert::Infallible;
            type Birth = NoBirths;
            fn init(&mut self) -> behavior::BehaviorActed<Self> {
                Ok(Actions::cont())
            }
            fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
                Ok(Actions::cont())
            }
        }

        let behavior = Echo;
        let mut driver = Driver::new(behavior, env);

        let result = driver.run().await;
        assert!(matches!(result, Ok(RunExit::EnvironmentClosed)));
        assert!(*retired.lock().unwrap());
    }

    #[allow(clippy::items_after_statements)]
    /// # Falsifier: `retire()` must be called on error.
    #[tokio::test]
    async fn retire_called_on_error() {
        let retired = Arc::new(Mutex::new(false));
        let env = Counter {
            events: vec![Some(1)].into(),
            retired: retired.clone(),
        };

        struct FailsInit;
        impl Behavior for FailsInit {
            type Addr = TestAddr;
            type Msg = u64;
            type Event = User<TestAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = &'static str;
            type Birth = NoBirths;
            fn init(&mut self) -> behavior::BehaviorActed<Self> {
                Err("fail")
            }
            fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
                unreachable!()
            }
        }

        let behavior = FailsInit;
        let mut driver = Driver::new(behavior, env);

        let _ = driver.run().await;
        assert!(*retired.lock().unwrap());
    }
}
