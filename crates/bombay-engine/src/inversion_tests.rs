//! Inversion oracles for the Driver's production path.
//!
//! Verifies that `ExclusiveExecutor::turn` is the only transition path and
//! that the Driver state machine prevents misuse.

#[cfg(test)]
mod oracles {
    use std::collections::VecDeque;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use behavior::{Actions, Behavior, BehaviorActed, MailAddr, Never, NoBirths, Step, User};

    use crate::{Driver, Environment, RunError, RunExit, RuntimeEffects};

    struct TestEnv {
        events: VecDeque<Option<u64>>,
        effects: Arc<Mutex<Vec<Vec<u64>>>>,
        retired: Arc<Mutex<bool>>,
    }

    impl Environment for TestEnv {
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

    struct Counter {
        value: u64,
    }

    impl Behavior for Counter {
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
            self.value += event.message;
            Ok(Actions {
                sends: vec![event.message],
                creates: Vec::new(),
                become_: Step::Continue,
            })
        }
    }

    /// Events are processed through `ExclusiveExecutor::turn`.
    #[tokio::test]
    async fn events_processed_through_executor() {
        let effects = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::new(Mutex::new(false));

        let env = TestEnv {
            events: vec![Some(1), Some(2), None].into(),
            effects: effects.clone(),
            retired: retired.clone(),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);

        driver.run_init().await.unwrap();
        let result = driver.run_loop().await;
        assert!(matches!(result, Ok(RunExit::EnvironmentClosed)));

        let log = effects.lock().unwrap();
        assert_eq!(log.len(), 3, "init + 2 events");
        assert!(log[0].is_empty(), "init has no sends");
        assert_eq!(log[1], vec![1]);
        assert_eq!(log[2], vec![2]);
    }

    /// `run_loop` on uninitialized driver panics.
    #[tokio::test]
    #[should_panic(expected = "non-running driver")]
    async fn run_loop_panics_before_init() {
        let env = TestEnv {
            events: vec![Some(1)].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        let _ = driver.run_loop().await;
    }

    /// `run_init` called twice panics.
    #[tokio::test]
    #[should_panic(expected = "non-uninitialized")]
    async fn run_init_panics_if_called_twice() {
        let env = TestEnv {
            events: VecDeque::new(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();
        let _ = driver.run_init().await;
    }

    /// A terminal initialization result cannot subsequently accept ingress.
    #[tokio::test]
    #[should_panic(expected = "non-running driver")]
    async fn run_loop_rejects_ingress_after_init_stop() {
        struct StopsInInit;

        impl Behavior for StopsInInit {
            type Addr = MailAddr;
            type Msg = u64;
            type Event = User<MailAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = std::convert::Infallible;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Ok(Actions {
                    sends: Vec::new(),
                    creates: Vec::new(),
                    become_: Step::Stop(behavior::Exit::Normal),
                })
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                panic!("terminal initialization must prevent transition")
            }
        }

        let env = TestEnv {
            events: vec![Some(1)].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(StopsInInit, env);
        assert!(driver.run_init().await.unwrap().is_some());
        let _ = driver.run_loop().await;
    }

    /// Returning a terminal loop outcome closes the public ingress boundary.
    #[test]
    fn environment_closure_rejects_loop_reentry() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let env = TestEnv {
            events: vec![None].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        rt.block_on(driver.run_init()).unwrap();
        assert!(matches!(
            rt.block_on(driver.run_loop()),
            Ok(RunExit::EnvironmentClosed)
        ));

        let reentry = catch_unwind(AssertUnwindSafe(|| {
            let _ = rt.block_on(driver.run_loop());
        }));
        assert!(
            reentry.is_err(),
            "terminal loop result must reject re-entry"
        );
    }

    /// Poison at the executor layer: a `Machine::step` panic leaves the
    /// executor permanently poisoned, and the next `turn` recovers the exact
    /// non-`Clone` input.
    #[test]
    fn exclusive_executor_poisons_and_recovers_exact_input() {
        use bombay_machine_executor::{
            ExclusiveExecutor, ExclusivePoisoned, ExclusiveState, PoisonedInput,
        };
        use bombay_transition::{Machine, Structure};

        #[derive(Debug)]
        struct NonClone(u32);

        struct PanicMachine;

        impl Machine for PanicMachine {
            type Input = NonClone;
            type Output = ();

            fn step(self, _input: Self::Input) -> ((), Self) {
                panic!("injected transition panic");
            }

            fn describe<V: Structure>(&self, _visitor: &mut V) -> V::Output {
                unreachable!("describe is not exercised by turn")
            }
        }

        let mut executor = ExclusiveExecutor::new(PanicMachine);
        assert_eq!(executor.state(), ExclusiveState::Ready);

        let panicked = catch_unwind(AssertUnwindSafe(|| executor.turn(NonClone(1))));
        assert!(panicked.is_err(), "transition panic must unwind");
        assert_eq!(
            executor.state(),
            ExclusiveState::Poisoned,
            "permanent poison"
        );

        // Later input is rejected intact: the exact non-Clone value is returned.
        let recovered = executor.turn(NonClone(42));
        match recovered {
            Err(PoisonedInput(NonClone(42))) => {}
            other => panic!("expected PoisonedInput(NonClone(42)), got {other:?}"),
        }

        assert!(matches!(executor.into_inner(), Err(ExclusivePoisoned)));
    }

    /// Dropping the driver before the loop is cancel-safe.
    #[tokio::test]
    async fn driver_is_cancel_safe() {
        let env = TestEnv {
            events: vec![Some(1), Some(2), Some(3)].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();
        drop(driver);
    }

    /// Cancellation during `interpret`: dropping the `run_loop` future while
    /// the environment is mid-effect releases the borrow and leaves the
    /// driver retirable.
    #[tokio::test]
    async fn cancellation_during_interpret_is_safe() {
        struct SlowEnv {
            events: VecDeque<Option<u64>>,
            entered_interpret: Arc<AtomicBool>,
        }

        impl Environment for SlowEnv {
            type Event = User<MailAddr, u64>;
            type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
            type Error = std::convert::Infallible;

            async fn next(&mut self) -> Option<Self::Event> {
                self.events.pop_front()?.map(|v| User::new(MailAddr(0), v))
            }

            async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
                // Init has empty sends; only block on the event effect.
                if !effect.sends.is_empty() {
                    self.entered_interpret.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_mins(1)).await;
                }
                Ok(())
            }

            async fn retire(&mut self) {}
        }

        let entered = Arc::new(AtomicBool::new(false));
        let env = SlowEnv {
            events: vec![Some(1)].into(),
            entered_interpret: entered.clone(),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();

        let aborted = {
            let run = driver.run_loop();
            tokio::pin!(run);
            tokio::select! {
                _ = &mut run => false,
                () = async {
                    while !entered.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    tokio::task::yield_now().await;
                } => true,
            }
        };
        assert!(aborted, "run_loop must still be awaiting interpret");

        driver.retire().await;
    }

    /// Typestate model exploration: `run_init` on a retired driver panics.
    #[tokio::test]
    #[should_panic(expected = "non-uninitialized")]
    async fn run_init_panics_after_retire() {
        let env = TestEnv {
            events: VecDeque::new(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();
        driver.retire().await;
        let _ = driver.run_init().await;
    }

    /// Typestate model exploration: `run_loop` on a retired driver panics.
    #[tokio::test]
    #[should_panic(expected = "non-running driver")]
    async fn run_loop_panics_after_retire() {
        let env = TestEnv {
            events: vec![Some(1)].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();
        driver.retire().await;
        let _ = driver.run_loop().await;
    }

    /// Typestate model exploration: `retire` is idempotent.
    #[tokio::test]
    async fn retire_is_idempotent() {
        let env = TestEnv {
            events: VecDeque::new(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);
        driver.run_init().await.unwrap();
        driver.retire().await;
        driver.retire().await;
    }

    /// Environment failure during init: `interpret` fails on the init effect,
    /// and `run_init` must return `RunError::Environment` — not silently
    /// proceed to the loop, not panic.
    #[tokio::test]
    async fn environment_failure_during_init_is_exact() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct InitEffectEnv {
            interpret_calls: Arc<AtomicUsize>,
        }

        impl Environment for InitEffectEnv {
            type Event = User<MailAddr, u64>;
            type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
            type Error = &'static str;

            async fn next(&mut self) -> Option<Self::Event> {
                None
            }

            async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
                let n = self.interpret_calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 && !effect.sends.is_empty() {
                    // First interpret call, with the init effect's sends.
                    Err("init effect rejected")
                } else {
                    Ok(())
                }
            }

            async fn retire(&mut self) {}
        }

        struct SendsOnInit;

        impl Behavior for SendsOnInit {
            type Addr = MailAddr;
            type Msg = u64;
            type Event = User<MailAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = std::convert::Infallible;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Ok(Actions {
                    sends: vec![7],
                    creates: Vec::new(),
                    become_: Step::Continue,
                })
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                Ok(Actions::cont())
            }
        }

        let env = InitEffectEnv {
            interpret_calls: Arc::new(AtomicUsize::new(0)),
        };
        let mut driver = Driver::new(SendsOnInit, env);

        let result = driver.run_init().await;
        assert!(
            matches!(result, Err(RunError::Environment("init effect rejected"))),
            "expected RunError::Environment on init effect, got {result:?}"
        );
    }

    /// Cancellation during init: dropping the `run_init` future while the
    /// init effect is mid-interpret releases the borrow and leaves the
    /// driver retirable.
    #[tokio::test]
    async fn cancellation_during_init_is_safe() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        struct BlockingInitEnv {
            entered_interpret: Arc<AtomicBool>,
        }

        impl Environment for BlockingInitEnv {
            type Event = User<MailAddr, u64>;
            type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
            type Error = std::convert::Infallible;

            async fn next(&mut self) -> Option<Self::Event> {
                None
            }

            async fn interpret(&mut self, effect: Self::Effect) -> Result<(), Self::Error> {
                if !effect.sends.is_empty() {
                    self.entered_interpret.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_mins(1)).await;
                }
                Ok(())
            }

            async fn retire(&mut self) {}
        }

        struct SendsOnInit2;

        impl Behavior for SendsOnInit2 {
            type Addr = MailAddr;
            type Msg = u64;
            type Event = User<MailAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = std::convert::Infallible;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Ok(Actions {
                    sends: vec![7],
                    creates: Vec::new(),
                    become_: Step::Continue,
                })
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                Ok(Actions::cont())
            }
        }

        let entered = Arc::new(AtomicBool::new(false));
        let env = BlockingInitEnv {
            entered_interpret: entered.clone(),
        };
        let mut driver = Driver::new(SendsOnInit2, env);

        let aborted = {
            let init = driver.run_init();
            tokio::pin!(init);
            tokio::select! {
                _ = &mut init => false,
                () = async {
                    while !entered.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    tokio::task::yield_now().await;
                } => true,
            }
        };
        assert!(aborted, "run_init must still be awaiting the init effect");

        driver.retire().await;
    }
    /// Retirement happens exactly once in the `run()` path: the environment's
    /// `retire` is invoked a single time, never zero and never twice.
    #[tokio::test]
    async fn run_retires_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingEnv {
            events: VecDeque<Option<u64>>,
            retires: Arc<AtomicUsize>,
        }

        impl Environment for CountingEnv {
            type Event = User<MailAddr, u64>;
            type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
            type Error = std::convert::Infallible;

            async fn next(&mut self) -> Option<Self::Event> {
                self.events.pop_front()?.map(|v| User::new(MailAddr(0), v))
            }

            async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Self::Error> {
                Ok(())
            }

            async fn retire(&mut self) {
                self.retires.fetch_add(1, Ordering::SeqCst);
            }
        }

        let retires = Arc::new(AtomicUsize::new(0));
        let env = CountingEnv {
            events: vec![Some(1), None].into(),
            retires: retires.clone(),
        };
        let mut driver = Driver::new(Counter { value: 0 }, env);

        let result = driver.run().await;
        assert!(matches!(result, Ok(RunExit::EnvironmentClosed)));
        assert_eq!(
            retires.load(Ordering::SeqCst),
            1,
            "retire must run exactly once"
        );
    }
    /// Rollback on init failure: a failing `init()` drops the behavior
    /// exactly once and leaves the driver in `Retired` (not `Uninitialized`,
    /// not `Running`).
    #[tokio::test]
    async fn init_failure_drops_behavior_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct DropCounting {
            drops: Arc<AtomicUsize>,
        }

        impl Drop for DropCounting {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl Behavior for DropCounting {
            type Addr = MailAddr;
            type Msg = u64;
            type Event = User<MailAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = &'static str;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Err("init failed")
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                unreachable!("transition must not be called after init failure")
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let env = TestEnv {
            events: VecDeque::new(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(
            DropCounting {
                drops: drops.clone(),
            },
            env,
        );

        let result = driver.run_init().await;
        assert!(matches!(result, Err(RunError::Behavior("init failed"))));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "behavior must drop exactly once"
        );
    }

    /// A failed `run_init` leaves the driver in `Retired`, so a second
    /// `run_init` panics (state is not `Uninitialized`).
    #[tokio::test]
    #[should_panic(expected = "non-uninitialized")]
    async fn failed_init_leaves_driver_retired() {
        struct FailsInitBehavior;

        impl Behavior for FailsInitBehavior {
            type Addr = MailAddr;
            type Msg = u64;
            type Event = User<MailAddr, u64>;
            type Sends = Vec<u64>;
            type Ph = Never;
            type Error = &'static str;
            type Birth = NoBirths;

            fn init(&mut self) -> BehaviorActed<Self> {
                Err("init failed")
            }

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                unreachable!()
            }
        }

        let env = TestEnv {
            events: VecDeque::new(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(FailsInitBehavior, env);

        let _ = driver.run_init().await;
        // Second run_init must panic: state is Retired, not Uninitialized.
        let _ = driver.run_init().await;
    }
    /// Inversion-sensitive production-path check: every event reaches
    /// `Behavior::transition` exactly once through `ExclusiveExecutor::turn`.
    /// A driver that processed an event twice (or skipped it) would fail the
    /// exact transition count.
    #[tokio::test]
    async fn turn_is_the_only_transition_path() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingBehavior {
            transitions: Arc<AtomicUsize>,
        }

        impl Behavior for CountingBehavior {
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
                self.transitions.fetch_add(1, Ordering::SeqCst);
                Ok(Actions {
                    sends: vec![event.message],
                    creates: Vec::new(),
                    become_: Step::Continue,
                })
            }
        }

        let transitions = Arc::new(AtomicUsize::new(0));
        let env = TestEnv {
            events: vec![Some(1), Some(2), Some(3), None].into(),
            effects: Arc::new(Mutex::new(Vec::new())),
            retired: Arc::new(Mutex::new(false)),
        };
        let mut driver = Driver::new(
            CountingBehavior {
                transitions: transitions.clone(),
            },
            env,
        );

        let outcome = driver.run().await;
        assert!(matches!(outcome, Ok(RunExit::EnvironmentClosed)));
        assert_eq!(
            transitions.load(Ordering::SeqCst),
            3,
            "each of 3 events must reach transition exactly once"
        );
    }
    /// Re-entry after a caught transition panic: the executor is poisoned and
    /// the next `run_loop` must retire the driver and return a typed
    /// `RunError::Poisoned`, not `unreachable!()` and not a second panic.
    #[test]
    fn poison_reentry_returns_typed_error() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        struct CountingEnv {
            events: VecDeque<u64>,
            next_calls: Arc<AtomicUsize>,
            retired: Arc<AtomicBool>,
        }

        impl Environment for CountingEnv {
            type Event = User<MailAddr, u64>;
            type Effect = RuntimeEffects<MailAddr, Vec<u64>, NoBirths>;
            type Error = std::convert::Infallible;

            async fn next(&mut self) -> Option<Self::Event> {
                self.next_calls.fetch_add(1, Ordering::SeqCst);
                self.events
                    .pop_front()
                    .map(|value| User::new(MailAddr(0), value))
            }

            async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Self::Error> {
                Ok(())
            }

            async fn retire(&mut self) {
                self.retired.store(true, Ordering::SeqCst);
            }
        }

        struct PanicOnTransition;

        impl Behavior for PanicOnTransition {
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

            fn transition(&mut self, _event: Self::Event) -> BehaviorActed<Self> {
                panic!("injected transition panic");
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let next_calls = Arc::new(AtomicUsize::new(0));
        let retired = Arc::new(AtomicBool::new(false));
        let env = CountingEnv {
            events: vec![1, 2].into(),
            next_calls: next_calls.clone(),
            retired: retired.clone(),
        };
        let mut driver = Driver::new(PanicOnTransition, env);
        rt.block_on(driver.run_init()).unwrap();

        let caught = catch_unwind(AssertUnwindSafe(|| {
            let _ = rt.block_on(driver.run_loop());
        }));
        assert!(caught.is_err(), "transition panic must unwind");
        assert_eq!(next_calls.load(Ordering::SeqCst), 1);

        // Re-entry detects poison before polling the second queued event.
        let result = rt.block_on(driver.run_loop());
        assert!(
            matches!(result, Err(RunError::Poisoned)),
            "expected RunError::Poisoned on re-entry, got {result:?}"
        );
        assert_eq!(
            next_calls.load(Ordering::SeqCst),
            1,
            "poisoned re-entry must not consume another environment event"
        );
        assert!(retired.load(Ordering::SeqCst));
    }
}
