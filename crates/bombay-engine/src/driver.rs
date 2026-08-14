//! Production Driver using Machine Executor's `ExclusiveExecutor`.
//!
//! The [`Driver`] stores an [`ExclusiveExecutor`]`<BehaviorMachine<B>>` and
//! delegates every production event through [`ExclusiveExecutor::turn`], which
//! calls [`Machine::step`] (Transition) → [`Behavior::transition`] (Behavior)
//! and returns the [`BehaviorActed`] output directly. Effect interpretation is
//! async and happens after the turn, outside the poison boundary.
//!
//! # Execution flow
//!
//! ```text
//! init: Behavior::init → BehaviorMachine → ExclusiveExecutor::new
//! loop: env.next() → executor.turn(event) → BehaviorActed
//!       → await Environment::interpret → loop
//! ```
//!
//! # Poison boundary
//!
//! [`ExclusiveExecutor::turn`] installs a poisoned seat before calling
//! [`Machine::step`]. A transition panic unwinds through the turn, leaves the
//! seat poisoned, and the driver propagates the unwind (bombay classifies it
//! as `TaskOutcome::Panicked`). If an outer caller catches that unwind and
//! reuses the public Driver, `run_loop` detects poison before polling the
//! environment, retires it, and returns [`RunError::Poisoned`].
//!
//! # Inversion guarantee (type-level, not runtime)
//!
//! `Machine::step` consumes `self` (affine). The machine is owned by
//! [`ExclusiveExecutor`], which exposes no way to step it other than
//! [`ExclusiveExecutor::turn`]. The type system therefore makes `turn` the
//! only transition path; a runtime test cannot detect a bypass the compiler
//! already rejects.

use behavior::{Actions, Address, Behavior, BirthMode, Compose, Exit, Never, SendAlgebra, Step};
use bombay_machine_executor::{ExclusiveExecutor, ExclusiveState};

use crate::{BehaviorMachine, Environment, RunError, RunExit};

/// One behavior transition with `become` removed.
pub struct RuntimeEffects<A: Address, Sends, Birth: BirthMode> {
    pub sends: Sends,
    pub creates: Vec<behavior::Create<A, Birth::Child>>,
}

async fn interpret<A, Sends, Birth, E>(
    actions: Actions<A, Never, Sends, Birth>,
    environment: &mut E,
) -> Result<Option<Exit<A>>, E::Error>
where
    A: Address,
    Sends: SendAlgebra,
    Birth: BirthMode,
    E: Environment<Effect = RuntimeEffects<A, Sends, Birth>>,
{
    let Actions {
        sends,
        creates,
        become_,
    } = actions;
    environment
        .interpret(RuntimeEffects { sends, creates })
        .await?;
    Ok(match become_ {
        Step::Continue => None,
        Step::Goto(never) => match never {},
        Step::Stop(exit) => Some(exit),
    })
}

/// Typestate for the driver lifecycle — no `Option` plus `expect`.
enum State<B: Behavior> {
    Definition(Compose<B>),
    Running(ExclusiveExecutor<BehaviorMachine<B>>),
    Terminated,
    Retired,
}

/// Application core driving one behavior through one runtime port.
///
/// Uses [`ExclusiveExecutor`] for allocation-free exclusive turns. Every event
/// goes through `turn`; effect interpretation is async, outside the executor.
///
/// # Compile-time bound
///
/// The `B: Behavior` bound rejects a payload type that does not implement
/// [`Behavior`]. This is a narrow static bound, not a proof that every
/// invalid composition is unrepresentable:
///
/// ```compile_fail
/// use bombay_engine::Driver;
/// // `u32` does not implement `Behavior`; this must not compile.
/// let _: Driver<u32, ()> = Driver::new(42u32, ());
/// ```
pub struct Driver<B: Behavior, E> {
    state: State<B>,
    environment: E,
}

impl<B, E> Driver<B, E>
where
    B: Behavior,
{
    pub fn new(behavior: B, environment: E) -> Self {
        Self::from_definition(Compose::new(behavior), environment)
    }

    /// Construct a driver from a fully composed, uninitialized definition.
    pub fn from_definition(definition: Compose<B>, environment: E) -> Self {
        Self {
            state: State::Definition(definition),
            environment,
        }
    }
}

impl<B, E> Driver<B, E>
where
    B: Behavior<Ph = Never> + Send,
    B::Event: Send,
    E: Environment<Event = B::Event, Effect = RuntimeEffects<B::Addr, B::Sends, B::Birth>>,
{
    /// Run initialization, construct the executor, interpret init effects.
    ///
    /// Transitions `Uninitialized → Running` for a continuing behavior or
    /// `Uninitialized → Terminated` for terminal initialization.
    /// Returns the terminal exit in the latter case.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::Behavior`] when [`Behavior::init`] fails.
    /// Returns [`RunError::Environment`] when the environment rejects
    /// an initialization effect.
    ///
    /// # Panics
    ///
    /// Panics if called on a driver that is not in the `Uninitialized` state.
    pub async fn run_init(
        &mut self,
    ) -> Result<Option<Exit<B::Addr>>, RunError<B::Error, E::Error>> {
        let State::Definition(definition) = std::mem::replace(&mut self.state, State::Retired)
        else {
            panic!("run_init called on non-uninitialized driver");
        };

        let initialized = definition.initialize().map_err(RunError::Behavior)?;
        let machine = BehaviorMachine::for_runtime(initialized.behavior);

        let exit = interpret(initialized.actions, &mut self.environment)
            .await
            .map_err(RunError::Environment)?;

        self.state = if exit.is_some() {
            State::Terminated
        } else {
            State::Running(ExclusiveExecutor::new(machine))
        };
        Ok(exit)
    }

    /// Run the event loop. Every event goes through
    /// [`ExclusiveExecutor::turn`].
    ///
    /// # Errors
    ///
    /// Returns [`RunError::Behavior`] when a [`Behavior::transition`] fails.
    /// Returns [`RunError::Environment`] when the environment rejects an
    /// effect.
    ///
    /// # Panics
    ///
    /// Panics if the driver is not in the `Running` state. A transition panic
    /// unwinds through this method (leaving the executor poisoned).
    pub async fn run_loop(
        &mut self,
    ) -> Result<RunExit<Exit<B::Addr>>, RunError<B::Error, E::Error>> {
        loop {
            // A caller can retain and reuse the Driver after catching a panic
            // from a previously-polled run_loop future. Detect that terminal
            // executor state before pulling another event from the environment;
            // otherwise the intact PoisonedInput returned by turn would merely
            // be consumed and discarded by this adapter.
            let poisoned = match &self.state {
                State::Running(executor) => executor.state() == ExclusiveState::Poisoned,
                _ => panic!("run_loop on non-running driver"),
            };
            if poisoned {
                self.environment.retire().await;
                self.state = State::Retired;
                return Err(RunError::Poisoned);
            }

            let Some(event) = self.environment.next().await else {
                self.state = State::Terminated;
                return Ok(RunExit::EnvironmentClosed);
            };

            let output = match &mut self.state {
                State::Running(executor) => executor.turn(event),
                _ => panic!("run_loop on non-running driver"),
            };

            // The pre-check above handles retained poison. This arm remains a
            // defensive boundary in case the executor contract grows another
            // safe way for turn to reject an input.
            let actions = match output {
                Ok(Ok(actions)) => actions,
                Ok(Err(error)) => {
                    self.state = State::Terminated;
                    return Err(RunError::Behavior(error));
                }
                Err(_poisoned) => {
                    self.environment.retire().await;
                    self.state = State::Retired;
                    return Err(RunError::Poisoned);
                }
            };

            match interpret(actions, &mut self.environment).await {
                Ok(Some(exit)) => {
                    self.state = State::Terminated;
                    return Ok(RunExit::Stopped(exit));
                }
                Ok(None) => {}
                Err(error) => {
                    self.state = State::Terminated;
                    return Err(RunError::Environment(error));
                }
            }
        }
    }

    /// Retire the environment.
    pub async fn retire(&mut self) {
        self.environment.retire().await;
        self.state = State::Retired;
    }

    /// Run init, then event loop, then retire.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::Behavior`] when the behavior fails during
    /// initialization or a transition. Returns [`RunError::Environment`]
    /// when the environment rejects an effect.
    pub async fn run(&mut self) -> Result<RunExit<Exit<B::Addr>>, RunError<B::Error, E::Error>> {
        let result = async {
            if let Some(exit) = self.run_init().await? {
                return Ok(RunExit::Stopped(exit));
            }
            self.run_loop().await
        }
        .await;
        self.environment.retire().await;
        self.state = State::Retired;
        result
    }
}
