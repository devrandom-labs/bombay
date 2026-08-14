//! Generation-local execution and terminal retirement primitives.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use behavior::{Address, Behavior, Crash, Exit, Never};
use bombay_engine::{Driver, Environment, RunError, RunExit, RuntimeEffects};
use observe::{Observation, Subject};

use crate::routing::PeerOutcome;
use crate::{ActorRef, EndpointRegistry, Handle, IncarnationEndpoint, TaskOutcome};

use super::lifecycle::{IncarnationReporter, LifecycleFactory};
use super::{Completion, IntoPeerOutcome};
use crate::LifecycleTransition;

type ExecutionResult<P, E, A> =
    Result<RunExit<Exit<A>>, RunError<<P as Behavior>::Error, <E as Environment>::Error>>;

/// Actor-owned resources whose drop is the first terminal action.
pub(crate) struct Incarnation<
    P: behavior::Behavior<Ph = Never>,
    E,
    L,
    T,
    A: Address,
    R: IncarnationReporter,
> {
    driver: Driver<P, E>,
    retirement: TerminalRetirement<L, T, A, R>,
    cancellation_requested: Arc<AtomicBool>,
}

/// Drop guard that releases one exact generation and publishes its outcome.
pub(crate) struct TerminalRetirement<L, T, A: Address, R: IncarnationReporter> {
    lease: Option<L>,
    subject: Option<Subject<(), TaskOutcome<T>>>,
    peer_subject: Option<Subject<(), PeerOutcome<A>>>,
    lifecycle: R,
}

impl<L, T, A: Address, R: IncarnationReporter> TerminalRetirement<L, T, A, R> {
    pub(crate) const fn new(
        lease: L,
        subject: Subject<(), TaskOutcome<T>>,
        peer_subject: Subject<(), PeerOutcome<A>>,
        lifecycle: R,
    ) -> Self {
        Self {
            lease: Some(lease),
            subject: Some(subject),
            peer_subject: Some(peer_subject),
            lifecycle,
        }
    }

    fn returned(mut self, value: T, peer: PeerOutcome<A>) {
        self.retire(TaskOutcome::Returned(value), peer);
    }

    fn retire(&mut self, outcome: TaskOutcome<T>, peer: PeerOutcome<A>) {
        drop(self.lease.take());
        self.lifecycle.emit(LifecycleTransition::Retired);
        let mut subject = self
            .subject
            .take()
            .expect("terminal outcome must be published exactly once");
        let mut peer_subject = self
            .peer_subject
            .take()
            .expect("peer outcome must be published exactly once");
        let detailed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            subject.complete(outcome);
        }));
        let normalized = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            peer_subject.complete(peer);
        }));
        self.lifecycle.emit(LifecycleTransition::Completed);
        if let Err(panic) = detailed {
            std::panic::resume_unwind(panic);
        }
        if let Err(panic) = normalized {
            std::panic::resume_unwind(panic);
        }
    }
}

impl<L, T, A: Address, R: IncarnationReporter> Drop for TerminalRetirement<L, T, A, R> {
    fn drop(&mut self) {
        if self.subject.is_none() {
            return;
        }
        let (outcome, peer) = if std::thread::panicking() {
            (TaskOutcome::Panicked, Err(Crash::Panicked))
        } else {
            (TaskOutcome::Cancelled, Err(Crash::Cancelled))
        };
        self.retire(outcome, peer);
    }
}

impl<P, E, L, R>
    Incarnation<P, E, L, Result<RunExit<Exit<P::Addr>>, RunError<P::Error, E::Error>>, P::Addr, R>
where
    P: Behavior<Ph = Never> + Send,
    P::Event: Send,
    E: Environment<Event = P::Event, Effect = RuntimeEffects<P::Addr, P::Sends, P::Birth>>,
    R: IncarnationReporter,
{
    /// Run and explicitly retire the actor environment before returning.
    ///
    /// Panic unwinding and future cancellation also drop `self`, preserving
    /// the same cleanup-before-terminal-observation edge.
    pub(crate) async fn run(mut self) {
        let result = match self.driver.run_init().await {
            Ok(Some(exit)) => {
                self.driver.retire().await;
                Ok(RunExit::Stopped(exit))
            }
            Ok(None) => {
                // Root initialization may contain synchronous user work. Give
                // an already-requested Tokio abort one cancellation boundary
                // before any ready mailbox input can enter the first turn.
                if self.cancellation_requested.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
                let result = self.driver.run_loop().await;
                self.driver.retire().await;
                result
            }
            Err(error) => {
                self.driver.retire().await;
                Err(error)
            }
        };
        drop(self.driver);
        let peer = result.peer_outcome();
        self.retirement.returned(result, peer);
    }

    /// Run the event loop of an incarnation whose init phase already ran.
    ///
    /// A behavior that stopped during initialization retires immediately with
    /// its terminal exit; every path retires the environment exactly once.
    pub(crate) async fn run_initialized(mut self, pending_exit: Option<Exit<P::Addr>>) {
        let result = if let Some(exit) = pending_exit {
            self.driver.retire().await;
            Ok(RunExit::Stopped(exit))
        } else {
            let result = self.driver.run_loop().await;
            self.driver.retire().await;
            result
        };
        drop(self.driver);
        let peer = result.peer_outcome();
        self.retirement.returned(result, peer);
    }
}

/// Prepared actor resources before any address registration.
///
/// The exact address generation is not yet claimed: dropping this value
/// leaves the provisional address unbound, and no delivery can resolve to it.
/// Only [`ProvisionalIncarnation::commit`] publishes the endpoint.
pub(crate) struct ProvisionalIncarnation<
    P: behavior::Behavior<Ph = Never>,
    E,
    T,
    A: Address,
    SE,
    SL,
> {
    driver: Driver<P, E>,
    address: A,
    endpoint: IncarnationEndpoint<A, ActorRef<A, SE>>,
    sender: SL,
    subject: Subject<(), TaskOutcome<T>>,
    observation: Observation<TaskOutcome<T>>,
    peer_subject: Subject<(), PeerOutcome<A>>,
    cancellation_requested: Arc<AtomicBool>,
}

impl<P: behavior::Behavior<Ph = Never, Addr = A>, E, T, A: Address, SE, SL>
    ProvisionalIncarnation<P, E, T, A, SE, SL>
{
    #[allow(
        clippy::too_many_arguments,
        reason = "the private aggregate receives each independently owned provisional seat"
    )]
    pub(crate) fn new(
        driver: Driver<P, E>,
        address: A,
        endpoint: IncarnationEndpoint<A, ActorRef<A, SE>>,
        sender: SL,
        subject: Subject<(), TaskOutcome<T>>,
        observation: Observation<TaskOutcome<T>>,
        peer_subject: Subject<(), PeerOutcome<A>>,
        cancellation_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            driver,
            address,
            endpoint,
            sender,
            subject,
            observation,
            peer_subject,
            cancellation_requested,
        }
    }

    /// Drive the initialization phase before any registration decision.
    pub(crate) fn driver(&mut self) -> &mut Driver<P, E> {
        &mut self.driver
    }

    /// Claim the address generation and publish the delivery endpoint.
    #[allow(
        clippy::type_complexity,
        reason = "the committed typestate carries every affine launch seat"
    )]
    pub(crate) fn commit<R, Factory>(
        self,
        router: &R,
        lifecycle: &Factory,
    ) -> Result<
        PreparedIncarnation<
            P,
            E,
            R::Registration,
            ActorRef<A, SL, Factory::Reporter>,
            T,
            A,
            Factory::Reporter,
        >,
        R::Error,
    >
    where
        R: EndpointRegistry<P, IncarnationEndpoint<A, ActorRef<A, SE>>>,
        Factory: LifecycleFactory<A, R::Registration>,
    {
        let registration = router.register(self.address, self.endpoint)?;
        let reporter = lifecycle.reporter(self.address, &registration);
        reporter.emit(LifecycleTransition::Prepared);
        let actor_ref = ActorRef::with_lifecycle(self.address, self.sender, reporter.clone());
        Ok(PreparedIncarnation::new(
            self.driver,
            registration,
            actor_ref,
            self.subject,
            self.observation,
            self.peer_subject,
            reporter,
            self.cancellation_requested,
        ))
    }
}

/// A fully prepared but not-yet-launched incarnation.
///
/// This is a single private typestate boundary, not an incremental builder:
/// incomplete preparation lives only in local variables, and this value can
/// exist only after the exact address generation has been claimed. Dropping it
/// before launch releases every prospective incarnation resource without
/// starting user code.
pub(crate) struct PreparedIncarnation<
    P: behavior::Behavior<Ph = Never>,
    E,
    L,
    R,
    T,
    A: Address,
    Reporter: IncarnationReporter,
> {
    driver: Driver<P, E>,
    lease: L,
    actor_ref: R,
    subject: Subject<(), TaskOutcome<T>>,
    observation: Observation<TaskOutcome<T>>,
    peer_subject: Subject<(), PeerOutcome<A>>,
    lifecycle: Reporter,
    cancellation_requested: Arc<AtomicBool>,
}

/// Whether the canonical launch operation must run initialization itself or
/// continue from an already completed transactional initialization phase.
pub(crate) enum LaunchMode<A: Address> {
    Uninitialized,
    Initialized(Option<Exit<A>>),
}

impl<P: behavior::Behavior<Ph = Never>, E, L, R, T, A: Address, Reporter: IncarnationReporter>
    PreparedIncarnation<P, E, L, R, T, A, Reporter>
{
    #[allow(
        clippy::too_many_arguments,
        reason = "the private aggregate receives each independently owned committed seat"
    )]
    pub(crate) fn new(
        driver: Driver<P, E>,
        lease: L,
        actor_ref: R,
        subject: Subject<(), TaskOutcome<T>>,
        observation: Observation<TaskOutcome<T>>,
        peer_subject: Subject<(), PeerOutcome<A>>,
        lifecycle: Reporter,
        cancellation_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            driver,
            lease,
            actor_ref,
            subject,
            observation,
            peer_subject,
            lifecycle,
            cancellation_requested,
        }
    }

    pub(crate) const fn actor_ref(&self) -> &R {
        &self.actor_ref
    }

    /// Consume the prepared state into its ordered launch ownership seats.
    #[allow(
        clippy::type_complexity,
        reason = "the tuple exposes three affine launch seats without another state object"
    )]
    pub(crate) fn into_launch_parts(
        self,
    ) -> (
        Incarnation<P, E, L, T, A, Reporter>,
        R,
        Observation<TaskOutcome<T>>,
        Arc<AtomicBool>,
    ) {
        (
            Incarnation {
                driver: self.driver,
                retirement: TerminalRetirement::new(
                    self.lease,
                    self.subject,
                    self.peer_subject,
                    self.lifecycle,
                ),
                cancellation_requested: self.cancellation_requested.clone(),
            },
            self.actor_ref,
            self.observation,
            self.cancellation_requested,
        )
    }
}

impl<P, E, L, R, A, Reporter> PreparedIncarnation<P, E, L, R, ExecutionResult<P, E, A>, A, Reporter>
where
    P: Behavior<Addr = A, Ph = Never> + Send + 'static,
    P::Event: Send + 'static,
    P::Sends: Send + 'static,
    P::Error: Send + Sync + 'static,
    <P::Birth as behavior::BirthMode>::Child: Send + 'static,
    E: Environment<Event = P::Event, Effect = RuntimeEffects<A, P::Sends, P::Birth>>
        + Send
        + 'static,
    E::Error: Send + Sync + 'static,
    L: Send + 'static,
    A: Address + Send + Sync + 'static,
    A::Nonce: Send + 'static,
    Reporter: IncarnationReporter + Clone + Send + 'static,
{
    /// Launch through the sole task/control/completion construction ceremony.
    pub(crate) fn launch(
        self,
        mode: LaunchMode<A>,
        restarted: bool,
    ) -> Handle<R, ExecutionResult<P, E, A>> {
        let lifecycle = self.lifecycle.clone();
        let (incarnation, actor_ref, observation, cancellation_requested) =
            self.into_launch_parts();
        let task = tokio::spawn(async move {
            lifecycle.emit(LifecycleTransition::Started);
            if restarted {
                lifecycle.emit(LifecycleTransition::Restarted);
            }
            match mode {
                LaunchMode::Uninitialized => incarnation.run().await,
                LaunchMode::Initialized(pending_exit) => {
                    incarnation.run_initialized(pending_exit).await;
                }
            }
        });
        let control = task.abort_handle();
        let completion = Completion::new(task, observation);
        Handle::new(actor_ref, control, completion, cancellation_requested)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    use behavior::{Actions, Behavior, Exit, MailAddr, Never, NoBirths, User};
    use bombay_engine::{Driver, Environment, RuntimeEffects};

    use super::{Incarnation, TerminalRetirement};

    struct Stop;

    impl Behavior for Stop {
        type Addr = MailAddr;
        type Msg = Never;
        type Event = User<MailAddr, Never>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self) -> behavior::BehaviorActed<Self> {
            Ok(Actions::stop(Exit::Normal))
        }

        fn transition(&mut self, _event: Self::Event) -> behavior::BehaviorActed<Self> {
            unreachable!()
        }
    }

    struct Probe(&'static str, Arc<Mutex<Vec<&'static str>>>);

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected completion wake panic");
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.1.lock().expect("drop order lock").push(self.0);
        }
    }

    struct Source {
        _probe: Probe,
    }

    impl Environment for Source {
        type Event = User<MailAddr, Never>;
        type Effect = RuntimeEffects<MailAddr, Vec<Never>, NoBirths>;
        type Error = Infallible;

        async fn next(&mut self) -> Option<Self::Event> {
            None
        }

        async fn interpret(&mut self, _effect: Self::Effect) -> Result<(), Infallible> {
            Ok(())
        }

        async fn retire(&mut self) {}
    }

    #[tokio::test]
    async fn successful_execution_retires_actor_before_terminal_lease() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let behavior = Stop;
        let environment = Source {
            _probe: Probe("environment", order.clone()),
        };
        let space = observe::ObservationSpace::new();
        let subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        let peer_space = observe::ObservationSpace::new();
        let peer_subject = peer_space.subject(()).expect("fresh peer subject");
        let peer_observation = peer_space.observe(&()).expect("registered peer subject");
        let incarnation = Incarnation {
            driver: Driver::new(behavior, environment),
            retirement: TerminalRetirement::new(
                Probe("lease", order.clone()),
                subject,
                peer_subject,
                crate::NoLifecycle,
            ),
            cancellation_requested: Arc::new(AtomicBool::new(false)),
        };
        incarnation.run().await;
        assert_eq!(
            *order.lock().expect("drop order lock"),
            ["environment", "lease"]
        );
        assert_eq!(
            observation.try_get(),
            Some(crate::TaskOutcome::Returned(Ok(crate::RunExit::Stopped(
                Exit::Normal
            ))))
        );
        assert_eq!(peer_observation.try_get(), Some(Ok(Exit::Normal)));
    }

    #[tokio::test]
    async fn terminal_retirement_releases_lease_before_publishing() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let space = observe::ObservationSpace::new();
        let subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        let peer_space = observe::ObservationSpace::new();
        let peer_subject = peer_space.subject(()).expect("fresh peer subject");
        let peer_observation = peer_space.observe(&()).expect("registered peer subject");
        TerminalRetirement::new(
            Probe("lease", order.clone()),
            subject,
            peer_subject,
            crate::NoLifecycle,
        )
        .returned(7, Ok(Exit::<MailAddr>::Normal));

        assert_eq!(*order.lock().expect("drop order lock"), ["lease"]);
        assert_eq!(observation.try_get(), Some(crate::TaskOutcome::Returned(7)));
        assert_eq!(peer_observation.try_get(), Some(Ok(Exit::Normal)));
    }

    #[test]
    fn panicking_observer_cannot_strand_the_other_terminal_domain() {
        let space = observe::ObservationSpace::new();
        let subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        let waker = std::task::Waker::from(Arc::new(PanicWake));
        assert!(!observation.register_waker(&waker));

        let peer_space = observe::ObservationSpace::new();
        let peer_subject = peer_space.subject(()).expect("fresh peer subject");
        let peer_observation = peer_space.observe(&()).expect("registered peer subject");
        let retirement = TerminalRetirement::new((), subject, peer_subject, crate::NoLifecycle);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            retirement.returned(7, Ok(Exit::<MailAddr>::Normal));
        }));
        assert!(result.is_err());
        assert_eq!(observation.try_get(), Some(crate::TaskOutcome::Returned(7)));
        assert_eq!(peer_observation.try_get(), Some(Ok(Exit::Normal)));
    }
}
