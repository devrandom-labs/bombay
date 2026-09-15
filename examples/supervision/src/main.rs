//! For learning fixed supervision with an explicit restart policy and delayed
//! backoff. The worker stops on a timer, the supervisor performs one restart,
//! then restart-budget exhaustion ends the application with the exact typed
//! supervision failure.

use core::time::Duration;

use bombay::behavior::{
    ActiveTurn, Backoff, Behavior, BehaviorActed, BehaviorBase, ChildHead, ChildRoute,
    ChildTopology, EventIngress, Here, NoBirths, NoSends, OneShot, Proxy, ProxyUnavailable,
    RestartConfiguration, RestartDenial, RestartPolicy, RestartTiming, StopOnShutdown, Strategy,
    Supervise, SuperviseError, SupervisionFailure, SupervisionFailureReason, SupervisionLifecycle,
    User, UserEvent, stop_on_supervision_failure,
};
use bombay::prelude::*;

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

type TimedWorker = OneShot<Worker>;
type ManagedWorker = StopOnShutdown<TimedWorker>;
type StableWorker = Proxy<ManagedWorker>;
type StableLayer = fn(ManagedWorker) -> StableWorker;
type SupervisedApplication = Supervise<ApplicationState, ManagedWorker, StableLayer>;
type SupervisorRunError = RunError<SuperviseError<Never, MailAddr>>;

enum ApplicationEvent {
    Lifecycle(SupervisionLifecycle<MailAddr>),
    WorkerUnavailable(ProxyUnavailable<MailAddr, Never>),
}

impl UserEvent for ApplicationEvent {
    type Addr = MailAddr;
    type Message = Never;

    fn user(_: MailAddr, message: Self::Message) -> Self {
        match message {}
    }

    fn into_user(self) -> Result<User<MailAddr, Self::Message>, Self> {
        Err(self)
    }
}

impl EventIngress<Here, SupervisionLifecycle<MailAddr>> for ApplicationEvent {
    fn ingress(lifecycle: SupervisionLifecycle<MailAddr>) -> Self {
        Self::Lifecycle(lifecycle)
    }
}

impl EventIngress<ChildRoute<StableWorker, ChildHead>, ProxyUnavailable<MailAddr, Never>>
    for ApplicationEvent
{
    fn ingress(unavailable: ProxyUnavailable<MailAddr, Never>) -> Self {
        Self::WorkerUnavailable(unavailable)
    }
}

#[derive(Default)]
struct ApplicationState {
    latest_supervision: Option<SupervisionLifecycle<MailAddr>>,
}

impl Protocol for ApplicationState {
    type Addr = MailAddr;
    type Msg = Never;
}

impl BehaviorBase for ApplicationState {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

impl Behavior for ApplicationState {
    type Protocol = Self;
    type Event = ApplicationEvent;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            ApplicationEvent::Lifecycle(lifecycle) => self.latest_supervision = Some(lifecycle),
            ApplicationEvent::WorkerUnavailable(unavailable) => match unavailable.command {},
        }
        Ok(Actions::cont())
    }
}

#[derive(TerminalProjection)]
#[allow(
    clippy::large_enum_variant,
    reason = "exact unboxed terminal custody is the observable example result"
)]
enum ApplicationTerminal {
    Root {
        origin: ActorOrigin<SupervisedApplication>,
        terminal: ActorRetirement<SupervisedApplication, Self>,
    },
    StableWorker {
        origin: ActorOrigin<ApplicationState, ChildHead>,
        terminal: ActorRetirement<StableWorker, Self>,
    },
    WorkerIncarnation {
        origin: ActorOrigin<StableWorker, ChildHead>,
        terminal: ActorRetirement<ManagedWorker, Self>,
    },
}

fn stop_worker(_: &mut Worker) -> Actions<MailAddr, Never, NoSends, NoBirths> {
    Actions::stop()
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "ChildTopology's owner contract models a potentially vacant worker slot"
)]
fn worker(_: usize) -> Option<ManagedWorker> {
    Some(StopOnShutdown::new(OneShot::new(
        Worker,
        TimerId(1),
        Duration::from_millis(1),
        stop_worker,
    )))
}

fn main() -> Result<(), SupervisorRunError> {
    let restart = RestartConfiguration::new(
        Strategy::OneForOne,
        RestartPolicy::Permanent,
        1,
        Duration::from_secs(30),
        RestartTiming::Delayed(
            Backoff::constant(Duration::from_millis(1)).expect("the restart delay is non-zero"),
        ),
    );
    let supervised = Supervise::new(
        ApplicationState::default(),
        ChildTopology::new([7], worker),
        restart,
        Proxy::new as StableLayer,
    )
    .expect("the supervised child nonce is unique")
    .with_failure_reaction(stop_on_supervision_failure::<ApplicationState>);

    let (termination, terminal): (_, ApplicationTerminal) = Application::new(supervised)
        .run_with(|application| async move { application.lifecycle().termination().await })?;
    let denial = RestartDenial::BudgetExceeded {
        restarts_in_window: 1,
        replacements_requested: 1,
        maximum_restarts: 1,
    };
    assert_eq!(
        termination,
        Ok(Exit::SupervisionFailed(
            SupervisionFailureReason::RestartDenied(denial)
        ))
    );
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("restart-budget exhaustion must preserve the supervised application state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert_eq!(behavior.child_count(), 1);
    assert_eq!(behavior.restarts_in_window(), 1);
    assert_eq!(behavior.pending_restarts(), 0);
    assert_eq!(
        behavior.base().latest_supervision,
        Some(SupervisionLifecycle::Retired {
            failure: SupervisionFailure::restart_denied(7, Ok(Exit::Normal), denial),
        })
    );
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_terminal_descendants(&descendants);
    Ok(())
}

fn assert_terminal_descendants(descendants: &[ApplicationTerminal]) {
    let [
        ApplicationTerminal::StableWorker {
            origin,
            terminal:
                ActorRetirement::OwnerCancelled {
                    control,
                    user,
                    descendants,
                    ..
                },
        },
    ] = descendants
    else {
        panic!("the supervised proxy must retain both worker retirements")
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), Some(7));
    assert!(control.is_empty());
    assert!(user.is_empty());

    let [first, second] = descendants.as_slice() else {
        panic!("the initial and replacement worker must both be retained")
    };
    let first_address = assert_worker_retirement(first, 0);
    let second_address = assert_worker_retirement(second, 1);
    assert_ne!(first_address, MailAddr::APPLICATION_ROOT);
    assert_ne!(second_address, MailAddr::APPLICATION_ROOT);
    assert_ne!(first_address, second_address);
}

fn assert_worker_retirement(terminal: &ApplicationTerminal, nonce: u64) -> MailAddr {
    let ApplicationTerminal::WorkerIncarnation {
        origin,
        terminal:
            ActorRetirement::Completed {
                control,
                user,
                descendants,
                completion,
                ..
            },
    } = terminal
    else {
        panic!("each timed worker must preserve its completed terminal state")
    };
    assert_eq!(origin.nonce(), Some(nonce));
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(*completion, Completion::Stopped);
    origin.address()
}
