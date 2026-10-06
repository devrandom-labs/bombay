use core::convert::Infallible;
use core::time::Duration;

use behavior_actors::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition,
    FailureReaction, FixedCommand, FixedSnapshot, FixedSupervisor, OrderedRoles, Recovery,
    RestartLimit, RestartRelease, StableProxy, Strategy, WorkerSource, WorkerSubmission, fixed,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::ProjectTerminal;
use bombay::actors::ActorExt as _;
use bombay::behavior::{ChildHead, MessageProtocol, Never};
use bombay::prelude::{
    ActorRetirement, ChildFailure, ChildOrigin, Completion, MailAddr, RootOrigin,
    TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

type ManagedWorker = StopOnShutdown<Worker>;

struct ActivationNotice(Option<oneshot::Sender<()>>);

impl ActivationPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        if let Some(sender) = self.0 {
            sender
                .send(())
                .unwrap_or_else(|()| panic!("the application boundary awaits activation"));
        }
        Ok(())
    }
}

struct Workshop;

impl WorkerSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    type WorkerRejection = Never;
    type SourceRejection = Never;
}

impl WorkerPreparationSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_first(
        &mut self,
        _: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, Never, Never> {
        WorkerPreparationStart::Submitted(WorkerSubmission::activated(
            Worker.stop_on_shutdown(),
            ActivationNotice(None),
        ))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_next(
        &mut self,
        _: &WorkerRole,
    ) -> Result<WorkerSubmission<ManagedWorker, ActivationNotice>, Never> {
        Ok(WorkerSubmission::activated(
            Worker.stop_on_shutdown(),
            ActivationNotice(None),
        ))
    }
}

type WorkerProxy = StableProxy<ManagedWorker, ActivationNotice>;
type ProxyWorker = StopOnShutdown<ManagedWorker>;
type Supervisor =
    FixedSupervisor<WorkerRole, ManagedWorker, ActivationNotice, Workshop, Infallible, Infallible>;
type RootSupervisor = StopOnShutdown<Supervisor>;
type WorkerProtocol = Worker;
type RootProtocol = MessageProtocol<MailAddr, FixedCommand<MailAddr, WorkerRole, WorkerProtocol>>;
type Status = MessageProtocol<MailAddr, FixedSnapshot<WorkerProtocol>>;
type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, WorkerProtocol>>;

#[derive(ActorSpaces)]
struct SupervisorSpaces {
    #[actor_space(RootProtocol)]
    root: ActorSpace<RootProtocol>,
    #[actor_space(WorkerProtocol)]
    workers: ActorSpace<WorkerProtocol>,
    #[actor_space(Status)]
    status: ActorSpace<Status>,
    #[actor_space(Capability)]
    capability: ActorSpace<Capability>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "retain original unboxed root, proxy and worker retirements without an extra allocation or disposal owner"
)]
#[derive(TerminalProjection)]
enum SupervisorTerminal {
    Root {
        origin: RootOrigin<RootSupervisor>,
        #[expect(
            clippy::type_complexity,
            reason = "the supervisor retirement retains its exact proxy-role child failures and complete terminal"
        )]
        terminal: ActorRetirement<
            RootSupervisor,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<Supervisor, ChildHead>, WorkerProxy>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Proxy {
        origin: ChildOrigin<Supervisor, ChildHead>,
        #[expect(
            clippy::type_complexity,
            reason = "the proxy retirement retains its exact worker-role child failures and complete terminal"
        )]
        terminal: ActorRetirement<
            WorkerProxy,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<WorkerProxy, ChildHead>, ProxyWorker>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Worker {
        origin: ChildOrigin<WorkerProxy, ChildHead>,
        terminal: ActorRetirement<ProxyWorker, Self, ()>,
    },
}

fn prepared_supervisor(activation_sender: oneshot::Sender<()>) -> Supervisor {
    let mut initial_activation = Some(activation_sender);
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the single declared role is unique");
    let release = RestartRelease::constant(Duration::from_millis(1))
        .expect("the positive release interval is valid");
    fixed(
        move |_: &WorkerRole| {
            let sender = initial_activation
                .take()
                .expect("the sole initial role owns one activation notice");
            Ok::<_, Infallible>(WorkerSubmission::activated(
                Worker.stop_on_shutdown(),
                ActivationNotice(Some(sender)),
            ))
        },
        roles,
        ActivationPolicy::new(1).expect("one activation is positive capacity"),
        Recovery::permanent(
            Workshop,
            Strategy::OneForOne,
            RestartLimit::new(1, Duration::from_secs(30)),
            release,
        ),
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("the sole worker is prepared"))
}

#[test]
fn fixed_supervisor_executes_activation_and_retires_its_proxy_tree() {
    let (activation_sender, activation_received) = oneshot::channel();
    let supervisor = prepared_supervisor(activation_sender);
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let (termination, root_origin, joined_actor) = App::new(supervisor.stop_on_shutdown(), spaces)
        .run_with(move |application| async move {
            activation_received
                .await
                .expect("the proxy activates its exact worker");
            let interface = application.interface(application.root().established_recipient());
            let caller = interface
                .external::<Status>()
                .expect("the supervisor caller is established");
            caller
                .send(interface.api(), FixedCommand::shutdown())
                .await
                .expect("the supervisor accepts shutdown");
            let lifecycle = application.lifecycle();
            tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                .await
                .expect("the supervisor terminates after shutdown")
        })
        .unwrap_or_else(|_| panic!("the supervisor runs its worker and shuts down"));
    let terminal: SupervisorTerminal = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );

    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_supervisor_terminal(terminal);
}

fn assert_orderly_supervisor_terminal(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns the supervisor root");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        descendants,
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor completes after its proxy tree");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SupervisorTerminal::Proxy { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the declared role retains its proxy")
    else {
        panic!("the declared child is the stable proxy");
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        descendants,
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor completes its proxy during shutdown");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SupervisorTerminal::Worker { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the activated worker remains in retirement custody")
    else {
        panic!("the nested child is the worker");
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (),

        capability_failures,
        unread_owner_cancellation,
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the proxy completes its worker during shutdown");
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert!(descendants.is_empty());
}
