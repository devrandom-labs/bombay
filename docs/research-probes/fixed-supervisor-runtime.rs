// Frozen 0.17.0 research probe. The executable 0.20.0 contract is in
// crates/bombay/tests/fixed_supervisor_runtime.rs and uses RootOrigin/ChildOrigin.
use core::convert::Infallible;
use core::time::Duration;

use behavior_actors::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition,
    FailureReaction, FixedCommand, FixedSnapshot, FixedSupervisor, OrderedRoles, Recovery,
    RestartLimit, RestartRelease, StableProxy, Strategy, WorkerSource, WorkerSubmission, fixed,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::actors::ActorExt as _;
use bombay::behavior::{ChildHead, MessageProtocol, Never};
use bombay::prelude::{ActorOrigin, ActorRetirement, Completion, MailAddr, TerminalProjection};
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
    async fn prepare_first(
        &mut self,
        _: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, Never, Never> {
        WorkerPreparationStart::Submitted(WorkerSubmission::activated(
            Worker.stop_on_shutdown(),
            ActivationNotice(None),
        ))
    }

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
    root: ActorSpace<RootProtocol>,
    workers: ActorSpace<WorkerProtocol>,
    status: ActorSpace<Status>,
    capability: ActorSpace<Capability>,
}

#[derive(TerminalProjection)]
enum SupervisorTerminal {
    Root {
        origin: ActorOrigin<RootSupervisor>,
        terminal: ActorRetirement<RootSupervisor, Self>,
    },
    Proxy {
        origin: ActorOrigin<Supervisor, ChildHead>,
        terminal: ActorRetirement<WorkerProxy, Self>,
    },
    Worker {
        origin: ActorOrigin<WorkerProxy, ChildHead>,
        terminal: ActorRetirement<ProxyWorker, Self>,
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
    let (termination, terminal): (_, SupervisorTerminal) =
        App::new(supervisor.stop_on_shutdown(), spaces)
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

    assert_eq!(termination, Ok(Exit::Normal));
    assert_orderly_supervisor_terminal(terminal);
}

fn assert_orderly_supervisor_terminal(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns the supervisor root");
    };
    assert_eq!(origin.nonce(), None);
    let ActorRetirement::Completed {
        descendants,
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor completes after its proxy tree");
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SupervisorTerminal::Proxy { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the declared role retains its proxy")
    else {
        panic!("the declared child is the stable proxy");
    };
    assert!(origin.nonce().is_some());
    let ActorRetirement::Completed {
        descendants,
        completion,
        ..
    } = terminal
    else {
        panic!("the supervisor completes its proxy during shutdown");
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SupervisorTerminal::Worker { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the activated worker remains in retirement custody")
    else {
        panic!("the nested child is the worker");
    };
    assert!(origin.nonce().is_some());
    let ActorRetirement::Completed {
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the proxy completes its worker during shutdown");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(descendants.is_empty());
}
