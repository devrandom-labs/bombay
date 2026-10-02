//! Run one fixed supervisor through worker failure, replacement, and shutdown.
//! The supervisor owns recovery policy; Bombay interprets its typed actions.

use core::convert::Infallible;
use core::time::Duration;

use bombay::actors::ActorExt as _;
use bombay::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticDisposition,
    FailureReaction, FixedCommand, FixedSnapshot, FixedSupervisor, OrderedRoles, Recovery,
    RestartLimit, RestartRelease, StableProxy, Strategy, WorkerSource, WorkerSubmission, fixed,
};
use bombay::behavior::{Actions, BehaviorActed, ChildHead, MessageProtocol, Never};
use bombay::prelude::{
    ActorRetirement, ChildOrigin, Completion, Exit, MailAddr, RootOrigin, StopOnShutdown,
    TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

enum WorkerCommand {
    Stop,
}

struct Worker;

#[bombay::actor]
impl Worker {
    fn receive(&mut self, message: WorkerCommand) -> BehaviorActed<Self> {
        match message {
            WorkerCommand::Stop => Ok(Actions::stop()),
        }
    }
}

type ManagedWorker = StopOnShutdown<Worker>;

struct ActivationNotice {
    role: WorkerRole,
    activated: mpsc::UnboundedSender<WorkerRole>,
}

impl ActivationPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    async fn activate(self) -> Result<Self::Ready, Self::Rejection> {
        self.activated
            .send(self.role)
            .expect("the application observes each worker activation");
        Ok(())
    }
}

struct Workshop {
    activated: mpsc::UnboundedSender<WorkerRole>,
}

impl WorkerSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    type WorkerRejection = Never;
    type SourceRejection = Never;
}

impl WorkerPreparationSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    async fn prepare_first(
        &mut self,
        role: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, Never, Never> {
        WorkerPreparationStart::Submitted(worker_submission(*role, &self.activated))
    }

    async fn prepare_next(
        &mut self,
        role: &WorkerRole,
    ) -> Result<WorkerSubmission<ManagedWorker, ActivationNotice>, Never> {
        Ok(worker_submission(*role, &self.activated))
    }
}

fn worker_submission(
    role: WorkerRole,
    activated: &mpsc::UnboundedSender<WorkerRole>,
) -> WorkerSubmission<ManagedWorker, ActivationNotice> {
    WorkerSubmission::activated(
        Worker.stop_on_shutdown(),
        ActivationNotice {
            role,
            activated: activated.clone(),
        },
    )
}

type WorkerProxy = StableProxy<ManagedWorker, ActivationNotice>;
type ProxyWorker = StopOnShutdown<ManagedWorker>;
type Supervisor =
    FixedSupervisor<WorkerRole, ManagedWorker, ActivationNotice, Workshop, Infallible, Infallible>;
type RootSupervisor = StopOnShutdown<Supervisor>;
type RootProtocol = MessageProtocol<MailAddr, FixedCommand<MailAddr, WorkerRole, Worker>>;
type Status = MessageProtocol<MailAddr, FixedSnapshot<Worker>>;
type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, Worker>>;

#[derive(ActorSpaces)]
struct SupervisorSpaces {
    #[actor_space(RootProtocol)]
    root: ActorSpace<RootProtocol>,
    #[actor_space(Worker)]
    workers: ActorSpace<Worker>,
    #[actor_space(Status)]
    status: ActorSpace<Status>,
    #[actor_space(Capability)]
    capability: ActorSpace<Capability>,
}

#[derive(TerminalProjection)]
enum SupervisorTerminal {
    Root {
        origin: RootOrigin<RootSupervisor>,
        terminal: ActorRetirement<RootSupervisor, Self>,
    },
    #[structural_child]
    Proxy {
        origin: ChildOrigin<Supervisor, ChildHead>,
        terminal: ActorRetirement<WorkerProxy, Self>,
    },
    #[structural_child]
    Worker {
        origin: ChildOrigin<WorkerProxy, ChildHead>,
        terminal: ActorRetirement<ProxyWorker, Self>,
    },
}

fn prepared_supervisor(activated: mpsc::UnboundedSender<WorkerRole>) -> Supervisor {
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("the one-role roster is distinct");
    let initial_activated = activated.clone();
    fixed(
        move |role: &WorkerRole| Ok::<_, Infallible>(worker_submission(*role, &initial_activated)),
        roles,
        ActivationPolicy::new(1).expect("one worker fits the activation policy"),
        Recovery::permanent(
            Workshop { activated },
            Strategy::OneForOne,
            RestartLimit::new(1, Duration::from_secs(30)),
            RestartRelease::immediate(),
        ),
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("the declared worker is prepared"))
}

fn run_supervision() {
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = prepared_supervisor(activated);
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let (termination, terminal): (_, SupervisorTerminal) =
        App::new(supervisor.stop_on_shutdown(), spaces)
            .run_with(move |application| async move {
                let first = tokio::time::timeout(Duration::from_secs(5), activations.recv())
                    .await
                    .expect("the first worker activates")
                    .expect("the first activation has a role");
                assert_eq!(first, WorkerRole::Primary);

                let interface = application.interface(application.root().established_recipient());
                let mut caller = interface
                    .external::<Capability>()
                    .expect("the capability caller is established");
                caller
                    .send(
                        interface.api(),
                        FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
                    )
                    .await
                    .expect("the supervisor accepts the capability query");
                let reply = caller
                    .receive()
                    .await
                    .expect("the primary capability is returned");
                let CapabilityResult::Ready { role, proxy } = reply.message else {
                    panic!("the primary proxy is ready")
                };
                assert_eq!(role, WorkerRole::Primary);
                caller
                    .send(&proxy, WorkerCommand::Stop)
                    .await
                    .expect("the first worker accepts its stop command");

                let replacement = tokio::time::timeout(Duration::from_secs(5), activations.recv())
                    .await
                    .expect("the replacement activates")
                    .expect("the replacement activation has a role");
                assert_eq!(replacement, WorkerRole::Primary);
                caller
                    .send(interface.api(), FixedCommand::shutdown())
                    .await
                    .expect("the supervisor accepts shutdown");
                let lifecycle = application.lifecycle();
                tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                    .await
                    .expect("the supervisor retires after replacement and shutdown")
            })
            .unwrap_or_else(|_| panic!("the supervisor runs its recovery policy"));

    assert_eq!(termination, Ok(Exit::Normal));
    assert_restarted_worker_retirement(terminal);
}

fn assert_restarted_worker_retirement(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns its supervisor root");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the supervisor returns completed terminal custody")
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let SupervisorTerminal::Proxy { origin, terminal } = descendants
        .into_iter()
        .next()
        .expect("the primary role retains its proxy")
    else {
        panic!("the primary child is a stable proxy")
    };
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the proxy returns both worker incarnations")
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    for descendant in descendants {
        let SupervisorTerminal::Worker { origin, terminal } = descendant else {
            panic!("the proxy retains a worker incarnation")
        };
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            completion,
            descendants,
            ..
        } = terminal
        else {
            panic!("the worker returns completed terminal custody")
        };
        assert_eq!(completion, Completion::Stopped);
        assert!(descendants.is_empty());
    }
}

fn main() {
    run_supervision();
}

#[cfg(test)]
mod tests {
    use super::prepared_supervisor;
    use bombay::atomic::ProxyPhase;
    use bombay::behavior::{CreationKind, Step};
    use bombay::prelude::Activate as _;
    use tokio::sync::mpsc;

    #[test]
    fn fixed_supervisor_initialization_preserves_role_and_correlation() {
        let (activated, _) = mpsc::unbounded_channel();
        let supervisor = prepared_supervisor(activated);
        let initialized = supervisor
            .initialize()
            .unwrap_or_else(|_| panic!("the fixed supervisor initializes its stable proxy"));
        assert_eq!(initialized.actions.creates.len(), 1);
        let proxy = initialized
            .actions
            .creates
            .iter()
            .next()
            .expect("the primary role owns one stable proxy");
        let creation = proxy.id();
        assert_eq!(proxy.kind(), CreationKind::Birth);
        assert_eq!(proxy.child().phase(), ProxyPhase::Dormant);
        assert_eq!(initialized.actions.sends.proxy_observations.len(), 1);
        assert_eq!(
            initialized.actions.sends.proxy_observations[0].child,
            creation
        );
        assert_eq!(initialized.actions.become_, Step::Continue);
    }
}
