// Frozen 0.17.0 research probe. The executable 0.20.0 contract is in
// crates/bombay/tests/fixed_supervisor_recovery.rs and uses RootOrigin/ChildOrigin.
use core::convert::Infallible;
use core::time::Duration;
use std::sync::{Arc, Mutex, PoisonError};

use behavior_actors::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticAccepted,
    DiagnosticDisposition, FailureReaction, FixedCommand, FixedDiagnostic, FixedSnapshot,
    FixedSupervisor, OrderedRoles, Recovery, RestartLimit, RestartRelease, StableProxy, Strategy,
    WorkerPreparationFailureReason, WorkerSource, WorkerSubmission, fixed,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::actors::ActorExt as _;
use bombay::behavior::{
    Actions, BehaviorActed, ChildHead, ItemSettlement, MessageProtocol, Never, SettledItem,
};
use bombay::prelude::{ActorOrigin, ActorRetirement, Completion, MailAddr, TerminalProjection};
use bombay::{ActorSpace, ActorSpaces, App, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
    Secondary,
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
            .unwrap_or_else(|_| panic!("the application observes each activation"));
        Ok(())
    }
}

struct Workshop {
    prepared: Arc<Mutex<Vec<WorkerRole>>>,
    activated: mpsc::UnboundedSender<WorkerRole>,
    policy: PreparationPolicy,
}

#[derive(Clone, Copy)]
enum PreparationPolicy {
    SubmitBoth,
    RejectSecondary,
}

#[derive(Debug, Eq, PartialEq)]
enum WorkshopRejection {
    WorkerUnavailable,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WorkerRetirement {
    Completed,
    OwnerCancelled,
}

impl WorkerSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    type WorkerRejection = WorkshopRejection;
    type SourceRejection = Never;
}

impl WorkerPreparationSource<WorkerRole, ManagedWorker, ActivationNotice> for Workshop {
    async fn prepare_first(
        &mut self,
        role: &WorkerRole,
    ) -> WorkerPreparationStart<ManagedWorker, ActivationNotice, WorkshopRejection, Never> {
        self.prepared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(*role);
        WorkerPreparationStart::Submitted(WorkerSubmission::activated(
            Worker.stop_on_shutdown(),
            ActivationNotice {
                role: *role,
                activated: self.activated.clone(),
            },
        ))
    }

    async fn prepare_next(
        &mut self,
        role: &WorkerRole,
    ) -> Result<WorkerSubmission<ManagedWorker, ActivationNotice>, WorkshopRejection> {
        self.prepared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(*role);
        match self.policy {
            PreparationPolicy::SubmitBoth => Ok(WorkerSubmission::activated(
                Worker.stop_on_shutdown(),
                ActivationNotice {
                    role: *role,
                    activated: self.activated.clone(),
                },
            )),
            PreparationPolicy::RejectSecondary => Err(WorkshopRejection::WorkerUnavailable),
        }
    }
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
    root: ActorSpace<RootProtocol>,
    workers: ActorSpace<Worker>,
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

fn prepared_supervisor(
    prepared: Arc<Mutex<Vec<WorkerRole>>>,
    activated: mpsc::UnboundedSender<WorkerRole>,
    policy: PreparationPolicy,
) -> Supervisor {
    let roles = OrderedRoles::new(WorkerRole::Primary, [WorkerRole::Secondary])
        .expect("the two declared roles are unique");
    let initial_activated = activated.clone();
    fixed(
        move |role: &WorkerRole| {
            Ok::<_, Infallible>(WorkerSubmission::activated(
                Worker.stop_on_shutdown(),
                ActivationNotice {
                    role: *role,
                    activated: initial_activated.clone(),
                },
            ))
        },
        roles,
        ActivationPolicy::new(2).expect("two workers fit the activation policy"),
        Recovery::permanent(
            Workshop {
                prepared,
                activated,
                policy,
            },
            Strategy::OneForAll,
            RestartLimit::new(2, Duration::from_secs(30)),
            RestartRelease::immediate(),
        ),
        FailureReaction::StopSupervisor,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .build()
    .unwrap_or_else(|_| panic!("both initial workers are prepared"))
}

#[test]
fn coordinated_recovery_prepares_replacement_roles_in_declaration_order() {
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = prepared_supervisor(
        Arc::clone(&prepared),
        activated,
        PreparationPolicy::SubmitBoth,
    );
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let (termination, terminal): (_, SupervisorTerminal) =
        App::new(supervisor.stop_on_shutdown(), spaces)
            .run_with(move |application| async move {
                let first = activations
                    .recv()
                    .await
                    .expect("the first worker activates");
                let second = activations
                    .recv()
                    .await
                    .expect("the second worker activates");
                assert_ne!(first, second);

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
                    .expect("the primary worker accepts its stop command");

                let first = activations
                    .recv()
                    .await
                    .expect("the first replacement activates");
                let second = activations
                    .recv()
                    .await
                    .expect("the second replacement activates");
                assert_ne!(first, second);
                caller
                    .send(interface.api(), FixedCommand::shutdown())
                    .await
                    .expect("the supervisor accepts shutdown");
                let lifecycle = application.lifecycle();
                tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                    .await
                    .expect("the supervisor terminates after recovery and shutdown")
            })
            .unwrap_or_else(|_| panic!("the two-role supervisor runs its policy"));

    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared.lock().unwrap_or_else(PoisonError::into_inner),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_restarted_workers_retired(terminal);
}

fn assert_restarted_workers_retired(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns its supervisor root")
    };
    assert_eq!(origin.nonce(), None);
    let ActorRetirement::Completed {
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the supervisor returns completed terminal custody")
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    for descendant in descendants {
        let SupervisorTerminal::Proxy { origin, terminal } = descendant else {
            panic!("the supervisor retains each stable proxy")
        };
        assert!(origin.nonce().is_some());
        let ActorRetirement::Completed {
            completion,
            descendants,
            ..
        } = terminal
        else {
            panic!("each proxy completes after its workers")
        };
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 2);
        for descendant in descendants {
            let SupervisorTerminal::Worker { origin, terminal } = descendant else {
                panic!("the proxy retains both worker incarnations")
            };
            assert!(origin.nonce().is_some());
            let ActorRetirement::Completed {
                completion,
                descendants,
                ..
            } = terminal
            else {
                panic!("each worker completes its retirement")
            };
            assert_eq!(completion, Completion::Stopped);
            assert!(descendants.is_empty());
        }
    }
}

#[test]
fn coordinated_recovery_rejects_the_second_role_after_preparing_the_first() {
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = prepared_supervisor(
        Arc::clone(&prepared),
        activated,
        PreparationPolicy::RejectSecondary,
    );
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let (termination, terminal): (_, SupervisorTerminal) = App::new(
        supervisor.stop_on_shutdown(),
        spaces,
    )
    .run_with(move |application| async move {
        let first = activations
            .recv()
            .await
            .expect("the first worker activates");
        let second = activations
            .recv()
            .await
            .expect("the second worker activates");
        assert_ne!(first, second);

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
            .expect("the primary worker accepts its stop command");

        let lifecycle = application.lifecycle();
        let rejection_retirement = async {
            tokio::select! {
                replacement = activations.recv() => {
                    panic!("a rejected second role cannot activate a replacement: {replacement:?}")
                }
                termination = lifecycle.termination() => termination,
            }
        };
        tokio::time::timeout(Duration::from_secs(5), rejection_retirement)
            .await
            .expect("the supervisor retires after the second role rejects")
    })
    .unwrap_or_else(|_| panic!("the second-role rejection is interpreted"));

    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared.lock().unwrap_or_else(PoisonError::into_inner),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_rejected_preparation_retirement(terminal);
}

fn assert_rejected_preparation_retirement(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns its supervisor root")
    };
    assert_eq!(origin.nonce(), None);
    let ActorRetirement::Completed {
        completion,
        descendants,
        settlements,
        ..
    } = terminal
    else {
        panic!("the supervisor keeps terminal custody after rejection")
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    let mut diagnostics = settlements
        .into_iter()
        .flat_map(|settlement| settlement.sends.inner.diagnostics)
        .collect::<Vec<_>>();
    let diagnostic = diagnostics.pop().expect("one exact terminal diagnostic");
    assert!(diagnostics.is_empty());
    let SettledItem::Attempted(ItemSettlement::Accepted(DiagnosticAccepted::Terminal(
        FixedDiagnostic::WorkerPreparationFailed(failure),
    ))) = diagnostic
    else {
        panic!("the second-role rejection stays a preparation diagnostic")
    };
    assert_eq!(failure.role(), &WorkerRole::Primary);
    let prepared_roles = failure
        .prepared()
        .map(|(role, _)| *role)
        .collect::<Vec<_>>();
    assert_eq!(prepared_roles, [WorkerRole::Primary]);
    let WorkerPreparationFailureReason::WorkerRejected { role, reason } = failure.reason() else {
        panic!("the later worker alone is rejected")
    };
    assert_eq!(*role, WorkerRole::Secondary);
    assert_eq!(*reason, WorkshopRejection::WorkerUnavailable);
    assert_eq!(failure.remaining_roles().count(), 0);
    let mut worker_retirements = Vec::new();
    for descendant in descendants {
        let SupervisorTerminal::Proxy { origin, terminal } = descendant else {
            panic!("the supervisor retains each stable proxy")
        };
        assert!(origin.nonce().is_some());
        let ActorRetirement::OwnerCancelled { descendants, .. } = terminal else {
            panic!("the terminal diagnostic cancels each proxy's owner task")
        };
        assert_eq!(descendants.len(), 1);
        let SupervisorTerminal::Worker { origin, terminal } = descendants
            .into_iter()
            .next()
            .expect("only the original worker belongs to this proxy")
        else {
            panic!("the proxy retains its original worker")
        };
        assert!(origin.nonce().is_some());
        let retirement = match terminal {
            ActorRetirement::Completed {
                completion,
                descendants,
                ..
            } => {
                assert_eq!(completion, Completion::Stopped);
                assert!(descendants.is_empty());
                WorkerRetirement::Completed
            }
            ActorRetirement::OwnerCancelled { descendants, .. } => {
                assert!(descendants.is_empty());
                WorkerRetirement::OwnerCancelled
            }
            _ => panic!("the original worker retains an exact terminal outcome"),
        };
        worker_retirements.push(retirement);
    }
    worker_retirements.sort();
    assert_eq!(
        worker_retirements,
        [
            WorkerRetirement::Completed,
            WorkerRetirement::OwnerCancelled
        ]
    );
}
