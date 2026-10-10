use bombay::ActorNotificationReceipts;
use bombay::ApplicationOutcome;
use core::convert::Infallible;
use core::future::Future;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::mem;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::runtime::Builder;

use behavior_actors::atomic::{
    ActivationPlan, ActivationPolicy, ActorDrainPolicy, CapabilityResult, DiagnosticAccepted,
    DiagnosticDisposition, FailureReaction, FixedCommand, FixedDiagnostic, FixedSnapshot,
    FixedSupervisor, FixedSupervisorEvent, OrderedRoles, Recovery, RestartLimit, RestartRelease,
    StableProxy, Strategy, WorkerPreparationFailureReason, WorkerSource, WorkerSubmission, fixed,
};
use behavior_actors::{Exit, StopOnShutdown};
use bombay::actors::ActorExt as _;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorSettlements, ChildHead, CreationSettlement, EventLayer,
    ItemSettlement, MessageProtocol, Never, NoSends, Protocol, SettledItem, Step, Stopped,
};
use bombay::prelude::{
    ActorRetirement, ChildFailure, ChildOrigin, Completion, MailAddr, RootOrigin,
    TerminalProjection,
};
use bombay::{
    ActorFailureAssessment, ActorSpace, ActorSpaces, App, ProjectTerminal, RetirementAssessment,
    WorkerPreparationSource, WorkerPreparationStart,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

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

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
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
    preparation: Option<PreparationRelease>,
}

struct PreparationRelease {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    resource: OwnedSemaphorePermit,
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
        if let Some(PreparationRelease {
            reached,
            release,
            resource,
        }) = self.preparation.take()
        {
            reached
                .send(())
                .expect("the caller observes actual source work");
            release.await.expect("the actual source work stays held");
            drop(resource);
        }
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

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
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
#[expect(
    clippy::large_enum_variant,
    reason = "retain complete original role retirements without adding a heap owner or changing terminal custody"
)]
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

fn prepared_supervisor(
    prepared: Arc<Mutex<Vec<WorkerRole>>>,
    activated: mpsc::UnboundedSender<WorkerRole>,
    policy: PreparationPolicy,
    preparation: Option<PreparationRelease>,
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
                preparation,
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
        None,
    );
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(supervisor.stop_on_shutdown(), spaces)
                .run_with::<SupervisorTerminal, _, _, _>(move |application| async move {
                    let first = activations
                        .recv()
                        .await
                        .expect("the first worker activates");
                    let second = activations
                        .recv()
                        .await
                        .expect("the second worker activates");
                    assert_ne!(first, second);

                    let interface =
                        application.interface(application.root().established_recipient());
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
                }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the two-role supervisor runs its policy");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((origin, joined)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal = SupervisorTerminal::project(origin, joined);

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
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        completion,
        descendants,
        ..
    } = terminal
    else {
        panic!("the supervisor returns completed terminal custody")
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    for descendant in descendants {
        let SupervisorTerminal::Proxy { origin, terminal } = descendant else {
            panic!("the supervisor retains each stable proxy")
        };
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            child_failures: (child_failures, ()),

            capability_failures,
            unread_owner_cancellation,
            completion,
            descendants,
            ..
        } = terminal
        else {
            panic!("each proxy completes after its workers")
        };
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(child_failures.is_empty());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 2);
        for descendant in descendants {
            let SupervisorTerminal::Worker { origin, terminal } = descendant else {
                panic!("the proxy retains both worker incarnations")
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
                panic!("each worker completes its retirement")
            };
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
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
        None,
    );
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host.block_on(App::new(
        supervisor.stop_on_shutdown(),
        spaces,
    )
    .run_with::<SupervisorTerminal, _, _, _>(move |application| async move {
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
    }))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the second-role rejection is interpreted");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((origin, joined)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let retirement = match joined {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual supervisor actor task failed: {failure}")
        }
        retirement => retirement,
    };
    let terminal = SupervisorTerminal::project(origin, retirement);

    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared.lock().unwrap_or_else(PoisonError::into_inner),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_rejected_preparation_retirement(terminal);
}

#[allow(
    clippy::too_many_lines,
    reason = "one complete ordered supervisor tree trace checks exact terminal diagnostics and every retained child cause"
)]
fn assert_rejected_preparation_retirement(terminal: SupervisorTerminal) {
    let SupervisorTerminal::Root { origin, terminal } = terminal else {
        panic!("the application returns its supervisor root")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        completion,
        descendants,
        settlements,
        ..
    } = terminal
    else {
        panic!("the supervisor keeps terminal custody after rejection")
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
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
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::OwnerCancelled {
            child_failures: (child_failures, ()),

            capability_failures,
            unread_owner_cancellation,
            descendants,
            ..
        } = terminal
        else {
            panic!("the terminal diagnostic cancels each proxy's owner task")
        };
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(child_failures.is_empty());
        assert_eq!(descendants.len(), 1);
        let SupervisorTerminal::Worker { origin, terminal } = descendants
            .into_iter()
            .next()
            .expect("only the original worker belongs to this proxy")
        else {
            panic!("the proxy retains its original worker")
        };
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let retirement = match terminal {
            ActorRetirement::Completed {
                child_failures: (),

                capability_failures,
                unread_owner_cancellation,
                completion,
                descendants,
                ..
            } => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(completion, Completion::Stopped);
                assert!(descendants.is_empty());
                WorkerRetirement::Completed
            }
            ActorRetirement::OwnerCancelled {
                child_failures: (),

                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            } => {
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
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

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual held-resource trace keeps two independent Application native products and both notification stages"
)]
fn joined_root_retirement_waits_for_owned_preparation_while_another_application_progresses() {
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let original_source = Arc::downgrade(&prepared);
    let (reached, preparation_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let resource = Arc::new(Semaphore::new(1));
    let resource_observation = resource.clone();
    let permit = resource
        .try_acquire_owned()
        .expect("one actual resource is held");
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = prepared_supervisor(
        prepared,
        activated,
        PreparationPolicy::SubmitBoth,
        Some(PreparationRelease {
            reached,
            release: released,
            resource: permit,
        }),
    );
    let spaces = SupervisorSpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        status: ActorSpace::new(),
        capability: ActorSpace::new(),
    };
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let outcome = host
        .block_on(
            App::new(supervisor.stop_on_shutdown(), spaces)
                .run_with::<SupervisorTerminal, _, _, _>(move |application| async move {
                    let first = activations.recv().await.unwrap();
                    let second = activations.recv().await.unwrap();
                    assert_ne!(first, second);
                    let target_address = application.root().address();
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface.external::<Capability>().unwrap();
                    caller
                        .send(
                            interface.api(),
                            FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
                        )
                        .await
                        .unwrap();
                    let reply = caller.receive().await.unwrap();
                    assert_eq!(reply.from, target_address);
                    let CapabilityResult::Ready { role, proxy } = reply.message else {
                        panic!("the primary worker is ready")
                    };
                    assert_eq!(role, WorkerRole::Primary);
                    caller.send(&proxy, WorkerCommand::Stop).await.unwrap();
                    preparation_started
                        .await
                        .expect("the real supervisor-owned task reached its held work");
                    let lifecycle = application.lifecycle();
                    lifecycle.request_shutdown().unwrap();
                    let termination = lifecycle.termination().await;
                    assert_eq!(termination, Ok(Exit::Normal));
                    let mut initial_observer = Box::pin(lifecycle.retirement());
                    let initial_report = initial_observer
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()));
                    let initial_permits = resource_observation.available_permits();
                    drop(initial_observer);
                    let mut report = Box::pin(lifecycle.retirement());
                    let (sibling_activated, mut sibling_activations) = mpsc::unbounded_channel();
                    let sibling_supervisor = prepared_supervisor(
                        Arc::new(Mutex::new(Vec::new())),
                        sibling_activated,
                        PreparationPolicy::SubmitBoth,
                        None,
                    );
                    let sibling_spaces = SupervisorSpaces {
                        root: ActorSpace::new(),
                        workers: ActorSpace::new(),
                        status: ActorSpace::new(),
                        capability: ActorSpace::new(),
                    };
                    // This is a separately owned Application on the same host, not a sibling child or networking session.
                    App::new(sibling_supervisor.stop_on_shutdown(), sibling_spaces)
                        .run_with::<SupervisorTerminal, _, _, _>(move |sibling| async move {
                            let first = sibling_activations.recv().await.unwrap();
                            let second = sibling_activations.recv().await.unwrap();
                            assert_ne!(first, second);
                            let sibling_address = sibling.root().address();
                            let interface =
                                sibling.interface(sibling.root().established_recipient());
                            let mut service = interface.external::<Capability>().unwrap();
                            service
                                .send(
                                    interface.api(),
                                    FixedCommand::capability(
                                        WorkerRole::Primary,
                                        service.recipient(),
                                    ),
                                )
                                .await
                                .unwrap();
                            let useful = service.receive().await.unwrap();
                            assert_eq!(useful.from, sibling_address);
                            let CapabilityResult::Ready { role, .. } = useful.message else {
                                panic!("the independent actor still replies")
                            };
                            assert_eq!(role, WorkerRole::Primary);
                            let pending_report = report
                                .as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()));
                            assert!(
                                matches!(pending_report, Poll::Pending),
                                "joined retirement stays absent across independent useful progress"
                            );
                            assert!(
                                matches!(initial_report, Poll::Pending),
                                "the independent first observer also stayed pending"
                            );
                            assert_eq!(initial_permits, 0);
                            assert_eq!(resource_observation.available_permits(), 0);
                            let admitted = release.send(());
                            admitted.expect(
                                "the actual actor-owned source work still receives its release",
                            );
                            let target_report = report.await;
                            assert_eq!(resource_observation.available_permits(), 1);
                            assert_eq!(
                                target_report.retirement(),
                                RetirementAssessment::Established
                            );
                            let replayed = lifecycle.retirement().await;
                            assert_eq!(replayed, target_report);
                            sibling.lifecycle().request_shutdown().unwrap();
                            let sibling_report = sibling.lifecycle().retirement().await;
                            (target_report, sibling_report)
                        })
                        .await
                        .unwrap_or_else(|_| {
                            panic!("the sibling Application enters the same caller-owned host")
                        })
                }),
        )
        .unwrap_or_else(|_| panic!("the target Application enters its caller-owned host"));
    let (
        ApplicationOutcome::Completed {
            output: sibling_outcome,
            cleanup: Ok(()),
        },
        Ok((target_origin, target_native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = outcome
    else {
        panic!("the original target native and both notification stages remain independently owned")
    };
    let (
        ApplicationOutcome::Completed {
            output: (target_report, sibling_report),
            cleanup: Ok(()),
        },
        Ok((sibling_origin, sibling_native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = sibling_outcome
    else {
        panic!(
            "the original sibling native and both notification stages remain independently owned"
        )
    };
    assert_eq!(
        target_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(
        sibling_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        sibling_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(original_source.strong_count(), 1);
    let returned_source = original_source.upgrade().unwrap();
    let prepared_roles = returned_source.lock().unwrap();
    assert_eq!(
        *prepared_roles,
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    drop(prepared_roles);
    drop(returned_source);
    let mut target = SupervisorTerminal::project(target_origin, target_native);
    let mut sibling = SupervisorTerminal::project(sibling_origin, sibling_native);
    for terminal in [&mut target, &mut sibling] {
        let SupervisorTerminal::Root { terminal, .. } = terminal else {
            panic!("both original roots retain their native products")
        };
        let (descendants, (failures, ())) = assert_joined_native(terminal);
        assert!(failures.is_empty());
        assert_eq!(descendants.len(), 2);
        for proxy in descendants {
            let SupervisorTerminal::Proxy { terminal, .. } = proxy else {
                panic!("the real supervisor retains its declared proxy")
            };
            let (workers, (failures, ())) = assert_joined_native(terminal);
            assert!(failures.is_empty());
            assert_eq!(workers.len(), 1);
            for worker in workers {
                let SupervisorTerminal::Worker { terminal, .. } = worker else {
                    panic!("the real proxy retains its actual worker")
                };
                let (descendants, ()) = assert_joined_native(terminal);
                assert!(descendants.is_empty());
            }
        }
        let ActorRetirement::Completed { settlements, .. } = terminal else {
            panic!("both roots retain their actual final Stop product")
        };
        assert_eq!(settlements.len(), 1);
        // Final inspection transfers the original product; the native source/control remains held.
        let retained = mem::take(settlements);
        let mut retained = retained.into_iter();
        let final_actions = retained.next().unwrap();
        assert_eq!(retained.len(), 0);
        assert!(matches!(final_actions.become_, Step::Stop(Stopped)));
        let CreationSettlement::Settled(creations) = final_actions.creations else {
            panic!("the final empty creation lane settled")
        };
        assert!(creations.is_empty());
        assert_eq!(final_actions.sends.owned, NoSends);
        let sends = final_actions.sends.inner;
        assert_eq!(sends.proxy_observations.len(), 0);
        let preparations = sends.worker_preparations.into_inputs();
        let operations = sends.proxy_operations.into_inputs();
        let schedules = sends.restart_schedules.into_inputs();
        assert_eq!(preparations.len(), 0);
        assert_eq!(operations.len(), 0);
        assert_eq!(schedules.len(), 0);
        assert_eq!(sends.lifecycle, NoSends);
        assert_eq!(sends.status_replies.as_slice().len(), 0);
        assert_eq!(sends.capability_replies.as_slice().len(), 0);
        assert_eq!(sends.diagnostics.len(), 0);
    }
    let SupervisorTerminal::Root {
        terminal: ActorRetirement::Completed { control, .. },
        ..
    } = &target
    else {
        panic!("the target conserves its completed root")
    };
    assert_eq!(control.len(), 1);
    assert!(matches!(
        &control[0],
        EventLayer::Inner(FixedSupervisorEvent::WorkerPreparationReturned(_))
    ));
    drop((target, sibling));
    assert_eq!(original_source.strong_count(), 0);
}

fn assert_joined_native<B, Failures>(
    retirement: &ActorRetirement<B, SupervisorTerminal, Failures>,
) -> (&[SupervisorTerminal], &Failures)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let (ActorRetirement::Completed {
        interpretation,
        source,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        user,
        descendants,
        child_failures,
        capability_failures,
        unread_owner_cancellation,
        additional_failures,
        terminal_report,
        retirement_failures,
        completion: Completion::Stopped,
        ..
    }
    | ActorRetirement::OwnerCancelled {
        interpretation,
        source,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        user,
        descendants,
        child_failures,
        capability_failures,
        unread_owner_cancellation,
        additional_failures,
        terminal_report,
        retirement_failures,
        ..
    }) = retirement
    else {
        panic!(
            "every real joined owner retains its complete normal or owner-cancelled native product"
        )
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(user.is_empty());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(additional_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(retirement_failures.is_empty());
    (descendants, child_failures)
}
