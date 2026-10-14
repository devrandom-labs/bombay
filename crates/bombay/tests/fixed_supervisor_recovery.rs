use bombay::ApplicationOutcome;
use bombay::{ActorCustody, ActorNotificationReceipts};
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
    Strategy, WorkerPreparationFailureReason, WorkerSource, WorkerSubmission, fixed,
};
use behavior_actors::{Exit, ShutdownRequested, StopOnShutdown};
use bombay::actors::ActorExt as _;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BehaviorSettlements, Births,
    ChildCreationOutcome, CreateChild, CreationEvent, CreationSequence, CreationSettlement,
    CreationSettlements, Creations, EstablishedActor, EstablishedDelivery, EstablishedRecipient,
    EventLayer, Here, Ingress, InitializationTurn, InterpretEstablished, InterpretInstalledActor,
    ItemSettlement, MessageProtocol, Never, NoSends, Protocol, SettledItem, Step, Stopped,
};
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin};
use bombay::{
    ActorFailureAssessment, ActorRef, Application, InstalledActor, RetirementAssessment,
    RetirementSchema, WorkerPreparationSource, WorkerPreparationStart,
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

type Supervisor =
    FixedSupervisor<WorkerRole, ManagedWorker, ActivationNotice, Workshop, Infallible, Infallible>;
type RootSupervisor = StopOnShutdown<Supervisor>;
type Status = MessageProtocol<MailAddr, FixedSnapshot<Worker>>;
type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, Worker>>;

bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { status: Status; capability: Capability; }
    }
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
            WorkerSubmission::activated(
                Worker.stop_on_shutdown(),
                ActivationNotice {
                    role: *role,
                    activated: initial_activated.clone(),
                },
            )
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
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    let first = activations
                        .recv()
                        .await
                        .expect("the first worker activates");
                    let second = activations
                        .recv()
                        .await
                        .expect("the second worker activates");
                    assert_ne!(first, second);

                    let supervisor = application.root().established_recipient();
                    let mut caller = application
                        .external::<Capability>()
                        .expect("the capability caller is established");
                    caller
                        .send(
                            &supervisor,
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
                        .send(&supervisor, FixedCommand::shutdown())
                        .await
                        .expect("the supervisor accepts shutdown");
                    tokio::time::timeout(Duration::from_secs(5), application.termination())
                        .await
                        .expect("the supervisor terminates after recovery and shutdown")
                },
            ),
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
    let retirement = match joined {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual supervisor actor task failed: {failure}")
        }
        retirement => retirement,
    };
    let terminal = retirement;

    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared.lock().unwrap_or_else(PoisonError::into_inner),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_restarted_workers_retired(origin, terminal);
}

fn assert_restarted_workers_retired(
    origin: RootOrigin<RootSupervisor>,
    terminal: ActorRetirement<RootSupervisor, supervisor_application::RootRetirements<RootSupervisor>, Vec<<supervisor_application::RootRetirements<RootSupervisor> as bombay::RetirementSchema>::Failure>>,
) {
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
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
        let supervisor_application::RootRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            completion,
            ..
        } = *terminal
        else {
            panic!("each proxy completes after its workers")
        };
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(child_failures.is_empty());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 2);
        for descendant in descendants {
            let supervisor_application::ProxyRetirements {
                origin,
                retirement: terminal,
            } = descendant;
            assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
            let ActorRetirement::Completed {
                custody:
                    ActorCustody {
                        child_failures,
                        capability_failures,
                        unread_owner_cancellation,
                        descendants,
                        ..
                    },
                completion,
                ..
            } = *terminal
            else {
                panic!("each worker completes its retirement")
            };
            assert_eq!(child_failures, []);
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(completion, Completion::Stopped);
            assert_eq!(descendants, []);
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
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host.block_on(Application::with_topology(
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

        let supervisor = application.root().established_recipient();
        let mut caller = application.external::<Capability>()
            .expect("the capability caller is established");
        caller
            .send(
                &supervisor,
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

                let rejection_retirement = async {
            tokio::select! {
                replacement = activations.recv() => {
                    panic!("a rejected second role cannot activate a replacement: {replacement:?}")
                }
                termination = application.termination() => termination,
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
    let terminal = retirement;

    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared.lock().unwrap_or_else(PoisonError::into_inner),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_rejected_preparation_retirement(origin, terminal);
}

#[allow(
    clippy::too_many_lines,
    reason = "one complete ordered supervisor tree trace checks exact terminal diagnostics and every retained child cause"
)]
fn assert_rejected_preparation_retirement(
    origin: RootOrigin<RootSupervisor>,
    terminal: ActorRetirement<RootSupervisor, supervisor_application::RootRetirements<RootSupervisor>, Vec<<supervisor_application::RootRetirements<RootSupervisor> as bombay::RetirementSchema>::Failure>>,
) {
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
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
        let supervisor_application::RootRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            ..
        } = *terminal
        else {
            panic!("the terminal diagnostic cancels each proxy's owner task")
        };
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(child_failures.is_empty());
        assert_eq!(descendants.len(), 1);
        let supervisor_application::ProxyRetirements {
            origin,
            retirement: terminal,
        } = descendants
            .into_iter()
            .next()
            .expect("only the original worker belongs to this proxy");
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let retirement = match *terminal {
            ActorRetirement::Completed {
                custody:
                    ActorCustody {
                        child_failures,
                        capability_failures,
                        unread_owner_cancellation,
                        descendants,
                        ..
                    },
                completion,
                ..
            } => {
                assert_eq!(child_failures, []);
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(completion, Completion::Stopped);
                assert_eq!(descendants, []);
                WorkerRetirement::Completed
            }
            ActorRetirement::OwnerCancelled {
                custody:
                    ActorCustody {
                        child_failures,
                        capability_failures,
                        unread_owner_cancellation,
                        descendants,
                        ..
                    },
                ..
            } => {
                assert_eq!(child_failures, []);
                assert!(capability_failures.is_empty());
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(descendants, []);
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
    let spaces = supervisor_application::Topology::new();
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let outcome = host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
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
                    let sibling_spaces = supervisor_application::Topology::new();
                    // This is a separately owned Application on the same host, not a sibling child or networking session.
                    Application::with_topology(
                        sibling_supervisor.stop_on_shutdown(),
                        sibling_spaces,
                    )
                    .run_with(move |sibling| async move {
                        let first = sibling_activations.recv().await.unwrap();
                        let second = sibling_activations.recv().await.unwrap();
                        assert_ne!(first, second);
                        let sibling_address = sibling.root().address();
                        let interface = sibling.interface(sibling.root().established_recipient());
                        let mut service = interface.external::<Capability>().unwrap();
                        service
                            .send(
                                interface.api(),
                                FixedCommand::capability(WorkerRole::Primary, service.recipient()),
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
                },
            ),
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
    let mut target = (target_origin, target_native);
    let mut sibling = (sibling_origin, sibling_native);
    for (_, terminal) in [&mut target, &mut sibling] {
        let (descendants, failures) = assert_joined_native(terminal);
        assert!(failures.is_empty());
        assert_eq!(descendants.len(), 2);
        for proxy in descendants {
            let terminal = proxy.retirement.as_ref();
            let (workers, failures) = assert_joined_native(terminal);
            assert!(failures.is_empty());
            assert_eq!(workers.len(), 1);
            for worker in workers {
                let terminal = worker.retirement.as_ref();
                let (descendants, failures) = assert_joined_native(terminal);
                assert_eq!(descendants, []);
                assert_eq!(failures, []);
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
        assert!(matches!(&final_actions.become_, Step::Stop(Stopped)));
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
    let ActorRetirement::Completed {
        custody: ActorCustody { control, .. },
        ..
    } = &target.1
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

fn assert_joined_native<B, Descendants>(
    retirement: &ActorRetirement<B, Descendants, Vec<Descendants::Failure>>,
) -> (&[Descendants], &[Descendants::Failure])
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Descendants: RetirementSchema,
{
    let (ActorRetirement::Completed {
        interpretation,
        source,
        custody:
            ActorCustody {
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
            },
        completion: Completion::Stopped,
        ..
    }
    | ActorRetirement::OwnerCancelled {
        interpretation,
        source,
        custody:
            ActorCustody {
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
            },
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

// This target is the actual supervisor, not the Counter or a worker proxy.
type PreparationExports = MessageProtocol<MailAddr, [EstablishedActor<RootSupervisor>; 2]>;
type PreparationCreations = <Births<RootSupervisor> as CreationSettlements<MailAddr>>::Settlements;

enum PreparationCommand {
    Export(EstablishedRecipient<PreparationExports>),
}
struct PreparationOwner {
    supervisors: Vec<RootSupervisor>,
    creations: Vec<PreparationCreations>,
    established: Vec<EstablishedActor<RootSupervisor>>,
}
impl Protocol for PreparationOwner {
    type Addr = MailAddr;
    type Msg = PreparationCommand;
}
impl BehaviorBase for PreparationOwner {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
impl Behavior for PreparationOwner {
    type Protocol = Self;
    type Event = CreationEvent<MailAddr, RootSupervisor, PreparationCommand>;
    type Sends = Vec<EstablishedDelivery<PreparationExports>>;
    type Birth = Births<RootSupervisor>;
    type Error = Never;
    type Ph = Never;
    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let mut creations = Creations::empty();
        for supervisor in self.supervisors.drain(..) {
            let id = sequence.issue().expect("two original child requests fit");
            creations = creations.and(CreateChild::birth(id, supervisor));
        }
        Ok(Actions::create(creations))
    }
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            CreationEvent::Settlements(settled) => {
                let original = settled.into_settlement();
                let CreationSettlement::Settled(children) = &original else {
                    panic!("the original child traversal settles")
                };
                for child in children {
                    let SettledItem::Attempted(ItemSettlement::Accepted(
                        ChildCreationOutcome::Established(committed),
                    )) = child
                    else {
                        panic!("the actual supervisors establish")
                    };
                    self.established.push(committed.actor());
                }
                self.creations.push(original);
                Ok(Actions::cont())
            }
            CreationEvent::User(user) => {
                let PreparationCommand::Export(reply) = user.message;
                let [target, sibling] = self.established.as_slice() else {
                    panic!("both original supervisor capabilities exist")
                };
                Ok(Actions::send(vec![EstablishedDelivery::new(
                    reply,
                    [target.clone(), sibling.clone()],
                )]))
            }
        }
    }
}
bombay::topology! {
    preparation_application {
        root: PreparationOwner { Supervisor => supervisor; }
        supervisor { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
        hosts { status: Status; capability: Capability; exports: PreparationExports; }
    }
}
#[derive(Default)]
struct SupervisorService {
    target: Option<InstalledActor<RootSupervisor>>,
    endpoint: Option<ActorRef<<RootSupervisor as Behavior>::Protocol>>,
}
impl InterpretInstalledActor<RootSupervisor> for SupervisorService {
    type Output = Option<InstalledActor<RootSupervisor>>;
    fn interpret_actor(&mut self, installed: InstalledActor<RootSupervisor>) -> Self::Output {
        self.target.replace(installed)
    }
}
impl InterpretEstablished<<RootSupervisor as Behavior>::Protocol> for SupervisorService {
    type Output = Option<ActorRef<<RootSupervisor as Behavior>::Protocol>>;
    fn interpret_established(
        &mut self,
        endpoint: ActorRef<<RootSupervisor as Behavior>::Protocol>,
    ) -> Self::Output {
        self.endpoint.replace(endpoint)
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep held child work, useful parent and sibling, and all original native receiving in one temporal trace"
)]
fn joined_child_retirement_waits_for_owned_preparation_while_parent_and_sibling_progress() {
    let prepared = Arc::new(Mutex::new(Vec::new()));
    let original_source = Arc::downgrade(&prepared);
    let resource = Arc::new(Semaphore::new(1));
    let resource_observation = resource.clone();
    let permit = resource
        .try_acquire_owned()
        .expect("one actual owned resource");
    let (reached, preparation_started) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (target_activated, mut target_activations) = mpsc::unbounded_channel();
    let (sibling_activated, mut sibling_activations) = mpsc::unbounded_channel();
    let target = prepared_supervisor(
        prepared,
        target_activated,
        PreparationPolicy::SubmitBoth,
        Some(PreparationRelease {
            reached,
            release: released,
            resource: permit,
        }),
    )
    .stop_on_shutdown();
    let sibling = prepared_supervisor(
        Arc::new(Mutex::new(Vec::new())),
        sibling_activated,
        PreparationPolicy::SubmitBoth,
        None,
    )
    .stop_on_shutdown();
    let owner = PreparationOwner {
        supervisors: vec![target, sibling],
        creations: Vec::new(),
        established: Vec::new(),
    };
    let spaces = preparation_application::Topology::new();
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let originals = host
        .block_on(
            Application::with_topology(owner.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    let activations = (
                        target_activations.recv().await,
                        target_activations.recv().await,
                        sibling_activations.recv().await,
                        sibling_activations.recv().await,
                    );
                    let interface = application.interface(());
                    let mut exports = interface.external::<PreparationExports>().unwrap();
                    let parent = application.root().established_recipient();
                    let parent_address = application.root().address();
                    let first_export = exports
                        .send(&parent, PreparationCommand::Export(exports.recipient()))
                        .await;
                    let issued = exports
                        .receive()
                        .await
                        .expect("actual creator Actions return both proofs");
                    let issued_from = issued.from;
                    let [target, sibling] = issued.message;
                    let mut service = SupervisorService::default();
                    let previous_endpoint = target.recipient().interpret(&mut service);
                    let previous_target = target.clone().interpret_actor(&mut service);
                    let endpoint = service
                        .endpoint
                        .as_ref()
                        .expect("exact ordinary termination endpoint");
                    let target_address = endpoint.address();
                    let installed = service
                        .target
                        .as_ref()
                        .expect("exact child stop and report capability");
                    let mut caller = interface.external::<Capability>().unwrap();
                    let target_request = caller
                        .send(
                            &target.recipient(),
                            FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
                        )
                        .await;
                    let target_capability = caller
                        .receive()
                        .await
                        .expect("the actual target worker capability");
                    let CapabilityResult::Ready { proxy, .. } = &target_capability.message else {
                        panic!("the actual target has a worker")
                    };
                    let recovery = caller.send(proxy, WorkerCommand::Stop).await;
                    let reached = preparation_started.await;
                    let shutdown =
                        installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
                    let termination = endpoint.termination().await;
                    let mut independent = Box::pin(installed.retirement());
                    let mut observation = Box::pin(installed.retirement());
                    let initial_report = {
                        let mut borrowing = Box::pin(async { independent.as_mut().await });
                        borrowing
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                    };
                    let initial_permits = resource_observation.available_permits();
                    let sibling_request = caller
                        .send(
                            &sibling.recipient(),
                            FixedCommand::capability(WorkerRole::Primary, caller.recipient()),
                        )
                        .await;
                    let sibling_reply = caller
                        .receive()
                        .await
                        .expect("the live sibling performs useful work");
                    let parent_request = exports
                        .send(&parent, PreparationCommand::Export(exports.recipient()))
                        .await;
                    let parent_reply = exports
                        .receive()
                        .await
                        .expect("the live parent exports its retained children");
                    let pending_report = observation
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()));
                    let pending_permits = resource_observation.available_permits();
                    let released = release.send(());
                    let report = match pending_report {
                        Poll::Ready(report) => report,
                        Poll::Pending => observation.await,
                    };
                    let replay = match initial_report {
                        Poll::Ready(report) => report,
                        Poll::Pending => independent.await,
                    };
                    let settled_permits = resource_observation.available_permits();
                    let sibling_after_request = caller
                        .send(
                            &sibling.recipient(),
                            FixedCommand::capability(WorkerRole::Secondary, caller.recipient()),
                        )
                        .await;
                    let sibling_after = caller
                        .receive()
                        .await
                        .expect("the same sibling continues after A joins");
                    let parent_shutdown = application.lifecycle().request_shutdown();
                    (
                        activations,
                        first_export,
                        issued_from,
                        previous_endpoint,
                        previous_target,
                        target_address,
                        parent_address,
                        target_request,
                        target_capability,
                        recovery,
                        reached,
                        shutdown,
                        termination,
                        initial_report,
                        initial_permits,
                        sibling_request,
                        sibling_reply,
                        parent_request,
                        parent_reply,
                        pending_report,
                        pending_permits,
                        released,
                        report,
                        replay,
                        settled_permits,
                        sibling_after_request,
                        sibling_after,
                        parent_shutdown,
                    )
                },
            ),
        )
        .unwrap_or_else(|_| panic!("the actual public Application starts"));
    let (
        ApplicationOutcome::Completed {
            output,
            cleanup: Ok(()),
        },
        Ok((origin, native)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = originals
    else {
        panic!("all original native and notification products remain independently owned")
    };
    let (
        activations,
        first_export,
        issued_from,
        previous_endpoint,
        previous_target,
        target_address,
        parent_address,
        target_request,
        target_capability,
        recovery,
        reached,
        shutdown,
        termination,
        initial_report,
        initial_permits,
        sibling_request,
        sibling_reply,
        parent_request,
        parent_reply,
        pending_report,
        pending_permits,
        released,
        report,
        replay,
        settled_permits,
        sibling_after_request,
        sibling_after,
        parent_shutdown,
    ) = output;
    let (target_first, target_second, sibling_first, sibling_second) = activations;
    assert!(target_first.is_some() && target_second.is_some());
    assert_ne!(target_first, target_second);
    assert!(sibling_first.is_some() && sibling_second.is_some());
    assert_ne!(sibling_first, sibling_second);
    assert!(first_export.is_ok());
    assert_eq!(issued_from, parent_address);
    assert!(previous_endpoint.is_none() && previous_target.is_none());
    assert!(target_request.is_ok() && recovery.is_ok() && reached.is_ok() && shutdown.is_ok());
    assert_eq!(target_capability.from, target_address);
    assert_eq!(termination, Ok(Exit::Normal));
    assert!(
        matches!(initial_report, Poll::Pending),
        "joined child report precedes no unfinished owned preparation"
    );
    assert!(
        matches!(pending_report, Poll::Pending),
        "joined child report stays absent across parent and sibling progress"
    );
    assert_eq!((initial_permits, pending_permits), (0, 0));
    assert!(sibling_request.is_ok() && parent_request.is_ok());
    assert_ne!(sibling_reply.from, target_address);
    assert_eq!(parent_reply.from, parent_address);
    let [parent_target, parent_sibling] = parent_reply.message;
    let mut sibling_endpoint = SupervisorService::default();
    let original_sibling = parent_sibling.recipient().interpret(&mut sibling_endpoint);
    assert!(original_sibling.is_none());
    assert_eq!(
        sibling_reply.from,
        sibling_endpoint.endpoint.as_ref().unwrap().address()
    );
    let mut target_endpoint = SupervisorService::default();
    let original_target = parent_target.recipient().interpret(&mut target_endpoint);
    assert!(original_target.is_none());
    assert_eq!(
        target_address,
        target_endpoint.endpoint.as_ref().unwrap().address()
    );
    assert!(released.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(report, replay);
    assert_eq!(settled_permits, 1);
    assert!(sibling_after_request.is_ok() && parent_shutdown.is_ok());
    assert_eq!(sibling_after.from, sibling_reply.from);
    for (reply, expected) in [
        (&target_capability, WorkerRole::Primary),
        (&sibling_reply, WorkerRole::Primary),
        (&sibling_after, WorkerRole::Secondary),
    ] {
        let CapabilityResult::Ready { role, .. } = &reply.message else {
            panic!("actual useful reply")
        };
        assert_eq!(*role, expected);
    }
    let parent = (origin, native);
    let (origin, terminal) = &parent;
    assert_eq!(origin.address(), parent_address);
    let (children, failures) = assert_joined_native(terminal);
    assert!(failures.is_empty());
    assert_eq!(children.len(), 2);
    let ActorRetirement::Completed {
        custody: ActorCustody { behavior, .. },
        ..
    } = terminal
    else {
        panic!("parent normal stop")
    };
    assert_eq!(behavior.base().creations.len(), 1);
    assert_eq!(behavior.base().established.len(), 2);
    assert!(behavior.base().supervisors.is_empty());
    let mut target_retirements = 0;
    for child in children {
        let preparation_application::RootRetirements { origin, retirement } = child;
        let terminal = retirement.as_ref();
        if origin.address() == target_address {
            target_retirements += 1;
            let ActorRetirement::Completed {
                custody: ActorCustody { control, .. },
                completion,
                ..
            } = terminal
            else {
                panic!("A retains its genuine normally completed native product")
            };
            assert_eq!(*completion, Completion::Stopped);
            assert_eq!(control.len(), 1);
            assert!(
                matches!(
                    &control[0],
                    EventLayer::Inner(FixedSupervisorEvent::WorkerPreparationReturned(_))
                ),
                "the held preparation returns its original late control into A's native product"
            );
        }
        let (proxies, failures) = assert_joined_native(terminal);
        assert!(failures.is_empty());
        assert_eq!(proxies.len(), 2);
        for proxy in proxies {
            let terminal = proxy.retirement.as_ref();
            let (workers, failures) = assert_joined_native(terminal);
            assert!(failures.is_empty());
            assert_eq!(workers.len(), 1);
            for worker in workers {
                let terminal = worker.retirement.as_ref();
                let (descendants, failures) = assert_joined_native(terminal);
                assert_eq!(descendants, []);
                assert_eq!(failures, []);
            }
        }
    }
    assert_eq!(target_retirements, 1);
    assert_eq!(original_source.strong_count(), 1);
    let source = original_source
        .upgrade()
        .expect("original prepared source retained natively");
    let prepared = source.lock().unwrap();
    assert_eq!(*prepared, [WorkerRole::Primary, WorkerRole::Secondary]);
    drop(prepared);
    drop(source);
    drop(parent);
    assert_eq!(original_source.strong_count(), 0);
}
