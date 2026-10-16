//! Run one fixed supervisor through worker failure, replacement, and shutdown.
//! The supervisor owns recovery policy; Bombay interprets its typed actions.

use bombay::ActorCustody;
use bombay::ActorNotificationReceipts;
use bombay::WorkOutcome;
use core::time::Duration;
use tokio::runtime::Builder;

use bombay::actors::ActorExt as _;
use bombay::behavior::{Actions, BehaviorActed, BehaviorBase, MessageProtocol, Never};
use bombay::composition::StashRoute;
use bombay::diagnostic::DiagnosticDisposition;
use bombay::lifecycle::ActorDrainPolicy;
use bombay::prelude::{ActorRetirement, Completion, Exit, MailAddr, StopOnShutdown};
use bombay::supervisor::roster::{
    self, CapabilityResult, FailureReaction, FixedCommand, FixedDiagnostic, Recovery,
    RecoveryPolicy, Strategy, WorkerPreparationFailureReason,
};
use bombay::worker::{
    OrderedRoles, RestartLimit, RestartRelease, StartupLimit, StartupPlan, WorkerSubmission,
};
use bombay::{Application, ApplicationScope, WorkerFactory};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
    Secondary,
    Tertiary,
}

enum WorkerCommand {
    Stop,
    Release,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopRelease {
    AwaitingRelease,
    Released,
}
struct Worker {
    release: StopRelease,
}

#[bombay::actor]
impl Worker {
    fn receive(&mut self, message: WorkerCommand) -> BehaviorActed<Self> {
        match message {
            WorkerCommand::Stop => {
                assert_eq!(self.release, StopRelease::Released);
                Ok(Actions::stop())
            }
            WorkerCommand::Release => {
                self.release = StopRelease::Released;
                Ok(Actions::cont())
            }
        }
    }
}

type ManagedWorker = StopOnShutdown<Worker>;

struct ActivationNotice {
    role: WorkerRole,
    activated: mpsc::UnboundedSender<WorkerRole>,
}

impl StartupPlan for ActivationNotice {
    type Ready = ();
    type Rejection = Never;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn start(self) -> Result<Self::Ready, Self::Rejection> {
        self.activated
            .send(self.role)
            .expect("the application observes each worker activation");
        Ok(())
    }
}

fn worker_submission(
    role: WorkerRole,
    activated: &mpsc::UnboundedSender<WorkerRole>,
) -> WorkerSubmission<ManagedWorker, ActivationNotice> {
    WorkerSubmission::with_startup(
        Worker {
            release: StopRelease::Released,
        }
        .stop_on_shutdown(),
        ActivationNotice {
            role,
            activated: activated.clone(),
        },
    )
}

type Capability = MessageProtocol<MailAddr, CapabilityResult<WorkerRole, Worker>>;

bombay::topology! {
    supervisor_application {
        root { Proxy => proxy; }
        proxy { Worker => worker; }
        worker {}
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete replacement and drain trace preserves original incarnation and terminal custody"
)]
fn fixed_factory_preserves_prepared_prefix_and_exact_refusal() {
    let (activated, mut activations) = mpsc::unbounded_channel();
    let scope = ApplicationScope::new();
    let mut diagnostics = scope
        .external::<MessageProtocol<MailAddr, _>>()
        .expect("the diagnostic receiver is established before root assembly");
    let reason = Box::new(vec![269_u64, 271]);
    let reason_pointer = reason.as_ptr();
    let mut reason = Some(reason);
    let prepared_roles = Arc::new(Mutex::new(Vec::new()));
    let source_roles = Arc::clone(&prepared_roles);
    let supervisor = {
        let roles = OrderedRoles::new(
            WorkerRole::Primary,
            [WorkerRole::Secondary, WorkerRole::Tertiary],
        )
        .expect("the one-role roster is distinct");
        let initial_activated = activated.clone();
        roster::new(
            move |role: &WorkerRole| worker_submission(*role, &initial_activated),
            roles,
            roster::Config {
                startup: StartupLimit::new(3).expect("one worker fits the activation policy"),
                recovery: Recovery::permanent(
                    WorkerFactory::new(move |role| {
                        source_roles.lock().expect("preparation roles").push(*role);
                        match role {
                            WorkerRole::Secondary => {
                                Err(reason.take().expect("one exact role refusal"))
                            }
                            WorkerRole::Primary | WorkerRole::Tertiary => {
                                Ok(worker_submission(*role, &activated))
                            }
                        }
                    }),
                    RecoveryPolicy {
                        strategy: Strategy::OneForAll,
                        limit: RestartLimit::new(1, Duration::from_secs(30)),
                        delay: RestartRelease::immediate(),
                    },
                ),
                failure: FailureReaction::StopSupervisor,
                shutdown: ActorDrainPolicy::WaitForActorGraph,
                diagnostics: DiagnosticDisposition::deliver_to(diagnostics.recipient()),
            },
        )
        .build()
        .unwrap_or_else(|_| panic!("the declared worker is prepared"))
    };
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            scope
                .with_topology(supervisor.stop_on_shutdown(), spaces)
                .run_with(move |application| async move {
                    let mut initial_roles = Vec::new();
                    for _ in 0..3 {
                        initial_roles.push(
                            tokio::time::timeout(Duration::from_secs(5), activations.recv())
                                .await
                                .expect("each original worker activates")
                                .expect("the activation stream remains available"),
                        );
                    }
                    for role in [
                        WorkerRole::Primary,
                        WorkerRole::Secondary,
                        WorkerRole::Tertiary,
                    ] {
                        assert_eq!(
                            initial_roles
                                .iter()
                                .filter(|activated| **activated == role)
                                .count(),
                            1
                        );
                    }

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
                        .expect("the first worker accepts its stop command");

                    let diagnostic =
                        tokio::time::timeout(Duration::from_secs(5), diagnostics.receive())
                            .await
                            .expect("the selected recovery reports its role refusal")
                            .expect("the diagnostic endpoint stays available")
                            .message;
                    let FixedDiagnostic::WorkerPreparationFailed(failure) = diagnostic else {
                        panic!("the original recovery failure reaches its typed customer");
                    };
                    assert_eq!(failure.role(), &WorkerRole::Primary);
                    assert_eq!(
                        failure
                            .prepared()
                            .map(|(role, _)| *role)
                            .collect::<Vec<_>>(),
                        [WorkerRole::Primary]
                    );
                    assert_eq!(
                        failure.remaining_roles().copied().collect::<Vec<_>>(),
                        [WorkerRole::Tertiary]
                    );
                    let WorkerPreparationFailureReason::WorkerRejected { role, reason } =
                        failure.reason()
                    else {
                        panic!("role refusal remains distinct from whole-source rejection");
                    };
                    assert_eq!(role, &WorkerRole::Secondary);
                    assert_eq!(reason.as_ptr(), reason_pointer);
                    assert_eq!(**reason, [269, 271]);
                    let termination =
                        tokio::time::timeout(Duration::from_secs(5), application.termination())
                            .await
                            .expect("the supervisor drains every original worker after refusal");
                    (termination, failure)
                }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the supervisor runs its recovery policy");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: (termination, failure),
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        *prepared_roles.lock().expect("preparation roles"),
        [WorkerRole::Primary, WorkerRole::Secondary]
    );
    assert_eq!(
        failure
            .prepared()
            .map(|(role, _)| *role)
            .collect::<Vec<_>>(),
        [WorkerRole::Primary]
    );
    assert_eq!(
        failure.remaining_roles().copied().collect::<Vec<_>>(),
        [WorkerRole::Tertiary]
    );
    let WorkerPreparationFailureReason::WorkerRejected { role, reason } = failure.reason() else {
        panic!("the joined application retains the original role refusal");
    };
    assert_eq!(role, &WorkerRole::Secondary);
    assert_eq!(reason.as_ptr(), reason_pointer);
    assert_eq!(**reason, [269, 271]);
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
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
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 3);
    for descendant in descendants {
        let supervisor_application::RootRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        let terminal = *terminal;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            completion,
            ..
        } = terminal
        else {
            panic!("the proxy returns both worker incarnations")
        };
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 1);
        for descendant in descendants {
            let supervisor_application::ProxyRetirements {
                origin,
                retirement: terminal,
            } = descendant;
            let terminal = *terminal;
            assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
            let ActorRetirement::Completed {
                custody:
                    ActorCustody {
                        capability_failures,
                        unread_owner_cancellation,
                        descendants,
                        ..
                    },
                completion,
                ..
            } = terminal
            else {
                panic!("the worker returns completed terminal custody")
            };
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(completion, Completion::Stopped);
            assert_eq!(descendants.len(), 0);
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete replacement and drain trace preserves original incarnation and terminal custody"
)]
fn fixed_factory_replaces_workers_with_stash_inside_shutdown() {
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor = {
        let roles =
            OrderedRoles::new(WorkerRole::Primary, []).expect("the one-role roster is distinct");
        let initial_activated = activated.clone();
        roster::new(
            move |role: &WorkerRole| {
                WorkerSubmission::with_startup(
                    Worker {
                        release: StopRelease::AwaitingRelease,
                    }
                    .with_stash(|worker, message| match message {
                        WorkerCommand::Stop
                            if worker.base().release == StopRelease::AwaitingRelease =>
                        {
                            StashRoute::Stash
                        }
                        WorkerCommand::Stop => StashRoute::Deliver,
                        WorkerCommand::Release => StashRoute::Release,
                    })
                    .stop_on_shutdown(),
                    ActivationNotice {
                        role: *role,
                        activated: initial_activated.clone(),
                    },
                )
            },
            roles,
            roster::Config {
                startup: StartupLimit::new(1).expect("one worker fits the activation policy"),
                recovery: Recovery::permanent(
                    WorkerFactory::new(move |role| {
                        WorkerSubmission::with_startup(
                            Worker {
                                release: StopRelease::AwaitingRelease,
                            }
                            .with_stash(|worker, message| match message {
                                WorkerCommand::Stop
                                    if worker.base().release == StopRelease::AwaitingRelease =>
                                {
                                    StashRoute::Stash
                                }
                                WorkerCommand::Stop => StashRoute::Deliver,
                                WorkerCommand::Release => StashRoute::Release,
                            })
                            .stop_on_shutdown(),
                            ActivationNotice {
                                role: *role,
                                activated: activated.clone(),
                            },
                        )
                    }),
                    RecoveryPolicy {
                        strategy: Strategy::OneForOne,
                        limit: RestartLimit::new(1, Duration::from_secs(30)),
                        delay: RestartRelease::immediate(),
                    },
                ),
                failure: FailureReaction::StopSupervisor,
                shutdown: ActorDrainPolicy::WaitForActorGraph,
                diagnostics: DiagnosticDisposition::terminate(),
            },
        )
        .build()
        .unwrap_or_else(|_| panic!("the declared worker is prepared"))
    };
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    let first = tokio::time::timeout(Duration::from_secs(5), activations.recv())
                        .await
                        .expect("the first worker activates")
                        .expect("the first activation has a role");
                    assert_eq!(first, WorkerRole::Primary);

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
                        .expect("the first worker accepts its stop command");
                    caller
                        .send(&proxy, WorkerCommand::Release)
                        .await
                        .expect("the worker releases its previously held stop");

                    let replacement =
                        tokio::time::timeout(Duration::from_secs(5), activations.recv())
                            .await
                            .expect("the replacement activates")
                            .expect("the replacement activation has a role");
                    assert_eq!(replacement, WorkerRole::Primary);
                    caller
                        .send(&supervisor, FixedCommand::shutdown())
                        .await
                        .expect("the supervisor accepts shutdown");
                    tokio::time::timeout(Duration::from_secs(5), application.termination())
                        .await
                        .expect("the supervisor retires after replacement and shutdown")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the supervisor runs its recovery policy");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    assert_eq!(termination, Ok(Exit::Normal));
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
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
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let supervisor_application::RootRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the primary role retains its proxy");
    let terminal = *terminal;
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the proxy returns both worker incarnations")
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    for descendant in descendants {
        let supervisor_application::ProxyRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        let terminal = *terminal;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            completion,
            ..
        } = terminal
        else {
            panic!("the worker returns completed terminal custody")
        };
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 0);
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one complete replacement and drain trace preserves original incarnation and terminal custody"
)]
fn fixed_factory_replaces_workers_with_shutdown_inside_stash() {
    let (activated, mut activations) = mpsc::unbounded_channel();
    let supervisor =
        {
            let roles = OrderedRoles::new(WorkerRole::Primary, [])
                .expect("the one-role roster is distinct");
            let initial_activated = activated.clone();
            roster::new(
                move |role: &WorkerRole| {
                    WorkerSubmission::with_startup(
                        Worker {
                            release: StopRelease::AwaitingRelease,
                        }
                        .stop_on_shutdown()
                        .with_stash(|worker, message| match message {
                            WorkerCommand::Stop
                                if worker.base().release == StopRelease::AwaitingRelease =>
                            {
                                StashRoute::Stash
                            }
                            WorkerCommand::Stop => StashRoute::Deliver,
                            WorkerCommand::Release => StashRoute::Release,
                        }),
                        ActivationNotice {
                            role: *role,
                            activated: initial_activated.clone(),
                        },
                    )
                },
                roles,
                roster::Config {
                    startup: StartupLimit::new(1).expect("one worker fits the activation policy"),
                    recovery: Recovery::permanent(
                        WorkerFactory::new(move |role| {
                            WorkerSubmission::with_startup(
                                Worker {
                                    release: StopRelease::AwaitingRelease,
                                }
                                .stop_on_shutdown()
                                .with_stash(|worker, message| match message {
                                    WorkerCommand::Stop
                                        if worker.base().release
                                            == StopRelease::AwaitingRelease =>
                                    {
                                        StashRoute::Stash
                                    }
                                    WorkerCommand::Stop => StashRoute::Deliver,
                                    WorkerCommand::Release => StashRoute::Release,
                                }),
                                ActivationNotice {
                                    role: *role,
                                    activated: activated.clone(),
                                },
                            )
                        }),
                        RecoveryPolicy {
                            strategy: Strategy::OneForOne,
                            limit: RestartLimit::new(1, Duration::from_secs(30)),
                            delay: RestartRelease::immediate(),
                        },
                    ),
                    failure: FailureReaction::StopSupervisor,
                    shutdown: ActorDrainPolicy::WaitForActorGraph,
                    diagnostics: DiagnosticDisposition::terminate(),
                },
            )
            .build()
            .unwrap_or_else(|_| panic!("the declared worker is prepared"))
        };
    let spaces = supervisor_application::Topology::new();
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::with_topology(supervisor.stop_on_shutdown(), spaces).run_with(
                move |application| async move {
                    let first = tokio::time::timeout(Duration::from_secs(5), activations.recv())
                        .await
                        .expect("the first worker activates")
                        .expect("the first activation has a role");
                    assert_eq!(first, WorkerRole::Primary);

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
                        .expect("the first worker accepts its stop command");
                    caller
                        .send(&proxy, WorkerCommand::Release)
                        .await
                        .expect("the worker releases its previously held stop");

                    let replacement =
                        tokio::time::timeout(Duration::from_secs(5), activations.recv())
                            .await
                            .expect("the replacement activates")
                            .expect("the replacement activation has a role");
                    assert_eq!(replacement, WorkerRole::Primary);
                    caller
                        .send(&supervisor, FixedCommand::shutdown())
                        .await
                        .expect("the supervisor accepts shutdown");
                    tokio::time::timeout(Duration::from_secs(5), application.termination())
                        .await
                        .expect("the supervisor retires after replacement and shutdown")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the supervisor runs its recovery policy");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: termination,
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    assert_eq!(termination, Ok(Exit::Normal));
    let origin = root_origin;
    let terminal = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
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
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 1);
    let supervisor_application::RootRetirements {
        origin,
        retirement: terminal,
    } = descendants
        .into_iter()
        .next()
        .expect("the primary role retains its proxy");
    let terminal = *terminal;
    assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                descendants,
                ..
            },
        completion,
        ..
    } = terminal
    else {
        panic!("the proxy returns both worker incarnations")
    };
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    for descendant in descendants {
        let supervisor_application::ProxyRetirements {
            origin,
            retirement: terminal,
        } = descendant;
        let terminal = *terminal;
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    capability_failures,
                    unread_owner_cancellation,
                    descendants,
                    ..
                },
            completion,
            ..
        } = terminal
        else {
            panic!("the worker returns completed terminal custody")
        };
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(descendants.len(), 0);
    }
}

#[test]
fn worker_factory_rejects_invalid_capabilities() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/worker_factory_not_callable.rs");
    cases.compile_fail("tests/compile/fail/worker_factory_wrong_output.rs");
    cases.compile_fail("tests/compile/fail/worker_factory_not_send.rs");
    cases.compile_fail("tests/compile/fail/worker_factory_clone.rs");
    cases.compile_fail("tests/compile/fail/worker_factory_reuse.rs");
    cases.compile_fail("tests/compile/fail/worker_factory_source_refusal.rs");
}
