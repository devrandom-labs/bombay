use bombay::ApplicationOutcome;
use core::convert::Infallible;
use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Builder;

use behavior_actors::atomic::{
    ActivationPolicy, ActorDrainPolicy, AdmissionRejection, AssignedReturnReason, Assignment,
    BacklogCapacity, DiagnosticDisposition, FifoCommand, FifoOutcome, FifoPool,
    ImmediateActivation, Interruption, OrderedRoles, PoolFailureReaction, PoolRecovery,
    RestartLimit, RestartRelease, SubmissionId, WorkerSource, WorkerSubmission, fifo, pool_worker,
};
use behavior_actors::{Crash, Exit, StopOnShutdown};
use bombay::ProjectTerminal;
use bombay::behavior::{Actions, ChildHead, MessageProtocol, Never};
use bombay::prelude::{
    ActorRetirement, ChildFailure, ChildOrigin, Completion, MailAddr, RootOrigin,
    TerminalProjection,
};
use bombay::{ActorSpace, ActorSpaces, App, WorkerPreparationSource, WorkerPreparationStart};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerRole {
    Primary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerAvailability {
    RetireAfterAssignment,
    RetireWithoutCompletion,
    RemainAvailable,
}

struct RecoverableWorker {
    availability: WorkerAvailability,
}

#[pool_worker(addr = MailAddr, result = u16)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the worker's pure transition uses the owning Behavior result contract"
)]
impl RecoverableWorker {
    fn transition(&mut self, assignment: Assignment<u8>) -> WorkerActed<Self> {
        match self.availability {
            WorkerAvailability::RetireAfterAssignment => {
                let result = u16::from(*assignment.payload());
                Ok(Actions::stop().with_send(assignment.complete(result)))
            }
            WorkerAvailability::RetireWithoutCompletion => Ok(Actions::stop()),
            WorkerAvailability::RemainAvailable => {
                let result = u16::from(*assignment.payload());
                Ok(Actions::cont().with_send(assignment.complete(result)))
            }
        }
    }
}

struct RecoveryWorkshop {
    preparations: Arc<AtomicUsize>,
    prepared: Arc<Notify>,
    release: PreparationRelease,
}

enum PreparationRelease {
    Immediate,
    Await(oneshot::Receiver<()>),
    Panic,
}

impl WorkerSource<WorkerRole, RecoverableWorker, ImmediateActivation> for RecoveryWorkshop {
    type WorkerRejection = Never;
    type SourceRejection = Never;
}

impl WorkerPreparationSource<WorkerRole, RecoverableWorker, ImmediateActivation>
    for RecoveryWorkshop
{
    async fn prepare_first(
        &mut self,
        role: &WorkerRole,
    ) -> WorkerPreparationStart<RecoverableWorker, ImmediateActivation, Never, Never> {
        assert_eq!(*role, WorkerRole::Primary);
        self.preparations.fetch_add(1, Ordering::SeqCst);
        self.prepared.notify_one();
        match &mut self.release {
            PreparationRelease::Immediate => {}
            PreparationRelease::Await(release) => {
                release.await.expect("the test releases worker preparation");
            }
            PreparationRelease::Panic => panic!("the replacement source failed"),
        }
        WorkerPreparationStart::Submitted(WorkerSubmission::immediate(RecoverableWorker {
            availability: WorkerAvailability::RemainAvailable,
        }))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn prepare_next(
        &mut self,
        _: &WorkerRole,
    ) -> Result<WorkerSubmission<RecoverableWorker, ImmediateActivation>, Never> {
        panic!("one role cannot request another worker in the same preparation")
    }
}

type Pool = FifoPool<
    WorkerRole,
    RecoverableWorker,
    ImmediateActivation,
    RecoveryWorkshop,
    Infallible,
    u8,
    u16,
>;
type RootProtocol = MessageProtocol<MailAddr, FifoCommand<MailAddr, WorkerRole, u8, u16>>;
type WorkerProtocol = MessageProtocol<MailAddr, Assignment<u8>>;
type CustomerProtocol = MessageProtocol<MailAddr, FifoOutcome<WorkerRole, u8, u16>>;

#[derive(ActorSpaces)]
struct RecoverySpaces {
    #[actor_space(RootProtocol)]
    root: ActorSpace<RootProtocol>,
    #[actor_space(WorkerProtocol)]
    workers: ActorSpace<WorkerProtocol>,
    #[actor_space(CustomerProtocol)]
    customers: ActorSpace<CustomerProtocol>,
}

#[allow(
    clippy::large_enum_variant,
    reason = "the exact worker and pool terminals remain unboxed for caller custody"
)]
#[derive(TerminalProjection)]
enum RecoveryTerminal {
    Root {
        origin: RootOrigin<Pool>,
        #[expect(
            clippy::type_complexity,
            reason = "the pool retirement retains exact worker origins and complete child failures"
        )]
        terminal: ActorRetirement<
            Pool,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<Pool, ChildHead>, StopOnShutdown<RecoverableWorker>>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Worker {
        origin: ChildOrigin<Pool, ChildHead>,
        terminal: ActorRetirement<StopOnShutdown<RecoverableWorker>, Self, ()>,
    },
}

fn recovery_pool(
    preparations: &Arc<AtomicUsize>,
    prepared: &Arc<Notify>,
    initial_availability: WorkerAvailability,
    interruption: Interruption,
    release: PreparationRelease,
) -> Pool {
    let roles =
        OrderedRoles::new(WorkerRole::Primary, []).expect("one role is a valid worker roster");
    fifo(
        move |_: &WorkerRole| {
            Ok::<_, Never>(WorkerSubmission::immediate(RecoverableWorker {
                availability: initial_availability,
            }))
        },
        roles,
        ActivationPolicy::new(1).expect("one activation is valid capacity"),
        PoolRecovery::permanent(
            RecoveryWorkshop {
                preparations: Arc::clone(preparations),
                prepared: Arc::clone(prepared),
                release,
            },
            RestartLimit::new(1, Duration::from_secs(30)),
            RestartRelease::immediate(),
            PoolFailureReaction::RetireRole,
        ),
        BacklogCapacity::new(8),
        interruption,
        ActorDrainPolicy::WaitForActorGraph,
        DiagnosticDisposition::terminate(),
    )
    .unwrap_or_else(|_| panic!("the first worker is prepared"))
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
)]
fn fifo_pool_prepares_one_replacement_and_drains_both_workers() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Notify::new());
    let pool = recovery_pool(
        &preparations,
        &prepared,
        WorkerAvailability::RetireAfterAssignment,
        Interruption::Retry,
        PreparationRelease::Immediate,
    );
    let spaces = RecoverySpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let observed_preparations = Arc::clone(&preparations);

    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(pool, spaces).run_with::<RecoveryTerminal, _, _, _>(
                move |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface
                        .external::<CustomerProtocol>()
                        .expect("the pool caller is established");
                    let customer = caller.recipient();
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(7), 3, customer.clone()),
                        )
                        .await
                        .expect("the first job reaches the pool");
                    let first_accepted =
                        tokio::time::timeout(Duration::from_secs(5), caller.receive())
                            .await
                            .expect("the pool admits the first job")
                            .expect("the pool reports first admission")
                            .message;
                    let (first_id, first_job) = first_accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the first outcome accepts the first job"));
                    assert_eq!(first_id, SubmissionId::new(7));
                    let first_completed =
                        tokio::time::timeout(Duration::from_secs(5), caller.receive())
                            .await
                            .expect("the retiring worker completes the first job")
                            .expect("the worker reports first completion")
                            .message;
                    assert_eq!(first_completed.role(), Some(&WorkerRole::Primary));
                    let (completed_job, first_result) = first_completed
                        .into_completed()
                        .unwrap_or_else(|_| panic!("the second outcome completes the first job"));
                    assert_eq!(completed_job, first_job);
                    assert_eq!(first_result, 3);

                    tokio::time::timeout(Duration::from_secs(5), prepared.notified())
                        .await
                        .expect("the pool requests its replacement source");
                    assert_eq!(observed_preparations.load(Ordering::SeqCst), 1);
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(8), 4, customer),
                        )
                        .await
                        .expect("the second job reaches the pool");
                    let second_accepted =
                        tokio::time::timeout(Duration::from_secs(5), caller.receive())
                            .await
                            .expect("the pool admits the second job")
                            .expect("the pool reports second admission")
                            .message;
                    let (second_id, second_job) = second_accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the third outcome accepts the second job"));
                    assert_eq!(second_id, SubmissionId::new(8));
                    let second_completed =
                        tokio::time::timeout(Duration::from_secs(5), caller.receive())
                            .await
                            .expect("the replacement completes the second job")
                            .expect("the worker reports second completion")
                            .message;
                    assert_eq!(second_completed.role(), Some(&WorkerRole::Primary));
                    let (completed_job, second_result) = second_completed
                        .into_completed()
                        .unwrap_or_else(|_| panic!("the fourth outcome completes the second job"));
                    assert_eq!(completed_job, second_job);
                    assert_eq!(second_result, 4);
                    caller
                        .send(interface.api(), FifoCommand::shutdown())
                        .await
                        .expect("the pool receives orderly shutdown");
                    let lifecycle = application.lifecycle();
                    tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                        .await
                        .expect("the pool drains after replacement")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool runs both workers and shuts down");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RecoveryTerminal = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_recovered_pool_terminal(terminal);
}

#[test]
fn fifo_pool_retries_the_exact_assigned_job_after_worker_stop() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Notify::new());
    let pool = recovery_pool(
        &preparations,
        &prepared,
        WorkerAvailability::RetireWithoutCompletion,
        Interruption::Retry,
        PreparationRelease::Immediate,
    );
    let spaces = RecoverySpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let observed_preparations = Arc::clone(&preparations);
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(pool, spaces).run_with::<RecoveryTerminal, _, _, _>(
                move |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface
                        .external::<CustomerProtocol>()
                        .expect("the retry customer is established");
                    let customer = caller.recipient();
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(9), 5, customer),
                        )
                        .await
                        .expect("the interrupted job reaches the pool");
                    let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the interrupted job is admitted")
                        .expect("the pool reports admission")
                        .message;
                    let (submission, job) = accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the first customer outcome admits the job"));
                    assert_eq!(submission, SubmissionId::new(9));

                    tokio::time::timeout(Duration::from_secs(5), prepared.notified())
                        .await
                        .expect("the stopped worker triggers one preparation");
                    assert_eq!(observed_preparations.load(Ordering::SeqCst), 1);
                    let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the replacement completes the interrupted job")
                        .expect("the customer receives the retried result")
                        .message;
                    assert_eq!(completed.role(), Some(&WorkerRole::Primary));
                    let (completed_job, result) = completed.into_completed().unwrap_or_else(|_| {
                        panic!("the retry completes without returning the job")
                    });
                    assert_eq!(completed_job, job);
                    assert_eq!(result, 5);
                    caller
                        .send(interface.api(), FifoCommand::shutdown())
                        .await
                        .expect("the pool receives orderly shutdown after retry");
                    let lifecycle = application.lifecycle();
                    tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                        .await
                        .expect("the pool drains both workers after retry")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the interrupted job completes after replacement");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RecoveryTerminal = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_recovered_pool_terminal(terminal);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
)]
fn fifo_pool_returns_the_assigned_payload_when_interruption_fails() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Notify::new());
    let pool = recovery_pool(
        &preparations,
        &prepared,
        WorkerAvailability::RetireWithoutCompletion,
        Interruption::Fail,
        PreparationRelease::Immediate,
    );
    let spaces = RecoverySpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let observed_preparations = Arc::clone(&preparations);
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(pool, spaces).run_with::<RecoveryTerminal, _, _, _>(
                move |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface
                        .external::<CustomerProtocol>()
                        .expect("the interrupted customer is established");
                    let customer = caller.recipient();
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(10), 6, customer.clone()),
                        )
                        .await
                        .expect("the first job reaches the pool");
                    let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the interrupted job is admitted")
                        .expect("the pool reports admission")
                        .message;
                    let (submission, job) = accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the first outcome admits the interrupted job"));
                    assert_eq!(submission, SubmissionId::new(10));
                    let returned = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the stopped worker returns its assigned job")
                        .expect("the pool reports the interrupted assignment")
                        .message;
                    assert_eq!(returned.role(), Some(&WorkerRole::Primary));
                    let (returned_job, payload, reason) = returned
                        .into_returned_assigned()
                        .unwrap_or_else(|_| panic!("the pool returns the exact assigned job"));
                    assert_eq!(returned_job, job);
                    assert_eq!(payload, 6);
                    assert_eq!(reason, AssignedReturnReason::WorkerStopped);

                    tokio::time::timeout(Duration::from_secs(5), prepared.notified())
                        .await
                        .expect("the source prepares a replacement after interruption");
                    assert_eq!(observed_preparations.load(Ordering::SeqCst), 1);
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(11), 7, customer),
                        )
                        .await
                        .expect("the replacement receives another job");
                    let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the second job is admitted")
                        .expect("the pool reports second admission")
                        .message;
                    let (submission, job) = accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the third outcome admits the second job"));
                    assert_eq!(submission, SubmissionId::new(11));
                    let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the replacement completes its job")
                        .expect("the customer receives a completed result")
                        .message;
                    assert_eq!(completed.role(), Some(&WorkerRole::Primary));
                    let (completed_job, result) = completed
                        .into_completed()
                        .unwrap_or_else(|_| panic!("the fourth outcome completes the second job"));
                    assert_eq!(completed_job, job);
                    assert_eq!(result, 7);
                    caller
                        .send(interface.api(), FifoCommand::shutdown())
                        .await
                        .expect("the pool receives orderly shutdown");
                    let lifecycle = application.lifecycle();
                    tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                        .await
                        .expect("the pool drains after the failed assignment")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool returns the failed job and drains");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RecoveryTerminal = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_recovered_pool_terminal(terminal);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one held-source shutdown controller completes cleanup before the full replacement and retirement oracles"
)]
fn shutdown_while_worker_source_is_held_avoids_replacement() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Notify::new());
    let (release_preparation, release) = oneshot::channel();
    let pool = recovery_pool(
        &preparations,
        &prepared,
        WorkerAvailability::RetireAfterAssignment,
        Interruption::Retry,
        PreparationRelease::Await(release),
    );
    let spaces = RecoverySpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(pool, spaces).run_with::<RecoveryTerminal, _, _, _>(
                move |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface
                        .external::<CustomerProtocol>()
                        .expect("the customer is established");
                    let customer = caller.recipient();
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(12), 8, customer.clone()),
                        )
                        .await
                        .expect("the first job reaches the pool");
                    let accepted = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the job is admitted")
                        .expect("the pool reports admission")
                        .message;
                    let (submission, job) = accepted
                        .into_accepted()
                        .unwrap_or_else(|_| panic!("the first outcome admits the job"));
                    assert_eq!(submission, SubmissionId::new(12));
                    let completed = tokio::time::timeout(Duration::from_secs(5), caller.receive())
                        .await
                        .expect("the first worker completes its job")
                        .expect("the customer receives completion")
                        .message;
                    let (completed_job, result) = completed
                        .into_completed()
                        .unwrap_or_else(|_| panic!("the completed outcome retains the job"));
                    assert_eq!(completed_job, job);
                    assert_eq!(result, 8);
                    tokio::time::timeout(Duration::from_secs(5), prepared.notified())
                        .await
                        .expect("the source begins replacement preparation");
                    caller
                        .send(interface.api(), FifoCommand::shutdown())
                        .await
                        .expect("shutdown is admitted while preparation is held");
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(13), 9, customer),
                        )
                        .await
                        .expect("the later job is admitted while preparation is held");
                    let shutdown_response =
                        tokio::time::timeout(Duration::from_secs(1), caller.receive()).await;
                    release_preparation
                        .send(())
                        .unwrap_or_else(|()| panic!("the source still awaits release"));
                    let rejected = shutdown_response
                        .expect("shutdown must fold while preparation is held")
                        .expect("the pool returns the later job")
                        .message;
                    let (submission, payload, reason) = rejected
                        .into_rejected()
                        .unwrap_or_else(|_| panic!("the later job is rejected after shutdown"));
                    assert_eq!(submission, SubmissionId::new(13));
                    assert_eq!(payload, 9);
                    assert_eq!(reason, AdmissionRejection::ShuttingDown);
                    let lifecycle = application.lifecycle();
                    tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                        .await
                        .expect("the pool finishes shutdown after the source returns")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool drains after held preparation");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RecoveryTerminal = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    let RecoveryTerminal::Root { terminal, .. } = terminal else {
        panic!("the root returns its terminal");
    };
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        descendants,
        behavior: _,
        settlements: _,
        control: _,
        user: _,
        completion: _,
        interpretation: retirement_interpretation,
        source: retirement_source,
        additional_failures: retirement_additional_failures,
        received_interpretation: retirement_received_interpretation,
        received_source: retirement_received_source,
        source_index: retirement_source_index,
        acquired_ingress: retirement_acquired_ingress,
        retirement_failures: retirement_native_failures,
        terminal_report: retirement_terminal_report,
    } = terminal
    else {
        panic!("the root completes its drain");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(descendants.len(), 1, "shutdown must suppress replacement");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the complete ordered ownership trace in one observable regression; test length does not justify a new abstraction"
)]
fn source_task_failure_terminates_the_active_pool() {
    let preparations = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(Notify::new());
    let pool = recovery_pool(
        &preparations,
        &prepared,
        WorkerAvailability::RetireAfterAssignment,
        Interruption::Retry,
        PreparationRelease::Panic,
    );
    let spaces = RecoverySpaces {
        root: ActorSpace::new(),
        workers: ActorSpace::new(),
        customers: ActorSpace::new(),
    };
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            App::new(pool, spaces).run_with::<RecoveryTerminal, _, _, _>(
                move |application| async move {
                    let interface =
                        application.interface(application.root().established_recipient());
                    let mut caller = interface
                        .external::<CustomerProtocol>()
                        .expect("the customer is established");
                    let customer = caller.recipient();
                    caller
                        .send(
                            interface.api(),
                            FifoCommand::submit(SubmissionId::new(41), 8, customer),
                        )
                        .await
                        .expect("the first job reaches the pool");
                    for _ in 0..2 {
                        tokio::time::timeout(Duration::from_secs(5), caller.receive())
                            .await
                            .expect("the first worker reports its job")
                            .expect("the customer receives one outcome");
                    }
                    tokio::time::timeout(Duration::from_secs(5), prepared.notified())
                        .await
                        .expect("the source begins preparation before failing");
                    let lifecycle = application.lifecycle();
                    tokio::time::timeout(Duration::from_secs(5), lifecycle.termination())
                        .await
                        .expect("a failed source task terminates the active pool")
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the pool reports source task failure");
        });
    drop(application_host);
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal: RecoveryTerminal = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(termination, Err(Crash::CapabilityFailed));
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    let RecoveryTerminal::Root { origin, terminal } = terminal else {
        panic!("the failed source returns its root terminal");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::CapabilityFailed {
        child_failures: (child_failures, ()),

        behavior,
        settlements,
        control,
        user,
        descendants,
        error,
        capability_failures,
        unread_owner_cancellation,
        interpretation: retirement_interpretation,
        source: retirement_source,
        additional_failures: retirement_additional_failures,
        received_interpretation: retirement_received_interpretation,
        received_source: retirement_received_source,
        source_index: retirement_source_index,
        acquired_ingress: retirement_acquired_ingress,
        retirement_failures: retirement_native_failures,
        terminal_report: retirement_terminal_report,
    } = terminal
    else {
        panic!("the failed source retains available pool state and the exact capability cause");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(error.is_panic());
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(descendants.len(), 1);
    drop((behavior, settlements, descendants, error));
}

fn assert_recovered_pool_terminal(terminal: RecoveryTerminal) {
    let RecoveryTerminal::Root { origin, terminal } = terminal else {
        panic!("the pool returns its root terminal");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        child_failures: (child_failures, ()),

        capability_failures,
        unread_owner_cancellation,
        descendants,
        completion,
        behavior: _,
        settlements: _,
        control: _,
        user: _,
        interpretation: retirement_interpretation,
        source: retirement_source,
        additional_failures: retirement_additional_failures,
        received_interpretation: retirement_received_interpretation,
        received_source: retirement_received_source,
        source_index: retirement_source_index,
        acquired_ingress: retirement_acquired_ingress,
        retirement_failures: retirement_native_failures,
        terminal_report: retirement_terminal_report,
    } = terminal
    else {
        panic!("the pool completes after its worker graph");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(child_failures.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);
    let mut worker_nonces = Vec::new();
    for descendant in descendants {
        let RecoveryTerminal::Worker { origin, terminal } = descendant else {
            panic!("both descendants are exact workers");
        };
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        worker_nonces.push(origin.nonce());
        let ActorRetirement::Completed {
            child_failures: (),

            capability_failures,
            unread_owner_cancellation,
            completion,
            descendants,
            behavior: _,
            settlements: _,
            control: _,
            user: _,
            interpretation: retirement_interpretation,
            source: retirement_source,
            additional_failures: retirement_additional_failures,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            retirement_failures: retirement_native_failures,
            terminal_report: retirement_terminal_report,
        } = terminal
        else {
            panic!("each worker completes rather than being cancelled");
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_additional_failures.is_empty());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_terminal_report.is_none());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(completion, Completion::Stopped);
        assert!(descendants.is_empty());
    }
    assert_ne!(worker_nonces[0], worker_nonces[1]);
}
