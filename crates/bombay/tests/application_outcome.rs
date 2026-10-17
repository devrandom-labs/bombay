use bombay::RetirementNotificationError;
use bombay::actors::ActorExt;
use bombay::behavior::{
    AllocationRejection, BehaviorActed, BehaviorBase, ClassifySettlement, Never, SettlementStatus,
};
use bombay::{
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, Application,
    ApplicationCleanupError, RetirementAssessment, WorkOutcome,
};
use core::future::Future;
use core::ptr;
use core::task::{Context, Poll, Waker};
use core::time::Duration;
use std::cell::Cell;
use std::panic::panic_any;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::Wake;
use tokio::sync::oneshot;

#[cfg(feature = "axum")]
use axum::Router;
#[cfg(feature = "axum")]
use std::net::TcpListener;

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use counter::{Counter, CounterMessage, CounterValue};

struct CounterSnapshot {
    value: u64,
    audit: Box<[u8]>,
}

struct RejectedCounter {
    original: CounterSnapshot,
}

enum CounterRefusal {
    Initialization,
}

#[bombay::actor(message = Never, error = CounterRefusal)]
impl RejectedCounter {
    #[allow(
        clippy::unused_self,
        reason = "the owning initialization contract borrows the rejected actor"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Err(CounterRefusal::Initialization)
    }
}

type CounterJourney = WorkOutcome<
    CounterSnapshot,
    CounterSnapshot,
    CounterSnapshot,
    CounterSnapshot,
    CounterSnapshot,
    CounterSnapshot,
    CounterSnapshot,
>;

#[test]
fn completed_work_borrows_then_returns_both_owned_values() {
    let output = CounterSnapshot {
        value: 1,
        audit: vec![2, 3].into_boxed_slice(),
    };
    let cleanup = CounterSnapshot {
        value: 4,
        audit: vec![5, 6].into_boxed_slice(),
    };
    let output_audit = output.audit.as_ptr();
    let cleanup_audit = cleanup.audit.as_ptr();
    let outcome: CounterJourney = WorkOutcome::Completed {
        output,
        cleanup: Ok(cleanup),
    };

    let output = outcome.work_output().expect("work completed");
    let cleanup = outcome
        .cleanup()
        .expect("cleanup was received")
        .as_ref()
        .expect("cleanup succeeded");
    assert_eq!((output.value, output.audit.as_ptr()), (1, output_audit));
    assert_eq!((cleanup.value, cleanup.audit.as_ptr()), (4, cleanup_audit));
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("completed work remains selectable"));
    let cleanup = cleanup.expect("cleanup succeeded independently");
    assert_eq!((output.value, output.audit.as_ptr()), (1, output_audit));
    assert_eq!((cleanup.value, cleanup.audit.as_ptr()), (4, cleanup_audit));
}

#[tokio::test]
async fn completed_work_preserves_cleanup_failure() {
    let (sender, receiver) = oneshot::channel::<()>();
    drop(sender);
    let cause = receiver.await.expect_err("publication closed");
    let outcome: CounterJourney = WorkOutcome::Completed {
        output: CounterSnapshot {
            value: 7,
            audit: vec![8].into_boxed_slice(),
        },
        cleanup: Err(ApplicationCleanupError::PublicationClosed(cause)),
    };
    let output = outcome
        .work_output()
        .expect("work succeeded despite cleanup failure");
    assert_eq!(output.value, 7);
    assert!(matches!(
        outcome.cleanup(),
        Some(Err(ApplicationCleanupError::PublicationClosed(_)))
    ));
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("cleanup failure cannot erase completed work"));
    assert_eq!((output.value, &*output.audit), (7, &[8][..]));
    assert!(matches!(
        cleanup,
        Err(ApplicationCleanupError::PublicationClosed(_))
    ));
}

#[test]
fn selecting_completed_work_returns_every_other_phase_untouched() {
    let phases: [CounterJourney; 5] = [
        WorkOutcome::StagingRejected {
            inputs: CounterSnapshot {
                value: 1,
                audit: vec![11].into_boxed_slice(),
            },
        },
        WorkOutcome::Unstarted {
            application: CounterSnapshot {
                value: 2,
                audit: vec![12].into_boxed_slice(),
            },
            work: CounterSnapshot {
                value: 3,
                audit: vec![13].into_boxed_slice(),
            },
        },
        WorkOutcome::Prepared {
            inputs: CounterSnapshot {
                value: 4,
                audit: vec![14].into_boxed_slice(),
            },
            work: CounterSnapshot {
                value: 5,
                audit: vec![15].into_boxed_slice(),
            },
            cleanup: Ok(CounterSnapshot {
                value: 6,
                audit: vec![16].into_boxed_slice(),
            }),
        },
        WorkOutcome::NotInvoked {
            work: CounterSnapshot {
                value: 7,
                audit: vec![17].into_boxed_slice(),
            },
            startup_error: None,
            cleanup: Ok(CounterSnapshot {
                value: 8,
                audit: vec![18].into_boxed_slice(),
            }),
        },
        WorkOutcome::Interrupted {
            cleanup: Ok(CounterSnapshot {
                value: 9,
                audit: vec![19].into_boxed_slice(),
            }),
        },
    ];
    for phase in phases {
        assert!(phase.work_output().is_none());
        let before_cleanup = phase
            .cleanup()
            .map(|cleanup| cleanup.as_ref().expect("retained cleanup").audit.as_ptr());
        let returned = phase
            .try_into_completed()
            .err()
            .expect("non-completed phase retains all inputs");
        let after_cleanup = returned
            .cleanup()
            .map(|cleanup| cleanup.as_ref().expect("retained cleanup").audit.as_ptr());
        assert_eq!(before_cleanup, after_cleanup);
        let snapshots = match returned {
            WorkOutcome::StagingRejected { inputs } => vec![inputs],
            WorkOutcome::Unstarted { application, work } => vec![application, work],
            WorkOutcome::Prepared {
                inputs,
                work,
                cleanup,
            } => vec![inputs, work, cleanup.expect("prepared cleanup")],
            WorkOutcome::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => {
                assert!(startup_error.is_none());
                vec![work, cleanup.expect("uninvoked cleanup")]
            }
            WorkOutcome::Interrupted { cleanup } => {
                vec![cleanup.expect("interrupted cleanup")]
            }
            WorkOutcome::Completed { .. } => panic!("rejection cannot forge completion"),
        };
        for snapshot in snapshots {
            assert_eq!(
                &*snapshot.audit,
                &[u8::try_from(snapshot.value + 10).expect("small snapshot")]
            );
        }
    }
}

#[tokio::test]
async fn counter_work_and_joined_retirement_remain_independently_owned() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let interface = application.interface(application.root().established_recipient());
            let mut customer = interface
                .external::<CounterValue>()
                .expect("real reply endpoint");
            customer
                .send(interface.api(), CounterMessage::Increment)
                .await
                .expect("increment admitted");
            customer
                .send(interface.api(), CounterMessage::Read(customer.recipient()))
                .await
                .expect("read admitted");
            let reply = customer.receive().await.expect("business reply");
            let shutdown = lifecycle.shutdown();
            assert_eq!(shutdown, Ok(()));
            CounterSnapshot {
                value: reply.message,
                audit: vec![1].into_boxed_slice(),
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the caller entered a host"))
        .into_parts();
    let (outcome, _root_report, native, notifications, ()) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    assert_eq!(outcome.work_output().expect("counter completed").value, 1);
    let (snapshot, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("counter work completed"));
    cleanup.expect("actual application cleanup completes");
    let (origin, retirement) = native.expect("joined root was received");
    assert_eq!(snapshot.value, 1);
    assert_eq!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
    let custody = retirement.custody().expect("joined actor custody survives");
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert!(ptr::eq(
        &raw const custody.behavior,
        retirement.behavior().expect("state retained")
    ));
    let borrowed = retirement
        .behavior()
        .expect("the joined actor remains inspectable");
    let ActorRetirement::Completed {
        custody: ActorCustody { behavior, .. },
        completion,
        ..
    } = &retirement
    else {
        panic!("the counter completed");
    };
    assert!(ptr::eq(borrowed, behavior));
    assert!(ptr::eq(
        retirement.completion().expect("the original completion"),
        completion
    ));
    let ActorRetirement::Completed {
        custody: ActorCustody {
            user, descendants, ..
        },
        settlements,
        completion,
        ..
    } = retirement
    else {
        panic!("complete root retirement remains available");
    };
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(user.is_empty());
    assert_eq!(descendants, []);
    assert_eq!(completion, bombay::Completion::Stopped);
}

#[tokio::test]
async fn startup_rejection_returns_uninvoked_local_work_and_original_actor() {
    let invoked = Rc::new(Cell::new(0));
    let work_invoked = Rc::clone(&invoked);
    let original = CounterSnapshot {
        value: 10,
        audit: vec![20].into_boxed_slice(),
    };
    let original_audit = original.audit.as_ptr();
    let outcome = Application::new(RejectedCounter { original }.stop_on_shutdown())
        .run_with(|_| async move {
            work_invoked.set(work_invoked.get() + 1);
            1_u64
        })
        .await
        .unwrap_or_else(|_| panic!("a host is entered"))
        .into_parts();
    let (outcome, _root_report, native, notifications, ()) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    assert!(outcome.work_output().is_none());
    assert!(matches!(
        &native,
        Ok((
            _,
            ActorRetirement::InitializationRejected {
                error: CounterRefusal::Initialization,
                ..
            }
        ))
    ));
    assert!(matches!(outcome.cleanup(), Some(Ok(()))));
    let custody = native
        .as_ref()
        .expect("startup refusal joined")
        .1
        .custody()
        .expect("refusal retains full custody");
    assert_eq!(
        custody.behavior.base().original.audit.as_ptr(),
        original_audit
    );
    assert_eq!(custody.user, []);
    let (_, retirement) = native.as_ref().expect("startup rejection was joined");
    assert!(retirement.completion().is_none());
    let original = &retirement
        .behavior()
        .expect("rejection retains its actor")
        .base()
        .original;
    assert_eq!(
        (original.value, original.audit.as_ptr()),
        (10, original_audit)
    );
    let returned = outcome
        .try_into_completed()
        .expect_err("startup did not invoke work");
    let WorkOutcome::NotInvoked {
        work,
        startup_error,
        cleanup,
    } = returned
    else {
        panic!("startup retains the real uninvoked phase");
    };
    assert!(startup_error.is_some());
    assert_eq!(invoked.get(), 0);
    cleanup.expect("actual application cleanup completes");
    let (_, retirement) = native.expect("startup rejection was joined");
    let ActorRetirement::InitializationRejected {
        custody: ActorCustody { behavior, .. },
        error: CounterRefusal::Initialization,
        ..
    } = retirement
    else {
        panic!("the exact actor and refusal survive");
    };
    let original = behavior.into_inner().original;
    assert_eq!(
        (original.value, original.audit.as_ptr()),
        (10, original_audit)
    );
    drop(work);
}

#[test]
fn borrowed_actor_preserves_the_complete_allocation_rejection() {
    let original = CounterSnapshot {
        value: 31,
        audit: Box::from([37, 41]),
    };
    let allocation = original.audit.as_ptr();
    let retirement: ActorRetirement<RejectedCounter, Never, ()> =
        ActorRetirement::AllocationRejected {
            behavior: RejectedCounter { original },
            reason: AllocationRejection::AddressAlreadyClaimed,
        };
    assert!(retirement.completion().is_none());
    let borrowed = retirement
        .behavior()
        .expect("allocation rejection retains the original actor");
    assert_eq!(
        (borrowed.original.value, borrowed.original.audit.as_ptr()),
        (31, allocation)
    );
    let ActorRetirement::AllocationRejected { behavior, reason } = retirement else {
        panic!("inspection preserves the complete original rejection");
    };
    assert_eq!(reason, AllocationRejection::AddressAlreadyClaimed);
    assert_eq!(behavior.original.audit.as_ptr(), allocation);
    assert_eq!(&*behavior.original.audit, &[37, 41]);
}

#[tokio::test]
async fn unavailable_actor_state_preserves_each_executor_cause() {
    let failure = tokio::spawn(async { panic!("original actor task failed") })
        .await
        .expect_err("the task's original panic remains native");
    let task = failure.id();
    let retirement: ActorRetirement<RejectedCounter, Never, ()> =
        ActorRetirement::ActorTaskFailed(failure);
    assert!(retirement.completion().is_none());
    assert!(retirement.behavior().is_none());
    let ActorRetirement::ActorTaskFailed(failure) = retirement else {
        panic!("borrowed inspection retains the original task failure");
    };
    assert_eq!(failure.id(), task);
    assert!(failure.is_panic());
    let panicked: ActorRetirement<RejectedCounter, Never, ()> = ActorRetirement::Panicked;
    assert!(panicked.completion().is_none());
    assert!(panicked.behavior().is_none());
    assert!(matches!(panicked, ActorRetirement::Panicked));
    let cancelled: ActorRetirement<RejectedCounter, Never, ()> = ActorRetirement::Cancelled;
    assert!(cancelled.completion().is_none());
    assert!(cancelled.behavior().is_none());
    assert!(matches!(cancelled, ActorRetirement::Cancelled));
}

#[tokio::test(flavor = "current_thread")]
async fn independent_counter_execution_infers_custody_and_keeps_local_work() {
    let original = Rc::new(CounterSnapshot {
        value: 43,
        audit: Box::from([47, 53]),
    });
    let allocation = original.audit.as_ptr();
    let work = Rc::clone(&original);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with(|application| async move {
            let shutdown = application.lifecycle().shutdown();
            assert_eq!(shutdown, Ok(()));
            work
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    execution.await;
    let outcome = receiving.await.into_parts();
    let (outcome, _root_report, native, notifications, ()) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let (returned, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the real local work completed"));
    assert!(Rc::ptr_eq(&original, &returned));
    assert_eq!(returned.audit.as_ptr(), allocation);
    cleanup.expect("actual application cleanup completes");
    let (_, retirement) = native.expect("the independent retirement is acquired");
    assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
    let custody = retirement.custody().expect("complete actor custody");
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
}

#[tokio::test(flavor = "current_thread")]
async fn independent_absent_work_infers_original_initialization_rejection() {
    let original = CounterSnapshot {
        value: 59,
        audit: Box::from([61, 67]),
    };
    let allocation = original.audit.as_ptr();
    let (execution, receiving) = Application::new(RejectedCounter { original }.stop_on_shutdown())
        .execute()
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    execution.await;
    let (
        WorkOutcome::NotInvoked {
            work: None,
            startup_error: Some(_),
            cleanup: Ok(()),
        },
        _root_report,
        Ok((
            _,
            ActorRetirement::InitializationRejected {
                custody,
                error: CounterRefusal::Initialization,
                ..
            },
        )),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = receiving.await.into_parts()
    else {
        panic!("absence and the original actor rejection remain independent");
    };
    assert_eq!(custody.behavior.base().original.audit.as_ptr(), allocation);
    assert_eq!(custody.behavior.base().original.value, 59);
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
}

#[tokio::test(flavor = "current_thread")]
async fn independent_execution_cancellation_returns_original_cold_callable() {
    let original = Rc::new(Cell::new(0));
    let invoked = Rc::clone(&original);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with(|application| async move {
            invoked.set(invoked.get() + 1);
            let shutdown = application.lifecycle().shutdown();
            assert_eq!(shutdown, Ok(()));
            invoked
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    drop(execution);
    let (cold_outcome, _root_report, cold_native, cold_notifications, ()) =
        receiving.await.into_parts();
    let WorkOutcome::Unstarted { application, work } = cold_outcome else {
        panic!("the original callable remains cold after execution surrender");
    };
    assert!(cold_native.is_err());
    assert!(cold_notifications.is_err());
    assert_eq!(original.get(), 0);
    let outcome = application
        .run_with(work)
        .await
        .unwrap_or_else(|_| panic!("the same callable can be retried on this host"))
        .into_parts();
    let (outcome, _root_report, native, notifications, ()) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let (returned, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("retried work completed"));
    assert!(Rc::ptr_eq(&original, &returned));
    assert_eq!(returned.get(), 1);
    cleanup.expect("actual application cleanup completes");
    let (_, retirement) = native.expect("retry retains its original retirement");
    assert!(retirement.behavior().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn independent_entity_execution_infers_root_custody_without_families() {
    let original = Rc::new(Cell::new(71));
    let work = Rc::clone(&original);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with_entities(|application| async move {
            let shutdown = application.lifecycle().shutdown();
            assert_eq!(shutdown, Ok(()));
            work
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    execution.await;
    let (outcome, _root_report, root, notifications, ()) = receiving.await.into_parts();
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let (returned, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("local work completed independently of receiving"));
    cleanup.expect("producer cleanup completes independently");
    assert!(Rc::ptr_eq(&original, &returned));
    let (_, retirement) = root.expect("the actual root retirement is acquired");
    assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
    assert_eq!(retirement.custody().expect("root custody").descendants, []);
}

#[tokio::test(flavor = "current_thread")]
async fn entity_runner_infers_rejected_root_and_preserves_uninvoked_work() {
    let original = CounterSnapshot {
        value: 73,
        audit: Box::from([79, 83]),
    };
    let allocation = original.audit.as_ptr();
    let invoked = Rc::new(Cell::new(0));
    let calls = Rc::clone(&invoked);
    let (outcome, _root_report, root, notifications, ()) =
        Application::new(RejectedCounter { original }.stop_on_shutdown())
            .run_with_entities(|_| async move {
                calls.set(calls.get() + 1);
            })
            .await
            .unwrap_or_else(|_| panic!("the caller owns the entered host"))
            .into_parts();
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let WorkOutcome::NotInvoked {
        work,
        startup_error: Some(_),
        cleanup: Ok(()),
    } = outcome
    else {
        panic!("initialization refusal returns the original uninvoked work");
    };
    assert_eq!(invoked.get(), 0);
    let (
        _,
        ActorRetirement::InitializationRejected {
            custody,
            error: CounterRefusal::Initialization,
            ..
        },
    ) = root.expect("the actual root refusal remains independently acquired")
    else {
        panic!("root receiving preserves the original failure phase");
    };
    assert_eq!(custody.behavior.base().original.audit.as_ptr(), allocation);
    assert_eq!(custody.behavior.base().original.value, 73);
    drop(work);
}

#[cfg(feature = "axum")]
#[tokio::test(flavor = "current_thread")]
async fn http_pair_infers_custody_and_returns_router_on_bind_refusal() {
    let occupied = TcpListener::bind("127.0.0.1:0").expect("the witness owns the occupied port");
    let address = occupied.local_addr().expect("the actual occupied address");
    let original = Rc::new(CounterSnapshot {
        value: 89,
        audit: Box::from([97, 101]),
    });
    let allocation = original.audit.as_ptr();
    let router_payload = Rc::clone(&original);
    let called = Rc::new(Cell::new(0));
    let router_called = Rc::clone(&called);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_axum(address, |application| {
            assert_eq!(router_payload.audit.as_ptr(), allocation);
            router_called.set(router_called.get() + 1);
            let shutdown = application.lifecycle().shutdown();
            assert_eq!(shutdown, Ok(()));
            Router::new()
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered HTTP host"));
    execution.await;
    let (cold_outcome, _root_report, cold_native, cold_notifications, ()) =
        receiving.await.into_parts();
    let WorkOutcome::StagingRejected {
        inputs: Ok((application, router, returned_address, cause)),
    } = cold_outcome
    else {
        panic!("prebinding returns the original cold application and router");
    };
    assert!(cold_native.is_err());
    assert!(cold_notifications.is_err());
    assert_eq!(called.get(), 0);
    assert_eq!(returned_address, address);
    assert_eq!(cause.kind(), std::io::ErrorKind::AddrInUse);
    drop(occupied);
    let outcome = application
        .run_axum(returned_address, router)
        .await
        .unwrap_or_else(|_| panic!("the same returned router is retryable"))
        .into_parts();
    let (outcome, _root_report, native, notifications, ()) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let (serving, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the actual router ran and HTTP serving completed"));
    serving.expect("root termination gracefully stops serving");
    assert_eq!(called.get(), 1);
    assert_eq!(original.audit.as_ptr(), allocation);
    cleanup.expect("actual application cleanup completes");
    let (_, retirement) = native.expect("the real root retirement is independently acquired");
    assert_eq!(retirement.completion(), Some(&bombay::Completion::Stopped));
}

#[tokio::test]
async fn completed_root_report_survives_owned_selection() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let admitted = application.shutdown();
            admitted.expect("the actual root admits shutdown");
            CounterSnapshot {
                value: 23,
                audit: vec![29, 31].into_boxed_slice(),
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let report = outcome
        .root_report()
        .expect("the joined root published its report");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let allocation = outcome
        .work_output()
        .expect("business work completed")
        .audit
        .as_ptr();
    let (value, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("completed selection preserves the independent root report"));
    assert_eq!((value.value, value.audit.as_ptr()), (23, allocation));
    assert_eq!(retirement.root_report(), Some(report));
    let (cleanup, retained_report, native, notifications, ()) = retirement.into_parts();
    cleanup.expect("actual cleanup joins");
    assert_eq!(retained_report, Some(report));
    let (origin, native) = native.expect("the original actor result remains owned");
    assert_eq!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    assert_eq!(native.completion(), Some(&bombay::Completion::Stopped));
    let notifications = notifications.expect("both original notification receipts remain owned");
    notifications
        .termination
        .expect("termination notification succeeds");
    notifications
        .retirement
        .expect("retirement notification succeeds");
}

#[tokio::test]
async fn rejected_startup_retains_root_report_and_original_inputs() {
    let invoked = Rc::new(Cell::new(0));
    let calls = Rc::clone(&invoked);
    let original = CounterSnapshot {
        value: 37,
        audit: vec![41, 43].into_boxed_slice(),
    };
    let allocation = original.audit.as_ptr();
    let outcome = Application::new(RejectedCounter { original }.stop_on_shutdown())
        .run_with(move |_| async move {
            calls.set(calls.get() + 1);
            47
        })
        .await
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let report = outcome
        .root_report()
        .expect("initialization refusal is a joined root report");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
    let returned = outcome
        .try_into_completed()
        .err()
        .expect("uninvoked work returns the whole outcome");
    assert_eq!(returned.root_report(), Some(report));
    assert_eq!(invoked.get(), 0);
    let (work, retained_report, native, notifications, ()) = returned.into_parts();
    assert_eq!(retained_report, Some(report));
    let WorkOutcome::NotInvoked {
        work,
        startup_error: Some(_),
        cleanup: Ok(()),
    } = work
    else {
        panic!("startup refusal retains the original callable and cleanup");
    };
    assert_eq!(Rc::strong_count(&invoked), 2);
    let (_, native) = native.expect("the rejected original root remains owned");
    let ActorRetirement::InitializationRejected {
        custody,
        error: CounterRefusal::Initialization,
        ..
    } = native
    else {
        panic!("the actual initialization error is preserved");
    };
    assert_eq!(
        (
            custody.behavior.base().original.value,
            custody.behavior.base().original.audit.as_ptr()
        ),
        (37, allocation)
    );
    let notifications = notifications.expect("notification results coexist with startup refusal");
    notifications
        .termination
        .expect("termination notification succeeds");
    notifications
        .retirement
        .expect("retirement notification succeeds");
    drop(work);
    assert_eq!(Rc::strong_count(&invoked), 1);
}

#[tokio::test]
async fn unpolled_execution_has_no_root_report_and_keeps_work() {
    let invoked = Rc::new(Cell::new(0));
    let calls = Rc::clone(&invoked);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with(move |_| async move {
            calls.set(calls.get() + 1);
            53
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    drop(execution);
    let outcome = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("an absent root report cannot wait for a publication that never starts");
    assert_eq!(outcome.root_report(), None);
    let returned = outcome
        .try_into_completed()
        .err()
        .expect("cold work retains its whole outcome");
    let (work, report, native, notifications, ()) = returned.into_parts();
    assert_eq!(report, None);
    let WorkOutcome::Unstarted { application, work } = work else {
        panic!("dropping unpolled execution retains the original application and callable");
    };
    assert_eq!(invoked.get(), 0);
    assert_eq!(Rc::strong_count(&invoked), 2);
    assert!(native.is_err());
    assert!(notifications.is_err());
    drop((application, work));
    assert_eq!(Rc::strong_count(&invoked), 1);
}

#[tokio::test]
async fn root_report_survives_both_timer_shutdown_orders() {
    let timer_outside = Application::new(
        Counter::new()
            .stop_on_shutdown()
            .with_one_shot(Duration::from_secs(60), |_| {
                bombay::behavior::Actions::stop()
            }),
    )
    .run_with(|application| async move { application.shutdown() })
    .await
    .unwrap_or_else(|_| panic!("timer-outside application starts"));
    let first = timer_outside
        .root_report()
        .expect("timer-outside root is joined");
    let (admitted, retirement) = timer_outside
        .try_into_completed()
        .unwrap_or_else(|_| panic!("timer-outside work completes"));
    admitted.expect("timer-outside shutdown is admitted");
    assert_eq!(retirement.root_report(), Some(first));
    retirement
        .cleanup()
        .as_ref()
        .expect("timer-outside cleanup succeeds");
    let shutdown_outside = Application::new(
        Counter::new()
            .with_one_shot(Duration::from_secs(60), |_| {
                bombay::behavior::Actions::stop()
            })
            .stop_on_shutdown(),
    )
    .run_with(|application| async move { application.shutdown() })
    .await
    .unwrap_or_else(|_| panic!("shutdown-outside application starts"));
    let second = shutdown_outside
        .root_report()
        .expect("shutdown-outside root is joined");
    let (admitted, retirement) = shutdown_outside
        .try_into_completed()
        .unwrap_or_else(|_| panic!("shutdown-outside work completes"));
    admitted.expect("shutdown-outside shutdown is admitted");
    assert_eq!(retirement.root_report(), Some(second));
    retirement
        .cleanup()
        .as_ref()
        .expect("shutdown-outside cleanup succeeds");
    for report in [first, second] {
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
}

#[tokio::test]
async fn dropped_running_execution_keeps_root_report() {
    let invoked = Rc::new(Cell::new(0));
    let calls = Rc::clone(&invoked);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with(move |_| async move {
            calls.set(calls.get() + 1);
            core::future::pending::<()>().await;
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let mut execution = Box::pin(execution);
    core::future::poll_fn(|context| {
        let pending = execution.as_mut().poll(context);
        assert!(matches!(pending, Poll::Pending));
        if invoked.get() == 1 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    drop(execution);
    let outcome = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("retained receiving joins the actor after execution cancellation");
    let report = outcome
        .root_report()
        .expect("the actual cancelled root was joined");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert!(matches!(
        outcome.work(),
        WorkOutcome::Interrupted { cleanup: Ok(()) }
    ));
    let returned = outcome
        .try_into_completed()
        .err()
        .expect("interrupted work keeps the whole report");
    assert_eq!(returned.root_report(), Some(report));
    let (_, retained_report, native, notifications, ()) = returned.into_parts();
    assert_eq!(retained_report, Some(report));
    let (_, native) = native.expect("the original cancelled actor remains owned");
    assert!(native.behavior().is_some());
    let notifications =
        notifications.expect("both cancellation notification receipts remain owned");
    notifications
        .termination
        .expect("termination notification succeeds");
    notifications
        .retirement
        .expect("retirement notification succeeds");
}

struct ReportObserver {
    failure: Mutex<Option<Arc<Vec<u64>>>>,
    caller: Waker,
}

impl Wake for ReportObserver {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.caller.wake_by_ref();
        let failure = self
            .failure
            .lock()
            .expect("the observer owns its exact panic")
            .take();
        panic_any(failure.expect("the observer releases its original panic once"));
    }
}

#[tokio::test]
async fn final_root_report_survives_notification_panic_without_reclassifying_actor() {
    let failure = Arc::new(vec![59_u64, 61]);
    let retained_failure = Arc::downgrade(&failure);
    let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with(move |application| async move {
            let mut observation = Box::pin(application.retirement());
            core::future::poll_fn(|context| {
                let waker = Waker::from(Arc::new(ReportObserver {
                    failure: Mutex::new(Some(Arc::clone(&failure))),
                    caller: context.waker().clone(),
                }));
                let mut observed = Context::from_waker(&waker);
                let pending = observation.as_mut().poll(&mut observed);
                assert!(matches!(pending, Poll::Pending));
                Poll::Ready(())
            })
            .await;
            drop(failure);
            let admitted = application.shutdown();
            admitted.expect("the root admits shutdown before report publication");
            let report = application.retirement().await;
            let independently_observed = observation.await;
            assert_eq!(report, independently_observed);
        })
        .unwrap_or_else(|_| panic!("the actual caller host is entered"));
    let ((), outcome) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(execution, receiving)
    })
    .await
    .expect("observer panic cannot stall the original joins");
    let report = outcome
        .root_report()
        .expect("the committed report survives outside Work");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("notification failure does not erase completed work"));
    assert_eq!(retirement.root_report(), Some(report));
    let (cleanup, retained_report, native, notifications, ()) = retirement.into_parts();
    cleanup.expect("the original cleanup task joins");
    assert_eq!(retained_report, Some(report));
    let (_, native) = native.expect("the complete actor remains independently owned");
    assert_eq!(native.completion(), Some(&bombay::Completion::Stopped));
    let notifications = notifications.expect("both original notification results remain owned");
    notifications
        .termination
        .expect("termination notification succeeds independently");
    let Err(RetirementNotificationError::Panicked { payload }) = notifications.retirement else {
        panic!("the later report-notification panic keeps its original receipt");
    };
    assert_eq!(retained_failure.strong_count(), 1);
    drop(payload);
    assert_eq!(retained_failure.strong_count(), 0);
}
