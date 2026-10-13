use bombay::actors::ActorExt;
use bombay::behavior::{
    AllocationRejection, BehaviorActed, BehaviorBase, ClassifySettlement, Never, SettlementStatus,
};
use bombay::{
    ActorCustody, ActorNotificationReceipts, ActorRetirement, Application, ApplicationCleanupError,
    ApplicationOutcome,
};
use core::ptr;
use std::cell::Cell;
use std::rc::Rc;
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

type CounterJourney = ApplicationOutcome<
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
    let outcome: CounterJourney = ApplicationOutcome::Completed {
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
    let outcome: CounterJourney = ApplicationOutcome::Completed {
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
        ApplicationOutcome::StagingRejected {
            inputs: CounterSnapshot {
                value: 1,
                audit: vec![11].into_boxed_slice(),
            },
        },
        ApplicationOutcome::Unstarted {
            application: CounterSnapshot {
                value: 2,
                audit: vec![12].into_boxed_slice(),
            },
            work: CounterSnapshot {
                value: 3,
                audit: vec![13].into_boxed_slice(),
            },
        },
        ApplicationOutcome::Prepared {
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
        ApplicationOutcome::NotInvoked {
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
        ApplicationOutcome::Interrupted {
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
            ApplicationOutcome::StagingRejected { inputs } => vec![inputs],
            ApplicationOutcome::Unstarted { application, work } => vec![application, work],
            ApplicationOutcome::Prepared {
                inputs,
                work,
                cleanup,
            } => vec![inputs, work, cleanup.expect("prepared cleanup")],
            ApplicationOutcome::NotInvoked {
                work,
                startup_error,
                cleanup,
            } => {
                assert!(startup_error.is_none());
                vec![work, cleanup.expect("uninvoked cleanup")]
            }
            ApplicationOutcome::Interrupted { cleanup } => {
                vec![cleanup.expect("interrupted cleanup")]
            }
            ApplicationOutcome::Completed { .. } => panic!("rejection cannot forge completion"),
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
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            CounterSnapshot {
                value: reply.message,
                audit: vec![1].into_boxed_slice(),
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the caller entered a host"));
    let (outcome, native, notifications) = outcome;
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
        .unwrap_or_else(|_| panic!("a host is entered"));
    let (outcome, native, notifications) = outcome;
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
    let ApplicationOutcome::NotInvoked {
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
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            work
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    execution.await;
    let outcome = receiving.await;
    let (outcome, native, notifications) = outcome;
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
        ApplicationOutcome::NotInvoked {
            work: None,
            startup_error: Some(_),
            cleanup: Ok(()),
        },
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
    ) = receiving.await
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
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            invoked
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    drop(execution);
    let (cold_outcome, cold_native, cold_notifications) = receiving.await;
    let ApplicationOutcome::Unstarted { application, work } = cold_outcome else {
        panic!("the original callable remains cold after execution surrender");
    };
    assert!(cold_native.is_err());
    assert!(cold_notifications.is_err());
    assert_eq!(original.get(), 0);
    let outcome = application
        .run_with(work)
        .await
        .unwrap_or_else(|_| panic!("the same callable can be retried on this host"));
    let (outcome, native, notifications) = outcome;
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
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            work
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    execution.await;
    let (outcome, root, notifications, ()) = receiving.await;
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
    let (outcome, root, notifications, ()) =
        Application::new(RejectedCounter { original }.stop_on_shutdown())
            .run_with_entities(|_| async move {
                calls.set(calls.get() + 1);
            })
            .await
            .unwrap_or_else(|_| panic!("the caller owns the entered host"));
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let ApplicationOutcome::NotInvoked {
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
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            Router::new()
        })
        .unwrap_or_else(|_| panic!("the caller owns the entered HTTP host"));
    execution.await;
    let (cold_outcome, cold_native, cold_notifications) = receiving.await;
    let ApplicationOutcome::StagingRejected {
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
        .unwrap_or_else(|_| panic!("the same returned router is retryable"));
    let (outcome, native, notifications) = outcome;
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
