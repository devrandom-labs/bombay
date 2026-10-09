use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorActed, ClassifySettlement, Never, SettlementStatus};
use bombay::{ActorRetirement, Application, ApplicationCleanupError, ApplicationOutcome};
use std::cell::Cell;
use std::rc::Rc;
use tokio::sync::oneshot;

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
        .run_with::<Never, _, _, _, _, _>(|application| async move {
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
    assert_eq!(outcome.work_output().expect("counter completed").value, 1);
    let (snapshot, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("counter work completed"));
    let (origin, retirement) = cleanup.expect("joined root was received");
    assert_eq!(snapshot.value, 1);
    assert_eq!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        settlements,
        user,
        descendants,
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
        .run_with::<Never, _, _, _, _, _>(|_| async move {
            work_invoked.set(work_invoked.get() + 1);
            1_u64
        })
        .await
        .unwrap_or_else(|_| panic!("a host is entered"));
    assert!(outcome.work_output().is_none());
    assert!(matches!(
        outcome.cleanup(),
        Some(Ok((
            _,
            ActorRetirement::InitializationRejected {
                error: CounterRefusal::Initialization,
                ..
            }
        )))
    ));
    let returned = outcome
        .try_into_completed()
        .err()
        .expect("startup did not invoke work");
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
    let (_, retirement) = cleanup.expect("startup rejection was joined");
    let ActorRetirement::InitializationRejected {
        behavior,
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
