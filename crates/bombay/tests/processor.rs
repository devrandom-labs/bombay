//! For learning how to select a standalone actor template and compose wrapper
//! policy through `ActorExt` without implementing a custom Behavior.

use bombay::WorkOutcome;
use bombay::{ActorCustody, ActorNotificationReceipts};
use tokio::runtime::Builder;
#[path = "../../../examples/actor-templates/src/processor.rs"]
mod processor;

use core::time::Duration;

use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;
use processor::{ProcessorMessage, ProcessorPhase, ProcessorState, transition};

#[test]
fn receive_timeout_replays_deferred_work_and_preserves_retirement() {
    let actor = Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
        .with_receive_timeout(Duration::from_millis(50), |processor| {
            if processor.state().completed(7) {
                Actions::stop()
            } else {
                Actions::cont()
            }
        })
        .stop_on_shutdown();

    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(Application::new(actor).run_with(|application| async move {
            let sender = application
                .sender()
                .expect("the example sender is established");
            sender
                .send(ProcessorMessage::Work(7))
                .await
                .expect("the actor accepts work while closed");
            sender
                .send(ProcessorMessage::Open)
                .await
                .expect("the actor opens and replays deferred work");
            let termination =
                tokio::time::timeout(Duration::from_secs(1), application.termination())
                    .await
                    .expect("the receive-timeout policy terminates before the example deadline");
            assert_eq!(termination, Ok(Exit::Normal));
        }))
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the receive-timeout actor starts and retires");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal = (
        origin,
        match retirement {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual actor returned its joined retirement: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_application_stopped(terminal);
}

#[test]
fn shutdown_preserves_the_complete_processor_retirement() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(
                Machine::new(ProcessorState::new(), ProcessorPhase::Closed, transition)
                    .stop_on_shutdown(),
            )
            .run_with(|application| async move {
                let shutdown = application.request_shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
            }),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the shutdown actor starts and retires");
        })
        .into_parts();
    drop(application_host);
    let (
        WorkOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let terminal = (
        origin,
        match retirement {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual actor returned its joined retirement: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_application_stopped(terminal);
}

fn assert_application_stopped<R>(terminal: (RootOrigin<R>, ActorRetirement<R, Never, Vec<Never>>))
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let (
        origin,
        ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    behavior,
                    control,
                    user,
                    descendants,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            settlements,
            completion,
            interpretation,
            source,
        },
    ) = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(child_failures, []);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants, []);
    assert_eq!(completion, Completion::Stopped);
}
