//! For learning direct Level-1 actor authoring through one stateful counter:
//! messages, state, typed reply, domain error, and transition remain together.
//! No reusable template is needed; the pure active fold remains the test oracle.

use bombay::ActorNotificationReceipts;
use bombay::ApplicationOutcome;
use futures_util::FutureExt;
use tokio::runtime::Builder;
mod counter;

use bombay::ProjectTerminal;
use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

use crate::counter::{Counter, CounterIncremented, CounterMessage, CounterProcessed, CounterValue};

struct Api {
    counter: EstablishedRecipient<Counter>,
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: RootOrigin<R>,
        terminal: ActorRetirement<R, Self, ()>,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "Retain original admission, processing and retirement receipts before asserting them."
)]
fn main() {
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous caller owns its explicit current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(Counter::new().stop_on_shutdown()).run_with::<ApplicationTerminal<
                StopOnShutdown<Counter>,
            >, _, _, _, _, _>(
                |application| async move {
                    let lifecycle = application.lifecycle();
                    let interface = application.interface(Api {
                        counter: application.root().established_recipient(),
                    });
                    let mut increments = interface.external::<CounterProcessed>().unwrap();
                    let mut snapshots = interface.external::<CounterValue>().unwrap();
                    let bare_admission = increments
                        .send(&interface.api().counter, CounterMessage::Increment)
                        .await;
                    let increment_admission = increments
                        .send(
                            &interface.api().counter,
                            CounterMessage::IncrementAndReply {
                                request_sequence: 1,
                                reply_to: increments.recipient(),
                            },
                        )
                        .await;
                    let read_admission = snapshots
                        .send(
                            &interface.api().counter,
                            CounterMessage::Read(snapshots.recipient()),
                        )
                        .await;
                    let snapshot = snapshots.receive().await;
                    let processing = increments.receive().now_or_never();
                    let shutdown = lifecycle.request_shutdown();
                    let termination = lifecycle.termination().await;
                    (
                        bare_admission,
                        increment_admission,
                        read_admission,
                        snapshot,
                        processing,
                        shutdown,
                        termination,
                    )
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the counter application retains exact cold or joined failure inputs");
        });
    drop(application_host);
    let (
        ApplicationOutcome::Completed {
            output:
                (
                    bare_admission,
                    increment_admission,
                    read_admission,
                    snapshot,
                    processing,
                    shutdown,
                    termination,
                ),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the original completed Work and joined root remain independently owned");
    };
    let retirement = match retirement {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the counter root returns its actual retirement: {failure}")
        }
        retirement => retirement,
    };
    let root_address = origin.address();
    let terminal = <ApplicationTerminal<_> as ProjectTerminal<_, _>>::project(origin, retirement);
    assert_application_stopped(terminal);
    let Some(snapshot) = &snapshot else {
        panic!("the native Counter snapshot must be acquired");
    };
    assert_eq!(snapshot.message, 2);
    let Some(Some(processing)) = &processing else {
        panic!("the exact increment processing receipt must be acquired");
    };
    assert_eq!(processing.from, root_address);
    assert_eq!(
        processing.message,
        CounterIncremented {
            request_sequence: 1
        }
    );
    assert!(bare_admission.is_ok());
    assert!(increment_admission.is_ok());
    assert!(read_admission.is_ok());
    assert_eq!(snapshot.from, root_address);
    assert_eq!(shutdown, Ok(()));
    assert_eq!(termination, Ok(Exit::Normal));
}

fn assert_application_stopped<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                child_failures: (),
                capability_failures,
                unread_owner_cancellation,
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
                interpretation,
                source,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
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
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use super::*;
    use crate::counter::{
        CounterError, CounterIncremented, CounterProcessed, assert_increment_processing_actions,
    };

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "acquire every original Work, native actor, and notification product before semantic assertions"
    )]
    fn correlated_increments_publish_after_the_actual_mutation() {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let (execution, (work, native, notifications)) = host.block_on(async {
            let prepared = Application::new(Counter::new().stop_on_shutdown())
                .execute_with::<_, _, ApplicationTerminal<StopOnShutdown<Counter>>, (), _, _>(
                    |application| async move {
                        let lifecycle = application.lifecycle();
                        let root_address = application.root().address();
                        let target = application.root().established_recipient();
                        let interface = application.interface(());
                        let mut first = interface.external::<CounterProcessed>().unwrap();
                        let mut second = interface.external::<CounterProcessed>().unwrap();
                        let mut values = interface.external::<CounterValue>().unwrap();
                        let processing_reply_to = first.recipient();
                        let values_reply_to = values.recipient();
                        let first_admission = first
                            .send(
                                &target,
                                CounterMessage::IncrementAndReply {
                                    request_sequence: 37,
                                    reply_to: processing_reply_to.clone(),
                                },
                            )
                            .await;
                        let second_admission = second
                            .send(
                                &target,
                                CounterMessage::IncrementAndReply {
                                    request_sequence: 41,
                                    reply_to: second.recipient(),
                                },
                            )
                            .await;
                        let read_admission = values
                            .send(&target, CounterMessage::Read(values_reply_to.clone()))
                            .await;
                        // Read is a native processing barrier, never an Increment receipt.
                        let (snapshot, observed_termination) = match &read_admission {
                            Ok(()) => tokio::select! {
                                snapshot = values.receive() => (snapshot, None),
                                termination = lifecycle.termination() => (None, Some(termination)),
                            },
                            Err(_) => (None, None),
                        };
                        let first_processing = first.receive().now_or_never();
                        let second_processing = second.receive().now_or_never();
                        let shutdown = lifecycle.request_shutdown();
                        let termination = match observed_termination {
                            Some(termination) => termination,
                            None => lifecycle.termination().await,
                        };
                        (
                            root_address,
                            processing_reply_to,
                            values_reply_to,
                            first_admission,
                            second_admission,
                            read_admission,
                            snapshot,
                            first_processing,
                            second_processing,
                            shutdown,
                            termination,
                        )
                    },
                );
            let (execution, receiving) = prepared.unwrap_or_else(|original| {
                panic!(
                    "the explicit entered test host is unavailable: {}",
                    original.2
                );
            });
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let received = receiving.await;
            (execution, received)
        });
        drop(host);

        // Every original Work/native/notification product is acquired before any oracle.
        assert!(execution.is_ok());
        let Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }) = &notifications
        else {
            panic!("both original notification receipts must succeed");
        };
        let Ok((origin, retirement)) = native else {
            panic!("the native actor result must be independently received");
        };
        let terminal =
            <ApplicationTerminal<_> as ProjectTerminal<_, _>>::project(origin, retirement);
        assert_application_stopped(terminal);
        let ApplicationOutcome::Completed {
            output:
                (
                    root_address,
                    processing_reply_to,
                    values_reply_to,
                    first_admission,
                    second_admission,
                    read_admission,
                    snapshot,
                    first_processing,
                    second_processing,
                    shutdown,
                    termination,
                ),
            cleanup: Ok(()),
        } = work
        else {
            panic!("the original Work observations must survive clean retirement");
        };
        let Some(snapshot) = &snapshot else {
            panic!("the actual native Counter snapshot must be acquired");
        };
        assert_eq!(snapshot.message, 2);
        assert_eq!(snapshot.from, root_address);
        assert!(first_admission.is_ok());
        assert!(second_admission.is_ok());
        assert!(read_admission.is_ok());
        assert_eq!(shutdown, Ok(()));
        assert_eq!(termination, Ok(Exit::Normal));
        let Some(Some(first_processing)) = &first_processing else {
            panic!("the first exact processing port must contain its actual receipt");
        };
        let Some(Some(second_processing)) = &second_processing else {
            panic!("the second exact processing port must contain its actual receipt");
        };
        assert_eq!(first_processing.from, root_address);
        assert_eq!(second_processing.from, root_address);
        assert_eq!(
            first_processing.message,
            CounterIncremented {
                request_sequence: 37
            },
        );
        assert_eq!(
            second_processing.message,
            CounterIncremented {
                request_sequence: 41
            },
        );
        assert_increment_processing_actions(processing_reply_to, values_reply_to);
    }
    #[test]
    fn overflowing_increment_keeps_the_original_error_without_processing_success() {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let (execution, (work, native, notifications)) = host.block_on(async {
            let prepared = Application::new(Counter::at_maximum().stop_on_shutdown())
                .execute_with::<_, _, ApplicationTerminal<StopOnShutdown<Counter>>, (), _, _>(
                |application| async move {
                    let lifecycle = application.lifecycle();
                    let root_address = application.root().address();
                    let target = application.root().established_recipient();
                    let interface = application.interface(());
                    let mut increments = interface.external::<CounterProcessed>().unwrap();
                    let mut values = interface.external::<CounterValue>().unwrap();
                    let increment_admission = increments
                        .send(
                            &target,
                            CounterMessage::IncrementAndReply {
                                request_sequence: 29,
                                reply_to: increments.recipient(),
                            },
                        )
                        .await;
                    let read_admission = values
                        .send(&target, CounterMessage::Read(values.recipient()))
                        .await;
                    let (snapshot, observed_termination) = match &read_admission {
                        Ok(()) => tokio::select! {
                            snapshot = values.receive() => (snapshot, None),
                            termination = lifecycle.termination() => (None, Some(termination)),
                        },
                        Err(_) => (None, None),
                    };
                    let processing = increments.receive().now_or_never();
                    // A successful-overflow inversion must still retire its surviving root.
                    let shutdown = lifecycle.request_shutdown();
                    let termination = match observed_termination {
                        Some(termination) => termination,
                        None => lifecycle.termination().await,
                    };
                    (
                        root_address,
                        increment_admission,
                        read_admission,
                        snapshot,
                        processing,
                        shutdown,
                        termination,
                    )
                },
            );
            let (execution, receiving) = prepared.unwrap_or_else(|original| {
                panic!(
                    "the explicit entered test host is unavailable: {}",
                    original.2
                );
            });
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let received = receiving.await;
            (execution, received)
        });
        drop(host);

        // No semantic oracle runs until all original owner products have been acquired.
        let Ok((origin, retirement)) = &native else {
            panic!("the native actor result must be independently received");
        };
        let ActorRetirement::BehaviorFailed { error, .. } = retirement else {
            panic!("overflow must retain the original native BehaviorFailed retirement");
        };
        assert_eq!(*error, CounterError::Overflow);
        assert!(execution.is_ok());
        let Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }) = &notifications
        else {
            panic!("both original notification receipts must succeed");
        };
        let ApplicationOutcome::Completed {
            output:
                (
                    root_address,
                    increment_admission,
                    _read_admission,
                    snapshot,
                    processing,
                    _shutdown,
                    termination,
                ),
            cleanup: Ok(()),
        } = &work
        else {
            panic!("the original Work observations must survive clean retirement");
        };
        assert_eq!(origin.address(), *root_address);
        assert_eq!(*root_address, MailAddr::APPLICATION_ROOT);
        assert!(increment_admission.is_ok());
        assert!(snapshot.is_none());
        assert!(!matches!(processing, Some(Some(_))));
        assert_eq!(*termination, Err(Crash::Failed));
    }
}
