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

    use bombay::behavior::{
        ActionSettlement, ExactDeliveryReason, ItemSettlement, SendLayer, SettledItem,
    };
    use bombay::{ActorFailureAssessment, RetirementAssessment};

    use super::*;
    use crate::counter::{
        CounterError, CounterIncremented, CounterProcessed, CounterSettlements,
        assert_increment_processing_actions,
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

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "retain all request ports, cancelled waits, exact refused deliveries and native receipts before the oracles"
    )]
    fn separate_read_ports_preserve_cancelled_waits_and_refuse_late_replies() {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        let (execution, (work, native, notifications)) = host.block_on(async {
            let prepared = Application::new(Counter::new().stop_on_shutdown())
                .execute_with::<_, _, ApplicationTerminal<StopOnShutdown<Counter>>, (), _, _>(
                    |application| async move {
                        let lifecycle = application.lifecycle();
                        let target = application.root().established_recipient();
                        let root_address = application.root().address();
                        let interface = application.interface(());
                        let mut first = interface.external::<CounterValue>().unwrap();
                        let mut second = interface.external::<CounterValue>().unwrap();
                        let mut continuation = interface.external::<CounterValue>().unwrap();
                        let first_address = first.address();
                        let second_address = second.address();
                        let continuation_address = continuation.address();
                        let first_recipient = first.recipient();
                        let first_admission =
                            first.try_send(&target, CounterMessage::Read(first_recipient.clone()));
                        let cancelled_wait = first.receive().now_or_never();
                        let increment = second.send(&target, CounterMessage::Increment).await;
                        let second_admission = second
                            .send(&target, CounterMessage::Read(second.recipient()))
                            .await;
                        let (second_reply, early_termination) = match &second_admission {
                            Ok(()) => tokio::select! {
                                reply = second.receive() => (reply, None),
                                termination = lifecycle.termination() => (None, Some(termination)),
                            },
                            Err(_) => (None, None),
                        };
                        let first_reply = first.receive().now_or_never();
                        first.close_admission();
                        let first_exhaustion = first.receive().now_or_never();
                        second.close_admission();
                        let second_exhaustion = second.receive().now_or_never();
                        let late_reply = second.send(&first_recipient, u64::MAX).await;
                        let late_read = second
                            .send(&target, CounterMessage::Read(first_recipient))
                            .await;
                        let later_increment = second.send(&target, CounterMessage::Increment).await;
                        let final_admission = continuation
                            .send(&target, CounterMessage::Read(continuation.recipient()))
                            .await;
                        let (final_reply, later_termination) = match &final_admission {
                            Ok(()) if early_termination.is_none() => tokio::select! {
                                reply = continuation.receive() => (reply, None),
                                termination = lifecycle.termination() => (None, Some(termination)),
                            },
                            Ok(()) | Err(_) => (None, None),
                        };
                        let extra_reply = continuation.receive().now_or_never();
                        let shutdown = lifecycle.request_shutdown();
                        let termination = match early_termination.or(later_termination) {
                            Some(termination) => termination,
                            None => lifecycle.termination().await,
                        };
                        let report = lifecycle.retirement().await;
                        (
                            root_address,
                            first_address,
                            second_address,
                            continuation_address,
                            first_admission,
                            [
                                increment,
                                second_admission,
                                late_read,
                                later_increment,
                                final_admission,
                            ],
                            cancelled_wait,
                            first_reply,
                            second_reply,
                            first_exhaustion,
                            second_exhaustion,
                            late_reply,
                            final_reply,
                            extra_reply,
                            shutdown,
                            termination,
                            report,
                        )
                    },
                );
            let (execution, receiving) = prepared.unwrap_or_else(|original| {
                panic!("the entered test host is unavailable: {}", original.2);
            });
            let execution = AssertUnwindSafe(execution).catch_unwind().await;
            let received = receiving.await;
            (execution, received)
        });
        drop(host);
        // All original owners are acquired before any semantic assertion.
        let Ok((origin, ActorRetirement::Completed { settlements, .. })) = &native else {
            panic!("the active Counter must retain its joined outcome");
        };
        let ApplicationOutcome::Completed {
            output:
                (
                    root_address,
                    first_address,
                    second_address,
                    continuation_address,
                    first_admission,
                    admissions,
                    cancelled_wait,
                    first_reply,
                    second_reply,
                    first_exhaustion,
                    second_exhaustion,
                    late_reply,
                    final_reply,
                    extra_reply,
                    shutdown,
                    termination,
                    report,
                ),
            cleanup: Ok(()),
        } = work
        else {
            panic!("both pending requests and all original observations survive cleanup");
        };
        assert!(execution.is_ok());
        assert!(matches!(
            notifications,
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            })
        ));
        assert_eq!(origin.address(), root_address);
        assert_ne!(first_address, second_address);
        assert_ne!(first_address, continuation_address);
        assert_ne!(second_address, continuation_address);
        assert!(first_admission.is_ok());
        assert!(admissions.iter().all(Result::is_ok));
        assert!(cancelled_wait.is_none());
        let Some(Some(first_reply)) = first_reply else {
            panic!("cancelling only the borrowed wait preserves the first request's reply");
        };
        let Some(second_reply) = second_reply else {
            panic!("the separate second request owns its actual reply");
        };
        let Some(final_reply) = final_reply else {
            panic!("the native Counter continues after the closed reply refusal");
        };
        assert_eq!(first_reply.from, root_address);
        assert_eq!(first_reply.message, 0);
        assert_eq!(second_reply.from, root_address);
        assert_eq!(second_reply.message, 1);
        assert_eq!(final_reply.from, root_address);
        assert_eq!(final_reply.message, 2);
        assert!(extra_reply.is_none());
        assert!(matches!(first_exhaustion, Some(None)));
        assert!(matches!(second_exhaustion, Some(None)));
        let Err(original_refusal) = late_reply else {
            panic!("the original late reply must remain refused by the exact closed port");
        };
        assert_eq!(shutdown, Ok(()));
        assert_eq!(termination, Ok(Exit::Normal));
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
        let mut refused_replies = 0;
        for ActionSettlement {
            creations,
            sends:
                SendLayer {
                    owned,
                    inner:
                        CounterSettlements {
                            values,
                            processed_increments,
                        },
                },
            become_,
        } in settlements
        {
            assert!(creations.is_empty());
            assert_eq!(owned.settlement_status(), SettlementStatus::Accepted);
            assert!(processed_increments.is_empty());
            assert!(matches!(become_, Step::Continue | Step::Stop(_)));
            for original in values {
                let SettledItem::Attempted(ItemSettlement::Rejected { item, reason }) = original
                else {
                    panic!("the original closed reply must be retained");
                };
                assert_eq!(*reason, ExactDeliveryReason::ClosedRecipient);
                assert_eq!(item.message, 1);
                refused_replies += 1;
            }
        }
        assert_eq!(refused_replies, 1);
        // Recover the same original direct-send payload after the native oracles.
        let original_reply = original_refusal.into_message();
        assert_eq!(original_reply, u64::MAX);
    }
}
