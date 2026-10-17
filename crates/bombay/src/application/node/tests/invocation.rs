use super::counter::{CounterIncremented, CounterProcessed, CounterValue};
use super::custody::completed_actor;
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use crate::actors::ActorExt;
use crate::application::execution::{
    ApplicationWorkCustody, ApplicationWorkPresence, ApplicationWorkPublication,
    execute_application_with,
};
use crate::application::{ApplicationScope, MailboxCapacity};
use crate::behavior::{Behavior, BehaviorBase, Never, User};
use crate::local::children::StructuralOrigins;
use crate::{ActorNotificationReceipts, Application, ApplicationHandle, WorkOutcome};
use behavior_actors::StopOnShutdown;
use core::future::Ready;
use std::ptr;
use std::time::Duration;
use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one native invocation and complete retirement trace"
)]
fn absent_work_still_drives_correlated_counter_operations() {
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let (handoff, granted) = oneshot::channel();
                let (finished, export_finished) = oneshot::channel();
                let mut transferred = None;
                let mut processed = None;
                let mut admission = None;
                let mut tail = None;
                let received_processed = &mut processed;
                let received_admission = &mut admission;
                let received_tail = &mut tail;
                let binding = async |application: &ApplicationHandle<
                    <Counter as Behavior>::Protocol,
                    <StopOnShutdown<Counter> as Behavior>::Event,
                >| {
                    transferred = Some(handoff.send(application.clone()));
                    let interface =
                        application.interface(application.root().established_recipient());
                    let exporting = async move {
                        let target = interface.api().clone();
                        let mut client = interface
                            .client_to::<_, CounterProcessed>(target)
                            .unwrap_or_else(|_| panic!("the actual reply client allocates"));
                        *received_admission = Some(
                            client
                                .send(CounterMessage::IncrementAndReply {
                                    request_sequence: 79,
                                    reply_to: client.recipient(),
                                })
                                .await,
                        );
                        *received_processed = Some(client.receive().await);
                        client.close_admission();
                        *received_tail = Some(client.receive().await);
                        let _published = finished.send(());
                    };
                    Ok::<_, ()>(Some(exporting))
                };
                let application = Application::new(Counter::new().stop_on_shutdown());
                let (execution, receiving) = execute_application_with::<
                    _,
                    StopOnShutdown<Counter>,
                    StopOnShutdown<Counter>,
                    _,
                    StructuralOrigins<<StopOnShutdown<Counter> as BehaviorBase>::Base>,
                    Never,
                    (),
                    Never,
                    _,
                    _,
                    _,
                    Never,
                    fn(
                        Never,
                        ApplicationHandle<
                            <Counter as Behavior>::Protocol,
                            <StopOnShutdown<Counter> as Behavior>::Event,
                        >,
                    ) -> Ready<Never>,
                    Ready<Never>,
                    _,
                    _,
                    (),
                    _,
                    _,
                >(
                    Handle::current(),
                    application,
                    (),
                    async |application: &Application<StopOnShutdown<Counter>, _>| {
                        ((), application.allocations())
                    },
                    |original: ApplicationWorkPublication<_, _, _, _, Never, _>,
                     (),
                     allocations| {
                        let (application, (), publication) = original.into_unstarted();
                        let (root, spaces, (), (), scope) = application.into_parts();
                        let scope = scope.unwrap_or(ApplicationScope {
                            allocations,
                            client_capacity: MailboxCapacity::default(),
                            mailbox_capacity: MailboxCapacity::default(),
                        });
                        Ok((
                            ApplicationWorkPublication {
                                publication: Some((
                                    ApplicationWorkCustody::Prepared(
                                        (root, spaces, scope),
                                        ApplicationWorkPresence::Absent(()),
                                    ),
                                    publication,
                                )),
                            },
                            (),
                        ))
                    },
                    ((), |_executor, (), ()| None),
                    Some(binding),
                );
                let observer = async {
                    let application = granted.await.expect("actual startup grants native control");
                    let export_finished = export_finished.await;
                    let mut reader = application
                        .client_to::<_, CounterValue>(application.root().established_recipient())
                        .unwrap_or_else(|_| panic!("independent actual reader allocates"));
                    let read = reader.send(CounterMessage::Read(reader.recipient())).await;
                    let value = reader.receive().await;
                    let stopped = application.request_shutdown();
                    let report = application.lifecycle().retirement().await;
                    reader.close_admission();
                    let tail = reader.receive().await;
                    (export_finished, read, value, stopped, report, tail)
                };
                let ((), observer) = tokio::join!(execution, observer);
                let (work, native, notifications) = receiving.await;
                let work = work.into_absent();
                move || {
                    let (export_finished, read, value, stopped, _report, read_tail) = observer;
                    read.expect("the independent read is admitted even when exporting is omitted");
                    stopped.expect("independent native control retires the actual root");
                    let (origin, native) = native.expect("original root retirement");
                    let root = origin.address();
                    let _counter = completed_actor(origin, native);
                    let Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }) = notifications
                    else {
                        panic!("both actual notification originals survive");
                    };
                    let WorkOutcome::Completed {
                        output: None,
                        cleanup: Ok(()),
                    } = work
                    else {
                        panic!("genuine absent work completes without a fabricated work future");
                    };
                    assert!(transferred.expect("one invocation").is_ok());
                    assert!(read_tail.is_none());
                    assert_eq!(
                        value,
                        Some(User::new(root, 1)),
                        "independent native value proves export progress"
                    );
                    assert!(export_finished.is_ok());
                    assert!(admission.expect("actual command attempt retained").is_ok());
                    assert_eq!(
                        processed,
                        Some(Some(User::new(
                            root,
                            CounterIncremented {
                                request_sequence: 79
                            }
                        )))
                    );
                    assert_eq!(tail, Some(None));
                }
            })
            .await
        })
        .expect("watchdog is setup protection, never the export progress oracle")
    };
    observations();
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one original append and terminal native retirement trace; retain complete rejected inputs without boxing them"
)]
fn absent_work_still_drives_move_only_ledger_append() {
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let original: Box<[u8]> = Box::from([83_u8, 89]);
                let original_pointer = ptr::from_ref(original.as_ref());
                let (handoff, granted) = oneshot::channel();
                let (finished, export_finished) = oneshot::channel();
                let mut transferred = None;
                let mut admission = None;
                let mut sender_origin = None;
                let received_admission = &mut admission;
                let received_origin = &mut sender_origin;
                let binding = async |application: &ApplicationHandle<
                    <MailLedger as Behavior>::Protocol,
                    <MailLedger as Behavior>::Event,
                >| {
                    transferred = Some(handoff.send(application.clone()));
                    let interface =
                        application.interface(application.root().established_recipient());
                    let exporting = async move {
                        let sender = interface
                            .sender_to(interface.api().clone())
                            .unwrap_or_else(|_| panic!("actual one-way sender allocates"));
                        *received_origin = Some(sender.address());
                        *received_admission =
                            Some(sender.send(LedgerCommand::Append(original)).await);
                        let _published = finished.send(());
                    };
                    Ok::<_, ()>(Some(exporting))
                };
                let application = Application::new(MailLedger {
                    entries: Vec::new(),
                });
                let (execution, receiving) = execute_application_with::<
                    _,
                    MailLedger,
                    MailLedger,
                    _,
                    StructuralOrigins<<MailLedger as BehaviorBase>::Base>,
                    Never,
                    (),
                    Never,
                    _,
                    _,
                    _,
                    Never,
                    fn(
                        Never,
                        ApplicationHandle<
                            <MailLedger as Behavior>::Protocol,
                            <MailLedger as Behavior>::Event,
                        >,
                    ) -> Ready<Never>,
                    Ready<Never>,
                    _,
                    _,
                    (),
                    _,
                    _,
                >(
                    Handle::current(),
                    application,
                    (),
                    async |application: &Application<MailLedger, _>| {
                        ((), application.allocations())
                    },
                    |original: ApplicationWorkPublication<_, _, _, _, Never, _>,
                     (),
                     allocations| {
                        let (application, (), publication) = original.into_unstarted();
                        let (root, spaces, (), (), scope) = application.into_parts();
                        let scope = scope.unwrap_or(ApplicationScope {
                            allocations,
                            client_capacity: MailboxCapacity::default(),
                            mailbox_capacity: MailboxCapacity::default(),
                        });
                        Ok((
                            ApplicationWorkPublication {
                                publication: Some((
                                    ApplicationWorkCustody::Prepared(
                                        (root, spaces, scope),
                                        ApplicationWorkPresence::Absent(()),
                                    ),
                                    publication,
                                )),
                            },
                            (),
                        ))
                    },
                    ((), |_executor, (), ()| None),
                    Some(binding),
                );
                let observer = async {
                    let application = granted
                        .await
                        .expect("actual startup grants the terminal-read target");
                    let export_finished = export_finished.await;
                    let mut reader = application
                        .client_to::<_, Receipts>(application.root().established_recipient())
                        .unwrap_or_else(|_| panic!("independent terminal reader allocates"));
                    let read = reader.send(LedgerCommand::Read(reader.recipient())).await;
                    let origins = reader.receive().await;
                    let report = application.lifecycle().retirement().await;
                    reader.close_admission();
                    let tail = reader.receive().await;
                    (export_finished, read, origins, report, tail)
                };
                let ((), observer) = tokio::join!(execution, observer);
                let (work, native, notifications) = receiving.await;
                let work = work.into_absent();
                move || {
                    let (export_finished, read, origins, _report, tail) = observer;
                    read.expect("independent terminal read stops even an empty ledger");
                    let (origin, native) = native.expect("original terminal actor custody");
                    let root = origin.address();
                    let ledger = completed_actor(origin, native);
                    let Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }) = notifications
                    else {
                        panic!("both actual notification originals survive");
                    };
                    let WorkOutcome::Completed {
                        output: None,
                        cleanup: Ok(()),
                    } = work
                    else {
                        panic!("genuine absent work completes without a fabricated work future");
                    };
                    assert!(transferred.expect("one invocation").is_ok());
                    assert!(tail.is_none());
                    assert_eq!(
                        ledger.entries.len(),
                        1,
                        "actual terminal actor must own the exported append"
                    );
                    let (sender, original) = &ledger.entries[0];
                    assert_eq!(ptr::from_ref(original.as_ref()), original_pointer);
                    assert_eq!(original.as_ref(), [83, 89]);
                    assert_eq!(sender_origin, Some(*sender));
                    assert_eq!(origins, Some(User::new(root, vec![*sender])));
                    assert!(export_finished.is_ok());
                    assert!(admission.expect("original admission retained").is_ok());
                }
            })
            .await
        })
        .expect("watchdog is setup protection, never the append progress oracle")
    };
    observations();
}

mod rejection;

mod startup;
