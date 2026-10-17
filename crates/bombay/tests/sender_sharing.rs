//! One-way producer sharing does not duplicate reply or lifecycle authority.

use bombay::actors::ActorExt;
use bombay::behavior::{ClassifySettlement, SettlementStatus};
use bombay::prelude::{ActorRetirement, Application, Completion, MailAddr};
use std::thread;

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use counter::{Counter, CounterMessage};

#[tokio::test(flavor = "current_thread")]
async fn two_threads_borrow_one_sender_and_the_actor_processes_both_commands() {
    let report = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let sender = application.sender().expect("one fresh sender identity");
            let address = sender.address();
            assert_ne!(address, application.root().address());
            let (first, second) = thread::scope(|scope| {
                let sender = &sender;
                let first = scope.spawn(move || sender.try_send(CounterMessage::Increment));
                let second = scope.spawn(move || sender.try_send(CounterMessage::Increment));
                (
                    first.join().expect("first producing thread joins"),
                    second.join().expect("second producing thread joins"),
                )
            });
            first.unwrap_or_else(|_| panic!("first command admitted"));
            second.unwrap_or_else(|_| panic!("second command admitted"));
            assert_eq!(sender.address(), address);
            drop(sender);
            let mut client = application.client().expect("sole counter reply receiver");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the root still admits after sender destruction"));
            let reply = client.receive().await.expect("actual counter reply");
            assert_eq!(reply.from, application.root().address());
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
            reply.message
        })
        .await
        .unwrap_or_else(|_| panic!("the actual application starts"));
    let (count, retirement) = report
        .try_into_completed()
        .unwrap_or_else(|_| panic!("both producers and native retirement complete"));
    assert_eq!(count, 2);
    assert!(retirement.cleanup().is_ok());
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("both notifications");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (origin, root) = retirement
        .retirement()
        .as_ref()
        .expect("native root returns");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        interpretation,
        source,
        settlements,
        ..
    } = root
    else {
        panic!("the original counter stops normally");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(custody.descendants, []);
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
}
