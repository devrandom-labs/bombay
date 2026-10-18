use bombay::actors::ActorExt as _;
use bombay::behavior::{BehaviorBase as _, MessageProtocol, NoSends, SendLayer, Step, Stopped};
use bombay::composition::{MessageAdapter, StashRoute, UnlimitedStash};
use bombay::{
    ActorCustody, ActorNotificationReceipts, ActorRetirement, ApplicationScope, Completion,
    MailAddr, MailboxCapacity,
};
use core::time::Duration;

struct Invoice {
    number: u64,
    dollars: u64,
    receipt: Box<[u8; 2]>,
}

struct Payment {
    number: u64,
    cents: u64,
    receipt: Box<[u8; 2]>,
}

fn payment(invoice: Invoice) -> Payment {
    Payment {
        number: invoice.number,
        cents: invoice.dollars * 100,
        receipt: invoice.receipt,
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "One retirement journey accounts for every original input and independent custody lane."
)]
async fn retirement_recovers_original_stashed_invoices_and_the_unread_mailbox_suffix() {
    let capacity = MailboxCapacity::new(2).expect("two effective invoice slots");
    assert_eq!(capacity.get(), 2);
    let scope = ApplicationScope::new().mailbox_capacity(capacity);
    let customer = scope
        .external::<MessageProtocol<MailAddr, Payment>>()
        .expect("the payment recipient exists");
    customer.close_admission();
    let destination = customer.recipient();
    let expected_destination = destination.clone();
    let first = Box::new([31, 37]);
    let second = Box::new([41, 43]);
    let third = Box::new([59, 61]);
    let expected = [
        (7, 19, [31, 37], first.as_ptr()),
        (23, 29, [41, 43], second.as_ptr()),
        (47, 53, [59, 61], third.as_ptr()),
    ];
    // Payments are paused when shutdown arrives. Their original invoices
    // belong to the retired stash or the unread mailbox, never to a new copy.
    let root = UnlimitedStash::new(MessageAdapter::new(destination, payment), |_, _| {
        StashRoute::Stash
    })
    .stop_on_shutdown();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        scope
            .with_root(root)
            .run_with(move |application| async move {
                let first_sender = application.sender().expect("the first customer exists");
                let second_sender = application.sender().expect("the second customer exists");
                let origins = [first_sender.address(), second_sender.address()];
                let first_admission = first_sender
                    .send(Invoice {
                        number: 7,
                        dollars: 19,
                        receipt: first,
                    })
                    .await;
                let second_admission = second_sender
                    .send(Invoice {
                        number: 23,
                        dollars: 29,
                        receipt: second,
                    })
                    .await;
                let third_admission = first_sender
                    .send(Invoice {
                        number: 47,
                        dollars: 53,
                        receipt: third,
                    })
                    .await;
                // Three admitted inputs exceed the verified two-slot mailbox,
                // requiring execution to acquire at least the first invoice.
                let shutdown = application.shutdown();
                (
                    [first_admission, second_admission, third_admission],
                    shutdown,
                    origins,
                    customer,
                )
            }),
    )
    .await
    .expect("paused payments must reach joined retirement")
    .unwrap_or_else(|_| panic!("the paused payment application starts"));
    let ((admissions, shutdown, origins, mut customer), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work and retirement stay independently owned"));
    assert!(admissions.iter().all(Result::is_ok));
    assert!(shutdown.is_ok());
    assert_ne!(origins[0], origins[1]);
    let (cleanup, _root_report, native, notifications, ()) = retirement.into_parts();
    assert!(cleanup.is_ok());
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both independent notification results survive");
    };
    let (origin, native) = native.expect("the native retirement survives");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            },
        interpretation,
        source,
        settlements,
        completion,
    } = native
    else {
        panic!("shutdown stops the paused payments normally");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(control.is_empty());
    assert_eq!(descendants.as_slice(), []);
    assert_eq!(child_failures.as_slice(), []);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let mut stops = 0;
    for settlement in settlements {
        assert!(settlement.creations.is_empty());
        let SendLayer {
            owned: NoSends,
            inner: sends,
        } = settlement.sends;
        assert!(sends.is_empty());
        match settlement.become_ {
            Step::Stop(Stopped) => stops += 1,
            Step::Continue => {}
            Step::Goto(never) => match never {},
        }
    }
    assert_eq!(stops, 1);
    let mut stash = behavior.into_inner();
    let held = stash.held();
    assert!(held > 0);
    let pending = stash.take();
    assert_eq!(pending.len(), held);
    assert_eq!(stash.held(), 0);
    let repeated = stash.take();
    assert!(repeated.is_empty());
    assert_eq!(stash.base().destination(), &expected_destination);
    let recovered: Vec<_> = pending.into_iter().chain(user).collect();
    assert_eq!(recovered.len(), expected.len());
    for ((input, (number, dollars, bytes, allocation)), origin) in recovered
        .into_iter()
        .zip(expected)
        .zip([origins[0], origins[1], origins[0]])
    {
        assert_eq!(input.from, origin);
        assert_eq!(input.message.number, number);
        assert_eq!(input.message.dollars, dollars);
        assert_eq!(*input.message.receipt, bytes);
        assert_eq!(input.message.receipt.as_ptr(), allocation);
    }
    let unexpected = customer.receive().await;
    if let Some(reply) = unexpected {
        let Payment {
            number,
            cents,
            receipt,
        } = reply.message;
        panic!("paused invoice {number} unexpectedly paid {cents} cents with {receipt:?}");
    }
}
