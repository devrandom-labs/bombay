use bombay::actors::ActorExt as _;
use bombay::behavior::{
    BehaviorBase as _, ExactDeliveryReason, ItemSettlement, MessageProtocol, NoSends, SendLayer,
    SettledItem, Step, Stopped, User,
};
use bombay::composition::{MessageAdapter, StashRoute, StashSends};
use bombay::{
    ActorCustody, ActorNotificationReceipts, ActorRetirement, ApplicationScope, Completion,
    MailAddr, MailboxCapacity,
};
use core::time::Duration;
use std::collections::BTreeMap;

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

#[derive(Clone, Copy)]
enum Customer {
    Waiting,
    Closed,
}

#[tokio::test(flavor = "current_thread")]
async fn a_customer_receives_every_refused_invoice_with_its_original_sender() {
    invoices(Customer::Waiting).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_closed_customer_leaves_original_refused_invoices_in_retirement() {
    invoices(Customer::Closed).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "One joined journey accounts for every original input and independent custody lane."
)]
async fn invoices(mode: Customer) {
    let capacity = MailboxCapacity::new(2).expect("two effective invoice slots");
    assert_eq!(capacity.get(), 2);
    let scope = ApplicationScope::new().mailbox_capacity(capacity);
    let mut refused = scope
        .external::<MessageProtocol<MailAddr, User<MailAddr, Invoice>>>()
        .expect("the refused-invoice recipient is established");
    if let Customer::Closed = mode {
        refused.close_admission();
    }
    let destination = refused.recipient();
    let expected_destination = destination.clone();
    let payments = scope
        .external::<MessageProtocol<MailAddr, Payment>>()
        .expect("the payment recipient is independently established");
    payments.close_admission();
    let paid = payments.recipient();
    let expected_paid = paid.clone();
    let first = Box::new([31, 37]);
    let second = Box::new([41, 43]);
    let third = Box::new([59, 61]);
    let fourth = Box::new([67, 71]);
    let expected = [
        (7, 19, [31, 37], first.as_ptr()),
        (23, 29, [41, 43], second.as_ptr()),
        (47, 53, [59, 61], third.as_ptr()),
        (73, 79, [67, 71], fourth.as_ptr()),
    ];
    let root = MessageAdapter::new(paid, payment)
        .with_stash(1, destination, |_, _| StashRoute::Stash)
        .stop_on_shutdown();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        scope
            .with_root(root)
            .run_with(move |application| async move {
                let first_sender = application.sender().expect("the first customer exists");
                let second_sender = application.sender().expect("the second customer exists");
                let origins = [first_sender.address(), second_sender.address()];
                let mut admissions = Vec::new();
                for (sender, invoice) in [
                    (
                        &first_sender,
                        Invoice {
                            number: 7,
                            dollars: 19,
                            receipt: first,
                        },
                    ),
                    (
                        &second_sender,
                        Invoice {
                            number: 23,
                            dollars: 29,
                            receipt: second,
                        },
                    ),
                    (
                        &first_sender,
                        Invoice {
                            number: 47,
                            dollars: 53,
                            receipt: third,
                        },
                    ),
                    (
                        &second_sender,
                        Invoice {
                            number: 73,
                            dollars: 79,
                            receipt: fourth,
                        },
                    ),
                ] {
                    let admitted = sender.send(invoice).await;
                    admissions.push(admitted);
                }
                // Four admissions into a verified two-slot mailbox force at least
                // two acquisitions: one held invoice and one refused invoice.
                let mut received = Vec::new();
                if let Customer::Waiting = mode {
                    for _ in 0..3 {
                        let delivery = refused.receive().await.expect("the actual refusal arrives");
                        received.push(delivery);
                    }
                }
                let shutdown = application.request_shutdown();
                (admissions, shutdown, origins, refused, received, payments)
            }),
    )
    .await
    .expect("refusal and joined retirement must finish")
    .unwrap_or_else(|_| panic!("the finite-stash application starts"));
    let ((admissions, shutdown, origins, mut customer, received, mut payments), retirement) =
        outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("work and all retirement lanes remain independently owned"));
    assert_eq!(admissions.len(), 4);
    assert!(admissions.iter().all(Result::is_ok));
    assert!(shutdown.is_ok());
    assert_ne!(origins[0], origins[1]);
    let (cleanup, native, notifications, ()) = retirement.into_parts();
    assert!(cleanup.is_ok());
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both independent notification results survive");
    };
    let (origin, native) = native.expect("native retirement survives");
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
        panic!("overflow continues until ordinary shutdown");
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
    let mut refused = Vec::new();
    let mut stops = 0;
    for settlement in settlements {
        assert!(settlement.creations.is_empty());
        let SendLayer {
            owned: NoSends,
            inner: StashSends { actor, refusals },
        } = settlement.sends;
        assert!(actor.is_empty());
        match settlement.become_ {
            Step::Stop(Stopped) => {
                stops += 1;
                assert!(refusals.is_empty());
            }
            Step::Continue => {}
            Step::Goto(never) => match never {},
        }
        for delivery in refusals {
            let SettledItem::Attempted(ItemSettlement::Rejected { item, reason }) = delivery else {
                panic!("accepted exact sends need no retained source receipt");
            };
            assert_eq!(reason, ExactDeliveryReason::ClosedRecipient);
            assert_eq!(item.to, expected_destination);
            refused.push(item.message);
        }
    }
    let mut stash = behavior.into_inner();
    assert_eq!(stash.held(), 1);
    assert_eq!(stash.base().destination(), &expected_paid);
    let held = stash.take();
    assert_eq!(held.len(), 1);
    assert_eq!(stash.held(), 0);
    let repeated = stash.take();
    assert!(repeated.is_empty());
    assert_eq!(stops, 1);
    assert_eq!(
        held.front()
            .expect("the first invoice is held")
            .message
            .number,
        7
    );
    match mode {
        Customer::Closed => {
            assert!(received.is_empty());
            assert!(!refused.is_empty());
        }
        Customer::Waiting => {
            assert_eq!(received.len(), 3);
            assert!(refused.is_empty());
            let numbers: Vec<_> = received
                .iter()
                .map(|delivery| delivery.message.message.number)
                .collect();
            assert_eq!(numbers, [23, 47, 73]);
        }
    }
    let received = received.into_iter().map(|delivery| {
        assert_eq!(delivery.from, MailAddr::APPLICATION_ROOT);
        delivery.message
    });
    let unread: Vec<_> = user.iter().map(|input| input.message.number).collect();
    assert!(unread.windows(2).all(|pair| pair[0] < pair[1]));
    let mut recovered = BTreeMap::new();
    for input in held.into_iter().chain(refused).chain(received).chain(user) {
        let duplicate = recovered.insert(input.message.number, input);
        assert!(duplicate.is_none());
    }
    assert_eq!(recovered.len(), expected.len());
    for ((number, dollars, bytes, allocation), original) in expected
        .into_iter()
        .zip([origins[0], origins[1], origins[0], origins[1]])
    {
        let input = recovered
            .remove(&number)
            .expect("each original invoice has one owner");
        assert_eq!(input.from, original);
        assert_eq!(input.message.number, number);
        assert_eq!(input.message.dollars, dollars);
        assert_eq!(*input.message.receipt, bytes);
        assert_eq!(input.message.receipt.as_ptr(), allocation);
    }
    assert!(recovered.is_empty());
    customer.close_admission();
    let unexpected = customer.receive().await;
    assert!(unexpected.is_none());
    let unexpected_payment = payments.receive().await;
    if let Some(delivery) = unexpected_payment {
        let Payment {
            number,
            cents,
            receipt,
        } = delivery.message;
        panic!("paused invoice {number} unexpectedly paid {cents} cents with {receipt:?}");
    }
}
