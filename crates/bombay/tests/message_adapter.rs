use bombay::actors::ActorExt as _;
use bombay::behavior::{
    BehaviorBase, ExactDeliveryReason, ItemSettlement, MessageProtocol, NoSends, SendLayer,
    SettledItem, Step, Stopped,
};
use bombay::composition::MessageAdapter;
use bombay::{
    ActorCustody, ActorNotificationReceipts, ActorRetirement, ApplicationScope, Completion,
    MailAddr, MailboxCapacity,
};
use core::time::Duration;
use std::collections::BTreeMap;

#[derive(Debug)]
struct Invoice {
    number: u64,
    dollars: u32,
    receipt: Box<[u8]>,
}

#[derive(Debug)]
struct Payment {
    invoice: u64,
    cents: u64,
    receipt: Box<[u8]>,
}

fn payment(invoice: Invoice) -> Payment {
    Payment {
        invoice: invoice.number,
        cents: u64::from(invoice.dollars) * 100,
        receipt: invoice.receipt,
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "One native journey keeps every owned retirement lane visible."
)]
async fn one_adapter_name_maps_an_invoice_to_its_established_recipient() {
    let scope = ApplicationScope::new();
    let mut customer = scope
        .external::<MessageProtocol<MailAddr, Payment>>()
        .expect("the payment recipient is established before root construction");
    let destination = customer.recipient();
    let original_destination = destination.clone();
    let receipt: Box<[u8]> = Box::from([11, 13, 17]);
    let allocation = receipt.as_ptr();
    let invoice = Invoice {
        number: 7,
        dollars: 19,
        receipt,
    };
    let root = MessageAdapter::new(destination, payment).stop_on_shutdown();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        scope
            .with_root(root)
            .run_with(move |application| async move {
                let sender = application.sender().expect("the invoice sender exists");
                let admitted = sender.send(invoice).await;
                let received = customer.receive().await;
                let stopped = application.request_shutdown();
                (admitted, received, stopped, customer)
            }),
    )
    .await
    .expect("the fixture must finish actual delivery and joined retirement")
    .unwrap_or_else(|_| panic!("the caller hosts the payment application"));
    let ((admitted, received, stopped, mut customer), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete payment work remains owned"));
    assert!(admitted.is_ok());
    assert!(stopped.is_ok());
    assert!(retirement.cleanup().is_ok());
    let (origin, actor) = retirement
        .retirement()
        .as_ref()
        .expect("the original actor retirement was acquired");
    assert_eq!(actor.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior: _,
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
        completion: _,
    } = actor
    else {
        panic!("the exact adapter stopped normally");
    };
    assert!(control.is_empty());
    assert!(user.is_empty());
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
        if matches!(settlement.become_, Step::Stop(Stopped)) {
            stops += 1;
            assert!(settlement.sends.inner.is_empty());
        } else {
            assert_eq!(settlement.become_, Step::Continue);
        }
        let SendLayer {
            owned: NoSends,
            inner: sends,
        } = &settlement.sends;
        // Established delivery discharges its accepted unit receipt. The
        // independently received original payment proves the actual transfer.
        assert!(sends.is_empty());
    }
    assert_eq!(stops, 1);
    assert_eq!(
        actor
            .behavior()
            .expect("the adapter survives")
            .base()
            .destination(),
        &original_destination
    );
    let received = received.expect("one actual mapped payment arrives");
    assert_eq!(received.from, origin.address());
    assert_eq!(received.message.invoice, 7);
    assert_eq!(received.message.cents, 1900);
    assert_eq!(received.message.receipt.as_ptr(), allocation);
    assert_eq!(received.message.receipt.as_ref(), [11, 13, 17]);
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = retirement.notifications()
    else {
        panic!("both independent notification results survive");
    };
    customer.close_admission();
    let extra = customer.receive().await;
    assert!(extra.is_none());
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "One native journey keeps every owned retirement lane visible."
)]
async fn closed_payment_recipient_preserves_mapped_rejections_and_unread_invoices() {
    let capacity = MailboxCapacity::new(2).expect("two effective invoice slots");
    assert_eq!(capacity.get(), 2);
    let scope = ApplicationScope::new().mailbox_capacity(capacity);
    let customer = scope
        .external::<MessageProtocol<MailAddr, Payment>>()
        .expect("the exact payment recipient exists");
    customer.close_admission();
    let destination = customer.recipient();
    let original_destination = destination.clone();
    let first: Box<[u8]> = Box::from([31, 37]);
    let second: Box<[u8]> = Box::from([41, 43]);
    let third: Box<[u8]> = Box::from([59, 61]);
    let expected = BTreeMap::from([
        (7, (19, 1900, first.as_ptr(), [31, 37])),
        (23, (29, 2900, second.as_ptr(), [41, 43])),
        (47, (53, 5300, third.as_ptr(), [59, 61])),
    ]);
    let root = MessageAdapter::new(destination, payment).stop_on_shutdown();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        scope
            .with_root(root)
            .run_with(move |application| async move {
                let sender = application.sender().expect("the invoice sender exists");
                let mut admissions = Vec::new();
                for invoice in [
                    Invoice {
                        number: 7,
                        dollars: 19,
                        receipt: first,
                    },
                    Invoice {
                        number: 23,
                        dollars: 29,
                        receipt: second,
                    },
                    Invoice {
                        number: 47,
                        dollars: 53,
                        receipt: third,
                    },
                ] {
                    let admitted = sender.send(invoice).await;
                    admissions.push(admitted);
                }
                // Three actual business inputs exceed the verified two-slot
                // capacity, proving the first was acquired before shutdown.
                let stopped = application.request_shutdown();
                (admissions, stopped, sender.address(), customer)
            }),
    )
    .await
    .expect("the fixture must finish actual rejection and joined retirement")
    .unwrap_or_else(|_| panic!("the caller hosts the actual rejection journey"));
    let ((admissions, stopped, sender, mut customer), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work and all retirement lanes remain owned"));
    assert_eq!(admissions.len(), 3);
    assert!(admissions.iter().all(Result::is_ok));
    assert!(stopped.is_ok());
    let (cleanup, native, notifications, ()) = retirement.into_parts();
    assert!(cleanup.is_ok());
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original notifications are acquired");
    };
    let (origin, native) = native.expect("the exact native retirement survives");
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
        panic!("the selected shutdown policy stops the ordinary adapter");
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(behavior.base().destination(), &original_destination);
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
    let mut unread = BTreeMap::new();
    for input in user {
        assert_eq!(input.from, sender);
        let duplicate = unread.insert(input.message.number, input.message);
        assert!(duplicate.is_none());
    }
    let mut rejected = BTreeMap::new();
    let mut stops = 0;
    for settlement in settlements {
        assert!(settlement.creations.is_empty());
        if matches!(settlement.become_, Step::Stop(Stopped)) {
            stops += 1;
            assert!(settlement.sends.inner.is_empty());
        } else {
            assert_eq!(settlement.become_, Step::Continue);
        }
        let SendLayer {
            owned: NoSends,
            inner: sends,
        } = settlement.sends;
        for item in sends {
            let SettledItem::Attempted(ItemSettlement::Rejected { item, reason }) = item else {
                panic!("the exact closed recipient refuses the complete payment");
            };
            assert_eq!(reason, ExactDeliveryReason::ClosedRecipient);
            assert_eq!(item.to, original_destination);
            let duplicate = rejected.insert(item.message.invoice, item.message);
            assert!(duplicate.is_none());
        }
    }
    assert_eq!(stops, 1);
    assert!(rejected.contains_key(&7));
    assert_eq!(unread.len() + rejected.len(), 3);
    for (number, (dollars, cents, allocation, bytes)) in expected {
        let original = unread.remove(&number);
        let mapped = rejected.remove(&number);
        match (original, mapped) {
            (Some(invoice), None) => {
                assert_eq!(invoice.dollars, dollars);
                assert_eq!(invoice.receipt.as_ptr(), allocation);
                assert_eq!(invoice.receipt.as_ref(), bytes);
            }
            (None, Some(payment)) => {
                assert_eq!(payment.cents, cents);
                assert_eq!(payment.receipt.as_ptr(), allocation);
                assert_eq!(payment.receipt.as_ref(), bytes);
            }
            _ => panic!("each original invoice has exactly one surviving owner"),
        }
    }
    assert!(unread.is_empty());
    assert!(rejected.is_empty());
    let unexpected = customer.receive().await;
    assert!(unexpected.is_none());
}
