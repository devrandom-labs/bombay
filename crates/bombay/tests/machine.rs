//! Machine contracts through Bombay's ordinary Application and exact native custody.

use bombay::behavior::{
    Actions, ExactDeliveryReason, ItemSettlement, MessageProtocol, Never, NoBirths, Recipient,
    SettledItem, Step, Stopped, User, delegate_transition, initialize,
};
use bombay::composition::InputLimit;
use bombay::prelude::{Crash, Exit, Machine, Move};
use bombay::{
    ActorCustody, ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement,
    ApplicationScope, Completion, MailAddr, MailboxCapacity, RetirementAssessment, WorkOutcome,
};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::runtime::Builder;

#[derive(Clone, Copy, Debug)]
enum Phase {
    Closed,
    Open(u8),
}

impl PartialEq for Phase {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Closed, Self::Closed) | (Self::Open(_), Self::Open(_))
        )
    }
}

#[derive(Debug)]
enum Command {
    Hold(Box<[u64]>),
    Deposit(Box<[u64]>),
    Refuse(Box<[u64]>),
    Open,
    Revise(u8),
    Stop,
}

#[derive(Default)]
struct Account {
    deposits: Vec<u64>,
    attempts: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct AccountError;

fn receive(
    phase: Phase,
    account: &mut Account,
    command: &Command,
) -> Result<Move<Phase>, AccountError> {
    match (phase, command) {
        (_, Command::Stop) => Ok(Move::Stop),
        (_, Command::Open) => Ok(Move::Goto(Phase::Open(1))),
        (Phase::Open(_), Command::Revise(revision)) => Ok(Move::Goto(Phase::Open(*revision))),
        (Phase::Closed, _) | (Phase::Open(_), Command::Hold(_)) => {
            account.attempts += 1;
            Ok(Move::Defer)
        }
        (Phase::Open(_), Command::Deposit(amounts)) => {
            account.deposits.extend_from_slice(amounts);
            Ok(Move::Stay)
        }
        (Phase::Open(_), Command::Refuse(amounts)) => {
            account.deposits.extend_from_slice(amounts);
            Err(AccountError)
        }
    }
}

fn continuing(actions: &Actions<MailAddr, Never, Vec<Never>, NoBirths>) {
    assert_eq!(actions.sends.as_slice(), []);
    assert!(actions.creates.is_empty());
    assert_eq!(actions.become_, Step::Continue);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one joined failure journey preserves every original input and independent native custody lane"
)]
fn native_refusal_preserves_progress_and_each_original_input() {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let capacity = MailboxCapacity::new(8).unwrap();
    let root = Machine::<MailAddr, _, _, _, _>::new(Account::default(), Phase::Closed, receive);
    let prefix: Box<[u64]> = Box::from([11_u64]);
    let prefix_pointer = prefix.as_ptr();
    let refused: Box<[u64]> = Box::from([17_u64]);
    let refused_pointer = refused.as_ptr();
    let suffix: Box<[u64]> = Box::from([19_u64]);
    let suffix_pointer = suffix.as_ptr();
    let (execution, (work, published, native, notifications, ())) = host.block_on(async {
        let prepared = ApplicationScope::new()
            .mailbox_capacity(capacity)
            .with_root(root)
            .execute_with(|application| async move {
                let first = application.sender().unwrap();
                let second = application.sender().unwrap();
                let origins = [first.address(), second.address()];
                let mut admissions = Vec::new();
                for (sender, command) in [
                    (&first, Command::Hold(prefix)),
                    (&first, Command::Deposit(Box::from([13_u64]))),
                    (&second, Command::Refuse(refused)),
                    (&first, Command::Deposit(suffix)),
                    (&first, Command::Open),
                ] {
                    admissions.push(sender.send(command).await);
                }
                // A source omission that accepts the refusal must still stop.
                // A failed root may instead return this complete Stop on send.
                let stop = first.send(Command::Stop).await;
                let termination = application.termination().await;
                let retirement = application.retirement().await;
                (origins, admissions, stop, termination, retirement)
            })
            .unwrap_or_else(|_| panic!("the test enters its ordinary Application host"));
        let (execution, receiving) = prepared;
        let execution = AssertUnwindSafe(execution).catch_unwind().await;
        let received = receiving.await.into_parts();
        (execution, received)
    });
    drop(host);

    let WorkOutcome::Completed {
        output: (origins, admissions, stop, termination, report),
        cleanup,
    } = work
    else {
        panic!("the completed Work and its original observations survive");
    };
    let (origin, native) = native.expect("the joined native root is received");
    let ActorRetirement::BehaviorFailed {
        custody:
            ActorCustody {
                mut behavior,
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
        error,
    } = native
    else {
        panic!("the domain refusal retains its original BehaviorFailed result");
    };
    let held = behavior.take();
    let repeated = behavior.take();

    // Primary inversion oracle: the accepted Open from A cannot replace B's refusal.
    assert_eq!(error.event.from, origins[1]);
    assert_ne!(origins[0], origins[1]);
    assert_eq!(error.cause, AccountError);
    let Command::Refuse(payload) = error.event.message else {
        panic!("the error owns the refused command, not its replay trigger");
    };
    assert_eq!(payload.as_ptr(), refused_pointer);
    assert_eq!(payload.as_ref(), &[17]);
    assert_eq!(behavior.state().deposits, [13, 17]);
    let Phase::Open(revision) = behavior.phase() else {
        panic!("the accepted phase change survives the later refusal");
    };
    assert_eq!(revision, 1);
    assert_eq!(held.len(), 2);
    let mut held = held.into_iter();
    let prefix = held.next().unwrap();
    let suffix = held.next().unwrap();
    let tail = held.next();
    assert_eq!(prefix.from, origins[0]);
    let Command::Hold(payload) = prefix.message else {
        panic!("the reheld prefix remains before the unread suffix");
    };
    assert_eq!(payload.as_ptr(), prefix_pointer);
    assert_eq!(payload.as_ref(), &[11]);
    assert_eq!(suffix.from, origins[0]);
    let Command::Deposit(payload) = suffix.message else {
        panic!("the unread deposit remains in native Machine custody");
    };
    assert_eq!(payload.as_ptr(), suffix_pointer);
    assert_eq!(payload.as_ref(), &[19]);
    assert!(tail.is_none());
    assert!(repeated.is_empty());
    assert_eq!(behavior.held(), 0);
    assert_eq!(admissions.len(), 5);
    assert!(admissions.iter().all(Result::is_ok));
    match stop {
        Ok(()) => {
            assert_eq!(user.len(), 1);
            assert_eq!(user[0].from, origins[0]);
            assert!(matches!(&user[0].message, Command::Stop));
        }
        Err(original) => {
            let message = original.into_message();
            assert!(matches!(&message, Command::Stop));
            assert!(user.is_empty());
        }
    }
    assert!(execution.is_ok());
    assert!(cleanup.is_ok());
    assert_eq!(termination, Err(Crash::Failed));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
    assert_eq!(published, Some(report));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(matches!(
        &notifications,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        })
    ));
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
    for settlement in settlements {
        assert_eq!(settlement.sends.as_slice(), []);
        assert!(settlement.creations.is_empty());
        assert_eq!(settlement.become_, Step::Continue);
    }
}

#[derive(Clone, Copy)]
enum Customer {
    Open,
    Closed,
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_machine_delivers_complete_refusals_to_the_open_customer() {
    deposits(Customer::Open).await;
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_machine_keeps_closed_customer_refusals_in_native_settlements() {
    deposits(Customer::Closed).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one joined journey accounts for original deposits across held inputs, delivered refusals and native rejected settlements"
)]
async fn deposits(customer: Customer) {
    let capacity = MailboxCapacity::new(8).unwrap();
    let scope = ApplicationScope::new().mailbox_capacity(capacity);
    let refused = scope
        .external::<MessageProtocol<MailAddr, User<MailAddr, Command>>>()
        .unwrap();
    if let Customer::Closed = customer {
        refused.close_admission();
    }
    let destination = refused.recipient();
    let expected_destination = destination.clone();
    let root = Machine::bounded(
        Account::default(),
        Phase::Closed,
        receive,
        InputLimit::new(1, destination),
    );
    let originals: [Box<[u64]>; 4] = [
        Box::from([23_u64]),
        Box::from([29_u64]),
        Box::from([31_u64]),
        Box::from([37_u64]),
    ];
    let expected = originals
        .each_ref()
        .map(|amounts| (amounts[0], amounts.as_ptr()));
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        scope.with_root(root).run_with(|application| async move {
            let first = application.sender().unwrap();
            let second = application.sender().unwrap();
            let origins = [first.address(), second.address()];
            let mut admissions = Vec::new();
            for (sender, amounts) in [&first, &second, &first, &second]
                .into_iter()
                .zip(originals)
            {
                admissions.push(sender.send(Command::Deposit(amounts)).await);
            }
            let stop = first.send(Command::Stop).await;
            let termination = application.termination().await;
            let retirement = application.retirement().await;
            // Eight sink slots hold all three refusals without receiving here.
            (origins, admissions, stop, termination, retirement, refused)
        }),
    )
    .await
    .expect("the actual Stop and joined retirement complete")
    .unwrap_or_else(|_| panic!("the finite Machine starts"));
    let (work, published, native, notifications, ()) = outcome.into_parts();
    let WorkOutcome::Completed {
        output: (origins, admissions, stop, termination, report, mut receiver),
        cleanup,
    } = work
    else {
        panic!("completed Work returns the real external receiver");
    };
    let (origin, native) = native.expect("the exact joined Machine survives");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                mut behavior,
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
        panic!("finite holding continues to the ordinary Stop");
    };
    receiver.close_admission();
    let mut delivered = Vec::new();
    while let Some(input) = receiver.receive().await {
        delivered.push(input);
    }
    let held = behavior.take();
    let repeated = behavior.take();

    // Both destinations reach cleanup under < -> <=; neither waits for three replies.
    assert_eq!(held.len(), 1);
    assert_eq!(behavior.state().attempts, 4);
    assert_eq!(behavior.state().deposits.as_slice(), [] as [u64; 0]);
    assert!(repeated.is_empty());
    assert_eq!(behavior.held(), 0);
    assert_eq!(admissions.len(), 4);
    assert!(admissions.iter().all(Result::is_ok));
    assert!(stop.is_ok());
    assert_ne!(origins[0], origins[1]);
    assert!(cleanup.is_ok());
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    let expected_failures = match customer {
        Customer::Open => ActorFailureAssessment::NoFailuresFound,
        Customer::Closed => ActorFailureAssessment::FailuresFound,
    };
    assert_eq!(report.failures(), expected_failures);
    assert_eq!(published, Some(report));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(completion, Completion::Stopped);
    assert!(matches!(
        &notifications,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        })
    ));
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
    let mut rejected = Vec::new();
    let mut stops = 0;
    for settlement in settlements {
        assert!(settlement.creations.is_empty());
        match settlement.become_ {
            Step::Continue => {}
            Step::Stop(Stopped) => stops += 1,
            Step::Goto(never) => match never {},
        }
        for delivery in settlement.sends {
            let SettledItem::Attempted(ItemSettlement::Rejected { item, reason }) = delivery else {
                panic!("successful exact delivery retains no refused source item");
            };
            assert_eq!(reason, ExactDeliveryReason::ClosedRecipient);
            assert_eq!(item.to, expected_destination);
            rejected.push(item.message);
        }
    }
    assert_eq!(stops, 1);
    match customer {
        Customer::Open => {
            assert_eq!(delivered.len(), 3);
            assert!(rejected.is_empty());
        }
        Customer::Closed => {
            assert!(delivered.is_empty());
            assert_eq!(rejected.len(), 3);
        }
    }
    let held_number = match &held.front().unwrap().message {
        Command::Deposit(amounts) => amounts[0],
        _ => panic!("the first original deposit remains held"),
    };
    assert_eq!(held_number, 23);
    let delivered = delivered.into_iter().map(|input| {
        assert_eq!(input.from, MailAddr::APPLICATION_ROOT);
        input.message
    });
    let mut recovered = BTreeMap::new();
    for input in held.into_iter().chain(rejected).chain(delivered) {
        let Command::Deposit(amounts) = input.message else {
            panic!("each custody lane contains an original deposit");
        };
        let duplicate = recovered.insert(amounts[0], (input.from, amounts));
        assert!(duplicate.is_none());
    }
    assert_eq!(recovered.len(), 4);
    for ((number, pointer), sender) in expected
        .into_iter()
        .zip([origins[0], origins[1], origins[0], origins[1]])
    {
        let (original_sender, amounts) = recovered.remove(&number).unwrap();
        assert_eq!(original_sender, sender);
        assert_eq!(amounts.as_ref(), &[number]);
        assert_eq!(amounts.as_ptr(), pointer);
    }
    assert!(recovered.is_empty());
}

#[test]
fn zero_slots_returns_original_actions_after_the_callback_mutation() {
    let refused =
        Recipient::<MessageProtocol<MailAddr, User<MailAddr, Command>>>::global(MailAddr(101));
    let mut account = Machine::bounded(
        Account::default(),
        Phase::Closed,
        receive,
        InputLimit::new(0, refused),
    );
    let initial = initialize(&mut account).unwrap();
    assert!(initial.sends.is_empty());
    assert!(initial.creates.is_empty());
    assert_eq!(initial.become_, Step::Continue);
    let amounts: Box<[u64]> = Box::from([103_u64]);
    let original = amounts.as_ptr();
    let actions = delegate_transition(
        &mut account,
        User::new(MailAddr(107), Command::Deposit(amounts)),
    )
    .unwrap();
    let mut sends = actions.sends;
    let delivery = sends
        .pop()
        .expect("zero slots returns one complete original");
    assert!(sends.is_empty());
    assert_eq!(delivery.to, refused);
    assert_eq!(delivery.message.from, MailAddr(107));
    let Command::Deposit(amounts) = delivery.message.message else {
        panic!("the refusal contains the original deposit");
    };
    assert_eq!(amounts.as_ptr(), original);
    assert_eq!(amounts.as_ref(), &[103]);
    assert!(actions.creates.is_empty());
    assert_eq!(actions.become_, Step::Continue);
    assert_eq!(account.state().attempts, 1);
    assert_eq!(account.held(), 0);
}

#[test]
fn admitted_replay_keeps_its_slot_without_a_new_capacity_refusal() {
    let refused =
        Recipient::<MessageProtocol<MailAddr, User<MailAddr, Command>>>::global(MailAddr(109));
    let mut account = Machine::bounded(
        Account::default(),
        Phase::Closed,
        receive,
        InputLimit::new(1, refused),
    );
    let initialized = initialize(&mut account).unwrap();
    assert!(initialized.sends.is_empty());
    assert!(initialized.creates.is_empty());
    assert_eq!(initialized.become_, Step::Continue);
    let amounts: Box<[u64]> = Box::from([113_u64]);
    let original = amounts.as_ptr();
    let deferred = delegate_transition(
        &mut account,
        User::new(MailAddr(127), Command::Hold(amounts)),
    )
    .unwrap();
    assert!(deferred.sends.is_empty());
    assert!(deferred.creates.is_empty());
    assert_eq!(deferred.become_, Step::Continue);
    let replayed =
        delegate_transition(&mut account, User::new(MailAddr(131), Command::Open)).unwrap();
    assert!(replayed.sends.is_empty());
    assert!(replayed.creates.is_empty());
    assert_eq!(replayed.become_, Step::Continue);
    assert_eq!(account.held(), 1);
    assert_eq!(account.state().attempts, 2);
    let mut originals = account.take();
    let held = originals.pop_front().unwrap();
    assert!(originals.is_empty());
    assert_eq!(held.from, MailAddr(127));
    let Command::Hold(amounts) = held.message else {
        panic!("replay preserves the admitted original");
    };
    assert_eq!(amounts.as_ptr(), original);
    assert_eq!(amounts.as_ref(), &[113]);
}

#[test]
fn exact_equal_phase_designations_preserve_complete_actions_without_replay() {
    let mut account =
        Machine::<MailAddr, _, _, _, _>::new(Account::default(), Phase::Closed, receive);
    let initialized = initialize(&mut account).unwrap();
    continuing(&initialized);
    let held = delegate_transition(
        &mut account,
        User::new(MailAddr(137), Command::Hold(Box::from([139_u64]))),
    )
    .unwrap();
    continuing(&held);
    let revision =
        delegate_transition(&mut account, User::new(MailAddr(149), Command::Revise(2))).unwrap();
    continuing(&revision);
    let replayed =
        delegate_transition(&mut account, User::new(MailAddr(151), Command::Open)).unwrap();
    continuing(&replayed);
    let Phase::Open(revision) = account.phase() else {
        panic!("replay designates the open phase");
    };
    assert_eq!(revision, 2);
    assert_eq!(account.state().attempts, 3);
    assert_eq!(account.held(), 1);
    let direct =
        delegate_transition(&mut account, User::new(MailAddr(157), Command::Revise(3))).unwrap();
    continuing(&direct);
    let Phase::Open(revision) = account.phase() else {
        panic!("direct designation preserves its exact phase");
    };
    assert_eq!(revision, 3);
    assert_eq!(account.state().attempts, 3);
    assert_eq!(account.held(), 1);
}
