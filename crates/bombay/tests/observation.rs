use behavior_actors::{EstablishedObservation, ObservationId, ObserveEstablished};
use bombay::ActorNotificationReceipts;
use bombay::behavior::{
    ActionSettlement, BehaviorBase, ExactDeliveryReason, ItemSettlement, MessageProtocol,
    SendLayer, SettledItem, Stopped,
};
use bombay::prelude::*;
use bombay::{ActorCustody, ApplicationScope};

mod application_support;

use application_support::assert_completed;

type Peer = MessageProtocol<MailAddr, String>;
type Notice = MessageProtocol<MailAddr, EstablishedObservation<Peer>>;

struct Watcher {
    recipient: EstablishedRecipient<Notice>,
}

#[bombay::actor(sends = pub(crate) {
    reports: Vec<EstablishedDelivery<Notice>>
})]
impl Watcher {
    #[allow(
        clippy::unused_self,
        reason = "the observation-only actor has no inhabited user-message lane"
    )]
    fn receive(&mut self, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[tokio::test(flavor = "current_thread")]
async fn exact_observation_survives_outer_shutdown_and_transfers_the_original_terminal_report() {
    let scope = ApplicationScope::new();
    let target = scope
        .external::<Peer>()
        .expect("the actual watched endpoint exists");
    let mut observer = scope
        .external::<Notice>()
        .expect("the actual report recipient exists");
    let request = ObserveEstablished::new(ObservationId(4), target.recipient());
    let watcher = Watcher {
        recipient: observer.recipient(),
    }
    .observe_termination(request, |watcher, report| {
        Actions::stop().with_send(EstablishedDelivery::new(watcher.recipient.clone(), report))
    })
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = scope
        .with_root(watcher)
        .run_with(|_| async move {
            drop(target);
            let report = observer.receive().await;
            observer.close_admission();
            let remaining = observer.receive().await;
            (report, remaining, observer.recipient())
        })
        .await
        .unwrap_or_else(|_| panic!("the caller hosts actual observation interpretation"))
        .into_parts();
    let ((report, remaining, recipient), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work and independent retirement remain owned"));
    cleanup.expect("the complete monitor retirement is joined");
    let terminal = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
    assert!(remaining.is_none());
    assert_eq!(
        terminal
            .1
            .behavior()
            .expect("the exact leaf survives")
            .base()
            .recipient,
        recipient
    );
    let report = report.expect("the real terminal report was delivered");
    assert_eq!(report.from, terminal.0.address());
    let EstablishedObservation::Stopped {
        relationship,
        outcome,
        at,
    } = report.message
    else {
        panic!("only the original matching terminal report reaches the reaction");
    };
    assert_eq!(relationship.id(), ObservationId(4));
    assert_eq!(outcome, Ok(Exit::Normal));
    drop((relationship, at));
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn exact_observation_survives_inner_shutdown_and_transfers_the_original_terminal_report() {
    let scope = ApplicationScope::new();
    let target = scope
        .external::<Peer>()
        .expect("the actual watched endpoint exists");
    let mut observer = scope
        .external::<Notice>()
        .expect("the actual report recipient exists");
    let request = ObserveEstablished::new(ObservationId(5), target.recipient());
    let watcher = Watcher {
        recipient: observer.recipient(),
    }
    .stop_on_shutdown()
    .observe_termination(request, |watcher, report| {
        Actions::stop().with_send(EstablishedDelivery::new(
            watcher.base().recipient.clone(),
            report,
        ))
    });
    let (outcome, native, notifications, ()) = scope
        .with_root(watcher)
        .run_with(|_| async move {
            drop(target);
            let report = observer.receive().await;
            observer.close_admission();
            let remaining = observer.receive().await;
            (report, remaining, observer.recipient())
        })
        .await
        .unwrap_or_else(|_| panic!("the caller hosts the exact inner-shutdown observation"))
        .into_parts();
    let ((report, remaining, recipient), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("observation cannot erase the independent work result"));
    cleanup.expect("the actual observation and inner actor retire together");
    let terminal = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
    assert!(remaining.is_none());
    assert_eq!(
        terminal
            .1
            .behavior()
            .expect("the exact leaf survives")
            .base()
            .recipient,
        recipient
    );
    let report = report.expect("the complete terminal report survives the inner shutdown layer");
    assert_eq!(report.from, terminal.0.address());
    let EstablishedObservation::Stopped {
        relationship,
        outcome,
        at,
    } = report.message
    else {
        panic!("the reaction transfers only the matching stopped relationship");
    };
    assert_eq!(relationship.id(), ObservationId(5));
    assert_eq!(outcome, Ok(Exit::Normal));
    drop((relationship, at));
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_while_observing_preserves_the_live_target_and_complete_retirement() {
    let scope = ApplicationScope::new();
    let target = scope
        .external::<Peer>()
        .expect("the watched endpoint remains independently owned");
    let mut observer = scope
        .external::<Notice>()
        .expect("the actual report consumer is established");
    let request = ObserveEstablished::new(ObservationId(6), target.recipient());
    let watcher = Watcher {
        recipient: observer.recipient(),
    }
    .observe_termination(request, |_, _| {
        panic!("shutdown must not fabricate a peer's terminal report")
    })
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = scope
        .with_root(watcher)
        .run_with(|application| async move {
            let sender = application
                .sender_to(target.recipient())
                .expect("the actual peer capability accepts an independent sender");
            let shutdown = application.shutdown();
            let termination = application.termination().await;
            (shutdown, termination, target, sender)
        })
        .await
        .unwrap_or_else(|_| panic!("the caller owns typed shutdown and receiving"))
        .into_parts();
    let ((shutdown, termination, mut target, sender), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("shutdown retains the complete original external owners"));
    assert_eq!(shutdown, Ok(()));
    assert_eq!(termination, Ok(Exit::Normal));
    cleanup.expect("the pending observation task joins before terminal custody returns");
    let terminal = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
    observer.close_admission();
    let fabricated = observer.receive().await;
    assert!(fabricated.is_none());
    assert_eq!(
        terminal
            .1
            .behavior()
            .expect("the exact leaf is retained")
            .base()
            .recipient,
        observer.recipient()
    );
    assert_completed(terminal, None);
    let sent = sender.send(String::from("still accepting")).await;
    assert!(sent.is_ok());
    let admitted = target
        .receive()
        .await
        .expect("observer retirement did not close the watched endpoint");
    assert_eq!(admitted.from, sender.address());
    assert_eq!(admitted.message, "still accepting");
    target.close_admission();
    let command = String::from("rejected after closure");
    let address = command.as_ptr();
    let rejection = sender
        .send(command)
        .await
        .expect_err("closed admission returns the original command");
    let command = rejection.into_message();
    assert_eq!(command.as_ptr(), address);
    assert_eq!(command, "rejected after closure");
    let remaining = target.receive().await;
    assert!(remaining.is_none());
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "inspect every original retirement field and the whole rejected terminal report in one native trace"
)]
async fn rejected_report_delivery_retains_the_whole_original_report_and_normal_actor_stop() {
    let scope = ApplicationScope::new();
    let target = scope
        .external::<Peer>()
        .expect("the genuine observed endpoint exists");
    let mut observer = scope
        .external::<Notice>()
        .expect("the genuine closed report recipient exists");
    observer.close_admission();
    let recipient = observer.recipient();
    let request = ObserveEstablished::new(ObservationId(7), target.recipient());
    let watcher = Watcher {
        recipient: recipient.clone(),
    }
    .observe_termination(request, |watcher, report| {
        Actions::stop().with_send(EstablishedDelivery::new(watcher.recipient.clone(), report))
    });
    let (outcome, native, notifications, ()) = scope
        .with_root(watcher)
        .run_with(|application| async move {
            drop(target);
            application.termination().await
        })
        .await
        .unwrap_or_else(|_| panic!("the exact observation interpreter runs on the current host"))
        .into_parts();
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("failed report delivery preserves the independent work result"));
    assert_eq!(termination, Ok(Exit::Normal));
    cleanup.expect("the failed effect retains complete joined custody");
    let (origin, retirement) = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                capability_failures,
                unread_owner_cancellation,
                behavior,
                control,
                user,
                descendants,
                child_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
        interpretation,
        source,
        settlements,
        completion,
    } = retirement
    else {
        panic!("the normal actor stop retains its independent rejected delivery");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert!(control.is_empty());
    assert_eq!(user, []);
    assert_eq!(descendants, []);
    assert_eq!(child_failures, []);
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(behavior.base().recipient, recipient);
    let mut settlements = settlements.into_iter();
    let ActionSettlement {
        creations,
        sends:
            SendLayer {
                owned: observations,
                inner: WatcherSettlements { reports },
            },
        become_,
    } = settlements
        .next()
        .expect("the whole rejected reaction settlement remains");
    let remaining = settlements.next();
    assert!(remaining.is_none());
    assert!(creations.is_empty());
    assert!(observations.is_empty());
    assert_eq!(become_, Step::Stop(Stopped));
    let mut reports = reports.into_iter();
    let SettledItem::Attempted(ItemSettlement::Rejected {
        item: delivery,
        reason,
    }) = reports
        .next()
        .expect("the original rejected delivery is retained")
    else {
        panic!("closed delivery must return its complete input");
    };
    let remaining = reports.next();
    assert!(remaining.is_none());
    assert_eq!(reason, ExactDeliveryReason::ClosedRecipient);
    assert_eq!(delivery.to, recipient);
    let EstablishedObservation::Stopped {
        relationship,
        outcome,
        at,
    } = delivery.message
    else {
        panic!("the exact stopped report survives its delivery failure");
    };
    assert_eq!(relationship.id(), ObservationId(7));
    assert_eq!(outcome, Ok(Exit::Normal));
    drop((relationship, at));
    let fabricated = observer.receive().await;
    assert!(fabricated.is_none());
}
