use core::time::Duration;

use bombay::ApplicationScope;
use bombay::behavior::{BehaviorBase, MessageProtocol, Never};
use bombay::prelude::*;

mod application_support;

use application_support::assert_completed;

#[derive(Debug, PartialEq, Eq)]
enum SessionNotice {
    Reminder,
    Warning,
    Expired,
}

struct Session {
    recipient: EstablishedRecipient<MessageProtocol<MailAddr, SessionNotice>>,
}

#[bombay::actor(
    sends = pub(crate) {
        notices: Vec<EstablishedDelivery<MessageProtocol<MailAddr, SessionNotice>>>
    },
)]
impl Session {
    #[allow(
        clippy::unused_self,
        reason = "this timer-only actor has no inhabited user-message lane"
    )]
    fn receive(&mut self, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[tokio::test(flavor = "current_thread")]
async fn distinct_timer_ids_preserve_every_wrapper_notice_and_complete_retirement() {
    let scope = ApplicationScope::new();
    let mut observer = scope
        .external::<MessageProtocol<MailAddr, SessionNotice>>()
        .expect("the session's actual notice recipient is established before construction");
    let session = Session {
        recipient: observer.recipient(),
    }
    .with_one_shot(TimerId(1), Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.recipient.clone(),
            SessionNotice::Reminder,
        ))
    })
    .with_one_shot(TimerId(2), Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Warning,
        ))
    })
    .with_periodic(TimerId(3), Duration::ZERO, |session| {
        Actions::stop().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Expired,
        ))
    })
    .stop_on_shutdown();
    let outcome = scope
        .with_root(session)
        .run_with(|application| async move { application.termination().await })
        .await
        .unwrap_or_else(|_| panic!("the caller hosts the session's real timer interpretation"));
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the normal timer expiry retains its independent work result"));
    assert_eq!(termination, Ok(Exit::Normal));
    let terminal = cleanup.expect("the complete session retirement is joined");
    assert_eq!(
        terminal
            .1
            .behavior()
            .expect("the original session survives")
            .base()
            .recipient,
        observer.recipient()
    );
    observer.close_admission();
    let mut notices = Vec::new();
    while let Some(notice) = observer.receive().await {
        assert_eq!(notice.from, terminal.0.address());
        notices.push(notice.message);
    }
    assert_eq!(
        notices,
        [
            SessionNotice::Reminder,
            SessionNotice::Warning,
            SessionNotice::Expired
        ]
    );
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn equal_timer_ids_replace_the_inner_schedule_without_fabricating_its_notice() {
    let scope = ApplicationScope::new();
    let mut observer = scope
        .external::<MessageProtocol<MailAddr, SessionNotice>>()
        .expect("the actual observer preserves every accepted notice");
    let session = Session {
        recipient: observer.recipient(),
    }
    .with_one_shot(TimerId(1), Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.recipient.clone(),
            SessionNotice::Reminder,
        ))
    })
    .with_one_shot(TimerId(1), Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Warning,
        ))
    })
    .with_periodic(TimerId(3), Duration::ZERO, |session| {
        Actions::stop().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Expired,
        ))
    })
    .stop_on_shutdown();
    let outcome = scope
        .with_root(session)
        .run_with(|application| async move { application.termination().await })
        .await
        .unwrap_or_else(|_| panic!("the actual caller host interprets replacement"));
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("timer replacement cannot erase the work result"));
    assert_eq!(termination, Ok(Exit::Normal));
    let terminal = cleanup.expect("normal expiry joins the original actor and timer custody");
    assert_eq!(
        terminal
            .1
            .behavior()
            .expect("the exact session is retained")
            .base()
            .recipient,
        observer.recipient()
    );
    observer.close_admission();
    let mut notices = Vec::new();
    while let Some(notice) = observer.receive().await {
        assert_eq!(notice.from, terminal.0.address());
        notices.push(notice.message);
    }
    // This is the existing explicit-ID replacement law, and the falsifier of
    // assigning one hidden fixed ID to independently declared timer wrappers.
    assert_eq!(notices, [SessionNotice::Warning, SessionNotice::Expired]);
    assert_completed(terminal, None);
}
