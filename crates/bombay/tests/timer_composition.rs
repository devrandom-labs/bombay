use behavior_actors::{
    OwnedScheduleAfter, ScheduleAfter, ScheduleAfterRejection, TimerElapsed, TimerGeneration,
};
use bombay::{
    ActorCustody, ActorNotificationReceipts, ActorRetirement, SettlementFailure, WorkOutcome,
};
use core::time::Duration;

use bombay::ApplicationScope;
use bombay::behavior::{
    Behavior, BehaviorBase, EventLayer, Here, InterpreterRequests, ItemSettlement, MessageProtocol,
    Never, NoBirths, NoSends, Protocol, SendLayer, SettledItem, User,
};
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
async fn independent_timers_preserve_every_wrapper_notice_and_complete_retirement() {
    let scope = ApplicationScope::new();
    let mut observer = scope
        .external::<MessageProtocol<MailAddr, SessionNotice>>()
        .expect("the session's actual notice recipient is established before construction");
    let session = Session {
        recipient: observer.recipient(),
    }
    .with_one_shot(Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.recipient.clone(),
            SessionNotice::Reminder,
        ))
    })
    .with_one_shot(Duration::ZERO, |session| {
        Actions::cont().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Warning,
        ))
    })
    .with_periodic(Duration::ZERO, |session| {
        Actions::stop().with_send(EstablishedDelivery::new(
            session.base().recipient.clone(),
            SessionNotice::Expired,
        ))
    })
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = scope
        .with_root(session)
        .run_with(|application| async move {
            tokio::time::timeout(Duration::from_secs(5), application.termination())
                .await
                .expect("timer termination is bounded")
        })
        .await
        .unwrap_or_else(|_| panic!("the caller hosts the session's real timer interpretation"))
        .into_parts();
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the normal timer expiry retains its independent work result"));
    assert_eq!(termination, Ok(Exit::Normal));
    cleanup.expect("the complete session retirement is joined");
    let terminal = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
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
    let session = NamedSession {
        recipient: observer.recipient(),
    }
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = scope
        .with_root(session)
        .run_with(|application| async move {
            tokio::time::timeout(Duration::from_secs(5), application.termination())
                .await
                .expect("timer termination is bounded")
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller host interprets replacement"))
        .into_parts();
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("timer replacement cannot erase the work result"));
    assert_eq!(termination, Ok(Exit::Normal));
    cleanup.expect("normal expiry joins the original actor and timer custody");
    let terminal = native.expect("the original native retirement remains available");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("the independent notification results remain owned");
    };
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

// This advanced control emits explicitly named effects; ordinary decorations
// above never ask callers for a timer name.
struct NamedSession {
    recipient: EstablishedRecipient<MessageProtocol<MailAddr, SessionNotice>>,
}

impl Protocol for NamedSession {
    type Addr = MailAddr;
    type Msg = Never;
}

impl BehaviorBase for NamedSession {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for NamedSession {
    type Protocol = Self;
    type Event = EventLayer<TimerElapsed, EventLayer<TimerElapsed, User<MailAddr, Never>>>;
    type Sends = SendLayer<
        InterpreterRequests<ScheduleAfter>,
        SendLayer<InterpreterRequests<ScheduleAfter>, SessionSends>,
    >;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: bombay::behavior::InitializationTurn) -> BehaviorActed<Self> {
        // The inner reminder port schedules first. The outer warning port
        // deliberately replaces that same actor-local name across ingress paths.
        Ok(Actions::new(
            SendLayer::new(
                InterpreterRequests::new(vec![
                    ScheduleAfter::new(TimerId(1), TimerGeneration(1), Duration::ZERO),
                    ScheduleAfter::new(TimerId(3), TimerGeneration(0), Duration::ZERO),
                ]),
                SendLayer::new(
                    InterpreterRequests::one(ScheduleAfter::new(
                        TimerId(1),
                        TimerGeneration(0),
                        Duration::ZERO,
                    )),
                    SessionSends {
                        notices: Vec::new(),
                    },
                ),
            ),
            bombay::behavior::Creations::empty(),
            Step::Continue,
        ))
    }

    fn transition(
        &mut self,
        _: bombay::behavior::ActiveTurn,
        event: Self::Event,
    ) -> BehaviorActed<Self> {
        let (notice, actions) = match event {
            EventLayer::Inner(EventLayer::Owned(TimerElapsed {
                id: TimerId(1),
                generation: TimerGeneration(0),
            })) => (SessionNotice::Reminder, Actions::cont()),
            EventLayer::Owned(TimerElapsed {
                id: TimerId(1),
                generation: TimerGeneration(1),
            }) => (SessionNotice::Warning, Actions::cont()),
            EventLayer::Owned(TimerElapsed {
                id: TimerId(3),
                generation: TimerGeneration(0),
            }) => (SessionNotice::Expired, Actions::stop()),
            EventLayer::Owned(_) | EventLayer::Inner(EventLayer::Owned(_)) => {
                return Ok(Actions::cont());
            }
            EventLayer::Inner(EventLayer::Inner(user)) => match user.message {},
        };
        Ok(actions.with_send(EstablishedDelivery::new(self.recipient.clone(), notice)))
    }
}

struct Appointment {
    recipient: EstablishedRecipient<MessageProtocol<MailAddr, SessionNotice>>,
    note: Box<[u8]>,
}

#[bombay::actor(message = Never, sends = pub(crate) {
    notices: Vec<EstablishedDelivery<MessageProtocol<MailAddr, SessionNotice>>>
})]
impl Appointment {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the actor initializer uses the owning controlled-error Behavior result"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().with_send(EstablishedDelivery::new(
            self.recipient.clone(),
            SessionNotice::Reminder,
        )))
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "retain the complete original timer rejection, independent effects and every retirement custody field in one native journey"
)]
async fn rejected_owned_timer_preserves_prefix_suffix_original_state_and_retirement() {
    let scope = ApplicationScope::new();
    let mut observer = scope
        .external::<MessageProtocol<MailAddr, SessionNotice>>()
        .expect("the original notice recipient is established before initialization");
    let note = Box::<[u8]>::from([127, 131, 137]);
    let allocation = note.as_ptr();
    let appointment = Appointment {
        recipient: observer.recipient(),
        note,
    }
    .with_one_shot(Duration::MAX, |appointment| {
        Actions::stop().with_send(EstablishedDelivery::new(
            appointment.recipient.clone(),
            SessionNotice::Expired,
        ))
    })
    .with_one_shot(Duration::ZERO, |appointment| {
        Actions::stop().with_send(EstablishedDelivery::new(
            appointment.base().recipient.clone(),
            SessionNotice::Warning,
        ))
    })
    .stop_on_shutdown();
    let entered = tokio::time::timeout(Duration::from_secs(5), scope.with_root(appointment).run())
        .await
        .expect("initial scheduling refusal has a finite complete retirement");
    let (work, native, notifications, ()) = entered
        .unwrap_or_else(|_| panic!("the actual caller host is entered"))
        .into_parts();
    let WorkOutcome::NotInvoked {
        work: None,
        startup_error: Some(startup_error),
        cleanup,
    } = work
    else {
        panic!("a refused initialization cannot grant application readiness");
    };
    cleanup.expect("native cleanup finishes independently of timer refusal");
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original notification results are received");
    };
    let (origin, retirement) = native.expect("the original actor retirement remains owned");
    let ActorRetirement::EffectsFailed {
        custody,
        initialization,
        interpretation,
        source,
        settlements,
        error,
    } = retirement
    else {
        panic!("the original timer rejection remains an effect failure");
    };
    assert_eq!(error, SettlementFailure::Rejected);
    assert!(initialization.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorCustody {
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
    } = custody;
    assert_eq!(behavior.base().note.as_ptr(), allocation);
    assert_eq!(&*behavior.base().note, &[127, 131, 137]);
    assert_eq!(behavior.base().recipient, observer.recipient());
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants, []);
    assert_eq!(child_failures, []);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());

    let mut settlements = settlements.into_iter();
    let settled = settlements
        .next()
        .expect("one complete initialization settlement");
    let extra = settlements.next();
    assert!(extra.is_none());
    assert!(settled.creations.is_empty());
    assert_eq!(settled.become_, Step::Continue);
    let SendLayer {
        owned: NoSends,
        inner:
            SendLayer {
                owned: accepted,
                inner:
                    SendLayer {
                        owned: rejected,
                        inner: receipts,
                    },
            },
    } = settled.sends;
    assert_eq!(
        rejected,
        [SettledItem::Attempted(ItemSettlement::Rejected {
            item: OwnedScheduleAfter::new(TimerGeneration(0), Duration::MAX),
            reason: ScheduleAfterRejection::DeadlineOverflow,
        })]
    );
    let [SettledItem::Attempted(ItemSettlement::Accepted(receipt))] = accepted.as_slice() else {
        panic!("an independent later timer is accepted after the ordinary rejection");
    };
    assert_eq!(receipt.id, Here);
    assert_eq!(receipt.generation, TimerGeneration(0));
    assert!(matches!(
        receipts.notices.as_slice(),
        [SettledItem::Attempted(ItemSettlement::Accepted(()))]
    ));
    observer.close_admission();
    let notice = observer
        .receive()
        .await
        .expect("the accepted prefix notice is independently received");
    assert_eq!(notice.from, origin.address());
    assert_eq!(notice.message, SessionNotice::Reminder);
    let remaining = observer.receive().await;
    assert!(remaining.is_none());
    drop((behavior, startup_error));
}
