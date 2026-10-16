//! A native timer conversion panic preserves every independent action owner.

use behavior_actors::{
    ScheduleAfter, ScheduleAt, TimerElapsed, TimerGeneration, TimerId, TimerScheduled,
};
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Here, InjectEvent, Interpretation,
    InterpretationProgress, InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoBirths,
    SettledItem, Step, User, UserEvent,
};
use bombay::{ActorRetirement, Application, MailAddr};
use core::time::Duration;
use std::time::Instant;

#[derive(behavior_macros::SendProduct)]
struct ReminderSends<Absolute, Relative> {
    absolute: Absolute,
    relative: Relative,
}

struct ReminderBatch {
    absolute: Vec<ScheduleAt>,
    relative: Vec<ScheduleAfter>,
}

enum ReminderEvent {
    Command(User<MailAddr, ReminderBatch>),
    Elapsed(TimerElapsed),
}

impl UserEvent for ReminderEvent {
    type Addr = MailAddr;
    type Message = ReminderBatch;

    fn user(from: MailAddr, message: ReminderBatch) -> Self {
        Self::Command(User::new(from, message))
    }

    fn into_user(self) -> Result<User<MailAddr, ReminderBatch>, Self> {
        match self {
            Self::Command(command) => Ok(command),
            elapsed @ Self::Elapsed(_) => Err(elapsed),
        }
    }
}

impl InjectEvent<TimerElapsed, Here> for ReminderEvent {
    fn inject_at(elapsed: TimerElapsed) -> Self {
        assert_ne!(elapsed.id, TimerId(2), "reminder event conversion failed");
        Self::Elapsed(elapsed)
    }
}

struct Reminders {
    notes: Box<[u8]>,
}

impl BehaviorBase for Reminders {
    type Base = Self;

    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for Reminders {
    type Protocol = MessageProtocol<MailAddr, ReminderBatch>;
    type Event = ReminderEvent;
    type Sends = ReminderSends<InterpreterRequests<ScheduleAt>, InterpreterRequests<ScheduleAfter>>;
    type Birth = NoBirths;
    type Ph = Never;
    type Error = Never;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            ReminderEvent::Command(command) => Ok(Actions {
                sends: ReminderSends {
                    absolute: InterpreterRequests::new(command.message.absolute),
                    relative: InterpreterRequests::new(command.message.relative),
                },
                ..Actions::cont()
            }),
            ReminderEvent::Elapsed(elapsed) => {
                self.notes[0] = u8::try_from(elapsed.id.0).expect("fixture timer IDs fit");
                Ok(Actions::stop())
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ReminderClock {
    Absolute,
    Relative,
}

#[expect(
    clippy::too_many_lines,
    reason = "the native panic must preserve the complete independent retirement and partial action"
)]
async fn conversion_panic_returns_timer_custody(clock: ReminderClock) {
    let deadline = Instant::now() + Duration::from_hours(24);
    let absolute = [
        ScheduleAt::new(TimerId(1), TimerGeneration(7), deadline),
        ScheduleAt::new(TimerId(2), TimerGeneration(11), deadline),
        ScheduleAt::new(TimerId(3), TimerGeneration(13), deadline),
    ];
    let relative = [
        ScheduleAfter::new(TimerId(1), TimerGeneration(17), Duration::from_hours(24)),
        ScheduleAfter::new(TimerId(2), TimerGeneration(19), Duration::from_hours(24)),
        ScheduleAfter::new(TimerId(3), TimerGeneration(23), Duration::from_hours(24)),
    ];
    let independent_absolute = ScheduleAt::new(TimerId(4), TimerGeneration(31), deadline);
    let independent = ScheduleAfter::new(TimerId(4), TimerGeneration(29), Duration::ZERO);
    let request = match clock {
        ReminderClock::Absolute => ReminderBatch {
            absolute: absolute.to_vec(),
            relative: vec![independent],
        },
        ReminderClock::Relative => ReminderBatch {
            absolute: vec![independent_absolute],
            relative: relative.to_vec(),
        },
    };
    let notes = Box::<[u8]>::from([31, 37]);
    let allocation = notes.as_ptr();
    let (work, native, notifications, ()) = Application::new(Reminders { notes })
        .run_with(|application| async move {
            let sender = application.sender().expect("ordinary application sender");
            sender
                .send(request)
                .await
                .unwrap_or_else(|_| panic!("the exact timer batch is admitted"));
            let termination =
                tokio::time::timeout(Duration::from_secs(5), application.termination())
                    .await
                    .expect("native conversion panic reaches retirement");
            assert_eq!(termination, Err(bombay::prelude::Crash::Panicked));
        })
        .await
        .unwrap_or_else(|_| panic!("native application starts"))
        .into_parts();
    let ((), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("business work independently observes termination"));
    cleanup.expect("cleanup joins the panicked actor");
    let notifications = notifications.expect("both native notification receipts survive");
    notifications.termination.expect("termination is published");
    notifications.retirement.expect("retirement is published");
    let (_, retirement) = native.expect("the complete native retirement survives");
    let ActorRetirement::HostExecutionPanicked {
        payload: _,
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("conversion panics during active action interpretation");
    };
    assert_eq!(custody.behavior.notes.as_ptr(), allocation);
    assert_eq!(&*custody.behavior.notes, &[31, 37]);
    assert!(settlements.is_empty());
    assert!(source.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    let Some(InterpretationProgress::Interpreting(((creations, sends), next))) = interpretation
    else {
        panic!("the original partial action survives");
    };
    assert!(matches!(next, Step::Continue));
    let Some(InterpretationProgress::Completed(Interpretation::Complete(creations))) = creations
    else {
        panic!("the creation lane completed before scheduling");
    };
    assert!(creations.is_empty());
    let Some(InterpretationProgress::Interpreting((absolute_rows, relative_rows))) = sends else {
        panic!("both timer lanes remain in their original product");
    };
    match clock {
        ReminderClock::Absolute => {
            let Some(InterpretationProgress::Original(relative_rows)) = relative_rows else {
                panic!("the independent relative timer remains untouched");
            };
            assert_eq!(
                relative_rows.into_iter().collect::<Vec<_>>(),
                vec![independent]
            );
            let Some(InterpretationProgress::Interpreting(rows)) = absolute_rows else {
                panic!("absolute timers retain the interrupted batch");
            };
            let mut rows = rows.into_iter();
            let Some(Some(InterpretationProgress::Completed(Interpretation::Complete(
                ItemSettlement::Accepted(receipt),
            )))) = rows.next()
            else {
                panic!("the accepted absolute prefix retains its actual receipt");
            };
            assert_eq!(
                receipt,
                TimerScheduled {
                    id: absolute[0].id,
                    generation: absolute[0].generation
                }
            );
            let Some(Some(InterpretationProgress::Interpreting((Some(pending), None)))) =
                rows.next()
            else {
                panic!("the original interrupted absolute timer must survive conversion panic");
            };
            assert_eq!(pending, absolute[1]);
            let Some(Some(InterpretationProgress::Original(suffix))) = rows.next() else {
                panic!("the absolute suffix remains unattempted");
            };
            assert_eq!(suffix, absolute[2]);
            assert!(rows.next().is_none());
        }
        ReminderClock::Relative => {
            let Some(InterpretationProgress::Completed(Interpretation::Complete(prefix))) =
                absolute_rows
            else {
                panic!("the independent absolute lane completed before relative scheduling");
            };
            let mut prefix = prefix.into_iter();
            let Some(SettledItem::Attempted(ItemSettlement::Accepted(receipt))) = prefix.next()
            else {
                panic!("the independent absolute lane retains its actual accepted receipt");
            };
            assert_eq!(
                receipt,
                TimerScheduled {
                    id: independent_absolute.id,
                    generation: independent_absolute.generation,
                }
            );
            assert!(prefix.next().is_none());
            let Some(InterpretationProgress::Interpreting(rows)) = relative_rows else {
                panic!("relative timers retain the interrupted batch");
            };
            let mut rows = rows.into_iter();
            let Some(Some(InterpretationProgress::Completed(Interpretation::Complete(
                ItemSettlement::Accepted(receipt),
            )))) = rows.next()
            else {
                panic!("the accepted relative prefix retains its actual receipt");
            };
            assert_eq!(
                receipt,
                TimerScheduled {
                    id: relative[0].id,
                    generation: relative[0].generation
                }
            );
            let Some(Some(InterpretationProgress::Interpreting((Some(pending), None)))) =
                rows.next()
            else {
                panic!("the original interrupted relative timer must survive conversion panic");
            };
            assert_eq!(pending, relative[1]);
            let Some(Some(InterpretationProgress::Original(suffix))) = rows.next() else {
                panic!("the relative suffix remains unattempted");
            };
            assert_eq!(suffix, relative[2]);
            assert!(rows.next().is_none());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn absolute_timer_panic_returns_pending_original_and_untouched_suffix() {
    conversion_panic_returns_timer_custody(ReminderClock::Absolute).await;
}

#[tokio::test(flavor = "current_thread")]
async fn relative_timer_panic_returns_pending_original_and_untouched_suffix() {
    conversion_panic_returns_timer_custody(ReminderClock::Relative).await;
}
