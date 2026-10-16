//! Real elapsed reports establish readiness in both timer/shutdown orders.

use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BirthMode, ChildReport,
    CreationSequence, EventIngress, InterpreterRequests, MessageProtocol, Never, NoBirths, NoSends,
    ReportToParent, Step, User, UserEvent,
};
use bombay::prelude::{Exit, TimerId};
use bombay::{ActorNotificationReceipts, ActorRetirement, Application, MailAddr};
use core::time::Duration;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WrapperOrder {
    TimerOutside,
    ShutdownOutside,
}

struct TimerOutside;
struct ShutdownOutside;

struct TimedWorker {
    order: WrapperOrder,
    payload: Box<[u8]>,
}

#[bombay::actor(message = Never, sends = { elapsed: InterpreterRequests<ReportToParent<WrapperOrder>> })]
impl TimedWorker {}

enum TimedParentEvent {
    Elapsed(ChildReport<WrapperOrder>),
}

impl UserEvent for TimedParentEvent {
    type Addr = MailAddr;
    type Message = Never;

    fn user(_: MailAddr, message: Never) -> Self {
        match message {}
    }

    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}

impl<Source: BirthMode> EventIngress<Source, ChildReport<WrapperOrder>> for TimedParentEvent {
    fn ingress(report: ChildReport<WrapperOrder>) -> Self {
        Self::Elapsed(report)
    }
}

struct TimedParent {
    outstanding: BTreeSet<WrapperOrder>,
}

impl TimedParent {
    fn new() -> Self {
        Self {
            outstanding: BTreeSet::from([
                WrapperOrder::TimerOutside,
                WrapperOrder::ShutdownOutside,
            ]),
        }
    }
}

impl BehaviorBase for TimedParent {
    type Base = Self;

    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for TimedParent {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = TimedParentEvent;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            TimedParentEvent::Elapsed(report) => {
                let _accepted = self.outstanding.remove(&report.report);
                if self.outstanding.is_empty() {
                    Ok(Actions::stop())
                } else {
                    Ok(Actions::cont())
                }
            }
        }
    }
}

fn replayed_elapsed_report_cannot_discharge_the_other_wrapper_order() {
    let mut sequence = CreationSequence::new();
    let timer = sequence.issue().expect("two report sources fit");
    let shutdown = sequence.issue().expect("two report sources fit");
    let mut parent = TimedParent::new();
    let first = bombay::behavior::delegate_transition(
        &mut parent,
        TimedParentEvent::Elapsed(ChildReport::new(timer, WrapperOrder::TimerOutside)),
    )
    .expect("elapsed is a normal transition");
    assert!(matches!(first.become_, Step::Continue));
    assert_eq!(
        parent.outstanding,
        BTreeSet::from([WrapperOrder::ShutdownOutside])
    );
    let replay = bombay::behavior::delegate_transition(
        &mut parent,
        TimedParentEvent::Elapsed(ChildReport::new(timer, WrapperOrder::TimerOutside)),
    )
    .expect("duplicate elapsed is a normal transition");
    assert!(matches!(replay.become_, Step::Continue));
    assert_eq!(
        parent.outstanding,
        BTreeSet::from([WrapperOrder::ShutdownOutside])
    );
    let completed = bombay::behavior::delegate_transition(
        &mut parent,
        TimedParentEvent::Elapsed(ChildReport::new(shutdown, WrapperOrder::ShutdownOutside)),
    )
    .expect("both actual reports complete the reunion");
    assert!(matches!(completed.become_, Step::Stop(_)));
    assert!(parent.outstanding.is_empty());
}

bombay::topology! {
    timed_application {
        root: TimedParent {}
        members {
            Timer(TimerOutside) => timer;
            Shutdown(ShutdownOutside) => shutdown;
        }
        timer {}
        shutdown {}
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "both native wrapper orders receive actual elapsed reports before returning original actors and both independently typed retirements"
)]
async fn actual_elapsed_reports_complete_both_orders_and_return_cancelled_child_custody() {
    let timer_payload: Box<[u8]> = Box::from([3, 7]);
    let timer_allocation = timer_payload.as_ptr();
    let shutdown_payload: Box<[u8]> = Box::from([11, 13]);
    let shutdown_allocation = shutdown_payload.as_ptr();
    let timer = TimedWorker {
        order: WrapperOrder::TimerOutside,
        payload: timer_payload,
    }
    .stop_on_shutdown()
    .with_one_shot(TimerId(1), Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.base().order))
    });
    let shutdown = TimedWorker {
        order: WrapperOrder::ShutdownOutside,
        payload: shutdown_payload,
    }
    .with_one_shot(TimerId(1), Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.order))
    })
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = Application::with_topology(
        TimedParent::new().stop_on_shutdown(),
        timed_application::Topology::new(),
    )
    .child(TimerOutside, timer)
    .child(ShutdownOutside, shutdown)
    .run_with(|application| async move {
        tokio::time::timeout(Duration::from_secs(5), application.termination())
            .await
            .expect("both actual timer reports must arrive")
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("entered the actual application host");
    })
    .into_parts();
    let (termination, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work returns only after both real elapsed reports");
    });
    assert_eq!(termination, Ok(Exit::Normal));
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (_, retirement) = native.expect("joined root and exact child custody");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                descendants,
                ..
            },
        ..
    } = retirement
    else {
        panic!("the reunion completed the root");
    };
    assert!(behavior.base().outstanding.is_empty());
    assert_eq!(descendants.len(), 2);
    let mut orders = BTreeSet::new();
    for report in descendants {
        let retirement = match report {
            timed_application::RootRetirements::Timer { origin, retirement } => {
                assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
                let ActorRetirement::OwnerCancelled {
                    custody:
                        ActorCustody {
                            behavior,
                            descendants,
                            child_failures,
                            ..
                        },
                    ..
                } = *retirement
                else {
                    panic!("timer reaction continued, so owner cancellation returns its actor");
                };
                assert_eq!(descendants, []);
                assert_eq!(child_failures, []);
                assert_eq!(behavior.base().payload.as_ptr(), timer_allocation);
                assert_eq!(&*behavior.base().payload, &[3, 7]);
                let _new_order = orders.insert(behavior.base().order);
                continue;
            }
            timed_application::RootRetirements::Shutdown { origin, retirement } => {
                assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
                retirement
            }
        };
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    behavior,
                    descendants,
                    child_failures,
                    ..
                },
            ..
        } = *retirement
        else {
            panic!("shutdown outside preserves the same cancellation custody");
        };
        assert_eq!(descendants, []);
        assert_eq!(child_failures, []);
        assert_eq!(behavior.base().payload.as_ptr(), shutdown_allocation);
        assert_eq!(&*behavior.base().payload, &[11, 13]);
        let _new_order = orders.insert(behavior.base().order);
    }
    assert_eq!(
        orders,
        BTreeSet::from([WrapperOrder::TimerOutside, WrapperOrder::ShutdownOutside])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn native_projection_keeps_timer_and_shutdown_orders_and_original_actor_custody() {
    replayed_elapsed_report_cannot_discharge_the_other_wrapper_order();
    actual_elapsed_reports_complete_both_orders_and_return_cancelled_child_custody().await;
}
