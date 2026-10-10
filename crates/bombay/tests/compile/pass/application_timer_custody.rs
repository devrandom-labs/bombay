//! Real elapsed reports establish readiness in both timer/shutdown orders.

use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BirthMode, ChildReport,
    CreationSequence, EventIngress, InterpreterRequests, MessageProtocol, Never, NoBirths, NoSends,
    ReportToParent, Step, User, UserEvent,
};
use bombay::prelude::{Exit, TimerId};
use bombay::{ActorRetirement, Application, ChildRetirementSelection, MailAddr};
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
    let outcome = Application::new(TimedParent::new().stop_on_shutdown())
        .child(TimerOutside, timer)
        .child(ShutdownOutside, shutdown)
        .run_with(|application| async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                application.lifecycle().termination(),
            )
            .await
            .expect("both actual timer reports must arrive")
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered the actual application host");
        });
    let (termination, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work returns only after both real elapsed reports");
    });
    assert_eq!(termination, Ok(Exit::Normal));
    let (_, retirement) = cleanup.expect("joined root and exact child custody");
    let ActorRetirement::Completed {
        behavior,
        descendants,
        ..
    } = retirement
    else {
        panic!("the reunion completed the root");
    };
    assert!(behavior.base().outstanding.is_empty());
    assert_eq!(descendants.len(), 2);
    let mut orders = BTreeSet::new();
    for report in descendants {
        let report = match report.try_into_child_retirement(&TimerOutside) {
            Ok(child) => {
                let ActorRetirement::OwnerCancelled {
                    behavior,
                    descendants,
                    child_failures,
                    ..
                } = *child.retirement
                else {
                    panic!("timer reaction continued, so owner cancellation returns its actor");
                };
                assert!(descendants.is_empty());
                assert_eq!(child_failures, ());
                assert_eq!(behavior.base().payload.as_ptr(), timer_allocation);
                assert_eq!(&*behavior.base().payload, &[3, 7]);
                let _new_order = orders.insert(behavior.base().order);
                continue;
            }
            Err(original) => original,
        };
        let child = report
            .try_into_child_retirement(&ShutdownOutside)
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the unmatched role retains the second child");
            });
        let ActorRetirement::OwnerCancelled {
            behavior,
            descendants,
            child_failures,
            ..
        } = *child.retirement
        else {
            panic!("shutdown outside preserves the same cancellation custody");
        };
        assert!(descendants.is_empty());
        assert_eq!(child_failures, ());
        assert_eq!(behavior.base().payload.as_ptr(), shutdown_allocation);
        assert_eq!(&*behavior.base().payload, &[11, 13]);
        let _new_order = orders.insert(behavior.base().order);
    }
    assert_eq!(
        orders,
        BTreeSet::from([WrapperOrder::TimerOutside, WrapperOrder::ShutdownOutside])
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    replayed_elapsed_report_cannot_discharge_the_other_wrapper_order();
    actual_elapsed_reports_complete_both_orders_and_return_cancelled_child_custody().await;
}
