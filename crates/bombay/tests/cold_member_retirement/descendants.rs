use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BirthMode, ChildReport,
    CreateChild, CreationSequence, Creations, EventIngress, InitializationTurn,
    InterpreterRequests, MessageProtocol, Never, NoBirths, NoSends, ReportToParent,
    RetirementBirths, Step, User, UserEvent,
};
use bombay::lifecycle::StopOnShutdown;
use bombay::prelude::Exit;
use bombay::timing::OneShot;
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

type RelayWorker = StopOnShutdown<OneShot<TimedWorker>>;
struct Relay {
    worker: Option<RelayWorker>,
    original: Box<[u8]>,
}
impl BehaviorBase for Relay {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
impl Behavior for Relay {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = TimedParentEvent;
    type Sends = InterpreterRequests<ReportToParent<WrapperOrder>>;
    type Ph = Never;
    type Error = Never;
    type Birth = RetirementBirths<RelayWorker>;
    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one fresh relay worker");
        let worker = self.worker.take().expect("relay initializes once");
        Ok(Actions::create(Creations::one(CreateChild::birth(
            id, worker,
        ))))
    }
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let TimedParentEvent::Elapsed(report) = event;
        Ok(Actions::stop().with_send(ReportToParent::new(report.report)))
    }
}
struct RelayWorkerRole;
impl bombay::behavior::ChildRole<Relay> for RelayWorkerRole {
    type Child = RelayWorker;
    type Position = bombay::behavior::ChildHead;
}
bombay::topology! {
    timed_application {
        root: TimedParent {}
        members {
            Timer(TimerOutside) => timer;
            Relay(ShutdownOutside) => relay;
        }
        timer {}
        relay for Relay { Worker(RelayWorkerRole) => worker; }
        worker {}
    }
}
#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one complete heterogeneous timer journey proves both wrapper orders and exact original nested actor custody"
)]
async fn inferred_cold_parent_preserves_its_original_actor_and_nested_timer_custody() {
    replayed_elapsed_report_cannot_discharge_the_other_wrapper_order();
    let first = Box::<[u8]>::from([3, 7]);
    let first_allocation = first.as_ptr();
    let second = Box::<[u8]>::from([11, 13]);
    let second_allocation = second.as_ptr();
    let relay = Box::<[u8]>::from([17, 19]);
    let relay_allocation = relay.as_ptr();
    let timer = TimedWorker {
        order: WrapperOrder::TimerOutside,
        payload: first,
    }
    .stop_on_shutdown()
    .with_one_shot(Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.base().order))
    });
    let worker = TimedWorker {
        order: WrapperOrder::ShutdownOutside,
        payload: second,
    }
    .with_one_shot(Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.order))
    })
    .stop_on_shutdown();
    let (outcome, _root_report, native, notifications, ()) = Application::with_topology(
        TimedParent::new().stop_on_shutdown(),
        timed_application::Topology::new(),
    )
    .child(TimerOutside, timer)
    .child(
        ShutdownOutside,
        Relay {
            worker: Some(worker),
            original: relay,
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move { application.termination().await })
    .await
    .unwrap_or_else(|_| panic!("the native host is entered"))
    .into_parts();
    let (termination, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("both actual report journeys complete"));
    assert_eq!(termination, Ok(Exit::Normal));
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (_, root) = native.expect("independent original root custody");
    let ActorRetirement::Completed { custody, .. } = root else {
        panic!("the root received both actual reports")
    };
    assert!(custody.behavior.base().outstanding.is_empty());
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.descendants.len(), 2);
    let mut roles = BTreeSet::new();
    for child in custody.descendants {
        match child {
            timed_application::RootRetirements::Timer { origin, retirement } => {
                let inserted = roles.insert("timer");
                assert!(inserted);
                assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
                let ActorRetirement::OwnerCancelled { custody, .. } = *retirement else {
                    panic!("continued direct timer returns cancellation custody")
                };
                assert_eq!(custody.behavior.base().payload.as_ptr(), first_allocation);
                assert_eq!(&*custody.behavior.base().payload, &[3, 7]);
                assert_eq!(custody.descendants.len(), 0);
                assert_eq!(custody.child_failures.len(), 0);
            }
            timed_application::RootRetirements::Relay { origin, retirement } => {
                let inserted = roles.insert("relay");
                assert!(inserted);
                assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
                let ActorRetirement::Completed { custody, .. } = *retirement else {
                    panic!("relay reports and stops with complete nested custody")
                };
                assert!(custody.behavior.base().worker.is_none());
                assert_eq!(custody.behavior.base().original.as_ptr(), relay_allocation);
                assert_eq!(&*custody.behavior.base().original, &[17, 19]);
                assert_eq!(custody.child_failures.len(), 0);
                assert_eq!(custody.descendants.len(), 1);
                let child = custody
                    .descendants
                    .into_iter()
                    .next()
                    .expect("the relay retains its original worker");
                let ActorRetirement::OwnerCancelled { custody, .. } = *child.retirement else {
                    panic!("the continued nested timer retains cancellation custody")
                };
                assert_eq!(custody.behavior.base().payload.as_ptr(), second_allocation);
                assert_eq!(&*custody.behavior.base().payload, &[11, 13]);
                assert_eq!(custody.descendants.len(), 0);
                assert_eq!(custody.child_failures.len(), 0);
            }
        }
    }
    assert_eq!(roles, BTreeSet::from(["timer", "relay"]));
}
