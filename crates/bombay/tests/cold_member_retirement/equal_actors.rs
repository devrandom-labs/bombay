//! Real elapsed reports establish readiness in both timer/shutdown orders.

use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BirthMode, ChildReport,
    CreationSequence, EventIngress, InterpreterRequests, MessageProtocol, Never, NoBirths, NoSends,
    ReportToParent, Step, User, UserEvent,
};
use bombay::prelude::{Exit, TimerId};
use bombay::{ActorRetirement, Application, MailAddr};
use core::time::Duration;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ServiceRole {
    Primary,
    Secondary,
}

struct Primary;
struct Secondary;

struct TimedWorker {
    role: ServiceRole,
    payload: Box<[u8]>,
}

#[bombay::actor(message = Never, sends = { elapsed: InterpreterRequests<ReportToParent<ServiceRole>> })]
impl TimedWorker {}

enum TimedParentEvent {
    Elapsed(ChildReport<ServiceRole>),
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

impl<Source: BirthMode> EventIngress<Source, ChildReport<ServiceRole>> for TimedParentEvent {
    fn ingress(report: ChildReport<ServiceRole>) -> Self {
        Self::Elapsed(report)
    }
}

struct TimedParent {
    outstanding: BTreeSet<ServiceRole>,
}

impl TimedParent {
    fn new() -> Self {
        Self {
            outstanding: BTreeSet::from([ServiceRole::Primary, ServiceRole::Secondary]),
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
        TimedParentEvent::Elapsed(ChildReport::new(timer, ServiceRole::Primary)),
    )
    .expect("elapsed is a normal transition");
    assert!(matches!(first.become_, Step::Continue));
    assert_eq!(parent.outstanding, BTreeSet::from([ServiceRole::Secondary]));
    let replay = bombay::behavior::delegate_transition(
        &mut parent,
        TimedParentEvent::Elapsed(ChildReport::new(timer, ServiceRole::Primary)),
    )
    .expect("duplicate elapsed is a normal transition");
    assert!(matches!(replay.become_, Step::Continue));
    assert_eq!(parent.outstanding, BTreeSet::from([ServiceRole::Secondary]));
    let completed = bombay::behavior::delegate_transition(
        &mut parent,
        TimedParentEvent::Elapsed(ChildReport::new(shutdown, ServiceRole::Secondary)),
    )
    .expect("both actual reports complete the reunion");
    assert!(matches!(completed.become_, Step::Stop(_)));
    assert!(parent.outstanding.is_empty());
}

bombay::topology! {
    timed_application {
        root: TimedParent {}
        members {
            Timer(Primary) => timer;
            Shutdown(Secondary) => shutdown;
        }
        timer in workers {}
        shutdown {}
        hosts { workers: TimedWorker; }
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
        role: ServiceRole::Primary,
        payload: timer_payload,
    }
    .stop_on_shutdown()
    .with_one_shot(TimerId(1), Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.base().role))
    });
    let shutdown = TimedWorker {
        role: ServiceRole::Secondary,
        payload: shutdown_payload,
    }
    .stop_on_shutdown()
    .with_one_shot(TimerId(1), Duration::ZERO, |worker| {
        Actions::cont().with_send(ReportToParent::new(worker.base().role))
    });
    let outcome = Application::with_topology(
        TimedParent::new().stop_on_shutdown(),
        timed_application::Topology::new(),
    )
    .child(Primary, timer)
    .child(Secondary, shutdown)
    .run_with(|application| async move {
        tokio::time::timeout(Duration::from_secs(5), application.termination())
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
                let _new_order = orders.insert(behavior.base().role);
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
        let _new_order = orders.insert(behavior.base().role);
    }
    assert_eq!(
        orders,
        BTreeSet::from([ServiceRole::Primary, ServiceRole::Secondary])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn equal_actor_types_preserve_distinct_cold_roles_and_original_custody() {
    replayed_elapsed_report_cannot_discharge_the_other_wrapper_order();
    actual_elapsed_reports_complete_both_orders_and_return_cancelled_child_custody().await;
}

#[test]
fn cold_occurrences_keep_hosted_and_exact_registration_distinct() {
    type TimerActor = bombay::timing::OneShot<bombay::lifecycle::StopOnShutdown<TimedWorker>>;
    type ShutdownActor = bombay::timing::OneShot<bombay::lifecycle::StopOnShutdown<TimedWorker>>;
    type OriginalRoot = bombay::lifecycle::StopOnShutdown<TimedParent>;
    type Parent = bombay::ApplicationBehavior<
        OriginalRoot,
        bombay::behavior::ChildCons<
            MailAddr,
            ShutdownActor,
            bombay::behavior::ChildCons<MailAddr, TimerActor, bombay::behavior::NoChildren>,
        >,
    >;
    type Descendants = timed_application::RootRetirements<OriginalRoot, ShutdownActor, TimerActor>;
    let topology = timed_application::Topology::new();
    let logical_scope =
        <timed_application::Topology as bombay::Hosts<TimedWorker>>::space(&topology)
            .registration_scope_id();
    let timer = <timed_application::Topology as bombay::NativeChild<
        Parent,
        bombay::behavior::ChildTail<bombay::behavior::ChildHead>,
        TimerActor,
        Descendants,
        Never,
    >>::registration(&topology);
    let shutdown = <timed_application::Topology as bombay::NativeChild<
        Parent,
        bombay::behavior::ChildHead,
        ShutdownActor,
        Descendants,
        Never,
    >>::registration(&topology);
    assert_eq!(timer.registration_scope_id(), logical_scope);
    assert_ne!(shutdown.registration_scope_id(), logical_scope);
}
