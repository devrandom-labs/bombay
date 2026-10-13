use bombay::ActorNotificationReceipts;
use bombay::behavior::{BehaviorSettlements, ClassifySettlement, Protocol, SettlementStatus};
use bombay::prelude::*;
use bombay::{ActorSpace, ApplicationLifecycle};
use core::time::Duration;

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use counter::{Counter, CounterError, CounterMessage, CounterValue};

struct CounterSession {
    value: u64,
}

enum CounterSessionCommand {
    Increment,
    Finish(EstablishedRecipient<CounterValue>),
}

#[bombay::actor(
    sends = { values: Vec<EstablishedDelivery<CounterValue>> },
    error = CounterError,
)]
impl CounterSession {
    fn receive(&mut self, command: CounterSessionCommand) -> BehaviorActed<Self> {
        match command {
            CounterSessionCommand::Increment => {
                self.value = self.value.checked_add(1).ok_or(CounterError::Overflow)?;
                Ok(Actions::cont())
            }
            CounterSessionCommand::Finish(customer) => {
                Ok(Actions::stop().send_values(EstablishedDelivery::new(customer, self.value)))
            }
        }
    }
}

async fn increment_counter<Event>(
    application: ApplicationHandle<Counter, Event>,
) -> ApplicationLifecycle<Counter, Event> {
    let lifecycle = application.lifecycle();
    let interface = application.interface(application.root().established_recipient());
    let mut customer = interface.external::<CounterValue>().expect("live customer");
    customer
        .send(interface.api(), CounterMessage::Increment)
        .await
        .expect("counter accepts increment");
    customer
        .send(interface.api(), CounterMessage::Read(customer.recipient()))
        .await
        .expect("counter accepts reply capability");
    let reply = customer
        .receive()
        .await
        .expect("counter returns business reply");
    assert_eq!(reply.message, 1);
    lifecycle
}

fn assert_counter_retirement<Actor>(retirement: ActorRetirement<Actor, Never, Vec<Never>>)
where
    Actor: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    Actor::Settlements: ClassifySettlement,
{
    assert!(retirement.behavior().is_some());
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        settlements,
        interpretation,
        source,
        completion,
    } = retirement
    else {
        panic!("the original complete counter retirement must return");
    };
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(custody.child_failures, []);
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants, []);
    assert!(custody.additional_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    drop(custody.behavior);
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_outside_timer_keeps_business_and_complete_retirement() {
    let root = Counter::new()
        .with_one_shot(TimerId(71), Duration::from_secs(60), |_| Actions::stop())
        .stop_on_shutdown();
    let outcome = Application::with_topology(root, ActorSpace::new())
        .run_with(|application| async move {
            let lifecycle = increment_counter(application).await;
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let repeated_shutdown = lifecycle.request_shutdown();
            assert!(matches!(
                repeated_shutdown,
                Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped)
            ));
            let termination = lifecycle.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("live application host");
        });
    let (outcome, native, notifications) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("business work completes");
    });
    cleanup.expect("actual application cleanup completes");
    let (origin, retirement) = native.expect("independent complete cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_counter_retirement(retirement);
}

#[tokio::test(flavor = "current_thread")]
async fn timer_outside_shutdown_infers_the_actual_lifecycle_lane() {
    let root = Counter::new().stop_on_shutdown().with_one_shot(
        TimerId(71),
        Duration::from_secs(60),
        |_| Actions::stop(),
    );
    let outcome = Application::with_topology(root, ActorSpace::new())
        .run_with(|application| async move {
            let lifecycle = increment_counter(application).await;
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let repeated_shutdown = lifecycle.request_shutdown();
            assert!(matches!(
                repeated_shutdown,
                Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped)
            ));
            let termination = lifecycle.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("live application host");
        });
    let (outcome, native, notifications) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("business work completes");
    });
    cleanup.expect("actual application cleanup completes");
    let (origin, retirement) = native.expect("independent complete cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_counter_retirement(retirement);
}

#[tokio::test(flavor = "current_thread")]
async fn natural_completion_needs_no_unused_shutdown_capability() {
    let root = CounterSession { value: 0 };
    let outcome = Application::with_topology(root, ActorSpace::new())
        .run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let interface = application.interface(application.root().established_recipient());
            let mut customer = interface.external::<CounterValue>().expect("live customer");
            customer
                .send(interface.api(), CounterSessionCommand::Increment)
                .await
                .expect("session accepts increment");
            customer
                .send(
                    interface.api(),
                    CounterSessionCommand::Finish(customer.recipient()),
                )
                .await
                .expect("session accepts its domain completion command");
            let reply = customer.receive().await.expect("complete business reply");
            assert_eq!(reply.message, 1);
            let termination = lifecycle.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("live application host");
        });
    let (outcome, native, notifications) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("business work completes");
    });
    cleanup.expect("actual application cleanup completes");
    let (origin, retirement) = native.expect("independent complete cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.behavior().expect("original session").value, 1);
    assert_counter_retirement(retirement);
}

#[test]
fn shutdown_requires_a_present_unambiguous_lane() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/application_shutdown_absent.rs");
    cases.compile_fail("tests/compile/fail/application_shutdown_ambiguous.rs");
}

#[cfg(feature = "axum")]
#[tokio::test(flavor = "current_thread")]
async fn http_ends_with_the_domain_session_without_a_shutdown_lane() {
    let (customer_publication, customer_receiving) = tokio::sync::oneshot::channel();
    let outcome = Application::with_topology(CounterSession { value: 0 }, ActorSpace::new())
        .run_axum(
            "127.0.0.1:0".parse().expect("local listener address"),
            move |application| {
                let interface = application.interface(application.root().established_recipient());
                let customer = tokio::spawn(async move {
                    let mut customer = interface.external::<CounterValue>().expect("live customer");
                    customer
                        .send(interface.api(), CounterSessionCommand::Increment)
                        .await
                        .expect("session accepts increment");
                    customer
                        .send(
                            interface.api(),
                            CounterSessionCommand::Finish(customer.recipient()),
                        )
                        .await
                        .expect("session accepts domain completion");
                    let reply = customer.receive().await.expect("complete business reply");
                    assert_eq!(reply.message, 1);
                });
                customer_publication
                    .send(customer)
                    .expect("outside customer custody");
                axum::Router::new()
            },
        )
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("live local HTTP host");
        });
    customer_receiving
        .await
        .expect("original customer task returns")
        .await
        .expect("customer completes independently");
    let (outcome, native, notifications) = outcome;
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = notifications
    else {
        panic!("both original actor notifications succeed");
    };
    let (serving, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("graceful serving completes");
    });
    serving.expect("original serving result");
    cleanup.expect("actual application cleanup completes");
    let (origin, retirement) = native.expect("independent complete root cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.behavior().expect("original session").value, 1);
    assert_counter_retirement(retirement);
}
