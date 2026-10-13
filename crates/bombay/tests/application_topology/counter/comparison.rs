use super::{Counter, CounterMessage, CounterValue, counter_application};
use bombay::behavior::{ClassifySettlement, Never, SettlementStatus};
use bombay::prelude::*;
use bombay::{ActorSpace, ApplicationTopology, RootRegistration};

struct CounterSpaces {
    counter: ActorSpace<Counter>,
}
impl RootRegistration<Counter> for CounterSpaces {
    fn root_space(&self) -> &ActorSpace<Counter> {
        &self.counter
    }
}
impl ApplicationTopology<StopOnShutdown<Counter>, ()> for CounterSpaces {
    type Descendants = Never;
}

#[tokio::test(flavor = "current_thread")]
async fn generated_leaf_matches_manual_counter_custody() {
    let generated = Application::with_topology(
        Counter::new().stop_on_shutdown(),
        counter_application::Topology::new(),
    )
    .run_with(|application| async move {
        let mut client = application.client::<CounterValue>().unwrap();
        let increment = client.send(CounterMessage::Increment).await;
        let read = client.send(CounterMessage::Read(client.recipient())).await;
        let received = client.receive().await;
        let shutdown = application.request_shutdown();
        let termination = application.termination().await;
        (increment, read, received, shutdown, termination)
    })
    .await;
    let manual = Application::with_topology(
        Counter::new().stop_on_shutdown(),
        CounterSpaces {
            counter: ActorSpace::new(),
        },
    )
    .run_with(|application| async move {
        let mut client = application.client::<CounterValue>().unwrap();
        let increment = client.send(CounterMessage::Increment).await;
        let read = client.send(CounterMessage::Read(client.recipient())).await;
        let received = client.receive().await;
        let shutdown = application.request_shutdown();
        let termination = application.termination().await;
        (increment, read, received, shutdown, termination)
    })
    .await;
    // Both complete receiving products are acquired before any comparison.
    let (generated_work, generated_native, generated_notifications) =
        generated.unwrap_or_else(|original| {
            drop(original);
            panic!("the generated explicit leaf starts");
        });
    let (manual_work, manual_native, manual_notifications) = manual.unwrap_or_else(|original| {
        drop(original);
        panic!("the handwritten explicit leaf starts");
    });
    let (generated_originals, generated_cleanup) = generated_work
        .try_into_completed()
        .unwrap_or_else(|original| {
            drop(original);
            panic!("generated leaf work remains acquired");
        });
    let (manual_originals, manual_cleanup) =
        manual_work.try_into_completed().unwrap_or_else(|original| {
            drop(original);
            panic!("manual leaf work remains acquired");
        });
    let mut values = Vec::new();
    for (originals, cleanup, native, notifications) in [
        (
            generated_originals,
            generated_cleanup,
            generated_native,
            generated_notifications,
        ),
        (
            manual_originals,
            manual_cleanup,
            manual_native,
            manual_notifications,
        ),
    ] {
        let (increment, read, reply, shutdown, termination) = originals;
        let notifications = notifications.unwrap();
        let (origin, retirement) = native.unwrap();
        assert!(cleanup.is_ok());
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        assert!(increment.is_ok());
        assert!(read.is_ok());
        assert_eq!(shutdown, Ok(()));
        assert_eq!(termination, Ok(Exit::Normal));
        let reply = reply.expect("the ordered native Read must have an actual reply");
        assert_eq!(reply.from, origin.address());
        assert_eq!(reply.message, 1);
        inspect_counter_retirement(origin, retirement);
        values.push(reply.message);
    }
    assert_eq!(values, [1, 1]);
}

fn inspect_counter_retirement(
    origin: RootOrigin<StopOnShutdown<Counter>>,
    retirement: ActorRetirement<StopOnShutdown<Counter>, Never, Vec<Never>>,
) {
    let ActorRetirement::Completed {
        custody,
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("both leaf roots retain the original completed Counter");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.child_failures.as_slice(), []);
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert_eq!(custody.descendants.as_slice(), []);
    // Counter state is independently observed through its actual typed Read.
}
