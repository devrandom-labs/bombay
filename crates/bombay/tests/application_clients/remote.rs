//! Public Application startup and retirement; these are not remote-command proofs.

use super::counter::CounterValue;
use super::{Counter, CounterMessage};
use bombay::actors::ActorExt;
use bombay::behavior::Behavior;
use bombay::prelude::{Application, StopOnShutdown};
use bombay::remote::{CredentialSource, NetworkLayout, NodeStartupRefusal};
use bombay::{ActorFailureAssessment, ApplicationHandle, ApplicationOutcome, RetirementAssessment};
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::cell::Cell;
use std::error::Error;
use std::ptr;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::application_support::{assert_completed, listening_network};

// Failure-only test watchdog; this is not an Application deadline or success oracle.
const WATCHDOG: Duration = Duration::from_secs(5);

#[test]
fn missing_host_returns_the_original_application_network_and_callable() {
    let original = Box::new(71_u64);
    let allocation = ptr::from_ref(original.as_ref());
    let credential: Box<str> = "original unconsumed deployment credential".into();
    let credential_allocation = credential.as_ptr();
    let mut network = listening_network("localhost:0".into());
    network.deployment_roots = CredentialSource::Base64(credential);
    let calls = Rc::new(Cell::new(0));
    let entered = Rc::clone(&calls);
    let work = move |application: ApplicationHandle<
        <Counter as Behavior>::Protocol,
        <StopOnShutdown<Counter> as Behavior>::Event,
    >| async move {
        entered.set(entered.get() + 1);
        let target = application.root().address();
        let mut client = application.client::<CounterValue>().unwrap();
        let read = client.try_send(CounterMessage::Read(client.recipient()));
        let value = if read.is_ok() {
            Some(timeout(WATCHDOG, client.receive()).await)
        } else {
            None
        };
        let stopped = application.request_shutdown();
        client.close_admission();
        let mut tail = Vec::new();
        while let Some(original) = client.receive().await {
            tail.push(original);
        }
        (original, target, read, value, stopped, tail)
    };
    let issued =
        Application::new(Counter::at_maximum().stop_on_shutdown()).execute_remote(network, work);
    let (application, network, work, cause) = match issued {
        Err(originals) => originals,
        Ok(products) => {
            drop(products);
            panic!("an absent host must return the original cold inputs")
        }
    };
    let before = calls.get();
    let executor = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (outcome, native, notifications) = executor
        .block_on(application.run_with(work))
        .unwrap_or_else(|_| panic!("returned application and callable run locally"));
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original callable completes"));
    drop(executor);
    let (original, target, read, value, stopped, tail) = output;
    let CredentialSource::Base64(credential) = network.deployment_roots else {
        panic!("the exact cold credential alternative survives")
    };
    let notifications = notifications.unwrap();
    assert_eq!(before, 0);
    assert_eq!(calls.get(), 1);
    assert_eq!(ptr::from_ref(original.as_ref()), allocation);
    assert_eq!(*original, 71);
    assert_eq!(credential.as_ptr(), credential_allocation);
    assert_eq!(&*credential, "original unconsumed deployment credential");
    assert_ne!(cause.to_string(), "");
    assert!(cleanup.is_ok() && read.is_ok() && stopped.is_ok());
    assert!(
        matches!(&value, Some(Ok(Some(value))) if (value.from, value.message) == (target, u64::MAX))
    );
    assert_eq!(tail.len(), 0);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_completed(native.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn configured_refusal_keeps_unstarted_inputs_and_its_independent_cause() {
    let original = Box::new(73_u64);
    let allocation = ptr::from_ref(original.as_ref());
    let address = "a".repeat(256);
    let address_allocation = address.as_ptr();
    let network = listening_network(address);
    let calls = Rc::new(Cell::new(0));
    let entered = Rc::clone(&calls);
    let work = move |application: ApplicationHandle<
        <Counter as Behavior>::Protocol,
        <StopOnShutdown<Counter> as Behavior>::Event,
    >| async move {
        entered.set(entered.get() + 1);
        let target = application.root().address();
        let mut client = application.client::<CounterValue>().unwrap();
        let read = client.try_send(CounterMessage::Read(client.recipient()));
        let value = if read.is_ok() {
            Some(timeout(WATCHDOG, client.receive()).await)
        } else {
            None
        };
        let stopped = application.request_shutdown();
        client.close_admission();
        let mut tail = Vec::new();
        while let Some(original) = client.receive().await {
            tail.push(original);
        }
        (original, target, read, value, stopped, tail)
    };
    let (execution, receiving, closing) =
        Application::new(Counter::at_maximum().stop_on_shutdown())
            .execute_remote(network, work)
            .unwrap_or_else(|_| panic!("the actual host accepts cold execution"));
    let ((), (outcome, absent_native, absent_notifications, ()), startup) =
        tokio::join!(execution, receiving, closing);
    let ApplicationOutcome::Unstarted {
        application: (application, network),
        work,
    } = outcome
    else {
        panic!("configuration refusal returns the original unstarted inputs")
    };
    let before = calls.get();
    let (outcome, native, notifications) = application
        .run_with(work)
        .await
        .unwrap_or_else(|_| panic!("the refused application's original callable runs locally"));
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original callable completes"));
    let (original, target, read, value, stopped, tail) = output;
    let Ok(Err(cause)) = startup else {
        panic!("independent receiving retains the sole actual startup refusal");
    };
    let NodeStartupRefusal::Address {
        field,
        cause: native_cause,
    } = &cause
    else {
        panic!("the native endpoint constructor retains its actual cause")
    };
    let source = Error::source(&cause).unwrap();
    let NetworkLayout::Peer {
        connect: None,
        listen: Some(direction),
    } = network.layout
    else {
        panic!("the original listening declaration survives")
    };
    let notifications = notifications.unwrap();
    assert_eq!(before, 0);
    assert_eq!(calls.get(), 1);
    assert_eq!(ptr::from_ref(original.as_ref()), allocation);
    assert_eq!(*original, 73);
    assert_eq!(direction.addresses.len(), 1);
    assert_eq!(direction.addresses[0].as_ptr(), address_allocation);
    assert_eq!(direction.addresses[0], "a".repeat(256));
    assert_eq!(*field, "listen/endpoints");
    assert_eq!(
        ptr::from_ref(source).addr(),
        ptr::from_ref(native_cause.as_ref()).addr()
    );
    assert!(absent_native.is_err() && absent_notifications.is_err());
    assert!(cleanup.is_ok() && read.is_ok() && stopped.is_ok());
    assert!(
        matches!(&value, Some(Ok(Some(value))) if (value.from, value.message) == (target, u64::MAX))
    );
    assert_eq!(tail.len(), 0);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_completed(native.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[expect(
    clippy::too_many_lines,
    reason = "retain actual work, native results and independent receipts through a cancelled borrowed wait"
)]
async fn borrowed_execution_wait_can_end_while_the_same_work_and_connection_remain_owned() {
    let original = Box::new(2_u64);
    let allocation = ptr::from_ref(original.as_ref());
    let calls = Rc::new(Cell::new(0));
    let entered_calls = Rc::clone(&calls);
    let (publication, mut parked) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (execution, receiving, closing) = Application::new(Counter::new().stop_on_shutdown())
        .execute_remote(
            listening_network("localhost:0".into()),
            move |application: ApplicationHandle<
                <Counter as Behavior>::Protocol,
                <StopOnShutdown<Counter> as Behavior>::Event,
            >| async move {
                entered_calls.set(entered_calls.get() + 1);
                let target = application.root().address();
                let mut client = application.client::<CounterValue>().unwrap();
                let mut admissions = Vec::new();
                for _ in 0..*original {
                    admissions.push(client.try_send(CounterMessage::Increment));
                }
                let read = client.try_send(CounterMessage::Read(client.recipient()));
                let value = if read.is_ok() {
                    Some(timeout(WATCHDOG, client.receive()).await)
                } else {
                    None
                };
                let stopped = application.request_shutdown();
                let retirement = timeout(WATCHDOG, application.lifecycle().retirement()).await;
                client.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
                let published = publication.send(());
                let resumed = released.await;
                (
                    original, target, admissions, read, value, stopped, retirement, tail,
                    published, resumed,
                )
            },
        )
        .unwrap_or_else(|_| panic!("the actual host accepts cold execution"));
    let mut execution = Box::pin(execution);
    let mut finished = None;
    let readiness = timeout(
        WATCHDOG,
        poll_fn(|context| {
            if let Poll::Ready(result) = pin!(&mut parked).poll(context) {
                return Poll::Ready(Some(result));
            }
            if finished.is_none()
                && let Poll::Ready(result) = execution.as_mut().poll(context)
            {
                finished = Some(result);
            }
            if finished.is_some() {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }),
    )
    .await;
    let cancelled_wait = if finished.is_none() {
        let mut borrowed = pin!(execution.as_mut());
        borrowed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    } else {
        Poll::Ready(())
    };
    let mut closing = pin!(closing);
    let early_close = closing
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    let resumed = release.send(());
    let completion = if finished.is_none() {
        Some(timeout(WATCHDOG, execution.as_mut()).await)
    } else {
        None
    };
    // On watchdog failure surrender only execution, then acquire its original cleanup.
    drop(execution);
    let (outcome, native, notifications, ()) = receiving.await;
    let (close_before_release, closed) = match early_close {
        Poll::Ready(original) => (Poll::Ready(()), original),
        Poll::Pending => (Poll::Pending, closing.await),
    };
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the same original work completes"));
    let (
        original,
        target,
        admissions,
        read,
        value,
        stopped,
        retirement,
        tail,
        publication,
        resumed_work,
    ) = output;
    let (joined, close) = closed
        .expect("startup report exists")
        .unwrap_or_else(|_| panic!("the configured connection opened"));
    let notifications = notifications.unwrap();
    assert!(matches!(readiness, Ok(Some(Ok(())))));
    assert!(finished.is_none());
    assert!(matches!(completion, Some(Ok(()))));
    assert!(cancelled_wait.is_pending());
    assert!(close_before_release.is_pending());
    assert_eq!(calls.get(), 1);
    assert_eq!(ptr::from_ref(original.as_ref()), allocation);
    assert_eq!(*original, 2);
    assert_eq!(admissions.len(), 2);
    assert!(admissions.iter().all(Result::is_ok));
    assert!(cleanup.is_ok() && read.is_ok() && stopped.is_ok());
    assert!(resumed.is_ok() && resumed_work.is_ok() && publication.is_ok());
    assert!(matches!(&value, Some(Ok(Some(value))) if (value.from, value.message) == (target, 2)));
    assert_eq!(tail.len(), 0);
    let retirement = retirement.expect("the actual retirement observation completed");
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    assert_eq!(
        retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(joined.is_ok());
    assert!(matches!(close, Ok(Ok(()))));
    assert_completed(native.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn run_remote_returns_the_same_independent_application_and_connection_results() {
    let original = Box::new(79_u64);
    let allocation = ptr::from_ref(original.as_ref());
    let result = Application::new(Counter::new().stop_on_shutdown())
        .run_remote(
            listening_network("localhost:0".into()),
            move |application: ApplicationHandle<
                <Counter as Behavior>::Protocol,
                <StopOnShutdown<Counter> as Behavior>::Event,
            >| async move {
                let target = application.root().address();
                let mut client = application.client::<CounterValue>().unwrap();
                let increment = client.try_send(CounterMessage::Increment);
                let read = client.try_send(CounterMessage::Read(client.recipient()));
                let value = if read.is_ok() {
                    Some(timeout(WATCHDOG, client.receive()).await)
                } else {
                    None
                };
                let stopped = application.request_shutdown();
                client.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = client.receive().await {
                    tail.push(original);
                }
                (original, target, increment, read, value, stopped, tail)
            },
        )
        .await;
    let (outcome, native, notifications, (), closed) =
        result.unwrap_or_else(|_| panic!("the actual host runs the same owned operations"));
    let (output, cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the unchanged Work output survives"));
    let (original, target, increment, read, value, stopped, tail) = output;
    let (joined, close) = closed
        .expect("startup report exists")
        .unwrap_or_else(|_| panic!("the configured connection opened"));
    let notifications = notifications.unwrap();
    assert_eq!(ptr::from_ref(original.as_ref()), allocation);
    assert_eq!(*original, 79);
    assert!(increment.is_ok() && read.is_ok() && stopped.is_ok() && cleanup.is_ok());
    assert!(matches!(&value, Some(Ok(Some(value))) if (value.from, value.message) == (target, 1)));
    assert_eq!(tail.len(), 0);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(joined.is_ok());
    assert!(matches!(close, Ok(Ok(()))));
    assert_completed(native.unwrap(), None);
}
