//! Public Application startup and retirement; these are not remote-command proofs.

use super::counter::CounterValue;
use super::{Counter, CounterMessage};
use bombay::actors::ActorExt;
use bombay::behavior::Behavior;
use bombay::prelude::{Application, StopOnShutdown};
use bombay::remote::{CredentialSource, NetworkLayout, NodeStartupRefusal};
use bombay::{ActorFailureAssessment, ApplicationHandle, ApplicationOutcome, RetirementAssessment};
use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use std::cell::Cell;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::application_support::{assert_completed, listening_network};

// Failure-only test watchdog; this is not an Application deadline or success oracle.
const WATCHDOG: Duration = Duration::from_secs(5);

struct WorkDisposal<Work> {
    work: Pin<Box<Work>>,
    cause: Option<Box<u64>>,
    disposals: Rc<Cell<usize>>,
}

impl<Work: Future> Future for WorkDisposal<Work> {
    type Output = Work::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().work.as_mut().poll(context)
    }
}

impl<Work> Drop for WorkDisposal<Work> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        if let Some(cause) = self.cause.take() {
            panic_any(cause);
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "retain the actual Ready Work output, original disposal panic and all independent remote retirement results before observations"
)]
fn ready_remote_output_survives_work_disposal_and_connection_retirement() {
    let original = Rc::new(vec![197_u64, 199]);
    let original_allocation = Rc::as_ptr(&original);
    let retained = Rc::downgrade(&original);
    let cause = Box::new(211_u64);
    let cause_allocation = ptr::from_ref(cause.as_ref());
    let disposals = Rc::new(Cell::new(0));
    let work_disposals = Rc::clone(&disposals);
    let host = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let acquired = host.block_on(async {
        let (execution, receiving, closing) = Application::new(Counter::new().stop_on_shutdown())
            .execute_remote(
                listening_network("localhost:0".into()),
                move |application: ApplicationHandle<
                    <Counter as Behavior>::Protocol,
                    <StopOnShutdown<Counter> as Behavior>::Event,
                >| WorkDisposal {
                    work: Box::pin(async move {
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
                        let retirement =
                            timeout(WATCHDOG, application.lifecycle().retirement()).await;
                        client.close_admission();
                        let mut tail = Vec::new();
                        while let Some(original) = client.receive().await {
                            tail.push(original);
                        }
                        (
                            original, target, increment, read, value, stopped, retirement, tail,
                        )
                    }),
                    cause: Some(cause),
                    disposals: work_disposals,
                },
            )
            .unwrap_or_else(|_| panic!("the actual multithread host accepts remote Work"));
        let mut execution = Box::pin(execution);
        let execution_fault = timeout(
            WATCHDOG,
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                    Err(cause) => Poll::Ready(Err(cause)),
                }
            }),
        )
        .await;
        // Only execution is surrendered; all independent original receivers remain.
        drop(execution);
        let application_products = receiving.await;
        let closed = closing.await;
        (execution_fault, application_products, closed)
    });
    drop(host);
    let (execution_fault, (outcome, native, notifications, ()), closed) = acquired;
    let Err(cause) = execution_fault.expect("the failure-only driver reached actual Work disposal")
    else {
        panic!("disposing the real Ready Work must expose its original panic");
    };
    let cause = cause
        .downcast_ref::<Box<u64>>()
        .expect("the caught panic retains the original allocated cause");
    assert_eq!(ptr::from_ref(cause.as_ref()), cause_allocation);
    assert_eq!(**cause, 211);
    assert_eq!(disposals.get(), 1);
    let (joined_close, close) = closed
        .expect("the independent connection startup report survives")
        .unwrap_or_else(|_| panic!("the actual configured remote connection opened"));
    let notifications = notifications.expect("both original root notification receipts survive");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(joined_close.is_ok() && matches!(close, Ok(Ok(()))));
    assert_completed(
        native.expect("the original root result remains independently received"),
        None,
    );
    let (output, cleanup) = outcome.try_into_completed().unwrap_or_else(|_| {
        panic!("the acquired Ready output must remain Completed after producer disposal")
    });
    let (original, target, increment, read, value, stopped, retirement, tail) = output;
    assert_eq!(Rc::as_ptr(&original), original_allocation);
    assert_eq!(original.as_slice(), [197, 199]);
    let same_original = retained
        .upgrade()
        .is_some_and(|retained| Rc::ptr_eq(&retained, &original));
    assert!(same_original);
    assert!(increment.is_ok() && read.is_ok() && stopped.is_ok() && cleanup.is_ok());
    assert!(matches!(&value, Some(Ok(Some(value))) if (value.from, value.message) == (target, 1)));
    assert_eq!(tail.len(), 0);
    let retirement = retirement.expect("the real joined-root report is acquired before Work Ready");
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    assert_eq!(
        retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    drop(original);
    assert_eq!(retained.strong_count(), 0);
}

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
