//! A work queue admits affine payloads through pre-root exact endpoints.

use bombay::ApplicationScope;
use bombay::actors::ActorExt as _;
use bombay::behavior::{BehaviorBase, EstablishedRecipient, MessageProtocol, Never};
use bombay::composition::StashRoute;
use bombay::prelude::{Completion, Exit, MailAddr, StopOnShutdown};
use bombay::routing::{WorkQueue, WorkQueueMessage, WorkQueueOutcome, WorkQueueRejection};
use bombay::{
    ActorNotificationReceipts, ActorSpace, ApplicationCleanupError, ApplicationOutcome,
    ApplicationTopology, RootRegistration,
};
use core::future::{Future, poll_fn};
use core::task::Poll;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::sync::{Mutex, PoisonError};
use tokio::runtime::Builder;

type Assignments = MessageProtocol<MailAddr, Box<String>>;
type Admissions = MessageProtocol<MailAddr, WorkQueueOutcome<Box<String>>>;
type Queue = WorkQueue<
    MailAddr,
    Box<String>,
    EstablishedRecipient<Assignments>,
    EstablishedRecipient<Admissions>,
>;

struct QueueRegistration {
    queues: ActorSpace<Queue>,
    failure: Mutex<Option<String>>,
}

impl RootRegistration<Queue> for QueueRegistration {
    fn root_space(&self) -> &ActorSpace<Queue> {
        let failure = self
            .failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(failure) = failure {
            panic_any(failure);
        }
        &self.queues
    }
}

impl ApplicationTopology<StopOnShutdown<Queue>, ()> for QueueRegistration {
    type Descendants = Never;
}

#[test]
fn exact_endpoint_equality_survives_closure_and_separates_application_namespaces() {
    let scope = ApplicationScope::new();
    let first = scope.external::<Assignments>().expect("first endpoint");
    let first_recipient = first.recipient();
    let clone = first_recipient.clone();
    let other_scope = ApplicationScope::new();
    let other = other_scope
        .external::<Assignments>()
        .expect("other namespace endpoint");
    assert_eq!(first.address(), other.address());
    assert_eq!(first_recipient, clone);
    assert_ne!(first_recipient, other.recipient());
    drop(first);
    drop(other);
    assert_eq!(first_recipient, clone);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete independent FIFO, exact rejection and retry custody journey together."
)]
fn work_queue_preserves_exact_dispatch_and_full_rejection_through_shutdown() {
    let scope = ApplicationScope::new();
    let mut worker = scope.external::<Assignments>().expect("worker endpoint");
    let mut admissions = scope.external::<Admissions>().expect("admission endpoint");
    let root = WorkQueue::new(1).stop_on_shutdown();
    let first = Box::new(String::from("first"));
    let first_pointer = ptr::from_ref(first.as_ref());
    let waiting = Box::new(String::from("waiting"));
    let waiting_pointer = ptr::from_ref(waiting.as_ref());
    let rejected = Box::new(String::from("rejected"));
    let rejected_pointer = ptr::from_ref(rejected.as_ref());
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller host");
    let unstarted = scope
        .with_root(root)
        .execute_with(|application| async move {
            let caller = application.sender().expect("queue sender");
            assert_ne!(caller.address(), worker.address());
            assert_ne!(caller.address(), admissions.address());
            for _ in 0..2 {
                caller
                    .send(WorkQueueMessage::Available {
                        worker: worker.recipient(),
                    })
                    .await
                    .expect("availability admitted");
            }
            caller
                .send(WorkQueueMessage::Submit {
                    value: first,
                    reply_to: admissions.recipient(),
                })
                .await
                .expect("first submitted");
            let dispatched = admissions.receive().await.expect("first dispatch").message;
            assert_eq!(dispatched, WorkQueueOutcome::Dispatched { queued: 0 });
            let first = worker
                .receive()
                .await
                .expect("assigned first payload")
                .message;
            assert_eq!(first.as_str(), "first");
            assert_eq!(ptr::from_ref(first.as_ref()), first_pointer);
            caller
                .send(WorkQueueMessage::Submit {
                    value: waiting,
                    reply_to: admissions.recipient(),
                })
                .await
                .expect("waiting submitted");
            let queued = admissions
                .receive()
                .await
                .expect("waiting admission")
                .message;
            assert_eq!(queued, WorkQueueOutcome::Queued { depth: 1 });
            caller
                .send(WorkQueueMessage::Submit {
                    value: rejected,
                    reply_to: admissions.recipient(),
                })
                .await
                .expect("overflow command admitted");
            let rejection = admissions
                .receive()
                .await
                .expect("complete overflow rejection")
                .message;
            let WorkQueueOutcome::Rejected {
                value: rejected,
                reason,
            } = rejection
            else {
                drop(rejection);
                panic!("overflow returns original value");
            };
            assert_eq!(reason, WorkQueueRejection::Full);
            assert_eq!(rejected.as_str(), "rejected");
            assert_eq!(ptr::from_ref(rejected.as_ref()), rejected_pointer);
            caller
                .send(WorkQueueMessage::Available {
                    worker: worker.recipient(),
                })
                .await
                .expect("worker made available");
            let dispatched = admissions
                .receive()
                .await
                .expect("waiting dispatch")
                .message;
            assert_eq!(dispatched, WorkQueueOutcome::Dispatched { queued: 0 });
            let waiting = worker
                .receive()
                .await
                .expect("assigned waiting payload")
                .message;
            assert_eq!(waiting.as_str(), "waiting");
            assert_eq!(ptr::from_ref(waiting.as_ref()), waiting_pointer);
            for _ in 0..2 {
                caller
                    .send(WorkQueueMessage::Withdraw {
                        worker: worker.recipient(),
                    })
                    .await
                    .expect("idempotent withdrawal");
            }
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            (first, waiting, rejected)
        });
    let Err((application, work, _missing_host)) = unstarted else {
        panic!("missing host returns the entire scope, root and uninvoked work");
    };
    let (outcome, native, notifications) = host
        .block_on(application.run_with(work))
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the original queue application retries");
        });
    let (work, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("complete queue custody");
    });
    assert_eq!(ptr::from_ref(work.0.as_ref()), first_pointer);
    assert_eq!(ptr::from_ref(work.1.as_ref()), waiting_pointer);
    assert_eq!(ptr::from_ref(work.2.as_ref()), rejected_pointer);
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (origin, retirement) = native.expect("joined queue cleanup");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let state = retirement
        .behavior()
        .expect("retained queue")
        .base()
        .state();
    assert_eq!(state.available().len(), 0);
    assert_eq!(state.queued(), 0);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep original setup rejection and real namespace retry in one controller."
)]
fn prepared_setup_rejection_keeps_early_endpoints_and_namespace_for_retry() {
    let scope = ApplicationScope::new();
    let mut worker = scope.external::<Assignments>().expect("early worker");
    let mut admissions = scope.external::<Admissions>().expect("early admission");
    let root = Queue::new(0).stop_on_shutdown();
    let spaces = QueueRegistration {
        queues: ActorSpace::new(),
        failure: Mutex::new(Some(String::from("root registration unavailable"))),
    };
    let value = Box::new(String::from("original retry payload"));
    let pointer = ptr::from_ref(value.as_ref());
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller host");
    let (outcome, native, notifications) = host.block_on(async {
        let (execution, receiver) = scope
            .with_topology(root, spaces)
            .execute_with(|application| async move {
                let caller = application.sender().expect("retried sender");
                assert_ne!(caller.address(), worker.address());
                assert_ne!(caller.address(), admissions.address());
                caller
                    .send(WorkQueueMessage::Available {
                        worker: worker.recipient(),
                    })
                    .await
                    .expect("retried availability");
                caller
                    .send(WorkQueueMessage::Submit {
                        value,
                        reply_to: admissions.recipient(),
                    })
                    .await
                    .expect("original work submitted");
                let admitted = admissions
                    .receive()
                    .await
                    .expect("original admission reply")
                    .message;
                assert_eq!(admitted, WorkQueueOutcome::Dispatched { queued: 0 });
                let value = worker
                    .receive()
                    .await
                    .expect("original early worker")
                    .message;
                let shutdown = application.request_shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                value
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("host entered");
            });
        let mut execution = Box::pin(execution);
        let failed_setup = poll_fn(|context| {
            Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
                execution.as_mut().poll(context)
            })))
        })
        .await;
        drop(execution);
        assert!(failed_setup.is_err());
        let (outcome, native, notifications) = receiver.await;
        assert!(native.is_err());
        assert!(notifications.is_err());
        let ApplicationOutcome::Prepared {
            inputs: (root, spaces, scope),
            work,
            cleanup,
        } = outcome
        else {
            panic!("original root, topology, namespace and uninvoked work returned");
        };
        assert!(matches!(
            &cleanup,
            Err(ApplicationCleanupError::PublicationClosed(_))
        ));
        drop(cleanup);
        scope
            .with_topology(root, spaces)
            .run_with(work)
            .await
            .unwrap_or_else(|original| {
                drop(original);
                panic!("same namespace retries");
            })
    });
    let (value, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("retry completes");
    });
    assert_eq!(ptr::from_ref(value.as_ref()), pointer);
    assert_eq!(value.as_str(), "original retry payload");
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (_, retirement) = native.expect("retried root custody");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    assert_eq!(
        retirement
            .behavior()
            .expect("retained queue")
            .base()
            .state()
            .queued(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_outside_stash_preserves_early_dispatch_and_retirement() {
    let scope = ApplicationScope::new();
    let mut worker = scope.external::<Assignments>().expect("early worker");
    let mut admissions = scope.external::<Admissions>().expect("early admission");
    let value = Box::new(String::from("released work"));
    let pointer = ptr::from_ref(value.as_ref());
    let root = WorkQueue::new(0)
        .with_stash(|queue, message| match message {
            WorkQueueMessage::Submit { .. } if queue.base().state().available().is_empty() => {
                StashRoute::Stash
            }
            WorkQueueMessage::Available { .. } => StashRoute::Release,
            WorkQueueMessage::Submit { .. } | WorkQueueMessage::Withdraw { .. } => {
                StashRoute::Deliver
            }
        })
        .stop_on_shutdown();
    let (outcome, native, notifications) = scope
        .with_root(root)
        .run_with(|application| async move {
            let caller = application.sender().expect("live root sender");
            assert_ne!(caller.address(), worker.address());
            assert_ne!(caller.address(), admissions.address());
            caller
                .send(WorkQueueMessage::Submit {
                    value,
                    reply_to: admissions.recipient(),
                })
                .await
                .expect("work stashed before availability");
            caller
                .send(WorkQueueMessage::Available {
                    worker: worker.recipient(),
                })
                .await
                .expect("availability releases original work");
            let admitted = admissions
                .receive()
                .await
                .expect("released admission")
                .message;
            assert_eq!(admitted, WorkQueueOutcome::Dispatched { queued: 0 });
            let value = worker
                .receive()
                .await
                .expect("exact released payload")
                .message;
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            value
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("wrapped application starts");
        });
    let (value, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("complete wrapped custody");
    });
    assert_eq!(ptr::from_ref(value.as_ref()), pointer);
    assert_eq!(value.as_str(), "released work");
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (_, retirement) = native.expect("joined wrapped root");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let state = retirement
        .behavior()
        .expect("original queue retained")
        .base()
        .state();
    assert_eq!(state.queued(), 0);
    assert_eq!(state.available().len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn stash_outside_shutdown_preserves_early_dispatch_and_retirement() {
    let scope = ApplicationScope::new();
    let mut worker = scope.external::<Assignments>().expect("early worker");
    let mut admissions = scope.external::<Admissions>().expect("early admission");
    let value = Box::new(String::from("released work"));
    let pointer = ptr::from_ref(value.as_ref());
    let root = WorkQueue::new(0)
        .stop_on_shutdown()
        .with_stash(|queue, message| match message {
            WorkQueueMessage::Submit { .. } if queue.base().state().available().is_empty() => {
                StashRoute::Stash
            }
            WorkQueueMessage::Available { .. } => StashRoute::Release,
            WorkQueueMessage::Submit { .. } | WorkQueueMessage::Withdraw { .. } => {
                StashRoute::Deliver
            }
        });
    let (outcome, native, notifications) = scope
        .with_root(root)
        .run_with(|application| async move {
            let caller = application.sender().expect("live root sender");
            assert_ne!(caller.address(), worker.address());
            assert_ne!(caller.address(), admissions.address());
            caller
                .send(WorkQueueMessage::Submit {
                    value,
                    reply_to: admissions.recipient(),
                })
                .await
                .expect("work stashed before availability");
            caller
                .send(WorkQueueMessage::Available {
                    worker: worker.recipient(),
                })
                .await
                .expect("availability releases original work");
            let admitted = admissions
                .receive()
                .await
                .expect("released admission")
                .message;
            assert_eq!(admitted, WorkQueueOutcome::Dispatched { queued: 0 });
            let value = worker
                .receive()
                .await
                .expect("exact released payload")
                .message;
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            value
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("wrapped application starts");
        });
    let (value, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("complete wrapped custody");
    });
    assert_eq!(ptr::from_ref(value.as_ref()), pointer);
    assert_eq!(value.as_str(), "released work");
    let () = cleanup.expect("application cleanup completes");
    let ActorNotificationReceipts {
        termination,
        retirement: retirement_notification,
    } = notifications.expect("independent original notification receipts");
    assert!(termination.is_ok());
    assert!(retirement_notification.is_ok());
    let (_, retirement) = native.expect("joined wrapped root");
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let state = retirement
        .behavior()
        .expect("original queue retained")
        .base()
        .state();
    assert_eq!(state.queued(), 0);
    assert_eq!(state.available().len(), 0);
}
