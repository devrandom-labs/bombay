//! A decoder unwind must not preempt the existing Node's joined finalization.
use super::protocol::{
    Binding, CounterOperation, EndpointRole, ProtectedMessage, ProviderProfile, Purpose,
    ReadReplyRef, ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope,
    limits, metadata, recorded,
};
use super::{
    Counter, CounterMessage, CounterValue, LedgerCommand, MailLedger, access, completed_actor,
};
use crate::RetirementOutcome;
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationClient, MailboxCapacity};
use crate::remote::{
    Attempt, Export, ExportLimits, Metadata, NodeCloseError, RefusalRetention, reply, submission,
};
use crate::{ActorRetirement, MailAddr, TrySendError, WorkOutcome};
use behavior::{EstablishedRecipient, Never, User};
use core::future::{IntoFuture, pending, poll_fn, ready};
use core::task::Poll;
use futures_util::FutureExt;
use std::cell::{Cell, Ref, RefCell};
use std::error::Error;
use std::io;
use std::mem;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any, resume_unwind};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::timeout;
use zenoh::bytes::ZBytes;
use zenoh::handlers::CallbackDrop;
use zenoh::query::{ConsolidationMode, Reply};
use zenoh::sample::{Locality, Sample};

enum QueryCompletion {
    BeforeClose,
    AfterTimeout,
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one actual held callback preserves native close results and independent concurrent Node retirement"
)]
fn session_close_returns_native_timeout_while_query_callback_is_held() {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the native close test runtime opens")
        .block_on(async {
            for completion in [QueryCompletion::BeforeClose, QueryCompletion::AfterTimeout] {
                timeout(Duration::from_secs(40), async {
                    let config = configuration("tls/localhost:0", &EndpointRole::Receiver)
                        .to_configuration()
                        .unwrap();
                    let session = zenoh::open(config).await.unwrap();
                    let queryable = session
                        .declare_queryable("bombay/close/query-timeout")
                        .allowed_origin(Locality::SessionLocal)
                        .await
                        .unwrap();
                    // Disconnect on controller cancellation before native owners drop.
                    let (release, continuation) = mpsc::channel::<()>();
                    let continuation = Mutex::new(continuation);
                    let (entered, callback_entry) = oneshot::channel();
                    let entered = Mutex::new(Some(entered));
                    let (destroyed, callback_destruction) = oneshot::channel();
                    let mut destroyed = Some(destroyed);
                    session
                        .get("bombay/close/query-timeout")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None)
                        .with(CallbackDrop {
                            callback: move |original: Reply| {
                                eprintln!(
                                    "native-close-component: callback entered at {:?}",
                                    Instant::now()
                                );
                                let announcement = entered.lock().unwrap().take();
                                match announcement {
                                    Some(announcement) => match announcement.send(original) {
                                        Ok(()) => match task::block_in_place(|| {
                                            eprintln!(
                                                "native-close-component: callback wait enter at {:?}",
                                                Instant::now()
                                            );
                                            continuation.lock().unwrap().recv()
                                        }) {
                                            Ok(()) | Err(_) => {
                                                eprintln!(
                                                    "native-close-component: callback wait returned at {:?}",
                                                    Instant::now()
                                                );
                                            }
                                        },
                                        Err(original) => drop(original),
                                    },
                                    None => drop(original),
                                }
                            },
                            drop: move || {
                                if let Some(publication) = destroyed.take() {
                                    match publication.send(()) {
                                        Ok(()) | Err(()) => {}
                                    }
                                }
                            },
                        })
                        .await
                        .unwrap();
                    let query = queryable.recv_async().await.unwrap();
                    let original = callback_entry
                        .await
                        .expect("the actual tracked query-timeout callback entered");
                    let (retirement_notice, mut retirement) = oneshot::channel();
                    let retirement_thread = thread::spawn(move || {
                        let original = catch_unwind(
                            access::native_allow_does_not_replace_the_application_grant,
                        );
                        match retirement_notice.send(original) {
                            Ok(()) => {}
                            Err(original) => drop(original),
                        }
                    });
                    let progress = timeout(Duration::from_secs(15), &mut retirement).await;
                    let (closed, released, destroyed) = match &completion {
                        QueryCompletion::BeforeClose => {
                            let released = release.send(());
                            let destroyed = callback_destruction.await;
                            let closed = AssertUnwindSafe(session.close().into_future())
                                .catch_unwind()
                                .await;
                            (closed, released, destroyed)
                        }
                        QueryCompletion::AfterTimeout => {
                            let closed = AssertUnwindSafe(session.close().into_future())
                                .catch_unwind()
                                .await;
                            let released = release.send(());
                            let destroyed = callback_destruction.await;
                            (closed, released, destroyed)
                        }
                    };
                    // This notifier follows callback return, but precedes field/task
                    // destruction. A repeated close does not prove the first joined.
                    drop(query);
                    drop(queryable);
                    let repeated = session.close().await;
                    drop(session);
                    let retirement = match progress {
                        Ok(original) => Ok(original),
                        Err(deadline) => Err((deadline, retirement.await)),
                    };
                    let joined = task::spawn_blocking(move || retirement_thread.join()).await;

                    assert!(matches!(&joined, Ok(Ok(()))));
                    assert!(released.is_ok());
                    assert!(destroyed.is_ok());
                    assert!(repeated.is_ok());
                    match original.result() {
                        Err(cause) => assert_eq!(cause.payload().to_bytes().as_ref(), b"Timeout"),
                        Ok(_) => panic!("the retained native reply must come from query timeout"),
                    }
                    match (completion, closed) {
                        (QueryCompletion::BeforeClose, Ok(Ok(()))) => {}
                        (QueryCompletion::AfterTimeout, Ok(Err(cause))) => {
                            assert!(cause.to_string().starts_with("close operation timed out!"));
                        }
                        _ => panic!(
                            "actual native close must retain its selected callback completion"
                        ),
                    }
                    assert!(
                        matches!(&retirement, Ok(Ok(Ok(())))),
                        "a held query callback must let the native Allow journey retire before release"
                    );
                })
                .await
                .expect("the native close controls finish within the existing watchdog");
            }
        });
}

enum SubscriptionRetirement {
    Clean,
    Panic(Box<u64>),
}

struct Subscription {
    received: Arc<Mutex<Vec<ZBytes>>>,
    disposed: Arc<AtomicUsize>,
    retirement: Arc<Mutex<SubscriptionRetirement>>,
}

impl Subscription {
    fn receive(&self, sample: &Sample) {
        self.received.lock().unwrap().push(sample.payload().clone());
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.disposed.fetch_add(1, Ordering::SeqCst);
        let retirement = mem::replace(
            &mut *self.retirement.lock().unwrap(),
            SubscriptionRetirement::Clean,
        );
        match retirement {
            SubscriptionRetirement::Clean => {}
            SubscriptionRetirement::Panic(original) => panic_any(original),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_disposal_panics_in_actual_session_close() {
    timeout(Duration::from_secs(40), async {
        for disposition in [
            SubscriptionRetirement::Clean,
            SubscriptionRetirement::Panic(Box::new(509)),
        ] {
            let expected = match &disposition {
                SubscriptionRetirement::Clean => None,
                SubscriptionRetirement::Panic(original) => Some(ptr::from_ref(original.as_ref())),
            };
            let received = Arc::new(Mutex::new(Vec::new()));
            let disposed = Arc::new(AtomicUsize::new(0));
            let retirement = Arc::new(Mutex::new(SubscriptionRetirement::Clean));
            let config = configuration("tls/localhost:0", &EndpointRole::Receiver)
                .to_configuration()
                .unwrap();
            let session = zenoh::open(config).await.unwrap();
            let subscription = Subscription {
                received: Arc::clone(&received),
                disposed: Arc::clone(&disposed),
                retirement: Arc::clone(&retirement),
            };
            session
                .declare_subscriber("bombay/close/disposal")
                .allowed_origin(Locality::SessionLocal)
                .callback(move |sample| subscription.receive(&sample))
                .background()
                .await
                .unwrap();
            session
                .put("bombay/close/disposal", vec![17_u8])
                .allowed_destination(Locality::SessionLocal)
                .await
                .unwrap();
            let delivered = received.lock().unwrap().pop();
            let remaining = received.lock().unwrap().len();
            let before_close = disposed.load(Ordering::SeqCst);
            let delivered = delivered.expect("the actual local callback received the sample");
            assert_eq!(delivered.to_bytes().as_ref(), &[17]);
            assert_eq!(remaining, 0);
            assert_eq!(before_close, 0);

            // Arm only after setup; an earlier setup assertion cannot double-panic
            // while disposing a live native Session.
            *retirement.lock().unwrap() = disposition;
            let acquired = AssertUnwindSafe(session.close().into_future())
                .catch_unwind()
                .await;
            let closed_before_repeat = session.is_closed();
            let first_disposals = disposed.load(Ordering::SeqCst);
            // A repeated close can find primitives already absent. It is not a
            // substitute for the interrupted first close's task-join result.
            let repeated = session.close().await;
            let final_disposals = disposed.load(Ordering::SeqCst);
            drop(session);

            assert!(closed_before_repeat);
            assert!(repeated.is_ok());
            assert_eq!(first_disposals, 1);
            assert_eq!(final_disposals, 1);
            match (expected, acquired) {
                (None, Ok(result)) => assert!(result.is_ok()),
                (Some(allocation), Err(original)) => {
                    let original = original
                        .downcast::<Box<u64>>()
                        .unwrap_or_else(|_| panic!("the exact callback disposal cause returns"));
                    assert_eq!(ptr::from_ref(original.as_ref().as_ref()), allocation);
                    assert_eq!(**original, 509);
                }
                _ => panic!("the real close must preserve its selected callback disposition"),
            }
        }
    })
    .await
    .expect("the native callback disposal controls complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one real close cut compares healthy and panic custody after useful Counter work and joined actor retirement"
)]
async fn session_close_panic_preserves_acquired_application_outcome() {
    timeout(Duration::from_secs(40), async {
        for disposition in [
            SubscriptionRetirement::Clean,
            SubscriptionRetirement::Panic(Box::new(521)),
        ] {
            let expected = match &disposition {
                SubscriptionRetirement::Clean => None,
                SubscriptionRetirement::Panic(original) => Some(ptr::from_ref(original.as_ref())),
            };
            let registered: [ProtectedMessage; 0] = [];
            let binding = Binding {
                deployment: "close-outcome",
                caller: "caller",
                controller: "owner",
                actor: "counter",
                host: "receiver",
                protocol: "counter",
                target_assignment: "root",
                logical_session: 523,
            };
            let verify = async |bytes: &[u8], proof: &[u8]| {
                recorded::verify::<CounterOperation>(
                    &ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    },
                    &registered,
                    binding,
                )
                .await
            };
            let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
                Err::<Box<[u8]>, _>(VerificationError::Invalid)
            };
            let current =
                |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
                    let scope = recorded::projection(receipt);
                    if scope.kind == Purpose::Request
                        && scope.bombay.runtime_generation.as_ref() == runtime
                        && scope.bombay.actor_generation == target.0
                        && recorded::identity(receipt).0 == &binding
                    {
                        Ok(())
                    } else {
                        Err(VerificationError::Invalid)
                    }
                };
            let maximum = NonZeroUsize::new(2).unwrap();
            let export = Export::new(
                "bombay/close/outcome",
                ExportLimits {
                    refusals: RefusalRetention::Retain,
                    queued_queries: maximum,
                    retained_queries: maximum,
                    retained_requests: maximum,
                    diagnostics: maximum,
                },
                limits(),
                |bytes: &[u8]| {
                    serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                        .map(|request| request.operation)
                },
                reply(
                    |CounterOperation::Read| {
                        Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| {
                            CounterMessage::Read(reply_to)
                        })
                    },
                    |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                        let scope = recorded::projection(receipt);
                        serde_json::to_vec(&ReadReplyRef {
                            bombay: &scope.bombay,
                            kind: Purpose::ReadReply,
                            operation: &scope.operation,
                            value: &original.message,
                        })
                        .map(Vec::into_boxed_slice)
                    },
                ),
                &verify,
                &protect,
                |left, right| recorded::identity(left) == recorded::identity(right),
                &current,
                &current,
            );
            let node = Application::new(Counter::new().stop_on_shutdown())
                .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
                .await
                .unwrap_or_else(|_| panic!("the real Counter Node opens before the close cut"));
            let session = node.session.clone();
            let received = Arc::new(Mutex::new(Vec::new()));
            let disposed = Arc::new(AtomicUsize::new(0));
            let retirement = Arc::new(Mutex::new(SubscriptionRetirement::Clean));
            let subscription = Subscription {
                received: Arc::clone(&received),
                disposed: Arc::clone(&disposed),
                retirement: Arc::clone(&retirement),
            };
            session
                .declare_subscriber("bombay/close/counter")
                .allowed_origin(Locality::SessionLocal)
                .callback(move |sample| subscription.receive(&sample))
                .background()
                .await
                .unwrap();
            session
                .put("bombay/close/counter", vec![19_u8])
                .allowed_destination(Locality::SessionLocal)
                .await
                .unwrap();
            let delivered = received.lock().unwrap().pop().unwrap();
            let remaining = received.lock().unwrap().len();
            let before_close = disposed.load(Ordering::SeqCst);
            assert_eq!(delivered.to_bytes().as_ref(), &[19]);
            assert_eq!(remaining, 0);
            assert_eq!(before_close, 0);

            let bound_address = Cell::new(None);
            let bound_address_ref = &bound_address;
            let mut node = node.exports(export, async move |application| {
                let target = application.root().established_recipient();
                let address = application
                    .actor_address(&target)
                    .expect("the real binding target belongs to this Application");
                bound_address_ref.set(Some(address));
                Ok::<_, Never>(target)
            });
            let observed_value = Cell::new(None);
            let observed_value_ref = &observed_value;
            let calls = Cell::new(0);
            let calls_ref = &calls;
            let output = Box::new(541_u64);
            let output_allocation = ptr::from_ref(output.as_ref());
            let acquired = AssertUnwindSafe(node.run_with(async move |application| {
                calls_ref.set(calls_ref.get() + 1);
                let mut values = application.client::<CounterValue>().unwrap();
                let increment = values.send(CounterMessage::Increment).await;
                let read = values.send(CounterMessage::Read(values.recipient())).await;
                let original = values.receive().await;
                if let Some(original) = original.as_ref() {
                    observed_value_ref.set(Some((original.from, original.message)));
                }
                let shutdown = application.shutdown();
                let joined = application.retirement().await;
                // No assertions or awaited work follow arming. Native final close,
                // after Application receiving, owns the callback's actual disposal.
                *retirement.lock().unwrap() = disposition;
                (output, increment, read, original, shutdown, joined)
            }))
            .catch_unwind()
            .await;
            let acquired_close = node.closed.as_ref().map(Result::is_ok);
            let closed_before_rescue = session.is_closed();
            let first_disposals = disposed.load(Ordering::SeqCst);
            // This is only best-effort cleanup. It cannot supply the interrupted
            // first close's result or establish that its SDK tasks joined.
            let rescued = session.close().await;
            let final_disposals = disposed.load(Ordering::SeqCst);
            let originals = match node.into_parts() {
                Ok(originals) => Some(originals),
                Err(node) => {
                    drop(node);
                    None
                }
            };
            drop(session);

            assert!(rescued.is_ok());
            assert!(closed_before_rescue);
            assert_eq!(first_disposals, 1);
            assert_eq!(final_disposals, 1);
            assert_eq!(calls.get(), 1);
            let origin = bound_address.get().expect("actual binding recorded the root");
            assert_eq!(observed_value.get(), Some((origin, 1)));
            let outcome = match acquired {
                Ok(outcome) => Some(outcome.unwrap_or_else(|_| panic!("the actual run was issued"))),
                Err(original) => {
                    let original = original
                        .downcast::<Box<u64>>()
                        .unwrap_or_else(|_| panic!("only the actual callback disposal cause escapes"));
                    assert_eq!(Some(ptr::from_ref(original.as_ref().as_ref())), expected);
                    assert_eq!(**original, 521);
                    None
                }
            };
            eprintln!(
                "close cut: outcome={}, node_close={acquired_close:?}, native_closed={closed_before_rescue}, extraction={}",
                outcome.is_some(), originals.is_some()
            );
            assert!(
                outcome.is_some(),
                "Node must return its acquired Application outcome after native close panics"
            );
            let (work, report, native, notifications, ()) = outcome.unwrap().into_parts();
            let (output, cleanup) = work
                .try_into_completed()
                .unwrap_or_else(|_| panic!("actual completed Counter Work remains owned"));
            let (output, increment, read, original, shutdown, joined) = output;
            assert_eq!(ptr::from_ref(output.as_ref()), output_allocation);
            assert_eq!(*output, 541);
            assert!(increment.is_ok() && read.is_ok() && shutdown.is_ok() && cleanup.is_ok());
            let original = original.expect("the actual original Counter reply returns");
            assert_eq!((original.from, original.message), (origin, 1));
            assert_eq!(report, Some(joined));
            let (root, native) = native.expect("the complete native root remains");
            assert_eq!(root.address(), origin);
            let _counter = completed_actor(root, native);
            let notifications = notifications.expect("both original notification results remain");
            assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
            let (export, binding, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
                originals.expect("the actual completed Node remains extractable");
            assert!(binding.as_ref().is_some_and(Result::is_ok));
            match (expected, closed) {
                (None, Ok(())) => {}
                (Some(allocation), Err(NodeCloseError::PollPanic { original })) => {
                    let original = original
                        .downcast::<Box<u64>>()
                        .unwrap_or_else(|_| panic!("Node retains the actual callback disposal type"));
                    assert_eq!(ptr::from_ref(original.as_ref().as_ref()), allocation);
                    assert_eq!(**original, 521);
                }
                _ => panic!("Node must retain the exact close disposition and original cause"),
            }
            assert!(polling.is_none() && disposal.is_none());
            assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
            let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) =
                export.into_parts();
            assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
            assert!(diagnostics.is_empty() && requests.is_empty());
        }
    })
    .await
    .expect("the real Node close controls complete before the failure-only watchdog");
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one real Node trace preserves every acquired Counter result beside the actual native close timeout"
)]
fn session_close_native_timeout_preserves_acquired_application_outcome() {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the native close test runtime opens")
        .block_on(async {
            for completion in [QueryCompletion::BeforeClose, QueryCompletion::AfterTimeout] {
                timeout(Duration::from_secs(40), async {
                    let registered: [ProtectedMessage; 0] = [];
                    let binding = Binding {
                        deployment: "native-close-outcome",
                        caller: "caller",
                        controller: "owner",
                        actor: "counter",
                        host: "receiver",
                        protocol: "counter",
                        target_assignment: "root",
                        logical_session: 557,
                    };
                    let verify = async |bytes: &[u8], proof: &[u8]| {
                        recorded::verify::<CounterOperation>(
                            &ProtectedMessage {
                                protected: bytes.into(),
                                proof: proof.into(),
                            },
                            &registered,
                            binding,
                        )
                        .await
                    };
                    let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
                        Err::<Box<[u8]>, _>(VerificationError::Invalid)
                    };
                    let current =
                        |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
                            let scope = recorded::projection(receipt);
                            if scope.kind == Purpose::Request
                                && scope.bombay.runtime_generation.as_ref() == runtime
                                && scope.bombay.actor_generation == target.0
                                && recorded::identity(receipt).0 == &binding
                            {
                                Ok(())
                            } else {
                                Err(VerificationError::Invalid)
                            }
                        };
                    let maximum = NonZeroUsize::new(2).unwrap();
                    let export = Export::new(
                        "bombay/native-close/outcome",
                        ExportLimits {
                            refusals: RefusalRetention::Retain,
                            queued_queries: maximum,
                            retained_queries: maximum,
                            retained_requests: maximum,
                            diagnostics: maximum,
                        },
                        limits(),
                        |bytes: &[u8]| {
                            serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                                .map(|request| request.operation)
                        },
                        reply(
                            |CounterOperation::Read| {
                                Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| {
                                    CounterMessage::Read(reply_to)
                                })
                            },
                            |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                                let scope = recorded::projection(receipt);
                                serde_json::to_vec(&ReadReplyRef {
                                    bombay: &scope.bombay,
                                    kind: Purpose::ReadReply,
                                    operation: &scope.operation,
                                    value: &original.message,
                                })
                                .map(Vec::into_boxed_slice)
                            },
                        ),
                        &verify,
                        &protect,
                        |left, right| recorded::identity(left) == recorded::identity(right),
                        &current,
                        &current,
                    );
                    let node = Application::new(Counter::new().stop_on_shutdown())
                        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
                        .await
                        .unwrap_or_else(|_| panic!("the real Counter Node opens before the close cut"));
                    let session = node.session.clone();
                    let queryable = session
                        .declare_queryable("bombay/native-close/query-timeout")
                        .allowed_origin(Locality::SessionLocal)
                        .await
                        .unwrap();
                    // Disconnect before native owners drop if this controller is cancelled.
                    let (release, continuation) = mpsc::channel::<()>();
                    let continuation = Mutex::new(continuation);
                    let (entered, callback_entry) = oneshot::channel();
                    let entered = Mutex::new(Some(entered));
                    let (destroyed, mut callback_destruction) = oneshot::channel();
                    let mut destroyed = Some(destroyed);
                    session
                        .get("bombay/native-close/query-timeout")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None)
                        .with(CallbackDrop {
                            callback: move |original: Reply| {
                                eprintln!(
                                    "native-close-outcome: callback entered at {:?}",
                                    Instant::now()
                                );
                                let announcement = entered.lock().unwrap().take();
                                match announcement {
                                    Some(announcement) => match announcement.send(original) {
                                        Ok(()) => match task::block_in_place(|| {
                                            eprintln!(
                                                "native-close-outcome: callback wait enter at {:?}",
                                                Instant::now()
                                            );
                                            continuation.lock().unwrap().recv()
                                        }) {
                                            Ok(()) | Err(_) => {
                                                eprintln!(
                                                    "native-close-outcome: callback wait returned at {:?}",
                                                    Instant::now()
                                                );
                                            }
                                        },
                                        Err(original) => drop(original),
                                    },
                                    None => drop(original),
                                }
                            },
                            drop: move || {
                                if let Some(publication) = destroyed.take() {
                                    match publication.send(()) {
                                        Ok(()) | Err(()) => {}
                                    }
                                }
                            },
                        })
                        .await
                        .unwrap();
                    let query = queryable.recv_async().await.unwrap();
                    let native_reply = callback_entry
                        .await
                        .expect("the actual tracked query-timeout callback entered before Node execution");
                    let bound_address = Cell::new(None);
                    let bound_address_ref = &bound_address;
                    let mut node = node.exports(export, async move |application| {
                        let target = application.root().established_recipient();
                        let address = application
                            .actor_address(&target)
                            .expect("the real binding target belongs to this Application");
                        bound_address_ref.set(Some(address));
                        Ok::<_, Never>(target)
                    });
                    let observed_value = Cell::new(None);
                    let observed_value_ref = &observed_value;
                    let calls = Cell::new(0);
                    let calls_ref = &calls;
                    let output = Box::new(541_u64);
                    let output_allocation = ptr::from_ref(output.as_ref());
                    let completion_ref = &completion;
                    let release_ref = &release;
                    let callback_destruction_ref = &mut callback_destruction;
                    let acquired = AssertUnwindSafe(node.run_with(async move |application| {
                        calls_ref.set(calls_ref.get() + 1);
                        let mut values = application.client::<CounterValue>().unwrap();
                        let increment = values.send(CounterMessage::Increment).await;
                        let read = values.send(CounterMessage::Read(values.recipient())).await;
                        let original = values.receive().await;
                        if let Some(original) = original.as_ref() {
                            observed_value_ref.set(Some((original.from, original.message)));
                        }
                        let shutdown = application.shutdown();
                        let joined = application.retirement().await;
                        let before_close = match completion_ref {
                            QueryCompletion::BeforeClose => {
                                let released = release_ref.send(());
                                let destroyed = callback_destruction_ref.await;
                                Some((released, destroyed))
                            }
                            QueryCompletion::AfterTimeout => None,
                        };
                        (output, increment, read, original, shutdown, joined, before_close)
                    }))
                    .catch_unwind()
                    .await;
                    // An uncompleted Work can still own the closure's gate borrows.
                    // Consume that carrier before reclaiming controller-owned gates.
                    let completed = match acquired {
                        Ok(Ok(outcome)) => {
                            let (work, report, native, notifications, ()) = outcome.into_parts();
                            match work.try_into_completed() {
                                Ok((output, cleanup)) => {
                                    Some((output, cleanup, report, native, notifications))
                                }
                                Err(original) => {
                                    drop(original);
                                    None
                                }
                            }
                        }
                        Ok(Err(original)) => {
                            drop(original);
                            None
                        }
                        Err(original) => {
                            drop(original);
                            None
                        }
                    };
                    let native_allocation = match &node.closed {
                        Some(Err(NodeCloseError::Native { cause })) => {
                            Some(ptr::from_ref(cause.as_ref()).cast::<()>())
                        }
                        Some(Ok(()) | Err(NodeCloseError::PollPanic { .. })) | None => None,
                    };
                    let after_close = match &completion {
                        QueryCompletion::BeforeClose => None,
                        QueryCompletion::AfterTimeout => {
                            let released = release.send(());
                            let destroyed = callback_destruction.await;
                            Some((released, destroyed))
                        }
                    };
                    // Also disconnect if an unexpected refusal never invoked healthy Work.
                    drop(release);
                    // CallbackDrop announces destruction beginning, not full task joining.
                    // A repeated close is best-effort cleanup, never a first-close result.
                    drop(query);
                    drop(queryable);
                    let rescued = session.close().await;
                    let originals = match node.into_parts() {
                        Ok(originals) => Some(originals),
                        Err(node) => {
                            drop(node);
                            None
                        }
                    };
                    drop(session);

                    assert!(rescued.is_ok());
                    assert_eq!(calls.get(), 1);
                    match native_reply.result() {
                        Err(cause) => assert_eq!(cause.payload().to_bytes().as_ref(), b"Timeout"),
                        Ok(_) => panic!("the actual tracked query must return its native timeout reply"),
                    }
                    let origin = bound_address.get().expect("actual binding recorded the root");
                    assert_eq!(observed_value.get(), Some((origin, 1)));
                    let (output, cleanup, report, native, notifications) =
                        completed.expect("the actual completed Counter Work and acquired outcome remain");
                    let (output, increment, read, original, shutdown, joined, before_close) = output;
                    assert_eq!(ptr::from_ref(output.as_ref()), output_allocation);
                    assert_eq!(*output, 541);
                    assert!(increment.is_ok() && read.is_ok() && shutdown.is_ok() && cleanup.is_ok());
                    let original = original.expect("the actual original Counter reply returns");
                    assert_eq!((original.from, original.message), (origin, 1));
                    assert_eq!(report, Some(joined));
                    let (root, native) = native.expect("the complete native root remains");
                    assert_eq!(root.address(), origin);
                    let _counter = completed_actor(root, native);
                    let notifications = notifications.expect("both original notification results remain");
                    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
                    let (export, binding, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
                        originals.expect("the actual completed Node remains extractable");
                    assert!(binding.as_ref().is_some_and(Result::is_ok));
                    let native_source = closed
                        .as_ref()
                        .err()
                        .and_then(Error::source)
                        .map(|source| ptr::from_ref(source).cast::<()>());
                    assert_eq!(native_source, native_allocation);
                    match (completion, closed, before_close, after_close) {
                        (QueryCompletion::BeforeClose, Ok(()), Some((released, destroyed)), None) => {
                            assert!(released.is_ok() && destroyed.is_ok());
                            assert!(native_allocation.is_none());
                        }
                        (
                            QueryCompletion::AfterTimeout,
                            Err(NodeCloseError::Native { cause }),
                            None,
                            Some((released, destroyed)),
                        ) => {
                            assert!(released.is_ok() && destroyed.is_ok());
                            assert_eq!(
                                Some(ptr::from_ref(cause.as_ref()).cast::<()>()),
                                native_allocation
                            );
                            assert!(cause.to_string().starts_with("close operation timed out!"));
                        }
                        _ => panic!("Node must preserve the actual native close disposition beside its acquired outcome"),
                    }
                    assert!(polling.is_none() && disposal.is_none());
                    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
                    let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) =
                        export.into_parts();
                    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
                    assert!(diagnostics.is_empty() && requests.is_empty());
                })
                .await
                .expect("the actual Node native-error controls finish within the existing watchdog");
            }
        });
}

#[test]
fn close_error_keeps_native_original_and_source() {
    // A representation companion, not an induced native close failure.
    let original = Box::new(io::Error::other("original native close cause"));
    let allocation = ptr::from_ref(original.as_ref());
    let closed = NodeCloseError::Native { cause: original };
    let source = closed
        .source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .expect("the original native error remains the standard source");
    assert_eq!(ptr::from_ref(source), allocation);
    assert_eq!(format!("{closed:?}"), "Native { .. }");
    assert_eq!(
        closed.to_string(),
        "the native Session close returned an error"
    );
    let NodeCloseError::Native { cause } = closed else {
        panic!("the native error is not classified as a polling panic");
    };
    let original = cause
        .downcast::<io::Error>()
        .expect("the same native error transfers out once");
    assert_eq!(ptr::from_ref(original.as_ref()), allocation);
    assert_eq!(original.to_string(), "original native close cause");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one unwind trace preserves every independent cleanup and original panic fact; its provider table is immutable throughout verification"
)]
async fn decoder_unwind_preserves_joined_node_finalization() {
    timeout(Duration::from_secs(40), async {
        let requests = RefCell::new(Vec::<ProtectedMessage>::new());
        let claims = RefCell::new(None::<Metadata>);
        let joined = RefCell::new(None);
        let retirement = RefCell::new(None);
        let decoded_allocation = Cell::new(None);
        let cause = Box::new(307_u64);
        let allocation = ptr::from_ref(cause.as_ref());
        let cause = RefCell::new(Some(cause));
        let verifications = Cell::new(0);
        let constructors = Cell::new(0);
        let encodings = Cell::new(0);
        let protections = Cell::new(0);
        let binding = Binding {
            deployment: "single-decoder-unwind",
            caller: "caller",
            controller: "owner",
            actor: "ledger",
            host: "receiver",
            protocol: "ledger",
            target_assignment: "ledger",
            logical_session: 1,
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            let actual = claims.borrow();
            let actual = actual.as_ref().expect("the actual binding supplies claims");
            current_scope(scope, actual, &(), Purpose::Request, deadline)?;
            if scope.bombay.runtime_generation.as_ref() != runtime
                || scope.bombay.actor_generation != target.0
            {
                return Err(VerificationError::Invalid);
            }
            Ok(())
        };
        let export = Export::new(
            "bombay/decoder-unwind",
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes: &[u8]| -> Result<(), Never> {
                decoded_allocation.set(Some(bytes.as_ptr()));
                let original = cause.take().expect("one original decoder panic allocation");
                panic_any(original);
            },
            submission(
                |()| {
                    Ok::<_, ()>(|| {
                        constructors.set(constructors.get() + 1);
                        LedgerCommand::Append(Box::from([29, 31]))
                    })
                },
                |_: &recorded::Receipt<()>,
                 _: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                    encodings.set(encodings.get() + 1);
                    Ok::<_, Never>(Box::<[u8]>::default())
                },
            ),
            async |bytes: &[u8], proof: &[u8]| {
                verifications.set(verifications.get() + 1);
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                recorded::verify::<()>(&original, &requests.borrow(), binding).await
            },
            async |_: &[u8], _: &recorded::Receipt<()>| {
                protections.set(protections.get() + 1);
                Err::<Box<[u8]>, ()>(())
            },
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current,
            &current,
        );
        let node = Application::new(MailLedger {
            entries: Vec::new(),
        })
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
        let runtime = node.runtime_id();
        // Deliberate owning-test access, never an ordinary application port.
        let session = node.session.clone();
        let (configured, configured_request) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let mut actual = metadata(
                ProviderProfile::Recorded,
                "ledger",
                runtime.clone(),
                runtime,
            );
            actual.deployment = binding.deployment.into();
            actual.actor_generation = application.actor_address(&target).unwrap().0;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &(),
            })
            .unwrap()
            .into_boxed_slice();
            let proof: Box<[u8]> = [19, 23].into();
            requests.borrow_mut().push(ProtectedMessage {
                protected: protected.clone(),
                proof: proof.clone(),
            });
            *claims.borrow_mut() = Some(actual);
            *joined.borrow_mut() = application.cleanup();
            *retirement.borrow_mut() = Some(application.lifecycle().retirement());
            let transferred = configured.send(ProtectedMessage { protected, proof });
            transferred.unwrap_or_else(|_| panic!("the actual Work awaits its configured request"));
            Ok::<_, Never>(target)
        });
        let mut operation = Box::pin(node.run_with(|_| async {
            let original = configured_request.await.unwrap();
            let incoming = session
                .get("bombay/decoder-unwind")
                .allowed_destination(Locality::SessionLocal)
                .payload(original.protected.to_vec())
                .attachment(original.proof.to_vec())
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20))
                .await
                .expect("the actual query starts before Work remains pending");
            pending::<()>().await;
            incoming
        }));
        let caught = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                original => Poll::Ready(original),
            }
        })
        .await;
        let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
        let joined = joined
            .take()
            .expect("actual startup supplies its real cleanup-completion observation");
        let joined = joined.await;
        // Original-source escape gets finite external cleanup, not fake Node evidence.
        let external_close = if caught.is_err() {
            Some(node.close().await)
        } else {
            None
        };
        let finalized = node.into_parts();
        let retirement = retirement
            .take()
            .expect("startup retains a separate consumer of the existing root observation")
            .await;
        let remaining_cause = cause.borrow();
        assert!(joined.is_ok() && disposed.is_ok() && session.is_closed());
        assert!(external_close.as_ref().is_none_or(Result::is_ok));
        assert_eq!(verifications.get(), 1);
        assert_eq!(constructors.get(), 0);
        assert_eq!(encodings.get(), 0);
        assert_eq!(protections.get(), 0);
        assert!(remaining_cause.is_none());
        if let Err(original) = &caught {
            let original = original
                .downcast_ref::<Box<u64>>()
                .expect("the external catcher retains the actual original decoder cause");
            assert_eq!(ptr::from_ref(original.as_ref()), allocation);
            assert_eq!(**original, 307);
        }
        assert!(
            finalized.is_ok(),
            "the Node must preserve its finalization facts after a decoder unwind"
        );
        let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            finalized.unwrap_or_else(|_| panic!("the Node acquired its actual post-join close"));
        let Ok(Poll::Ready(Ok(outcome))) = caught else {
            panic!("the actual Node operation must return its acquired outcome");
        };
        let (work, root_report, native, notifications, ()) = outcome.into_parts();
        let (origin, native) =
            native.expect("actual root cancellation remains independently owned");
        let notifications = notifications.expect("both actual root notifications remain owned");
        let (_, _, startup, withdrawal, diagnostics, requests_retained) = export.into_parts();
        let rows = requests_retained
            .into_iter()
            .map(|request| {
                request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("kernel disposal releases the actual request lease"))
            })
            .collect::<Vec<_>>();
        let [row] = rows.as_slice() else {
            panic!("one original verified request survives finalization");
        };
        let receipt = row
            .2
            .as_ref()
            .expect("verification returned")
            .as_ref()
            .unwrap();
        let expected = requests.borrow();
        let [expected] = expected.as_slice() else {
            panic!("one independently configured protected request");
        };
        let polling = polling.expect("retain the actual kernel poll panic outside its producer");
        let polling = polling.downcast_ref::<Box<u64>>().unwrap();
        let ActorRetirement::Retained {
            custody,
            outcome: RetirementOutcome::OwnerCancelled { .. },
        } = native
        else {
            panic!("kernel disposal cancels the original still-running Ledger");
        };
        let claims = claims.borrow();
        let claims = claims
            .as_ref()
            .expect("actual startup metadata remains available");
        assert!(matches!(
            &work,
            WorkOutcome::Interrupted { cleanup: Ok(()) }
        ));
        assert_eq!(root_report, Some(retirement));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(matches!(&bound, Some(Ok(_))) && closed.is_ok());
        assert!(external_close.is_none() && disposal.is_none());
        assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
        assert_eq!(ptr::from_ref(polling.as_ref()), allocation);
        assert_eq!(**polling, 307);
        assert_eq!(origin.address().0, claims.actor_generation);
        assert!(custody.behavior.entries.is_empty() && custody.user.is_empty());
        assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
        assert!(
            custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty()
        );
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty() && row.0.is_none());
        assert_eq!(row.1.0.as_ref(), expected.protected.as_ref());
        assert_eq!(row.1.1.as_ref(), expected.proof.as_ref());
        assert_eq!(Some(row.1.0.as_ptr()), decoded_allocation.get());
        assert_eq!(&recorded::projection(receipt).bombay, claims);
        assert_eq!(recorded::projection(receipt).kind, Purpose::Request);
        assert!(row.3.is_none() && row.4.is_none());
        assert!(row.5.is_none() && row.6.is_none() && row.7.is_empty());
        assert!(row.8.0.is_none() && row.8.1.is_none() && row.8.2.is_none());
        assert!(
            row.9.0.is_none()
                && row.9.1.is_none()
                && row.9.2.is_none()
                && row.9.3.is_none()
                && row.9.4.is_none()
                && row.9.5.is_none()
        );
    })
    .await
    .expect("the complete decoder-unwind and actual cleanup trace terminates");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionStage {
    Scope,
    Admission,
    Retry,
}

enum PermissionRelease<'permission> {
    Return,
    FirstNative {
        announced: oneshot::Sender<()>,
        returned: &'permission Cell<Option<Result<(), ()>>>,
    },
    Panic(Box<u64>),
}

struct PermissionLoan<'permission> {
    claims: Ref<'permission, Option<Metadata>>,
    release: PermissionRelease<'permission>,
}

impl Drop for PermissionLoan<'_> {
    fn drop(&mut self) {
        match mem::replace(&mut self.release, PermissionRelease::Return) {
            PermissionRelease::Return => {}
            PermissionRelease::FirstNative {
                announced,
                returned,
            } => {
                returned.set(Some(announced.send(())));
            }
            PermissionRelease::Panic(original) => {
                let target = self.claims.as_ref().map(|claims| claims.actor_generation);
                panic_any((target, original));
            }
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one real second-Full retry retains the original permission panic, native reply endpoint and complete Node finalization; nominal provider tables are immutable during verification"
)]
fn retry_permission_unwind_preserves_the_second_native_full() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(40), async {
                let requests = RefCell::new(Vec::<ProtectedMessage>::new());
                let claims = RefCell::new(None::<Metadata>);
                let joined = RefCell::new(None);
                let retirement = RefCell::new(None);
                let stage = Cell::new(PermissionStage::Scope);
                let grants = RefCell::new(Vec::new());
                let verified_allocations = Cell::new(None);
                let decoded_allocation = Cell::new(None);
                let constructions = Cell::new(0);
                let encodings = Cell::new(0);
                let protections = Cell::new(0);
                let cause = Box::new(401_u64);
                let cause_allocation = ptr::from_ref(cause.as_ref());
                let cause = RefCell::new(Some(cause));
                let ordinary = RefCell::new(
                    None::<ApplicationClient<EstablishedRecipient<Counter>, CounterValue>>,
                );
                let prefix = RefCell::new(Vec::new());
                let first_full = RefCell::new(None);
                let refill = RefCell::new(Vec::new());
                let second_full = RefCell::new(None);
                let first_entered = RefCell::new(None);
                let first_worker = RefCell::new(None);
                let second_worker = RefCell::new(None);
                let second_entered = RefCell::new(None);
                let (release_second, retry_wait) = mpsc::channel::<()>();
                let retry_wait = RefCell::new(Some(retry_wait));
                let (first_returned, returned_first) = oneshot::channel();
                let first_returned = RefCell::new(Some(first_returned));
                let first_return_receipt = Cell::new(None);
                let incoming = RefCell::new(None);
                let binding = Binding {
                    deployment: "second-full-unwind",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "counter",
                    logical_session: 1,
                };
                let maximum = NonZeroUsize::new(2).unwrap();
                let deadline = Instant::now() + Duration::from_secs(30);
                let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
                    let scope = recorded::projection(receipt);
                    let actual = claims.borrow();
                    {
                        let actual = actual.as_ref().ok_or(VerificationError::Invalid)?;
                        current_scope(scope, actual, &(), Purpose::Request, deadline)?;
                        if scope.bombay.runtime_generation.as_ref() != runtime
                            || scope.bombay.actor_generation != target.0
                        {
                            return Err(VerificationError::Invalid);
                        }
                    }
                    Ok::<_, VerificationError>(actual)
                };
                let current_request =
                    |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
                        let claims = current(receipt, runtime, target)?;
                        let actual = stage.get();
                        grants.borrow_mut().push(actual);
                        let release = match actual {
                            PermissionStage::Scope => {
                                stage.set(PermissionStage::Admission);
                                PermissionRelease::Return
                            }
                            PermissionStage::Admission => {
                                stage.set(PermissionStage::Retry);
                                PermissionRelease::FirstNative {
                                    announced: first_returned
                                        .borrow_mut()
                                        .take()
                                        .expect("the first native return has one observer"),
                                    returned: &first_return_receipt,
                                }
                            }
                            PermissionStage::Retry => {
                                // The actual capacity wait has completed. Freeze the
                                // sole actor worker before a real competing sender
                                // consumes that available capacity ahead of retry.
                                let released = retry_wait
                                    .borrow_mut()
                                    .take()
                                    .expect("exactly one actual retry occupies the worker");
                                let (announced, entered) = mpsc::channel::<()>();
                                let worker = tokio::spawn(async move {
                                    let announced = announced.send(());
                                    let released = released.recv_timeout(Duration::from_secs(5));
                                    (announced, released)
                                });
                                *second_worker.borrow_mut() = Some(worker);
                                let entered = entered.recv_timeout(Duration::from_secs(5));
                                *second_entered.borrow_mut() = Some(entered);
                                second_entered.borrow().as_ref().unwrap().as_ref().expect(
                                    "finite setup: the sole actor worker is blocked before refill",
                                );
                                let ordinary = ordinary.borrow();
                                let ordinary = ordinary
                                    .as_ref()
                                    .expect("Work owns an independently allocated actual sender");
                                for _ in 0..2 {
                                    refill
                                        .borrow_mut()
                                        .push(ordinary.try_send(CounterMessage::Increment));
                                }
                                *second_full.borrow_mut() =
                                    Some(ordinary.try_send(CounterMessage::Increment));
                                PermissionRelease::Panic(
                                    cause
                                        .borrow_mut()
                                        .take()
                                        .expect("the actual retry guard owns one original panic"),
                                )
                            }
                        };
                        Ok::<_, VerificationError>(PermissionLoan { claims, release })
                    };
                let export = Export::new(
                    "bombay/second-full-unwind",
                    ExportLimits {
                        refusals: RefusalRetention::Retain,
                        queued_queries: maximum,
                        retained_queries: maximum,
                        retained_requests: maximum,
                        diagnostics: maximum,
                    },
                    limits(),
                    |bytes: &[u8]| {
                        decoded_allocation.set(Some(bytes.as_ptr()));
                        let ReadRequest { operation: (), .. } =
                            serde_json::from_slice::<ReadRequest<()>>(bytes)?;
                        Ok::<_, serde_json::Error>(())
                    },
                    reply(
                        |()| {
                            Ok::<_, ()>(|reply_to| {
                                constructions.set(constructions.get() + 1);
                                CounterMessage::Read(reply_to)
                            })
                        },
                        |_: &recorded::Receipt<()>, _: &User<MailAddr, u64>| {
                            encodings.set(encodings.get() + 1);
                            Ok::<_, Never>(Box::<[u8]>::default())
                        },
                    ),
                    async |bytes: &[u8], proof: &[u8]| {
                        verified_allocations.set(Some((bytes.as_ptr(), proof.as_ptr())));
                        let original = ProtectedMessage {
                            protected: bytes.into(),
                            proof: proof.into(),
                        };
                        recorded::verify::<()>(&original, &requests.borrow(), binding).await
                    },
                    async |_: &[u8], _: &recorded::Receipt<()>| {
                        protections.set(protections.get() + 1);
                        Err::<Box<[u8]>, ()>(())
                    },
                    |left, right| recorded::identity(left) == recorded::identity(right),
                    &current_request,
                    &current,
                );
                let node = Application::new(Counter::new().stop_on_shutdown())
                    .mailbox_capacity(MailboxCapacity::new(2).unwrap())
                    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
                    .await
                    .unwrap_or_else(|_| panic!("the real bounded Counter Node opens"));
                let runtime = node.runtime_id();
                let session = node.session.clone();
                let (configured, configured_request) = oneshot::channel();
                let mut node = node.exports(export, async |application| {
                    let target = application.root().established_recipient();
                    let mut actual = metadata(
                        ProviderProfile::Recorded,
                        "counter",
                        runtime.clone(),
                        runtime,
                    );
                    actual.deployment = binding.deployment.into();
                    actual.actor_generation = application.actor_address(&target).unwrap().0;
                    let protected = serde_json::to_vec(&ReadRequestRef {
                        bombay: &actual,
                        kind: Purpose::Request,
                        operation: &(),
                    })
                    .unwrap()
                    .into_boxed_slice();
                    let proof: Box<[u8]> = [43, 47].into();
                    requests.borrow_mut().push(ProtectedMessage {
                        protected: protected.clone(),
                        proof: proof.clone(),
                    });
                    *claims.borrow_mut() = Some(actual);
                    *joined.borrow_mut() = application.cleanup();
                    *retirement.borrow_mut() = Some(application.lifecycle().retirement());
                    let published = configured.send(ProtectedMessage { protected, proof });
                    published.unwrap_or_else(|_| panic!("Work awaits the actual configured query"));
                    Ok::<_, Never>(target)
                });
                let session_ref = &session;
                let ordinary_ref = &ordinary;
                let first_worker_ref = &first_worker;
                let first_entered_ref = &first_entered;
                let prefix_ref = &prefix;
                let first_full_ref = &first_full;
                let incoming_ref = &incoming;
                let mut operation = Box::pin(node.run_with(move |application| async move {
                    // This sender is owned by Work, not by the permission
                    // callback. Kernel disposal releases the second worker
                    // before waiting for native actor cleanup.
                    let request = configured_request.await.unwrap();
                    let ordinary = application.client::<CounterValue>().unwrap_or_else(|_| {
                        panic!("the independent actual Counter client allocates")
                    });
                    *ordinary_ref.borrow_mut() = Some(ordinary);
                    let (release_first, released) = mpsc::channel::<()>();
                    let (announced, entered) = oneshot::channel();
                    *first_worker_ref.borrow_mut() = Some(tokio::spawn(async move {
                        let announced = announced.send(());
                        let released = released.recv_timeout(Duration::from_secs(5));
                        (announced, released)
                    }));
                    *first_entered_ref.borrow_mut() = Some(entered.await);
                    {
                        let ordinary = ordinary_ref.borrow();
                        let ordinary = ordinary.as_ref().unwrap();
                        for _ in 0..2 {
                            prefix_ref
                                .borrow_mut()
                                .push(ordinary.try_send(CounterMessage::Increment));
                        }
                        *first_full_ref.borrow_mut() =
                            Some(ordinary.try_send(CounterMessage::Increment));
                    }
                    let receiving = session_ref
                        .get("bombay/second-full-unwind")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(request.protected.to_vec())
                        .attachment(request.proof.to_vec())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(20))
                        .await
                        .expect("the actual native query starts");
                    *incoming_ref.borrow_mut() = Some(receiving);
                    returned_first
                        .await
                        .expect("the first native call returns before capacity is released");
                    drop(release_first);
                    pending::<()>().await;
                    drop(release_second);
                }));
                let caught = poll_fn(|context| {
                    match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        original => Poll::Ready(original),
                    }
                })
                .await;
                let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
                let joined = joined
                    .take()
                    .expect("actual startup retains its cleanup observer")
                    .await;
                let external_close = if caught.is_err() {
                    Some(node.close().await)
                } else {
                    None
                };
                let finalized = node.into_parts();
                let retirement = retirement
                    .take()
                    .expect("actual startup retains its independent root observer")
                    .await;
                let first_worker = first_worker
                    .take()
                    .expect("the first real worker was installed")
                    .await;
                let second_worker = second_worker
                    .take()
                    .expect("the retry installed its actual blocker")
                    .await;
                let incoming = incoming
                    .take()
                    .expect("the actual native reply receiver survives Work");
                let mut wire = Vec::new();
                while let Ok(original) = incoming.recv_async().await {
                    wire.push(original);
                }
                let mut ordinary = ordinary
                    .take()
                    .expect("the actual independent client survives Work");
                let caller = ordinary.address();
                ordinary.close_admission();
                let mut ordinary_tail = Vec::new();
                while let Some(original) = ordinary.receive().await {
                    ordinary_tail.push(original);
                }
                let (
                    export,
                    bound,
                    closed,
                    polling,
                    disposal,
                    withdrawal_polling,
                    withdrawal_disposal,
                ) = finalized
                    .unwrap_or_else(|_| panic!("the Node acquires cleanup and actual close"));
                let Ok(Poll::Ready(Ok(outcome))) = caught else {
                    panic!("the actual Node retains the guard panic and returns its outcome");
                };
                let (work, root_report, native, notifications, ()) = outcome.into_parts();
                let (origin, native) = native.expect("actual original actor retirement survives");
                let notifications = notifications.expect("both actual notifications survive");
                let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
                let [request] = <[_; 1]>::try_from(rows)
                    .unwrap_or_else(|_| panic!("one actual protected query is retained"));
                let (
                    canonical,
                    original,
                    verified,
                    decoded,
                    native_reply,
                    encoded,
                    protected,
                    published,
                    refusals,
                    panics,
                ) = request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("joined disposal releases every request producer"));
                let receipt = verified.expect("actual verification completed").unwrap();
                let polling =
                    polling.expect("the kernel retains the actual guard destructor panic");
                let (panic_target, panic_original) = polling
                    .downcast_ref::<(Option<u64>, Box<u64>)>()
                    .expect("retain the original coherent scope and panic payload");
                let claims = claims
                    .try_borrow_mut()
                    .expect("joined disposal releases every actual coherent permission loan");
                let claims = claims
                    .as_ref()
                    .expect("actual configured metadata survives");
                let originals = requests.borrow();
                let [expected] = originals.as_slice() else {
                    panic!("one original provider request survives");
                };
                let (first_announced, first_released) =
                    first_worker.expect("the first blocker joins");
                let (second_announced, second_released) =
                    second_worker.expect("the retry blocker joins");
                let custody = native
                    .custody()
                    .expect("the complete original actor remains owned");
                let first_full = first_full.take();
                let second_full = second_full.take();
                assert!(joined.is_ok() && disposed.is_ok());
                assert!(matches!(work, WorkOutcome::Interrupted { cleanup: Ok(()) }));
                assert!(external_close.is_none() && disposal.is_none());
                assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
                assert!(matches!(bound, Some(Ok(_))) && closed.is_ok() && session.is_closed());
                assert_eq!(root_report, Some(retirement));
                assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
                assert!(matches!(
                    &native,
                    ActorRetirement::Retained {
                        outcome: RetirementOutcome::OwnerCancelled { .. },
                        ..
                    }
                ));
                assert_eq!(origin.address().0, claims.actor_generation);
                assert!(
                    custody.capability_failures.is_empty()
                        && custody.additional_failures.is_empty()
                );
                assert!(
                    custody.retirement_failures.is_empty() && custody.child_failures.is_empty()
                );
                for original in &custody.user {
                    assert_eq!(original.from, caller);
                    assert!(matches!(original.message, CounterMessage::Increment));
                }
                let first_entered = first_entered.borrow();
                let second_entered = second_entered.borrow();
                let grants = grants.borrow();
                let cause = cause.borrow();
                assert!(first_entered.as_ref().is_some_and(Result::is_ok));
                assert!(second_entered.as_ref().is_some_and(Result::is_ok));
                assert!(first_announced.is_ok());
                assert!(matches!(
                    first_released,
                    Err(RecvTimeoutError::Disconnected)
                ));
                assert!(second_announced.is_ok());
                assert!(matches!(
                    second_released,
                    Err(RecvTimeoutError::Disconnected)
                ));
                assert_eq!(first_return_receipt.get(), Some(Ok(())));
                assert_eq!(
                    grants.as_slice(),
                    [
                        PermissionStage::Scope,
                        PermissionStage::Admission,
                        PermissionStage::Retry,
                    ]
                );
                let prefix = prefix.borrow();
                assert_eq!(prefix.len(), 2);
                assert!(prefix.iter().all(Result::is_ok));
                assert!(matches!(
                    first_full,
                    Some(Err(TrySendError::Full(CounterMessage::Increment)))
                ));
                let refill = refill.borrow();
                assert_eq!(refill.len(), 2);
                for original in refill.iter() {
                    assert!(matches!(
                        original,
                        Ok(()) | Err(TrySendError::Full(CounterMessage::Increment))
                    ));
                }
                assert!(matches!(
                    second_full,
                    Some(Err(TrySendError::Full(CounterMessage::Increment)))
                ));
                assert_eq!(*panic_target, Some(claims.actor_generation));
                assert_eq!(ptr::from_ref(panic_original.as_ref()), cause_allocation);
                assert_eq!(**panic_original, 401);
                assert!(cause.is_none());
                assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
                assert!(diagnostics.is_empty() && canonical.is_some());
                assert_eq!(original.0.as_ref(), expected.protected.as_ref());
                assert_eq!(original.1.as_ref(), expected.proof.as_ref());
                assert_eq!(
                    verified_allocations.get(),
                    Some((original.0.as_ptr(), original.1.as_ptr()))
                );
                assert_eq!(decoded_allocation.get(), Some(original.0.as_ptr()));
                assert_eq!(&recorded::projection(&receipt).bombay, claims);
                assert_eq!(recorded::projection(&receipt).kind, Purpose::Request);
                assert!(decoded.is_none());
                assert_eq!(constructions.get(), 1);
                assert!(encoded.is_none() && protected.is_none());
                assert!(published.is_empty() && wire.is_empty() && ordinary_tail.is_empty());
                assert_eq!(encodings.get(), 0);
                assert_eq!(protections.get(), 0);
                assert!(matches!(refusals, (None, None, None)));
                assert!(matches!(panics, (None, None, None, None, None, None)));
                let (constructor, client, admission, received, tail) =
                    native_reply.expect("the actual second Full remains outside its producer");
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the original actual reply client survives the guard unwind");
                };
                let Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(reply_to)))) =
                    admission
                else {
                    panic!("the second actual native Full survives permission release");
                };
                let recipient = client.recipient();
                assert!(constructor.is_none());
                assert_eq!(recipient, reply_to);
                assert!(received.is_none());
                assert_eq!(tail.len(), 1);
                assert!(tail[0].is_none());
            })
            .await
            .expect("the actual retry unwind, worker release and joined cleanup terminate");
        });
}
#[derive(Debug)]
struct RefusalDisposal<'scope> {
    source: VerificationError,
    cause: &'scope RefCell<Option<Box<u64>>>,
    disposals: &'scope Cell<usize>,
    invalid: &'scope Cell<usize>,
}

impl Drop for RefusalDisposal<'_> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        if matches!(&self.source, VerificationError::Invalid) {
            self.invalid.set(self.invalid.get() + 1);
        }
        // A one-shot original prevents a second destructor panic on unwind.
        let original = self.cause.take();
        if let Some(original) = original {
            resume_unwind(original);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one real refusal disposal keeps original panic, native Query, interrupted Work and all independent cleanup facts together"
)]
async fn discharged_refusal_panic_preserves_joined_node_finalization() {
    timeout(Duration::from_secs(10), async {
        let records = RefCell::new(Vec::<ProtectedMessage>::new());
        let claims = RefCell::new(None::<Metadata>);
        let joined = RefCell::new(None);
        let retirement = RefCell::new(None);
        let incoming = RefCell::new(None);
        let transmitted = RefCell::new(None);
        let original = Box::new(443_u64);
        let allocation = ptr::from_ref(original.as_ref()).cast::<()>();
        let cause = RefCell::new(Some(original));
        let disposals = Cell::new(0);
        let invalid = Cell::new(0);
        let verifications = Cell::new(0);
        let decodings = Cell::new(0);
        let constructors = Cell::new(0);
        let encodings = Cell::new(0);
        let protections = Cell::new(0);
        let binding = Binding {
            deployment: "refusal-disposal-unwind",
            caller: "caller",
            controller: "owner",
            actor: "ledger",
            host: "receiver",
            protocol: "ledger",
            target_assignment: "ledger",
            logical_session: 1,
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let current = |receipt: &recorded::Receipt<()>, runtime: &str, target: MailAddr| {
            let actual = claims.borrow();
            let actual = actual.as_ref().expect("actual binder-owned claims");
            current_scope(
                recorded::projection(receipt),
                actual,
                &(),
                Purpose::Request,
                deadline,
            )?;
            if actual.runtime_generation.as_ref() != runtime || actual.actor_generation != target.0
            {
                return Err(VerificationError::Invalid);
            }
            Ok(())
        };
        let export = Export::new(
            "bombay/refusal-disposal-unwind",
            ExportLimits {
                refusals: RefusalRetention::Discharge,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: NonZeroUsize::new(1).unwrap(),
                diagnostics: maximum,
            },
            limits(),
            |bytes: &[u8]| {
                decodings.set(decodings.get() + 1);
                let ReadRequest { operation: (), .. } =
                    serde_json::from_slice::<ReadRequest<()>>(bytes)?;
                Ok::<_, serde_json::Error>(())
            },
            submission(
                |()| {
                    Ok::<_, ()>(|| {
                        constructors.set(constructors.get() + 1);
                        LedgerCommand::Append(Box::from([29, 31]))
                    })
                },
                |_: &recorded::Receipt<()>,
                 _: &Attempt<Result<(), TrySendError<LedgerCommand>>>| {
                    encodings.set(encodings.get() + 1);
                    Err::<Box<[u8]>, ()>(())
                },
            ),
            |bytes: &[u8], proof: &[u8]| {
                verifications.set(verifications.get() + 1);
                let request = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                let verified = recorded::verify::<()>(&request, &records.borrow(), binding)
                    .into_inner()
                    .map_err(|source| RefusalDisposal {
                        source,
                        cause: &cause,
                        disposals: &disposals,
                        invalid: &invalid,
                    });
                ready(verified)
            },
            async |_: &[u8], _: &recorded::Receipt<()>| {
                protections.set(protections.get() + 1);
                Err::<Box<[u8]>, ()>(())
            },
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current,
            &current,
        );
        let node = Application::new(MailLedger {
            entries: Vec::new(),
        })
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual Ledger Node opens"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let (configured, configured_request) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let mut actual = metadata(
                ProviderProfile::Recorded,
                "ledger",
                runtime.clone(),
                runtime,
            );
            actual.deployment = binding.deployment.into();
            actual.actor_generation = application.actor_address(&target).unwrap().0;
            let protected = serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &(),
            })
            .unwrap()
            .into_boxed_slice();
            records.borrow_mut().push(ProtectedMessage {
                protected: protected.clone(),
                proof: Box::from([19_u8, 23]),
            });
            *claims.borrow_mut() = Some(actual);
            *joined.borrow_mut() = application.cleanup();
            *retirement.borrow_mut() = Some(application.lifecycle().retirement());
            configured
                .send(ProtectedMessage {
                    protected,
                    proof: Box::from([31_u8, 37]),
                })
                .unwrap_or_else(|_| panic!("Work retains the actual unregistered proof pair"));
            Ok::<_, Never>(target)
        });
        let mut operation = Box::pin(node.run_with(|_| async {
            let request = configured_request.await.unwrap();
            let receiving = session
                .get("bombay/refusal-disposal-unwind")
                .allowed_destination(Locality::SessionLocal)
                .payload(request.protected.to_vec())
                .attachment(request.proof.to_vec())
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20))
                .await
                .expect("the genuine native query starts");
            *incoming.borrow_mut() = Some(receiving);
            *transmitted.borrow_mut() = Some(request);
            pending::<()>().await;
        }));
        let caught = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| operation.as_mut().poll(context))) {
                Ok(Poll::Pending) => Poll::Pending,
                original => Poll::Ready(original),
            }
        })
        .await;
        let disposed = catch_unwind(AssertUnwindSafe(|| drop(operation)));
        let joined = joined.take().expect("actual cleanup observation").await;
        let external_close = if caught.is_err() {
            Some(node.close().await)
        } else {
            None
        };
        let finalized = node.into_parts();
        let retirement = retirement
            .take()
            .expect("independent actual root observation")
            .await;
        let incoming = incoming
            .take()
            .expect("Work entered its actual native Query");
        let mut replies = Vec::new();
        let native_eof = loop {
            match incoming.recv_async().await {
                Ok(original) => replies.push(original),
                Err(cause) => break cause,
            }
        };
        // Native callback EOF, whole Node operation and independent root join
        // have all settled before a containment or expected-result assertion.
        assert!(joined.is_ok() && disposed.is_ok() && session.is_closed());
        assert!(external_close.as_ref().is_none_or(Result::is_ok));
        assert!(
            finalized.is_ok(),
            "ordinary refusal Drop must stay inside Node's caught poll"
        );
        let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            finalized.unwrap_or_else(|_| panic!("the actual Node retained close and binding"));
        let Ok(Poll::Ready(Ok(outcome))) = caught else {
            panic!(
                "the Node must return its actual acquired outcome after retaining the Drop cause"
            );
        };
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (origin, native) = native.expect("original root joined");
        let notifications = notifications.expect("both original notifications");
        let (_, _, startup, withdrawal, diagnostics, rows) = export.into_parts();
        let polling = polling.expect("original refusal disposal panic remains Node-owned");
        let polling = polling
            .downcast_ref::<u64>()
            .expect("the same original u64 panic payload");
        let ActorRetirement::Retained {
            custody,
            outcome: RetirementOutcome::OwnerCancelled { .. },
        } = native
        else {
            panic!("the still-live Ledger is joined through owner cancellation");
        };
        let actual = claims.borrow();
        let actual = actual.as_ref().expect("actual certified claims remain");
        let remaining_cause = cause.borrow();
        let recorded = records.borrow();
        let transmitted = transmitted.borrow();
        let transmitted = transmitted
            .as_ref()
            .expect("original transmitted query inputs remain caller-owned");
        assert!(matches!(work, WorkOutcome::Interrupted { cleanup: Ok(()) }));
        assert_eq!(report, Some(retirement));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(matches!(bound, Some(Ok(_))) && closed.is_ok());
        assert!(external_close.is_none() && disposal.is_none());
        assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
        assert_eq!(ptr::from_ref(polling).cast::<()>(), allocation);
        assert_eq!(*polling, 443);
        assert_eq!(origin.address().0, actual.actor_generation);
        assert!(custody.behavior.entries.is_empty() && custody.user.is_empty());
        assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
        assert!(
            custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty()
        );
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty() && rows.is_empty());
        assert_eq!(verifications.get(), 1);
        assert_eq!(
            (
                decodings.get(),
                constructors.get(),
                encodings.get(),
                protections.get()
            ),
            (0, 0, 0, 0)
        );
        assert_eq!((disposals.get(), invalid.get()), (1, 1));
        assert!(remaining_cause.is_none());
        assert_eq!(recorded.len(), 1);
        assert_eq!(transmitted.protected, recorded[0].protected);
        assert_eq!(transmitted.proof.as_ref(), [31, 37]);
        assert!(replies.is_empty());
        // Keep the actual native receiving failure rather than manufacture EOF.
        drop((native_eof, incoming, replies));
    })
    .await
    .expect("watchdog supplies no disposal or cleanup credit");
}
