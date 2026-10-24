//! Binding completes before the original caller-local Work is invoked.
use super::super::counter::{CounterIncremented, CounterProcessed, CounterValue};
use super::super::custody::completed_actor;
use super::super::protocol::{
    self, CounterOperation, EndpointRole, ProtectedMessage, Purpose, ReadReplyRef, ReadRequest,
    configuration, limits, recorded,
};
use super::super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::application::work::invoke;
use crate::behavior::{Behavior, EstablishedRecipient, Never, User};
use crate::remote::{Export, ExportLimits, RefusalRetention, reply};
use crate::{
    ActorFailureAssessment, ActorRetirement, Application, ApplicationHandle, ApplicationScope,
    MailAddr, RetirementAssessment, WorkOutcome,
};
use behavior_actors::StopOnShutdown;
use core::future::{Future, Ready, poll_fn, ready};
use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

type CounterHandle = ApplicationHandle<
    <Counter as Behavior>::Protocol,
    <StopOnShutdown<Counter> as Behavior>::Event,
>;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one actual reply, binding gate and complete native retirement trace"
)]
async fn pending_binding_preserves_work_until_native_reply_is_acquired() {
    timeout(Duration::from_secs(10), async {
        let scope = ApplicationScope::new();
        let mut replies = scope.external::<CounterProcessed>().unwrap();
        let mut admission = None;
        let mut reply = None;
        let mut permission = None;
        let mut tail = None;
        let mut report = None;
        let mut stopped = None;
        let calls = Rc::new(Cell::new(0));
        let polls = Rc::new(Cell::new(0));
        let output = Box::new(179_u64);
        let original_output = ptr::from_ref(output.as_ref());
        let invoked = Rc::clone(&calls);
        let polled = Rc::clone(&polls);
        let stop = &mut stopped;
        let work = Box::new(move |application: CounterHandle| {
            invoked.set(invoked.get() + 1);
            let mut original = Some(output);
            poll_fn(move |_context| {
                polled.set(polled.get() + 1);
                *stop = Some(application.shutdown());
                Poll::Ready(original.take().expect("one original bare Work output"))
            })
        });
        let (bound, binding_ready) = oneshot::channel();
        let (release, permitted) = oneshot::channel();
        let acquired_admission = &mut admission;
        let acquired_reply = &mut reply;
        let acquired_permission = &mut permission;
        let acquired_tail = &mut tail;
        let acquired_report = &mut report;
        let reply_owner = &mut replies;
        let binding = async move |application: &CounterHandle| {
            let root = application.root().established_recipient();
            *acquired_admission = Some(
                reply_owner
                    .send(
                        &root,
                        CounterMessage::IncrementAndReply {
                            request_sequence: 173,
                            reply_to: reply_owner.recipient(),
                        },
                    )
                    .await,
            );
            *acquired_reply = Some(reply_owner.receive().await);
            let published = bound.send(());
            published.expect("actual binding publishes its acquired reply");
            *acquired_permission = Some(permitted.await);
            let lifecycle = application.lifecycle();
            Ok::<_, ()>(Some(async move {
                reply_owner.close_admission();
                *acquired_tail = Some(reply_owner.receive().await);
                *acquired_report = Some(lifecycle.retirement().await);
            }))
        };
        let (execution, (receiving, received_families)) = scope
            .with_root(Counter::new().stop_on_shutdown())
            .execute_with_entities_invocation(
                ApplicationWorkPresence::<_, Never>::Supplied((work, invoke)),
                Some(binding),
            )
            .unwrap_or_else(|_| panic!("the actual caller-local host is entered"));
        let observer = async {
            let acquired = binding_ready.await;
            let observations = (calls.get(), polls.get());
            let released = release.send(());
            (acquired, observations, released)
        };
        let ((), startup) = tokio::join!(execution, observer);
        let (work, root_report, native, notifications) = receiving.await;
        let work = work.into_supplied_with_entities();
        let () = received_families.await;
        let (origin, native) = native.expect("actual joined native result");
        let root = origin.address();
        let notifications = notifications.expect("both independent notification results");
        let (acquired, before_work, released) = startup;
        let root_report = root_report.expect("the joined root retains its actual report");
        assert_eq!(root_report.retirement(), RetirementAssessment::Established);
        assert_eq!(
            root_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        let WorkOutcome::Completed { output, cleanup } = work else {
            panic!("only successfully bound Work may complete");
        };
        assert!(cleanup.is_ok());
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(acquired.is_ok() && released.is_ok());
        assert_eq!(before_work, (0, 0));
        assert_eq!(calls.get(), 1);
        assert_eq!(polls.get(), 1);
        assert_eq!(ptr::from_ref(output.as_ref()), original_output);
        assert_eq!(*output, 179);
        assert!(matches!(admission, Some(Ok(()))));
        assert_eq!(
            reply,
            Some(Some(User::new(
                root,
                CounterIncremented {
                    request_sequence: 173
                }
            )))
        );
        assert!(matches!(permission, Some(Ok(()))));
        assert!(matches!(stopped, Some(Ok(()))));
        assert!(matches!(tail, Some(None)));
        let report = report.expect("the exact retirement observation was acquired");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let _counter = completed_actor(origin, native);
    })
    .await
    .expect("failure-only watchdog, never the binding ordering oracle");
}

#[test]
fn absent_binding_needs_no_invocation_value() {
    let application = Application::new(Counter::new().stop_on_shutdown());
    let attempted = application.execute_with_entities_invocation(
        ApplicationWorkPresence::<
            (Never, fn(Never, CounterHandle) -> Ready<Never>),
            (),
        >::Absent(()),
        None::<fn(&CounterHandle) -> Ready<Result<Option<Ready<()>>, ()>>>,
    );
    match attempted {
        Err((_application, ApplicationWorkPresence::Absent(()), _cause)) => {}
        _ => panic!("missing-host refusal preserves genuine absence without a callback"),
    }
}

struct BindingDisposal<Binding> {
    binding: Pin<Box<Binding>>,
    cause: Option<Arc<Vec<u8>>>,
    polls: Rc<Cell<usize>>,
    disposals: Rc<Cell<usize>>,
}

impl<Binding: Future> Future for BindingDisposal<Binding> {
    type Output = Binding::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let binding = self.get_mut();
        binding.polls.set(binding.polls.get() + 1);
        binding.binding.as_mut().poll(context)
    }
}

impl<Binding> Drop for BindingDisposal<Binding> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        if let Some(cause) = self.cause.take() {
            panic_any(cause);
        }
    }
}

enum BindingProgress {
    Ready,
    Pending,
}

#[tokio::test(flavor = "current_thread")]
async fn ready_binding_disposal_preserves_original_uninvoked_work() {
    binding_disposal_preserves_original_work(BindingProgress::Ready).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pending_binding_disposal_preserves_original_uninvoked_work() {
    binding_disposal_preserves_original_work(BindingProgress::Pending).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one actual producer disposal and complete original Work recovery trace"
)]
async fn binding_disposal_preserves_original_work(progress: BindingProgress) {
    timeout(Duration::from_secs(10), async {
        let calls = Rc::new(Cell::new(0));
        let invoked = Rc::clone(&calls);
        let output = Box::new(197_u64);
        let original_output = ptr::from_ref(output.as_ref());
        let work = Box::new(move |_: CounterHandle| {
            invoked.set(invoked.get() + 1);
            ready(output)
        });
        let original_work = ptr::from_ref(work.as_ref()).cast::<()>();
        let cause = Arc::new(vec![199_u8, 211]);
        let original_cause = Arc::as_ptr(&cause);
        let polls = Rc::new(Cell::new(0));
        let polled = Rc::clone(&polls);
        let disposals = Rc::new(Cell::new(0));
        let disposed = Rc::clone(&disposals);
        let (started, mut binding_started) = oneshot::channel();
        let (release, permitted) = oneshot::channel();
        let release = match &progress {
            BindingProgress::Ready => {
                release
                    .send(())
                    .expect("release actual binding before polling");
                None
            }
            BindingProgress::Pending => Some(release),
        };
        let mut application = None;
        let acquired = &mut application;
        let (execution, (receiving, received_families)) =
            Application::new(Counter::new().stop_on_shutdown())
                .execute_with_entities_invocation(
                    ApplicationWorkPresence::<_, Never>::Supplied((work, invoke)),
                    Some(move |application: &CounterHandle| {
                        *acquired = Some(application.clone());
                        BindingDisposal {
                            binding: Box::pin(async move {
                                started.send(()).expect("publish actual binding poll");
                                permitted
                                    .await
                                    .expect("Ready binding receives real permission");
                                Ok::<Option<Ready<()>>, ()>(None)
                            }),
                            cause: Some(cause),
                            polls: polled,
                            disposals: disposed,
                        }
                    }),
                )
                .unwrap_or_else(|_| panic!("actual host enters the one execution owner"));
        let mut execution = Box::pin(execution);
        let polled = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                Ok(Poll::Pending) if polls.get() == 0 => Poll::Pending,
                observed => Poll::Ready(observed),
            }
        })
        .await;
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(execution)));
        let started = binding_started.try_recv();
        let (work, root_report, native, notifications) = receiving.await;
        let work = work.into_supplied_with_entities();
        let () = received_families.await;
        let (origin, native) = native.expect("actual cancelled root is independently acquired");
        let notifications = notifications.expect("both actual notification receipts acquired");
        let application = application.expect("actual binding transferred the original root handle");
        let report = application.retirement().await;
        assert_eq!(root_report, Some(report));
        assert_eq!(origin.address(), application.root().address());
        assert!(matches!(native, ActorRetirement::OwnerCancelled { .. }));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(started.is_ok());
        assert!(polls.get() > 0);
        assert_eq!(disposals.get(), 1);
        assert_eq!(calls.get(), 0);
        let cause = match (progress, polled, dropped) {
            (BindingProgress::Ready, Err(cause), Ok(())) => cause,
            (BindingProgress::Pending, Ok(Poll::Pending), Err(cause)) => {
                let release = release.expect("unreleased gate was retained outside execution");
                assert!(release.is_closed());
                cause
            }
            _ => panic!("only actual Ready/poll disposal or Pending/owner disposal is accepted"),
        };
        let cause = cause
            .downcast_ref::<Arc<Vec<u8>>>()
            .expect("original typed cause");
        assert_eq!(Arc::as_ptr(cause), original_cause);
        assert_eq!(cause.as_slice(), [199, 211]);
        let WorkOutcome::NotInvoked {
            work,
            startup_error: None,
            cleanup: Ok(()),
        } = work
        else {
            panic!("binding disposal preserves original Work through actual joined cleanup");
        };
        assert_eq!(ptr::from_ref(work.as_ref()).cast::<()>(), original_work);
        let output = invoke(work, application).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(ptr::from_ref(output.as_ref()), original_output);
        assert_eq!(*output, 197);
    })
    .await
    .expect("failure-only watchdog; never the disposal or cancellation oracle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_binding_refusal_preserves_its_original_after_disposal() {
    public_binding_refusal(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_binding_refusal_preserves_its_original_after_disposal_panic() {
    public_binding_refusal(Some(Arc::new(vec![229, 233]))).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one actual cold Node retains the original binder result, unused Work, panic and every joined cleanup before extraction"
)]
async fn public_binding_refusal(disposal: Option<Arc<Vec<u8>>>) {
    timeout(Duration::from_secs(10), async {
        let expected_panic = disposal.as_ref().map(Arc::clone);
        let original_refusal = Box::<[u8]>::from([223, 227]);
        let refusal_allocation = original_refusal.as_ptr();
        let verified = Cell::new(0);
        let protected = Cell::new(0);
        let constructions = Cell::new(0);
        let registered: [ProtectedMessage; 0] = [];
        let binding = protocol::Binding {
            deployment: "binding-disposal", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "counter", logical_session: 257,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            verified.set(verified.get() + 1);
            recorded::verify::<CounterOperation>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &registered, binding,
            ).await
        };
        let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
            protected.set(protected.get() + 1);
            Err::<Box<[u8]>, _>(protocol::VerificationError::Invalid)
        };
        let current = |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &binding {
                Ok(())
            } else { Err(protocol::VerificationError::Invalid) }
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let export = Export::new(
            "bombay/binding-disposal/read",
            ExportLimits { refusals: RefusalRetention::Retain,
queued_queries: maximum, retained_queries: maximum,
                retained_requests: maximum, diagnostics: maximum },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation),
            reply(
                |CounterOperation::Read| Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| {
                    constructions.set(constructions.get() + 1);
                    CounterMessage::Read(reply_to)
                }),
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef {
                        bombay: &scope.bombay, kind: Purpose::ReadReply,
                        operation: &scope.operation, value: &original.message,
                    }).map(Vec::into_boxed_slice)
                },
            ),
            &verify, &protect,
            |left: &recorded::Receipt<CounterOperation>, right: &recorded::Receipt<CounterOperation>| {
                recorded::identity(left) == recorded::identity(right)
            },
            &current, &current,
        );
        let calls = Rc::new(Cell::new(0));
        let invoked = Rc::clone(&calls);
        let output = Box::new(239_u64);
        let output_allocation = ptr::from_ref(output.as_ref());
        let work = Box::new(move |application: CounterHandle| {
            invoked.set(invoked.get() + 1);
            async move {
                let shutdown = application.shutdown();
                let retired = application.retirement().await;
                (output, shutdown, retired)
            }
        });
        let work_allocation = ptr::from_ref(work.as_ref()).cast::<()>();
        let polls = Rc::new(Cell::new(0));
        let disposals = Rc::new(Cell::new(0));
        let polled = Rc::clone(&polls);
        let disposed = Rc::clone(&disposals);
        let application = &RefCell::new(None);
        let acquired = application;
        let refusal = &original_refusal;
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver)).await
            .unwrap_or_else(|_| panic!("the actual cold Counter Node opens"));
        let connection = node.session.clone();
        let initially_closed = connection.is_closed();
        let mut node = node.exports(export, move |application: &CounterHandle| {
            *acquired.borrow_mut() = Some(application.clone());
            BindingDisposal {
                binding: Box::pin(ready(Err::<EstablishedRecipient<<Counter as Behavior>::Protocol>, _>(refusal))),
                cause: disposal, polls: polled, disposals: disposed,
            }
        });
        let outcome = node.run_with(work).await
            .unwrap_or_else(|_| panic!("the actual cold declaration accepts execution"));
        let (work, root_report, native, notifications, ()) = outcome.into_parts();
        let application = application.borrow_mut().take()
            .expect("the actual binder acquired its root handle");
        let retired = application.retirement().await;
        let (origin, native) = native.expect("the original native root is independently received");
        let notifications = notifications.expect("both original notifications are received");
        let closed = node.closed.as_ref().expect("Node acquires native close before returning");
        assert!(!initially_closed && connection.is_closed() && closed.is_ok());
        assert_eq!(root_report, Some(retired));
        assert_eq!(origin.address(), application.root().address());
        assert!(matches!(&native, ActorRetirement::OwnerCancelled { .. }));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert_eq!(polls.get(), 1);
        assert_eq!(disposals.get(), 1);
        assert_eq!(calls.get(), 0);
        assert_eq!((verified.get(), protected.get(), constructions.get()), (0, 0, 0));
        assert!(node.disposal_panic.is_none());
        match (&expected_panic, &node.poll_panic) {
            (Some(expected), Some(original)) => {
                let original = original.downcast_ref::<Arc<Vec<u8>>>()
                    .expect("the actual original binder disposal panic remains typed");
                assert!(Arc::ptr_eq(original, expected));
                assert_eq!(original.as_slice(), [229, 233]);
            }
            (None, None) => {}
            _ => panic!("the actual binder disposal determines the original poll panic"),
        }
        let WorkOutcome::NotInvoked { work, startup_error: None, cleanup: Ok(()) } = work else {
            panic!("binding refusal returns the original unused Work after actual cleanup");
        };
        assert_eq!(ptr::from_ref(work.as_ref()).cast::<()>(), work_allocation);
        let recovered = invoke(work, application).await;
        assert_eq!(calls.get(), 1);
        assert_eq!(ptr::from_ref(recovered.0.as_ref()), output_allocation);
        assert_eq!(*recovered.0, 239);
        assert_eq!(recovered.2, retired);
        let (export, bound, closed, polling, disposal) = match node.into_parts() {
            Ok(originals) => originals,
            Err(node) => {
                let closed = node.closed.as_ref().expect("the refused extraction still owns close");
                assert!(closed.is_ok() && connection.is_closed());
                assert!(node.disposal_panic.is_none());
                assert_eq!(node.poll_panic.is_some(), expected_panic.is_some());
                panic!("the actual Ready binding refusal must survive producer disposal for public extraction");
            }
        };
        let Err(refusal) = bound.expect("the actual binder returned its original result") else {
            panic!("the actual binder returned the original borrowed refusal");
        };
        assert_eq!(refusal.as_ptr(), refusal_allocation);
        assert!(ptr::eq(refusal, ptr::from_ref(&original_refusal)));
        assert_eq!(refusal.as_ref(), [223, 227]);
        assert!(closed.is_ok() && disposal.is_none());
        assert_eq!(polling.is_some(), expected_panic.is_some());
        let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) = export.into_parts();
        assert!(startup.is_none() && withdrawal.is_none());
        assert!(diagnostics.is_empty() && requests.is_empty());
    }).await.expect("failure-only watchdog; actual binding and cleanup supply the oracles");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one genuine absent Work trace retains actual successful binding, its producer panic and every independent cleanup before extraction"
)]
async fn public_binding_target_survives_disposal_with_genuine_work_absence() {
    timeout(Duration::from_secs(10), async {
        let verified = Cell::new(0);
        let protected = Cell::new(0);
        let constructions = Cell::new(0);
        let registered: [ProtectedMessage; 0] = [];
        let binding = protocol::Binding {
            deployment: "binding-disposal", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "counter", logical_session: 263,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            verified.set(verified.get() + 1);
            recorded::verify::<CounterOperation>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &registered, binding,
            ).await
        };
        let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
            protected.set(protected.get() + 1);
            Err::<Box<[u8]>, _>(protocol::VerificationError::Invalid)
        };
        let current = |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            if scope.kind == Purpose::Request
                && scope.bombay.runtime_generation.as_ref() == runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &binding {
                Ok(())
            } else { Err(protocol::VerificationError::Invalid) }
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let export = Export::new(
            "bombay/binding-disposal/absent-read",
            ExportLimits { refusals: RefusalRetention::Retain,
queued_queries: maximum, retained_queries: maximum,
                retained_requests: maximum, diagnostics: maximum },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation),
            reply(
                |CounterOperation::Read| Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| {
                    constructions.set(constructions.get() + 1);
                    CounterMessage::Read(reply_to)
                }),
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef {
                        bombay: &scope.bombay, kind: Purpose::ReadReply,
                        operation: &scope.operation, value: &original.message,
                    }).map(Vec::into_boxed_slice)
                },
            ),
            &verify, &protect,
            |left: &recorded::Receipt<CounterOperation>, right: &recorded::Receipt<CounterOperation>| {
                recorded::identity(left) == recorded::identity(right)
            },
            &current, &current,
        );
        let cause = Arc::new(vec![241_u8, 251]);
        let expected_cause = Arc::clone(&cause);
        let polls = Rc::new(Cell::new(0));
        let disposals = Rc::new(Cell::new(0));
        let polled = Rc::clone(&polls);
        let disposed = Rc::clone(&disposals);
        let application = &RefCell::new(None);
        let acquired = application;
        let expected_target = &RefCell::new(None);
        let observed_target = expected_target;
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver)).await
            .unwrap_or_else(|_| panic!("the actual absent Work Counter Node opens"));
        let connection = node.session.clone();
        let initially_closed = connection.is_closed();
        let mut node = node.exports(export, move |application: &CounterHandle| {
            let target = application.root().established_recipient();
            *observed_target.borrow_mut() = Some(target.clone());
            *acquired.borrow_mut() = Some(application.clone());
            BindingDisposal {
                binding: Box::pin(ready(Ok::<_, Never>(target))),
                cause: Some(cause), polls: polled, disposals: disposed,
            }
        });
        let outcome = node.run().await
            .unwrap_or_else(|_| panic!("actual Node issues execution without supplied Work"));
        let (work, root_report, native, notifications, ()) = outcome.into_parts();
        let application = application.borrow_mut().take()
            .expect("the actual binder acquired its root handle before Ready");
        let retired = application.retirement().await;
        let (origin, native) = native.expect("the original native root remains independent");
        let notifications = notifications.expect("both original notifications are acquired");
        let closed = node.closed.as_ref().expect("actual Node acquired native close");
        let panic = node.poll_panic.as_ref()
            .expect("the actual successful binder producer panics during disposal");
        let panic = panic.downcast_ref::<Arc<Vec<u8>>>()
            .expect("the original binder disposal payload retains its type");
        assert!(!initially_closed && connection.is_closed() && closed.is_ok());
        assert_eq!(root_report, Some(retired));
        assert_eq!(origin.address(), application.root().address());
        assert!(matches!(&native, ActorRetirement::OwnerCancelled { .. }));
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(matches!(&work, WorkOutcome::NotInvoked {
            work: None, startup_error: None, cleanup: Ok(())
        }));
        assert_eq!(polls.get(), 1);
        assert_eq!(disposals.get(), 1);
        assert!(Arc::ptr_eq(panic, &expected_cause));
        assert_eq!(panic.as_slice(), [241, 251]);
        assert!(node.disposal_panic.is_none());
        assert_eq!((verified.get(), protected.get(), constructions.get()), (0, 0, 0));
        let (export, bound, closed, polling, disposal) = match node.into_parts() {
            Ok(originals) => originals,
            Err(node) => {
                let closed = node.closed.as_ref().expect("refused extraction still owns native close");
                assert!(closed.is_ok() && connection.is_closed());
                assert!(node.poll_panic.is_some() && node.disposal_panic.is_none());
                panic!("the actual Ready binding target must survive producer disposal for public extraction");
            }
        };
        let target = bound.expect("the actual successful binding result remains")
            .expect("the binder returned its actual root target");
        let expected_target = expected_target.borrow();
        let expected_target = expected_target.as_ref().expect("actual target was observed before Ready");
        let root_target = application.root().established_recipient();
        assert_eq!(&target, expected_target);
        assert_eq!(target, root_target);
        assert!(closed.is_ok() && polling.is_some() && disposal.is_none());
        let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) = export.into_parts();
        assert!(startup.is_none() && withdrawal.is_none());
        assert!(diagnostics.is_empty() && requests.is_empty());
    }).await.expect("failure-only watchdog; actual absent Work and binding originals supply the oracles");
}
