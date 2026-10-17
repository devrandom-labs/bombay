//! Binding completes before the original caller-local Work is invoked.
use super::super::counter::{CounterIncremented, CounterProcessed};
use super::super::custody::completed_actor;
use super::super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::application::work::invoke;
use crate::behavior::{Behavior, Never, User};
use crate::{
    ActorFailureAssessment, ActorRetirement, Application, ApplicationHandle, ApplicationScope,
    RetirementAssessment, WorkOutcome,
};
use behavior_actors::StopOnShutdown;
use core::future::{Future, Ready, poll_fn, ready};
use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::Cell;
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
                *stop = Some(application.request_shutdown());
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
        let (work, native, notifications) = receiving.await;
        let work = work.into_supplied_with_entities();
        let () = received_families.await;
        let (origin, native) = native.expect("actual joined native result");
        let root = origin.address();
        let notifications = notifications.expect("both independent notification results");
        let (acquired, before_work, released) = startup;
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
        let (work, native, notifications) = receiving.await;
        let work = work.into_supplied_with_entities();
        let () = received_families.await;
        let (origin, native) = native.expect("actual cancelled root is independently acquired");
        let notifications = notifications.expect("both actual notification receipts acquired");
        let application = application.expect("actual binding transferred the original root handle");
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
