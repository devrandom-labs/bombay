//! Existing invocation and independent caller-local work; no Node/service API selected.
//! Compare returning bare `WorkFuture` against wrapping it in an async future.
use super::counter::{CounterIncremented, CounterProcessed};
use super::custody::completed_actor;
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::behavior::{Never, User};
use crate::remote::verify;
use crate::{
    ActorFailureAssessment, ActorNotificationReceipts, Application, ApplicationHandle,
    RetirementAssessment, WorkOutcome,
};
use core::future::{Future, Ready, poll_fn};
use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

struct ReplyWork {
    reply: oneshot::Receiver<Rc<Vec<u8>>>,
    panic: Option<Arc<Vec<u8>>>,
}

impl Future for ReplyWork {
    type Output = Rc<Vec<u8>>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().reply)
            .poll(context)
            .map(|reply| reply.expect("the independently driven companion transfers its result"))
    }
}

impl Drop for ReplyWork {
    fn drop(&mut self) {
        if let Some(original) = self.panic.take() {
            panic_any(original);
        }
    }
}

#[test]
#[expect(clippy::too_many_lines, reason = "one complete cleanup trace")]
fn projected_handoff_keeps_ready_work_and_provider_result() {
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let request = Rc::new(vec![31_u8, 37]);
                let provider_calls = RefCell::new(Vec::new());
                let provider_receipt = Rc::new(vec![41_u8, 43]);
                let original_provider = Rc::as_ptr(&provider_receipt);
                let original_request = Rc::as_ptr(&request);
                let output = Rc::new(vec![47_u8, 53]);
                let original_output = Rc::as_ptr(&output);
                let panic = Arc::new(vec![59_u8, 61]);
                let original_panic = Arc::as_ptr(&panic);
                let (handoff, granted) = oneshot::channel();
                let (publication, reply) = oneshot::channel();
                let mut transferred = None;
                let mut retained = None;
                let work = move |_| ReplyWork {
                    reply,
                    panic: Some(panic),
                };
                let (execution, (receiving, received_families)) =
                    Application::new(Counter::new().stop_on_shutdown())
                        .execute_with_entities_invocation(
                            ApplicationWorkPresence::Supplied(work),
                            |work: ApplicationWorkPresence<_, Never>, application| {
                                let interface = application
                                    .interface(application.root().established_recipient());
                                let lifecycle = application.lifecycle();
                                transferred = Some(handoff.send((interface, lifecycle)));
                                Ok((
                                    ApplicationWorkPresence::Supplied(work.into_supplied()(
                                        application,
                                    )),
                                    None::<Ready<()>>,
                                ))
                            },
                        )
                        .unwrap_or_else(|_| panic!("the actual host is entered"));
                let companion = async {
                    let (interface, lifecycle) =
                        granted.await.expect("actual committed root handoff");
                    verify(&request, &mut retained, |borrowed| async {
                        provider_calls.borrow_mut().push(Rc::as_ptr(borrowed));
                        Ok::<_, Never>(provider_receipt)
                    })
                    .await;
                    let target = interface.api().clone();
                    let mut client = interface
                        .client_to::<_, CounterProcessed>(target)
                        .unwrap_or_else(|_| panic!("the actual reply client allocates"));
                    let admitted = client
                        .send(CounterMessage::IncrementAndReply {
                            request_sequence: 67,
                            reply_to: client.recipient(),
                        })
                        .await;
                    let processed = client.receive().await;
                    let stopped = lifecycle.request_shutdown();
                    let report = lifecycle.retirement().await;
                    client.close_admission();
                    let unread = client.receive().await;
                    let published = publication.send(output);
                    (admitted, processed, stopped, report, unread, published)
                };
                let kernel = async {
                    let mut execution = Box::pin(execution);
                    let fault = poll_fn(|context| {
                        match catch_unwind(AssertUnwindSafe(|| execution.as_mut().poll(context))) {
                            Ok(Poll::Pending) => Poll::Pending,
                            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                            Err(original) => Poll::Ready(Err(original)),
                        }
                    })
                    .await;
                    drop(execution);
                    fault
                };
                let (fault, companion) = tokio::join!(kernel, companion);
                let (work, native, notifications) = receiving.await;
                let work = work.into_supplied_with_entities();
                let () = received_families.await;
                move || {
                    let (admitted, processed, stopped, report, unread, published) = companion;
                    let transfer = transferred.expect("Invoke attempted its actual handoff");
                    assert!(transfer.is_ok());
                    let receipt = retained.expect("provider returned").unwrap();
                    assert_eq!(Rc::as_ptr(&receipt), original_provider);
                    assert_eq!(receipt.as_slice(), [41, 43]);
                    assert_eq!(*provider_calls.borrow(), [original_request]);
                    admitted.expect("actual command admission");
                    stopped.expect("exact root shutdown request");
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                    assert!(unread.is_none());
                    assert!(published.is_ok());
                    let (origin, native) = native.expect("original native retirement");
                    let root = origin.address();
                    let _counter = completed_actor(origin, native);
                    assert_eq!(
                        processed,
                        Some(User::new(
                            root,
                            CounterIncremented {
                                request_sequence: 67
                            }
                        ))
                    );
                    let Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }) = notifications
                    else {
                        panic!("both real notifications survive independently");
                    };
                    let original = fault.expect_err("the bare WorkFuture destructor really panics");
                    let payload = original
                        .downcast_ref::<Arc<Vec<u8>>>()
                        .expect("original typed panic");
                    assert_eq!(Arc::as_ptr(payload), original_panic);
                    assert_eq!(payload.as_slice(), [59, 61]);
                    let WorkOutcome::Completed {
                        output,
                        cleanup: Ok(()),
                    } = work
                    else {
                        panic!("the bare Ready result must survive producer disposal");
                    };
                    assert_eq!(Rc::as_ptr(&output), original_output);
                    assert_eq!(output.as_slice(), [47, 53]);
                }
            })
            .await
        })
        .expect("the complete invocation, companion and retirement trace stays bounded")
    };
    observations();
}

#[test]
fn missing_host_returns_original_work_before_handoff() {
    let calls = Rc::new(Cell::new(0));
    let handoffs = Rc::new(Cell::new(0));
    let work_calls = Rc::clone(&calls);
    let invoke_calls = Rc::clone(&handoffs);
    let original = Box::new(vec![71_u8, 73]);
    let original_pointer = ptr::from_ref(original.as_ref());
    let (handoff, mut granted) = oneshot::channel();
    let mut transferred = None;
    let transfer = &mut transferred;
    let work = move |application: ApplicationHandle<_, _>| {
        work_calls.set(work_calls.get() + 1);
        async move {
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (original, stopped, report)
        }
    };
    let Err((application, work, cause)) = Application::new(Counter::new().stop_on_shutdown())
        .execute_with_entities_invocation(
            ApplicationWorkPresence::Supplied(work),
            move |work: ApplicationWorkPresence<_, Never>, application| {
                invoke_calls.set(invoke_calls.get() + 1);
                let interface = application.interface(application.root().established_recipient());
                let lifecycle = application.lifecycle();
                *transfer = Some(handoff.send((interface, lifecycle)));
                Ok((
                    ApplicationWorkPresence::Supplied(work.into_supplied()(application)),
                    None::<Ready<()>>,
                ))
            },
        )
    else {
        panic!("no host is entered before the actual invocation");
    };
    let before = (calls.get(), handoffs.get());
    let received = granted.try_recv();
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let (execution, receiving) = application
                    .execute_with_entities(work.into_supplied())
                    .unwrap_or_else(|_| panic!("the same originals enter the real host"));
                execution.await;
                let outcome = receiving.await;
                move || {
                    let (work, native, notifications, ()) = outcome.into_parts();
                    assert!(cause.is_missing_context());
                    assert_eq!(before, (0, 0));
                    assert!(matches!(&received, Err(TryRecvError::Closed)));
                    assert_eq!(handoffs.get(), 0);
                    assert!(transferred.is_none());
                    assert_eq!(calls.get(), 1);
                    let (origin, native) = native.expect("the recovered root really retires");
                    let _counter = completed_actor(origin, native);
                    let Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }) = notifications
                    else {
                        panic!("both actual notification receipts survive");
                    };
                    let WorkOutcome::Completed {
                        output: (original, stopped, report),
                        cleanup: Ok(()),
                    } = work
                    else {
                        panic!("the recovered original callable runs exactly once");
                    };
                    stopped.expect("the recovered Work requests actual shutdown");
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                    assert_eq!(ptr::from_ref(original.as_ref()), original_pointer);
                    assert_eq!(original.as_slice(), [71, 73]);
                }
            })
            .await
        })
        .expect("the complete invocation, companion and retirement trace stays bounded")
    };
    observations();
}

mod invocation;
