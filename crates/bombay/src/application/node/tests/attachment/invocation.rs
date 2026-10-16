use super::super::counter::{CounterIncremented, CounterProcessed};
use super::super::custody::completed_actor;
use super::super::{Counter, CounterMessage};
use super::ReplyWork;
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::behavior::{Never, User};
use crate::remote::verify;
use crate::{ActorNotificationReceipts, Application, WorkOutcome};
use core::future::{Future, poll_fn};
use core::task::Poll;
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one original work result and native export trace across producer disposal"
)]
fn internal_exports_keep_originals_when_ready_work_disposal_panics() {
    let observations = {
        let host = Builder::new_current_thread().enable_all().build().unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let request = Rc::new(vec![97_u8, 101]);
                let original_request = Rc::as_ptr(&request);
                let provider_calls = RefCell::new(Vec::new());
                let provider_receipt = Rc::new(vec![103_u8, 107]);
                let original_provider = Rc::as_ptr(&provider_receipt);
                let output = Rc::new(vec![109_u8, 113]);
                let original_output = Rc::as_ptr(&output);
                let panic = Arc::new(vec![127_u8, 131]);
                let original_panic = Arc::as_ptr(&panic);
                let (publication, reply) = oneshot::channel();
                let mut retained_provider = None;
                let mut retained_native = None;
                let mut retained_publication = None;
                let retained = &mut retained_provider;
                let calls = &provider_calls;
                let request = &request;
                let native = &mut retained_native;
                let published = &mut retained_publication;
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
                                let exporting = async move {
                                    verify(request, retained, |borrowed| async {
                                        calls.borrow_mut().push(Rc::as_ptr(borrowed));
                                        Ok::<_, Never>(provider_receipt)
                                    })
                                    .await;
                                    let target = interface.api().clone();
                                    let mut client = interface
                                        .client_to::<_, CounterProcessed>(target)
                                        .unwrap_or_else(|_| {
                                            panic!("the actual correlated reply client allocates")
                                        });
                                    let admitted = client
                                        .send(CounterMessage::IncrementAndReply {
                                            request_sequence: 137,
                                            reply_to: client.recipient(),
                                        })
                                        .await;
                                    let processed = client.receive().await;
                                    let stopped = lifecycle.request_shutdown();
                                    let report = lifecycle.retirement().await;
                                    client.close_admission();
                                    let tail = client.receive().await;
                                    *native = Some((admitted, processed, stopped, report, tail));
                                    *published = Some(publication.send(output));
                                };
                                (
                                    ApplicationWorkPresence::Supplied(work.into_supplied()(
                                        application,
                                    )),
                                    Some(exporting),
                                )
                            },
                        )
                        .unwrap_or_else(|_| panic!("the actual host is entered"));
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
                let (work, native, notifications) = receiving.await;
                let work = work.into_supplied_with_entities();
                let () = received_families.await;
                move || {
                    let (admitted, processed, stopped, _report, tail) = retained_native
                        .expect("actual native outcomes stored before Work was made Ready");
                    admitted.expect("actual correlated command admitted");
                    stopped.expect("actual lifecycle shutdown request");
                    let (origin, native) = native.expect("original native actor retirement");
                    let root = origin.address();
                    let _counter = completed_actor(origin, native);
                    let Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }) = notifications
                    else {
                        panic!("both native notification originals survive");
                    };
                    let WorkOutcome::Completed {
                        output,
                        cleanup: Ok(()),
                    } = work
                    else {
                        panic!(
                            "the exact bare Ready work output must survive its producer Drop panic"
                        );
                    };
                    assert_eq!(Rc::as_ptr(&output), original_output);
                    assert_eq!(output.as_slice(), [109, 113]);
                    let fault = fault.expect_err("actual bare producer disposal panics");
                    let panic = fault
                        .downcast_ref::<Arc<Vec<u8>>>()
                        .expect("original typed panic");
                    assert_eq!(Arc::as_ptr(panic), original_panic);
                    assert_eq!(panic.as_slice(), [127, 131]);
                    let receipt = retained_provider
                        .expect("original provider result retained outside exporting")
                        .unwrap();
                    assert_eq!(Rc::as_ptr(&receipt), original_provider);
                    assert_eq!(receipt.as_slice(), [103, 107]);
                    assert_eq!(*provider_calls.borrow(), [original_request]);
                    assert_eq!(
                        processed,
                        Some(User::new(
                            root,
                            CounterIncremented {
                                request_sequence: 137
                            }
                        ))
                    );
                    assert!(tail.is_none());
                    assert!(
                        retained_publication
                            .expect("original Work publication disposition retained")
                            .is_ok()
                    );
                }
            })
            .await
        })
        .expect("the actual work, export and cleanup trace stays bounded")
    };
    observations();
}
