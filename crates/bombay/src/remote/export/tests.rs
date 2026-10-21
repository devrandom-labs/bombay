use super::Attempt;
use super::declaration::{Commands, Delivery, Reception, reply};
use crate::actors::ActorExt;
use crate::{
    ActorFailureAssessment, Application, Completion, MailAddr, MailboxCapacity,
    RetirementAssessment, TrySendError,
};
use behavior::{Never, User};
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;
use std::cell::Cell;
use std::sync::mpsc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;
use counter::{Counter, CounterMessage, CounterValue};

#[path = "comparison.rs"]
mod comparison;

#[test]
fn full_read_reuses_its_original_reply_endpoint_once() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(40), async {
                let constructors = Cell::new(0);
                let commitments = Cell::new(0);
                let declarations = reply(
                    |()| {
                        Ok::<_, ()>(|reply_to| {
                            constructors.set(constructors.get() + 1);
                            CounterMessage::Read(reply_to)
                        })
                    },
                    |_: &(), original: &User<MailAddr, u64>| {
                        Ok::<_, Never>(original.message.to_ne_bytes().to_vec().into_boxed_slice())
                    },
                );
                let canonical_count = &commitments;
                let outcome = Application::new(Counter::new().stop_on_shutdown())
                    .mailbox_capacity(MailboxCapacity::new(2).unwrap())
                    .run_with(|application| async move {
                        let interface =
                            application.interface(application.root().established_recipient());
                        let mut ordinary = application
                            .client::<CounterValue>()
                            .unwrap_or_else(|_| panic!("the independent Counter client allocates"));
                        let mut native = Commands::<(), (), Counter>::select(&declarations, ())
                            .unwrap_or_else(|()| panic!("one actual Read constructor is selected"));
                        let prepared = Commands::<(), (), Counter>::prepare(
                            &declarations,
                            &mut native,
                            &interface,
                        );
                        prepared
                            .expect("Reply owns endpoint preparation")
                            .expect("the real reply endpoint allocates");
                        let (release_worker, released) = mpsc::channel::<()>();
                        let (announced, worker_entered) = oneshot::channel();
                        let worker = tokio::spawn(async move {
                            let announced = announced.send(());
                            let released = released.recv();
                            (announced, released)
                        });
                        let entered = worker_entered.await;
                        let prefix = (0..2)
                            .map(|_| ordinary.try_send(CounterMessage::Increment))
                            .collect::<Vec<_>>();
                        let independent = ordinary.try_send(CounterMessage::Increment);
                        let control = Some(application.lifecycle());
                        let canonical = || canonical_count.set(canonical_count.get() + 1);
                        let first =
                            Delivery::attempt(&declarations, &mut native, &control, canonical);
                        let retained = match &native.2 {
                            Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(
                                reply_to,
                            )))) => Some(reply_to.clone()),
                            _ => None,
                        };
                        let repeated_full =
                            Delivery::attempt(&declarations, &mut native, &control, canonical);
                        let still_retained = match &native.2 {
                            Attempt::Returned(Err(TrySendError::Full(CounterMessage::Read(
                                reply_to,
                            )))) => Some(reply_to.clone()),
                            _ => None,
                        };
                        drop(release_worker);
                        let worker = worker.await;
                        let fence = ordinary
                            .send(CounterMessage::Read(ordinary.recipient()))
                            .await;
                        let before = if fence.is_ok() {
                            ordinary.receive().await
                        } else {
                            None
                        };
                        let retried =
                            Delivery::attempt(&declarations, &mut native, &control, canonical);
                        let repeated =
                            Delivery::attempt(&declarations, &mut native, &control, canonical);
                        // Keep reply admission open until every preceding Read
                        // has completed its reply effect on the actual actor.
                        let fence_after = ordinary
                            .send(CounterMessage::Read(ordinary.recipient()))
                            .await;
                        let after = if fence_after.is_ok() {
                            ordinary.receive().await
                        } else {
                            None
                        };
                        if let Some(receiving) = Delivery::receive(
                            &declarations,
                            &mut native,
                            &control,
                            application.retirement(),
                        ) {
                            receiving.await;
                        }
                        if let Some(draining) = declarations.drain(&mut native) {
                            draining.await;
                        }
                        let shutdown = application.shutdown();
                        (
                            entered,
                            worker,
                            prefix,
                            independent,
                            first,
                            retained,
                            repeated_full,
                            still_retained,
                            fence,
                            before,
                            retried,
                            repeated,
                            fence_after,
                            after,
                            native,
                            shutdown,
                        )
                    })
                    .await
                    .unwrap_or_else(|_| panic!("the actual Counter Application starts"));
                let (work, root_report, root, notifications, ()) = outcome.into_parts();
                let (output, cleanup) = work
                    .try_into_completed()
                    .unwrap_or_else(|_| panic!("the original caller output survives cleanup"));
                cleanup.expect("the actual Application cleanup joins");
                let (origin, root) = root.expect("the original Counter retirement survives");
                let notifications = notifications.expect("both native notifications survive");
                let root_report = root_report.expect("the runtime retains its actual root report");
                assert_eq!(root_report.retirement(), RetirementAssessment::Established);
                assert_eq!(
                    root_report.failures(),
                    ActorFailureAssessment::NoFailuresFound
                );
                let (
                    entered,
                    worker,
                    prefix,
                    independent,
                    first,
                    retained,
                    repeated_full,
                    still_retained,
                    fence,
                    before,
                    retried,
                    repeated,
                    fence_after,
                    after,
                    (constructor, client, admission, received, tail),
                    shutdown,
                ) = output;
                assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
                assert!(root.custody().is_some() && shutdown.is_ok() && entered.is_ok());
                assert_eq!(root.completion(), Some(&Completion::Stopped));
                let (announced, released) = worker.expect("the blocked worker joins");
                assert!(announced.is_ok() && released.is_err());
                assert_eq!(prefix.len(), 2);
                assert!(prefix.iter().all(Result::is_ok));
                assert!(matches!(
                    independent,
                    Err(TrySendError::Full(CounterMessage::Increment))
                ));
                assert!(first.is_ok() && repeated_full.is_ok() && fence.is_ok());
                let before = before.as_ref().map(|reply| (reply.from, reply.message));
                assert_eq!(before, Some((origin.address(), 2)));
                assert!(retained.is_some());
                assert_eq!(retained, still_retained);
                assert!(retried.is_ok() && repeated.is_ok());
                assert!(fence_after.is_ok());
                let after = after.as_ref().map(|reply| (reply.from, reply.message));
                assert_eq!(after, Some((origin.address(), 2)));
                assert!(
                    matches!(admission, Attempt::Returned(Ok(()))),
                    "native Full must be retried after capacity returns"
                );
                assert!(constructor.is_none());
                assert_eq!(constructors.get(), 1);
                assert_eq!(commitments.get(), 1);
                let Attempt::Returned(Ok(client)) = client else {
                    panic!("the one original reply client survives");
                };
                let endpoint = client.recipient();
                assert_eq!(retained.as_ref(), Some(&endpoint));
                let received = received
                    .as_ref()
                    .and_then(Option::as_ref)
                    .map(|reply| (reply.from, reply.message));
                assert_eq!(received, Some((origin.address(), 2)));
                assert_eq!(tail.len(), 1);
                assert!(matches!(tail[0], None));
            })
            .await
            .expect("the complete Counter admission and cleanup trace terminates");
        });
}

#[tokio::test(flavor = "current_thread")]
async fn acquired_first_eof_prevents_another_tail_receive() {
    let outcome = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let declarations = reply(
                |()| Ok::<_, ()>(CounterMessage::Read),
                |_: &(), original: &User<MailAddr, u64>| {
                    Ok::<_, Never>(original.message.to_ne_bytes().to_vec().into_boxed_slice())
                },
            );
            let interface = application.interface(application.root().established_recipient());
            let mut native = Commands::<(), (), Counter>::select(&declarations, ())
                .unwrap_or_else(|()| panic!("the actual Read constructor is selected"));
            let prepared =
                Commands::<(), (), Counter>::prepare(&declarations, &mut native, &interface);
            prepared
                .expect("reply preparation applies")
                .expect("the real endpoint allocates");
            if let Attempt::Returned(Ok(client)) = &native.1 {
                client.close_admission();
            }
            let control = Some(application.lifecycle());
            let mut commitment = None;
            let admitted = Delivery::attempt(&declarations, &mut native, &control, || {
                commitment = Some(())
            });
            if let Some(receiving) = Delivery::receive(
                &declarations,
                &mut native,
                &control,
                application.retirement(),
            ) {
                receiving.await;
            }
            let observed = if let Some(draining) = declarations.drain(&mut native) {
                let mut draining = pin!(draining);
                Some(poll_fn(|cx| Poll::Ready(draining.as_mut().poll(cx))).await)
            } else {
                None
            };
            let shutdown = application.shutdown();
            (commitment, admitted, observed, native, shutdown)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Application host is entered"));
    let (work, root_report, native_root, notifications, ()) = outcome.into_parts();
    let (
        (
            commitment,
            admitted,
            observed,
            (constructor, client, admission, received, tail),
            _shutdown,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the finite owning observation survives actual cleanup"));
    cleanup.expect("the Application cleanup task joins");
    let (origin, root) =
        native_root.expect("the actual root retirement remains independently owned");
    let root_report = root_report.expect("the runtime retains its actual root report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(root.custody().is_some());
    let notifications = notifications.expect("both actual notification receipts survive");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(commitment, Some(()));
    assert!(admitted.is_ok());
    assert!(constructor.is_none());
    assert!(matches!(client, Attempt::Returned(Ok(_))));
    assert!(matches!(admission, Attempt::Returned(Ok(()))));
    assert!(matches!(received, Some(None)));
    assert!(tail.is_empty());
    assert_ne!(observed, Some(Poll::Pending));
}
