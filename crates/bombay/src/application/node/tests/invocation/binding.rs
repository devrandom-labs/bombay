//! Public Node binding: retained waits and explicit whole-operation surrender.
use super::super::counter::{CounterIncremented, CounterProcessed, CounterValue};
use super::super::custody::completed_actor;
use super::super::protocol::{
    self, CounterOperation, EndpointRole, ProtectedMessage, Purpose, ReadReplyRef, ReadRequest,
    configuration, limits, recorded,
};
use super::super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::node::Binding;
use crate::behavior::{Behavior, EstablishedRecipient, Never, User};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::{Export, ExportLimits, RefusalRetention, reply};
use crate::{
    ActorFailureAssessment, Application, ApplicationHandle, MailAddr, RetirementAssessment,
    WorkOutcome,
};
use behavior_actors::{Crash, StopOnShutdown};
use core::future::{Future, poll_fn};
use core::pin::pin;
use futures_util::FutureExt;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::ptr;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::timeout;

type CounterHandle = ApplicationHandle<
    <Counter as Behavior>::Protocol,
    <StopOnShutdown<Counter> as Behavior>::Event,
>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one public pending binding retains its original operation, independent progress and every joined result"
)]
async fn borrowed_binding_wait_keeps_the_original_node_operation() {
    Box::pin(timeout(Duration::from_secs(30), async {
        let registered: [ProtectedMessage; 0] = [];
        let binding = protocol::Binding {
            deployment: "pending-binding", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "root", logical_session: 443,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<CounterOperation>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &registered, binding,
            ).await
        };
        let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
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
            "bombay/binding/retained",
            ExportLimits { refusals: RefusalRetention::Retain, queued_queries: maximum,
                retained_queries: maximum, retained_requests: maximum, diagnostics: maximum },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation),
            reply(
                |CounterOperation::Read| Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| CounterMessage::Read(reply_to)),
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef { bombay: &scope.bombay, kind: Purpose::ReadReply,
                        operation: &scope.operation, value: &original.message }).map(Vec::into_boxed_slice)
                },
            ),
            &verify, &protect,
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current, &current,
        );
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver)).await
            .unwrap_or_else(|_| panic!("the actual public Node opens before binding"));
        let connection = node.session.clone();
        let initially_closed = connection.is_closed();
        let (entered, binding_pending) = oneshot::channel();
        let (release, permitted) = oneshot::channel::<()>();
        let mut node = node.exports(export, async move |application: &CounterHandle| {
            let target = application.root().established_recipient();
            let origin = application.actor_address(&target).unwrap();
            let mut replies = application.client::<CounterProcessed>().unwrap();
            let admitted = replies.send(CounterMessage::IncrementAndReply {
                request_sequence: 449, reply_to: replies.recipient(),
            }).await;
            let original = replies.receive().await;
            let mut entered = Some((entered, (application.clone(), origin, admitted, original)));
            let mut permitted = pin!(permitted);
            poll_fn(|context| {
                let observed = permitted.as_mut().poll(context);
                if observed.is_pending()
                    && let Some((publication, original)) = entered.take()
                {
                    let published = publication.send(original);
                    published.unwrap_or_else(|_| panic!("the actual Pending binder has an independent observer"));
                }
                observed
            }).await.expect("the retained original permission releases binding");
            Ok::<_, Never>(target)
        });
        let calls = Cell::new(0);
        let output = Box::new(457_u64);
        let output_allocation = ptr::from_ref(output.as_ref());
        let invoked = &calls;
        let work = Box::new(move |application: CounterHandle| {
            invoked.set(invoked.get() + 1);
            async move {
                let shutdown = application.shutdown();
                let retired = application.retirement().await;
                (output, shutdown, retired)
            }
        });
        let (outcome, application, origin, admission, original, cancelled_wait, before, held, sibling, released) = {
            let mut operation = Box::pin(node.run_with(work));
            let (application, origin, admission, original) = tokio::select! {
                biased;
                original = binding_pending => original.expect("the real opened Node reaches pending binding"),
                _ = operation.as_mut() => panic!("the owned operation cannot complete through a held binder"),
            };
            let cancelled_wait = operation.as_mut().now_or_never();
            let before = calls.get();
            let sibling = Application::new(Counter::new().stop_on_shutdown()).run_with(async |application| {
                let mut values = application.client::<CounterValue>().unwrap();
                let increment = values.send(CounterMessage::Increment).await;
                let read = values.send(CounterMessage::Read(values.recipient())).await;
                let original = values.receive().await;
                let shutdown = application.shutdown();
                (increment, read, original, shutdown)
            }).await.unwrap_or_else(|_| panic!("the independent Counter Application starts"));
            let held = calls.get();
            let released = release.send(());
            let outcome = operation.await.unwrap_or_else(|_| panic!("the SAME retained Node operation resumes"));
            (outcome, application, origin, admission, original, cancelled_wait, before, held, sibling, released)
        };
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (sibling_work, sibling_report, sibling_native, sibling_notifications, ()) = sibling.into_parts();
        let (sibling_values, sibling_cleanup) = sibling_work.try_into_completed()
            .unwrap_or_else(|_| panic!("independent Work retains its actual completed disposition"));
        let extraction = node.into_parts();
        let (sibling_increment, sibling_read, sibling_value, sibling_shutdown) = sibling_values;
        let (sibling_origin, sibling_native) = sibling_native.expect("the independent native root remains");
        let sibling_value = sibling_value.expect("the unrelated Application really progresses while binding is held");
        assert_eq!((sibling_value.from, sibling_value.message), (sibling_origin.address(), 1));
        assert!(sibling_increment.is_ok() && sibling_read.is_ok() && sibling_shutdown.is_ok() && sibling_cleanup.is_ok());
        let _sibling = completed_actor(sibling_origin, sibling_native);
        let sibling_report = sibling_report.expect("the independent joined report remains");
        assert_eq!(sibling_report.retirement(), RetirementAssessment::Established);
        let sibling_notifications = sibling_notifications.expect("independent notifications remain");
        assert!(sibling_notifications.termination.is_ok() && sibling_notifications.retirement.is_ok());
        assert!(cancelled_wait.is_none() && released.is_ok());
        let expected = Some(User::new(origin, CounterIncremented { request_sequence: 449 }));
        assert!(admission.is_ok());
        assert_eq!(original, expected);
        let WorkOutcome::Completed { output, cleanup } = work else {
            panic!("the retained pending operation must invoke its original Work once after binding");
        };
        assert_eq!((before, held, calls.get()), (0, 0, 1));
        assert!(cleanup.is_ok() && output.1.is_ok());
        assert_eq!(ptr::from_ref(output.0.as_ref()), output_allocation);
        assert_eq!(*output.0, 457);
        let report = report.expect("the actual joined root report remains");
        assert_eq!(report, output.2);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let (root_origin, root) = native.expect("the complete original root remains");
        assert_eq!(root_origin.address(), origin);
        let _counter = completed_actor(root_origin, root);
        let notifications = notifications.expect("both original notification results remain");
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        let (export, bound, closed, polling, disposal) = extraction
            .unwrap_or_else(|_| panic!("joined Node execution retains its actual binding and close"));
        let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) = export.into_parts();
        let bound = bound.expect("the original binder returned").expect("the original binder succeeded");
        let target = application.actor_address(&bound).expect("the same original bound target remains associated");
        assert_eq!(target, origin);
        assert!(!initially_closed && connection.is_closed() && closed.is_ok());
        assert!(polling.is_none() && disposal.is_none());
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty() && requests.is_empty());
    })).await.expect("failure-only watchdog; actual pending, actor and original outcomes prove the law");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one surrendered public run retains independent pending, disposal, actor and cleanup observations without inventing native recovery"
)]
async fn surrendered_binding_run_retires_its_actor_without_claiming_joined_node_close() {
    Box::pin(timeout(Duration::from_secs(30), async {
        let registered: [ProtectedMessage; 0] = [];
        let binding = protocol::Binding {
            deployment: "pending-binding", caller: "caller", controller: "owner",
            actor: "counter", host: "receiver", protocol: "counter",
            target_assignment: "root", logical_session: 461,
        };
        let verify = async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<CounterOperation>(
                &ProtectedMessage { protected: bytes.into(), proof: proof.into() },
                &registered, binding,
            ).await
        };
        let protect = async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
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
        let mut export = Export::new(
            "bombay/binding/surrendered",
            ExportLimits { refusals: RefusalRetention::Retain, queued_queries: maximum,
                retained_queries: maximum, retained_requests: maximum, diagnostics: maximum },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                .map(|request| request.operation),
            reply(
                |CounterOperation::Read| Ok::<_, CounterOperation>(|reply_to: EstablishedRecipient<CounterValue>| CounterMessage::Read(reply_to)),
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef { bombay: &scope.bombay, kind: Purpose::ReadReply,
                        operation: &scope.operation, value: &original.message }).map(Vec::into_boxed_slice)
                },
            ),
            &verify, &protect,
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current, &current,
        );
        let withdraw = export.take_withdrawal().unwrap();
        let mut withdrawal = Box::pin(withdraw());
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver)).await
            .unwrap_or_else(|_| panic!("the actual public Node opens before pending binding"));
        let connection = node.session.clone();
        let initially_closed = connection.is_closed();
        let (entered, binding_pending) = oneshot::channel();
        let (permission, permitted) = oneshot::channel::<()>();
        let mut node = node.exports(export, async move |application: &CounterHandle| {
            let target = application.root().established_recipient();
            let origin = application.actor_address(&target).unwrap();
            let mut replies = application.client::<CounterProcessed>().unwrap();
            let admitted = replies.send(CounterMessage::IncrementAndReply {
                request_sequence: 463, reply_to: replies.recipient(),
            }).await;
            let original = replies.receive().await;
            let mut entered = Some((entered, (application.clone(), origin, admitted, original, replies)));
            let mut permitted = pin!(permitted);
            poll_fn(|context| {
                let observed = permitted.as_mut().poll(context);
                if observed.is_pending()
                    && let Some((publication, original)) = entered.take()
                {
                    let published = publication.send(original);
                    published.unwrap_or_else(|_| panic!("the actual Pending binder has an independent observer"));
                }
                observed
            }).await.expect("this trace never grants binding permission");
            Ok::<_, Never>(target)
        });
        let calls = Cell::new(0);
        let invoked = &calls;
        let (captured, mut unused_work) = oneshot::channel();
        let output = Box::new(467_u64);
        let work = Box::new(move |application: CounterHandle| {
            invoked.set(invoked.get() + 1);
            async move {
                let published = captured.send(output);
                let shutdown = application.shutdown();
                (published, shutdown)
            }
        });
        let (application, origin, admission, original, mut replies) = {
            let mut operation = Box::pin(node.run_with(work));
            let original = tokio::select! {
                biased;
                original = binding_pending => original.expect("the real opened Node reaches pending binding"),
                _ = operation.as_mut() => panic!("the owning run cannot finish through a held binder"),
            };
            drop(operation);
            original
        };
        // These independent observations precede any manual shutdown or connection close.
        let disposed_work = unused_work.try_recv();
        let disposed_binding = permission.send(());
        // First polling requests withdrawal; no native result has been acquired.
        let requested_withdrawal = withdrawal.as_mut().now_or_never();
        let joining = application.cleanup().expect("the actual Node binder carries real cleanup observation");
        let reply_address = replies.address();
        let mut admissions = Vec::new();
        let mut continued = Vec::new();
        let mut retirement = None;
        let admitted = replies.send(CounterMessage::IncrementAndReply {
            request_sequence: 479, reply_to: replies.recipient(),
        }).await;
        if admitted.is_ok() {
            tokio::select! {
                biased;
                report = application.retirement() => retirement = Some(report),
                original = replies.receive() => continued.push(original),
            }
        }
        admissions.push(admitted);
        // One turn may straddle the cancellation poll. A second causal reply cannot.
        if continued.first().is_some_and(Option::is_some) {
            let admitted = replies.send(CounterMessage::IncrementAndReply {
                request_sequence: 487, reply_to: replies.recipient(),
            }).await;
            if admitted.is_ok() {
                tokio::select! {
                    biased;
                    report = application.retirement() => retirement = Some(report),
                    original = replies.receive() => continued.push(original),
                }
            }
            admissions.push(admitted);
        }
        let before_rescue = continued.iter().flatten()
            .map(|original| (original.from, original.message.request_sequence)).collect::<Vec<_>>();
        let rescue = if before_rescue.len() == 2 { Some(application.shutdown()) } else { None };
        let retired = match retirement {
            Some(original) => original,
            None => application.retirement().await,
        };
        let terminated = application.termination().await;
        let cleanup = joining.await;
        replies.close_admission();
        if continued.iter().all(Option::is_some) {
            while let Some(original) = replies.receive().await {
                continued.push(Some(original));
            }
        }
        let sibling = Application::new(Counter::new().stop_on_shutdown()).run_with(async |application| {
            let mut values = application.client::<CounterValue>().unwrap();
            let increment = values.send(CounterMessage::Increment).await;
            let read = values.send(CounterMessage::Read(values.recipient())).await;
            let original = values.receive().await;
            let shutdown = application.shutdown();
            (increment, read, original, shutdown)
        }).await.unwrap_or_else(|_| panic!("the independent Counter Application starts after surrender"));
        let mut extraction = node.into_parts();
        let incomplete = extraction.as_mut().err()
            .map(|original| (original.binding.take(), original.closed.take()));
        let explicit_close = match &mut extraction {
            Err(original) => original.close().await,
            Ok(_) => connection.close().await,
        };
        let (work, report, native, notifications, ()) = sibling.into_parts();
        let ((increment, read, value, shutdown), sibling_cleanup) = work.try_into_completed()
            .unwrap_or_else(|_| panic!("independent Work completes before cleanup"));
        let (sibling_origin, sibling_native) = native.expect("the independent original native root remains");
        let value = value.expect("the independent Application makes real progress after surrender");
        assert_eq!((value.from, value.message), (sibling_origin.address(), 1));
        assert!(increment.is_ok() && read.is_ok() && shutdown.is_ok() && sibling_cleanup.is_ok());
        let _sibling = completed_actor(sibling_origin, sibling_native);
        let report = report.expect("the independent joined report remains");
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        let notifications = notifications.expect("both independent notification receipts remain");
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(cleanup.is_ok() && explicit_close.is_ok());
        assert!(!initially_closed && connection.is_closed());
        assert_eq!(calls.get(), 0);
        assert!(matches!(disposed_work, Err(TryRecvError::Closed)));
        assert!(disposed_binding.is_err() && requested_withdrawal.is_none());
        let observed = continued.iter().flatten()
            .map(|original| (original.from, original.message.request_sequence)).collect::<Vec<_>>();
        let expected = [479, 487].into_iter().take(observed.len()).map(|sequence| (origin, sequence)).collect::<Vec<_>>();
        assert!(continued.iter().all(Option::is_some));
        assert_eq!(observed, expected);
        assert!(observed.len() <= 1, "surrender must prevent a second causally later Counter turn, including accepted replies drained after retirement: {observed:?}");
        if let Some(rescue) = rescue {
            assert!(rescue.is_ok(), "adverse-source cleanup retains its actual shutdown result");
        }
        assert_eq!(terminated, Err(Crash::Cancelled));
        assert_eq!(retired.retirement(), RetirementAssessment::Established);
        assert_eq!(retired.failures(), ActorFailureAssessment::NoFailuresFound);
        let expected = Some(User::new(origin, CounterIncremented { request_sequence: 463 }));
        assert!(admission.is_ok());
        assert_eq!(original, expected);
        for (index, admitted) in admissions.into_iter().enumerate() {
            if let Err(original) = admitted {
                let original = original.into_message();
                let CounterMessage::IncrementAndReply { request_sequence, reply_to } = original else {
                    panic!("refused post-surrender admission retains its exact correlated command");
                };
                let endpoint = reply_to.interpret(&mut ExtractLocalEndpoint);
                assert_eq!(request_sequence, [479, 487][index]);
                assert_eq!(endpoint.address(), reply_address);
            }
        }
        let (binding, closed) = incomplete.expect("the pre-close extraction retained the same incomplete Node");
        assert!(matches!(binding, Some(Binding::Invoked(None))));
        assert!(closed.is_none());
        let Err(node) = extraction else {
            panic!("whole run surrender cannot manufacture a returned binding and joined native close");
        };
        assert!(node.closed.is_none() && node.poll_panic.is_none() && node.disposal_panic.is_none());
        let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) = node.exports.into_parts();
        assert!(startup.is_none() && withdrawal.is_none());
        assert!(diagnostics.is_empty() && requests.is_empty());
        // Original Work/native terminal products were surrendered, never reconstructed.
    })).await.expect("failure-only watchdog; actual disposal, actor replies and cleanup prove the law");
}
