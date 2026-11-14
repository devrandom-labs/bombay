//! Exact request reply-endpoint isolation and post-retirement retention.
use super::super::counter::CounterValue;
use super::super::custody::completed_actor;
use super::super::protocol::{
    Binding, CounterOperation, EndpointRole, ProtectedMessage, ProviderProfile, Purpose,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::super::{Counter, CounterMessage};
use bombay::actors::ActorExt;
use bombay::behavior::{EstablishedRecipient, Never, User};
use bombay::remote::{Attempt, Export, ExportLimits, Metadata, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "the complete late-reply trace retains both owners and cleanup while verification borrows immutable registered fixtures"
)]
async fn cold_counter_request_keeps_its_own_reply_and_late_tail() {
    tokio::time::timeout(Duration::from_secs(40), async {
        let records = RefCell::new(Vec::<ProtectedMessage>::new());
        let claims = RefCell::new(Vec::<Metadata>::new());
        let recipients = RefCell::new(Vec::new());
        let before = RefCell::new(Vec::new());
        let originals =
            |raw: &[u8], proof: &[u8], receipt: &recorded::Receipt<CounterOperation>| {
                let scope = recorded::projection(receipt);
                let metadata = &scope.bombay;
                (
                    raw.to_vec(),
                    proof.to_vec(),
                    serde_json::to_vec(&ReadRequestRef {
                        bombay: metadata,
                        kind: scope.kind,
                        operation: scope.operation,
                    })
                    .unwrap(),
                    *recorded::identity(receipt).0,
                    [
                        raw.as_ptr(),
                        proof.as_ptr(),
                        metadata.provider_profile.as_ptr(),
                        metadata.deployment.as_ptr(),
                        metadata.caller.as_ptr(),
                        metadata.caller_session.as_ptr(),
                        metadata.controller.as_ptr(),
                        metadata.actor.as_ptr(),
                        metadata.host.as_ptr(),
                        metadata.protocol.as_ptr(),
                        metadata.grant_issuer.as_ptr(),
                        metadata.runtime_generation.as_ptr(),
                    ],
                )
            };
        let binding = Binding {
            deployment: "two-node-read-study",
            caller: "caller",
            controller: "owner",
            actor: "counter",
            host: "receiver",
            protocol: "counter",
            target_assignment: "association-counter",
            logical_session: 41,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let current =
            |receipt: &recorded::Receipt<CounterOperation>, runtime: &str, target: MailAddr| {
                let claims = claims.borrow();
                let current = claims
                    .iter()
                    .find(|current| current.request_sequence == recorded::identity(receipt).1)
                    .ok_or(VerificationError::Invalid)?;
                if current.runtime_generation.as_ref() != runtime
                    || current.actor_generation != target.0
                {
                    return Err(VerificationError::Invalid);
                }
                current_scope(
                    recorded::projection(receipt),
                    current,
                    &CounterOperation::Read,
                    Purpose::Request,
                    deadline,
                )
            };
        let maximum = NonZeroUsize::new(2).unwrap();
        let export = Export::new(
            "bombay/association/read",
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes| {
                serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes)
                    .map(|request| request.operation)
            },
            reply(
                |CounterOperation::Read| {
                    let recipients = &recipients;
                    Ok::<_, CounterOperation>(
                        move |reply_to: EstablishedRecipient<CounterValue>| {
                            recipients.borrow_mut().push(reply_to.clone());
                            CounterMessage::Read(reply_to)
                        },
                    )
                },
                |_: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    serde_json::to_vec(&original.message).map(Vec::into_boxed_slice)
                },
            ),
            async |raw: &[u8], proof: &[u8]| {
                let original = ProtectedMessage {
                    protected: raw.into(),
                    proof: proof.into(),
                };
                let result =
                    recorded::verify::<CounterOperation>(&original, &records.borrow(), binding)
                        .await;
                if let Ok(receipt) = &result {
                    before.borrow_mut().push(originals(raw, proof, receipt));
                }
                result
            },
            async |_: &[u8], _: &recorded::Receipt<CounterOperation>| {
                Ok::<_, Never>(Box::<[u8]>::from([13_u8, 17]))
            },
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current,
            &current,
        );
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("the actual Counter Node opens"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let (configured, request) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let mut first = metadata(
                ProviderProfile::Recorded,
                "counter",
                runtime.clone(),
                runtime.clone(),
            );
            first.actor_generation = application.actor_address(&target).unwrap().0;
            let mut second = metadata(
                ProviderProfile::Recorded,
                "counter",
                runtime.clone(),
                runtime,
            );
            second.actor_generation = first.actor_generation;
            second.request_sequence = first.request_sequence.checked_add(1).unwrap();
            let mut requests = Vec::new();
            for current in [first, second] {
                let original = ProtectedMessage {
                    protected: serde_json::to_vec(&ReadRequestRef {
                        bombay: &current,
                        kind: Purpose::Request,
                        operation: CounterOperation::Read,
                    })
                    .unwrap()
                    .into_boxed_slice(),
                    proof: Box::from([7_u8, 11]),
                };
                records.borrow_mut().push(ProtectedMessage {
                    protected: original.protected.clone(),
                    proof: original.proof.clone(),
                });
                claims.borrow_mut().push(current);
                requests.push(original);
            }
            configured
                .send(requests)
                .unwrap_or_else(|_| panic!("Work retains the two real requests"));
            Ok::<_, Never>(target)
        });
        let outcome = node
            .run_with(|application| {
                let session = &session;
                let recipients = &recipients;
                async move {
                    let requests = request
                        .await
                        .expect("binding supplies the two original requests");
                    let sender = application.sender().unwrap();
                    let mut fence = application
                        .client_to::<_, CounterValue>(application.root().established_recipient())
                        .unwrap_or_else(|_| {
                            panic!("the independent FIFO fence client is allocated")
                        });
                    let primed = sender.send(CounterMessage::Increment).await;
                    let first_query = session
                        .get("bombay/association/read")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(10))
                        .payload(requests[0].protected.to_vec())
                        .attachment(requests[0].proof.to_vec())
                        .await;
                    let mut first_replies = Vec::new();
                    let first_end = if let Ok(receiving) = &first_query {
                        loop {
                            match receiving.recv_async().await {
                                Ok(reply) => first_replies.push(reply),
                                Err(cause) => break Some(cause),
                            }
                        }
                    } else {
                        None
                    };
                    let first_recipient = recipients.borrow().first().cloned();
                    let late = match &first_recipient {
                        Some(recipient) => {
                            Some(sender.send(CounterMessage::Read(recipient.clone())).await)
                        }
                        None => None,
                    };
                    let fenced = fence.send(CounterMessage::Read(fence.recipient())).await;
                    let first_fence = if fenced.is_ok() {
                        fence.receive().await
                    } else {
                        None
                    };
                    let incremented = sender.send(CounterMessage::Increment).await;
                    let second_query = session
                        .get("bombay/association/read")
                        .allowed_destination(Locality::SessionLocal)
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(10))
                        .payload(requests[1].protected.to_vec())
                        .attachment(requests[1].proof.to_vec())
                        .await;
                    let mut second_replies = Vec::new();
                    let second_end = if let Ok(receiving) = &second_query {
                        loop {
                            match receiving.recv_async().await {
                                Ok(reply) => second_replies.push(reply),
                                Err(cause) => break Some(cause),
                            }
                        }
                    } else {
                        None
                    };
                    let fenced_again = fence.send(CounterMessage::Read(fence.recipient())).await;
                    let second_fence = if fenced_again.is_ok() {
                        fence.receive().await
                    } else {
                        None
                    };
                    let stopped = application.shutdown();
                    (
                        requests,
                        primed,
                        first_query,
                        first_replies,
                        first_end,
                        first_recipient,
                        late,
                        fenced,
                        first_fence,
                        incremented,
                        second_query,
                        second_replies,
                        second_end,
                        fenced_again,
                        second_fence,
                        stopped,
                        fence,
                    )
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the actual Node enters its host"));
        let (export, bound, closed, polling, disposal) = node
            .into_parts()
            .unwrap_or_else(|_| panic!("the original Node finalization is acquired"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (
            (
                requests,
                primed,
                first_query,
                first_replies,
                first_end,
                first_recipient,
                late,
                fenced,
                first_fence,
                incremented,
                second_query,
                second_replies,
                second_end,
                fenced_again,
                second_fence,
                stopped,
                mut fence,
            ),
            cleanup,
        ) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("original Work completes"));
        fence.close_admission();
        let mut fence_tail = Vec::new();
        loop {
            let original = fence.receive().await;
            let ended = original.is_none();
            fence_tail.push(original);
            if ended {
                break;
            }
        }
        let (origin, native) = native.expect("the complete native root is acquired");
        let _counter = completed_actor(origin, native);
        let report = report.expect("the independent root report is acquired");
        let notifications = notifications.expect("both notification receipts are acquired");
        let (_, _, startup, withdrawal, diagnostics, retained) = export.into_parts();
        let rows = retained
            .into_iter()
            .map(|request| {
                request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("each request releases its actual producer loans"))
            })
            .collect::<Vec<_>>();
        let after = rows
            .iter()
            .map(|row| {
                originals(
                    &row.1.0,
                    &row.1.1,
                    row.2.as_ref().unwrap().as_ref().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let before = before.into_inner();
        let recipients = recipients.borrow();
        let claims = claims.borrow();
        assert!(cleanup.is_ok() && closed.is_ok() && session.is_closed());
        assert!(polling.is_none() && disposal.is_none());
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(matches!(bound, Some(Ok(_))));
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(diagnostics.is_empty());
        assert!(primed.is_ok() && incremented.is_ok() && stopped.is_ok());
        assert!(first_query.is_ok() && second_query.is_ok());
        assert!(first_end.is_some() && second_end.is_some());
        assert!(matches!(late, Some(Ok(()))) && fenced.is_ok() && fenced_again.is_ok());
        let first_fence = first_fence.expect("the actual fence observes the late A send");
        let second_fence = second_fence.expect("the actual fence observes B before shutdown");
        assert_eq!(
            (first_fence.from, first_fence.message),
            (origin.address(), 1)
        );
        assert_eq!(
            (second_fence.from, second_fence.message),
            (origin.address(), 2)
        );
        assert!(matches!(fence_tail.as_slice(), [None]));
        assert_eq!(rows.len(), 2);
        assert_eq!(recipients.len(), 2);
        assert_eq!(before, after);
        assert!(rows[0].0.is_some() && rows[1].0.is_some());
        assert_ne!(rows[0].0, rows[1].0);
        assert_ne!(recipients[0], recipients[1]);
        assert_eq!(first_recipient.as_ref(), Some(&recipients[0]));
        for (index, row) in rows.iter().enumerate() {
            let receipt = row.2.as_ref().unwrap().as_ref().unwrap();
            let scope = recorded::projection(receipt);
            let expected = &claims[index];
            assert_eq!(scope.bombay, *expected);
            assert_eq!(scope.kind, Purpose::Request);
            assert_eq!(scope.operation, CounterOperation::Read);
            assert_eq!(recorded::identity(receipt).0, &binding);
            assert_eq!(row.1.0, requests[index].protected);
            assert_eq!(row.1.1, requests[index].proof);
            assert!(row.3.is_none());
            assert!(matches!(row.8, (None, None, None)));
            assert!(matches!(row.9, (None, None, None, None, None, None)));
            let (constructor, client, admission, received, tail) = row
                .4
                .as_ref()
                .expect("each real native operation remains in its own row");
            let Attempt::Returned(Ok(client)) = client else {
                panic!("the original native client remains");
            };
            let recipient = client.recipient();
            assert!(constructor.is_none() && matches!(admission, Attempt::Returned(Ok(()))));
            assert_eq!(recipient, recipients[index]);
            assert_ne!(client.address(), origin.address());
            let primary = received.as_ref().unwrap().as_ref().unwrap();
            assert_eq!(primary.from, origin.address());
            assert_eq!(primary.message, if index == 0 { 1 } else { 2 });
            match (index, tail.as_slice()) {
                (0, [Some(late), None]) => {
                    assert_eq!((late.from, late.message), (origin.address(), 1));
                }
                (1, [None]) => {}
                _ => panic!("A retains exactly its late reply and EOF; B retains only its EOF"),
            }
            let expected = if index == 0 {
                b"1".as_slice()
            } else {
                b"2".as_slice()
            };
            assert_eq!(row.5.as_ref().unwrap().as_ref().unwrap().as_ref(), expected);
            assert_eq!(row.6.as_ref().unwrap().as_ref().unwrap().as_ref(), [13, 17]);
            assert_eq!(row.7.len(), 1);
            assert!(row.7.iter().all(Result::is_ok));
        }
        for (replies, expected) in [(&first_replies, b"1"), (&second_replies, b"2")] {
            let [reply] = replies.as_slice() else {
                panic!("each Query retains exactly one actual reply");
            };
            let result = reply.result();
            let sample = result
                .as_ref()
                .expect("the actual protected reply is successful");
            let payload = sample.payload().to_bytes();
            let proof = sample
                .attachment()
                .expect("the actual reply proof remains")
                .to_bytes();
            assert_eq!(payload.as_ref(), expected);
            assert_eq!(proof.as_ref(), [13, 17]);
        }
    })
    .await
    .expect("the native association trace and all cleanup finish");
}
