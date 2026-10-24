//! Ready provider results coexist with actual disposal causes before any later stage.
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Step, Stopped, User};
use bombay::prelude::Application;
use bombay::remote::{
    Attempt, Client, ClientLimits, Export, ExportLimits, Metadata, RefusalRetention, reply,
};
use bombay::{ActorFailureAssessment, ApplicationScope, MailAddr, RetirementAssessment};
use futures_util::future::Either;
use std::cell::Cell;
use std::cell::RefCell;
use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::panic::panic_any;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

use super::network as protocol;
use super::{Counter, CounterMessage, CounterValue, custody, providers, reader, requester};
use custody::completed_actor;
use providers::{
    CounterOperation, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, limits, logical_scope, metadata, recorded,
};
use reader::{Reader, ReaderReply};
use requester::{CounterProtectedMeaning, CounterRequests, CounterServiceInput, Scope};

#[derive(Clone, Copy)]
enum ProviderFault {
    Healthy,
    ProtectionDisposal,
    VerificationDisposal,
}

#[derive(Debug)]
struct ProviderDisposalCause(Box<[u8]>);

struct ProviderDisposal<'observations, Producer> {
    producer: Pin<Box<Producer>>,
    cause: Option<ProviderDisposalCause>,
    polls: &'observations Cell<usize>,
    disposals: &'observations Cell<usize>,
}

impl<Producer: Future> Future for ProviderDisposal<'_, Producer> {
    type Output = Producer::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.get_mut();
        original.polls.set(original.polls.get() + 1);
        original.producer.as_mut().poll(context)
    }
}

impl<Producer> Drop for ProviderDisposal<'_, Producer> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        if let Some(original) = self.cause.take() {
            panic_any(original);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::large_futures,
    reason = "the actual two-Node control retains all concrete provider and native outcomes without extra boxing"
)]
async fn ready_providers_deliver_the_original_checked_read() {
    tokio::time::timeout(Duration::from_secs(10), read(ProviderFault::Healthy))
        .await
        .expect("the healthy real provider trace joins before the native deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::large_futures,
    reason = "the actual two-Node protection fault retains all concrete provider and native outcomes without extra boxing"
)]
async fn protection_disposal_preserves_ready_proof_without_entering_get() {
    tokio::time::timeout(
        Duration::from_secs(10),
        read(ProviderFault::ProtectionDisposal),
    )
    .await
    .expect("the protection disposal trace joins before the native deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::large_futures,
    reason = "the actual two-Node verification fault retains all concrete provider and native outcomes without extra boxing"
)]
async fn verification_disposal_preserves_ready_receipt_without_delivering() {
    tokio::time::timeout(
        Duration::from_secs(10),
        read(ProviderFault::VerificationDisposal),
    )
    .await
    .expect("the verification disposal trace joins before the native deadline");
}

#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one causal provider fault trace retains the original request, independent acquired results, exact panic cause and both joined outcomes"
)]
async fn read(fault: ProviderFault) {
    let address = protocol::reserved_address();
    let maximum = NonZeroUsize::new(4).unwrap();
    let records = &RefCell::new(Vec::<(Scope, ProtectedMessage)>::new());
    let claims = &RefCell::new(None::<Metadata>);
    let caller_identity = &RefCell::new(None::<String>);
    let encoded = &RefCell::new(Vec::new());
    let checked = &RefCell::new(Vec::new());
    let acknowledged = &RefCell::new(Vec::new());
    let reader_address = &Cell::new(None);
    let protection_polls = &Cell::new(0);
    let protection_disposals = &Cell::new(0);
    let verification_polls = &Cell::new(0);
    let verification_disposals = &Cell::new(0);
    let protection_cause = ProviderDisposalCause(Box::from([79_u8, 83, 89]));
    let protection_address = protection_cause.0.as_ptr();
    let protection_cause = &RefCell::new(Some(protection_cause));
    let verification_cause = ProviderDisposalCause(Box::from([97_u8, 101, 103]));
    let verification_address = verification_cause.0.as_ptr();
    let verification_cause = &RefCell::new(Some(verification_cause));
    let receiver_verifications = &Cell::new(0);
    let receiver_constructions = &Cell::new(0);
    let receiver_encodings = &Cell::new(0);
    let receiver_protections = &Cell::new(0);
    let priming = RefCell::new(None);
    let (started, observed) = oneshot::channel();
    let started = RefCell::new(Some(started));
    let receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(protocol::configuration(
            &address,
            &protocol::EndpointRole::Receiver,
        ))
        .await
        .unwrap_or_else(|_| panic!("the actual receiving Counter Node opens"));
    let receiver_id = receiver.runtime_id();
    let export = Export::new(
        "bombay/outbound/read",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest>(bytes),
        reply(
            |request: ReadRequest| match request.operation {
                CounterOperation::Read => Ok::<_, ReadRequest>(move |reply_to| {
                    receiver_constructions.set(receiver_constructions.get() + 1);
                    CounterMessage::Read(reply_to)
                }),
            },
            |receipt: &recorded::Receipt, original: &User<MailAddr, u64>| {
                receiver_encodings.set(receiver_encodings.get() + 1);
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).0.bombay,
                    kind: Purpose::ReadReply,
                    operation: CounterOperation::Read,
                    value: &original.message,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            receiver_verifications.set(receiver_verifications.get() + 1);
            recorded::verify(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                records,
            )
            .await
        },
        async |bytes: &[u8], receipt: &recorded::Receipt| {
            receiver_protections.set(receiver_protections.get() + 1);
            let parsed: ReadReply =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if parsed.bombay != recorded::projection(receipt).0.bombay
                || parsed.kind != Purpose::ReadReply
                || parsed.operation != CounterOperation::Read
            {
                return Err(VerificationError::Invalid);
            }
            recorded::protect(bytes, receipt, records).await
        },
        |left, right| recorded::projection(left).1 == recorded::projection(right).1,
        |receipt: &recorded::Receipt, runtime: &str, target: MailAddr| {
            let actual = recorded::projection(receipt).0;
            if claims.borrow().as_ref() == Some(&actual.bombay)
                && actual.kind == Purpose::Request
                && actual.operation == CounterOperation::Read
                && actual.bombay.runtime_generation.as_ref() == runtime
                && actual.bombay.actor_generation == target.0
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |receipt: &recorded::Receipt, runtime: &str, target: MailAddr| {
            let actual = recorded::projection(receipt).0;
            if claims.borrow().as_ref() == Some(&actual.bombay)
                && actual.bombay.runtime_generation.as_ref() == runtime
                && actual.bombay.actor_generation == target.0
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        },
    );
    let mut receiver = receiver.exports(export, async |application| {
        let target = application.root().established_recipient();
        let actual_address = application
            .actor_address(&target)
            .expect("the certified actual receiving root");
        let mut counter = application
            .client::<CounterValue>()
            .expect("a real FIFO priming client");
        let increment = counter.send(CounterMessage::Increment).await;
        let admitted = counter
            .send(CounterMessage::Read(counter.recipient()))
            .await;
        let fenced = counter.receive().await;
        *priming.borrow_mut() = Some((increment, admitted, fenced));
        let published = started
            .borrow_mut()
            .take()
            .expect("one real startup publication")
            .send((actual_address, application.lifecycle()));
        published.unwrap_or_else(|_| panic!("the outside consumer retains startup receiving"));
        Ok::<_, VerificationError>(target)
    });
    let outgoing = async {
        let (counter_address, lifecycle) = observed.await.expect("actual receiver binding");
        {
            let scope = ApplicationScope::new();
            let requests = scope.external::<CounterRequests>().unwrap();
            let source_address = requests.address();
            let mut logical = logical_scope();
            logical.identity.binding.actor_generation = counter_address.0;
            let reader = Reader {
                service: requests.recipient(),
                scope: logical,
                replies: Vec::new(),
            };
            let caller = scope
                .with_root(
                    reader.with_deadline(Some(Instant::now() + Duration::from_secs(1)), |_| {
                        Step::Stop(Stopped)
                    }),
                )
                .open_node(protocol::configuration(
                    &address,
                    &protocol::EndpointRole::Caller,
                ))
                .await
                .unwrap_or_else(|_| panic!("the actual caller Node opens"));
            let caller_id = caller.runtime_id();
            *caller_identity.borrow_mut() = Some(caller_id.clone());
            let mut declared = metadata(
                ProviderProfile::Recorded,
                caller_id.clone(),
                receiver_id.clone(),
            );
            declared.actor_generation = counter_address.0;
            *claims.borrow_mut() = Some(declared);
            let client = Client::new(
                requests,
                "bombay/outbound/read",
                ClientLimits {
                    concurrent_requests: maximum,
                    retained_requests: maximum,
                    retained_responses: maximum,
                },
                limits(),
                Duration::from_secs(20),
                move |original: &User<MailAddr, CounterServiceInput>,
                      runtime: &str,
                      target: MailAddr| {
                    if original.from != target
                        || caller_identity.borrow().as_deref() != Some(runtime)
                    {
                        return Err(VerificationError::Invalid);
                    }
                    match original.message {
                        CounterServiceInput::Verify(sequence)
                            if sequence == logical.identity.sequence =>
                        {
                            let bytes = serde_json::to_vec(&ReadRequestRef {
                                bombay: claims
                                    .borrow()
                                    .as_ref()
                                    .expect("the actual configured claims"),
                                kind: Purpose::Request,
                                operation: CounterOperation::Read,
                            })
                            .map_err(VerificationError::Declaration)?
                            .into_boxed_slice();
                            encoded
                                .borrow_mut()
                                .push((original.from, sequence, bytes.to_vec()));
                            Ok(Some(bytes))
                        }
                        CounterServiceInput::ProtectedConsumed {
                            correlation,
                            purpose,
                        } if correlation == logical.identity.sequence
                            && purpose == requester::CounterProofPurpose::ReadReply =>
                        {
                            acknowledged
                                .borrow_mut()
                                .push((original.from, correlation, purpose));
                            Ok(None)
                        }
                        _ => Err(VerificationError::Invalid),
                    }
                },
                move |bytes: &[u8], original: &User<MailAddr, CounterServiceInput>| {
                    let producer = match original.message {
                        CounterServiceInput::Verify(sequence)
                            if sequence == logical.identity.sequence =>
                        {
                            recorded::protect_request(
                                bytes,
                                claims
                                    .borrow()
                                    .as_ref()
                                    .expect("the actual configured claims"),
                                logical,
                                records,
                            )
                        }
                        _ => ready(Err(VerificationError::Invalid)),
                    };
                    let cause = match fault {
                        ProviderFault::ProtectionDisposal => protection_cause.borrow_mut().take(),
                        ProviderFault::Healthy | ProviderFault::VerificationDisposal => None,
                    };
                    ProviderDisposal {
                        producer: Box::pin(producer),
                        cause,
                        polls: protection_polls,
                        disposals: protection_disposals,
                    }
                },
                move |bytes: &[u8], proof: &[u8]| {
                    let producer = recorded::verify(
                        &ProtectedMessage {
                            protected: bytes.into(),
                            proof: proof.into(),
                        },
                        records,
                    );
                    let cause = match fault {
                        ProviderFault::VerificationDisposal => {
                            verification_cause.borrow_mut().take()
                        }
                        ProviderFault::Healthy | ProviderFault::ProtectionDisposal => None,
                    };
                    ProviderDisposal {
                        producer: Box::pin(producer),
                        cause,
                        polls: verification_polls,
                        disposals: verification_disposals,
                    }
                },
                move |receipt: &recorded::Receipt,
                      bytes: &[u8],
                      original: &User<MailAddr, CounterServiceInput>| {
                    let parsed: ReadReply =
                        serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                    let (protected, scope) = recorded::projection(receipt);
                    if protected.bombay != parsed.bombay
                        || claims.borrow().as_ref() != Some(&parsed.bombay)
                        || protected.kind != Purpose::ReadReply
                        || parsed.kind != Purpose::ReadReply
                        || protected.operation != CounterOperation::Read
                        || parsed.operation != CounterOperation::Read
                        || scope != logical
                        || !matches!(original.message, CounterServiceInput::Verify(sequence) if sequence == logical.identity.sequence)
                    {
                        return Err(VerificationError::Invalid);
                    }
                    checked.borrow_mut().push((scope, parsed.value));
                    Ok(ReaderReply {
                        scope,
                        meaning: CounterProtectedMeaning::ReadReply(parsed.value),
                    })
                },
                move |original: &User<MailAddr, CounterServiceInput>,
                      runtime: &str,
                      target: MailAddr| {
                    if original.from == target
                        && caller_identity.borrow().as_deref() == Some(runtime)
                    {
                        Ok(())
                    } else {
                        Err(VerificationError::Invalid)
                    }
                },
                move |receipt: &recorded::Receipt,
                      original: &User<MailAddr, CounterServiceInput>,
                      runtime: &str,
                      target: MailAddr| {
                    let (protected, scope) = recorded::projection(receipt);
                    if original.from == target
                        && caller_identity.borrow().as_deref() == Some(runtime)
                        && scope == logical
                        && claims.borrow().as_ref() == Some(&protected.bombay)
                        && protected.kind == Purpose::ReadReply
                    {
                        Ok(())
                    } else {
                        Err(VerificationError::Invalid)
                    }
                },
            );
            let mut caller = caller.exports(client, async |application| {
                let target = application.root().established_recipient();
                let address = application.actor_address(&target)?;
                reader_address.set(Some(address));
                Ok::<_, bombay::remote::ExportError>(target)
            });
            let outcome = caller
                .run()
                .await
                .unwrap_or_else(|_| panic!("actual absent-Work caller startup"));
            let terminal = caller
                .into_parts()
                .unwrap_or_else(|_| panic!("joined outgoing owner and actual native close"));
            let shutdown = lifecycle.shutdown();
            let retirement = lifecycle.retirement().await;
            (
                counter_address,
                shutdown,
                retirement,
                source_address,
                outcome,
                terminal,
            )
        }
    };
    let (counter_outcome, outgoing) = tokio::join!(receiver.run(), outgoing);
    let counter_outcome =
        counter_outcome.unwrap_or_else(|_| panic!("the receiver runs with genuine Work absence"));
    let (export, bound, closed, polling, disposal) = receiver
        .into_parts()
        .unwrap_or_else(|_| panic!("all receiver originals and native close acquired"));
    let (work, report, native, notifications, ()) = counter_outcome.into_parts();
    let (_absence, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the receiver has genuine Work absence"));
    let (origin, native) = native.expect("actual original receiving Counter");
    let report = report.expect("actual receiver retirement report");
    let notifications = notifications.expect("both actual receiver publications");
    let (_inputs, declarations, declared, withdrawn, diagnostics, rows) = export.into_parts();
    let primed = priming
        .into_inner()
        .expect("the priming producer stored its original results");
    assert!(cleanup.is_ok() && closed.is_ok() && primed.0.is_ok() && primed.1.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    notifications
        .termination
        .expect("receiver termination published");
    notifications
        .retirement
        .expect("receiver joined retirement published");
    let actual_counter = completed_actor(origin, native);
    let target = bound
        .expect("actual binding invoked")
        .expect("actual root association");
    let fenced = primed.2.expect("the actual FIFO read returns");
    assert_eq!((fenced.from, fenced.message), (outgoing.0, 1));
    assert!(outgoing.1.is_ok());
    assert_eq!(outgoing.2, report);
    assert!(matches!(declared, Some(Ok(()))) && matches!(withdrawn, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    {
        let (counter_address, _, _, source_address, caller_outcome, terminal) = outgoing;
        let (whole_client, bound, caller_closed, polling, disposal) = terminal;
        let (work, report, native, notifications, ()) = caller_outcome.into_parts();
        let (_absence, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the Reader runs with genuine Work absence"));
        let report = report.expect("actual Reader retirement report");
        let (origin, native) = native.expect("actual original Reader");
        let notifications = notifications.expect("both original Reader publications");
        let retained_reader = completed_actor(origin, native);
        let reader = retained_reader.base();
        let recipient = bound
            .expect("actual caller binding invoked")
            .expect("the actual Reader is associated");
        let reader_address = reader_address
            .get()
            .expect("actual certified Reader address");
        notifications
            .termination
            .expect("Reader termination published");
        notifications
            .retirement
            .expect("Reader joined retirement published");
        assert!(cleanup.is_ok() && caller_closed.is_ok());
        assert!(polling.is_none() && disposal.is_none());
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert_ne!(source_address, reader_address);
        let (mut source, inputs, startup, withdrawal, joined, source_eof, outgoing_rows, routes) =
            whole_client.into_parts();
        let (listener, observed, listener_withdrawal) = routes;
        let outgoing_count = outgoing_rows.len();
        let mut outgoing_rows = outgoing_rows.into_iter();
        let (
            source_user,
            source_refusal,
            request_encoding,
            request_protection,
            request_permission,
            request_capacity,
            query,
            callback_refusal,
            replies,
            callback_eof,
            protection_panics,
            operation_panics,
            completed,
        ) = outgoing_rows
            .next()
            .expect("the actual initial Actions request is retained");
        let remaining_source = outgoing_rows.collect::<Vec<_>>();
        let mut source_tail = Vec::new();
        if source_eof.is_none() {
            source.close_admission();
            while let Some(original) = source.receive().await {
                source_tail.push(original);
            }
        }
        let encoded = encoded.borrow();
        let checked = checked.borrow();
        let acknowledged = acknowledged.borrow();
        let registry = records.borrow();
        let receiver_count = rows.len();
        let receiver_rows = rows
            .into_iter()
            .map(|row| {
                row.try_into_parts().unwrap_or_else(|_| {
                    panic!("joined receiving producers release their original facts")
                })
            })
            .collect::<Vec<_>>();
        let protection_original = protection_panics.2.map(|original| {
            original
                .downcast::<ProviderDisposalCause>()
                .unwrap_or_else(|_| panic!("retain the actual typed protection disposal cause"))
        });
        assert!(
            matches!(startup, Some(Ok(())))
                && matches!(withdrawal, Some(Ok(())))
                && matches!(joined, Some(Ok(())))
        );
        assert!(source_eof.is_some() && source_tail.is_empty());
        assert!(matches!(listener, Some(Ok(()))) && matches!(listener_withdrawal, Some(Ok(()))));
        assert!(
            request_permission.is_none()
                && request_capacity.is_none()
                && source_refusal.is_none()
                && callback_refusal.is_none()
        );
        assert!(protection_panics.0.is_none() && protection_panics.1.is_none());
        assert!(
            operation_panics.0.is_none()
                && operation_panics.1.is_none()
                && operation_panics.2.is_none()
        );
        assert_eq!(source_user.from, reader_address);
        assert!(matches!(
            source_user.message,
            CounterServiceInput::Verify(23)
        ));
        let request_bytes = request_encoding
            .as_ref()
            .expect("actual request encoding result")
            .as_ref()
            .unwrap_or_else(|_| panic!("the concrete request encodes"))
            .as_ref()
            .expect("Verify selects one real Get");
        let request_proof = request_protection
            .as_ref()
            .expect("actual Ready protection result")
            .as_ref()
            .unwrap_or_else(|_| panic!("the finite registered proof succeeds"));
        assert_eq!((protection_polls.get(), protection_disposals.get()), (1, 1));
        assert_eq!(request_bytes.as_ref(), registry[0].1.protected.as_ref());
        assert_eq!(request_proof.as_ref(), registry[0].1.proof.as_ref());
        match fault {
            ProviderFault::ProtectionDisposal => {
                assert!(observed.is_none());
                let original = protection_original
                    .as_ref()
                    .expect("Ready proof and original disposal panic coexist");
                assert_eq!(original.0.as_ptr(), protection_address);
                assert_eq!(original.0.as_ref(), [79, 83, 89]);
                // Primary old-source failure: actual protection was acquired,
                // but its failed disposal must not authorize native Get entry.
                assert!(matches!(query, Attempt::Unattempted));
                assert!(replies.is_empty() && callback_eof.is_none());
                assert_eq!(reader.replies.as_slice(), []);
                assert!(checked.is_empty() && acknowledged.is_empty());
                assert_eq!(
                    (verification_polls.get(), verification_disposals.get()),
                    (0, 0)
                );
                assert_eq!(receiver_count, 0);
                assert_eq!(
                    (
                        receiver_verifications.get(),
                        receiver_constructions.get(),
                        receiver_encodings.get(),
                        receiver_protections.get()
                    ),
                    (0, 0, 0, 0)
                );
                assert_eq!(registry.len(), 1);
                assert_eq!(outgoing_count, 1);
                assert!(remaining_source.is_empty());
            }
            ProviderFault::Healthy | ProviderFault::VerificationDisposal => {
                assert!(matches!(observed, Some(Ok(()))));
                assert!(protection_original.is_none());
                assert!(matches!(query, Attempt::Returned(Ok(()))));
                assert!(callback_eof.is_some());
                let [response] = <[_; 1]>::try_from(replies)
                    .unwrap_or_else(|_| panic!("one original checked native response remains"));
                // Observe the actual native fragments before consuming their owner.
                let sample_bytes = response.payload().flatten().copied().collect::<Vec<_>>();
                let sample_proof = response
                    .proof()
                    .expect("actual native response proof")
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                let (cause, wire, shape, verified, decoded, permission, verification_panics) =
                    response.into_parts();
                let wire = wire.expect("the exact actual native bytes/proof remain");
                let receipt = verified
                    .expect("the actual provider Ready result remains")
                    .unwrap_or_else(|_| panic!("the exact registered native reply verifies"));
                let scope = recorded::projection(&receipt);
                let verification_original = verification_panics.2.map(|original| {
                    original
                        .downcast::<ProviderDisposalCause>()
                        .unwrap_or_else(|_| {
                            panic!("retain the actual typed verification disposal cause")
                        })
                });
                assert!(cause.is_none() && matches!(shape, Some(Ok(()))) && permission.is_none());
                assert!(verification_panics.0.is_none() && verification_panics.1.is_none());
                assert_eq!(wire.0.as_ref(), sample_bytes.as_slice());
                assert_eq!(wire.1.as_ref(), sample_proof.as_slice());
                assert_eq!(wire.0.as_ref(), registry[1].1.protected.as_ref());
                assert_eq!(wire.1.as_ref(), registry[1].1.proof.as_ref());
                assert_eq!((scope.1, scope.0.kind), (reader.scope, Purpose::ReadReply));
                assert_eq!(
                    (verification_polls.get(), verification_disposals.get()),
                    (1, 1)
                );
                match fault {
                    ProviderFault::VerificationDisposal => {
                        let original = verification_original
                            .as_ref()
                            .expect("Ready receipt and original disposal panic coexist");
                        assert_eq!(original.0.as_ptr(), verification_address);
                        assert_eq!(original.0.as_ref(), [97, 101, 103]);
                        // Primary old-source failure: the actual Ready receipt
                        // survives, but disposal must stop decoding and delivery.
                        assert!(decoded.is_none());
                        assert_eq!(reader.replies.as_slice(), []);
                        assert!(checked.is_empty() && acknowledged.is_empty());
                        assert_eq!(outgoing_count, 1);
                        assert!(remaining_source.is_empty());
                    }
                    ProviderFault::Healthy => {
                        assert!(verification_original.is_none());
                        assert_eq!(
                            reader.replies.as_slice(),
                            [CounterProtectedMeaning::ReadReply(1)]
                        );
                        assert!(matches!(
                            decoded,
                            Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
                        ));
                        assert_eq!(checked.as_slice(), [(reader.scope, 1)]);
                        assert_eq!(
                            acknowledged.as_slice(),
                            [(
                                reader_address,
                                23,
                                requester::CounterProofPurpose::ReadReply
                            )]
                        );
                        assert_eq!(outgoing_count, 2);
                        let [ack] = <[_; 1]>::try_from(remaining_source).unwrap_or_else(|_| {
                            panic!("the actual terminal Actions acknowledgement remains")
                        });
                        assert_eq!(ack.0.from, reader_address);
                        assert!(matches!(
                            ack.0.message,
                            CounterServiceInput::ProtectedConsumed {
                                correlation: 23,
                                purpose: requester::CounterProofPurpose::ReadReply
                            }
                        ));
                        assert!(matches!(ack.2, Some(Ok(None))) && ack.3.is_none());
                        assert!(matches!(ack.6, Attempt::Unattempted) && ack.8.is_empty());
                    }
                    ProviderFault::ProtectionDisposal => {
                        unreachable!("the outer exhaustive branch excludes protection failure")
                    }
                }
                assert_eq!(receiver_count, 1);
                assert_eq!(
                    (
                        receiver_verifications.get(),
                        receiver_constructions.get(),
                        receiver_encodings.get(),
                        receiver_protections.get()
                    ),
                    (1, 1, 1, 1)
                );
                assert_eq!(registry.len(), 2);
                let [receiver_row] = <[_; 1]>::try_from(receiver_rows)
                    .unwrap_or_else(|_| panic!("one complete original incoming Request remains"));
                let (
                    key,
                    raw,
                    verified,
                    input,
                    native,
                    encoding,
                    protection,
                    publications,
                    refusals,
                    panics,
                ) = receiver_row;
                let verified = verified
                    .expect("original incoming verification result")
                    .unwrap_or_else(|_| panic!("actual finite incoming verification succeeds"));
                let receiver_scope = recorded::projection(&verified);
                assert_eq!(
                    (receiver_scope.1, receiver_scope.0.kind),
                    (reader.scope, Purpose::Request)
                );
                assert_eq!(raw.0.as_ref(), request_bytes.as_ref());
                assert_eq!(raw.1.as_ref(), request_proof.as_ref());
                assert!(key.is_some() && input.is_none());
                assert!(
                    matches!(refusals, (None, None, None))
                        && matches!(panics, (None, None, None, None, None, None))
                );
                let (constructor, client, admission, counter_reply, tail) =
                    native.expect("actual native read custody");
                assert!(
                    constructor.is_none()
                        && matches!(client, Attempt::Returned(Ok(_)))
                        && matches!(admission, Attempt::Returned(Ok(())))
                );
                let original = counter_reply
                    .expect("the actual native receive returns")
                    .expect("the real Counter read replies");
                assert_eq!((original.from, original.message), (counter_address, 1));
                assert!(matches!(tail.as_slice(), [None]));
                assert!(
                    encoding.as_ref().is_some_and(Result::is_ok)
                        && protection.as_ref().is_some_and(Result::is_ok)
                );
                assert_eq!(publications.len(), 1);
                assert!(publications.iter().all(Result::is_ok));
                drop((receipt, cause, verification_original));
            }
        }
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].0, reader_address);
        drop((
            protection_original,
            completed,
            source,
            inputs,
            recipient,
            declarations,
            target,
            actual_counter,
        ));
    }
}
