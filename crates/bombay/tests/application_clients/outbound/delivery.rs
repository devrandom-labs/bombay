//! A real actor read reaches its checked reply before joined native closure.
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Step, Stopped, User};
use bombay::prelude::Application;
use bombay::remote::{Client, ClientLimits};
use bombay::remote::{Export, ExportLimits, Metadata, reply};
use bombay::{ActorFailureAssessment, ApplicationScope, MailAddr, RetirementAssessment};
use std::cell::Cell;
use std::cell::RefCell;
use std::num::NonZeroUsize;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::large_futures,
    reason = "the complete two-Node public consumer retains concrete native outcomes without extra boxing"
)]
async fn actor_read_uses_the_node_owned_outgoing_contract() {
    tokio::time::timeout(Duration::from_secs(10), read())
        .await
        .expect("the complete joined comparison finishes before the native deadline");
}

#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    clippy::await_holding_refcell_ref,
    reason = "one causal two-Node trace preserves exact rejected requests and immutable configured claims through the actual borrowed protector"
)]
async fn read() {
    let address = protocol::reserved_address();
    let maximum = NonZeroUsize::new(4).unwrap();
    let records = &RefCell::new(Vec::<(Scope, ProtectedMessage)>::new());
    let claims = &RefCell::new(None::<Metadata>);
    let caller_identity = &RefCell::new(None::<String>);
    let encoded = &RefCell::new(Vec::new());
    let checked = &RefCell::new(Vec::new());
    let acknowledged = &RefCell::new(Vec::new());
    let reader_address = &Cell::new(None);
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
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest>(bytes),
        reply(
            |request: ReadRequest| match request.operation {
                CounterOperation::Read => Ok::<_, ReadRequest>(CounterMessage::Read),
            },
            |receipt: &recorded::Receipt, original: &User<MailAddr, u64>| {
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
                async move |bytes: &[u8], original: &User<MailAddr, CounterServiceInput>| {
                    match original.message {
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
                            .await
                        }
                        _ => Err(VerificationError::Invalid),
                    }
                },
                async |bytes: &[u8], proof: &[u8]| {
                    recorded::verify(
                        &ProtectedMessage {
                            protected: bytes.into(),
                            proof: proof.into(),
                        },
                        records,
                    )
                    .await
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
        let (_, _, _, source_address, caller_outcome, terminal) = outgoing;
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
        assert_eq!(
            reader.replies.as_slice(),
            [CounterProtectedMeaning::ReadReply(1)]
        );
        let encoded = encoded.borrow();
        let checked = checked.borrow();
        let acknowledged = acknowledged.borrow();
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0].0, reader_address);
        assert_eq!(checked.as_slice(), [(reader.scope, 1)]);
        assert_eq!(
            acknowledged.as_slice(),
            [(
                reader_address,
                23,
                requester::CounterProofPurpose::ReadReply
            )]
        );
        // Keep the complete terminal Client: its public original-fact extraction
        // product is deliberately not invented by this consumer comparison.
        drop((
            whole_client,
            recipient,
            rows,
            declarations,
            target,
            actual_counter,
        ));
    }
}
