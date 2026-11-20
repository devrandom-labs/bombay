//! A real actor read reaches its checked reply before joined native closure.
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Step, Stopped, User};
use bombay::prelude::Application;
use bombay::remote::{Attempt, Client, ClientLimits};
use bombay::remote::{Export, ExportLimits, Metadata, RefusalRetention, reply};
use bombay::{ActorFailureAssessment, ApplicationScope, MailAddr, RetirementAssessment};
use futures_util::future::{Either, join};
use std::cell::Cell;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

use super::super::ledger::{LedgerCommand, MailLedger};
use super::network as protocol;
use super::{Counter, CounterMessage, CounterValue, custody, providers, reader, requester};
use custody::completed_actor;
use providers::{
    CounterOperation, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, limits, logical_scope, metadata, recorded,
};
use reader::{Reader, ReaderReply};
use requester::{CounterProtectedMeaning, CounterRequests, CounterServiceInput, Scope};

#[derive(Debug, PartialEq, Eq)]
enum Incompatibility {
    Wire {
        required: u64,
        actual: u64,
    },
    Schema {
        required: u64,
        actual: u64,
    },
    Profile {
        required: Box<str>,
        actual: Box<str>,
    },
}

fn compatible(
    actual: &Metadata,
    wire: u64,
    schema: u64,
    profile: &str,
) -> Result<(), Incompatibility> {
    if actual.wire_version != wire {
        return Err(Incompatibility::Wire {
            required: wire,
            actual: actual.wire_version,
        });
    }
    if actual.schema_version != schema {
        return Err(Incompatibility::Schema {
            required: schema,
            actual: actual.schema_version,
        });
    }
    if actual.provider_profile.as_ref() != profile {
        return Err(Incompatibility::Profile {
            required: profile.into(),
            actual: actual.provider_profile.clone(),
        });
    }
    Ok(())
}

#[test]
fn configured_compatibility_accepts_the_supported_pair() {
    check_compatibility(1, 1, "read-record-test-v1", None);
}

#[test]
fn configured_compatibility_refuses_an_unknown_wire_version() {
    check_compatibility(
        2,
        1,
        "read-record-test-v1",
        Some(Incompatibility::Wire {
            required: 2,
            actual: 1,
        }),
    );
}

#[test]
fn configured_compatibility_refuses_an_unknown_schema_version() {
    check_compatibility(
        1,
        2,
        "read-record-test-v1",
        Some(Incompatibility::Schema {
            required: 2,
            actual: 1,
        }),
    );
}

#[test]
fn configured_compatibility_refuses_a_different_provider_profile() {
    check_compatibility(
        1,
        1,
        "read-scheduled-test-v1",
        Some(Incompatibility::Profile {
            required: "read-scheduled-test-v1".into(),
            actual: "read-record-test-v1".into(),
        }),
    );
}

#[expect(
    clippy::large_futures,
    reason = "the finite two-Node test keeps its measured 20 KiB concrete future on the runtime thread stack, matching the existing delivery test"
)]
fn check_compatibility(wire: u64, schema: u64, profile: &str, refusal: Option<Incompatibility>) {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the existing two-worker runtime is available")
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(10),
                compatibility(wire, schema, profile, refusal),
            )
            .await
            .expect("the actual compatibility consumer and both native cleanups finish");
        });
}

#[expect(
    clippy::large_futures,
    clippy::too_many_lines,
    clippy::result_large_err,
    clippy::await_holding_refcell_ref,
    reason = "one real two-Node trace retains caller input, typed incompatibility, protected originals and both native outcomes before independent oracles"
)]
async fn compatibility(wire: u64, schema: u64, profile: &str, refusal: Option<Incompatibility>) {
    let maximum = NonZeroUsize::new(4).unwrap();
    let records = &RefCell::new(Vec::<(Scope, ProtectedMessage)>::new());
    let claims = &RefCell::new(None::<Metadata>);
    let caller_identity = &RefCell::new(None::<String>);
    let encoded = &RefCell::new(Vec::new());
    let constructed = &Cell::new(0);
    let verified = &Cell::new(0);
    let priming = RefCell::new(None);
    let (started, observed) = oneshot::channel();
    let started = RefCell::new(Some(started));
    let receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(protocol::configuration(
            "tls/localhost:0",
            &protocol::EndpointRole::Receiver,
        ))
        .await
        .unwrap_or_else(|_| panic!("the real compatible receiver opens"));
    let address = protocol::listener_address(receiver.listen_addresses().await);
    let receiver_id = receiver.runtime_id();
    let export = Export::new(
        "bombay/outbound/compatibility",
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
                CounterOperation::Read => {
                    constructed.set(constructed.get() + 1);
                    Ok::<_, ReadRequest>(CounterMessage::Read)
                }
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
            verified.set(verified.get() + 1);
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
        let target_address = application
            .actor_address(&target)
            .expect("the actual receiving Counter");
        let mut counter = application
            .client::<CounterValue>()
            .expect("independent ordinary Counter client");
        let increment = counter.send(CounterMessage::Increment).await;
        let admission = counter
            .send(CounterMessage::Read(counter.recipient()))
            .await;
        let original = counter.receive().await;
        *priming.borrow_mut() = Some((increment, admission, original));
        let published = started
            .borrow_mut()
            .take()
            .expect("one startup publication")
            .send((target_address, application.lifecycle(), counter));
        published.unwrap_or_else(|_| panic!("the independent caller retains startup receiving"));
        Ok::<_, VerificationError>(target)
    });
    let outgoing = async {
        let (target, lifecycle, mut counter) =
            observed.await.expect("the receiver is actually primed");
        let scope = ApplicationScope::new();
        let requests = scope.external::<CounterRequests>().unwrap();
        let mut logical = logical_scope();
        logical.identity.binding.actor_generation = target.0;
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
            .unwrap_or_else(|_| panic!("the real configured caller opens"));
        let caller_id = caller.runtime_id();
        *caller_identity.borrow_mut() = Some(caller_id.clone());
        let mut configured = metadata(ProviderProfile::Recorded, caller_id.clone(), receiver_id);
        configured.actor_generation = target.0;
        *claims.borrow_mut() = Some(configured);
        let client = Client::new(
            requests,
            "bombay/outbound/compatibility",
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
                if original.from != target || caller_identity.borrow().as_deref() != Some(runtime) {
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
                                .expect("configured receiver declarations"),
                            kind: Purpose::Request,
                            operation: CounterOperation::Read,
                        })
                        .map_err(VerificationError::Declaration)?
                        .into_boxed_slice();
                        encoded.borrow_mut().push(bytes.to_vec());
                        Ok(Some(bytes))
                    }
                    CounterServiceInput::ProtectedConsumed {
                        correlation,
                        purpose,
                    } if correlation == logical.identity.sequence
                        && purpose == requester::CounterProofPurpose::ReadReply =>
                    {
                        Ok(None)
                    }
                    _ => Err(VerificationError::Invalid),
                }
            },
            async move |bytes: &[u8], _: &User<MailAddr, CounterServiceInput>| {
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
                let (protected, actual) = recorded::projection(receipt);
                if protected.bombay != parsed.bombay
                    || claims.borrow().as_ref() != Some(&parsed.bombay)
                    || protected.kind != Purpose::ReadReply
                    || parsed.kind != Purpose::ReadReply
                    || parsed.operation != CounterOperation::Read
                    || actual != logical
                    || !matches!(original.message, CounterServiceInput::Verify(sequence) if sequence == logical.identity.sequence)
                {
                    return Err(VerificationError::Invalid);
                }
                Ok(ReaderReply {
                    scope: actual,
                    meaning: CounterProtectedMeaning::ReadReply(parsed.value),
                })
            },
            move |_: &User<MailAddr, CounterServiceInput>, _: &str, _: MailAddr| {
                compatible(
                    claims
                        .borrow()
                        .as_ref()
                        .expect("the trusted configured destination"),
                    wire,
                    schema,
                    profile,
                )
            },
            move |receipt: &recorded::Receipt,
                  original: &User<MailAddr, CounterServiceInput>,
                  runtime: &str,
                  target: MailAddr| {
                let (protected, actual) = recorded::projection(receipt);
                if original.from == target
                    && caller_identity.borrow().as_deref() == Some(runtime)
                    && actual == logical
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
            Ok::<_, bombay::remote::ExportError>(application.root().established_recipient())
        });
        let outcome = caller
            .run()
            .await
            .unwrap_or_else(|_| panic!("the configured caller starts normally"));
        let terminal = caller
            .into_parts()
            .unwrap_or_else(|_| panic!("the actual caller closes"));
        let admission = counter
            .send(CounterMessage::Read(counter.recipient()))
            .await;
        let observed = counter.receive().await;
        let shutdown = lifecycle.shutdown();
        let retirement = lifecycle.retirement().await;
        (
            target, outcome, terminal, admission, observed, shutdown, retirement,
        )
    };
    let (receiving, outgoing) = join(receiver.run(), outgoing).await;
    let receiving = receiving.unwrap_or_else(|_| panic!("the real receiver ran"));
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
        receiver
            .into_parts()
            .unwrap_or_else(|_| panic!("all native receiver owners joined"));
    let (work, report, native, notifications, ()) = receiving.into_parts();
    let (_, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the receiver has genuine Work absence"));
    let report = report.expect("the receiver retirement report");
    let (origin, native) = native.expect("the actual receiver state");
    let _counter = completed_actor(origin, native);
    let notifications = notifications.expect("both receiver publications");
    notifications
        .termination
        .expect("receiver termination publication");
    notifications
        .retirement
        .expect("receiver retirement publication");
    let (_, _, startup, withdrawn, diagnostics, rows) = export.into_parts();
    let receiver_bound = bound.expect("the receiver bound");
    let (target, caller_outcome, terminal, admission, observed, shutdown, retirement) = outgoing;
    let (
        client,
        caller_bound,
        caller_closed,
        caller_polling,
        caller_disposal,
        caller_withdrawal_polling,
        caller_withdrawal_disposal,
    ) = terminal;
    let (work, caller_report, native, notifications, ()) = caller_outcome.into_parts();
    let (_, caller_cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the Reader has genuine Work absence"));
    let (origin, native) = native.expect("the actual Reader state");
    let caller_address = origin.address();
    let retained = completed_actor(origin, native);
    let reader = retained.base();
    let notifications = notifications.expect("both caller publications");
    notifications
        .termination
        .expect("caller termination publication");
    notifications
        .retirement
        .expect("caller retirement publication");
    let (source, _, started, withdrawn_client, joined, _, requests, routes) = client.into_parts();
    let caller_target = caller_bound
        .expect("the caller bound")
        .expect("the caller association");
    let (listener, visible, listener_withdrawal) = routes;
    assert!(cleanup.is_ok() && caller_cleanup.is_ok() && closed.is_ok() && caller_closed.is_ok());
    assert!(
        polling.is_none()
            && disposal.is_none()
            && withdrawal_polling.is_none()
            && withdrawal_disposal.is_none()
    );
    assert!(
        caller_polling.is_none()
            && caller_disposal.is_none()
            && caller_withdrawal_polling.is_none()
            && caller_withdrawal_disposal.is_none()
    );
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(caller_report, Some(report));
    assert_eq!(retirement, report);
    assert!(shutdown.is_ok() && admission.is_ok());
    assert!(receiver_bound.is_ok());
    assert!(
        matches!(startup, Some(Ok(())))
            && matches!(withdrawn, Some(Ok(())))
            && diagnostics.is_empty()
    );
    assert!(
        matches!(started, Some(Ok(())))
            && matches!(withdrawn_client, Some(Ok(())))
            && matches!(joined, Some(Ok(())))
    );
    assert!(matches!(listener, Some(Ok(()))) && matches!(listener_withdrawal, Some(Ok(()))));
    let (increment, before_admission, before) =
        priming.into_inner().expect("the priming originals remain");
    let before = before.expect("the independent initial Counter Read");
    let after = observed.expect("the independent final Counter Read");
    assert!(increment.is_ok() && before_admission.is_ok());
    assert_eq!((before.from, before.message), (target, 1));
    assert_eq!((after.from, after.message), (target, 1));
    let mut requests = requests.into_iter();
    let (
        original,
        failure,
        encoding,
        protection,
        permission,
        capacity,
        attempt,
        callback_refusal,
        responses,
        callback_eof,
        protection_panics,
        operation_panics,
        completed,
    ) = requests.next().expect("the actual initial Reader request");
    assert_eq!(original.from, caller_address);
    assert!(matches!(original.message, CounterServiceInput::Verify(23)));
    assert!(
        failure.is_none() && capacity.is_none(),
        "configured ({wire}, {schema}, {profile}) request failure={failure:?}, capacity={capacity:?}, completed={completed:?}"
    );
    assert!(
        protection_panics.0.is_none()
            && protection_panics.1.is_none()
            && protection_panics.2.is_none()
    );
    assert!(
        operation_panics.0.is_none()
            && operation_panics.1.is_none()
            && operation_panics.2.is_none()
    );
    if let Some(expected) = refusal {
        assert_eq!(permission.as_ref(), Some(&expected));
        assert!(encoding.is_none() && protection.is_none());
        assert!(
            matches!(attempt, Attempt::Unattempted)
                && callback_refusal.is_none()
                && callback_eof.is_none()
        );
        let remaining = requests.next();
        assert!(responses.is_empty() && remaining.is_none());
        assert!(encoded.borrow().is_empty() && records.borrow().is_empty() && rows.is_empty());
        assert_eq!((verified.get(), constructed.get()), (0, 0));
        assert_eq!(reader.replies, [] as [CounterProtectedMeaning; 0]);
    } else {
        assert!(permission.is_none() && matches!(attempt, Attempt::Returned(Ok(()))));
        assert!(callback_refusal.is_none() && callback_eof.is_some());
        assert!(matches!(encoding, Some(Ok(Some(_)))) && matches!(protection, Some(Ok(_))));
        assert!(matches!(visible, Some(Ok(()))));
        assert_eq!((verified.get(), constructed.get(), rows.len()), (1, 1, 1));
        assert_eq!(
            reader.replies.as_slice(),
            [CounterProtectedMeaning::ReadReply(1)]
        );
        assert_eq!(encoded.borrow().len(), 1);
        let records = records.borrow();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].1.protected.as_ref(),
            encoded.borrow()[0].as_slice()
        );
        assert_eq!(records[0].1.proof.as_ref(), [7, 11]);
        assert_eq!(records[1].1.proof.as_ref(), [13, 17]);
        let [response] = <[_; 1]>::try_from(responses)
            .unwrap_or_else(|_| panic!("one actual checked native reply remains"));
        let native_bytes = response.payload().flatten().copied().collect::<Vec<_>>();
        let native_proof = response
            .proof()
            .expect("the actual native reply proof")
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let (cause, wire, shape, verified, decoded, permission, panics) = response.into_parts();
        let wire = wire.expect("the exact protected reply remains");
        let receipt = verified
            .expect("the real reply provider returned")
            .unwrap_or_else(|_| panic!("the real reply verified"));
        assert!(cause.is_none() && matches!(shape, Some(Ok(()))) && permission.is_none());
        assert!(panics.0.is_none() && panics.1.is_none() && panics.2.is_none());
        assert!(matches!(
            decoded,
            Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
        ));
        assert_eq!(wire.0.as_ref(), native_bytes.as_slice());
        assert_eq!(wire.1.as_ref(), native_proof.as_slice());
        assert_eq!(wire.0.as_ref(), records[1].1.protected.as_ref());
        assert_eq!(wire.1.as_ref(), records[1].1.proof.as_ref());
        assert_eq!(recorded::projection(&receipt).1, reader.scope);
        let acknowledgment = requests
            .next()
            .expect("the real Reader acknowledges its reply");
        assert!(matches!(
            acknowledgment.0.message,
            CounterServiceInput::ProtectedConsumed {
                correlation: 23,
                purpose: requester::CounterProofPurpose::ReadReply
            }
        ));
        assert!(
            matches!(acknowledgment.2, Some(Ok(None)))
                && matches!(acknowledgment.6, Attempt::Unattempted)
        );
        let remaining = requests.next();
        assert!(remaining.is_none());
    }
    drop((source, caller_target));
}

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
    let address = "tls/localhost:0".to_owned();
    let maximum = NonZeroUsize::new(4).unwrap();
    let records = &RefCell::new(Vec::<(Scope, ProtectedMessage)>::new());
    let claims = &RefCell::new(None::<Metadata>);
    let caller_identity = &RefCell::new(None::<String>);
    let encoded = &RefCell::new(Vec::new());
    let checked = &RefCell::new(Vec::new());
    let acknowledged = &RefCell::new(Vec::new());
    let reader_address = &Cell::new(None);
    let priming = RefCell::new(None);
    let local_original: Box<[u8]> = [127, 131].into();
    let local_allocation = local_original.as_ptr();
    let local_delivery = RefCell::new(None);
    let (started, observed) = oneshot::channel();
    let started = RefCell::new(Some(started));
    let receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(protocol::configuration(
            &address,
            &protocol::EndpointRole::Receiver,
        ))
        .await
        .unwrap_or_else(|_| panic!("the actual receiving Counter Node opens"));
    let address = protocol::listener_address(receiver.listen_addresses().await);
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
        let mut local = application
            .external::<MailLedger>()
            .expect("an unrelated ordinary local protocol needs no Serde or export");
        let local_address = local.address();
        let admission = local
            .send(&local.recipient(), LedgerCommand::Append(local_original))
            .await;
        let original = local.receive().await;
        local.close_admission();
        let tail = local.receive().await;
        *local_delivery.borrow_mut() = Some((local_address, admission, original, tail));
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
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
        receiver
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
    let (local_address, local_admission, local_received, local_tail) = local_delivery
        .into_inner()
        .expect("the local endpoint retains its actual admission and receives");
    let User { from, message } = local_received.expect("the actual local command arrives");
    let LedgerCommand::Append(local_original) = message else {
        panic!("the unrelated local endpoint retains the original Append command");
    };
    assert!(local_admission.is_ok() && local_tail.is_none());
    assert_eq!(from, local_address);
    assert_eq!(local_original.as_ref(), [127, 131]);
    assert_eq!(local_original.as_ptr(), local_allocation);
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
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
        let (
            whole_client,
            bound,
            caller_closed,
            polling,
            disposal,
            withdrawal_polling,
            withdrawal_disposal,
        ) = terminal;
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
        assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
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
        let (source, inputs, startup, withdrawal, joined, source_eof, outgoing_rows, routes) =
            whole_client.into_parts();
        let (listener, observed, listener_withdrawal) = routes;
        assert!(
            matches!(listener, Some(Ok(())))
                && matches!(observed, Some(Ok(())))
                && matches!(listener_withdrawal, Some(Ok(())))
        );
        // Keep all original terminal products through the route observations.
        drop((
            source,
            inputs,
            startup,
            withdrawal,
            joined,
            source_eof,
            outgoing_rows,
            recipient,
            rows,
            declarations,
            target,
            actual_counter,
        ));
    }
}
