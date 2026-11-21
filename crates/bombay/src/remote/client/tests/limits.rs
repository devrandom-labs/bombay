//! Distinct active-operation, Source-row and native-response count policies.
use super::super::{Client, ClientLimits, Request};
use crate::application::{ApplicationScope, MailboxCapacity};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::export::Exports;
use crate::remote::{
    Attempt, ConnectionEndpoints, CredentialSource, IngressError, Metadata, Network, NetworkLayout,
};
use crate::{MailAddr, SendError};
use behavior::{EstablishedRecipient, MessageProtocol, Never, User};
use futures_util::FutureExt;
use futures_util::future::Either;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn, ready};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{Arc, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::mpsc::{Receiver, error::TrySendError};
use tokio::sync::{Semaphore, TryAcquireError, oneshot};
use tokio::task::unconstrained;
use zenoh::bytes::ZBytes;
use zenoh::handlers::FifoChannel;
use zenoh::query::Reply as NativeReply;
use zenoh::sample::Locality;
use zenoh_config::{Config, ModeDependentValue, WhatAmI};

#[path = "../../../../../../examples/counter/src/counter.rs"]
#[expect(
    clippy::duplicate_mod,
    dead_code,
    reason = "the capacity trace compiles the same nominal Counter protocol without its separate increment application"
)]
mod counter;
use counter::{Counter, CounterMessage, CounterValue};
#[path = "../../../../tests/protected_counter/providers.rs"]
#[expect(
    clippy::duplicate_mod,
    dead_code,
    reason = "the capacity trace uses the existing finite Recorded policy while the shared fixture also owns Scheduled controls"
)]
mod providers;
#[path = "../../../../tests/protected_counter/requester.rs"]
#[expect(
    clippy::duplicate_mod,
    dead_code,
    reason = "the capacity trace reuses the exact nominal protocol without its separate admission actor"
)]
mod requester;
use providers::{
    CounterOperation, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, limits, logical_scope, metadata, recorded,
};
use requester::Scope;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_active_request_keeps_two_source_rows_and_three_native_responses() {
    tokio::time::timeout(Duration::from_secs(10), retain_requests(2, 3))
        .await
        .expect("the complete actual native count trace joins before query expiry");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_active_request_keeps_four_source_rows_and_two_native_responses() {
    tokio::time::timeout(Duration::from_secs(10), retain_requests(4, 2))
        .await
        .expect("the complete actual native count trace joins before query expiry");
}

#[expect(
    clippy::too_many_lines,
    reason = "one finite native trace acquires cleanup, all original rows and replies before independent count oracles"
)]
async fn retain_requests(retained: usize, responses: usize) {
    let mut configuration = Config::default();
    configuration
        .set_mode(Some(WhatAmI::Peer))
        .expect("the private native fixture selects Peer");
    configuration
        .set_queries_max_retained(NonZeroUsize::new(retained + 1))
        .expect("native timeout ownership has capacity for every finite source input");
    configuration
        .connect
        .set_endpoints(ModeDependentValue::Unique(Vec::new()))
        .expect("no outgoing native endpoint");
    configuration
        .listen
        .set_endpoints(ModeDependentValue::Unique(Vec::new()))
        .expect("no listening native endpoint");
    configuration
        .scouting
        .multicast
        .set_enabled(Some(false))
        .expect("no multicast scouting");
    configuration
        .scouting
        .multicast
        .set_listen(Some(ModeDependentValue::Unique(false)))
        .expect("no multicast listening");
    configuration
        .scouting
        .gossip
        .set_enabled(Some(false))
        .expect("no gossip scouting");
    let session = zenoh::open(configuration)
        .await
        .expect("the actual local native Session opens");
    let runtime = session.zid().to_string();
    let key = "bombay/outbound/distinct-limits";
    let queryable = session
        .declare_queryable(key)
        .allowed_origin(Locality::SessionLocal)
        .await
        .expect("the actual native receiving Queryable opens");
    let scope = ApplicationScope::new().client_capacity(
        MailboxCapacity::new(retained + 1).expect("all original inputs fit before serving"),
    );
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .expect("the real outgoing Source allocates");
    let target = scope
        .external::<MessageProtocol<MailAddr, ()>>()
        .expect("the real independent return endpoint allocates");
    let source_address = source.address();
    let target_address = target.address();
    let target_endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let registries = (0..=retained)
        .map(|_| RefCell::new(Vec::<(Scope, ProtectedMessage)>::new()))
        .collect::<Vec<_>>();
    let mut originals = Vec::new();
    let mut admitted = Vec::new();
    for sequence in 0..=retained {
        let mut claims = metadata(ProviderProfile::Recorded, "caller".into(), runtime.clone());
        claims.actor_generation = target_address.0;
        claims.request_sequence = u64::try_from(sequence).expect("the finite source sequence fits");
        let original = serde_json::to_vec(&ReadRequestRef {
            bombay: &claims,
            kind: Purpose::Request,
            operation: CounterOperation::Read,
        })
        .expect("the actual declared read input encodes")
        .into_boxed_slice();
        originals.push((original.as_ptr(), original.to_vec()));
        admitted.push(target.send(&source.recipient(), original).await);
    }

    let selected = RefCell::new(Vec::new());
    let protecting = Cell::new(0usize);
    let peak = Cell::new(0usize);
    let protection_calls = RefCell::new(Vec::new());
    let decoding_calls = Cell::new(0usize);
    let authority = RefCell::new(target_address);
    let (release, released) = oneshot::channel();
    let released = released.shared();
    let (publication, joined) = oneshot::channel();
    let mut client = Client::new(
        source,
        key,
        ClientLimits {
            concurrent_requests: NonZeroUsize::new(1).unwrap(),
            retained_requests: NonZeroUsize::new(retained).unwrap(),
            retained_responses: NonZeroUsize::new(responses).unwrap(),
        },
        limits(),
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            let request: ReadRequest = serde_json::from_slice(&original.message)
                .map_err(VerificationError::Declaration)?;
            if actual_runtime != runtime
                || actual_target != target_address
                || original.from != target_address
                || request.kind != Purpose::Request
                || request.operation != CounterOperation::Read
            {
                return Err(VerificationError::Invalid);
            }
            selected.borrow_mut().push(request.bombay.request_sequence);
            Ok(Some(original.message.to_vec().into_boxed_slice()))
        },
        async |bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            let request: ReadRequest =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            let position = usize::try_from(request.bombay.request_sequence)
                .map_err(|_| VerificationError::Invalid)?;
            protection_calls
                .borrow_mut()
                .push((request.bombay.request_sequence, original.message.as_ptr()));
            protecting.set(protecting.get() + 1);
            peak.set(peak.get().max(protecting.get()));
            // Every actual protector waits, so the first serve poll exposes P
            // without letting a wrong P start a native Get before the snapshot.
            let released = released.clone().await;
            protecting.set(protecting.get() - 1);
            released.map_err(VerificationError::Unavailable)?;
            let mut logical = logical_scope();
            logical.identity.sequence = request.bombay.request_sequence;
            logical.identity.binding.actor_generation = target_address.0;
            recorded::protect_request(bytes, &request.bombay, logical, &registries[position]).await
        },
        async |bytes: &[u8], proof: &[u8]| {
            let response: ReadReply =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            let position = usize::try_from(response.bombay.request_sequence)
                .map_err(|_| VerificationError::Invalid)?;
            recorded::verify(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &registries[position],
            )
            .await
        },
        |_: &recorded::Receipt, _: &[u8], _: &User<MailAddr, Box<[u8]>>| {
            decoding_calls.set(decoding_calls.get() + 1);
            Err::<(), _>(VerificationError::Invalid)
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            let authority = authority.borrow();
            if original.from == *authority
                && actual_target == *authority
                && actual_runtime == runtime
            {
                Ok(authority)
            } else {
                Err("the declared request route is not this return endpoint")
            }
        },
        |_: &recorded::Receipt, _: &User<MailAddr, Box<[u8]>>, _: &str, _: MailAddr| {
            Err::<(), _>("the count control retains checked replies without actor delivery")
        },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::declare(&mut client, &session)
    .await;
    let (
        first_poll,
        initially_selected,
        initially_protecting,
        initially_entered,
        release_receipt,
        received,
    ) = {
        // A previous test's cooperative budget must not truncate this initial poll.
        // At most four acquired rows run; every protector then waits on the gate.
        let mut serving = pin!(unconstrained(
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, ()>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
            >>::serve(
                &mut client,
                (target_endpoint, target_address),
                runtime.clone(),
                joined.shared()
            )
        ));
        let first_poll = poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await;
        let initially_selected = selected.borrow().clone();
        let initially_protecting = protecting.get();
        let initially_entered = protection_calls.borrow().len();
        // Release precedes every law assertion, including all source substitutions.
        let release_receipt = release.send(());
        let received = match first_poll {
            Poll::Pending => {
                let native = async {
                    let mut received = Vec::new();
                    // This is the actual observed acquisition count, not expected N.
                    // It keeps an N/R substitution finite; final expectations stay N.
                    for _ in 0..initially_selected.len() {
                        let query = queryable
                            .recv_async()
                            .await
                            .expect("each actually selected request reaches the native Queryable");
                        let raw = query
                            .payload()
                            .expect("the real Get owns its encoded request")
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice();
                        let proof = query
                            .attachment()
                            .expect("the real Get owns its protection proof")
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice();
                        let request: ReadRequest =
                            serde_json::from_slice(&raw).expect("the actual finite request parses");
                        let position = usize::try_from(request.bombay.request_sequence)
                            .expect("the finite supplied sequence fits");
                        let receipt = recorded::verify(
                            &ProtectedMessage {
                                protected: raw.clone(),
                                proof: proof.clone(),
                            },
                            &registries[position],
                        )
                        .await
                        .unwrap_or_else(|_| {
                            panic!("the actual full registered request pair verifies")
                        });
                        let bytes = serde_json::to_vec(&ReadReplyRef {
                            bombay: &recorded::projection(&receipt).0.bombay,
                            kind: Purpose::ReadReply,
                            operation: CounterOperation::Read,
                            value: &request.bombay.request_sequence,
                        })
                        .expect("the actual protected read response encodes")
                        .into_boxed_slice();
                        let response_proof =
                            recorded::protect(&bytes, &receipt, &registries[position])
                                .await
                                .unwrap_or_else(|_| {
                                    panic!("the actual second finite pair registers")
                                });
                        let mut published = Vec::new();
                        for _ in 0..=responses {
                            // Distinct actual native envelopes carry one registered
                            // pair; proof success is not a placeholder constant.
                            published.push(
                                query
                                    .reply(key, bytes.to_vec())
                                    .attachment(response_proof.to_vec())
                                    .await,
                            );
                        }
                        drop(query); // Actual FINAL, not a predicted timeout.
                        received.push((position, raw, proof, bytes, response_proof, published));
                    }
                    // Last reply-policy completion is not assumed here. Actual
                    // retirement must acquire remaining native originals and EOF.
                    let joined = publication.send(());
                    (received, joined)
                };
                let ((), received) = tokio::join!(serving.as_mut(), native);
                received
            }
            Poll::Ready(()) => {
                let joined = publication.send(());
                (Vec::new(), joined)
            }
        };
        (
            first_poll,
            initially_selected,
            initially_protecting,
            initially_entered,
            release_receipt,
            received,
        )
    };
    <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::withdraw(&mut client)
    .await;
    let withdrawn = queryable.undeclare().await;
    let closed = session.close().await;
    // Inspect the actual typed owner before the public consuming handoff boxes
    // its cause. The original and native cause remain in that owner unchanged.
    let callback_observations = client
        .requests
        .borrow()
        .iter()
        .map(|request| {
            let refusal = request
                .callback_refusal
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            refusal.as_ref().map(|refusal| match refusal {
                IngressError::Capacity { original, cause } => {
                    let cause = match cause {
                        TryAcquireError::NoPermits => TryAcquireError::NoPermits,
                        TryAcquireError::Closed => TryAcquireError::Closed,
                    };
                    let original = original
                        .result()
                        .map(|sample| {
                            (
                                sample.payload().to_bytes().into_owned().into_boxed_slice(),
                                sample
                                    .attachment()
                                    .map(|proof| proof.to_bytes().into_owned().into_boxed_slice()),
                            )
                        })
                        .map_err(|cause| {
                            cause.payload().to_bytes().into_owned().into_boxed_slice()
                        });
                    Either::Left((original, cause))
                }
                IngressError::Queue { cause } => Either::Right(cause.to_string()),
            })
        })
        .collect::<Vec<_>>();
    let (mut source, _inputs, startup, withdrawal, joined, source_eof, rows, _routes) =
        client.into_parts();
    let mut prefix = Vec::new();
    let mut prefix_eof = None;
    if source_eof.is_none() {
        while let Some(original) = source.receive().await {
            prefix.push(original);
        }
        prefix_eof = Some(());
    }
    let (received, joined_publication) = received;
    let last_query = received.last().map(|native| native.0);
    let protection_calls = protection_calls.borrow();

    assert!(admitted.iter().all(Result::is_ok));
    assert!(declared.is_ok() && withdrawn.is_ok() && closed.is_ok());
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(
        matches!(joined, Some(Ok(()))) && joined_publication.is_ok() && release_receipt.is_ok()
    );
    assert!(first_poll.is_pending());
    assert_eq!(source.address(), source_address);
    assert_eq!(initially_selected.len(), retained);
    assert_eq!(
        (initially_protecting, initially_entered, peak.get()),
        (1, 1, 1)
    );
    assert_eq!(rows.len(), retained);
    assert!(source_eof.is_none() && prefix_eof.is_some());
    let [remaining]: [_; 1] = prefix
        .try_into()
        .unwrap_or_else(|_| panic!("only the one accepted input beyond N remains unread"));
    assert_eq!(remaining.from, target_address);
    assert_eq!(remaining.message.as_ptr(), originals[retained].0);
    assert_eq!(remaining.message.as_ref(), originals[retained].1.as_slice());
    assert_eq!(received.len(), retained);
    assert_eq!(protection_calls.len(), retained);
    assert_eq!(protecting.get(), 0);
    assert_eq!(decoding_calls.get(), 0);
    for (position, row) in rows.into_iter().enumerate() {
        let (
            original,
            refusal,
            encoded,
            protected,
            permission,
            capacity,
            query,
            callback,
            replies,
            eof,
            protection_panics,
            operation_panics,
            completed,
        ) = row;
        let sequence = u64::try_from(position).expect("the finite original sequence fits");
        assert_eq!(original.from, target_address);
        assert_eq!(original.message.as_ptr(), originals[position].0);
        assert_eq!(original.message.as_ref(), originals[position].1.as_slice());
        assert!(refusal.is_none() && permission.is_none() && capacity.is_none());
        assert!(matches!(query, Attempt::Returned(Ok(()))) && eof.is_some());
        if Some(position) != last_query {
            // With P1, the next actual Get proves this prior permit was released
            // after its real response loop reached EOF and returned.
            assert!(completed.is_some());
        }
        assert!(
            matches!(protection_panics, (None, None, None))
                && matches!(operation_panics, (None, None, None))
        );
        let native = received
            .iter()
            .find(|native| native.0 == position)
            .expect("the native Query independently identifies this supplied sequence");
        assert_eq!(native.1.as_ref(), originals[position].1.as_slice());
        assert!(native.5.iter().all(Result::is_ok));
        assert_eq!(native.5.len(), responses + 1);
        let encoded = encoded
            .expect("actual encoding returned")
            .unwrap_or_else(|_| panic!("the real request encodes"))
            .expect("this actual input requests a Get");
        let protected = protected
            .expect("actual protection returned")
            .unwrap_or_else(|_| panic!("the real finite request pair registers"));
        assert_eq!(encoded.as_ref(), native.1.as_ref());
        assert_eq!(protected.as_ref(), native.2.as_ref());
        // The native semaphore spans the queue AND acquired Response owners.
        // Wrong R can leave fewer/more replies or no first-refusal original.
        assert_eq!(replies.len(), responses);
        assert!(callback.is_some());
        let Some(Either::Left((refused, cause))) = &callback_observations[position] else {
            panic!("the actual first refusal retains its count cause before queue transfer");
        };
        let (refused, refused_proof) = refused
            .as_ref()
            .unwrap_or_else(|_| panic!("the refused original is a real success Sample"));
        let refused_proof = refused_proof
            .as_ref()
            .expect("the original refused proof remains");
        assert!(matches!(cause, TryAcquireError::NoPermits));
        assert_eq!(refused.as_ref(), native.3.as_ref());
        assert_eq!(refused_proof.as_ref(), native.4.as_ref());
        for response in replies {
            let payload = response.payload().flatten().copied().collect::<Vec<_>>();
            let proof = response
                .proof()
                .expect("the actual response owns its proof")
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            let (cause, wire, shape, verified, decoded, permission, panics) = response.into_parts();
            assert_eq!(payload.as_slice(), native.3.as_ref());
            assert_eq!(proof.as_slice(), native.4.as_ref());
            assert!(cause.is_none() && decoded.is_none());
            assert!(matches!(panics, (None, None, None)));
            match verified {
                Some(Ok(receipt)) => {
                    let verified = recorded::projection(&receipt);
                    assert_eq!(verified.0.bombay.request_sequence, sequence);
                    assert_eq!(verified.1.identity.sequence, sequence);
                    assert!(matches!(shape, Some(Ok(()))));
                    assert_eq!(
                        permission,
                        Some("the count control retains checked replies without actor delivery")
                    );
                    let wire = wire.expect("a processed response retains its actual wire pair");
                    assert_eq!(wire.0.as_ref(), payload.as_slice());
                    assert_eq!(wire.1.as_ref(), proof.as_slice());
                }
                None => {
                    // Last-query native custody can be acquired by retirement;
                    // no provider completion is fabricated after joined.
                    assert_eq!(Some(position), last_query);
                    assert!(completed.is_none());
                    assert!(wire.is_none() && shape.is_none() && permission.is_none());
                }
                Some(Err(_)) => {
                    panic!("every processed native reply matches its registered full pair")
                }
            }
        }
        let registry = registries[position].borrow();
        assert_eq!(registry.len(), 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_native_reply_keeps_the_second_request_waiting() {
    held_replies(ReplyTraffic::Single).await;
}

#[test]
fn held_reply_consumer_bounds_a_valid_tls_burst_without_blocking_health() {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the finite reply consumer owns its runtime")
        .block_on(held_replies(ReplyTraffic::Burst));
}

#[derive(Clone, Copy)]
enum ReplyTraffic {
    Single,
    Burst,
}

#[expect(
    clippy::too_many_lines,
    reason = "one permit trace retains both actual request futures, native originals and cleanup before the saved pre-release observations"
)]
async fn held_replies(traffic: ReplyTraffic) {
    const HEALTH: &str = "bombay/outbound/reply-health";
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut configuration = Config::default();
        configuration.set_mode(Some(WhatAmI::Peer)).unwrap();
        configuration
            .set_queries_max_retained(NonZeroUsize::new(2))
            .unwrap();
        configuration
            .connect
            .set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .unwrap();
        configuration
            .listen
            .set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .unwrap();
        configuration
            .scouting
            .multicast
            .set_enabled(Some(false))
            .unwrap();
        configuration
            .scouting
            .multicast
            .set_listen(Some(ModeDependentValue::Unique(false)))
            .unwrap();
        configuration
            .scouting
            .gossip
            .set_enabled(Some(false))
            .unwrap();
        let (session, peer) = match traffic {
            ReplyTraffic::Single => (
                zenoh::open(configuration)
                    .await
                    .expect("the actual native Session opens"),
                None,
            ),
            ReplyTraffic::Burst => {
                let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
                let endpoints = |address: String, identity: &str| ConnectionEndpoints {
                    addresses: vec![address],
                    certificate: CredentialSource::File(
                        certificates.join(format!("{identity}-certificate.pem")),
                    ),
                    private_key: CredentialSource::File(
                        certificates.join(format!("{identity}-private-key.pem")),
                    ),
                };
                let network = |layout| Network {
                    layout,
                    deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
                    max_retained_queries: NonZeroUsize::new(3).unwrap(),
                };
                let receiving = zenoh::open(
                    network(NetworkLayout::Peer {
                        connect: None,
                        listen: Some(endpoints("localhost:0".into(), "recipient")),
                    })
                    .to_configuration()
                    .unwrap(),
                )
                .await
                .expect("the actual TLS reply peer opens");
                let [locator]: [_; 1] = receiving
                    .info()
                    .locators()
                    .await
                    .try_into()
                    .expect("the peer owns one actual bound TLS endpoint");
                let caller = zenoh::open(
                    network(NetworkLayout::Peer {
                        connect: Some(endpoints(locator.address().as_str().to_owned(), "caller")),
                        listen: None,
                    })
                    .to_configuration()
                    .unwrap(),
                )
                .await
                .expect("the actual TLS Client Session opens");
                (caller, Some(receiving))
            }
        };
        let receiving = peer.as_ref().unwrap_or(&session);
        let locality = match traffic {
            ReplyTraffic::Single => Locality::SessionLocal,
            ReplyTraffic::Burst => Locality::Remote,
        };
        let runtime = session.zid().to_string();
        let key = "bombay/outbound/held-native-permit";
        let queryable = receiving
            .declare_queryable(key)
            .allowed_origin(locality)
            .await
            .expect("the actual local Queryable opens");
        let health = match traffic {
            ReplyTraffic::Single => None,
            ReplyTraffic::Burst => Some(
                session
                    .declare_queryable(HEALTH)
                    .allowed_origin(Locality::Remote)
                    .with(FifoChannel::new(1))
                    .await
                    .expect("the actual Client Session owns the independent health callback"),
            ),
        };
        let scope = ApplicationScope::new().client_capacity(
            MailboxCapacity::new(match traffic {
                ReplyTraffic::Single => 2,
                ReplyTraffic::Burst => 3,
            })
            .unwrap(),
        );
        let source = scope
            .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
            .unwrap();
        let mut target = scope.external::<MessageProtocol<MailAddr, u64>>().unwrap();
        let source_address = source.address();
        let target_address = target.address();
        let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        let registries = [RefCell::new(Vec::new()), RefCell::new(Vec::new())];
        let mut originals = Vec::new();
        let mut admitted = Vec::new();
        for sequence in 0..2u64 {
            let mut claims = metadata(ProviderProfile::Recorded, "caller".into(), runtime.clone());
            claims.actor_generation = target_address.0;
            claims.request_sequence = sequence;
            let original = serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: CounterOperation::Read,
            })
            .unwrap()
            .into_boxed_slice();
            originals.push((original.as_ptr(), original.to_vec()));
            admitted.push(target.send(&source.recipient(), original).await);
        }
        let authority = RefCell::new(target_address);
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let reply_gate = RefCell::new(match traffic {
            ReplyTraffic::Single => None,
            ReplyTraffic::Burst => Some((entered, released)),
        });
        let mut client = Client::new(
            source,
            key,
            ClientLimits {
                concurrent_requests: NonZeroUsize::new(1).unwrap(),
                retained_requests: NonZeroUsize::new(2).unwrap(),
                retained_responses: NonZeroUsize::new(1).unwrap(),
            },
            limits(),
            Duration::from_secs(20),
            |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
                let request: ReadRequest = serde_json::from_slice(&original.message)
                    .map_err(VerificationError::Declaration)?;
                if original.from != target_address
                    || actual_target != target_address
                    || actual_runtime != runtime
                    || request.kind != Purpose::Request
                    || request.operation != CounterOperation::Read
                {
                    return Err(VerificationError::Invalid);
                }
                Ok(Some(original.message.to_vec().into_boxed_slice()))
            },
            async |bytes: &[u8], _: &User<MailAddr, Box<[u8]>>| {
                let request: ReadRequest =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                let position = usize::try_from(request.bombay.request_sequence)
                    .map_err(|_| VerificationError::Invalid)?;
                let mut logical = logical_scope();
                logical.identity.sequence = request.bombay.request_sequence;
                logical.identity.binding.actor_generation = target_address.0;
                recorded::protect_request(bytes, &request.bombay, logical, &registries[position])
                    .await
            },
            async |bytes: &[u8], proof: &[u8]| {
                let response: ReadReply =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                let position = usize::try_from(response.bombay.request_sequence)
                    .map_err(|_| VerificationError::Invalid)?;
                let receipt = recorded::verify(
                    &ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    },
                    &registries[position],
                )
                .await?;
                let gate = reply_gate.borrow_mut().take();
                if let Some((entered, released)) = gate {
                    let announced = entered.send(());
                    announced
                        .expect("the native driver observes the actual verified response wait");
                    released.await.map_err(VerificationError::Unavailable)?;
                }
                Ok::<_, VerificationError>(receipt)
            },
            |receipt: &recorded::Receipt, bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
                let response: ReadReply =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                let request: ReadRequest = serde_json::from_slice(&original.message)
                    .map_err(VerificationError::Declaration)?;
                if response.bombay != recorded::projection(receipt).0.bombay
                    || response.bombay != request.bombay
                    || response.kind != Purpose::ReadReply
                    || response.operation != CounterOperation::Read
                    || response.value != request.bombay.request_sequence
                {
                    return Err(VerificationError::Invalid);
                }
                Ok(response.value)
            },
            |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
                let authority = authority.borrow();
                if original.from == *authority
                    && actual_target == *authority
                    && actual_runtime == runtime
                {
                    Ok(authority)
                } else {
                    Err("the actual request does not name this return endpoint")
                }
            },
            |receipt: &recorded::Receipt,
             original: &User<MailAddr, Box<[u8]>>,
             actual_runtime: &str,
             actual_target| {
                let authority = authority.borrow();
                let claims = &recorded::projection(receipt).0.bombay;
                if original.from == *authority
                    && actual_target == *authority
                    && actual_runtime == runtime
                    && claims.runtime_generation.as_ref() == actual_runtime
                    && claims.actor_generation == actual_target.0
                {
                    Ok(authority)
                } else {
                    Err("the actual reply does not name this return endpoint")
                }
            },
        );
        let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
            MessageProtocol<MailAddr, u64>,
            Never,
            (),
            EstablishedRecipient<MessageProtocol<MailAddr, u64>>,
        >>::declare(&mut client, &session)
        .await;
        // Consume the real admitted Source prefix once, retaining its exact Users.
        client.source.close_admission();
        let first_original = client
            .source
            .receive()
            .await
            .expect("the first admitted original");
        let second_original = client
            .source
            .receive()
            .await
            .expect("the second admitted original");
        let source_eof = client.source.receive().await;
        if source_eof.is_none() {
            client.source_eof = Some(());
        }
        let (publication, joined) = oneshot::channel();
        let joined = joined.shared();
        let (
            first_poll,
            second_poll,
            second_query_before_release,
            second_protection_before_release,
            second_registry_before_release,
            received,
        ) = {
            let first = Rc::new(Request::new(first_original));
            let second = Rc::new(Request::new(second_original));
            client.requests.borrow_mut().push(Rc::clone(&first));
            client.requests.borrow_mut().push(Rc::clone(&second));
            Client::<_, _, _, _, _, _, _, _, _, _>::select(
                &client.inputs,
                &first,
                &runtime,
                target_address,
            );
            Client::<_, _, _, _, _, _, _, _, _, _>::select(
                &client.inputs,
                &second,
                &runtime,
                target_address,
            );
            let querier = client
                .querier
                .as_ref()
                .expect("actual declaration acquired this Querier");
            // Use the real initial native observation, not a manufactured ready-route result.
            let observed = loop {
                match querier.matching_status().await {
                    Ok(status) if status.matching() => break Ok(()),
                    Ok(_) => match client
                        .listener
                        .as_ref()
                        .expect("native listener acquired")
                        .recv_async()
                        .await
                    {
                        Ok(_) => {}
                        Err(cause) => break Err(cause),
                    },
                    Err(cause) => break Err(cause),
                }
            };
            *client.visibility.borrow_mut() = Some(observed);
            let capacity = Arc::new(Semaphore::new(client.inputs.1.concurrent_requests.get()));
            let mut first_wait = pin!(unconstrained(
                Client::<_, _, _, _, _, _, _, _, _, _>::request(
                    &client.inputs,
                    &first,
                    querier,
                    &endpoint,
                    source_address,
                    target_address,
                    &runtime,
                    Arc::clone(&capacity),
                    ready(&client.visibility),
                    joined.clone(),
                )
            ));
            let mut second_wait = pin!(unconstrained(
                Client::<_, _, _, _, _, _, _, _, _, _>::request(
                    &client.inputs,
                    &second,
                    querier,
                    &endpoint,
                    source_address,
                    target_address,
                    &runtime,
                    Arc::clone(&capacity),
                    ready(&client.visibility),
                    joined.clone(),
                )
            ));
            // Recorded protection is Ready and native Get entry returns Ready;
            // this poll reaches A's actual native response receive, with P held.
            let first_poll =
                poll_fn(|context| Poll::Ready(first_wait.as_mut().poll(context))).await;
            let held = queryable
                .recv_async()
                .await
                .expect("A's actual native Query arrived");
            // Manually poll a temporary borrow; resume the same pinned B later.
            let second_poll = {
                let mut borrowed = second_wait.as_mut();
                poll_fn(|context| Poll::Ready(borrowed.as_mut().poll(context))).await
            };
            let second_query_before_release = match &*second.query.borrow() {
                Attempt::Unattempted => Attempt::Unattempted,
                Attempt::Uncertain => Attempt::Uncertain,
                Attempt::Returned(_) => Attempt::Returned(()),
            };
            // Save only the actual result phase; the complete original output
            // or refusal stays owned by B's row through final extraction.
            let second_protection_before_release = second
                .protected
                .borrow()
                .as_ref()
                .map(|original| original.as_ref().map(|_| ()).map_err(|_| ()));
            let second_registry_before_release = registries[1].borrow().len();
            // No assertion precedes release. The same two-query driver also
            // completes if the inverse has already submitted B while A is held.
            let native = async {
                let mut held = Some(held);
                let mut received = Vec::new();
                let mut health_observed = Vec::new();
                let mut entering = Some(entering);
                let mut release = Some(release);
                for _ in 0..2 {
                    let query = match held.take() {
                        Some(query) => query,
                        None => queryable
                            .recv_async()
                            .await
                            .expect("B's actual native Query arrived"),
                    };
                    let raw = query
                        .payload()
                        .expect("actual request payload")
                        .to_bytes()
                        .into_owned()
                        .into_boxed_slice();
                    let proof = query
                        .attachment()
                        .expect("actual request proof")
                        .to_bytes()
                        .into_owned()
                        .into_boxed_slice();
                    let request: ReadRequest = serde_json::from_slice(&raw).unwrap();
                    let position = usize::try_from(request.bombay.request_sequence).unwrap();
                    let receipt = recorded::verify(
                        &ProtectedMessage {
                            protected: raw.clone(),
                            proof: proof.clone(),
                        },
                        &registries[position],
                    )
                    .await
                    .unwrap_or_else(|_| panic!("the full registered request verifies"));
                    let bytes = serde_json::to_vec(&ReadReplyRef {
                        bombay: &recorded::projection(&receipt).0.bombay,
                        kind: Purpose::ReadReply,
                        operation: CounterOperation::Read,
                        value: &request.bombay.request_sequence,
                    })
                    .unwrap()
                    .into_boxed_slice();
                    let response_proof = recorded::protect(&bytes, &receipt, &registries[position])
                        .await
                        .unwrap_or_else(|_| panic!("the full actual response pair registers"));
                    // This releases A's real native wait, even if B was early.
                    let published = query
                        .reply(key, bytes.to_vec())
                        .attachment(response_proof.to_vec())
                        .await;
                    let mut published = vec![published];
                    if matches!(traffic, ReplyTraffic::Burst) && position == 0 {
                        let entered = entering.take().expect("one actual response gate").await;
                        for _ in 0..3 {
                            published.push(
                                query
                                    .reply(key, bytes.to_vec())
                                    .attachment(response_proof.to_vec())
                                    .await,
                            );
                        }
                        let querier = receiving.declare_querier(HEALTH).await.unwrap();
                        let listener = querier
                            .matching_listener()
                            .with(FifoChannel::new(2))
                            .await
                            .unwrap();
                        let initial = querier.matching_status().await.unwrap();
                        let matched = if initial.matching() {
                            initial
                        } else {
                            loop {
                                let status = listener.recv_async().await.unwrap();
                                if status.matching() {
                                    break status;
                                }
                            }
                        };
                        let listener_closed = listener.undeclare().await;
                        let querier_closed = querier.undeclare().await;
                        let receiving_health = receiving
                            .get(HEALTH)
                            .allowed_destination(Locality::Remote)
                            .payload(vec![149_u8])
                            .await
                            .expect("the remote peer submits the independent health Query");
                        let health_query = health
                            .as_ref()
                            .expect("the Client owns health")
                            .recv_async()
                            .await
                            .expect("the actual Client callback progresses");
                        let health_input = health_query
                            .payload()
                            .map(|payload| payload.to_bytes().into_owned());
                        let health_published = health_query.reply(HEALTH, vec![151_u8]).await;
                        drop(health_query);
                        let mut health_replies = Vec::new();
                        let health_end = loop {
                            match receiving_health.recv_async().await {
                                Ok(original) => health_replies.push(original),
                                Err(cause) => break Err::<(), _>(cause),
                            }
                        };
                        let connections = (
                            session.zid().to_string(),
                            receiving.zid().to_string(),
                            session.info().links().await.collect::<Vec<_>>(),
                            receiving.info().links().await.collect::<Vec<_>>(),
                        );
                        let callback_before_release = {
                            let refusal = first
                                .callback_refusal
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner);
                            refusal.as_ref().map(|refusal| match refusal {
                                IngressError::Capacity { original, cause } => Either::Left((
                                    original
                                        .result()
                                        .map(|sample| {
                                            (
                                                sample.payload().to_bytes().into_owned(),
                                                sample
                                                    .attachment()
                                                    .map(|proof| proof.to_bytes().into_owned()),
                                            )
                                        })
                                        .map_err(|error| error.payload().to_bytes().into_owned()),
                                    match cause {
                                        TryAcquireError::NoPermits => TryAcquireError::NoPermits,
                                        TryAcquireError::Closed => TryAcquireError::Closed,
                                    },
                                )),
                                IngressError::Queue { cause } => Either::Right(cause.to_string()),
                            })
                        };
                        let released = release.take().expect("one verifier release").send(());
                        health_observed.push((
                            (entered, released),
                            (health_input, health_published, health_replies, health_end),
                            (matched, listener_closed, querier_closed),
                            connections,
                            callback_before_release,
                        ));
                    }
                    drop(query); // Actual native FINAL; no timeout represents EOF.
                    received.push((position, raw, proof, bytes, response_proof, published));
                }
                (received, health_observed)
            };
            let ((), (), received) =
                tokio::join!(first_wait.as_mut(), second_wait.as_mut(), native);
            (
                first_poll,
                second_poll,
                second_query_before_release,
                second_protection_before_release,
                second_registry_before_release,
                received,
            )
        };
        let (received, health_observed) = received;
        let joined_publication = publication.send(());
        // Direct request futures have returned and their borrows have ended.
        <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
            MessageProtocol<MailAddr, u64>,
            Never,
            (),
            EstablishedRecipient<MessageProtocol<MailAddr, u64>>,
        >>::withdraw(&mut client)
        .await;
        target.close_admission();
        let mut deliveries = Vec::new();
        while let Some(original) = target.receive().await {
            deliveries.push(original);
        }
        let health_closed = match health {
            Some(health) => Some(health.undeclare().await),
            None => None,
        };
        let withdrawn = queryable.undeclare().await;
        let closed = session.close().await;
        let peer_closed = match peer.as_ref() {
            Some(peer) => Some(peer.close().await),
            None => None,
        };
        // Response keeps its native count charge through this completed cleanup;
        // no ordering between health and the excess Reply callbacks is assumed.
        let first_callback = {
            let requests = client.requests.borrow();
            let refusal = requests[0]
                .callback_refusal
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            refusal.as_ref().map(|refusal| match refusal {
                IngressError::Capacity { original, cause } => Either::Left((
                    original
                        .result()
                        .map(|sample| {
                            (
                                sample.payload().to_bytes().into_owned(),
                                sample
                                    .attachment()
                                    .map(|proof| proof.to_bytes().into_owned()),
                            )
                        })
                        .map_err(|error| error.payload().to_bytes().into_owned()),
                    match cause {
                        TryAcquireError::NoPermits => TryAcquireError::NoPermits,
                        TryAcquireError::Closed => TryAcquireError::Closed,
                    },
                )),
                IngressError::Queue { cause } => Either::Right(cause.to_string()),
            })
        };
        let (source, _inputs, startup, withdrawal, joined_fact, source_eof_fact, rows, routes) =
            client.into_parts();
        // All native originals, both real EOFs and cleanup precede every oracle.
        let mut delivered = deliveries
            .iter()
            .map(|original| (original.from, original.message))
            .collect::<Vec<_>>();
        delivered.sort_unstable();
        let response_trace = rows
            .iter()
            .map(|row| {
                row.8
                    .iter()
                    .map(|response| {
                        (
                            response.payload().flatten().copied().collect::<Vec<_>>(),
                            response
                                .proof()
                                .map(|proof| proof.flatten().copied().collect::<Vec<_>>()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected_responses = received
            .iter()
            .map(|native| vec![(native.3.to_vec(), Some(native.4.to_vec()))])
            .collect::<Vec<_>>();
        let health_trace = health_observed
            .iter()
            .map(|(gate, health, _, _, _)| {
                (
                    gate.0.is_ok(),
                    gate.1.is_ok(),
                    health.0.as_deref(),
                    health.1.is_ok(),
                    health
                        .2
                        .iter()
                        .map(|reply| {
                            reply
                                .result()
                                .as_ref()
                                .ok()
                                .map(|sample| sample.payload().to_bytes().into_owned())
                        })
                        .collect::<Vec<_>>(),
                    health.3.is_err(),
                )
            })
            .collect::<Vec<_>>();
        let expected_health = match traffic {
            ReplyTraffic::Single => Vec::new(),
            ReplyTraffic::Burst => vec![(
                true,
                true,
                Some([149_u8].as_slice()),
                true,
                vec![Some(vec![151_u8])],
                true,
            )],
        };
        assert_eq!(
            (&delivered, response_trace, health_trace),
            (
                &vec![(source_address, 0), (source_address, 1)],
                expected_responses,
                expected_health
            )
        );
        match traffic {
            ReplyTraffic::Single => {
                assert!(
                    health_closed.is_none() && peer_closed.is_none() && first_callback.is_none()
                );
            }
            ReplyTraffic::Burst => {
                assert!(
                    matches!(health_closed, Some(Ok(()))) && matches!(peer_closed, Some(Ok(())))
                );
                let Some(Either::Left((Ok((payload, Some(proof))), cause))) = &first_callback
                else {
                    panic!("the actual first excess native Reply and capacity cause remain owned");
                };
                assert_eq!(payload.as_slice(), received[0].3.as_ref());
                assert_eq!(proof.as_slice(), received[0].4.as_ref());
                assert!(matches!(cause, TryAcquireError::NoPermits));
                for (_, _, (matched, listener_closed, querier_closed), connections, _) in
                    &health_observed
                {
                    assert!(
                        matched.matching() && listener_closed.is_ok() && querier_closed.is_ok()
                    );
                    assert_ne!(connections.0, connections.1);
                    for links in [&connections.2, &connections.3] {
                        let [link] = links.as_slice() else {
                            panic!("one actual TLS link joins the Sessions");
                        };
                        assert!(
                            link.src().to_string().starts_with("tls/")
                                && link.dst().to_string().starts_with("tls/")
                        );
                    }
                }
            }
        }
        assert!(admitted.iter().all(Result::is_ok));
        assert!(declared.is_ok() && withdrawn.is_ok() && closed.is_ok());
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(matches!(routes, (Some(Ok(())), Some(Ok(())), Some(Ok(())))));
        assert!(joined_publication.is_ok() && joined_fact.is_none());
        assert!(source_eof.is_none() && source_eof_fact.is_some());
        assert_eq!(source.address(), source_address);
        assert!(first_poll.is_pending() && second_poll.is_pending());
        assert_eq!(rows.len(), 2);
        assert_eq!(received.len(), 2);
        assert_eq!(deliveries.len(), 2);
        assert_eq!(delivered, [(source_address, 0), (source_address, 1)]);
        for (position, row) in rows.into_iter().enumerate() {
            let (
                original,
                refusal,
                encoded,
                protected,
                permission,
                capacity,
                query,
                callback,
                responses,
                eof,
                protection_panics,
                operation_panics,
                completed,
            ) = row;
            let native = received
                .iter()
                .find(|native| native.0 == position)
                .expect("the actual request sequence identifies the row");
            let encoded = encoded
                .expect("actual encoding returned")
                .unwrap_or_else(|_| panic!("valid original encoding"))
                .expect("this source original requests a real Get");
            let protected = protected
                .expect("actual protection returned")
                .unwrap_or_else(|_| panic!("registered original proof"));
            assert_eq!(original.from, target_address);
            assert_eq!(original.message.as_ptr(), originals[position].0);
            assert_eq!(original.message.as_ref(), originals[position].1.as_slice());
            assert_eq!(native.1.as_ref(), originals[position].1.as_slice());
            assert_eq!(encoded.as_ref(), native.1.as_ref());
            assert_eq!(protected.as_ref(), native.2.as_ref());
            assert!(native.5.iter().all(Result::is_ok));
            assert_eq!(
                native.5.len(),
                match (traffic, position) {
                    (ReplyTraffic::Burst, 0) => 4,
                    _ => 1,
                }
            );
            assert!(refusal.is_none() && permission.is_none() && capacity.is_none());
            match (traffic, position) {
                (ReplyTraffic::Burst, 0) => assert!(callback.is_some()),
                _ => assert!(callback.is_none()),
            }
            assert!(matches!(query, Attempt::Returned(Ok(()))) && eof.is_some());
            assert!(
                matches!(protection_panics, (None, None, None))
                    && matches!(operation_panics, (None, None, None))
            );
            // This component host directly polls request, not serve's outer provide.
            assert!(completed.is_none());
            assert_eq!(responses.len(), 1);
            for response in responses {
                let payload = response.payload().flatten().copied().collect::<Vec<_>>();
                let proof = response
                    .proof()
                    .expect("actual response proof")
                    .flatten()
                    .copied()
                    .collect::<Vec<_>>();
                let (cause, wire, shape, verified, decoded, permission, panics) =
                    response.into_parts();
                let wire = wire.expect("actual processed wire pair");
                let receipt = verified
                    .expect("actual verification returned")
                    .unwrap_or_else(|_| panic!("registered response pair"));
                let projection = recorded::projection(&receipt);
                let sequence = u64::try_from(position).unwrap();
                assert_eq!(payload.as_slice(), native.3.as_ref());
                assert_eq!(proof.as_slice(), native.4.as_ref());
                assert_eq!(wire.0.as_ref(), payload.as_slice());
                assert_eq!(wire.1.as_ref(), proof.as_slice());
                assert_eq!(projection.0.bombay.request_sequence, sequence);
                assert_eq!(projection.1.identity.sequence, sequence);
                assert!(cause.is_none() && matches!(shape, Some(Ok(()))) && permission.is_none());
                assert!(matches!(
                    decoded,
                    Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
                ));
                assert!(matches!(panics, (None, None, None)));
            }
            let registry = registries[position].borrow();
            assert_eq!(registry.len(), 2);
        }
        // A's actual native wait still owned capacity at this saved point.
        // Both original rows, admitted replies and cleanup are now acquired.
        assert!(matches!(second_query_before_release, Attempt::Unattempted));
        // B had no protection result or registered pair before A's real EOF.
        assert!(second_protection_before_release.is_none());
        assert_eq!(second_registry_before_release, 0);
    })
    .await
    .expect("the finite actual native permit trace joins before query expiry");
}

#[derive(Debug, PartialEq, Eq)]
struct RetainedPayloads {
    source_row: usize,
    source_unread_allowance: usize,
    source_pending_allowance: usize,
    source_returned_allowance: usize,
    encoded_request: usize,
    request_proof: usize,
    native_submission_allowance: usize,
    native_request: usize,
    native_replies: usize,
    queued_reply_allowance: usize,
    refused_reply: usize,
    arriving_reply_allowance: usize,
    reply_wire: usize,
    target_allowance: usize,
    receipt_text: usize,
    provisioned_payloads: usize,
    oracle_payloads: usize,
}

fn metadata_text_bytes(metadata: &Metadata) -> usize {
    [
        metadata.provider_profile.len(),
        metadata.deployment.len(),
        metadata.caller.len(),
        metadata.caller_session.len(),
        metadata.controller.len(),
        metadata.actor.len(),
        metadata.host.len(),
        metadata.protocol.len(),
        metadata.grant_issuer.len(),
        metadata.runtime_generation.len(),
    ]
    .into_iter()
    .sum()
}

fn reply_payload_bytes(reply: &NativeReply) -> usize {
    match reply.result() {
        Ok(sample) => sample.payload().len() + sample.attachment().map_or(0, ZBytes::len),
        Err(error) => error.payload().len(),
    }
}

#[test]
fn retained_payload_bytes_include_pending_senders_and_unconsumed_replies() {
    let runtime = RuntimeBuilder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the existing native host opens");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(40), retained_payloads())
            .await
            .expect("a watchdog supplies no payload-byte or original-custody evidence");
    });
}

#[expect(
    clippy::too_many_lines,
    reason = "one finite ownership trace retains all payload categories and original producer results before its complete oracle"
)]
async fn retained_payloads() {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let endpoints = |address: String, identity: &str| ConnectionEndpoints {
        addresses: vec![address],
        certificate: CredentialSource::File(
            certificates.join(format!("{identity}-certificate.pem")),
        ),
        private_key: CredentialSource::File(
            certificates.join(format!("{identity}-private-key.pem")),
        ),
    };
    let network = |layout| Network {
        layout,
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(1).unwrap(),
    };
    let peer = zenoh::open(
        network(NetworkLayout::Peer {
            connect: None,
            listen: Some(endpoints("localhost:0".into(), "recipient")),
        })
        .to_configuration()
        .unwrap(),
    )
    .await
    .expect("the actual TLS peer opens");
    let [locator]: [_; 1] = peer
        .info()
        .locators()
        .await
        .try_into()
        .expect("the peer owns one bound endpoint");
    let session = zenoh::open(
        network(NetworkLayout::Peer {
            connect: Some(endpoints(locator.address().as_str().to_owned(), "caller")),
            listen: None,
        })
        .to_configuration()
        .unwrap(),
    )
    .await
    .expect("the actual Client Session opens");
    let runtime = session.zid().to_string();
    let key = "bombay/outbound/retained-payloads";
    let queryable = peer
        .declare_queryable(key)
        .allowed_origin(Locality::Remote)
        .with(FifoChannel::new(1))
        .await
        .expect("the native reply peer declares");
    let capacity = MailboxCapacity::new(1).unwrap();
    let scope = ApplicationScope::new().client_capacity(capacity);
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .unwrap();
    let destination = source.recipient();
    let source_address = source.address();
    let mut target = scope.external::<MessageProtocol<MailAddr, u64>>().unwrap();
    let target_address = target.address();
    let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let mut source_requests = Vec::new();
    let mut source_oracles = Vec::new();
    let mut declared_text = 0;
    for sequence in 0..4 {
        let mut declared = metadata(ProviderProfile::Recorded, "caller".into(), runtime.clone());
        declared.actor_generation = target_address.0;
        declared.request_sequence = sequence;
        declared_text = metadata_text_bytes(&declared);
        let frame = serde_json::to_vec(&ReadRequestRef {
            bombay: &declared,
            kind: Purpose::Request,
            operation: CounterOperation::Read,
        })
        .unwrap()
        .into_boxed_slice();
        source_oracles.push((frame.as_ptr(), frame.to_vec()));
        source_requests.push(frame);
    }
    let request_maximum = source_oracles
        .iter()
        .map(|(_, frame)| frame.len())
        .max()
        .unwrap();
    let reply_oracle = {
        let declared: ReadRequest = serde_json::from_slice(&source_oracles[0].1).unwrap();
        serde_json::to_vec(&ReadReplyRef {
            bombay: &declared.bombay,
            kind: Purpose::ReadReply,
            operation: CounterOperation::Read,
            value: &0,
        })
        .unwrap()
    };
    let proof_maximum = 2;
    let reply_maximum = reply_oracle.len() + proof_maximum;
    let oracle_bytes = source_oracles
        .iter()
        .map(|(_, bytes)| bytes.len())
        .sum::<usize>()
        + reply_oracle.len();
    let registry = RefCell::new(Vec::new());
    let active_receipt_text = Cell::new(0);
    let (entered, entering) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gate = RefCell::new(Some((entered, released)));
    let mut client = Client::new(
        source,
        key,
        ClientLimits {
            concurrent_requests: NonZeroUsize::new(1).unwrap(),
            retained_requests: NonZeroUsize::new(1).unwrap(),
            retained_responses: NonZeroUsize::new(1).unwrap(),
        },
        limits(),
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            let declared: ReadRequest = serde_json::from_slice(&original.message)
                .map_err(VerificationError::Declaration)?;
            if original.from != target_address
                || actual_target != target_address
                || actual_runtime != runtime
                || declared.kind != Purpose::Request
                || declared.operation != CounterOperation::Read
            {
                return Err(VerificationError::Invalid);
            }
            Ok(Some(original.message.clone()))
        },
        async |bytes: &[u8], _: &User<MailAddr, Box<[u8]>>| {
            let declared: ReadRequest =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            let mut logical = logical_scope();
            logical.identity.sequence = declared.bombay.request_sequence;
            logical.identity.binding.actor_generation = target_address.0;
            recorded::protect_request(bytes, &declared.bombay, logical, &registry).await
        },
        async |bytes: &[u8], proof: &[u8]| {
            // Both the temporary pair and its parser DTO end before the gate.
            let receipt = {
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                recorded::verify(&original, &registry).await?
            };
            let held = gate.borrow_mut().take();
            if let Some((entered, released)) = held {
                active_receipt_text.set(metadata_text_bytes(
                    &recorded::projection(&receipt).0.bombay,
                ));
                let announced = entered.send(());
                announced.expect("the actual owned receipt reaches the held verifier");
                released.await.map_err(VerificationError::Unavailable)?;
                active_receipt_text.set(0);
            }
            Ok::<_, VerificationError>(receipt)
        },
        |receipt: &recorded::Receipt, bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            let response: ReadReply =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            let request: ReadRequest = serde_json::from_slice(&original.message)
                .map_err(VerificationError::Declaration)?;
            if response.bombay != recorded::projection(receipt).0.bombay
                || response.bombay != request.bombay
                || response.kind != Purpose::ReadReply
                || response.operation != CounterOperation::Read
                || response.value != request.bombay.request_sequence
            {
                return Err(VerificationError::Invalid);
            }
            Ok(response.value)
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            if original.from == target_address
                && actual_runtime == runtime
                && actual_target == target_address
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        },
        |receipt: &recorded::Receipt,
         original: &User<MailAddr, Box<[u8]>>,
         actual_runtime: &str,
         actual_target| {
            let declared = &recorded::projection(receipt).0.bombay;
            if original.from == target_address
                && actual_runtime == runtime
                && actual_target == target_address
                && declared.runtime_generation.as_ref() == actual_runtime
                && declared.actor_generation == actual_target.0
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, u64>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, u64>>,
    >>::declare(&mut client, &session)
    .await;
    let [
        acquired_request,
        queued_request,
        filling_request,
        blocked_request,
    ]: [_; 4] = source_requests.try_into().unwrap();
    let admitted_acquired = target.send(&destination, acquired_request).await;
    let admitted_queued = target.send(&destination, queued_request).await;
    let (
        source_phases,
        source_prefix,
        source_end,
        filling_returned,
        blocked_returned,
        held_payloads,
        completed_payloads,
        native,
        refused_reply,
        rows,
        startup,
        withdrawal,
        routes,
        native_cleanup,
    ) = {
        let mut filling_sender = pin!(unconstrained(target.send(&destination, filling_request)));
        let mut blocked_sender = pin!(unconstrained(target.send(&destination, blocked_request)));
        let filling_poll =
            poll_fn(|context| Poll::Ready(filling_sender.as_mut().poll(context))).await;
        let blocked_poll =
            poll_fn(|context| Poll::Ready(blocked_sender.as_mut().poll(context))).await;
        let phase = |observed: &Poll<Result<(), SendError<Box<[u8]>>>>| match observed {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => Poll::Ready(result.as_ref().copied().map_err(|_| ())),
        };
        let filling_initial = phase(&filling_poll);
        let blocked_initial = phase(&blocked_poll);
        let acquired_original = client
            .source
            .receive()
            .await
            .expect("the actually admitted first original exists");
        client.source.close_admission();
        let filling_frontier = match filling_poll {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                poll_fn(|context| Poll::Ready(filling_sender.as_mut().poll(context))).await
            }
        };
        let filling_phase = phase(&filling_frontier);
        let (held_payloads, completed_payloads, native, refused_reply, native_cleanup) = {
            let first = Rc::new(Request::new(acquired_original));
            client.requests.borrow_mut().push(Rc::clone(&first));
            Client::<_, _, _, _, _, _, _, _, _, _>::select(
                &client.inputs,
                &first,
                &runtime,
                target_address,
            );
            let querier = client.querier.as_ref().unwrap();
            let observed = loop {
                match querier.matching_status().await {
                    Ok(status) if status.matching() => break Ok(()),
                    Ok(_) => {
                        client
                            .listener
                            .as_ref()
                            .unwrap()
                            .recv_async()
                            .await
                            .unwrap();
                    }
                    Err(cause) => break Err(cause),
                }
            };
            *client.visibility.borrow_mut() = Some(observed);
            let inventory = |request: &(ZBytes, ZBytes), server_frame: usize| {
                let responses = first.replies.borrow();
                let native_replies = responses
                    .iter()
                    .map(|response| {
                        response.payload().map(<[u8]>::len).sum::<usize>()
                            + response
                                .proof()
                                .map_or(0, |proof| proof.map(<[u8]>::len).sum())
                    })
                    .sum::<usize>();
                let queued =
                    first.incoming.borrow().as_ref().map_or(0, Receiver::len) * reply_maximum;
                let refused = first
                    .callback_refusal
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let refused_reply = refused.as_ref().map_or(0, |refusal| match refusal {
                    IngressError::Capacity { original, .. } => reply_payload_bytes(original),
                    IngressError::Queue { cause } => match cause {
                        TrySendError::Full((original, _)) | TrySendError::Closed((original, _)) => {
                            reply_payload_bytes(original)
                        }
                    },
                });
                let pending = |stage: Poll<Result<(), ()>>, length| {
                    if stage.is_pending() { length } else { 0 }
                };
                let queued_source = |stage, length| {
                    if matches!(stage, Poll::Ready(Ok(()))) {
                        length
                    } else {
                        0
                    }
                };
                let returned = |stage, length| {
                    if matches!(stage, Poll::Ready(Err(()))) {
                        length
                    } else {
                        0
                    }
                };
                RetainedPayloads {
                    source_row: first.original.message.len(),
                    source_unread_allowance: source_oracles[1].1.len()
                        + queued_source(filling_phase, source_oracles[2].1.len())
                        + queued_source(blocked_initial, source_oracles[3].1.len()),
                    source_pending_allowance: pending(filling_phase, source_oracles[2].1.len())
                        + pending(blocked_initial, source_oracles[3].1.len()),
                    source_returned_allowance: returned(filling_phase, source_oracles[2].1.len())
                        + returned(blocked_initial, source_oracles[3].1.len()),
                    encoded_request: first
                        .encoded
                        .borrow()
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                        .and_then(|value| value.as_ref())
                        .map_or(0, |value| value.len()),
                    request_proof: first
                        .protected
                        .borrow()
                        .as_ref()
                        .and_then(|result| result.as_ref().ok())
                        .map_or(0, |value| value.len()),
                    native_submission_allowance: request_maximum
                        + proof_maximum
                        + 2 * reply_maximum,
                    native_request: request.0.len() + request.1.len(),
                    native_replies,
                    queued_reply_allowance: queued,
                    refused_reply,
                    arriving_reply_allowance: (2 * reply_maximum)
                        .saturating_sub(native_replies + queued + refused_reply),
                    reply_wire: responses
                        .iter()
                        .map(|response| {
                            response
                                .wire
                                .borrow()
                                .as_ref()
                                .map_or(0, |wire| wire.0.len() + wire.1.len())
                        })
                        .sum(),
                    target_allowance: responses
                        .iter()
                        .filter(|response| {
                            let decoded = response.decoded.borrow();
                            matches!(
                                decoded.as_ref(),
                                Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
                            )
                        })
                        .count()
                        * size_of::<u64>(),
                    receipt_text: active_receipt_text.get()
                        + responses
                            .iter()
                            .map(|response| {
                                response
                                    .verified
                                    .borrow()
                                    .as_ref()
                                    .and_then(|result| result.as_ref().ok())
                                    .map_or(0, |receipt| {
                                        metadata_text_bytes(&recorded::projection(receipt).0.bombay)
                                    })
                            })
                            .sum::<usize>(),
                    provisioned_payloads: server_frame
                        + registry
                            .borrow()
                            .iter()
                            .map(|(_, pair)| pair.protected.len() + pair.proof.len())
                            .sum::<usize>(),
                    oracle_payloads: oracle_bytes,
                }
            };
            let capacity = Arc::new(Semaphore::new(client.inputs.1.concurrent_requests.get()));
            let (_joined, joined) = oneshot::channel();
            let joined = joined.shared();
            let request = Client::<_, _, _, _, _, _, _, _, _, _>::request(
                &client.inputs,
                &first,
                querier,
                &endpoint,
                source_address,
                target_address,
                &runtime,
                capacity,
                ready(&client.visibility),
                joined,
            );
            let native = async {
                let query = queryable
                    .recv_async()
                    .await
                    .expect("the actual native request arrives");
                let view = (
                    query.payload().unwrap().clone(),
                    query.attachment().unwrap().clone(),
                );
                // All server parser/receipt temporaries end before the held census.
                let (bytes, proof) = {
                    let original = ProtectedMessage {
                        protected: view
                            .0
                            .slices()
                            .flatten()
                            .copied()
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                        proof: view
                            .1
                            .slices()
                            .flatten()
                            .copied()
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    };
                    let receipt = recorded::verify(&original, &registry).await.unwrap();
                    let bytes = serde_json::to_vec(&ReadReplyRef {
                        bombay: &recorded::projection(&receipt).0.bombay,
                        kind: Purpose::ReadReply,
                        operation: CounterOperation::Read,
                        value: &0,
                    })
                    .unwrap()
                    .into_boxed_slice();
                    let proof = recorded::protect(&bytes, &receipt, &registry)
                        .await
                        .unwrap();
                    (bytes, proof)
                };
                let first = query
                    .reply(key, bytes.to_vec())
                    .attachment(proof.to_vec())
                    .await;
                let entered = entering.await;
                let second = query
                    .reply(key, bytes.to_vec())
                    .attachment(proof.to_vec())
                    .await;
                let held = inventory(&view, bytes.len() + proof.len());
                let released = release.send(());
                drop(query);
                (held, (view, bytes, proof, first, second, entered, released))
            };
            let ((), (held, native)) = futures_util::future::join(request, native).await;
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, u64>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, u64>>,
            >>::withdraw(&mut client)
            .await;
            let queryable_closed = queryable.undeclare().await;
            let session_closed = session.close().await;
            let peer_closed = peer.close().await;
            // Keep every response, unread input and pending sender through cleanup.
            let completed = inventory(&native.0, native.1.len() + native.2.len());
            // These supported immutable views share the retained native original;
            // no payload copy is introduced into the census or its evidence.
            let refused_reply = {
                let refusal = first
                    .callback_refusal
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let view = |original: &NativeReply, cause| {
                    original.result().as_ref().ok().map(|sample| {
                        (
                            sample.payload().clone(),
                            sample.attachment().cloned(),
                            cause,
                        )
                    })
                };
                refusal.as_ref().and_then(|refusal| match refusal {
                    IngressError::Capacity { original, cause } => view(
                        original,
                        Some(match cause {
                            TryAcquireError::NoPermits => TryAcquireError::NoPermits,
                            TryAcquireError::Closed => TryAcquireError::Closed,
                        }),
                    ),
                    IngressError::Queue { cause } => match cause {
                        TrySendError::Full((original, _)) | TrySendError::Closed((original, _)) => {
                            view(original, None)
                        }
                    },
                })
            };
            (
                held,
                completed,
                native,
                refused_reply,
                (queryable_closed, session_closed, peer_closed),
            )
        };
        let (mut source, _inputs, startup, withdrawal, _, _, rows, routes) = client.into_parts();
        let mut prefix = Vec::new();
        let end = loop {
            let observed = {
                let mut receive = pin!(unconstrained(source.receive()));
                poll_fn(|context| Poll::Ready(receive.as_mut().poll(context))).await
            };
            match observed {
                Poll::Ready(Some(original)) => prefix.push(original),
                Poll::Ready(None) => break Poll::Ready(()),
                Poll::Pending => break Poll::Pending,
            }
        };
        drop(source);
        let filling_result = match filling_frontier {
            Poll::Ready(result) => result,
            Poll::Pending => filling_sender.as_mut().await,
        };
        let blocked_result = match blocked_poll {
            Poll::Ready(result) => result,
            Poll::Pending => blocked_sender.as_mut().await,
        };
        (
            (filling_initial, blocked_initial, filling_phase),
            prefix,
            end,
            filling_result.map_err(SendError::into_message),
            blocked_result.map_err(SendError::into_message),
            held_payloads,
            completed_payloads,
            native,
            refused_reply,
            rows,
            startup,
            withdrawal,
            routes,
            native_cleanup,
        )
    };
    target.close_admission();
    let mut deliveries = Vec::new();
    while let Some(original) = target.receive().await {
        deliveries.push(original);
    }
    let (queryable_closed, session_closed, peer_closed) = native_cleanup;
    let expected = RetainedPayloads {
        source_row: source_oracles[0].1.len(),
        source_unread_allowance: source_oracles[1].1.len() + source_oracles[2].1.len(),
        source_pending_allowance: source_oracles[3].1.len(),
        source_returned_allowance: 0,
        encoded_request: source_oracles[0].1.len(),
        request_proof: proof_maximum,
        native_submission_allowance: request_maximum + proof_maximum + 2 * reply_maximum,
        native_request: source_oracles[0].1.len() + proof_maximum,
        native_replies: reply_maximum,
        queued_reply_allowance: 0,
        refused_reply: reply_maximum,
        arriving_reply_allowance: 0,
        reply_wire: reply_maximum,
        target_allowance: size_of::<u64>(),
        receipt_text: declared_text,
        provisioned_payloads: reply_maximum
            + source_oracles[0].1.len()
            + proof_maximum
            + reply_maximum,
        oracle_payloads: oracle_bytes,
    };
    let prefix = source_prefix
        .iter()
        .map(|original| {
            (
                original.from,
                original.message.as_ptr(),
                original.message.as_ref(),
            )
        })
        .collect::<Vec<_>>();
    let acquired = rows
        .iter()
        .map(|row| (row.0.from, row.0.message.as_ptr(), row.0.message.as_ref()))
        .collect::<Vec<_>>();
    let filling_original = filling_returned
        .as_ref()
        .err()
        .map(|original| (original.as_ptr(), original.as_ref()));
    let returned = blocked_returned
        .as_ref()
        .err()
        .map(|original| (original.as_ptr(), original.as_ref()));
    let held_stable = (
        (
            held_payloads.source_row,
            held_payloads.source_unread_allowance,
            held_payloads.source_pending_allowance,
            held_payloads.source_returned_allowance,
        ),
        (
            held_payloads.encoded_request,
            held_payloads.request_proof,
            held_payloads.native_submission_allowance,
            held_payloads.native_request,
            held_payloads.native_replies,
        ),
        (
            held_payloads.reply_wire,
            held_payloads.target_allowance,
            held_payloads.receipt_text,
            held_payloads.provisioned_payloads,
            held_payloads.oracle_payloads,
        ),
    );
    let reply_pairs = rows
        .iter()
        .map(|row| {
            row.8
                .iter()
                .map(|response| {
                    (
                        response.payload().flatten().eq(native.1.iter()),
                        response
                            .proof()
                            .map(|proof| proof.flatten().eq(native.2.iter())),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let native_request_matches = (
        native.0.0.slices().flatten().eq(source_oracles[0].1.iter()),
        native.0.1.slices().flatten().copied().eq([7_u8, 11]),
    );
    let held_caps = (
        held_payloads.reply_wire <= reply_maximum,
        held_payloads.receipt_text <= declared_text,
        held_payloads.target_allowance <= size_of::<u64>(),
        held_payloads.native_replies + held_payloads.queued_reply_allowance <= reply_maximum,
        held_payloads.refused_reply <= reply_maximum,
        held_payloads.native_replies
            + held_payloads.queued_reply_allowance
            + held_payloads.refused_reply
            + held_payloads.arriving_reply_allowance
            == 2 * reply_maximum,
    );
    // Whole byte categories and all actual producer/recipient source_oracles precede tags.
    assert_eq!(
        (
            (
                source_phases,
                source_end,
                acquired,
                prefix,
                filling_returned.is_ok(),
                filling_original,
                returned
            ),
            (&completed_payloads, held_stable, held_caps),
            (
                &deliveries,
                reply_pairs,
                native_request_matches,
                (native.1.as_ref(), native.2.as_ref())
            )
        ),
        (
            (
                (Poll::Pending, Poll::Pending, Poll::Ready(Ok(()))),
                Poll::Pending,
                vec![(
                    target_address,
                    source_oracles[0].0,
                    source_oracles[0].1.as_slice()
                )],
                vec![
                    (
                        target_address,
                        source_oracles[1].0,
                        source_oracles[1].1.as_slice()
                    ),
                    (
                        target_address,
                        source_oracles[2].0,
                        source_oracles[2].1.as_slice()
                    )
                ],
                true,
                None,
                Some((source_oracles[3].0, source_oracles[3].1.as_slice()))
            ),
            (
                &expected,
                (
                    (
                        source_oracles[0].1.len(),
                        source_oracles[1].1.len() + source_oracles[2].1.len(),
                        source_oracles[3].1.len(),
                        0
                    ),
                    (
                        source_oracles[0].1.len(),
                        proof_maximum,
                        expected.native_submission_allowance,
                        source_oracles[0].1.len() + proof_maximum,
                        reply_maximum
                    ),
                    (
                        reply_maximum,
                        0,
                        declared_text,
                        expected.provisioned_payloads,
                        oracle_bytes
                    )
                ),
                (true, true, true, true, true, true)
            ),
            (
                &vec![User::new(source_address, 0)],
                vec![vec![(true, Some(true))]],
                (true, true),
                (reply_oracle.as_slice(), [13_u8, 17].as_slice())
            )
        ),
        "held payload inventory: {held_payloads:?}"
    );
    assert!(declared.is_ok() && admitted_acquired.is_ok() && admitted_queued.is_ok());
    assert!(queryable_closed.is_ok() && session_closed.is_ok() && peer_closed.is_ok());
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(routes, (Some(Ok(())), Some(Ok(())), Some(Ok(())))));
    assert_eq!(capacity.get(), 2);
    let frame_limits = limits();
    assert!(
        request_maximum <= frame_limits.input_bytes.get()
            && reply_oracle.len() <= frame_limits.input_bytes.get()
            && proof_maximum <= frame_limits.proof_bytes.get()
    );
    assert!(native.3.is_ok() && native.4.is_ok() && native.5.is_ok() && native.6.is_ok());
    let (refused_payload, refused_proof, refused_cause) =
        refused_reply.expect("the actual first excess native original remains retained");
    let refused_payload_matches = refused_payload.slices().flatten().eq(native.1.iter());
    let refused_proof_matches = refused_proof
        .as_ref()
        .map(|proof| proof.slices().flatten().eq(native.2.iter()));
    assert!(refused_payload_matches && refused_proof_matches == Some(true));
    assert!(matches!(refused_cause, Some(TryAcquireError::NoPermits)));
    let [row]: [_; 1] = rows
        .try_into()
        .unwrap_or_else(|_| panic!("one retained request row"));
    let (
        original,
        refusal,
        encoded,
        protected,
        permission,
        capacity,
        query,
        callback,
        responses,
        eof,
        protection_panics,
        operation_panics,
        completed,
    ) = row;
    assert_eq!(original.from, target_address);
    assert_eq!(original.message.as_ptr(), source_oracles[0].0);
    assert_eq!(original.message.as_ref(), source_oracles[0].1.as_slice());
    assert!(refusal.is_none() && permission.is_none() && capacity.is_none());
    assert!(matches!(query, Attempt::Returned(Ok(()))) && eof.is_some() && callback.is_some());
    assert!(
        matches!(protection_panics, (None, None, None))
            && matches!(operation_panics, (None, None, None))
    );
    assert!(completed.is_none());
    let encoded = encoded
        .expect("the actual encoder returned")
        .unwrap()
        .unwrap();
    let protected = protected.expect("the actual protector returned").unwrap();
    assert_eq!(encoded.as_ref(), source_oracles[0].1.as_slice());
    assert_eq!(protected.as_ref(), [7, 11]);
    let [response]: [_; 1] = responses.try_into().unwrap();
    let payload_matches = response.payload().flatten().eq(native.1.iter());
    let proof_matches = response
        .proof()
        .map(|proof| proof.flatten().eq(native.2.iter()));
    assert!(payload_matches && proof_matches == Some(true));
    let (cause, wire, shape, receipt, decoded, permission, panics) = response.into_parts();
    let receipt = receipt.expect("the actual verification returned");
    assert!(cause.is_none() && matches!(shape, Some(Ok(()))) && receipt.is_ok());
    let wire = wire.unwrap();
    assert_eq!(wire.0.as_ref(), native.1.as_ref());
    assert_eq!(wire.1.as_ref(), native.2.as_ref());
    assert!(
        matches!(decoded, Some(Ok(Either::Right(Attempt::Returned(Ok(()))))))
            && permission.is_none()
    );
    assert!(matches!(panics, (None, None, None)));
}
