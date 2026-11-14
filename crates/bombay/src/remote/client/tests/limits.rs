//! Distinct active-operation, Source-row and native-response count policies.
use super::super::{Client, ClientLimits, Request};
use crate::MailAddr;
use crate::application::{ApplicationScope, MailboxCapacity};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::export::Exports;
use crate::remote::{Attempt, IngressError};
use behavior::{EstablishedRecipient, MessageProtocol, Never, User};
use futures_util::FutureExt;
use futures_util::future::Either;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn, ready};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{Arc, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{Semaphore, TryAcquireError, oneshot};
use tokio::task::unconstrained;
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
#[expect(
    clippy::too_many_lines,
    reason = "one permit trace retains both actual request futures, native originals and cleanup before the saved pre-release observations"
)]
async fn held_native_reply_keeps_the_second_request_waiting() {
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
        let session = zenoh::open(configuration)
            .await
            .expect("the actual native Session opens");
        let runtime = session.zid().to_string();
        let key = "bombay/outbound/held-native-permit";
        let queryable = session
            .declare_queryable(key)
            .allowed_origin(Locality::SessionLocal)
            .await
            .expect("the actual local Queryable opens");
        let scope = ApplicationScope::new().client_capacity(MailboxCapacity::new(2).unwrap());
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
                recorded::verify(
                    &ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    },
                    &registries[position],
                )
                .await
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
                    drop(query); // Actual native FINAL; no timeout represents EOF.
                    received.push((position, raw, proof, bytes, response_proof, published));
                }
                received
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
        let withdrawn = queryable.undeclare().await;
        let closed = session.close().await;
        let (source, _inputs, startup, withdrawal, joined_fact, source_eof_fact, rows, routes) =
            client.into_parts();
        // All native originals, both real EOFs and cleanup precede every oracle.
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
        let mut delivered = deliveries
            .iter()
            .map(|original| (original.from, original.message))
            .collect::<Vec<_>>();
        delivered.sort_unstable();
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
            assert!(native.5.is_ok());
            assert!(
                refusal.is_none()
                    && permission.is_none()
                    && capacity.is_none()
                    && callback.is_none()
            );
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
