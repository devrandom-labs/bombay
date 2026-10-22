//! Actual native response custody, not provider or two-host transport evidence.
mod limits;
mod prefix;

use super::Response;
use crate::remote::try_retain;
use behavior::Never;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::timeout;
use zenoh::bytes::ZBytes;
use zenoh::query::{ConsolidationMode, ReplyError as NativeReplyError};
use zenoh::sample::Locality;
use zenoh_config::{Config, ModeDependentValue, WhatAmI};

#[derive(Debug)]
enum Envelope {
    Error,
    WithoutProof,
    EmptyProof,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "retain each actual native response and cleanup receipt before checking its affine handoff"
)]
async fn response_handoff_preserves_native_error_charge_and_borrowed_originals() {
    let observations = timeout(Duration::from_secs(10), async {
        let maximum = 8192;
        let mut configuration = Config::default();
        configuration
            .set_mode(Some(WhatAmI::Peer))
            .expect("the local native fixture selects Peer mode");
        // Three real Gets can still have SDK timeout tasks retaining capacity
        // after their callback EOF. The explicit fixture workload is three.
        configuration
            .set_queries_max_retained(NonZeroUsize::new(3))
            .expect("the actual native fixture retains its three Gets");
        configuration
            .connect
            .set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .expect("the local native fixture has no outgoing endpoints");
        configuration
            .listen
            .set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .expect("the local native fixture has no listening endpoints");
        configuration
            .scouting
            .multicast
            .set_enabled(Some(false))
            .expect("the local native fixture does not multicast");
        configuration
            .scouting
            .multicast
            .set_listen(Some(ModeDependentValue::Unique(false)))
            .expect("the local native fixture does not listen for multicast");
        configuration
            .scouting
            .gossip
            .set_enabled(Some(false))
            .expect("the local native fixture does not gossip");
        let session = zenoh::open(configuration)
            .await
            .expect("the actual local native Session opens");
        let key = "bombay/response-owner";
        let queryable = session
            .declare_queryable(key)
            .allowed_origin(Locality::SessionLocal)
            .await
            .expect("the actual native Queryable opens");
        let mut responses = Vec::new();
        let mut native = Vec::new();
        for envelope in [
            Envelope::Error,
            Envelope::WithoutProof,
            Envelope::EmptyProof,
        ] {
            let count = Arc::new(Semaphore::new(1));
            let charged = Arc::clone(&count);
            let (publication, mut incoming) = mpsc::channel(1);
            let refusal = Arc::new(Mutex::new(None));
            let refused = Arc::clone(&refusal);
            let issued = session
                .get(key)
                .allowed_destination(Locality::SessionLocal)
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(20))
                .callback(move |original| {
                    if let Err(original) = try_retain(&publication, &charged, original) {
                        let mut refused = refused.lock().unwrap_or_else(PoisonError::into_inner);
                        if refused.is_none() {
                            *refused = Some(original);
                        } else {
                            // Same finite first-original diagnostic policy as
                            // the actual owner; no unbounded callback ledger.
                            drop(original);
                        }
                    }
                })
                .await;
            let query = queryable
                .recv_async()
                .await
                .expect("the actual Get reaches its native Queryable");
            let mut payload = ZBytes::writer();
            payload.append(vec![0x61; maximum].into());
            payload.append(vec![0x62].into());
            let published = match &envelope {
                Envelope::Error => query.reply_err(payload.finish()).await,
                Envelope::WithoutProof => query.reply(key, payload.finish()).await,
                Envelope::EmptyProof => {
                    query
                        .reply(key, payload.finish())
                        .attachment(ZBytes::new())
                        .await
                }
            };
            // Actual last Query disposal sends FINAL, releasing the callback.
            drop(query);
            let original = incoming
                .recv()
                .await
                .expect("the one actual native response is transferred");
            let callback_eof = incoming.recv().await;
            let before = match original.0.result() {
                Ok(sample) => sample
                    .payload()
                    .slices()
                    .map(|fragment| (fragment.as_ptr(), fragment.len()))
                    .collect::<Vec<_>>(),
                Err(cause) => cause
                    .payload()
                    .slices()
                    .map(|fragment| (fragment.as_ptr(), fragment.len()))
                    .collect::<Vec<_>>(),
            };
            let response = Response::<Never, Never, Never, Never, Never>::new(original);
            let borrowed = response
                .payload()
                .map(|fragment| (fragment.as_ptr(), fragment.len()))
                .collect::<Vec<_>>();
            // This test-owned content observation copies bytes deliberately; the
            // Response's borrowed API and its wire slot do not materialize them.
            let contents = response.payload().flatten().copied().collect::<Vec<_>>();
            let proof = response
                .proof()
                .map(|fragments| fragments.map(<[u8]>::to_vec).collect::<Vec<_>>());
            let cause = response
                .cause()
                .and_then(|cause| cause.downcast_ref::<NativeReplyError>())
                .map(|cause| (cause.to_string(), cause.encoding().clone()));
            let available = count.available_permits();
            responses.push((
                envelope, response, count, before, borrowed, contents, proof, cause, available,
            ));
            native.push((issued, published, callback_eof, refusal));
        }
        let withdrawn = queryable.undeclare().await;
        let closed = session.close().await;
        (maximum, responses, native, withdrawn, closed)
    })
    .await
    .expect("the whole native custody trace finishes before native query expiry");

    let (maximum, responses, native, withdrawn, closed) = observations;
    // Cleanup is acquired before either handoff or assertion. These actual
    // receipts remain outside all response producers while cause disposal runs.
    let mut handoffs = Vec::new();
    for (envelope, response, count, before, borrowed, contents, proof, cause, available) in
        responses
    {
        let (error, wire, shape, verified, decoded, permission, panics) = response.into_parts();
        let transferred = count.available_permits();
        let source = error
            .as_ref()
            .and_then(|error| error.source())
            .and_then(|source| source.downcast_ref::<NativeReplyError>())
            .map(|source| {
                (
                    source
                        .payload()
                        .slices()
                        .map(|fragment| (fragment.as_ptr(), fragment.len()))
                        .collect::<Vec<_>>(),
                    source
                        .payload()
                        .slices()
                        .flatten()
                        .copied()
                        .collect::<Vec<_>>(),
                    source.to_string(),
                    source.encoding().clone(),
                )
            });
        let diagnostics = error
            .as_ref()
            .map(|error| (error.to_string(), format!("{error:?}")));
        drop(error);
        let disposed = count.available_permits();
        handoffs.push((
            envelope,
            before,
            borrowed,
            contents,
            proof,
            cause,
            available,
            wire,
            shape,
            verified,
            decoded,
            permission,
            panics,
            transferred,
            source,
            diagnostics,
            disposed,
        ));
    }

    assert!(withdrawn.is_ok());
    assert!(closed.is_ok());
    assert_eq!(native.len(), 3);
    for (issued, published, callback_eof, refusal) in native {
        let refusal = refusal.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(issued.is_ok());
        assert!(published.is_ok());
        assert!(callback_eof.is_none());
        assert!(refusal.is_none());
    }
    assert_eq!(handoffs.len(), 3);
    let mut expected = vec![0x61; maximum];
    expected.push(0x62);
    let mut error_diagnostics = Vec::new();
    for (
        envelope,
        before,
        borrowed,
        contents,
        proof,
        cause,
        available,
        wire,
        shape,
        verified,
        decoded,
        permission,
        panics,
        transferred,
        source,
        diagnostics,
        disposed,
    ) in handoffs
    {
        assert_eq!(before, borrowed);
        assert_eq!(contents, expected);
        assert!(contents.len() > maximum);
        assert_eq!(available, 0);
        assert!(wire.is_none());
        assert!(shape.is_none());
        assert!(verified.is_none());
        assert!(decoded.is_none());
        assert!(permission.is_none());
        assert!(panics.0.is_none());
        assert!(panics.1.is_none());
        assert!(panics.2.is_none());
        match envelope {
            Envelope::Error => {
                // This is the intended killer for a naked error transfer that
                // releases its original charge before the error is disposed.
                assert_eq!(transferred, 0);
                let (source_originals, source_contents, source_display, source_encoding) =
                    source.expect("the transferred charged owner retains the actual native cause");
                let (borrowed_display, borrowed_encoding) =
                    cause.expect("the borrowed error exposes the actual native cause");
                assert_eq!(source_originals, before);
                assert_eq!(source_contents, expected);
                assert_eq!(borrowed_display, source_display);
                assert_eq!(borrowed_encoding, source_encoding);
                assert!(proof.is_none());
                assert_eq!(disposed, 1);
                let diagnostics = diagnostics.expect("the returned error has outer diagnostics");
                error_diagnostics.push(diagnostics);
            }
            Envelope::WithoutProof => {
                assert_eq!(transferred, 1);
                assert!(proof.is_none());
                assert!(cause.is_none());
                assert!(source.is_none());
                assert!(diagnostics.is_none());
            }
            Envelope::EmptyProof => {
                assert_eq!(transferred, 1);
                let fragments = proof.expect("actual empty attachment remains present");
                assert!(fragments.iter().all(Vec::is_empty));
                assert!(cause.is_none());
                assert!(source.is_none());
                assert!(diagnostics.is_none());
            }
        }
    }
    // These are last: diagnostic refusal must not mask original-cause custody,
    // count boundaries, actual native EOF or either cleanup result.
    assert_eq!(error_diagnostics.len(), 1);
    for (display, debug) in error_diagnostics {
        assert_eq!(display, "native response contains an error");
        assert_eq!(debug, "ResponseError { .. }");
    }
}
