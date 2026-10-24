//! Invalid traffic must not permanently exclude a later valid increment.
use super::super::super::counter::{Counter, CounterIncremented, CounterMessage, CounterValue};
use super::super::assert_retirement;
use super::{peer, protocol, same_counter, wire};
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Attempt, Export, ExportError, ExportLimits, RefusalRetention, json, reply};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, VerificationError, configuration, limits,
    recorded, reserved_address,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::future::{Future, poll_fn, ready};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::CounterInput;

#[derive(Clone, Copy)]
enum InvalidRequest {
    Proof,
    Framing,
    Scope,
}

#[derive(Debug)]
struct Refusal<'scope> {
    source: VerificationError,
    disposed: &'scope Cell<usize>,
    invalid: &'scope Cell<usize>,
}

impl Drop for Refusal<'_> {
    fn drop(&mut self) {
        self.disposed.set(self.disposed.get() + 1);
        if matches!(&self.source, VerificationError::Invalid) {
            self.invalid.set(self.invalid.get() + 1);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_recovers_after_invalid_request_pressure() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(1).unwrap(), InvalidRequest::Proof),
    )
    .await
    .expect("watchdog supplies no valid-progress or cleanup credit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_after_invalid_request_has_a_matched_capacity_control() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(2).unwrap(), InvalidRequest::Proof),
    )
    .await
    .expect("the same real valid request and complete cleanup must finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_recovers_after_json_framing_refusal() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(1).unwrap(), InvalidRequest::Framing),
    )
    .await
    .expect("watchdog supplies no framing recovery or cleanup credit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_recovers_after_request_scope_refusal() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(1).unwrap(), InvalidRequest::Scope),
    )
    .await
    .expect("watchdog supplies no scope recovery or cleanup credit");
}

#[expect(
    clippy::too_many_lines,
    reason = "one finite trace retains invalid and valid originals, independent processing and all joined cleanup before its progress oracle"
)]
async fn recovery(retained_requests: NonZeroUsize, invalid_request: InvalidRequest) {
    let address = reserved_address();
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual recovery Node opens"));
    let receiver_id = node.runtime_id();
    let peer = peer::Peer::open(&address).await;
    let caller_id = peer.runtime_id();
    let requests = RefCell::new(Vec::new());
    let replies = RefCell::new(Vec::new());
    let calls = Cell::new(0);
    let constructors = Cell::new(0);
    let disposed = Cell::new(0);
    let invalid = Cell::new(0);
    // Native Query EOF does not establish local Query/permit destruction.
    // Both logical capacities reserve BOTH actual native queries independently.
    let maximum = NonZeroUsize::new(2).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let current_request =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            let operation = CounterInput::Increment(scope.bombay.request_sequence);
            let claims = wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_id,
                runtime,
                target,
                scope.bombay.request_sequence,
            );
            protocol::current_scope(
                scope,
                &claims,
                &operation,
                protocol::Purpose::Request,
                deadline,
            )
            .map_err(|source| Refusal {
                source,
                disposed: &disposed,
                invalid: &invalid,
            })
        };
    let current_reply =
        |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
            wire::current(
                recorded::projection(receipt),
                runtime,
                target,
                wire::binding("counter"),
                &caller_id,
                deadline,
            )
        };
    let export = Export::new(
        peer::COUNTER,
        ExportLimits {
            refusals: RefusalRetention::Discharge,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests,
            diagnostics: maximum,
        },
        limits(),
        wire::decode_counter,
        reply(
            |input| {
                let constructors = &constructors;
                wire::increment(input).map(|command| {
                    move |reply_to| {
                        constructors.set(constructors.get() + 1);
                        command(reply_to)
                    }
                })
            },
            wire::processed,
        ),
        |bytes: &[u8], proof: &[u8]| {
            calls.set(calls.get() + 1);
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let original = recorded::verify::<CounterInput>(
                &original,
                &requests.borrow(),
                wire::binding("counter"),
            )
            .into_inner()
            .map_err(|source| Refusal {
                source,
                disposed: &disposed,
                invalid: &invalid,
            });
            ready(original)
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
            wire::protect(bytes, recorded::projection(receipt), &replies, maximum)
        },
        same_counter,
        &current_request,
        &current_reply,
    );
    let (configured, inputs) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let actor = application
            .actor_address(&target)
            .expect("actual certified Counter");
        let mut claims = wire::claims(
            ProviderProfile::Recorded,
            "counter",
            &caller_id,
            &receiver_id,
            actor,
            430,
        );
        if let InvalidRequest::Scope = invalid_request {
            claims.actor_generation = actor
                .0
                .checked_add(1)
                .expect("the finite test has a distinct wrong actor generation");
        }
        let mut first = wire::register(claims, CounterInput::Increment(430), &requests);
        match invalid_request {
            // Preserve the registered pair; transmit a genuinely unregistered proof.
            InvalidRequest::Proof => first.proof = Box::from([181_u8, 193]),
            // A complete JSON array violates the existing protected-object frame
            // before verifier invocation, without an oversized-input shortcut.
            InvalidRequest::Framing => first.protected = Box::from(*b"[]"),
            // The registered complete pair verifies, but actual target scope differs.
            InvalidRequest::Scope => {}
        }
        let valid = wire::register(
            wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_id,
                &receiver_id,
                actor,
                431,
            ),
            CounterInput::Increment(431),
            &requests,
        );
        configured
            .send([first, valid])
            .unwrap_or_else(|_| panic!("peer retains both actual inputs"));
        Ok::<_, Infallible>(target)
    });
    let (queried, queries_done) = oneshot::channel();
    let (shutdown_published, shutdown_received) = oneshot::channel();
    let (closed, retired) = oneshot::channel();
    let execution = async {
        let outcome = node
            .run_with(|application| async move {
                queries_done
                    .await
                    .expect("both native query streams finished while the export stayed live");
                let target = application
                    .actor_address(&application.root().established_recipient())
                    .expect("the same actual Counter remains bound");
                let mut local = application.client::<CounterValue>().unwrap();
                let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                let value = local.receive().await;
                local.close_admission();
                let mut tail = Vec::new();
                while let Some(original) = local.receive().await {
                    tail.push(original);
                }
                let shutdown = application.shutdown();
                let publication = shutdown_published.send(shutdown);
                (target, admitted, value, tail, publication, local)
            })
            .await;
        let publication = closed.send(());
        (outcome, publication)
    };
    let exchange = async {
        let pair = inputs
            .await
            .expect("actual binding registers the two pairs");
        peer.quota(
            pair,
            async || {
                queried
                    .send(())
                    .expect("actual Work awaits both native query streams");
                shutdown_received
                    .await
                    .expect("Work transfers its actual shutdown result")
            },
            retired,
        )
        .await
    };
    let ((outcome, publication), (peer, shutdown)) = tokio::join!(execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("actual recovery Work runs"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual Node close and binding remain"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (observed, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual Work observations remain"));
    let (target, admitted, value, tail, shutdown_publication, _local) = observed;
    let (origin, native) = native.expect("actual root has joined");
    let report = report.expect("actual independent root report remains");
    let notifications = notifications.expect("both actual notifications remain");
    let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
    let retained = rows.len();
    // Acquire and settle every real owner before any expected-result assertion.
    cleanup.expect("actual application cleanup joins");
    publication.expect("actual peer awaits Node close");
    closed.expect("actual Node connection closes");
    peer.closed
        .as_ref()
        .expect("actual peer connection closes independently");
    startup
        .expect("native declaration was attempted")
        .expect("native declaration succeeded");
    withdrawal
        .expect("native withdrawal was attempted")
        .expect("native withdrawal succeeded");
    bound
        .expect("actual binding returned")
        .expect("actual Counter binding succeeded");
    shutdown.expect("actual Work shutdown was accepted");
    shutdown_publication.expect("the same actual shutdown result reached the peer");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual joined-retirement publication");
    assert_retirement(&native);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(polling.is_none() && disposal.is_none());
    assert_eq!(origin.address(), target);
    assert!(admitted.is_ok() && tail.is_empty());
    let refused = peer
        .refused
        .iter()
        .map(|original| original.lock().unwrap())
        .collect::<Vec<_>>();
    assert!(refused.iter().all(|original| original.is_none()));
    assert!(
        peer.replies.is_empty(),
        "the refused first request must yield no native reply"
    );
    let registered = requests.borrow();
    assert_eq!(registered.len(), 2);
    for valid in rows {
        let (
            key,
            raw,
            verified,
            decoded,
            native_reply,
            encoding,
            protection,
            publications,
            refusals,
            panics,
        ) = valid
            .try_into_parts()
            .unwrap_or_else(|_| panic!("valid producer has joined"));
        if raw.0 != registered[1].protected {
            // A no-discharge inverse may retain the actual first refusal.
            // Observe that original; do not synthesize a replacement cause or
            // require its absence before the independent progress oracle.
            match invalid_request {
                InvalidRequest::Proof => {
                    let Some(Err(cause)) = verified.as_ref() else {
                        panic!("the actual transmitted proof is unregistered");
                    };
                    assert_eq!(raw.0, registered[0].protected);
                    assert_eq!(raw.1.as_ref(), [181, 193]);
                    assert!(matches!(&cause.source, VerificationError::Invalid));
                    assert!(matches!(&refusals, (None, None, None)));
                }
                InvalidRequest::Framing => {
                    assert_eq!(raw.0.as_ref(), b"[]");
                    assert_eq!(raw.1, registered[0].proof);
                    assert!(verified.is_none());
                    assert!(matches!(
                        &refusals,
                        (
                            None,
                            None,
                            Some(ExportError::Json {
                                cause: json::Error::OuterObject
                            })
                        )
                    ));
                }
                InvalidRequest::Scope => {
                    let receipt = verified
                        .as_ref()
                        .expect("actual verification was attempted")
                        .as_ref()
                        .unwrap_or_else(|_| panic!("the actual registered pair verifies"));
                    let scope = recorded::projection(receipt);
                    let cause = refusals
                        .0
                        .as_ref()
                        .expect("actual current-request refusal remains");
                    assert_eq!(raw.0, registered[0].protected);
                    assert_eq!(raw.1, registered[0].proof);
                    assert_ne!(scope.bombay.actor_generation, target.0);
                    assert!(matches!(&cause.source, VerificationError::Invalid));
                    assert!(refusals.1.is_none() && refusals.2.is_none());
                }
            }
            assert!(
                key.is_none()
                    && decoded.is_none()
                    && native_reply.is_none()
                    && encoding.is_none()
                    && protection.is_none()
                    && publications.is_empty()
            );
            assert!(matches!(panics, (None, None, None, None, None, None)));
            continue;
        }
        assert_eq!(raw.0, registered[1].protected);

        assert_eq!(raw.1, registered[1].proof);
        let receipt = verified
            .expect("actual valid provider result")
            .expect("registered pair verifies");
        let scope = recorded::projection(&receipt);
        assert_eq!(scope.bombay.request_sequence, 431);
        assert_eq!(scope.operation, CounterInput::Increment(431));
        assert!(key.is_some() && decoded.is_none());
        let (unsent, client, admission, received, tail) =
            native_reply.expect("actual increment receipt");
        assert!(unsent.is_none());
        assert!(matches!(client, Attempt::Returned(Ok(_))));
        assert!(matches!(admission, Attempt::Returned(Ok(()))));
        let expected = Some(Some(User::new(
            target,
            CounterIncremented {
                request_sequence: 431,
            },
        )));
        assert_eq!(received, expected);
        assert!(matches!(tail.as_slice(), [None]));
        let encoding = encoding
            .expect("actual processed encoding")
            .expect("encoding succeeds");
        let protection = protection
            .expect("actual processed protection")
            .expect("protection succeeds");
        let [publication] =
            <[_; 1]>::try_from(publications).unwrap_or_else(|_| panic!("one actual publication"));
        publication.expect("actual native publication succeeds");
        assert!(matches!(refusals, (None, None, None)));
        assert!(matches!(panics, (None, None, None, None, None, None)));
        let [reply] = peer.additional.as_slice() else {
            panic!("one actual valid native reply")
        };
        let native = reply.result();
        let sample = native.as_ref().expect("the actual reply is a Sample");
        let payload = sample.payload().to_bytes();
        let proof = sample
            .attachment()
            .expect("actual protected reply proof")
            .to_bytes();
        assert_eq!(payload.as_ref(), encoding.as_ref());
        assert_eq!(proof.as_ref(), protection.as_ref());
        let received = ProtectedMessage {
            protected: payload.into_owned().into_boxed_slice(),
            proof: proof.into_owned().into_boxed_slice(),
        };
        let verified = recorded::verify::<CounterInput>(
            &received,
            &replies.borrow(),
            wire::binding("counter"),
        )
        .into_inner()
        .expect("the actual received pair was protected by the same policy");
        let reply_scope = recorded::projection(&verified);
        assert_eq!(reply_scope.bombay, scope.bombay);
        assert_eq!(reply_scope.kind, protocol::Purpose::Processed);
        assert_eq!(reply_scope.operation, CounterInput::Increment(431));
    }
    // The actual no-discharge inverse must reach this independent progress
    // oracle after all cleanup. Later healthy-only oracles get no inverse credit.
    let expected = Some(User::new(target, 1));
    assert_eq!(
        value, expected,
        "a valid increment must progress on the same live export after invalid traffic stops"
    );
    assert!(ingress.is_empty());
    assert_eq!(retained, 1);
    let (expected_calls, expected_disposals) = match invalid_request {
        InvalidRequest::Proof | InvalidRequest::Scope => (2, 1),
        InvalidRequest::Framing => (1, 0),
    };
    assert_eq!(calls.get(), expected_calls);
    assert_eq!(constructors.get(), 1);
    assert_eq!(peer.additional.len(), 1);
    assert_eq!(disposed.get(), expected_disposals);
    assert_eq!(invalid.get(), expected_disposals);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one causal refusal, two admitted originals and a cached duplicate retain all native and joined facts before key and processing oracles"
)]
async fn issued_request_keys_survive_reused_refusal_capacity() {
    let trace = async {
        let address = reserved_address();
        let maximum = NonZeroUsize::new(4).unwrap();
        let mut network = configuration(&address, &EndpointRole::Receiver);
        network.max_retained_queries = maximum;
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(network)
            .await
            .unwrap_or_else(|_| panic!("the same actual Counter Node opens"));
        let receiver_id = node.runtime_id();
        let peer = peer::Peer::open(&address).await;
        let caller_id = peer.runtime_id();
        let requests = RefCell::new(Vec::new());
        let replies = RefCell::new(Vec::new());
        let allocations = RefCell::new(Vec::new());
        let constructors = Cell::new(0);
        let (entered, first_pending) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let first_provider = RefCell::new(Some((entered, released)));
        let deadline = Instant::now() + Duration::from_secs(30);
        let current =
            |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
                wire::current(
                    recorded::projection(receipt),
                    runtime,
                    target,
                    wire::binding("counter"),
                    &caller_id,
                    deadline,
                )
            };
        let export = Export::new(
            peer::COUNTER,
            ExportLimits {
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: NonZeroUsize::new(3).unwrap(),
                diagnostics: maximum,
                refusals: RefusalRetention::Discharge,
            },
            limits(),
            wire::decode_counter,
            reply(
                |input| {
                    let constructors = &constructors;
                    wire::increment(input).map(|command| {
                        move |reply_to| {
                            constructors.set(constructors.get() + 1);
                            command(reply_to)
                        }
                    })
                },
                wire::processed,
            ),
            async |bytes: &[u8], proof: &[u8]| {
                allocations
                    .borrow_mut()
                    .push((bytes.as_ptr(), proof.as_ptr()));
                let first = first_provider.borrow_mut().take();
                if let Some((entered, released)) = first {
                    let mut entered = Some(entered);
                    let mut released = pin!(released);
                    poll_fn(|context| match released.as_mut().poll(context) {
                        Poll::Pending => {
                            if let Some(entered) = entered.take() {
                                let publication = entered.send(());
                                publication.expect("peer observes an actual Pending verifier poll");
                            }
                            Poll::Pending
                        }
                        original @ Poll::Ready(_) => original,
                    })
                    .await
                    .expect("the peer releases A after B's actual callback EOF");
                }
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                let verification = recorded::verify::<CounterInput>(
                    &original,
                    &requests.borrow(),
                    wire::binding("counter"),
                );
                verification.await
            },
            async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
                wire::protect(bytes, recorded::projection(receipt), &replies, maximum)
            },
            same_counter,
            &current,
            &current,
        );
        let (configured, inputs) = oneshot::channel();
        let mut node = node.exports(export, async |application| {
            let target = application.root().established_recipient();
            let actor = application
                .actor_address(&target)
                .expect("actual certified Counter");
            let mut first = wire::register(
                wire::claims(
                    ProviderProfile::Recorded,
                    "counter",
                    &caller_id,
                    &receiver_id,
                    actor,
                    430,
                ),
                CounterInput::Increment(430),
                &requests,
            );
            first.proof = Box::from([181_u8, 193]);
            let canonical = wire::register(
                wire::claims(
                    ProviderProfile::Recorded,
                    "counter",
                    &caller_id,
                    &receiver_id,
                    actor,
                    431,
                ),
                CounterInput::Increment(431),
                &requests,
            );
            let neighbor = wire::register(
                wire::claims(
                    ProviderProfile::Recorded,
                    "counter",
                    &caller_id,
                    &receiver_id,
                    actor,
                    432,
                ),
                CounterInput::Increment(432),
                &requests,
            );
            let publication = configured.send([first, canonical, neighbor]);
            publication.expect("the peer retains the three actual protected requests");
            Ok::<_, Infallible>(target)
        });
        let (queried, queries_done) = oneshot::channel();
        let (shutdown_published, shutdown_received) = oneshot::channel();
        let (closed, retired) = oneshot::channel();
        let execution = async {
            let outcome = node
                .run_with(|application| async move {
                    queries_done
                        .await
                        .expect("the four actual native streams finished");
                    let target = application
                        .actor_address(&application.root().established_recipient())
                        .expect("the same bound Counter stays alive");
                    let mut local = application.client::<CounterValue>().unwrap();
                    let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                    let value = local.receive().await;
                    local.close_admission();
                    let mut tail = Vec::new();
                    while let Some(original) = local.receive().await {
                        tail.push(original);
                    }
                    let shutdown = application.shutdown();
                    let publication = shutdown_published.send(shutdown);
                    (target, admitted, value, tail, publication, local)
                })
                .await;
            let publication = closed.send(());
            (outcome, publication)
        };
        let exchange = async {
            let [first, canonical, neighbor] =
                inputs.await.expect("actual binding supplies all requests");
            peer.wait_for(peer::COUNTER).await;
            let (mut first_stream, first_refused) = peer.query(peer::COUNTER, &first).await;
            first_pending
                .await
                .expect("A's real verifier reached Pending before B is sent");
            let (mut canonical_stream, canonical_refused) =
                peer.query(peer::COUNTER, &canonical).await;
            let mut canonical_replies = Vec::new();
            let canonical_eof = loop {
                match canonical_stream.recv().await {
                    Some(original) => canonical_replies.push(original),
                    original @ None => break original,
                }
            };
            // A is released unconditionally after B's actual stream ends. No
            // expected reply/key/count assertion can prevent this release.
            let release = release.send(());
            let mut first_replies = Vec::new();
            let first_eof = loop {
                match first_stream.recv().await {
                    Some(original) => first_replies.push(original),
                    original @ None => break original,
                }
            };
            let (mut neighbor_stream, neighbor_refused) =
                peer.query(peer::COUNTER, &neighbor).await;
            let mut neighbor_replies = Vec::new();
            let neighbor_eof = loop {
                match neighbor_stream.recv().await {
                    Some(original) => neighbor_replies.push(original),
                    original @ None => break original,
                }
            };
            let (mut duplicate_stream, duplicate_refused) =
                peer.query(peer::COUNTER, &canonical).await;
            let mut duplicate_replies = Vec::new();
            let duplicate_eof = loop {
                match duplicate_stream.recv().await {
                    Some(original) => duplicate_replies.push(original),
                    original @ None => break original,
                }
            };
            let publication = queried.send(());
            let shutdown = shutdown_received
                .await
                .expect("Work transfers its actual shutdown result");
            retired
                .await
                .expect("actual Node cleanup and close completed");
            let peer = peer.close().await;
            (
                [
                    first_replies,
                    canonical_replies,
                    neighbor_replies,
                    duplicate_replies,
                ],
                [first_eof, canonical_eof, neighbor_eof, duplicate_eof],
                [
                    first_refused,
                    canonical_refused,
                    neighbor_refused,
                    duplicate_refused,
                ],
                release,
                publication,
                shutdown,
                peer,
            )
        };
        let (
            (outcome, publication),
            (streams, eof, refused, release, queries_published, shutdown, peer),
        ) = tokio::join!(execution, exchange);
        let outcome = outcome.unwrap_or_else(|_| panic!("actual same-Node Work remains owned"));
        let (export, bound, closed, polling, disposal) = node
            .into_parts()
            .unwrap_or_else(|_| panic!("actual binding and Node close remain"));
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (observed, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("actual Work completes after all four streams"));
        let (target, admitted, value, tail, shutdown_publication, _local) = observed;
        let (origin, native) = native.expect("the actual root joined");
        let report = report.expect("the independent root report remains");
        let notifications = notifications.expect("both actual notifications remain");
        let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
        let originals = rows
            .into_iter()
            .map(|row| {
                row.try_into_parts()
                    .unwrap_or_else(|_| panic!("joined cleanup ended every actual producer loan"))
            })
            .collect::<Vec<_>>();
        cleanup.expect("actual application cleanup joined");
        publication.expect("the peer awaited actual Node close");
        closed.expect("the actual Node Session closes");
        peer.closed
            .as_ref()
            .expect("the independent native peer closes");
        startup
            .expect("actual declaration attempted")
            .expect("actual declaration succeeded");
        withdrawal
            .expect("actual withdrawal attempted")
            .expect("actual withdrawal succeeded");
        bound
            .expect("actual binding returned")
            .expect("actual binding succeeded");
        release.expect("the actual pending A was released without an oracle dependency");
        queries_published.expect("Work observed all four streams");
        shutdown.expect("the actual Work shutdown was accepted");
        shutdown_publication.expect("its original shutdown result reached the peer");
        notifications
            .termination
            .expect("actual termination publication");
        notifications
            .retirement
            .expect("actual joined-retirement publication");
        assert_retirement(&native);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(polling.is_none() && disposal.is_none());
        let origin_address = origin.address();
        assert_eq!(origin_address, target);
        assert!(admitted.is_ok() && tail.is_empty());
        assert!(eof.iter().all(Option::is_none));
        let refused = refused
            .iter()
            .map(|original| original.lock().unwrap())
            .collect::<Vec<_>>();
        assert!(refused.iter().all(|original| original.is_none()));

        let registered = requests.borrow();
        let canonical = originals
            .iter()
            .find(|original| original.1.0 == registered[1].protected && original.4.is_some())
            .expect("B's actual canonical operation remains independently retained");
        let duplicate = originals
            .iter()
            .find(|original| original.1.0 == registered[1].protected && original.4.is_none());
        let canonical_key = canonical
            .0
            .as_ref()
            .expect("B actually issued its key before A returned");
        let duplicate_key = duplicate.and_then(|original| original.0.as_ref());
        let expected_key = Some(canonical_key);
        // FIRST key oracle: the compaction cut may discharge the duplicate as
        // Conflict. Keep that absence observable instead of unwrapping it early.
        assert_eq!(
            duplicate_key, expected_key,
            "reusing A's unkeyed capacity must preserve B's issued key and cached reply"
        );

        let neighbor = originals
            .iter()
            .find(|original| original.1.0 == registered[2].protected && original.4.is_some())
            .expect("C's actual canonical operation remains independently retained");
        let neighbor_key = neighbor
            .0
            .as_ref()
            .expect("C's real operation has its own key");
        assert_ne!(canonical_key, neighbor_key);
        assert_eq!(originals.len(), 3);
        assert!(ingress.is_empty() && streams[0].is_empty());
        let allocations = allocations.borrow();
        assert_eq!(allocations.len(), 4);
        assert_eq!(constructors.get(), 2);
        assert_eq!(registered.len(), 3);
        let duplicate = duplicate.expect("the actual duplicate retained its own original");
        for (original, registered, allocation, sequence) in [
            (canonical, &registered[1], allocations[1], 431),
            (neighbor, &registered[2], allocations[2], 432),
            (duplicate, &registered[1], allocations[3], 431),
        ] {
            let expected_operation = CounterInput::Increment(sequence);
            let receipt = original
                .2
                .as_ref()
                .expect("actual provider result")
                .as_ref()
                .expect("the complete actual pair verifies");
            let scope = recorded::projection(receipt);
            let pointers = (original.1.0.as_ptr(), original.1.1.as_ptr());
            assert_eq!(original.1.0, registered.protected);
            assert_eq!(original.1.1, registered.proof);
            assert_eq!(pointers, allocation);
            assert_eq!(scope.bombay.request_sequence, sequence);
            assert_eq!(scope.operation, expected_operation);
            assert_eq!(scope.kind, protocol::Purpose::Request);
            assert!(original.3.is_none());
            assert!(matches!(&original.8, (None, None, None)));
            assert!(matches!(&original.9, (None, None, None, None, None, None)));
            let [publication] = original.7.as_slice() else {
                panic!("one actual publication per retained query");
            };
            assert!(publication.is_ok());
        }
        for (original, sequence) in [(canonical, 431), (neighbor, 432)] {
            let (unsent, client, admission, received, tail) = original
                .4
                .as_ref()
                .expect("the actual native increment operation remains");
            let expected = Some(Some(User::new(
                target,
                CounterIncremented {
                    request_sequence: sequence,
                },
            )));
            assert!(unsent.is_none());
            assert!(matches!(client, Attempt::Returned(Ok(_))));
            assert!(matches!(admission, Attempt::Returned(Ok(()))));
            assert_eq!(received, &expected);
            assert!(matches!(tail.as_slice(), [None]));
        }
        assert!(duplicate.4.is_none() && duplicate.5.is_none() && duplicate.6.is_none());
        let canonical_encoding = canonical
            .5
            .as_ref()
            .expect("actual B encoding")
            .as_ref()
            .expect("B encoding succeeded");
        let canonical_proof = canonical
            .6
            .as_ref()
            .expect("actual B protection")
            .as_ref()
            .expect("B protection succeeded");
        let neighbor_encoding = neighbor
            .5
            .as_ref()
            .expect("actual C encoding")
            .as_ref()
            .expect("C encoding succeeded");
        let neighbor_proof = neighbor
            .6
            .as_ref()
            .expect("actual C protection")
            .as_ref()
            .expect("C protection succeeded");
        let protected = replies.borrow();
        assert_eq!(protected.len(), 2);
        for (stream, encoded, proof, request) in [
            (&streams[1], canonical_encoding, canonical_proof, canonical),
            (&streams[2], neighbor_encoding, neighbor_proof, neighbor),
            (&streams[3], canonical_encoding, canonical_proof, canonical),
        ] {
            let [reply] = stream.as_slice() else {
                panic!("exactly one native reply, including cached B");
            };
            let result = reply.result();
            let sample = result.as_ref().expect("an actual native Sample");
            let payload = sample.payload().to_bytes();
            let attachment = sample
                .attachment()
                .expect("the actual native proof")
                .to_bytes();
            assert_eq!(payload.as_ref(), encoded.as_ref());
            assert_eq!(attachment.as_ref(), proof.as_ref());
            let received = ProtectedMessage {
                protected: payload.into_owned().into_boxed_slice(),
                proof: attachment.into_owned().into_boxed_slice(),
            };
            let receipt =
                recorded::verify::<CounterInput>(&received, &protected, wire::binding("counter"))
                    .into_inner()
                    .expect("the whole actual received pair is registered");
            let received_scope = recorded::projection(&receipt);
            let request_receipt = request
                .2
                .as_ref()
                .expect("actual request verification")
                .as_ref()
                .expect("actual request receipt");
            let request_scope = recorded::projection(request_receipt);
            assert_eq!(received_scope.bombay, request_scope.bombay);
            assert_eq!(received_scope.operation, request_scope.operation);
            assert_eq!(received_scope.kind, protocol::Purpose::Processed);
        }
        let expected = Some(User::new(target, 2));
        assert_eq!(
            value, expected,
            "B and C execute once each; the later B duplicate only replays its original reply"
        );
    };
    let trace = pin!(trace);
    tokio::time::timeout(Duration::from_secs(40), trace)
        .await
        .expect("watchdog supplies no key, processing or cleanup credit");
}
