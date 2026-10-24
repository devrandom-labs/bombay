//! Invalid traffic must not permanently exclude a later valid increment.
use super::super::super::counter::{Counter, CounterIncremented, CounterMessage, CounterValue};
use super::super::assert_retirement;
use super::{peer, protocol, same_counter, wire};
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Attempt, Export, ExportError, ExportLimits, reply};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, VerificationError, configuration, limits,
    recorded, reserved_address,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::CounterInput;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_recovers_after_invalid_request_pressure() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(1).unwrap()),
    )
    .await
    .expect("watchdog supplies no valid-progress or cleanup credit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_increment_after_invalid_request_has_a_matched_capacity_control() {
    tokio::time::timeout(
        Duration::from_secs(10),
        recovery(NonZeroUsize::new(2).unwrap()),
    )
    .await
    .expect("the same real valid request and complete cleanup must finish");
}

#[expect(
    clippy::too_many_lines,
    reason = "one finite trace retains invalid and valid originals, independent processing and all joined cleanup before its progress oracle"
)]
async fn recovery(retained_requests: NonZeroUsize) {
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
    // Native Query EOF does not establish local Query/permit destruction.
    // Both logical capacities reserve BOTH actual native queries independently.
    let maximum = NonZeroUsize::new(2).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
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
            recorded::verify::<CounterInput>(
                &original,
                &requests.borrow(),
                wire::binding("counter"),
            )
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
        let mut invalid = wire::register(
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
        // Preserve the registered pair unchanged; this actual transmitted
        // proof is not in the same finite verifier's table.
        invalid.proof = Box::from([181_u8, 193]);
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
            .send([invalid, valid])
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
        "invalid proof must yield no native reply"
    );
    assert_eq!(peer.additional.len(), retained - 1);
    assert_eq!(retained, retained_requests.get());
    assert_eq!(calls.get(), retained);
    assert_eq!(constructors.get(), retained - 1);
    match ingress.as_slice() {
        [] => assert_eq!(retained_requests.get(), 2),
        [ExportError::RetentionCapacity] => assert_eq!(retained_requests.get(), 1),
        _ => panic!("the complete actual row-capacity disposition is retained"),
    }
    let registered = requests.borrow();
    assert_eq!(registered.len(), 2);
    let mut rows = rows.into_iter();
    let invalid = rows
        .next()
        .expect("the actual invalid input acquired its row");
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
    ) = invalid
        .try_into_parts()
        .unwrap_or_else(|_| panic!("invalid producer has joined"));
    assert_eq!(raw.0, registered[0].protected);
    assert_eq!(raw.1.as_ref(), [181, 193]);
    assert!(matches!(verified, Some(Err(VerificationError::Invalid))));
    assert!(
        key.is_none()
            && decoded.is_none()
            && native_reply.is_none()
            && encoding.is_none()
            && protection.is_none()
            && publications.is_empty()
    );
    assert!(matches!(refusals, (None, None, None)));
    assert!(matches!(panics, (None, None, None, None, None, None)));
    if let Some(valid) = rows.next() {
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
        assert_eq!(
            received,
            Some(Some(User::new(
                target,
                CounterIncremented {
                    request_sequence: 431
                }
            )))
        );
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
    let remaining = rows.next();
    assert!(remaining.is_none());
    // Original N1 must reach ONLY this independent processing oracle after all
    // controls/cleanup; matched N2 proves this same valid policy really runs.
    assert_eq!(
        value,
        Some(User::new(target, 1)),
        "a valid increment must progress on the same live export after invalid traffic stops"
    );
}
