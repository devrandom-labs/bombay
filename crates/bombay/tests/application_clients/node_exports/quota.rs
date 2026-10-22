//! The lifetime custody maximum includes original canonical and refused rows.
use super::super::super::counter::Counter;
use super::super::assert_retirement;
use super::{peer, protocol, same_counter, wire};
use bombay::actors::ActorExt;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{Attempt, Export, ExportError, ExportLimits, reply};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use protocol::{
    EndpointRole, ProtectedMessage, ProviderProfile, configuration, limits, recorded,
    reserved_address,
};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::future::ready;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use wire::CounterInput;

#[derive(Clone, Copy)]
enum VerificationPolicy {
    Accept,
    Refuse,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_replay_cannot_exceed_total_request_custody() {
    tokio::time::timeout(
        Duration::from_secs(10),
        complete(VerificationPolicy::Accept),
    )
    .await
    .expect("finite full-quota replay finishes before native query expiry");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_refusal_consumes_request_custody_before_the_next_factory() {
    tokio::time::timeout(
        Duration::from_secs(10),
        complete(VerificationPolicy::Refuse),
    )
    .await
    .expect("finite full-quota refusal finishes before native query expiry");
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete quota trace retains original causes, native outcomes and both closes"
)]
async fn complete(policy: VerificationPolicy) {
    let address = reserved_address();
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("actual quota Node opens"));
    let receiver_id = node.runtime_id();
    let peer = peer::Peer::open(&address).await;
    let caller_id = peer.runtime_id();
    let configuration_scope = String::from("borrowed quota refusal");
    let original: Box<[u8]> = [113, 127].into();
    let allocation = original.as_ptr();
    let original_cause = RefCell::new(Some(original));
    let calls = Cell::new(0);
    let constructors = &Cell::new(0);
    let requests = RefCell::new(Vec::new());
    let replies = RefCell::new(Vec::new());
    let maximum = NonZeroUsize::new(2).unwrap();
    let verify = |bytes: &[u8], proof: &[u8]| {
        calls.set(calls.get() + 1);
        let result = match policy {
            VerificationPolicy::Accept => {
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                Ok(recorded::verify::<CounterInput>(
                    &original,
                    &requests.borrow(),
                    wire::binding("counter"),
                )
                .into_inner()
                .unwrap_or_else(|_| panic!("the real registered request verifies")))
            }
            VerificationPolicy::Refuse => Err((
                configuration_scope.as_str(),
                original_cause
                    .borrow_mut()
                    .take()
                    .expect("the one actual borrowed refusal cause"),
            )),
        };
        ready(result)
    };
    let protect = async |bytes: &[u8], receipt: &recorded::Receipt<CounterInput>| {
        wire::protect(bytes, recorded::projection(receipt), &replies, maximum)
    };
    let permission = RefCell::new(wire::binding("counter"));
    let deadline = Instant::now() + Duration::from_secs(30);
    let current = |receipt: &recorded::Receipt<CounterInput>, runtime: &str, target: MailAddr| {
        let guard = permission.borrow();
        wire::current(
            recorded::projection(receipt),
            runtime,
            target,
            *guard,
            &caller_id,
            deadline,
        )?;
        Ok::<_, wire::Refusal>(guard)
    };
    let export = Export::new(
        peer::COUNTER,
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: NonZeroUsize::new(1).unwrap(),
            diagnostics: maximum,
        },
        limits(),
        wire::decode_counter,
        reply(
            |input| {
                wire::counter_read(input).map(|command| {
                    move |reply_to| {
                        constructors.set(constructors.get() + 1);
                        command(reply_to)
                    }
                })
            },
            wire::value,
        ),
        &verify,
        &protect,
        same_counter,
        &current,
        &current,
    );
    let (published, configured) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let address = application
            .actor_address(&target)
            .expect("actual certified root address");
        let request = wire::register(
            wire::claims(
                ProviderProfile::Recorded,
                "counter",
                &caller_id,
                &receiver_id,
                address,
                331,
            ),
            CounterInput::Read,
            &requests,
        );
        let publication = published.send((request, application.lifecycle(), address));
        publication.unwrap_or_else(|_| panic!("private peer awaits actual quota binding"));
        Ok::<_, Infallible>(target)
    });
    let (closed, retired) = oneshot::channel();
    let execution = async {
        let outcome = node.run().await;
        let publication = closed.send(());
        (outcome, publication)
    };
    let exchange = async {
        let (request, lifecycle, address) = configured.await.expect("actual quota binding");
        let (peer, shutdown) = peer.quota(request, || lifecycle.shutdown(), retired).await;
        (peer, shutdown, address)
    };
    let ((outcome, publication), (peer, shutdown, target)) = tokio::join!(execution, exchange);
    let outcome = outcome.unwrap_or_else(|_| panic!("actual quota execution is issued"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual quota cleanup and network close complete"));
    let (work, root_report, native, notifications, ()) = outcome.into_parts();
    let root_report = root_report.expect("the joined root retains its runtime report");
    assert_eq!(root_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        root_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(polling.is_none() && disposal.is_none());
    let (absence, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the quota execution retains genuine Work absence"));
    let (_, native) = native.expect("actual quota root retirement");
    let notifications = notifications.expect("actual quota notification originals");
    let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
    let [row] = <[_; 1]>::try_from(rows)
        .unwrap_or_else(|_| panic!("exactly one original row under total quota"));
    let (
        key,
        original,
        verified,
        decoded,
        native_reply,
        encoding,
        protection,
        publications,
        (request_refusal, reply_refusal, refusal),
        (
            verification_factory,
            verification_poll,
            verification_disposal,
            protection_factory,
            protection_poll,
            protection_disposal,
        ),
    ) = row
        .try_into_parts()
        .unwrap_or_else(|_| panic!("joined cleanup releases every actual request loan"));
    bound
        .expect("actual binding result")
        .expect("quota binding succeeds");
    publication.expect("peer waits for actual Node close");
    closed.expect("actual quota Node close result");
    peer.closed
        .as_ref()
        .expect("actual peer closes independently");
    startup
        .expect("actual quota startup attempted")
        .expect("actual queryable declaration");
    withdrawal
        .expect("actual quota declaration withdrawn")
        .expect("actual native withdrawal");
    cleanup.expect("actual quota application cleanup joins");
    match absence {
        None => {}
        Some(never) => match never {},
    }
    assert!(shutdown.is_ok());
    notifications
        .termination
        .expect("actual quota termination publication");
    notifications
        .retirement
        .expect("actual quota joined retirement publication");
    assert_retirement(&native);
    assert_eq!(calls.get(), 1);
    let [ExportError::RetentionCapacity] = ingress.as_slice() else {
        panic!("the second raw query is explicitly discharged at the full request quota");
    };
    let registered = requests.borrow();
    assert_eq!(registered.len(), 1);
    assert_eq!(original.0, registered[0].protected);
    assert_eq!(original.1, registered[0].proof);
    assert!(
        decoded.is_none()
            && request_refusal.is_none()
            && reply_refusal.is_none()
            && refusal.is_none()
    );
    assert!(
        verification_factory.is_none()
            && verification_poll.is_none()
            && verification_disposal.is_none()
            && protection_factory.is_none()
            && protection_poll.is_none()
            && protection_disposal.is_none()
    );
    match policy {
        VerificationPolicy::Accept => {
            assert_eq!(constructors.get(), 1);
            let receipt = verified
                .expect("actual verification result")
                .expect("actual accepted receipt");
            let scope = recorded::projection(&receipt);
            assert_eq!(scope.bombay.actor_generation, target.0);
            assert_eq!(scope.bombay.request_sequence, 331);
            assert_eq!(scope.operation, CounterInput::Read);
            let (unsent, client, admission, received, tail) =
                native_reply.expect("actual canonical native reply");
            assert!(key.is_some() && unsent.is_none());
            assert!(matches!(client, Attempt::Returned(Ok(_))));
            assert!(matches!(admission, Attempt::Returned(Ok(()))));
            let original = received
                .expect("actual native receive attempted")
                .expect("actual Counter read reply");
            assert_eq!((original.from, original.message), (target, 0));
            assert!(matches!(tail.as_slice(), [None]));
            encoding
                .expect("actual reply encoding")
                .expect("pure read encoding succeeds");
            protection
                .expect("actual protection result")
                .expect("actual proof retained");
            assert_eq!(publications.len(), 1);
            for publication in publications {
                publication.expect("canonical reply publication succeeds");
            }
            assert_eq!(peer.received(), 1);
            let original = original_cause.borrow();
            let original = original
                .as_ref()
                .expect("acceptance leaves refusal cause untouched");
            assert_eq!(original.as_ptr(), allocation);
            let protected = peer.protected();
            let registered = replies.borrow();
            assert_eq!(protected.len(), 1);
            assert_eq!(protected[0].protected, registered[0].protected);
            assert_eq!(protected[0].proof, registered[0].proof);
        }
        VerificationPolicy::Refuse => {
            assert_eq!(constructors.get(), 0);
            let Err((scope, original)) = verified.expect("actual provider refusal retained") else {
                panic!("the original borrowed cause survives the full quota");
            };
            assert_eq!(scope, configuration_scope);
            assert_eq!(original.as_ptr(), allocation);
            assert_eq!(original.as_ref(), [113, 127]);
            assert!(
                key.is_none()
                    && native_reply.is_none()
                    && encoding.is_none()
                    && protection.is_none()
                    && publications.is_empty()
            );
            assert_eq!(peer.received(), 0);
        }
    }
}
