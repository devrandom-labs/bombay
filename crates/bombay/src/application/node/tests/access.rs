//! Finite native Query ACL crossed with the existing application authority loan.
use super::counter::{CounterIncremented, CounterValue};
use super::custody::completed_actor;
use super::permissions::{LocalAuthorityRefusal, local_current};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadRequest, ReadRequestRef,
    VerificationError, configuration, limits, listener_address, metadata, recorded,
};
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::behavior::{Never, User};
use crate::remote::{
    Attempt, CredentialSource, Export, ExportLimits, Metadata, NetworkLayout, RefusalRetention,
    reply,
};
use crate::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh_config::{Permission, WhatAmI};

const PROTECTED: &str = "bombay/acl/increment";
const HEALTH: &str = "bombay/acl/connection";
const BINDING: Binding = Binding {
    deployment: "two-node-read-study",
    caller: "caller",
    controller: "owner",
    actor: "counter",
    host: "receiver",
    protocol: "counter",
    target_assignment: "acl-counter",
    logical_session: 479,
};

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Operation {
    Increment,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_allow_and_grant_admit_exactly_one_increment() {
    crossing(Permission::Allow, Some(BINDING)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_allow_does_not_replace_the_application_grant() {
    crossing(Permission::Allow, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_deny_prevents_ingress_despite_the_application_grant() {
    crossing(Permission::Deny, Some(BINDING)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_deny_precedes_the_absent_application_grant() {
    crossing(Permission::Deny, None).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one finite secure crossing acquires complete connection, query, actor and retirement facts before independent policy oracles"
)]
async fn crossing(permission: Permission, grant: Option<Binding>) {
    timeout(
        Duration::from_secs(15),
        Box::pin(async {
            let address = listener_address();
            let mut router_network = configuration(&address, &EndpointRole::Receiver);
            let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
            let NetworkLayout::Peer {
                listen: Some(endpoint),
                ..
            } = &mut router_network.layout
            else {
                panic!("the controlled infrastructure has one listening direction");
            };
            endpoint.certificate =
                CredentialSource::File(certificates.join("router-certificate.pem"));
            endpoint.private_key =
                CredentialSource::File(certificates.join("router-private-key.pem"));
            let mut router_config = router_network.to_configuration().unwrap();
            router_config.set_mode(Some(WhatAmI::Router)).unwrap();
            router_config
                .insert_json5(
                    "access_control",
                    &json!({
                        "enabled": true,
                        "default_permission": "allow",
                        "rules": [{
                            "id": "protected-query",
                            "key_exprs": [PROTECTED],
                            "messages": ["query"],
                            "flows": ["ingress"],
                            "permission": permission,
                        }],
                        "subjects": [{
                            "id": "authenticated-caller",
                            "cert_common_names": ["BombayCandidate-caller"],
                            "link_protocols": ["tls"],
                        }],
                        "policies": [{
                            "rules": ["protected-query"],
                            "subjects": ["authenticated-caller"],
                        }],
                    })
                    .to_string(),
                )
                .unwrap();
            let router = zenoh::open(router_config)
                .await
                .expect("the real TLS Router opens");
            let mut receiving = configuration(&address, &EndpointRole::Receiver);
            let NetworkLayout::Peer {
                listen: Some(connect),
                ..
            } = receiving.layout
            else {
                panic!("the actual recipient identity is supplied");
            };
            receiving.layout = NetworkLayout::RouterClient { connect };
            let node = Application::new(Counter::new().stop_on_shutdown())
                .open_node(receiving)
                .await
                .unwrap_or_else(|_| {
                    panic!("the actual Counter Node connects securely to the Router")
                });
            let runtime = node.runtime_id();
            let mut outgoing = configuration(&address, &EndpointRole::Caller);
            // Both health Queries and the protected Query can retain native timeout captures after EOF.
            outgoing.max_retained_queries = NonZeroUsize::new(3).unwrap();
            let NetworkLayout::Peer {
                connect: Some(connect),
                ..
            } = outgoing.layout
            else {
                panic!("the actual caller identity is supplied");
            };
            outgoing.layout = NetworkLayout::RouterClient { connect };
            let caller = zenoh::open(outgoing.to_configuration().unwrap())
                .await
                .expect("the same native caller connects securely to the Router");
            let caller_id = caller.zid().to_string();
            let router_id = router.zid().to_string();
            let health =
                node.session.declare_queryable(HEALTH).await.expect(
                    "the actual receiving connection declares its independent health endpoint",
                );
            let records = RefCell::new(Vec::<ProtectedMessage>::new());
            let authority = RefCell::new(grant);
            let current_claims = RefCell::new(None::<Metadata>);
            let verified = Cell::new(0);
            let checked = Cell::new(0);
            let decoded = Cell::new(0);
            let constructed = Cell::new(0);
            let encoded = Cell::new(0);
            let protected = Cell::new(0);
            let deadline = Instant::now() + Duration::from_secs(30);
            let current =
                |receipt: &recorded::Receipt<Operation>, actual_runtime: &str, target: MailAddr| {
                    let claims = current_claims.borrow();
                    let claims = claims.as_ref().expect("actual binding precedes serving");
                    if claims.runtime_generation.as_ref() != actual_runtime
                        || claims.actor_generation != target.0
                    {
                        return Err(LocalAuthorityRefusal::Scope {
                            cause: VerificationError::Invalid,
                        });
                    }
                    local_current(
                        &authority,
                        receipt,
                        recorded::projection,
                        recorded::identity,
                        claims,
                        &Operation::Increment,
                        deadline,
                    )
                };
            let maximum = NonZeroUsize::new(1).unwrap();
            let export = Export::new(
                PROTECTED,
                ExportLimits {
                    refusals: RefusalRetention::Retain,
                    queued_queries: maximum,
                    retained_queries: maximum,
                    retained_requests: maximum,
                    diagnostics: maximum,
                },
                limits(),
                |bytes| {
                    decoded.set(decoded.get() + 1);
                    serde_json::from_slice::<ReadRequest<Operation>>(bytes)
                        .map(|request| request.bombay.request_sequence)
                },
                reply(
                    |request_sequence| {
                        let constructed = &constructed;
                        Ok::<_, u64>(move |reply_to| {
                            constructed.set(constructed.get() + 1);
                            CounterMessage::IncrementAndReply {
                                request_sequence,
                                reply_to,
                            }
                        })
                    },
                    |_: &recorded::Receipt<Operation>,
                     original: &User<MailAddr, CounterIncremented>| {
                        encoded.set(encoded.get() + 1);
                        serde_json::to_vec(&original.message.request_sequence)
                            .map(Vec::into_boxed_slice)
                    },
                ),
                async |bytes: &[u8], proof: &[u8]| {
                    verified.set(verified.get() + 1);
                    let original = ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    };
                    recorded::verify::<Operation>(&original, &records.borrow(), BINDING)
                        .into_inner()
                },
                async |_: &[u8], _: &recorded::Receipt<Operation>| {
                    protected.set(protected.get() + 1);
                    Ok::<_, Never>(Box::<[u8]>::from([13, 17]))
                },
                |left, right| recorded::identity(left) == recorded::identity(right),
                |receipt: &recorded::Receipt<Operation>, actual_runtime: &str, target: MailAddr| {
                    checked.set(checked.get() + 1);
                    current(receipt, actual_runtime, target)
                },
                &current,
            );
            let (configured, request) = oneshot::channel();
            let mut node = node.exports(export, async |application| {
                let target = application.root().established_recipient();
                let mut claims = metadata(
                    ProviderProfile::Recorded,
                    "counter",
                    caller_id.clone(),
                    runtime.clone(),
                );
                claims.actor_generation = application.actor_address(&target).unwrap().0;
                claims.request_sequence = 53;
                let original = ProtectedMessage {
                    protected: serde_json::to_vec(&ReadRequestRef {
                        bombay: &claims,
                        kind: Purpose::Request,
                        operation: Operation::Increment,
                    })
                    .unwrap()
                    .into_boxed_slice(),
                    proof: Box::from([7, 11]),
                };
                records.borrow_mut().push(ProtectedMessage {
                    protected: original.protected.clone(),
                    proof: original.proof.clone(),
                });
                *current_claims.borrow_mut() = Some(claims);
                configured
                    .send(original)
                    .unwrap_or_else(|_| panic!("actual Work receives the protected pair"));
                Ok::<_, Never>(target)
            });
            let outcome = node
                .run_with(|application| {
                    let caller = &caller;
                    async move {
                        let original = request
                            .await
                            .expect("the actual export binding supplies the request");
                        let querier = caller
                            .declare_querier(PROTECTED)
                            .timeout(Duration::from_secs(60))
                            .consolidation(ConsolidationMode::None)
                            .await
                            .unwrap();
                        let matching = querier.matching_listener().await.unwrap();
                        let initial = querier.matching_status().await.unwrap();
                        let matched = if initial.matching() {
                            initial
                        } else {
                            loop {
                                let status = matching.recv_async().await.unwrap();
                                if status.matching() {
                                    break status;
                                }
                            }
                        };
                        let health_queries = caller
                            .declare_querier(HEALTH)
                            .timeout(Duration::from_secs(60))
                            .consolidation(ConsolidationMode::None)
                            .await
                            .unwrap();
                        let health_matching = health_queries.matching_listener().await.unwrap();
                        let initial = health_queries.matching_status().await.unwrap();
                        let health_matched = if initial.matching() {
                            initial
                        } else {
                            loop {
                                let status = health_matching.recv_async().await.unwrap();
                                if status.matching() {
                                    break status;
                                }
                            }
                        };
                        let healthy = async |marker: u8| {
                            let receiving =
                                health_queries.get().payload(vec![marker]).await.expect(
                                    "same connected caller submits the independent health Query",
                                );
                            let query = health
                                .recv_async()
                                .await
                                .expect("same receiving connection acquires the health Query");
                            let payload = query
                                .payload()
                                .expect("actual health payload")
                                .to_bytes()
                                .into_owned();
                            let published = query.reply(HEALTH, payload.clone()).await;
                            drop(query);
                            let mut replies = Vec::new();
                            let eof = loop {
                                match receiving.recv_async().await {
                                    Ok(reply) => replies.push(reply),
                                    Err(cause) => break cause,
                                }
                            };
                            (
                                marker,
                                payload,
                                published,
                                replies,
                                eof,
                                receiving.is_disconnected(),
                            )
                        };
                        let before = healthy(61).await;
                        let receiving = querier
                            .get()
                            .payload(original.protected.to_vec())
                            .attachment(original.proof.to_vec())
                            .await
                            .expect("the actual protected Query is submitted");
                        let mut replies = Vec::new();
                        let eof = loop {
                            match receiving.recv_async().await {
                                Ok(reply) => replies.push(reply),
                                Err(cause) => break cause,
                            }
                        };
                        let disconnected = receiving.is_disconnected();
                        let after = healthy(67).await;
                        let mut local = application.client::<CounterValue>().unwrap();
                        let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                        let value = local.receive().await;
                        local.close_admission();
                        let tail = local.receive().await;
                        let listener_withdrawal = matching.undeclare().await;
                        let querier_withdrawal = querier.undeclare().await;
                        let health_listener_withdrawal = health_matching.undeclare().await;
                        let health_querier_withdrawal = health_queries.undeclare().await;
                        let health_withdrawal = health.undeclare().await;
                        let shutdown = application.shutdown();
                        (
                            original,
                            matched,
                            before,
                            replies,
                            eof,
                            disconnected,
                            after,
                            admitted,
                            value,
                            tail,
                            listener_withdrawal,
                            querier_withdrawal,
                            health_matched,
                            health_listener_withdrawal,
                            health_querier_withdrawal,
                            health_withdrawal,
                            shutdown,
                        )
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("the actual Counter Node runs its ordinary Work"));
            let (work, report, native, notes, ()) = outcome.into_parts();
            let (observations, cleanup) = work
                .try_into_completed()
                .unwrap_or_else(|_| panic!("actual Work and cleanup results are acquired"));
            let (export, bound, closed, polling, disposal) = node
                .into_parts()
                .unwrap_or_else(|_| panic!("actual Node finalization is acquired"));
            let caller_closed = caller.close().await;
            let router_closed = router.close().await;
            let (origin, native) = native.expect("the independent native root is acquired");
            let report = report.expect("the independent report is acquired");
            let notes = notes.expect("both independent notifications are acquired");
            let (_, _, startup, withdrawal, ingress, rows) = export.into_parts();
            let (
                original,
                matched,
                before,
                replies,
                _eof,
                disconnected,
                after,
                admitted,
                value,
                tail,
                listener_withdrawal,
                querier_withdrawal,
                health_matched,
                health_listener_withdrawal,
                health_querier_withdrawal,
                health_withdrawal,
                shutdown,
            ) = observations;
            cleanup.expect("actual Application cleanup joins");
            closed.expect("actual receiving Node closes");
            caller_closed.expect("actual caller connection closes");
            router_closed.expect("actual Router closes");
            listener_withdrawal.expect("actual protected matching listener withdraws");
            querier_withdrawal.expect("actual protected querier withdraws");
            health_listener_withdrawal.expect("actual health matching listener withdraws");
            health_querier_withdrawal.expect("actual health querier withdraws");
            health_withdrawal.expect("actual independent health endpoint withdraws");
            startup
                .expect("actual declaration attempted")
                .expect("actual declaration succeeded");
            withdrawal
                .expect("actual withdrawal attempted")
                .expect("actual withdrawal succeeded");
            let _counter = completed_actor(origin, native);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            assert!(notes.termination.is_ok() && notes.retirement.is_ok());
            assert!(polling.is_none() && disposal.is_none() && matches!(bound, Some(Ok(_))));
            assert!(ingress.is_empty() && admitted.is_ok() && shutdown.is_ok() && tail.is_none());
            assert!(matched.matching() && health_matched.matching() && disconnected);
            assert_ne!(runtime, caller_id);
            assert_ne!(runtime, router_id);
            assert_ne!(caller_id, router_id);
            for (marker, payload, published, replies, _eof, disconnected) in [before, after] {
                published
                    .expect("the same receiving connection actually publishes its health reply");
                assert_eq!(payload, [marker]);
                assert!(disconnected);
                let [reply] = <[_; 1]>::try_from(replies)
                    .unwrap_or_else(|_| panic!("one actual health reply precedes EOF"));
                let sample = reply
                    .result()
                    .expect("the real secure health reply succeeds");
                assert_eq!(sample.payload().to_bytes().as_ref(), [marker]);
            }
            let expected_value = match (permission, grant) {
                (Permission::Allow, Some(_)) => 1,
                (Permission::Allow, None) | (Permission::Deny, _) => 0,
            };
            assert_eq!(
                value,
                Some(User::new(origin.address(), expected_value)),
                "only double-allow increments the actual Counter once"
            );
            match (permission, grant) {
                (Permission::Deny, _) => {
                    assert!(
                        rows.is_empty(),
                        "native ACL denial prevents protected ingress"
                    );
                    assert!(replies.is_empty());
                    assert_eq!(
                        (
                            verified.get(),
                            checked.get(),
                            decoded.get(),
                            constructed.get(),
                            encoded.get(),
                            protected.get()
                        ),
                        (0, 0, 0, 0, 0, 0)
                    );
                }
                (Permission::Allow, grant) => {
                    let [row] = <[_; 1]>::try_from(rows)
                        .unwrap_or_else(|_| panic!("one actual protected Query is retained"));
                    let (
                        key,
                        raw,
                        verified_receipt,
                        decoded_input,
                        native_reply,
                        encoding,
                        protection,
                        publications,
                        refusals,
                        panics,
                    ) = row.try_into_parts().unwrap_or_else(|_| {
                        panic!("the actual request releases all producer loans")
                    });
                    assert_eq!(raw.0, original.protected);
                    assert_eq!(raw.1, original.proof);
                    let receipt = verified_receipt
                        .expect("actual provider returned")
                        .expect("the original protected pair verifies");
                    let scope = recorded::projection(&receipt);
                    assert_eq!(scope.kind, Purpose::Request);
                    assert_eq!(scope.operation, Operation::Increment);
                    assert_eq!(scope.bombay.caller_session.as_ref(), caller_id);
                    assert_eq!(scope.bombay.runtime_generation.as_ref(), runtime);
                    assert_eq!(scope.bombay.actor_generation, origin.address().0);
                    assert_eq!(recorded::identity(&receipt).0, &BINDING);
                    assert!(
                        decoded_input.is_none()
                            && matches!(panics, (None, None, None, None, None, None))
                    );
                    assert!(refusals.1.is_none() && refusals.2.is_none());
                    if grant.is_none() {
                        assert!(matches!(
                            refusals.0,
                            Some(LocalAuthorityRefusal::GrantRemoved)
                        ));
                        assert!(
                            key.is_none()
                                && native_reply.is_none()
                                && encoding.is_none()
                                && protection.is_none()
                        );
                        assert!(publications.is_empty() && replies.is_empty());
                        assert_eq!(
                            (
                                verified.get(),
                                checked.get(),
                                decoded.get(),
                                constructed.get(),
                                encoded.get(),
                                protected.get()
                            ),
                            (1, 1, 0, 0, 0, 0)
                        );
                    } else {
                        assert!(refusals.0.is_none() && key.is_some());
                        let (constructor, allocated, admission, processed, tail) =
                            native_reply.expect("the native increment custody is retained");
                        assert!(
                            constructor.is_none() && matches!(allocated, Attempt::Returned(Ok(_)))
                        );
                        assert!(matches!(admission, Attempt::Returned(Ok(()))));
                        assert_eq!(
                            processed,
                            Some(Some(User::new(
                                origin.address(),
                                CounterIncremented {
                                    request_sequence: 53
                                }
                            )))
                        );
                        assert_eq!(tail, [None]);
                        let encoding = encoding.expect("actual encoding returned").unwrap();
                        let protection = protection.expect("actual protection returned").unwrap();
                        assert_eq!(encoding.as_ref(), b"53");
                        assert_eq!(protection.as_ref(), [13, 17]);
                        assert_eq!(publications.len(), 1);
                        assert!(publications.iter().all(Result::is_ok));
                        let [reply] = <[_; 1]>::try_from(replies)
                            .unwrap_or_else(|_| panic!("one actual protected reply precedes EOF"));
                        let sample = reply.result().expect("actual protected reply succeeds");
                        assert_eq!(sample.payload().to_bytes().as_ref(), b"53");
                        assert_eq!(sample.attachment().unwrap().to_bytes().as_ref(), [13, 17]);
                        assert_eq!(
                            (
                                verified.get(),
                                checked.get(),
                                decoded.get(),
                                constructed.get(),
                                encoded.get(),
                                protected.get()
                            ),
                            (1, 2, 1, 1, 1, 1)
                        );
                    }
                }
            }
        }),
    )
    .await
    .expect("watchdog supplies no connectivity, ACL, permission or retirement credit");
}
