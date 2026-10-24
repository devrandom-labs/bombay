//! Foreign client authentication with matched controls; no full R20 claim.
use super::{counter, custody, protocol};
use crate::actors::ActorExt;
use crate::behavior::{Never, User};
use crate::remote::{
    Attempt, CredentialSource, Export, ExportLimits, Network, NetworkLayout, RefusalRetention,
    reply,
};
use crate::{ActorFailureAssessment, Application, MailAddr, RetirementAssessment};
use futures_util::future::Either;
use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;

use counter::{Counter, CounterMessage, CounterValue};
use custody::completed_actor;
use protocol::{
    Binding, CounterOperation, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply,
    ReadReplyRef, ReadRequest, ReadRequestRef, VerificationError, configuration, limits, metadata,
    recorded, reserved_address,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    reason = "one listener trace retains both matched controls, the original foreign open result and complete cleanup"
)]
async fn foreign_client_is_refused_while_the_listener_remains_usable() {
    timeout(Duration::from_secs(10), async {
        let address = reserved_address();
        let node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("the public listening Node opens"));
        let runtime = node.runtime_id();
        let connection = configuration(&address, &EndpointRole::Caller);
        let mut foreign_connection = configuration(&address, &EndpointRole::Caller);
        let foreign = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/prds/static-remote-actors/transport-custody/certificates/foreign");
        let NetworkLayout::Peer {
            connect: Some(endpoint),
            listen: None,
        } = &mut foreign_connection.layout
        else {
            panic!("the foreign fixture changes only a connecting client's identity");
        };
        endpoint.certificate = CredentialSource::File(foreign.join("localhost-certificate.pem"));
        endpoint.private_key = CredentialSource::File(foreign.join("localhost-private-key.pem"));
        let originals = |network: &Network| {
            let NetworkLayout::Peer {
                connect: Some(endpoint),
                listen: None,
            } = &network.layout
            else {
                panic!("the original connecting Network remains a connect-only Peer");
            };
            let [address] = endpoint.addresses.as_slice() else {
                panic!("the original single listening address remains supplied");
            };
            let paths = [
                &network.deployment_roots,
                &endpoint.certificate,
                &endpoint.private_key,
            ]
            .map(|source| match source {
                CredentialSource::File(path) => {
                    let bytes = path.as_os_str().as_encoded_bytes();
                    (bytes.as_ptr(), bytes.len())
                }
                CredentialSource::Base64(_) => panic!("the actual test credentials are files"),
            });
            (
                network.max_retained_queries,
                endpoint.addresses.as_ptr(),
                address.as_ptr(),
                address.len(),
                paths,
            )
        };
        let foreign_originals = originals(&foreign_connection);
        let connect = async |network: &Network| {
            let mut native_configuration = network.to_configuration().unwrap();
            // Private fixture policy: await one actual Peer connection attempt.
            // Public Peer startup/retry policy and every TLS input stay unchanged.
            native_configuration
                .insert_json5("connect/timeout_ms", "0")
                .unwrap();
            native_configuration
                .insert_json5("connect/exit_on_failure", "true")
                .unwrap();
            zenoh::open(native_configuration).await
        };
        let peer = connect(&connection)
            .await
            .expect("the first matched native Peer connects");
        let requests = RefCell::new(Vec::<ProtectedMessage>::new());
        let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
        let binding = Binding {
            deployment: "two-node-read-study",
            caller: "caller",
            controller: "owner",
            actor: "counter",
            host: "receiver",
            protocol: "counter",
            target_assignment: "one-listening-counter",
            logical_session: 401,
        };
        let current = |receipt: &recorded::Receipt<CounterOperation>,
                       current_runtime: &str,
                       target: MailAddr| {
            let scope = recorded::projection(receipt);
            // The existing exact registered byte/proof pair authorizes each
            // actual caller Session. A first caller's ZID is not a second policy.
            if scope.kind == Purpose::Request
                && scope.operation == CounterOperation::Read
                && scope.bombay.runtime_generation.as_ref() == current_runtime
                && scope.bombay.actor_generation == target.0
                && recorded::identity(receipt).0 == &binding
            {
                Ok(())
            } else {
                Err(VerificationError::Invalid)
            }
        };
        let maximum = NonZeroUsize::new(2).unwrap();
        let export = Export::new(
            "bombay/security/matched",
            ExportLimits {
                refusals: RefusalRetention::Retain,
                queued_queries: maximum,
                retained_queries: maximum,
                retained_requests: maximum,
                diagnostics: maximum,
            },
            limits(),
            |bytes: &[u8]| serde_json::from_slice::<ReadRequest<CounterOperation>>(bytes),
            reply(
                |request: ReadRequest<CounterOperation>| match request.operation {
                    CounterOperation::Read => {
                        Ok::<_, ReadRequest<CounterOperation>>(CounterMessage::Read)
                    }
                },
                |receipt: &recorded::Receipt<CounterOperation>, original: &User<MailAddr, u64>| {
                    let scope = recorded::projection(receipt);
                    serde_json::to_vec(&ReadReplyRef {
                        bombay: &scope.bombay,
                        kind: Purpose::ReadReply,
                        operation: scope.operation,
                        value: &original.message,
                    })
                    .map(Vec::into_boxed_slice)
                },
            ),
            async |bytes: &[u8], proof: &[u8]| {
                let original = ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                };
                let registered = requests.borrow();
                recorded::verify::<CounterOperation>(&original, &registered, binding).into_inner()
            },
            async |bytes: &[u8], receipt: &recorded::Receipt<CounterOperation>| {
                let reply: ReadReply<u64, CounterOperation> =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                let original = recorded::projection(receipt);
                if reply.bombay != original.bombay
                    || reply.kind != Purpose::ReadReply
                    || reply.operation != original.operation
                {
                    return Err(VerificationError::Invalid);
                }
                let proof = Box::<[u8]>::from([37_u8, 41]);
                protected_replies.borrow_mut().push(ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.clone(),
                });
                Ok(proof)
            },
            |left, right| recorded::identity(left) == recorded::identity(right),
            &current,
            &current,
        );
        let mut node = node.exports(export, async |application| {
            Ok::<_, Never>(application.root().established_recipient())
        });
        let execution = Box::pin(node.run_with(|application| {
            let requests = &requests;
            let runtime = &runtime;
            let connect = &connect;
            let connection = &connection;
            let foreign_connection = &foreign_connection;
            async move {
                let target = application.root().established_recipient();
                let actor = application.actor_address(&target).unwrap();
                let read = async |peer: zenoh::Session, sequence: u64| {
                    let caller = peer.zid().to_string();
                    let mut claims = metadata(
                        ProviderProfile::Recorded,
                        "counter",
                        caller.clone(),
                        runtime.to_owned(),
                    );
                    claims.actor_generation = actor.0;
                    claims.request_sequence = sequence;
                    let protected = serde_json::to_vec(&ReadRequestRef {
                        bombay: &claims,
                        kind: Purpose::Request,
                        operation: CounterOperation::Read,
                    })
                    .unwrap()
                    .into_boxed_slice();
                    let proof = Box::<[u8]>::from([29_u8, 31]);
                    let payload = protected.to_vec();
                    let attachment = proof.to_vec();
                    requests
                        .borrow_mut()
                        .push(ProtectedMessage { protected, proof });
                    let querier = peer
                        .declare_querier("bombay/security/matched")
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(20))
                        .await
                        .expect("an actual matched native querier opens");
                    let listener = querier
                        .matching_listener()
                        .await
                        .expect("actual matching listener opens");
                    let initial = querier.matching_status().await.unwrap();
                    let matched = if initial.matching() {
                        initial
                    } else {
                        loop {
                            let observed = listener.recv_async().await.unwrap();
                            if observed.matching() {
                                break observed;
                            }
                        }
                    };
                    let receiving = querier
                        .get()
                        .payload(payload)
                        .attachment(attachment)
                        .await
                        .expect("an actual matched protected Read is submitted");
                    let mut replies = Vec::new();
                    let eof = loop {
                        match receiving.recv_async().await {
                            Ok(original) => replies.push(original),
                            Err(cause) => break cause,
                        }
                    };
                    let disconnected = receiving.is_disconnected();
                    let listener_withdrawal = listener.undeclare().await;
                    let querier_withdrawal = querier.undeclare().await;
                    let closed = peer.close().await;
                    (
                        peer,
                        caller,
                        claims,
                        matched,
                        replies,
                        eof,
                        disconnected,
                        listener_withdrawal,
                        querier_withdrawal,
                        closed,
                    )
                };
                let before = read(peer, 23).await;
                let foreign_open = match connect(foreign_connection).await {
                    Err(cause) => Either::Left(cause),
                    Ok(peer) => {
                        let closed = peer.close().await;
                        Either::Right((peer, closed))
                    }
                };
                // No Get is manufactured when native open yields no Session.
                let mut local = application.client::<CounterValue>().unwrap();
                let admitted = local.send(CounterMessage::Read(local.recipient())).await;
                let value = local.receive().await;
                let incremented = local.send(CounterMessage::Increment).await;
                let after_peer = connect(connection)
                    .await
                    .expect("a NEW matched native Peer connects after the foreign attempt");
                let after = read(after_peer, 24).await;
                local.close_admission();
                let local_tail = local.receive().await;
                let shutdown = application.shutdown();
                (
                    target,
                    actor,
                    before,
                    foreign_open,
                    admitted,
                    value,
                    incremented,
                    after,
                    local_tail,
                    shutdown,
                    local,
                )
            }
        }))
        .await
        .unwrap_or_else(|_| panic!("the actual public Node runs supplied Work"));
        let (work, report, native, notes, ()) = execution.into_parts();
        let (observations, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the actual Work output is retained"));
        let (origin, native) = native.expect("the actual native root joined");
        let notes = notes.expect("both actual notifications are acquired");
        let report = report.expect("the independent runtime report is retained");
        let (export, bound, closed, polling, disposal) = node
            .into_parts()
            .unwrap_or_else(|_| panic!("actual Node close and binding are acquired"));
        let (_inputs, _declarations, startup, withdrawal, ingress, rows) = export.into_parts();
        let rows: [_; 2] = rows
            .try_into()
            .unwrap_or_else(|_| panic!("exactly two good protected requests are retained"));
        let (
            target,
            actor,
            before,
            foreign_open,
            admitted,
            value,
            incremented,
            after,
            local_tail,
            shutdown,
            _local,
        ) = observations;
        let expected_local = Some(User::new(actor, 0));
        let foreign_originals_after = originals(&foreign_connection);
        cleanup.expect("actual application cleanup joins");
        closed.expect("actual listening Node connection closes");
        startup
            .expect("native declaration attempted")
            .expect("native declaration succeeds");
        withdrawal
            .expect("native withdrawal attempted")
            .expect("native withdrawal succeeds");
        let bound = bound
            .expect("actual binding returned")
            .expect("actual binding succeeds");
        let foreign_open = match foreign_open {
            Either::Left(cause) => Either::Left(cause),
            Either::Right((peer, closed)) => {
                closed.expect("an unexpectedly authenticated foreign Session is actually closed");
                Either::Right(peer)
            }
        };
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(notes.termination.is_ok() && notes.retirement.is_ok());
        assert!(polling.is_none() && disposal.is_none());
        assert_eq!(bound, target);
        assert!(ingress.is_empty());
        assert!(admitted.is_ok() && incremented.is_ok() && shutdown.is_ok());
        assert_eq!(value, expected_local);
        assert!(local_tail.is_none());
        assert_eq!(actor, origin.address());
        assert_eq!(foreign_originals, foreign_originals_after);
        assert_ne!(before.1, after.1);
        let registered = requests.borrow();
        let [request_before, request_after] = registered.as_slice() else {
            panic!("exactly the two actual good request pairs were registered");
        };
        let registered_replies = protected_replies.borrow();
        let [reply_before, reply_after] = registered_replies.as_slice() else {
            panic!("exactly two actual reply pairs were registered");
        };
        for (((observed, row), request), (registered_reply, expected_value)) in [before, after]
            .into_iter()
            .zip(rows)
            .zip([request_before, request_after])
            .zip([(reply_before, 0), (reply_after, 1)])
        {
            let (
                _peer,
                caller,
                claims,
                matched,
                replies,
                eof,
                disconnected,
                listener_withdrawal,
                querier_withdrawal,
                closed,
            ) = observed;
            listener_withdrawal.expect("actual matched listener withdraws");
            querier_withdrawal.expect("actual matched querier withdraws");
            closed.expect("actual matched Peer closes");
            let (
                key,
                raw,
                verified,
                decoded,
                native_reply,
                encoded,
                protected,
                publications,
                refusals,
                panics,
            ) = row
                .try_into_parts()
                .unwrap_or_else(|_| panic!("joined request has no surviving producer loan"));
            let expected_native = Some(Some(User::new(actor, expected_value)));
            let caller_scope = claims.caller_session.as_ref();
            let actual_caller = caller.as_str();
            assert!(matched.matching() && disconnected);
            let _eof = eof;
            assert!(key.is_some() && decoded.is_none());
            assert_eq!(raw.0, request.protected);
            assert_eq!(raw.1, request.proof);
            assert_eq!(caller_scope, actual_caller);
            let receipt = verified.expect("actual request verifier returned").unwrap();
            let request_scope = recorded::projection(&receipt);
            assert_eq!(request_scope.bombay, claims);
            assert_eq!(request_scope.kind, Purpose::Request);
            assert_eq!(request_scope.operation, CounterOperation::Read);
            let (constructor, allocated, admission, native_value, tail) =
                native_reply.expect("actual Read native tuple is retained");
            assert!(constructor.is_none());
            assert!(matches!(&allocated, Attempt::Returned(Ok(_))));
            assert!(matches!(&admission, Attempt::Returned(Ok(()))));
            assert_eq!(native_value, expected_native);
            assert_eq!(tail, [None]);
            let encoded = encoded.expect("actual reply encoding returned").unwrap();
            let protected = protected
                .expect("actual reply protection returned")
                .unwrap();
            assert_eq!(publications.len(), 1);
            assert!(publications.iter().all(Result::is_ok));
            assert!(matches!(&refusals, (None, None, None)));
            assert!(matches!(&panics, (None, None, None, None, None, None)));
            let [reply] = <[_; 1]>::try_from(replies)
                .unwrap_or_else(|_| panic!("one original reply arrived before actual EOF"));
            let result = reply.result();
            let sample = result.as_ref().expect("actual native reply is a Sample");
            let payload = sample.payload().to_bytes();
            let proof = sample.attachment().expect("actual reply proof").to_bytes();
            assert_eq!(payload.as_ref(), encoded.as_ref());
            assert_eq!(proof.as_ref(), protected.as_ref());
            assert_eq!(registered_reply.protected.as_ref(), payload.as_ref());
            assert_eq!(registered_reply.proof.as_ref(), proof.as_ref());
            let received = ProtectedMessage {
                protected: payload.into_owned().into_boxed_slice(),
                proof: proof.into_owned().into_boxed_slice(),
            };
            let verified_reply =
                recorded::verify::<CounterOperation>(&received, &registered_replies, binding)
                    .into_inner()
                    .expect("actual native reply matches its registered pair");
            let reply_scope = recorded::projection(&verified_reply);
            let decoded_reply: ReadReply<u64, CounterOperation> =
                serde_json::from_slice(&received.protected).unwrap();
            assert_eq!(reply_scope.bombay, claims);
            assert_eq!(reply_scope.kind, Purpose::ReadReply);
            assert_eq!(decoded_reply.bombay, claims);
            assert_eq!(decoded_reply.kind, Purpose::ReadReply);
            assert_eq!(decoded_reply.operation, CounterOperation::Read);
            assert_eq!(decoded_reply.value, expected_value);
        }
        let _counter = completed_actor(origin, native);
        // Both actual matched controls, all independent outcomes and complete
        // cleanup have succeeded before this sole foreign-authentication oracle.
        assert!(
            matches!(&foreign_open, Either::Left(_)),
            "the actual foreign client acquired a native Session"
        );
    })
    .await
    .expect("watchdog supplies no native handshake or authentication credit");
}
