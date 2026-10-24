//! Live existing Requester consumes a verified, typed protected Counter Read reply.
//! Positive finite test providers and one configured logical binding only.
use super::providers::{
    CounterOperation, ProofScope, ProtectedMessage, ProviderProfile, Purpose, ReadReply,
    ReadReplyRef, ReadRequest, ReadRequestRef, VerificationError, limits, logical_scope, metadata,
    recorded, scheduled,
};
use super::requester::CounterOperation as NativeOperation;
use super::{
    Counter, CounterMessage, CounterProofPurpose, CounterProtectedMeaning, CounterRequester,
    CounterRequesterMessage, CounterRequesterState, CounterRequests, CounterServiceInput,
    CounterValue, Permission, Scope, completed_actor,
};
use bombay::ApplicationScope;
use bombay::actors::ActorExt;
use bombay::behavior::User;
use bombay::prelude::{Application, MailAddr};
use bombay::remote::{
    Attempt, Export, ExportError, ExportLimits, Metadata, RefusalRetention, json, reply,
    try_retain, verify,
};
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use bombay::{ActorFailureAssessment, RetirementAssessment};
use core::ops::AsyncFn;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::timeout;
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

fn configuration(address: &str, role: &EndpointRole) -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let (certificate, private_key) = match role {
        EndpointRole::Receiver => ("recipient-certificate.pem", "recipient-private-key.pem"),
        EndpointRole::Caller => ("caller-certificate.pem", "caller-private-key.pem"),
    };
    let endpoints = ConnectionEndpoints {
        addresses: vec![
            address
                .strip_prefix("tls/")
                .expect("the fixture uses TLS")
                .to_owned(),
        ],
        certificate: CredentialSource::File(certificates.join(certificate)),
        private_key: CredentialSource::File(certificates.join(private_key)),
    };
    let layout = match role {
        EndpointRole::Receiver => NetworkLayout::Peer {
            connect: None,
            listen: Some(endpoints),
        },
        EndpointRole::Caller => NetworkLayout::Peer {
            connect: Some(endpoints),
            listen: None,
        },
    };
    Network {
        layout,
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}
enum EndpointRole {
    Caller,
    Receiver,
}
fn reserved_address() -> String {
    let reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    format!("tls/localhost:{}", address.port())
}
#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "preserve the original fixture's borrowed validation signature during owner relocation"
)]
fn current_scope(
    scope: &ProofScope,
    current: &Metadata,
    operation: &CounterOperation,
    purpose: Purpose,
    deadline: Instant,
) -> Result<(), VerificationError> {
    if scope.bombay != *current
        || scope.operation != *operation
        || scope.kind != purpose
        || Instant::now() >= deadline
    {
        return Err(VerificationError::Invalid);
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one causal trace preserves actual Actions, protected originals, both Nodes and complete native custody through the final requester-state oracle"
)]
#[expect(
    clippy::similar_names,
    reason = "retain distinct native receiver/received values and receipt projection/reply protection in the original trace"
)]
async fn live_read<Receipt>(
    profile: ProviderProfile,
    request_protector: impl AsyncFn(
        &[u8],
        &Metadata,
        Scope,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError>,
    protector: impl AsyncFn(
        &[u8],
        &Receipt,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
    projection: fn(&Receipt) -> (&ProofScope, Scope),
    evidence: fn(&Receipt) -> Option<&[u8]>,
) {
    let address = reserved_address();
    let mut receiver = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("explicit receiver Node startup"));
    let receiver_connection = receiver.session.clone();
    let receiver_session = &receiver_connection;
    let (execution, receiving) = receiver.execute_with(|receiver| async move {
        let target = receiver.root().established_recipient();
        let target_address = receiver.root().address();
        let mut counter = receiver.export_client::<_, CounterValue>(target.clone())
            .unwrap_or_else(|_| panic!("certified actual receiving root"));
        let primed = counter.try_send(CounterMessage::Increment);
        let scope = logical_scope();
        let application = ApplicationScope::new();
        let mut requests = application.external::<CounterRequests>().unwrap();
        let legacy_values = application.external::<CounterValue>().unwrap();
        let requester = CounterRequester { service: requests.recipient(), counter: target,
            replies: legacy_values.recipient(), state: CounterRequesterState::Ready };
        let capacity = Arc::new(Semaphore::new(2));
        let (incoming, mut queries) = mpsc::channel(2);
        let publications = Arc::new(Mutex::new(Vec::new()));
        let recorded_publications = Arc::clone(&publications);
        let queryable = receiver_session.declare_queryable("bombay/live/read")
            .callback(move |query| {
                let original = try_retain(&incoming, &capacity, query);
                recorded_publications.lock().unwrap().push(original);
            }).await.unwrap();
        let mut caller = application.with_root(requester.stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Caller)).await
            .unwrap_or_else(|_| panic!("explicit caller Node startup"));
        let service_requests = &mut requests;
        let query_receiving = &mut queries;
        let caller_connection = caller.session.clone();
        let caller_session = &caller_connection;
        let (caller_execution, caller_receiving) = caller.execute_with(|caller| async move {
            let requester = caller.root().established_recipient();
            let requester_address = caller.root().address();
            let command = caller.export_client::<_, CounterRequests>(requester)
                .unwrap_or_else(|_| panic!("certified actual live requester"));
            let caller_id = caller_session.zid();
            let receiver_id = receiver_session.zid();
            let current = metadata(profile, caller_id.to_string(), receiver_id.to_string());
            let deadline = Instant::now() + Duration::from_secs(30);
            let permission = Permission::Granted;
            // Explicit finite provider assignment: the legacy logical Scope and
            // complete SDK declarations coexist. No cast/hash maps ZID into Scope,
            // and transport identity is not the logical binding/replay namespace.
            let records = RefCell::new(Vec::with_capacity(2));
            let initial_records = records.borrow().len();
            let protection_calls = Cell::new(0);
            let begun = command.try_send(CounterRequesterMessage::BeginProtected { correlation: 23, scope });
            let (replies, mut receiving_reply) = mpsc::channel(2);
            let reply_capacity = Arc::new(Semaphore::new(2));
            let reply_publications = Arc::new(Mutex::new(Vec::new()));
            let callback_replies = Arc::clone(&reply_publications);
            // A driver timeout retains its actual cause and earns no semantic
            // credit. No native Query is issued until the real Actions trigger.
            let trigger = if begun.is_ok() { Some(timeout(Duration::from_secs(3), service_requests.receive()).await) }
                else { None };
            let mut original = None;
            let mut original_allocations = None;
            if matches!(&trigger, Some(Ok(Some(User { message: CounterServiceInput::Verify(23), .. })))) {
                let request_bytes = serde_json::to_vec(&ReadRequestRef { bombay: &current,
                    kind: Purpose::Request, operation: CounterOperation::Read }).unwrap().into_boxed_slice();
                let mut protection = None;
                verify(&request_bytes, &mut protection, |bytes| {
                    protection_calls.set(protection_calls.get() + 1);
                    request_protector(bytes, &current, scope, &records)
                }).await;
                original = Some(match protection.unwrap() {
                    Ok(proof) => {
                        let message = ProtectedMessage { protected: request_bytes, proof };
                        original_allocations = Some((message.protected.as_ptr(), message.proof.as_ptr()));
                        Ok(message)
                    }
                    Err(cause) => Err((request_bytes, cause)),
                });
            }
            let mut wire_reply = None;
            let issued = if let Some(Ok(original)) = &original {
                Some(caller_session.get("bombay/live/read").consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(3)).payload(original.protected.to_vec()).attachment(original.proof.to_vec())
                    .callback(move |reply| {
                        let retained = try_retain(&replies, &reply_capacity, reply);
                        callback_replies.lock().unwrap().push(retained);
                    }).await)
            } else { None };
            let query = if matches!(&issued, Some(Ok(()))) { tokio::select! {
                query = query_receiving.recv() => query,
                reply = receiving_reply.recv(), if wire_reply.is_none() => { wire_reply = reply; None }
            }} else { None };
            let mut received_request = None;
            let mut request_shape = None;
            let mut request_declaration = None;
            let mut request_verification = None;
            let mut request_scope = None;
            let mut admitted = None;
            let mut native_reply = None;
            let mut reply_publication = None;
            let mut produced_reply = None;
            let mut protection = None;
            if let (Some(Ok(Some(User { message: CounterServiceInput::Verify(23), .. }))), Some((query, _))) = (&trigger, &query) {
                if let (Some(bytes), Some(proof)) = (query.payload(), query.attachment()) {
                    received_request = Some(ProtectedMessage { protected: bytes.to_bytes().into_owned().into_boxed_slice(),
                        proof: proof.to_bytes().into_owned().into_boxed_slice() });
                    let received = received_request.as_ref().unwrap();
                    request_shape = Some(json::check(&received.protected, &received.proof, &limits()));
                    if matches!(&request_shape, Some(Ok(()))) {
                        request_declaration = Some(serde_json::from_slice::<ReadRequest>(&received.protected));
                        verify(received, &mut request_verification, |r| provider(r, &records)).await;
                    }
                    if let (Some(Ok(declared)), Some(Ok(receipt))) = (&request_declaration, &request_verification) {
                        let (claims, authorized_logical) = projection(receipt);
                        request_scope = Some(current_scope(claims, &current, &CounterOperation::Read, Purpose::Request, deadline));
                        if matches!(&request_scope, Some(Ok(()))) && declared.bombay == current && declared.kind == Purpose::Request
                            && declared.operation == CounterOperation::Read && matches!(permission, Permission::Granted)
                            && authorized_logical == scope && scope.identity.sequence == current.request_sequence && scope.operation == NativeOperation::Read {
                            admitted = Some(counter.try_send(CounterMessage::Read(counter.recipient())));
                            if matches!(&admitted, Some(Ok(()))) {
                                native_reply = tokio::select! {
                                    result = counter.receive() => result,
                                    reply = receiving_reply.recv() => { wire_reply = reply; None }
                                };
                            }
                            if let Some(reply) = &native_reply {
                                let bytes = serde_json::to_vec(&ReadReplyRef { bombay: &current, kind: Purpose::ReadReply,
                                    operation: CounterOperation::Read, value: &reply.message }).unwrap();
                                produced_reply = Some(bytes);
                                let inputs = (produced_reply.as_ref().unwrap().as_slice(), receipt, &records);
                                verify(&inputs, &mut protection, |(bytes, request, records)| protector(bytes, request, records)).await;
                                if let Some(Ok(proof)) = &protection {
                                    reply_publication = Some(query.reply("bombay/live/read", produced_reply.as_ref().unwrap().as_slice()).attachment(proof.as_ref()).await);
                                }
                            }
                        }
                    }
                }
                if reply_publication.is_none() { reply_publication = Some(query.reply_err("protected read refused").await); }
            }
            if matches!(&issued, Some(Ok(()))) && wire_reply.is_none() { wire_reply = receiving_reply.recv().await; }
            let mut received_reply = None;
            let mut reply_shape = None;
            let mut reply_declaration = None;
            let mut reply_verification = None;
            let mut reply_scope = None;
            let mut delivered = None;
            if let Some((reply, _)) = &wire_reply
                && let Ok(sample) = reply.result() {
                    received_reply = Some(ProtectedMessage { protected: sample.payload().to_bytes().into_owned().into_boxed_slice(),
                        proof: sample.attachment().map(|p| p.to_bytes().into_owned().into_boxed_slice()).unwrap_or_default() });
                    let received = received_reply.as_ref().unwrap();
                    reply_shape = Some(json::check(&received.protected, &received.proof, &limits()));
                    if matches!(&reply_shape, Some(Ok(()))) {
                        reply_declaration = Some(serde_json::from_slice::<ReadReply>(&received.protected));
                        verify(received, &mut reply_verification, |r| provider(r, &records)).await;
                    }
                    if let (Some(Ok(declared)), Some(Ok(receipt))) = (&reply_declaration, &reply_verification) {
                        let (claims, authorized_logical) = projection(receipt);
                        reply_scope = Some(current_scope(claims, &current, &CounterOperation::Read, Purpose::ReadReply, deadline));
                        if matches!(&reply_scope, Some(Ok(()))) && declared.bombay == current && declared.kind == Purpose::ReadReply
                            && declared.operation == CounterOperation::Read && matches!(permission, Permission::Granted) && authorized_logical == scope {
                            delivered = Some(command.try_send(CounterRequesterMessage::ProtectedReply { correlation: 23, scope,
                                meaning: CounterProtectedMeaning::ReadReply(declared.value) }));
                        }
                    }
            }
            let mut additional_reply = None;
            let acknowledged = if matches!(&delivered, Some(Ok(()))) { tokio::select! {
                acknowledged = service_requests.receive() => acknowledged,
                reply = receiving_reply.recv() => { additional_reply = reply; None }
            }} else { None };
            let stopped = caller.shutdown();
            let report = caller.lifecycle().retirement().await;
            (current, original, original_allocations, initial_records, protection_calls.into_inner(), begun, trigger, issued, query,
                received_request, request_shape, request_declaration, request_verification, request_scope,
                admitted, native_reply, produced_reply, protection, reply_publication, wire_reply, received_reply, reply_shape,
                reply_declaration, reply_verification, reply_scope, delivered, acknowledged, additional_reply,
                reply_publications, stopped, report, command, requester_address, caller_id, receiver_id)
        }).unwrap_or_else(|_| panic!("caller Work host entry"));
        let ((), outcome) = tokio::join!(caller_execution, caller_receiving);
        let (outcome, _root_report, caller_native, caller_notifications, ()) = outcome.into_parts();
        let (read, caller_cleanup) = outcome.try_into_completed().unwrap_or_else(|_| panic!("original caller Work"));
        let caller_closed = caller.close().await;
        let undeclared = queryable.undeclare().await;
        let mut remaining_queries = Vec::new();
        while let Ok(original) = queries.try_recv() { remaining_queries.push(original); }
        let stopped = receiver.shutdown();
        let report = receiver.lifecycle().retirement().await;
        (read, caller_native, caller_notifications, caller_cleanup, caller_closed, undeclared,
            remaining_queries, requests, legacy_values, publications, primed, stopped, report, target_address, scope, caller_connection)
    }).unwrap_or_else(|_| panic!("receiver Work host entry"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (outcome, _root_report, receiver_native, receiver_notifications, ()) = outcome.into_parts();
    let (output, receiver_cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original receiver Work"));
    let receiver_closed = receiver.close().await;
    let (
        read,
        caller_native,
        caller_notifications,
        caller_cleanup,
        caller_closed,
        undeclared,
        remaining_queries,
        requests,
        legacy_values,
        publications,
        primed,
        receiver_stopped,
        receiver_report,
        target_address,
        configured_scope,
        caller_connection,
    ) = output;
    let (
        current,
        original,
        original_allocations,
        initial_records,
        protection_calls,
        begun,
        trigger,
        issued,
        query,
        received_request,
        request_shape,
        request_declaration,
        request_verification,
        request_scope,
        admitted,
        native_reply,
        produced_reply,
        protection,
        reply_publication,
        wire_reply,
        received_reply,
        reply_shape,
        reply_declaration,
        reply_verification,
        reply_scope,
        delivered,
        acknowledged,
        additional_reply,
        reply_publications,
        caller_stopped,
        caller_report,
        command,
        requester_address,
        caller_id,
        receiver_id,
    ) = read;
    // All actual Work/native/notification/Queryable/Session cleanup is acquired
    // before observing the domain outcome. Original Query/Reply tuples remain here.
    assert!(receiver_connection.is_closed());
    assert!(caller_connection.is_closed());
    assert!(receiver_cleanup.is_ok() && caller_cleanup.is_ok());
    assert!(receiver_closed.is_ok() && caller_closed.is_ok() && undeclared.is_ok());
    for notifications in [&caller_notifications, &receiver_notifications] {
        let notifications = notifications.as_ref().unwrap();
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    }
    for report in [caller_report, receiver_report] {
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
    let (origin, retirement) = caller_native.unwrap();
    let requester = completed_actor(origin, retirement).into_inner();
    let (origin, retirement) = receiver_native.unwrap();
    let receiver_actor = completed_actor(origin, retirement);
    // This delivery oracle remains finite when the obsolete constant proof is
    // substituted: peer refusal sends a native error, and no ACK is awaited.
    assert!(
        matches!(&delivered, Some(Ok(()))),
        "actor-issued request protection must enable checked reply delivery"
    );
    assert_eq!(initial_records, 0);
    assert_eq!(protection_calls, 1);
    let original = original.as_ref().unwrap().as_ref().unwrap();
    let original_allocations = original_allocations.unwrap();
    assert!(primed.is_ok() && begun.is_ok() && caller_stopped.is_ok() && receiver_stopped.is_ok());
    assert!(matches!(&issued, Some(Ok(()))));
    assert_eq!(target_address, MailAddr::APPLICATION_ROOT);
    assert_eq!(requester_address, MailAddr::APPLICATION_ROOT);
    let caller_id_text = caller_id.to_string();
    let receiver_id_text = receiver_id.to_string();
    assert_ne!(caller_id, receiver_id);
    assert_eq!(current.caller_session.as_ref(), caller_id_text);
    assert_eq!(current.runtime_generation.as_ref(), receiver_id_text);
    let binding = configured_scope.identity.binding;
    assert_eq!(current.deployment.as_ref(), binding.deployment);
    assert_eq!(current.caller.as_ref(), binding.caller);
    assert_eq!(current.controller.as_ref(), binding.controller);
    assert_eq!(current.actor.as_ref(), binding.actor);
    assert_eq!(current.host.as_ref(), binding.host);
    assert_eq!(current.protocol.as_ref(), binding.protocol);
    assert_eq!(current.grant_issuer.as_ref(), configured_scope.grant_issuer);
    assert_eq!(
        current.authority_generation,
        configured_scope.authority_generation
    );
    assert_eq!(current.request_sequence, configured_scope.identity.sequence);
    assert_eq!(original.protected.as_ptr(), original_allocations.0);
    assert_eq!(original.proof.as_ptr(), original_allocations.1);
    let trigger = trigger
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap();
    assert_eq!(trigger.from, requester_address);
    assert!(matches!(trigger.message, CounterServiceInput::Verify(23)));
    assert!(remaining_queries.is_empty());
    let publications = publications.lock().unwrap();
    assert_eq!(publications.len(), 1);
    for original in publications.iter() {
        assert!(original.is_ok());
    }
    let reply_publications = reply_publications.lock().unwrap();
    assert_eq!(reply_publications.len(), 1);
    for original in reply_publications.iter() {
        assert!(original.is_ok());
    }
    let (query, _query_permit) = query.as_ref().unwrap();
    let native_request = query.payload().unwrap().to_bytes();
    let native_proof = query.attachment().unwrap().to_bytes();
    assert_eq!(native_request.as_ref(), original.protected.as_ref());
    assert_eq!(native_proof.as_ref(), original.proof.as_ref());
    let received = received_request.as_ref().unwrap();
    assert_eq!(received.protected.as_ref(), original.protected.as_ref());
    assert_eq!(received.proof.as_ref(), original.proof.as_ref());
    assert!(matches!(&request_shape, Some(Ok(()))));
    assert!(matches!(&request_scope, Some(Ok(()))));
    let declared = request_declaration.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(declared.bombay, current);
    assert_eq!(declared.kind, Purpose::Request);
    assert_eq!(declared.operation, CounterOperation::Read);
    assert_receipt(request_verification.as_ref(), evidence, &[7, 11]);
    assert!(matches!(&admitted, Some(Ok(()))));
    let native = native_reply.as_ref().unwrap();
    assert_eq!(native.from, target_address);
    assert_eq!(native.message, 1);
    assert!(matches!(&reply_publication, Some(Ok(()))));
    let (wire, _reply_permit) = wire_reply.as_ref().unwrap();
    let sample = wire.result().unwrap();
    assert_eq!(sample.key_expr().as_str(), "bombay/live/read");
    let raw_reply = received_reply.as_ref().unwrap();
    assert_eq!(
        produced_reply.as_ref().unwrap().as_slice(),
        raw_reply.protected.as_ref()
    );
    match &protection {
        Some(Ok(proof)) => assert_eq!(proof.as_ref(), raw_reply.proof.as_ref()),
        _ => panic!("actual reply proof must be retained after producer completion"),
    }
    let native_reply_bytes = sample.payload().to_bytes();
    let native_reply_proof = sample.attachment().unwrap().to_bytes();
    assert_eq!(raw_reply.protected.as_ref(), native_reply_bytes.as_ref());
    assert_eq!(raw_reply.proof.as_ref(), native_reply_proof.as_ref());
    assert!(matches!(&reply_shape, Some(Ok(()))));
    assert!(matches!(&reply_scope, Some(Ok(()))));
    let declared = reply_declaration.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(declared.bombay, current);
    assert_eq!(declared.kind, Purpose::ReadReply);
    assert_eq!(declared.operation, CounterOperation::Read);
    assert_eq!(declared.value, native.message);
    assert_receipt(reply_verification.as_ref(), evidence, &[13, 17]);
    assert!(additional_reply.is_none());
    let acknowledged = acknowledged.as_ref().unwrap();
    assert_eq!(acknowledged.from, requester_address);
    assert!(matches!(
        acknowledged.message,
        CounterServiceInput::ProtectedConsumed {
            correlation: 23,
            purpose: CounterProofPurpose::ReadReply,
        }
    ));
    let CounterRequesterState::Protected {
        correlation,
        scope,
        receipts,
    } = &requester.state
    else {
        panic!("the genuine requester retains its protected state");
    };
    assert_eq!(*correlation, 23);
    assert_eq!(*scope, configured_scope);
    // Independent final actor state is the forwarding-law oracle, not a label
    // placed by the service after publication. The actor also emitted the fence.
    assert_eq!(receipts.as_slice(), [CounterProtectedMeaning::ReadReply(1)]);
    drop((command, requests, legacy_values, receiver_actor, requester));
}

fn assert_receipt<Receipt>(
    result: Option<&Result<Receipt, VerificationError>>,
    evidence: fn(&Receipt) -> Option<&[u8]>,
    expected_proof: &[u8],
) {
    match result {
        Some(Ok(receipt)) => {
            if let Some(proof) = evidence(receipt) {
                assert_eq!(proof, expected_proof);
            }
        }
        Some(Err(VerificationError::Declaration(original))) => {
            panic!("original declaration cause: {original}")
        }
        Some(Err(VerificationError::Unavailable(original))) => {
            panic!("original publication cause: {original}")
        }
        Some(Err(VerificationError::ProofCapacity)) => {
            panic!("finite proof record capacity exhausted")
        }
        Some(Err(VerificationError::Invalid)) => panic!("registered bytes/profile refused"),
        None => panic!("the original verification result was not acquired"),
    }
}

#[test]
fn recorded_live_requester_stores_checked_read_reply() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(live_read(
            ProviderProfile::Recorded,
            recorded::protect_request,
            recorded::verify,
            recorded::protect,
            recorded::projection,
            |_| None,
        ));
}
#[test]
fn scheduled_live_requester_stores_checked_read_reply() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(live_read(
            ProviderProfile::Scheduled,
            scheduled::protect_request,
            scheduled::verify,
            scheduled::protect,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}

// Current cold ownership comparison: cancelling a borrowed Node/wire wait does
// not dispose the actual pending provider owned by that same Node operation.
#[expect(
    clippy::too_many_lines,
    reason = "one causal trace retains pending provider custody, independent progress, replay and all cleanup before its oracles"
)]
async fn cold_pending_protection<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError>,
    protector: impl AsyncFn(
        &[u8],
        &Receipt,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
    gated_protector: impl AsyncFn(
        &[u8],
        &Receipt,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
        oneshot::Receiver<()>,
    ) -> Result<Box<[u8]>, VerificationError>,
    projection: fn(&Receipt) -> (&ProofScope, Scope),
    evidence: fn(&Receipt) -> Option<&[u8]>,
    same_request: fn(&Rc<Receipt>, &Rc<Receipt>) -> bool,
) {
    let records = (0..4).map(|_| RefCell::new(Vec::new())).collect::<Vec<_>>();
    let configured = RefCell::new(None);
    let consumers = RefCell::new(Vec::new());
    let snapshots = RefCell::new(Vec::new());
    let verified = Cell::new(0);
    let constructed = Cell::new(0);
    let gated_calls = Cell::new(0);
    let ordinary_calls = Cell::new(0);
    let pending_polls = Cell::new(0);
    let (entered, entering) = oneshot::channel();
    let entered = RefCell::new(Some(entered));
    let transfers = RefCell::new(Vec::new());
    let (release, released) = oneshot::channel();
    let released = RefCell::new(Some(released));
    let (progressed, mut progress) = oneshot::channel();
    let (duplicated, duplicate_verified) = oneshot::channel();
    let duplicated = RefCell::new(Some(duplicated));
    let (configured_requests, requests) = oneshot::channel();
    let maximum = NonZeroUsize::new(4).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let address = reserved_address();
    let mut network = configuration(&address, &EndpointRole::Receiver);
    network.max_retained_queries = maximum;
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(network)
        .await
        .unwrap_or_else(|_| panic!("actual cold Counter opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let current = |receipt: &Rc<Receipt>, actual: &str, target: MailAddr| {
        let (claims, _) = projection(receipt);
        let configured = configured.borrow();
        let Some((first, second)) = configured.as_ref() else {
            return Err(VerificationError::Invalid);
        };
        let expected = match claims.bombay.request_sequence {
            23 => first,
            24 => second,
            _ => return Err(VerificationError::Invalid),
        };
        if claims.bombay.runtime_generation.as_ref() != actual
            || claims.bombay.actor_generation != target.0
        {
            return Err(VerificationError::Invalid);
        }
        current_scope(
            claims,
            expected,
            &CounterOperation::Read,
            Purpose::Request,
            deadline,
        )
    };
    let export = Export::new(
        "bombay/protection/cold",
        ExportLimits {
            refusals: RefusalRetention::Retain,
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
        },
        limits(),
        |bytes| serde_json::from_slice::<ReadRequest>(bytes).map(|request| request.operation),
        reply(
            |CounterOperation::Read| {
                let constructed = &constructed;
                Ok::<_, CounterOperation>(move |reply_to| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Read(reply_to)
                })
            },
            |receipt: &Rc<Receipt>, original: &User<MailAddr, u64>| {
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &projection(receipt).0.bombay,
                    kind: Purpose::ReadReply,
                    operation: CounterOperation::Read,
                    value: &original.message,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let index = verified.get();
            verified.set(index + 1);
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            let receipt = Rc::new(provider(&original, &records[index]).await?);
            let (claims, logical) = projection(&receipt);
            // This observes every metadata field; verification still uses the
            // original protected bytes and proof, never this serialization.
            let observed_metadata = serde_json::to_vec(&claims.bombay)
                .expect("actual receipt metadata serializes for observation");
            snapshots.borrow_mut().push((
                (bytes.as_ptr(), proof.as_ptr()),
                Rc::as_ptr(&receipt),
                claims.bombay.caller.as_ptr(),
                evidence(&receipt).map(<[u8]>::as_ptr),
                observed_metadata,
                claims.kind,
                claims.operation,
                logical,
            ));
            consumers.borrow_mut().push(Rc::clone(&receipt));
            if index == 2 {
                let duplicated = duplicated
                    .borrow_mut()
                    .take()
                    .expect("the actual duplicate verification publishes its one receipt fence");
                transfers.borrow_mut().push(duplicated.send(()));
            }
            Ok::<_, VerificationError>(receipt)
        },
        async |bytes: &[u8], receipt: &Rc<Receipt>| match projection(receipt)
            .0
            .bombay
            .request_sequence
        {
            23 => {
                gated_calls.set(gated_calls.get() + 1);
                let released = released
                    .borrow_mut()
                    .take()
                    .expect("the one actual A protection producer owns its release");
                let mut production = pin!(gated_protector(bytes, receipt, &records[0], released));
                poll_fn(|context| {
                    let observation = production.as_mut().poll(context);
                    if observation.is_pending() {
                        pending_polls.set(pending_polls.get() + 1);
                        let entered = entered.borrow_mut().take();
                        if let Some(entered) = entered {
                            transfers.borrow_mut().push(entered.send(()));
                        }
                    }
                    observation
                })
                .await
            }
            24 => {
                ordinary_calls.set(ordinary_calls.get() + 1);
                protector(bytes, receipt, &records[1]).await
            }
            _ => Err(VerificationError::Invalid),
        },
        same_request,
        &current,
        &current,
    );
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut first = metadata(profile, runtime.clone(), runtime.clone());
        let target_address = application
            .actor_address(&target)
            .unwrap_or_else(|_| panic!("the actual root is certified"));
        first.actor_generation = target_address.0;
        let mut second = metadata(profile, runtime.clone(), runtime.clone());
        second.actor_generation = target_address.0;
        second.request_sequence = 24;
        let first_scope = logical_scope();
        let mut second_scope = first_scope;
        second_scope.identity.sequence = 24;
        let mut duplicate_scope = first_scope;
        duplicate_scope.authority_generation = 20;
        let first_bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: &first,
            kind: Purpose::Request,
            operation: CounterOperation::Read,
        })
        .unwrap();
        let second_bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: &second,
            kind: Purpose::Request,
            operation: CounterOperation::Read,
        })
        .unwrap();
        let mut conflict_bytes = first_bytes.clone();
        conflict_bytes.push(b' ');
        let originals = [
            first_bytes.clone(),
            second_bytes,
            first_bytes,
            conflict_bytes,
        ]
        .into_iter()
        .zip([first_scope, second_scope, duplicate_scope, first_scope])
        .enumerate()
        .map(|(index, (bytes, scope))| {
            records[index].borrow_mut().push((
                scope,
                ProtectedMessage {
                    protected: bytes.clone().into_boxed_slice(),
                    proof: Box::from([7, 11]),
                },
            ));
            ProtectedMessage {
                protected: bytes.into_boxed_slice(),
                proof: Box::from([7, 11]),
            }
        })
        .collect::<Vec<_>>();
        *configured.borrow_mut() = Some((first, second));
        configured_requests
            .send(originals)
            .unwrap_or_else(|_| panic!("actual Work retains all configured originals"));
        Ok::<_, VerificationError>(target)
    });
    let mut completed = None;
    let (first_poll, progress, second_poll, references, begun_calls, released, outcome) = {
        let session = &session;
        let mut operation = pin!(node.run_with(move |application| async move {
            let originals = requests
                .await
                .expect("the actual binder supplies four originals");
            let mut ordinary = application
                .export_client::<_, CounterValue>(application.root().established_recipient())
                .unwrap_or_else(|_| panic!("actual ordinary Counter client"));
            let primed = ordinary.try_send(CounterMessage::Increment);
            let first_get = session
                .get("bombay/protection/cold")
                .allowed_destination(Locality::SessionLocal)
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(60))
                .payload(originals[0].protected.to_vec())
                .attachment(originals[0].proof.to_vec())
                .await;
            let first = first_get
                .as_ref()
                .expect("actual A Query constructor succeeds");
            let entering = entering.await;
            let advanced = ordinary.try_send(CounterMessage::Increment);
            let second_get = session
                .get("bombay/protection/cold")
                .allowed_destination(Locality::SessionLocal)
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(60))
                .payload(originals[1].protected.to_vec())
                .attachment(originals[1].proof.to_vec())
                .await;
            let second = second_get
                .as_ref()
                .expect("actual B Query constructor succeeds");
            let mut second_replies = Vec::new();
            let second_end = loop {
                match second.recv_async().await {
                    Ok(original) => second_replies.push(original),
                    Err(cause) => break Err::<(), _>(cause),
                }
            };
            let duplicate_get = session
                .get("bombay/protection/cold")
                .allowed_destination(Locality::SessionLocal)
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(60))
                .payload(originals[2].protected.to_vec())
                .attachment(originals[2].proof.to_vec())
                .await;
            let duplicate = duplicate_get
                .as_ref()
                .expect("actual duplicate Query starts");
            let duplicate_verified = duplicate_verified.await;
            let conflict_get = session
                .get("bombay/protection/cold")
                .allowed_destination(Locality::SessionLocal)
                .consolidation(ConsolidationMode::None)
                .timeout(Duration::from_secs(60))
                .payload(originals[3].protected.to_vec())
                .attachment(originals[3].proof.to_vec())
                .await;
            let conflict = conflict_get.as_ref().expect("actual conflict Query starts");
            let mut conflict_replies = Vec::new();
            let conflict_end = loop {
                match conflict.recv_async().await {
                    Ok(original) => conflict_replies.push(original),
                    Err(cause) => break Err::<(), _>(cause),
                }
            };
            let first_wait = {
                let mut receiving = pin!(first.recv_async());
                receiving
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let duplicate_wait = {
                let mut receiving = pin!(duplicate.recv_async());
                receiving
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            };
            let progressed = progressed.send(());
            let mut first_replies = Vec::new();
            let first_end = loop {
                match first.recv_async().await {
                    Ok(original) => first_replies.push(original),
                    Err(cause) => break Err::<(), _>(cause),
                }
            };
            let mut duplicate_replies = Vec::new();
            let duplicate_end = loop {
                match duplicate.recv_async().await {
                    Ok(original) => duplicate_replies.push(original),
                    Err(cause) => break Err::<(), _>(cause),
                }
            };
            ordinary.close_admission();
            let mut tail = Vec::new();
            while let Some(original) = ordinary.receive().await {
                tail.push(original);
            }
            let stopped = application.shutdown();
            let report = application.lifecycle().retirement().await;
            (
                originals,
                primed,
                advanced,
                entering,
                first_get,
                second_get,
                duplicate_get,
                conflict_get,
                duplicate_verified,
                first_wait,
                duplicate_wait,
                progressed,
                first_replies,
                second_replies,
                duplicate_replies,
                conflict_replies,
                (first_end, second_end, duplicate_end, conflict_end),
                tail,
                stopped,
                report,
            )
        }));
        // This borrowed wait ends only after actual B wire completion and conflict
        // EOF; it leaves the same Node operation and gated producer owned here.
        let (first_poll, progress) = {
            let mut waiting = pin!(poll_fn(|context| {
                let observation = match operation.as_mut().poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(original) => {
                        completed = Some(original);
                        Poll::Ready(())
                    }
                };
                match progress.try_recv() {
                    Ok(original) => Poll::Ready((observation, Ok(original))),
                    Err(TryRecvError::Empty) if completed.is_none() => Poll::Pending,
                    Err(cause) => Poll::Ready((observation, Err(cause))),
                }
            }));
            waiting.as_mut().await
        };
        let references = consumers.borrow().first().map(Rc::strong_count);
        let begun_calls = (
            gated_calls.get(),
            ordinary_calls.get(),
            records[0].borrow().len(),
        );
        let second_poll = if completed.is_none() {
            let mut waiting = pin!(poll_fn(|context| operation.as_mut().poll(context)));
            match waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Pending => Poll::Pending,
                Poll::Ready(original) => {
                    completed = Some(original);
                    Poll::Ready(())
                }
            }
        } else {
            Poll::Ready(())
        };
        let released = release.send(());
        let outcome = match completed.take() {
            Some(original) => original,
            None => operation.await,
        };
        (
            first_poll,
            progress,
            second_poll,
            references,
            begun_calls,
            released,
            outcome,
        )
    };
    let outcome = outcome.unwrap_or_else(|_| panic!("actual cold Work host entry"));
    let (export, bound, closed, polling, disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual binding and Session close are retained"));
    let (work, report, native, notes, ()) = outcome.into_parts();
    let (observations, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("all original Work observations survive"));
    let (_inputs, _declarations, startup, withdrawal, diagnostics, requests) = export.into_parts();
    let rows = requests
        .into_iter()
        .map(|request| {
            request
                .try_into_parts()
                .unwrap_or_else(|_| panic!("actual producer custody leases end"))
        })
        .collect::<Vec<_>>();
    let (origin, retirement) = native.expect("actual Counter root remains");
    let root_address = origin.address();
    let _counter = completed_actor(origin, retirement);
    let report = report.expect("independent root report remains");
    let notes = notes.expect("both actual notification receipts remain");
    let (
        originals,
        primed,
        advanced,
        entering,
        first_get,
        second_get,
        duplicate_get,
        conflict_get,
        duplicate_verified,
        first_wait,
        duplicate_wait,
        progressed,
        first_replies,
        second_replies,
        duplicate_replies,
        conflict_replies,
        endings,
        tail,
        stopped,
        work_report,
    ) = observations;
    let snapshots = snapshots.borrow();
    let transfers = transfers.borrow();
    let consumers = consumers.replace(Vec::new());
    let same_receipts = consumers.iter().map(Rc::as_ptr).collect::<Vec<_>>();
    drop(consumers);
    let record_counts = records
        .iter()
        .map(|records| records.borrow().len())
        .collect::<Vec<_>>();
    // Every application/native/wire/withdrawal/Session result is owned above.
    assert!(cleanup.is_ok() && closed.is_ok() && session.is_closed());
    assert!(notes.termination.is_ok() && notes.retirement.is_ok());
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(work_report.retirement(), RetirementAssessment::Established);
    assert_eq!(
        work_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(matches!(&bound, Some(Ok(_))) && polling.is_none() && disposal.is_none());
    assert!(matches!(&startup, Some(Ok(()))) && matches!(&withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty() && primed.is_ok() && advanced.is_ok() && stopped.is_ok());
    let second_native = rows
        .get(1)
        .and_then(|row| row.4.as_ref())
        .and_then(|native| native.3.as_ref())
        .and_then(Option::as_ref)
        .map(|original| (original.from, original.message));
    assert_eq!(
        second_native,
        Some((root_address, 2)),
        "independent native Read2 completes while A protection remains pending"
    );
    assert!(
        first_get.is_ok() && second_get.is_ok() && duplicate_get.is_ok() && conflict_get.is_ok()
    );
    assert!(matches!(first_poll, Poll::Pending) && matches!(second_poll, Poll::Pending));
    assert!(matches!(first_wait, Poll::Pending) && matches!(duplicate_wait, Poll::Pending));
    assert!(progress.is_ok() && progressed.is_ok() && entering.is_ok() && released.is_ok());
    assert!(duplicate_verified.is_ok());
    assert_eq!(references, Some(2));
    assert_eq!(begun_calls, (1, 1, 1));
    assert!(pending_polls.get() >= 1);
    assert_eq!(
        (
            verified.get(),
            constructed.get(),
            gated_calls.get(),
            ordinary_calls.get()
        ),
        (4, 2, 1, 1)
    );
    assert_eq!(transfers.len(), 2);
    assert!(transfers.iter().all(Result::is_ok));
    assert_eq!(record_counts, [2, 2, 1, 1]);
    assert!(tail.is_empty() && conflict_replies.is_empty());
    assert!(endings.0.is_err() && endings.1.is_err() && endings.2.is_err() && endings.3.is_err());
    assert_eq!(rows.len(), 4);
    assert_eq!(snapshots.len(), 4);
    let first_key = rows[0].0.as_ref().expect("actual A canonical key");
    let second_key = rows[1].0.as_ref().expect("actual B canonical key");
    let duplicate_key = rows[2]
        .0
        .as_ref()
        .expect("actual duplicate canonical identity");
    assert_eq!(first_key, duplicate_key);
    assert_ne!(first_key, second_key);
    assert!(rows[3].0.is_none());
    assert!(matches!(&rows[3].8.2, Some(ExportError::Conflict)));
    for (index, row) in rows.into_iter().enumerate() {
        let (
            key,
            raw,
            receipt,
            decoded,
            native,
            encoding,
            reply_protection,
            publications,
            refusals,
            panics,
        ) = row;
        let receipt = receipt
            .expect("the actual provider result is retained")
            .unwrap_or_else(|_| panic!("configured provider accepts the original"));
        let count = Rc::strong_count(&receipt);
        let pointer = Rc::as_ptr(&receipt);
        let receipt = Rc::try_unwrap(receipt).unwrap_or_else(|_| {
            panic!("the exact nominal receipt returns after producer disposal")
        });
        let (claims, logical) = projection(&receipt);
        let mut expected_scope = logical_scope();
        if index == 1 {
            expected_scope.identity.sequence = 24;
        }
        if index == 2 {
            expected_scope.authority_generation = 20;
        }
        let snapshot = &snapshots[index];
        let receipt_proof = evidence(&receipt);
        let proof_pointer = receipt_proof.map(<[u8]>::as_ptr);
        let observed_metadata = serde_json::to_vec(&claims.bombay)
            .expect("terminal receipt metadata serializes for observation");
        assert_eq!(count, 1);
        assert_eq!(pointer, snapshot.1);
        assert_eq!(pointer, same_receipts[index]);
        assert_eq!((raw.0.as_ptr(), raw.1.as_ptr()), snapshot.0);
        assert_eq!(raw.0, originals[index].protected);
        assert_eq!(raw.1, originals[index].proof);
        assert_eq!(observed_metadata, snapshot.4);
        assert_eq!(logical, expected_scope);
        assert_eq!(
            (claims.kind, claims.operation, logical),
            (snapshot.5, snapshot.6, snapshot.7)
        );
        assert_eq!(claims.bombay.caller.as_ptr(), snapshot.2);
        assert_eq!(proof_pointer, snapshot.3);
        if let Some(proof) = receipt_proof {
            assert_eq!(proof, [7, 11]);
        }
        assert!(matches!(&panics, (None, None, None, None, None, None)));
        assert!(refusals.0.is_none() && refusals.1.is_none());
        if index < 2 {
            let (constructor, client, admission, received, tail) =
                native.expect("real native Read owner");
            let received = received
                .expect("actual first receive occurs")
                .expect("actual User remains");
            let Attempt::Returned(Ok(client)) = client else {
                panic!("actual native client construction succeeds");
            };
            let decoded_reply = serde_json::from_slice::<ReadReply>(
                encoding
                    .as_ref()
                    .expect("actual encoding acquired")
                    .as_ref()
                    .expect("actual JSON encoding succeeds"),
            );
            let decoded_reply = decoded_reply.expect("actual wire reply retains typed meaning");
            assert!(constructor.is_none() && decoded.is_none() && key.is_some());
            assert!(matches!(&admission, Attempt::Returned(Ok(()))));
            assert_eq!(received.from, root_address);
            assert_eq!(received.message, index as u64 + 1);
            assert_eq!(decoded_reply.value, received.message);
            assert_eq!(decoded_reply.bombay, claims.bombay);
            assert_eq!(
                (decoded_reply.kind, decoded_reply.operation),
                (Purpose::ReadReply, CounterOperation::Read)
            );
            assert!(matches!(tail.as_slice(), [None]));
            assert!(matches!(&reply_protection, Some(Ok(proof)) if proof.as_ref() == [13, 17]));
            assert_eq!(publications.len(), 1);
            assert!(publications.iter().all(Result::is_ok) && refusals.2.is_none());
            drop(client);
        } else {
            assert!(
                decoded.is_none()
                    && native.is_none()
                    && encoding.is_none()
                    && reply_protection.is_none()
            );
            if index == 2 {
                assert!(refusals.2.is_none());
            }
            assert_eq!(
                logical.authority_generation,
                if index == 2 { 20 } else { 19 }
            );
            assert_eq!(publications.len(), usize::from(index == 2));
            assert!(publications.iter().all(Result::is_ok));
        }
    }
    for (replies, expected, index) in [
        (&first_replies, 1, 0),
        (&second_replies, 2, 1),
        (&duplicate_replies, 1, 0),
    ] {
        assert_eq!(replies.len(), 1);
        let result = replies[0].result();
        let sample = result.as_ref().expect("actual protected reply sample");
        let bytes = sample.payload().to_bytes();
        let proof = sample.attachment().expect("actual reply proof").to_bytes();
        let reply = serde_json::from_slice::<ReadReply>(&bytes).expect("actual protected DTO");
        let reply_metadata = serde_json::to_vec(&reply.bombay)
            .expect("actual wire metadata serializes for observation");
        let records = records[index].borrow();
        let original = &records[1].1;
        assert_eq!(sample.key_expr().as_str(), "bombay/protection/cold");
        assert_eq!(reply.value, expected);
        assert_eq!(reply_metadata, snapshots[index].4);
        assert_eq!(bytes.as_ref(), original.protected.as_ref());
        assert_eq!(proof.as_ref(), original.proof.as_ref());
        assert_eq!(
            (reply.kind, reply.operation),
            (Purpose::ReadReply, CounterOperation::Read)
        );
        assert_eq!(proof.as_ref(), [13, 17]);
    }
}

#[test]
#[expect(
    clippy::large_futures,
    reason = "poll the complete finite recorded provider trace with its concrete retained native outcomes"
)]
fn recorded_cold_protection_preserves_requests_and_replay() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                cold_pending_protection(
                    ProviderProfile::Recorded,
                    recorded::verify,
                    recorded::protect,
                    recorded::protect_when_released,
                    recorded::projection,
                    |_| None,
                    |left, right| {
                        recorded::projection(left).1.identity
                            == recorded::projection(right).1.identity
                    },
                ),
            )
            .await
            .expect("finite pending provider trace completes before native expiry");
        });
}

#[test]
#[expect(
    clippy::large_futures,
    reason = "poll the complete finite scheduled provider trace with its concrete retained native outcomes"
)]
fn scheduled_cold_protection_preserves_requests_and_replay() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(
                Duration::from_secs(40),
                cold_pending_protection(
                    ProviderProfile::Scheduled,
                    scheduled::verify,
                    scheduled::protect,
                    scheduled::protect_when_released,
                    scheduled::projection,
                    |receipt| Some(scheduled::proof(receipt)),
                    |left, right| {
                        scheduled::projection(left).1.identity
                            == scheduled::projection(right).1.identity
                    },
                ),
            )
            .await
            .expect("finite pending provider trace completes before native expiry");
        });
}
