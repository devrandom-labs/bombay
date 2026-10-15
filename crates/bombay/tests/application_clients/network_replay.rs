//! Two real Nodes, typed Task Actions and a terminating Ledger replay comparison.
//! Finite profiles and this Read-only schema are test inputs, not a shipping contract.
//! Positive provider equations only: fixed current authority/permission and one
//! exact replay key; no stale/unavailable/revocation/window/Stop witness here.
use super::counter::CounterValue;
use super::{Counter, CounterMessage, LedgerCommand, MailLedger, Receipts};
use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, EstablishedRecipient, MessageProtocol, Never, Protocol, User,
};
use bombay::lifecycle::{Task, TaskMessage, TaskResult, TaskState};
use bombay::prelude::{Application, MailAddr, StopOnShutdown};
use bombay::remote::{IngressError, Metadata, json, try_retain, verify};
use bombay::{
    ActorFailureAssessment, ActorNotificationReceipts, ActorRetirement, ActorRetirementReport,
    ApplicationClient, RetirementAssessment, RootOrigin, SendError, TrySendError,
};
use core::ops::AsyncFn;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::env;
use std::fmt::Debug;
use std::future::{Future, Ready, ready};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use zenoh::query::{ConsolidationMode, Query, Reply};
use zenoh::{Error as ZenohError, Session};
use zenoh_config::Config;

#[path = "../application_support/mod.rs"]
mod application_support;
use application_support::assert_completed;

#[derive(Debug)]
struct ProtectedMessage {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
type RequestBatch = MessageProtocol<MailAddr, TaskResult<Vec<ProtectedMessage>>>;
type RequestTask = Task<MailAddr, Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
type BatchCommand = TaskMessage<Vec<ProtectedMessage>, EstablishedRecipient<RequestBatch>>;
type SenderRoot = StopOnShutdown<RequestTask>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LedgerOperation {
    Read,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Purpose {
    Request,
    ReadReply,
}
#[derive(Clone, Copy)]
enum ProviderProfile {
    Recorded,
    Scheduled,
}
impl ProviderProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Recorded => "read-record-test-v1",
            Self::Scheduled => "read-scheduled-test-v1",
        }
    }
}
#[derive(Clone, Copy)]
enum ReadRetention {
    ReuseFirst,
    Resend,
}
fn read_retention() -> ReadRetention {
    match env::var("BOMBAY_NODE_READ_REPLAY").as_deref() {
        Err(env::VarError::NotPresent) | Ok("reuse-first") => ReadRetention::ReuseFirst,
        Ok("resend") => ReadRetention::Resend,
        original => panic!("unsupported explicit test comparison: {original:?}"),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest<Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Serialize)]
struct ReadRequestRef<'a, Operation> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply<Value, Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
    value: Value,
}
#[derive(Serialize)]
struct ReadReplyRef<'a, Value, Operation> {
    bombay: &'a Metadata,
    kind: Purpose,
    operation: Operation,
    value: &'a Value,
}
// Provider scope ignores protocol-owned value fields; concrete DTO decoding above
// is independent. Matching the entire registered byte/proof pair remains mandatory.
#[derive(Deserialize)]
struct ProofScope<Operation> {
    bombay: Metadata,
    kind: Purpose,
    operation: Operation,
}
#[derive(Debug)]
enum VerificationError {
    Invalid,
    Declaration(serde_json::Error),
    Unavailable(oneshot::error::RecvError),
}
fn registered<Operation: DeserializeOwned>(
    original: &ProtectedMessage,
    records: &[ProtectedMessage],
    profile: ProviderProfile,
) -> Result<ProofScope<Operation>, VerificationError> {
    let scope: ProofScope<Operation> =
        serde_json::from_slice(&original.protected).map_err(VerificationError::Declaration)?;
    if scope.bombay.provider_profile.as_ref() != profile.name()
        || !records
            .iter()
            .any(|record| record.protected == original.protected && record.proof == original.proof)
    {
        return Err(VerificationError::Invalid);
    }
    Ok(scope)
}
mod recorded {
    use super::*;
    pub(super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
    }
    pub(super) fn projection<Operation>(receipt: &Receipt<Operation>) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(super) fn verify<Operation: DeserializeOwned>(
        original: &ProtectedMessage,
        records: &[ProtectedMessage],
    ) -> Ready<Result<Receipt<Operation>, VerificationError>> {
        ready(
            registered(original, records, ProviderProfile::Recorded).map(|scope| Receipt { scope }),
        )
    }
}
mod scheduled {
    use super::*;
    pub(super) struct Receipt<Operation> {
        scope: ProofScope<Operation>,
        original_proof: Box<[u8]>,
    }
    pub(super) fn projection<Operation>(receipt: &Receipt<Operation>) -> &ProofScope<Operation> {
        &receipt.scope
    }
    pub(super) fn proof<Operation>(receipt: &Receipt<Operation>) -> &[u8] {
        &receipt.original_proof
    }
    pub(super) async fn verify<Operation: DeserializeOwned>(
        original: &ProtectedMessage,
        records: &[ProtectedMessage],
    ) -> Result<Receipt<Operation>, VerificationError> {
        let (published, received) = oneshot::channel();
        let original =
            registered(original, records, ProviderProfile::Scheduled).map(|scope| Receipt {
                scope,
                original_proof: original.proof.clone(),
            });
        let transfer = published.send(original);
        match transfer {
            Ok(()) => received.await.map_err(VerificationError::Unavailable)?,
            Err(original) => original,
        }
    }
}

fn configuration(address: &str, role: &EndpointRole) -> Config {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let (listen, connect, certificate, key, certificate_owner, key_owner) = match role {
        EndpointRole::Receiver => (
            vec![address],
            vec![],
            "listen_certificate",
            "listen_private_key",
            "recipient-certificate.pem",
            "recipient-private-key.pem",
        ),
        EndpointRole::Caller => (
            vec![],
            vec![address],
            "connect_certificate",
            "connect_private_key",
            "caller-certificate.pem",
            "caller-private-key.pem",
        ),
    };
    let mut tls = serde_json::json!({
        "root_ca_certificate": certificates.join("root-ca.pem"),
        "enable_mtls": true, "verify_name_on_connect": true, "use_public_pki": false
    });
    tls[certificate] = serde_json::json!(certificates.join(certificate_owner));
    tls[key] = serde_json::json!(certificates.join(key_owner));
    let declaration = serde_json::json!({
        "mode": "peer", "queries_max_retained": 2,
        "listen": {"endpoints": listen},
        "connect": {"endpoints": connect, "timeout_ms": 2000, "exit_on_failure": true},
        "scouting": {"multicast": {"enabled": false, "listen": false}, "gossip": {"enabled": false}},
        "transport": {"link": {"protocols": ["tls"], "tls": tls}}
    }).to_string();
    Config::from_deserializer(&mut serde_json::Deserializer::from_str(&declaration)).unwrap()
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
fn metadata(
    profile: ProviderProfile,
    protocol: &str,
    caller: String,
    receiver: String,
) -> Metadata {
    Metadata {
        wire_version: 1,
        schema_version: 1,
        provider_profile: profile.name().into(),
        deployment: "two-node-read-study".into(),
        caller: "caller".into(),
        caller_session: caller.into(),
        controller: "owner".into(),
        actor: protocol.into(),
        host: "receiver".into(),
        protocol: protocol.into(),
        grant_issuer: "issuer".into(),
        runtime_generation: receiver.into(),
        actor_generation: 0,
        authority_generation: 19,
        request_sequence: 23,
    }
}
fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        proof_bytes: NonZeroUsize::new(64).unwrap(),
        token_bytes: NonZeroUsize::new(128).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    }
}
fn current_scope<Operation: Eq>(
    scope: &ProofScope<Operation>,
    current: &Metadata,
    operation: &Operation,
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

struct ReadAcquisition<Operation, Receipt, Command, WireValue> {
    invocation: Result<(), ZenohError>,
    query_publication: Option<Result<(), IngressError<Query>>>,
    query: Option<Result<(Query, OwnedSemaphorePermit), mpsc::error::TryRecvError>>,
    original: Option<ProtectedMessage>,
    shape: Option<Result<(), json::Error>>,
    declaration: Option<Result<ReadRequest<Operation>, serde_json::Error>>,
    verification: Option<Result<Receipt, VerificationError>>,
    scope: Option<Result<(), VerificationError>>,
    admission: Option<Result<(), TrySendError<Command>>>,
    source: Option<usize>,
    reply_publication: Option<Result<(), ZenohError>>,
    reply: Option<(Reply, OwnedSemaphorePermit)>,
    reply_original: Option<ProtectedMessage>,
    reply_shape: Option<Result<(), json::Error>>,
    reply_declaration: Option<Result<ReadReply<WireValue, Operation>, serde_json::Error>>,
    reply_verification: Option<Result<Receipt, VerificationError>>,
    reply_scope: Option<Result<(), VerificationError>>,
}
struct ReadStudy<Target, Replies, Operation, Receipt, WireValue>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    current: Metadata,
    target: MailAddr,
    caller_zid: String,
    receiver_zid: String,
    batch: Option<User<MailAddr, TaskResult<Vec<ProtectedMessage>>>>,
    batch_allocations: Vec<(*const u8, *const u8)>,
    begun: Result<(), SendError<BatchCommand>>,
    acquisitions: Vec<ReadAcquisition<Operation, Receipt, Target::Msg, WireValue>>,
    native: Vec<User<MailAddr, Replies::Msg>>,
    first_retirement: Option<ActorRetirementReport>,
    expected_reply: EstablishedRecipient<Replies>,
    #[expect(
        clippy::type_complexity,
        reason = "retain each original Query refusal and its native callback publication"
    )]
    query_callbacks:
        Arc<Mutex<Vec<Result<(), mpsc::error::TrySendError<Result<(), IngressError<Query>>>>>>>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the original Reply and native ingress cause for every callback"
    )]
    reply_callbacks: Vec<Arc<Mutex<Vec<Result<(), IngressError<Reply>>>>>>,
    query_drain: Vec<(Query, OwnedSemaphorePermit)>,
    undeclared: Result<(), ZenohError>,
    sender_cleanup: Result<(), bombay::ApplicationCleanupError>,
    #[expect(
        clippy::type_complexity,
        reason = "retain the concrete Task root and every original native retirement lane"
    )]
    sender_native: Result<
        (
            RootOrigin<SenderRoot>,
            ActorRetirement<SenderRoot, Never, Vec<Never>>,
        ),
        oneshot::error::RecvError,
    >,
    sender_notifications: Result<ActorNotificationReceipts, oneshot::error::RecvError>,
    sender_closed: Result<(), ZenohError>,
    client: ApplicationClient<EstablishedRecipient<Target>, Replies>,
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one causal trace keeps explicit policy inputs and all original acquisition and cleanup products together"
)]
async fn read_study<Target, Replies, Operation, Receipt, WireValue, FirstRetirement>(
    mut client: ApplicationClient<EstablishedRecipient<Target>, Replies>,
    receiver_session: &Session,
    address: &str,
    target: MailAddr,
    protocol: &str,
    operation: Operation,
    profile: ProviderProfile,
    retention: ReadRetention,
    expected: WireValue,
    read: fn(EstablishedRecipient<Replies>) -> Target::Msg,
    encode: fn(&Replies::Msg) -> WireValue,
    mut first_retirement: Option<FirstRetirement>,
    provider: impl AsyncFn(&ProtectedMessage, &[ProtectedMessage]) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<Operation>,
) -> ReadStudy<Target, Replies, Operation, Receipt, WireValue>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
    Operation: Copy + Eq + Serialize + DeserializeOwned,
    WireValue: Serialize + DeserializeOwned,
    FirstRetirement: Future<Output = ActorRetirementReport>,
{
    let query_capacity = Arc::new(Semaphore::new(2));
    let (incoming, mut received) = mpsc::channel(2);
    let (announced, mut announcements) = mpsc::channel(2);
    let query_callbacks = Arc::new(Mutex::new(Vec::new()));
    let callbacks = Arc::clone(&query_callbacks);
    let capacity = Arc::clone(&query_capacity);
    let queryable = receiver_session
        .declare_queryable("bombay/replay/read")
        .callback(move |query| {
            let retained = try_retain(&incoming, &capacity, query);
            let announced = announced.try_send(retained);
            callbacks.lock().unwrap().push(announced);
        })
        .await
        .unwrap();
    let mut sender = Application::new(RequestTask::new().stop_on_shutdown())
        .open_node(configuration(address, &EndpointRole::Caller))
        .await
        .unwrap_or_else(|_| panic!("explicit caller Node setup"));
    let expected_reply = client.recipient();
    let query_receiving = &mut received;
    let (execution, receiving) = sender.execute_with(|application, caller_session| async move {
        let caller_zid = caller_session.zid().to_string();
        let receiver_zid = receiver_session.zid().to_string();
        let current = metadata(profile, protocol, caller_zid.clone(), receiver_zid.clone());
        let deadline = Instant::now() + Duration::from_secs(30);
        let request_bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: &current, kind: Purpose::Request, operation,
        }).unwrap();
        let reply_bytes = serde_json::to_vec(&ReadReplyRef {
            bombay: &current, kind: Purpose::ReadReply, operation, value: &expected,
        }).unwrap();
        let records = vec![
            ProtectedMessage { protected: request_bytes.clone().into_boxed_slice(), proof: Box::from([7, 11]) },
            ProtectedMessage { protected: reply_bytes.into_boxed_slice(), proof: Box::from([13, 17]) },
        ];
        let originals = (0..2).map(|_| ProtectedMessage {
            protected: request_bytes.clone().into_boxed_slice(), proof: Box::from([7, 11]),
        }).collect::<Vec<_>>();
        let batch_allocations = originals.iter().map(|r| (r.protected.as_ptr(), r.proof.as_ptr())).collect();
        let mut requests = application.external::<RequestBatch>().unwrap();
        let begun = requests.send(&application.root().established_recipient(), BatchCommand::Complete {
            result: originals, reply_to: requests.recipient(),
        }).await;
        let batch = if begun.is_ok() { requests.receive().await } else { None };
        let mut native = Vec::new();
        native.try_reserve_exact(2).unwrap();
        let mut acquisitions = Vec::new();
        let mut reply_callbacks = Vec::new();
        let mut joined_first = None;
        if let Some(User { message: TaskResult::Completed(originals), .. }) = &batch {
            for request in originals {
                let reply_capacity = Arc::new(Semaphore::new(2));
                let (incoming_reply, mut received_reply) = mpsc::channel(2);
                let publications = Arc::new(Mutex::new(Vec::new()));
                let callback_publications = Arc::clone(&publications);
                let invocation = caller_session.get("bombay/replay/read")
                    .consolidation(ConsolidationMode::None).timeout(Duration::from_secs(3))
                    .payload(request.protected.to_vec()).attachment(request.proof.to_vec())
                    .callback(move |reply| {
                        let retained = try_retain(&incoming_reply, &reply_capacity, reply);
                        callback_publications.lock().unwrap().push(retained);
                    }).await;
                let mut acquired = ReadAcquisition {
                    invocation, query_publication: None, query: None, original: None, shape: None,
                    declaration: None, verification: None, scope: None, admission: None, source: None,
                    reply_publication: None, reply: None, reply_original: None, reply_shape: None, reply_declaration: None,
                    reply_verification: None, reply_scope: None,
                };
                if acquired.invocation.is_ok() {
                    let publication = tokio::select! {
                        publication = announcements.recv() => publication,
                        reply = received_reply.recv() => { acquired.reply = reply; None }
                    };
                    acquired.query_publication = publication;
                    if matches!(&acquired.query_publication, Some(Ok(()))) {
                        acquired.query = Some(query_receiving.try_recv());
                        if let Some(Ok((query, _permit))) = &acquired.query {
                            if let (Some(payload), Some(proof)) = (query.payload(), query.attachment()) {
                                acquired.original = Some(ProtectedMessage {
                                    protected: payload.to_bytes().into_owned().into_boxed_slice(),
                                    proof: proof.to_bytes().into_owned().into_boxed_slice(),
                                });
                                let original = acquired.original.as_ref().unwrap();
                                acquired.shape = Some(json::check(&original.protected, &original.proof, &limits()));
                                if matches!(&acquired.shape, Some(Ok(()))) {
                                    acquired.declaration = Some(serde_json::from_slice(&original.protected));
                                    verify(original, &mut acquired.verification, |r| provider(r, &records)).await;
                                }
                                if let (Some(Ok(declared)), Some(Ok(receipt))) = (&acquired.declaration, &acquired.verification) {
                                    acquired.scope = Some(current_scope(projection(receipt), &current, &operation,
                                        Purpose::Request, deadline));
                                    if matches!(&acquired.scope, Some(Ok(()))) && declared.bombay == current
                                        && declared.kind == Purpose::Request && declared.operation == operation {
                                        let reuse = match retention {
                                            ReadRetention::ReuseFirst => native.first().map(|_| 0),
                                            ReadRetention::Resend => None,
                                        };
                                        let source = if let Some(index) = reuse {
                                            Some(index)
                                        } else {
                                            acquired.admission = Some(client.try_send(read(client.recipient())));
                                            if matches!(&acquired.admission, Some(Ok(()))) {
                                                let observed = tokio::select! {
                                                    reply = client.receive() => reply,
                                                    reply = received_reply.recv() => { acquired.reply = reply; None }
                                                };
                                                match observed {
                                                    Some(reply) => { native.push(reply); Some(native.len() - 1) }
                                                    None => None,
                                                }
                                            } else { None }
                                        };
                                        if let Some(source) = source {
                                            acquired.source = Some(ptr::from_ref(&native[source]).addr());
                                            let actual = encode(&native[source].message);
                                            let bytes = serde_json::to_vec(&ReadReplyRef {
                                                bombay: &current, kind: Purpose::ReadReply, operation, value: &actual,
                                            }).unwrap();
                                            acquired.reply_publication = Some(query.reply("bombay/replay/read", bytes)
                                                .attachment(vec![13, 17]).await);
                                        } else {
                                            acquired.reply_publication = Some(query.reply_err("native read refused").await);
                                        }
                                    }
                                }
                            }
                            if acquired.reply_publication.is_none() {
                                acquired.reply_publication = Some(query.reply_err("protected read refused").await);
                            }
                        }
                    } else if let Some(Err(refusal)) = &acquired.query_publication {
                        let query = match refusal {
                            IngressError::Capacity { original, .. }
                            | IngressError::Queue { cause: mpsc::error::TrySendError::Full((original, _))
                                | mpsc::error::TrySendError::Closed((original, _)) } => original,
                        };
                        acquired.reply_publication = Some(query.reply_err("retained query refused").await);
                    }
                    if acquired.reply.is_none() { acquired.reply = received_reply.recv().await; }
                    if let Some((reply, _permit)) = &acquired.reply
                        && let Ok(sample) = reply.result() {
                            acquired.reply_original = Some(ProtectedMessage {
                                protected: sample.payload().to_bytes().into_owned().into_boxed_slice(),
                                proof: sample.attachment().map(|p| p.to_bytes().into_owned().into_boxed_slice()).unwrap_or_default(),
                            });
                            let original = acquired.reply_original.as_ref().unwrap();
                            acquired.reply_shape = Some(json::check(&original.protected, &original.proof, &limits()));
                            if matches!(&acquired.reply_shape, Some(Ok(()))) {
                                acquired.reply_declaration = Some(serde_json::from_slice(&original.protected));
                                verify(original, &mut acquired.reply_verification, |r| provider(r, &records)).await;
                                if let Some(Ok(receipt)) = &acquired.reply_verification {
                                    acquired.reply_scope = Some(current_scope(projection(receipt), &current, &operation,
                                        Purpose::ReadReply, deadline));
                                }
                            }
                    }
                }
                if native.len() == 1 && joined_first.is_none()
                    && let Some(retirement) = first_retirement.take() {
                    joined_first = Some(retirement.await);
                }
                reply_callbacks.push(publications);
                acquisitions.push(acquired);
            }
        }
        (current, caller_zid, receiver_zid, batch, batch_allocations, begun,
            native, acquisitions, reply_callbacks, joined_first, client, requests)
    }).unwrap_or_else(|_| panic!("the concrete sender Application enters its host"));
    let ((), (outcome, sender_native, sender_notifications)) = tokio::join!(execution, receiving);
    let (output, sender_cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original sender Work is independently received"));
    let (
        current,
        caller_zid,
        receiver_zid,
        batch,
        batch_allocations,
        begun,
        native,
        acquisitions,
        reply_callbacks,
        joined_first,
        client,
        requests,
    ) = output;
    drop(requests);
    let sender_closed = sender.close().await;
    let undeclared = queryable.undeclare().await;
    let mut query_drain = Vec::new();
    while let Ok(original) = received.try_recv() {
        query_drain.push(original);
    }
    ReadStudy {
        current,
        target,
        caller_zid,
        receiver_zid,
        batch,
        batch_allocations,
        begun,
        acquisitions,
        native,
        first_retirement: joined_first,
        expected_reply,
        query_callbacks,
        reply_callbacks,
        query_drain,
        undeclared,
        sender_cleanup,
        sender_native,
        sender_notifications,
        sender_closed,
        client,
    }
}

fn assert_study<Target, Replies, Operation, Receipt, WireValue>(
    study: &ReadStudy<Target, Replies, Operation, Receipt, WireValue>,
    expected: &WireValue,
    operation: Operation,
    retention: ReadRetention,
) where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
    Operation: Copy + Eq + Debug,
    WireValue: Eq + Debug,
{
    assert!(study.sender_cleanup.is_ok());
    assert!(study.sender_closed.is_ok());
    let notifications = study.sender_notifications.as_ref().unwrap();
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_, retirement) = study.sender_native.as_ref().unwrap();
    let custody = retirement.custody().unwrap();
    assert_eq!(custody.behavior.base().state(), TaskState::Completed);
    assert!(custody.capability_failures.is_empty() && custody.additional_failures.is_empty());
    assert!(custody.unread_owner_cancellation.is_none() && custody.retirement_failures.is_empty());
    assert!(study.begun.is_ok() && study.undeclared.is_ok());
    assert!(study.query_drain.is_empty());
    assert_ne!(study.caller_zid, study.receiver_zid);
    assert_eq!(study.current.caller_session.as_ref(), study.caller_zid);
    assert_eq!(
        study.current.runtime_generation.as_ref(),
        study.receiver_zid
    );
    assert_eq!(study.current.actor_generation, 0);
    assert_eq!(study.target, MailAddr::APPLICATION_ROOT);
    let batch = study.batch.as_ref().unwrap();
    assert_eq!(batch.from, MailAddr::APPLICATION_ROOT);
    let TaskResult::Completed(originals) = &batch.message else {
        panic!("the actual Task sends its complete batch");
    };
    assert_eq!(originals.len(), 2);
    for (original, (protected, proof)) in originals.iter().zip(&study.batch_allocations) {
        assert_eq!(original.protected.as_ptr(), *protected);
        assert_eq!(original.proof.as_ptr(), *proof);
    }
    let callbacks = study.query_callbacks.lock().unwrap();
    assert_eq!(callbacks.len(), 2);
    for result in callbacks.iter() {
        assert!(result.is_ok());
    }
    assert_eq!(study.acquisitions.len(), 2);
    let first = &study.acquisitions[0];
    assert!(matches!(&first.admission, Some(Ok(()))));
    assert!(!study.native.is_empty());
    for actual in &study.native {
        assert_eq!(actual.from, study.target);
    }
    for (index, acquisition) in study.acquisitions.iter().enumerate() {
        assert!(acquisition.invocation.is_ok());
        assert!(matches!(&acquisition.query_publication, Some(Ok(()))));
        assert!(matches!(&acquisition.query, Some(Ok(_))));
        assert!(matches!(&acquisition.shape, Some(Ok(()))));
        let original = acquisition.original.as_ref().unwrap();
        assert_eq!(
            original.protected.as_ref(),
            originals[index].protected.as_ref()
        );
        assert_eq!(original.proof.as_ref(), originals[index].proof.as_ref());
        assert_verification(acquisition.verification.as_ref());
        let declared = acquisition.declaration.as_ref().unwrap().as_ref().unwrap();
        assert_eq!(declared.bombay, study.current);
        assert_eq!(declared.kind, Purpose::Request);
        assert_eq!(declared.operation, operation);
        assert!(matches!(&acquisition.scope, Some(Ok(()))));
        assert!(matches!(&acquisition.reply_publication, Some(Ok(()))));
        let publications = study.reply_callbacks[index].lock().unwrap();
        assert_eq!(publications.len(), 1);
        for result in publications.iter() {
            assert!(result.is_ok());
        }
        let (reply, _permit) = acquisition.reply.as_ref().unwrap();
        if let Some(Ok(declared)) = &acquisition.reply_declaration {
            let sample = reply.result().unwrap();
            assert_eq!(sample.key_expr().as_str(), "bombay/replay/read");
            let raw = acquisition.reply_original.as_ref().unwrap();
            let actual_payload = sample.payload().to_bytes();
            assert_eq!(raw.protected.as_ref(), actual_payload.as_ref());
            assert_eq!(raw.proof.as_ref(), [13, 17]);
            assert_eq!(declared.bombay, study.current);
            assert_eq!(declared.kind, Purpose::ReadReply);
            assert_eq!(declared.operation, operation);
            assert_eq!(&declared.value, expected);
            assert!(matches!(&acquisition.reply_shape, Some(Ok(()))));
            assert_verification(acquisition.reply_verification.as_ref());
            assert!(acquisition.reply_original.is_some());
            assert!(matches!(&acquisition.reply_scope, Some(Ok(()))));
        } else {
            assert!(matches!(retention, ReadRetention::Resend));
            assert_eq!(index, 1);
            let refusal = reply.result().unwrap_err();
            let actual_refusal = refusal.payload().to_bytes();
            assert_eq!(actual_refusal.as_ref(), b"native read refused");
        }
    }
}

fn assert_verification<Receipt>(verification: Option<&Result<Receipt, VerificationError>>) {
    match verification {
        Some(Ok(_)) => {}
        Some(Err(VerificationError::Declaration(cause))) => panic!("original declaration: {cause}"),
        Some(Err(VerificationError::Unavailable(cause))) => {
            panic!("original provider publication: {cause}")
        }
        Some(Err(VerificationError::Invalid)) => panic!("exact byte/proof/profile refused"),
        None => panic!("the actual provider result was not acquired"),
    }
}
fn assert_notifications(receipts: &Result<ActorNotificationReceipts, oneshot::error::RecvError>) {
    let receipts = receipts.as_ref().unwrap();
    assert!(receipts.termination.is_ok());
    assert!(receipts.retirement.is_ok());
}
fn assert_report(report: ActorRetirementReport) {
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
}

async fn counter_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(&ProtectedMessage, &[ProtectedMessage]) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<CounterOperation>,
    proof: fn(&Receipt) -> Option<&[u8]>,
) {
    let retention = read_retention();
    let address = reserved_address();
    let mut node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("explicit receiver Node setup"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let target = application.root().established_recipient();
            let target_address = application.root().address();
            let client = application
                .export_client::<_, CounterValue>(target)
                .unwrap_or_else(|_| panic!("actual root certification and native allocation"));
            let primed = client.try_send(CounterMessage::Increment);
            let study = read_study(
                client,
                session,
                &address,
                target_address,
                "counter",
                CounterOperation::Read,
                profile,
                retention,
                1,
                CounterMessage::Read,
                |value| *value,
                None::<Ready<ActorRetirementReport>>,
                provider,
                projection,
            )
            .await;
            let stopped = application.request_shutdown();
            let report = application.lifecycle().retirement().await;
            (study, primed, stopped, report)
        })
        .unwrap_or_else(|_| panic!("receiver host entry"));
    let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
    let ((study, primed, stopped, report), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original receiver Work output"));
    let closed = node.close().await;
    let native = native.unwrap();
    assert!(closed.is_ok() && cleanup.is_ok());
    assert_notifications(&notifications);
    assert_report(report);
    assert!(primed.is_ok() && stopped.is_ok());
    assert_study(&study, &1, CounterOperation::Read, retention);
    assert_provider_proof(&study, proof);
    let first = study.native.first().unwrap();
    assert_eq!(first.from, study.target);
    assert_eq!(first.message, 1);
    let first_source = ptr::from_ref(first).addr();
    let sources = study
        .acquisitions
        .iter()
        .map(|a| a.source)
        .collect::<Vec<_>>();
    let attempts = study
        .acquisitions
        .iter()
        .filter(|a| a.admission.is_some())
        .count();
    let sender_native = study.sender_native.unwrap();
    assert_completed(native, None);
    assert_completed(sender_native, None);
    // Counter Read is nonmutating: this observes local attempts and two real
    // correlated Replies, not an actor-internal read counter or a replay protocol.
    assert_eq!(
        attempts, 1,
        "a duplicate must reuse the first actual native outcome"
    );
    assert_eq!(study.native.len(), 1);
    assert_eq!(sources, [Some(first_source), Some(first_source)]);
    assert!(study.acquisitions[1].admission.is_none());
    drop(study.client);
}

#[expect(
    clippy::too_many_lines,
    reason = "the terminating Ledger trace preserves all native originals through cleanup before its replay oracle"
)]
async fn ledger_case<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(&ProtectedMessage, &[ProtectedMessage]) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> &ProofScope<LedgerOperation>,
    proof: fn(&Receipt) -> Option<&[u8]>,
) {
    let retention = read_retention();
    let address = reserved_address();
    let mut node = Application::new(
        MailLedger {
            entries: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .open_node(configuration(&address, &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("explicit Ledger Node setup"));
    let (execution, receiving) = node
        .execute_with(|application, session| async move {
            let target = application.root().established_recipient();
            let target_address = application.root().address();
            let client = application
                .export_client::<_, Receipts>(target)
                .unwrap_or_else(|_| {
                    panic!("actual Ledger root certification and native allocation")
                });
            let caller = client.address();
            let original = Box::<[u8]>::from([59, 61]);
            let allocation = original.as_ptr();
            let primed = client.try_send(LedgerCommand::Append(original));
            let expected = vec![u64::from(caller)];
            let study = read_study(
                client,
                session,
                &address,
                target_address,
                "ledger",
                LedgerOperation::Read,
                profile,
                retention,
                expected,
                LedgerCommand::Read,
                |origins| origins.iter().copied().map(u64::from).collect::<Vec<_>>(),
                Some(application.lifecycle().retirement()),
                provider,
                projection,
            )
            .await;
            (study, primed, caller, allocation)
        })
        .unwrap_or_else(|_| panic!("Ledger host entry"));
    let ((), (outcome, native, notifications)) = tokio::join!(execution, receiving);
    let ((study, primed, caller, allocation), cleanup) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original Ledger Work output"));
    let closed = node.close().await;
    let native = native.unwrap();
    let (_, retirement) = &native;
    let ledger = retirement.custody().unwrap().behavior.base();
    assert!(closed.is_ok() && cleanup.is_ok());
    assert_notifications(&notifications);
    let first_report = study.first_retirement.unwrap();
    assert_report(first_report);
    assert!(primed.is_ok());
    let [(origin, original)] = ledger.entries.as_slice() else {
        panic!("one original append survives");
    };
    assert_eq!(*origin, caller);
    assert_eq!(original.as_ptr(), allocation);
    assert_eq!(original.as_ref(), [59, 61]);
    assert_study(
        &study,
        &vec![u64::from(caller)],
        LedgerOperation::Read,
        retention,
    );
    assert_provider_proof(&study, proof);
    let first = study.native.first().unwrap();
    assert_eq!(first.from, study.target);
    assert_eq!(first.message.as_slice(), [caller]);
    let first_source = ptr::from_ref(first).addr();
    let sources = study
        .acquisitions
        .iter()
        .map(|a| a.source)
        .collect::<Vec<_>>();
    let attempts = study
        .acquisitions
        .iter()
        .filter(|a| a.admission.is_some())
        .count();
    let second = &study.acquisitions[1];
    match retention {
        ReadRetention::Resend => {
            // The stateless baseline must establish the genuine native cause
            // before the intended one-attempt oracle; a timeout is not this fact.
            let Some(Err(TrySendError::Closed(LedgerCommand::Read(original)))) = &second.admission
            else {
                panic!("the second actual attempt returns its original Closed(Read)");
            };
            assert_eq!(original, &study.expected_reply);
            let (reply, _) = second.reply.as_ref().unwrap();
            assert!(reply.result().is_err());
        }
        ReadRetention::ReuseFirst => {
            assert!(second.admission.is_none());
            let (reply, _) = second.reply.as_ref().unwrap();
            assert!(reply.result().is_ok());
        }
    }
    let sender_native = study.sender_native.unwrap();
    assert_completed(native, None);
    assert_completed(sender_native, None);
    assert_eq!(
        attempts, 1,
        "the terminating Ledger's duplicate must reuse its first native outcome"
    );
    assert_eq!(study.native.len(), 1);
    assert_eq!(sources, [Some(first_source), Some(first_source)]);
    drop(study.client);
}

fn assert_provider_proof<Target, Replies, Operation, Receipt, Value>(
    study: &ReadStudy<Target, Replies, Operation, Receipt, Value>,
    proof: fn(&Receipt) -> Option<&[u8]>,
) where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
{
    for acquired in &study.acquisitions {
        if let Some(Ok(receipt)) = &acquired.verification
            && let Some(original_proof) = proof(receipt)
        {
            assert_eq!(original_proof, [7, 11]);
        }
        if let Some(Ok(receipt)) = &acquired.reply_verification
            && let Some(original_proof) = proof(receipt)
        {
            assert_eq!(original_proof, [13, 17]);
        }
    }
}

#[test]
fn recorded_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Recorded,
            recorded::verify::<CounterOperation>,
            recorded::projection,
            |_| None,
        ));
}
#[test]
fn scheduled_counter_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(counter_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<CounterOperation>,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}
#[test]
fn recorded_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Recorded,
            recorded::verify::<LedgerOperation>,
            recorded::projection,
            |_| None,
        ));
}
#[test]
fn scheduled_ledger_node_reuses_first_read_outcome() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(ledger_case(
            ProviderProfile::Scheduled,
            scheduled::verify::<LedgerOperation>,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}
