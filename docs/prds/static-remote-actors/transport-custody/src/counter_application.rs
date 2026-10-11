//! Finite counter comparison: no production identity, replay or resource profile.
//! Parsed buffers are newly owned copies; native Queries/Samples remain acquired through close.
use super::{
    ControlCommand, FIXTURE_WAIT, PendingReceipt, REQUEST_KEY, RequestIngress,
    TransportObservation, TransportOperation, TransportReceipts, WorkerRole, control_command,
    invalid_control, observe, relinquish_receipt, request_ingress, send_request,
};
use bombay::behavior::{
    BehaviorBase, BehaviorSettlements, ClassifySettlement, EstablishedDelivery, MessageProtocol,
    NoSends, SettlementStatus, Step,
};
use bombay::prelude::*;
use bombay::{ActorNotificationReceipts, ApplicationOutcome, TrySendError};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    io::{self, BufRead},
};
use thiserror::Error;
use tokio::{sync::oneshot, time::timeout};
use zenoh::{Config, Result as TransportResult, Session};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Increment,
    Read,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Scope {
    caller: String,
    deployment: String,
    purpose: String,
    controller: String,
    actor: String,
    protocol: String,
    operation: Operation,
    host: String,
    runtime: u64,
    actor_generation: u64,
    grant_issuer: String,
    authority_generation: u64,
}
fn configured_scope(operation: Operation, purpose: &str) -> Scope {
    Scope {
        caller: "alice".into(),
        deployment: "test-deployment".into(),
        purpose: purpose.into(),
        controller: "controller".into(),
        actor: "counter🦀".into(),
        protocol: "counter-protocol".into(),
        operation,
        host: "delegated-node".into(),
        runtime: 3,
        actor_generation: 5,
        grant_issuer: "grant-controller".into(),
        authority_generation: 7,
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum CounterOperation {
    Increment { amount: u64 },
    Read,
}
impl CounterOperation {
    fn operation(self) -> Operation {
        match self {
            Self::Increment { .. } => Operation::Increment,
            Self::Read => Operation::Read,
        }
    }
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtectedRequest {
    scope: Scope,
    request: u64,
    command: CounterOperation,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtectedReply {
    scope: Scope,
    request: u64,
    value: u64,
}
#[derive(Clone, Copy)]
enum Provider {
    Record,
    Scheduled,
}
#[derive(Debug, Error)]
enum VerificationError {
    #[error("invalid counter fixture evidence")]
    Invalid,
    #[error("counter fixture JSON invalid")]
    Syntax(#[from] serde_json::Error),
    #[error("counter fixture evidence unavailable")]
    Unavailable(#[from] oneshot::error::RecvError),
}
struct EvidenceRecord {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
fn records(provider: Provider, purpose: &str) -> Vec<EvidenceRecord> {
    [
        (1, CounterOperation::Increment { amount: 1 }),
        (2, CounterOperation::Read),
    ]
    .into_iter()
    .map(|(request, command)| {
        let protected = match purpose {
            "command" => serde_json::to_vec(&ProtectedRequest {
                scope: configured_scope(command.operation(), purpose),
                request,
                command,
            })
            .unwrap(),
            "reply" => serde_json::to_vec(&ProtectedReply {
                scope: configured_scope(command.operation(), purpose),
                request,
                value: 42,
            })
            .unwrap(),
            _ => unreachable!("only the two finite fixture purposes exist"),
        };
        let issuer = match provider {
            Provider::Record => "record",
            Provider::Scheduled => "scheduled",
        };
        EvidenceRecord {
            protected: protected.into_boxed_slice(),
            proof: format!("fixture-{issuer}-{purpose}-{request}\0")
                .into_bytes()
                .into_boxed_slice(),
        }
    })
    .collect()
}
mod record_provider {
    use super::{EvidenceRecord, ProtectedReply, ProtectedRequest, VerificationError};
    pub(super) struct RequestReceipt {
        request: ProtectedRequest,
        record: usize,
    }
    pub(super) struct ReplyReceipt {
        reply: ProtectedReply,
        record: usize,
    }
    impl RequestReceipt {
        pub(super) fn request(&self) -> &ProtectedRequest {
            &self.request
        }
        pub(super) fn record(&self) -> usize {
            self.record
        }
    }
    impl ReplyReceipt {
        pub(super) fn reply(&self) -> &ProtectedReply {
            &self.reply
        }
        pub(super) fn record(&self) -> usize {
            self.record
        }
    }
    fn matching_record(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<usize, VerificationError> {
        for (record, known) in records.iter().enumerate() {
            if known.protected.as_ref() == protected && known.proof.as_ref() == proof {
                return Ok(record);
            }
        }
        Err(VerificationError::Invalid)
    }
    pub(super) fn request(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<RequestReceipt, VerificationError> {
        let record = matching_record(records, protected, proof)?;
        Ok(RequestReceipt {
            request: serde_json::from_slice(protected)?,
            record,
        })
    }
    pub(super) fn reply(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<ReplyReceipt, VerificationError> {
        let record = matching_record(records, protected, proof)?;
        Ok(ReplyReceipt {
            reply: serde_json::from_slice(protected)?,
            record,
        })
    }
}
mod scheduled_provider {
    use super::{EvidenceRecord, ProtectedReply, ProtectedRequest, VerificationError, oneshot};
    pub(super) struct RequestReceipt {
        request: ProtectedRequest,
        query: usize,
    }
    pub(super) struct ReplyReceipt {
        reply: ProtectedReply,
        query: usize,
    }
    impl RequestReceipt {
        pub(super) fn request(&self) -> &ProtectedRequest {
            &self.request
        }
        pub(super) fn query(&self) -> usize {
            self.query
        }
    }
    impl ReplyReceipt {
        pub(super) fn reply(&self) -> &ProtectedReply {
            &self.reply
        }
        pub(super) fn query(&self) -> usize {
            self.query
        }
    }
    async fn matching_query(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<usize, VerificationError> {
        let (publication, receiving) = oneshot::channel();
        let query = records.iter().position(|known| {
            known.protected.as_ref() == protected && known.proof.as_ref() == proof
        });
        let transferred = publication.send(query);
        assert!(
            transferred.is_ok(),
            "the original query receiver is retained"
        );
        receiving.await?.ok_or(VerificationError::Invalid)
    }
    pub(super) async fn request(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<RequestReceipt, VerificationError> {
        let query = matching_query(records, protected, proof).await?;
        Ok(RequestReceipt {
            request: serde_json::from_slice(protected)?,
            query,
        })
    }
    pub(super) async fn reply(
        records: &[EvidenceRecord],
        protected: &[u8],
        proof: &[u8],
    ) -> Result<ReplyReceipt, VerificationError> {
        let query = matching_query(records, protected, proof).await?;
        Ok(ReplyReceipt {
            reply: serde_json::from_slice(protected)?,
            query,
        })
    }
}
struct RawRequest {
    request: u64,
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
struct CounterReply {
    request: u64,
    operation: Operation,
    value: u64,
}
type RemoteRequests = MessageProtocol<MailAddr, RawRequest>;
type CounterReplies = MessageProtocol<MailAddr, CounterReply>;
type Acknowledgements = MessageProtocol<MailAddr, (u64, Option<u64>)>;
enum CounterCommand {
    Execute {
        original: RawRequest,
        command: CounterOperation,
        reply_to: EstablishedRecipient<CounterReplies>,
    },
}
struct Counter {
    value: u64,
    processed: Vec<(MailAddr, CounterOperation, RawRequest)>,
}
#[bombay::actor(sends = { replies: Vec<EstablishedDelivery<CounterReplies>>, })]
impl Counter {
    fn receive(&mut self, from: MailAddr, command: CounterCommand) -> BehaviorActed<Self> {
        let CounterCommand::Execute {
            original,
            command,
            reply_to,
        } = command;
        match command {
            CounterOperation::Increment { amount } => self.value += amount,
            CounterOperation::Read => {}
        }
        let reply = CounterReply {
            request: original.request,
            operation: command.operation(),
            value: self.value,
        };
        self.processed.push((from, command, original));
        Ok(Actions::cont().send_replies(EstablishedDelivery::new(reply_to, reply)))
    }
}
enum ClientCommand {
    Begin {
        requests: VecDeque<RawRequest>,
        service: EstablishedRecipient<RemoteRequests>,
    },
    RecordReply {
        request: u64,
        evidence: Result<record_provider::ReplyReceipt, VerificationError>,
        service: EstablishedRecipient<RemoteRequests>,
        acknowledgement: EstablishedRecipient<Acknowledgements>,
    },
    ScheduledReply {
        request: u64,
        evidence: Result<scheduled_provider::ReplyReceipt, VerificationError>,
        service: EstablishedRecipient<RemoteRequests>,
        acknowledgement: EstablishedRecipient<Acknowledgements>,
    },
}
#[derive(Default)]
struct Client {
    remaining: VecDeque<RawRequest>,
    origins: Vec<MailAddr>,
    record_replies: Vec<Result<record_provider::ReplyReceipt, VerificationError>>,
    scheduled_replies: Vec<Result<scheduled_provider::ReplyReceipt, VerificationError>>,
    replies: Vec<(u64, Option<u64>)>,
}
#[bombay::actor(sends = {
    requests: Vec<EstablishedDelivery<RemoteRequests>>,
    acknowledgements: Vec<EstablishedDelivery<Acknowledgements>>,
})]
impl Client {
    fn receive(&mut self, from: MailAddr, command: ClientCommand) -> BehaviorActed<Self> {
        self.origins.push(from);
        match command {
            ClientCommand::Begin { requests, service } => {
                self.remaining.extend(requests);
                let request = self
                    .remaining
                    .pop_front()
                    .expect("the concrete Begin owns its first command");
                Ok(Actions::cont().send_requests(EstablishedDelivery::new(service, request)))
            }
            ClientCommand::RecordReply {
                request,
                evidence,
                service,
                acknowledgement,
            } => {
                let value = evidence.as_ref().ok().map(|receipt| receipt.reply().value);
                self.record_replies.push(evidence);
                self.after_reply(request, value, service, acknowledgement)
            }
            ClientCommand::ScheduledReply {
                request,
                evidence,
                service,
                acknowledgement,
            } => {
                let value = evidence.as_ref().ok().map(|receipt| receipt.reply().value);
                self.scheduled_replies.push(evidence);
                self.after_reply(request, value, service, acknowledgement)
            }
        }
    }
    fn after_reply(
        &mut self,
        request: u64,
        value: Option<u64>,
        service: EstablishedRecipient<RemoteRequests>,
        acknowledgement: EstablishedRecipient<Acknowledgements>,
    ) -> BehaviorActed<Self> {
        self.replies.push((request, value));
        let actions = Actions::cont()
            .send_acknowledgements(EstablishedDelivery::new(acknowledgement, (request, value)));
        match value {
            Some(_) => match self.remaining.pop_front() {
                Some(next) => Ok(actions.send_requests(EstablishedDelivery::new(service, next))),
                None => Ok(actions),
            },
            None => Ok(actions),
        }
    }
}
fn complete_record(
    request: u64,
    evidence: Result<record_provider::ReplyReceipt, VerificationError>,
    service: EstablishedRecipient<RemoteRequests>,
    acknowledgement: EstablishedRecipient<Acknowledgements>,
) -> ClientCommand {
    ClientCommand::RecordReply {
        request,
        evidence,
        service,
        acknowledgement,
    }
}
fn complete_scheduled(
    request: u64,
    evidence: Result<scheduled_provider::ReplyReceipt, VerificationError>,
    service: EstablishedRecipient<RemoteRequests>,
    acknowledgement: EstablishedRecipient<Acknowledgements>,
) -> ClientCommand {
    ClientCommand::ScheduledReply {
        request,
        evidence,
        service,
        acknowledgement,
    }
}
fn expected_operation(request: u64) -> Operation {
    match request {
        1 => Operation::Increment,
        2 => Operation::Read,
        _ => unreachable!("finite fixture identities"),
    }
}
fn valid_request(
    claim: &ProtectedRequest,
    request: u64,
) -> std::result::Result<CounterOperation, VerificationError> {
    if claim.request != request
        || claim.scope != configured_scope(expected_operation(request), "command")
        || claim.command.operation() != expected_operation(request)
    {
        return Err(VerificationError::Invalid);
    }
    Ok(claim.command)
}
fn valid_reply(claim: &ProtectedReply, request: u64) -> std::result::Result<(), VerificationError> {
    if claim.request != request
        || claim.scope != configured_scope(expected_operation(request), "reply")
    {
        return Err(VerificationError::Invalid);
    }
    Ok(())
}
fn completed_actor<B>(
    origin: RootOrigin<B>,
    retirement: ActorRetirement<B, Never, ()>,
    address: MailAddr,
) -> (B, Vec<B::Settlements>)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        behavior,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        completion,
        capability_failures,
        unread_owner_cancellation,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = retirement
    else {
        panic!("the complete original native actor product remains acquired")
    };
    assert_eq!(origin.address(), address);
    assert_eq!(completion, Completion::Stopped);
    assert!(control.is_empty() && user.is_empty() && descendants.is_empty());
    assert!(capability_failures.is_empty() && unread_owner_cancellation.is_none());
    assert!(interpretation.is_none() && source.is_none() && additional_failures.is_empty());
    assert!(received_interpretation.is_none() && received_source.is_none());
    assert!(source_index.is_none() && acquired_ingress.is_none());
    assert!(retirement_failures.is_empty() && terminal_report.is_none());
    (behavior, settlements)
}
async fn receive_sample(pending: &PendingReceipt<'_>) -> TransportResult<zenoh::sample::Sample> {
    match pending {
        PendingReceipt::Query { replies, .. } => Ok(timeout(FIXTURE_WAIT, replies.recv_async())
            .await??
            .into_result()?),
        PendingReceipt::Publication { subscriber, .. } => {
            Ok(timeout(FIXTURE_WAIT, subscriber.recv_async()).await??)
        }
    }
}

pub(super) async fn execute(
    role: WorkerRole,
    operation: TransportOperation,
    config: Config,
) -> TransportResult<TransportReceipts> {
    let session = zenoh::open(config).await?;
    match role {
        WorkerRole::RecordCounterRecipient => {
            recipient(role, Provider::Record, operation, session).await
        }
        WorkerRole::ScheduledCounterRecipient => {
            recipient(role, Provider::Scheduled, operation, session).await
        }
        WorkerRole::RecordCounterCaller => caller(role, Provider::Record, operation, session).await,
        WorkerRole::ScheduledCounterCaller => {
            caller(role, Provider::Scheduled, operation, session).await
        }
        WorkerRole::Router | WorkerRole::Recipient | WorkerRole::Caller => {
            unreachable!("ordinary transport roles stay with their owner")
        }
    }
}

async fn recipient(
    role: WorkerRole,
    provider: Provider,
    operation: TransportOperation,
    session: Session,
) -> TransportResult<TransportReceipts> {
    let request_records = records(provider, "command");
    let reply_records = records(provider, "reply");
    let mut ingress = match request_ingress(&session, operation).await {
        Ok(ingress) => ingress,
        Err(original) => {
            let session_close = session.close().await;
            return Ok(TransportReceipts {
                role,
                worker: Err(original),
                session_close,
            });
        }
    };
    let configured_requests = &request_records;
    let configured_replies = &reply_records;
    let receiving = Application::new(
        Counter {
            value: 41,
            processed: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .run_with::<Never, _, _, _, _, _>(|application| async move {
        let interface = application.interface(());
        let mut service = interface.external::<CounterReplies>().unwrap();
        let root_address = application.root().address();
        let service_address = service.address();
        let target = application.root().established_recipient();
        let mut record_receipts = Vec::new();
        let mut scheduled_receipts = Vec::new();
        let mut allocations = Vec::new();
        let mut queries = Vec::new();
        let mut samples = Vec::new();
        let mut processed_replies = Vec::new();
        let mut refused = None;
        let mut requests = Vec::new();
        let work: TransportResult<()> = async {
            observe(&TransportObservation::Ready { role })?;
            for request in 1..=2 {
                let line = io::stdin()
                    .lock()
                    .lines()
                    .next()
                    .ok_or_else(|| invalid_control("RELEASE required"))??;
                if !matches!(control_command(&line)?, ControlCommand::Release) {
                    return Err(invalid_control("counter recipient needs RELEASE").into());
                }
                let (protected, proof) = match &mut ingress {
                    RequestIngress::Query(queryable) => {
                        queries.push(timeout(FIXTURE_WAIT, queryable.recv_async()).await??);
                        let query = queries.last().expect("the native query was acquired");
                        let protected = query
                            .payload()
                            .ok_or_else(|| invalid_control("missing command payload"))?
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice();
                        let proof = query
                            .attachment()
                            .ok_or_else(|| invalid_control("missing command proof"))?
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice();
                        (protected, proof)
                    }
                    RequestIngress::Publication { subscriber, .. } => {
                        samples.push(timeout(FIXTURE_WAIT, subscriber.recv_async()).await??);
                        let sample = samples.last().expect("the native publication was acquired");
                        let protected = sample.payload().to_bytes().into_owned().into_boxed_slice();
                        let proof = sample
                            .attachment()
                            .ok_or_else(|| invalid_control("missing command proof"))?
                            .to_bytes()
                            .into_owned()
                            .into_boxed_slice();
                        (protected, proof)
                    }
                };
                requests.push(RawRequest {
                    request,
                    protected,
                    proof,
                });
                let original = requests
                    .last()
                    .expect("the parsed command buffers are owned");
                let command = match provider {
                    Provider::Record => {
                        record_receipts.push(record_provider::request(
                            configured_requests,
                            &original.protected,
                            &original.proof,
                        )?);
                        valid_request(
                            record_receipts
                                .last()
                                .expect("the record receipt is owned")
                                .request(),
                            request,
                        )?
                    }
                    Provider::Scheduled => {
                        scheduled_receipts.push(
                            scheduled_provider::request(
                                configured_requests,
                                &original.protected,
                                &original.proof,
                            )
                            .await?,
                        );
                        valid_request(
                            scheduled_receipts
                                .last()
                                .expect("the scheduled receipt is owned")
                                .request(),
                            request,
                        )?
                    }
                };
                allocations.push((
                    original.protected.as_ptr() as usize,
                    original.proof.as_ptr() as usize,
                ));
                // Exclusive service ownership: no await from this final fixture check to insertion.
                let now = 9_u64;
                let deadline = 10_u64;
                if now >= deadline {
                    return Err(invalid_control("fixture command expired").into());
                }
                let original = requests
                    .pop()
                    .expect("the final check transfers its command");
                let admitted = service.try_send(
                    &target,
                    CounterCommand::Execute {
                        original,
                        command,
                        reply_to: service.recipient(),
                    },
                );
                match admitted {
                    Ok(()) => {}
                    Err(error @ TrySendError::Full(_)) => {
                        refused = Some(error);
                        return Err(
                            invalid_control("counter command full: original acquired").into()
                        );
                    }
                    Err(error @ TrySendError::Closed(_)) => {
                        refused = Some(error);
                        return Err(
                            invalid_control("counter command closed: original acquired").into()
                        );
                    }
                }
                let processed = service
                    .receive()
                    .await
                    .ok_or_else(|| invalid_control("missing typed Counter Actions reply"))?;
                let reply = ProtectedReply {
                    scope: configured_scope(processed.message.operation, "reply"),
                    request,
                    value: processed.message.value,
                };
                processed_replies.push(processed);
                let protected_reply = serde_json::to_vec(&reply)?;
                observe(&TransportObservation::CounterProcessed {
                    request,
                    value: reply.value,
                })?;
                observe(&TransportObservation::PotentiallyTransmitted { operation })?;
                match &mut ingress {
                    RequestIngress::Query(_) => {
                        queries
                            .last()
                            .expect("the query ingress acquired this request")
                            .reply(REQUEST_KEY, protected_reply)
                            .attachment(configured_replies[(request - 1) as usize].proof.to_vec())
                            .await?;
                    }
                    RequestIngress::Publication { publisher, .. } => {
                        publisher
                            .put(protected_reply)
                            .attachment(configured_replies[(request - 1) as usize].proof.to_vec())
                            .await?;
                    }
                }
                observe(&TransportObservation::InvocationReturned { operation })?;
            }
            let line = io::stdin()
                .lock()
                .lines()
                .next()
                .ok_or_else(|| invalid_control("EXIT required"))??;
            if !matches!(control_command(&line)?, ControlCommand::Exit) {
                return Err(invalid_control("counter recipient needs EXIT").into());
            }
            service.close_admission();
            let exhausted = service.receive().await;
            assert!(exhausted.is_none());
            match ingress {
                RequestIngress::Query(queryable) => queryable.undeclare().await?,
                RequestIngress::Publication {
                    subscriber,
                    publisher,
                } => {
                    subscriber.undeclare().await?;
                    publisher.undeclare().await?;
                }
            }
            Ok(())
        }
        .await;
        let shutdown = application.lifecycle().request_shutdown();
        (
            work,
            shutdown,
            root_address,
            service_address,
            allocations,
            record_receipts,
            scheduled_receipts,
            queries,
            samples,
            processed_replies,
            refused,
            requests,
        )
    })
    .await;
    let session_close = session.close().await;
    let receiving = match receiving {
        Ok(acquired) => acquired,
        Err((cold_application, original_work, cause)) => {
            // Explicit terminal fixture discharge after actual session close; no actor was acquired.
            drop((cold_application, original_work));
            return Ok(TransportReceipts {
                role,
                worker: Err(cause.into()),
                session_close,
            });
        }
    };
    assert!(
        matches!(
            (&receiving, &session_close),
            (
                (
                    ApplicationOutcome::Completed {
                        cleanup: Ok(()),
                        ..
                    },
                    Ok(_),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(())
                    })
                ),
                Ok(())
            )
        ),
        "complete original Application/native/notification/close products required"
    );
    let (
        ApplicationOutcome::Completed {
            output:
                (
                    work,
                    shutdown,
                    root_address,
                    service_address,
                    allocations,
                    record_receipts,
                    scheduled_receipts,
                    queries,
                    samples,
                    processed_replies,
                    refused,
                    requests,
                ),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = receiving
    else {
        unreachable!("the complete original receiving product was checked by borrow");
    };
    assert_eq!(shutdown, Ok(()));
    let (counter, settlements) = completed_actor(origin, retirement, root_address);
    let [settlement] = settlements.as_slice() else {
        panic!("final Counter Stop product")
    };
    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
    assert!(settlement.creations.is_empty());
    assert!(matches!(settlement.sends.owned, NoSends));
    assert_eq!(settlement.sends.inner.replies.len(), 0);
    assert!(matches!(settlement.become_, Step::Stop(_)));
    let worker = work.and_then(|()| {
        if counter.base().value != 42 || counter.base().processed.len() != 2 {
            return Err(invalid_control(
                "actual Counter processing required: balance42 and Increment/Read trace",
            )
            .into());
        }
        assert!(refused.is_none());
        assert!(requests.is_empty());
        assert_eq!(queries.len() + samples.len(), 2);
        assert_eq!(processed_replies.len(), 2);
        for (index, (from, command, original)) in counter.base().processed.iter().enumerate() {
            assert_eq!(processed_replies[index].from, root_address);
            assert_eq!(processed_replies[index].message.request, index as u64 + 1);
            assert_eq!(
                processed_replies[index].message.operation,
                command.operation()
            );
            assert_eq!(processed_replies[index].message.value, 42);
            assert_eq!(*from, service_address);
            assert_eq!(
                *command,
                if index == 0 {
                    CounterOperation::Increment { amount: 1 }
                } else {
                    CounterOperation::Read
                }
            );
            assert_eq!(original.request, index as u64 + 1);
            assert_eq!(
                original.protected.as_ref(),
                request_records[index].protected.as_ref()
            );
            assert_eq!(
                original.proof.as_ref(),
                request_records[index].proof.as_ref()
            );
            assert_eq!(
                (
                    original.protected.as_ptr() as usize,
                    original.proof.as_ptr() as usize
                ),
                allocations[index]
            );
        }
        match provider {
            Provider::Record => {
                assert_eq!(record_receipts.len(), 2);
                assert!(scheduled_receipts.is_empty());
                for (index, receipt) in record_receipts.iter().enumerate() {
                    assert_eq!(receipt.record(), index);
                    assert_eq!(receipt.request().request, index as u64 + 1);
                }
            }
            Provider::Scheduled => {
                assert_eq!(scheduled_receipts.len(), 2);
                assert!(record_receipts.is_empty());
                for (index, receipt) in scheduled_receipts.iter().enumerate() {
                    assert_eq!(receipt.query(), index);
                    assert_eq!(receipt.request().request, index as u64 + 1);
                }
            }
        }
        observe(&TransportObservation::CounterNative {
            value: counter.base().value,
            commands: 2,
        })?;
        Ok(())
    });
    Ok(TransportReceipts {
        role,
        worker,
        session_close,
    })
}

async fn caller(
    role: WorkerRole,
    provider: Provider,
    operation: TransportOperation,
    session: Session,
) -> TransportResult<TransportReceipts> {
    let request_records = records(provider, "command");
    let reply_records = records(provider, "reply");
    let configured_requests = &request_records;
    let configured_replies = &reply_records;
    let connection = &session;
    let receiving = Application::new(Client::default().stop_on_shutdown())
        .run_with::<Never, _, _, _, _, _>(|application| async move {
            let interface = application.interface(());
            let mut service = interface.external::<RemoteRequests>().unwrap();
            let mut acknowledgements = interface.external::<Acknowledgements>().unwrap();
            let root_address = application.root().address();
            let service_address = service.address();
            let target = application.root().established_recipient();
            let mut outgoing = Vec::new();
            let mut replies = Vec::new();
            let mut samples = Vec::new();
            let mut request_origins = Vec::new();
            let mut consumed = Vec::new();
            let mut record_pending = Vec::new();
            let mut scheduled_pending = Vec::new();
            let mut refused_completions = Vec::new();
            let work: TransportResult<()> = async {
                observe(&TransportObservation::Ready { role })?;
                let line = io::stdin()
                    .lock()
                    .lines()
                    .next()
                    .ok_or_else(|| invalid_control("RUN required"))??;
                if !matches!(control_command(&line)?, ControlCommand::Run) {
                    return Err(invalid_control("counter caller needs RUN").into());
                }
                let requests = configured_requests
                    .iter()
                    .enumerate()
                    .map(|(index, record)| RawRequest {
                        request: index as u64 + 1,
                        protected: record.protected.to_vec().into_boxed_slice(),
                        proof: record.proof.to_vec().into_boxed_slice(),
                    })
                    .collect();
                let begun = service
                    .send(
                        &target,
                        ClientCommand::Begin {
                            requests,
                            service: service.recipient(),
                        },
                    )
                    .await;
                if let Err(original) = begun {
                    refused_completions.push(original);
                    return Err(invalid_control("the caller Begin was refused").into());
                }
                for request in 1..=2 {
                    let emitted = service
                        .receive()
                        .await
                        .ok_or_else(|| invalid_control("missing caller request Actions"))?;
                    request_origins.push(emitted.from);
                    outgoing.push(emitted.message);
                    let original = outgoing.last().expect("the emitted command is owned");
                    let pending =
                        send_request(connection, operation, &original.protected, &original.proof)
                            .await?;
                    samples.push(receive_sample(&pending).await?);
                    let sample = samples.last().expect("the native reply Sample is owned");
                    let protected = sample.payload().to_bytes().into_owned().into_boxed_slice();
                    let proof = sample
                        .attachment()
                        .ok_or_else(|| invalid_control("missing counter reply proof"))?
                        .to_bytes()
                        .into_owned()
                        .into_boxed_slice();
                    replies.push((protected, proof));
                    let (protected, proof) =
                        replies.last().expect("the parsed reply buffers are owned");
                    let completed = match provider {
                        Provider::Record => {
                            record_pending.push(record_provider::reply(
                                configured_replies,
                                protected,
                                proof,
                            )?);
                            valid_reply(
                                record_pending
                                    .last()
                                    .expect("the record reply is owned")
                                    .reply(),
                                request,
                            )?;
                            complete_record(
                                request,
                                Ok(record_pending
                                    .pop()
                                    .expect("the valid record reply transfers once")),
                                service.recipient(),
                                acknowledgements.recipient(),
                            )
                        }
                        Provider::Scheduled => {
                            scheduled_pending.push(
                                scheduled_provider::reply(configured_replies, protected, proof)
                                    .await?,
                            );
                            valid_reply(
                                scheduled_pending
                                    .last()
                                    .expect("the scheduled reply is owned")
                                    .reply(),
                                request,
                            )?;
                            complete_scheduled(
                                request,
                                Ok(scheduled_pending
                                    .pop()
                                    .expect("the valid scheduled reply transfers once")),
                                service.recipient(),
                                acknowledgements.recipient(),
                            )
                        }
                    };
                    let delivered = service.send(&target, completed).await;
                    if let Err(original) = delivered {
                        refused_completions.push(original);
                        return Err(
                            invalid_control("the verified caller completion was refused").into(),
                        );
                    }
                    let acknowledged = acknowledgements.receive().await.ok_or_else(|| {
                        invalid_control("required pure caller Actions consumer missing")
                    })?;
                    consumed.push(acknowledged);
                    let value = consumed
                        .last()
                        .expect("the typed acknowledgement is owned")
                        .message
                        .1
                        .ok_or_else(|| {
                            invalid_control("the caller received no verified reply value")
                        })?;
                    relinquish_receipt(pending).await?;
                    observe(&TransportObservation::CounterConsumed { request, value })?;
                }
                let line = io::stdin()
                    .lock()
                    .lines()
                    .next()
                    .ok_or_else(|| invalid_control("EXIT required"))??;
                if !matches!(control_command(&line)?, ControlCommand::Exit) {
                    return Err(invalid_control("counter caller needs EXIT").into());
                }
                service.close_admission();
                acknowledgements.close_admission();
                let exhausted_requests = service.receive().await;
                let exhausted_acknowledgements = acknowledgements.receive().await;
                assert!(exhausted_requests.is_none() && exhausted_acknowledgements.is_none());
                Ok(())
            }
            .await;
            let shutdown = application.lifecycle().request_shutdown();
            (
                work,
                shutdown,
                root_address,
                service_address,
                outgoing,
                replies,
                samples,
                request_origins,
                consumed,
                record_pending,
                scheduled_pending,
                refused_completions,
            )
        })
        .await;
    let session_close = session.close().await;
    let receiving = match receiving {
        Ok(acquired) => acquired,
        Err((cold_application, original_work, cause)) => {
            // Explicit terminal fixture discharge after actual session close; no actor was acquired.
            drop((cold_application, original_work));
            return Ok(TransportReceipts {
                role,
                worker: Err(cause.into()),
                session_close,
            });
        }
    };
    assert!(
        matches!(
            (&receiving, &session_close),
            (
                (
                    ApplicationOutcome::Completed {
                        cleanup: Ok(()),
                        ..
                    },
                    Ok(_),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(())
                    })
                ),
                Ok(())
            )
        ),
        "complete original Application/native/notification/close products required"
    );
    let (
        ApplicationOutcome::Completed {
            output:
                (
                    work,
                    shutdown,
                    root_address,
                    service_address,
                    outgoing,
                    replies,
                    samples,
                    request_origins,
                    consumed,
                    record_pending,
                    scheduled_pending,
                    refused_completions,
                ),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = receiving
    else {
        unreachable!("the complete original receiving product was checked by borrow");
    };
    assert_eq!(shutdown, Ok(()));
    let (client, settlements) = completed_actor(origin, retirement, root_address);
    let [settlement] = settlements.as_slice() else {
        panic!("final caller Stop product")
    };
    assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
    assert!(settlement.creations.is_empty());
    assert!(matches!(settlement.sends.owned, NoSends));
    assert_eq!(settlement.sends.inner.requests.len(), 0);
    assert_eq!(settlement.sends.inner.acknowledgements.len(), 0);
    assert!(matches!(settlement.become_, Step::Stop(_)));
    let worker = work.and_then(|()| {
        assert_eq!(client.base().origins, [service_address; 3]);
        assert!(client.base().remaining.is_empty());
        assert_eq!(client.base().replies, [(1, Some(42)), (2, Some(42))]);
        assert!(record_pending.is_empty() && scheduled_pending.is_empty());
        assert!(refused_completions.is_empty());
        assert_eq!(request_origins, [root_address; 2]);
        assert_eq!(samples.len(), 2);
        assert_eq!(consumed.len(), 2);
        assert_eq!(outgoing.len(), 2);
        assert_eq!(replies.len(), 2);
        for index in 0..2 {
            assert_eq!(consumed[index].from, root_address);
            assert_eq!(consumed[index].message, (index as u64 + 1, Some(42)));
            assert_eq!(outgoing[index].request, index as u64 + 1);
            assert_eq!(
                outgoing[index].protected.as_ref(),
                request_records[index].protected.as_ref()
            );
            assert_eq!(
                outgoing[index].proof.as_ref(),
                request_records[index].proof.as_ref()
            );
            assert_eq!(
                replies[index].0.as_ref(),
                reply_records[index].protected.as_ref()
            );
            assert_eq!(
                replies[index].1.as_ref(),
                reply_records[index].proof.as_ref()
            );
        }
        match provider {
            Provider::Record => {
                assert_eq!(client.base().record_replies.len(), 2);
                assert!(client.base().scheduled_replies.is_empty());
                for (index, evidence) in client.base().record_replies.iter().enumerate() {
                    let receipt = evidence.as_ref().unwrap();
                    assert_eq!(receipt.record(), index);
                    assert_eq!(receipt.reply().request, index as u64 + 1);
                    assert_eq!(receipt.reply().value, 42);
                }
            }
            Provider::Scheduled => {
                assert_eq!(client.base().scheduled_replies.len(), 2);
                assert!(client.base().record_replies.is_empty());
                for (index, evidence) in client.base().scheduled_replies.iter().enumerate() {
                    let receipt = evidence.as_ref().unwrap();
                    assert_eq!(receipt.query(), index);
                    assert_eq!(receipt.reply().request, index as u64 + 1);
                    assert_eq!(receipt.reply().value, 42);
                }
            }
        }
        observe(&TransportObservation::CounterClientNative { replies: 2 })?;
        Ok(())
    });
    Ok(TransportReceipts {
        role,
        worker,
        session_close,
    })
}
