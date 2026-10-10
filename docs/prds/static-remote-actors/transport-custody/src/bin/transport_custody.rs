//! Disposable transport observations; no actor admission or identity proof.
use std::{
    env,
    future::IntoFuture,
    io::{self, BufRead, ErrorKind, Write},
    path::Path,
    process::{ExitCode, Termination},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    runtime::Builder,
    time::{sleep, timeout},
};
use zenoh::{
    Config, Result, Session,
    bytes::ZBytes,
    handlers::{FifoChannel, FifoChannelHandler},
    pubsub::{Publisher, Subscriber},
    qos::CongestionControl,
    query::{ConsolidationMode, Querier, Query, QueryTarget, Queryable, Reply, ReplyKeyExpr},
    sample::{Locality, Sample},
};

#[path = "../counter_application.rs"]
mod counter_application;

// Explicit comparison settings, never production resource defaults.
const FIXTURE_CAPACITY: usize = 4;
const FIXTURE_WAIT: Duration = Duration::from_secs(5);
const REQUEST_KEY: &str = "research/custody/request";
const RECEIPT_KEY: &str = "research/custody/receipt";
const PROTECTED_TEXT: &str =
    "\t { \"amount\": 9007199254740993, \"marker\": \"static-remote-fixture🦀\" }\n ";
// Opaque comparison fixtures only; these bytes do not establish authenticity.
const REQUEST_PROOF: &[u8] = b"\0fixture-request\xff\r\n";
const REPLY_PROOF: &[u8] = b"\xfffixture-reply\0\n";

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkerRole {
    Router,
    Recipient,
    Caller,
    RecordCounterCaller,
    RecordCounterRecipient,
    ScheduledCounterCaller,
    ScheduledCounterRecipient,
}

enum ConnectionLayout {
    Peer,
    RouterClient,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TransportOperation {
    Query,
    Publication,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ControlCommand {
    Run,
    Wait,
    Cancel,
    Release,
    Exit,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRequest {
    marker: String,
    amount: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureReceipt {
    protected_text: String,
    observations: u64,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum TransportObservation {
    CounterProcessed {
        request: u64,
        value: u64,
    },
    CounterConsumed {
        request: u64,
        value: u64,
    },
    CounterNative {
        value: u64,
        commands: u64,
    },
    CounterClientNative {
        replies: u64,
    },
    Ready {
        role: WorkerRole,
    },
    PotentiallyTransmitted {
        operation: TransportOperation,
    },
    InvocationReturned {
        operation: TransportOperation,
    },
    Received {
        protected_text: String,
        proof: Vec<u8>,
    },
    Receipt {
        receipt: FixtureReceipt,
        proof: Vec<u8>,
    },
    NativeReplyError {
        payload: Vec<u8>,
        encoding: String,
    },
    ReceiverClosed {
        cause: String,
    },
    WaitExpired,
    WaitCancelled,
    SessionCloseReturned {
        role: WorkerRole,
    },
    WorkerFailed {
        cause: String,
    },
}

enum RequestIngress<'a> {
    Query(Queryable<FifoChannelHandler<Query>>),
    Publication {
        subscriber: Subscriber<FifoChannelHandler<Sample>>,
        publisher: Publisher<'a>,
    },
}

enum PendingReceipt<'a> {
    Query {
        querier: Querier<'a>,
        replies: FifoChannelHandler<Reply>,
    },
    Publication {
        subscriber: Subscriber<FifoChannelHandler<Sample>>,
        publisher: Publisher<'a>,
    },
}

struct TransportReceipts {
    role: WorkerRole,
    worker: Result<()>,
    session_close: Result<()>,
}

impl Termination for TransportReceipts {
    fn report(self) -> ExitCode {
        let Self {
            role,
            worker,
            session_close,
        } = self;
        // Both observations are attempted before either native result is discharged.
        let worker_observation = match &worker {
            Ok(()) => Ok(()),
            Err(cause) => observe(&TransportObservation::WorkerFailed {
                cause: cause.to_string(),
            }),
        };
        let session_close_observation = match &session_close {
            Ok(()) => observe(&TransportObservation::SessionCloseReturned { role }),
            Err(cause) => observe(&TransportObservation::WorkerFailed {
                cause: cause.to_string(),
            }),
        };
        let mut exit = ExitCode::SUCCESS;
        let mut diagnostic = io::stderr().lock();
        for (origin, outcome) in [
            ("worker", worker),
            ("session close", session_close),
            ("worker observation", worker_observation),
            ("session close observation", session_close_observation),
        ] {
            if let Err(cause) = outcome {
                exit = ExitCode::FAILURE;
                // This is explicit terminal fixture consumption, after close and both observations.
                let diagnostic_receipt = writeln!(diagnostic, "{origin}: {cause}");
                if diagnostic_receipt.is_err() {
                    exit = ExitCode::FAILURE;
                }
            }
        }
        exit
    }
}

fn invalid_control(reason: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, reason)
}

fn observe(observation: &TransportObservation) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, observation)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn control_command(line: &str) -> Result<ControlCommand> {
    Ok(serde_json::from_str(line)?)
}

fn configuration(
    role: WorkerRole,
    layout: ConnectionLayout,
    address: &str,
    certificates: &Path,
) -> Result<Config> {
    let (mode, owner, listen, connect) = match (role, layout) {
        (WorkerRole::Router, ConnectionLayout::RouterClient) => {
            ("router", "router", vec![address], vec![])
        }
        (WorkerRole::Router, ConnectionLayout::Peer) => {
            return Err(invalid_control("peer fixture has no router role").into());
        }
        (
            WorkerRole::Recipient
            | WorkerRole::RecordCounterRecipient
            | WorkerRole::ScheduledCounterRecipient,
            ConnectionLayout::Peer,
        ) => ("peer", "recipient", vec![address], vec![]),
        (
            WorkerRole::Caller
            | WorkerRole::RecordCounterCaller
            | WorkerRole::ScheduledCounterCaller,
            ConnectionLayout::Peer,
        ) => ("peer", "caller", vec![], vec![address]),
        (
            WorkerRole::Recipient
            | WorkerRole::RecordCounterRecipient
            | WorkerRole::ScheduledCounterRecipient,
            ConnectionLayout::RouterClient,
        ) => ("client", "recipient", vec![], vec![address]),
        (
            WorkerRole::Caller
            | WorkerRole::RecordCounterCaller
            | WorkerRole::ScheduledCounterCaller,
            ConnectionLayout::RouterClient,
        ) => ("client", "caller", vec![], vec![address]),
    };
    let certificate = certificates.join(format!("{owner}-certificate.pem"));
    let private_key = certificates.join(format!("{owner}-private-key.pem"));
    Config::from_json5(&json!({
        "mode": mode,
        "connect": { "endpoints": connect, "timeout_ms": 2000, "exit_on_failure": true },
        "listen": { "endpoints": listen },
        "scouting": { "multicast": { "enabled": false, "listen": false }, "gossip": { "enabled": false } },
        "transport": { "link": { "protocols": ["tls"], "tls": {
            "root_ca_certificate": certificates.join("root-ca.pem"),
            "listen_private_key": private_key, "listen_certificate": certificate,
            "connect_private_key": private_key, "connect_certificate": certificate,
            "enable_mtls": true, "verify_name_on_connect": true, "use_public_pki": false
        } } }
    }).to_string())
}

async fn matching_query(querier: &Querier<'_>) -> Result<()> {
    timeout(FIXTURE_WAIT, async {
        loop {
            if querier.matching_status().await?.matching() {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

async fn matching_publication(publisher: &Publisher<'_>) -> Result<()> {
    timeout(FIXTURE_WAIT, async {
        loop {
            if publisher.matching_status().await?.matching() {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

fn fixture_receipt(
    payload: &ZBytes,
    attachment: Option<&ZBytes>,
    observations: u64,
) -> Result<FixtureReceipt> {
    let proof = attachment.ok_or_else(|| invalid_control("fixture request has no proof"))?;
    assert_eq!(proof.to_bytes().as_ref(), REQUEST_PROOF);
    let protected_text = payload.try_to_string()?.into_owned();
    assert_eq!(protected_text.as_bytes(), PROTECTED_TEXT.as_bytes());
    let request: FixtureRequest = serde_json::from_str(&protected_text)?;
    assert_eq!(request.marker, "static-remote-fixture🦀");
    assert_eq!(request.amount, 9_007_199_254_740_993);
    observe(&TransportObservation::Received {
        protected_text: protected_text.clone(),
        proof: proof.to_bytes().into_owned(),
    })?;
    Ok(FixtureReceipt {
        protected_text,
        observations,
    })
}

fn received_receipt(payload: &ZBytes, attachment: Option<&ZBytes>) -> Result<()> {
    let proof = attachment.ok_or_else(|| invalid_control("fixture reply has no proof"))?;
    assert_eq!(proof.to_bytes().as_ref(), REPLY_PROOF);
    let receipt: FixtureReceipt = serde_json::from_slice(&payload.to_bytes())?;
    assert_eq!(receipt.protected_text.as_bytes(), PROTECTED_TEXT.as_bytes());
    assert!(receipt.observations > 0);
    observe(&TransportObservation::Receipt {
        receipt,
        proof: proof.to_bytes().into_owned(),
    })
}

async fn request_ingress(
    session: &Session,
    operation: TransportOperation,
) -> Result<RequestIngress<'_>> {
    Ok(match operation {
        TransportOperation::Query => RequestIngress::Query(
            session
                .declare_queryable(REQUEST_KEY)
                .allowed_origin(Locality::Remote)
                .with(FifoChannel::new(FIXTURE_CAPACITY))
                .await?,
        ),
        TransportOperation::Publication => RequestIngress::Publication {
            subscriber: session
                .declare_subscriber(REQUEST_KEY)
                .allowed_origin(Locality::Remote)
                .with(FifoChannel::new(FIXTURE_CAPACITY))
                .await?,
            publisher: session
                .declare_publisher(RECEIPT_KEY)
                .allowed_destination(Locality::Remote)
                .congestion_control(CongestionControl::Block)
                .await?,
        },
    })
}

async fn recipient(session: &Session, operation: TransportOperation) -> Result<()> {
    let ingress = request_ingress(session, operation).await?;
    observe(&TransportObservation::Ready {
        role: WorkerRole::Recipient,
    })?;
    let mut observations = 0;
    for line in io::stdin().lock().lines() {
        match control_command(&line?)? {
            ControlCommand::Release => {
                match &ingress {
                    RequestIngress::Query(queryable) => {
                        let query = timeout(FIXTURE_WAIT, queryable.recv_async()).await??;
                        observations += 1;
                        let payload = query
                            .payload()
                            .ok_or_else(|| invalid_control("fixture query has no payload"))?;
                        let receipt = fixture_receipt(payload, query.attachment(), observations)?;
                        let encoded = serde_json::to_vec(&receipt)?;
                        observe(&TransportObservation::PotentiallyTransmitted { operation })?;
                        let invocation = query
                            .reply(REQUEST_KEY, encoded)
                            .attachment(REPLY_PROOF.to_vec())
                            .into_future();
                        invocation.await?;
                        observe(&TransportObservation::InvocationReturned { operation })?;
                    }
                    RequestIngress::Publication {
                        subscriber,
                        publisher,
                    } => {
                        let sample = timeout(FIXTURE_WAIT, subscriber.recv_async()).await??;
                        observations += 1;
                        let receipt =
                            fixture_receipt(sample.payload(), sample.attachment(), observations)?;
                        let encoded = serde_json::to_vec(&receipt)?;
                        // A cancelled caller may no longer subscribe. Still invoke the native
                        // receipt publication; its return cannot establish remote delivery.
                        observe(&TransportObservation::PotentiallyTransmitted { operation })?;
                        let invocation = publisher
                            .put(encoded)
                            .attachment(REPLY_PROOF.to_vec())
                            .into_future();
                        invocation.await?;
                        observe(&TransportObservation::InvocationReturned { operation })?;
                    }
                }
            }
            ControlCommand::Exit => break,
            ControlCommand::Run | ControlCommand::Wait | ControlCommand::Cancel => {
                return Err(invalid_control("recipient requires RELEASE or EXIT").into());
            }
        }
    }
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

async fn send_request<'session>(
    session: &'session Session,
    operation: TransportOperation,
    protected: &[u8],
    proof: &[u8],
) -> Result<PendingReceipt<'session>> {
    match operation {
        TransportOperation::Query => {
            let querier = session
                .declare_querier(REQUEST_KEY)
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .accept_replies(ReplyKeyExpr::MatchingQuery)
                .allowed_destination(Locality::Remote)
                .timeout(FIXTURE_WAIT)
                .congestion_control(CongestionControl::Block)
                .await?;
            matching_query(&querier).await?;
            observe(&TransportObservation::PotentiallyTransmitted { operation })?;
            let invocation = querier
                .get()
                .payload(protected.to_vec())
                .attachment(proof.to_vec())
                .with(FifoChannel::new(FIXTURE_CAPACITY))
                .into_future();
            let replies = invocation.await?;
            observe(&TransportObservation::InvocationReturned { operation })?;
            Ok(PendingReceipt::Query { querier, replies })
        }
        TransportOperation::Publication => {
            let subscriber = session
                .declare_subscriber(RECEIPT_KEY)
                .allowed_origin(Locality::Remote)
                .with(FifoChannel::new(FIXTURE_CAPACITY))
                .await?;
            let publisher = session
                .declare_publisher(REQUEST_KEY)
                .allowed_destination(Locality::Remote)
                .congestion_control(CongestionControl::Block)
                .await?;
            matching_publication(&publisher).await?;
            observe(&TransportObservation::PotentiallyTransmitted { operation })?;
            let invocation = publisher
                .put(protected.to_vec())
                .attachment(proof.to_vec())
                .into_future();
            invocation.await?;
            observe(&TransportObservation::InvocationReturned { operation })?;
            Ok(PendingReceipt::Publication {
                subscriber,
                publisher,
            })
        }
    }
}

async fn await_receipt(pending: &PendingReceipt<'_>) -> Result<()> {
    match pending {
        PendingReceipt::Query { replies, .. } => {
            match timeout(FIXTURE_WAIT, replies.recv_async()).await {
                Ok(Ok(reply)) => match reply.into_result() {
                    Ok(sample) => received_receipt(sample.payload(), sample.attachment())?,
                    Err(cause) => observe(&TransportObservation::NativeReplyError {
                        payload: cause.payload().to_bytes().into_owned(),
                        encoding: cause.encoding().to_string(),
                    })?,
                },
                Ok(Err(cause)) => observe(&TransportObservation::ReceiverClosed {
                    cause: cause.to_string(),
                })?,
                Err(_) => observe(&TransportObservation::WaitExpired)?,
            }
        }
        PendingReceipt::Publication { subscriber, .. } => {
            match timeout(FIXTURE_WAIT, subscriber.recv_async()).await {
                Ok(Ok(sample)) => received_receipt(sample.payload(), sample.attachment())?,
                Ok(Err(cause)) => observe(&TransportObservation::ReceiverClosed {
                    cause: cause.to_string(),
                })?,
                Err(_) => observe(&TransportObservation::WaitExpired)?,
            }
        }
    }
    Ok(())
}

async fn relinquish_receipt(pending: PendingReceipt<'_>) -> Result<()> {
    match pending {
        PendingReceipt::Query { querier, replies } => {
            querier.undeclare().await?;
            drop(replies);
        }
        PendingReceipt::Publication {
            subscriber,
            publisher,
        } => {
            subscriber.undeclare().await?;
            publisher.undeclare().await?;
        }
    }
    Ok(())
}

async fn caller(session: &Session, operation: TransportOperation) -> Result<()> {
    observe(&TransportObservation::Ready {
        role: WorkerRole::Caller,
    })?;
    let mut pending = None;
    for line in io::stdin().lock().lines() {
        match control_command(&line?)? {
            ControlCommand::Run => {
                if pending.is_some() {
                    return Err(invalid_control("fixture already owns a pending receipt").into());
                }
                pending = Some(
                    send_request(session, operation, PROTECTED_TEXT.as_bytes(), REQUEST_PROOF)
                        .await?,
                );
            }
            ControlCommand::Wait => {
                await_receipt(
                    pending
                        .as_ref()
                        .ok_or_else(|| invalid_control("WAIT needs a pending receipt"))?,
                )
                .await?
            }
            ControlCommand::Cancel => {
                let relinquished = pending
                    .take()
                    .ok_or_else(|| invalid_control("CANCEL needs a pending receipt"))?;
                relinquish_receipt(relinquished).await?;
                observe(&TransportObservation::WaitCancelled)?;
            }
            ControlCommand::Exit => break,
            ControlCommand::Release => {
                return Err(invalid_control("caller cannot release recipient work").into());
            }
        }
    }
    if let Some(pending) = pending {
        relinquish_receipt(pending).await?;
    }
    Ok(())
}

async fn execute(
    role: WorkerRole,
    operation: TransportOperation,
    config: Config,
) -> Result<TransportReceipts> {
    match role {
        WorkerRole::RecordCounterCaller
        | WorkerRole::RecordCounterRecipient
        | WorkerRole::ScheduledCounterCaller
        | WorkerRole::ScheduledCounterRecipient => {
            return counter_application::execute(role, operation, config).await;
        }
        WorkerRole::Router | WorkerRole::Recipient | WorkerRole::Caller => {}
    }
    let session = zenoh::open(config).await?;
    let outcome = match role {
        WorkerRole::Recipient => recipient(&session, operation).await,
        WorkerRole::Caller => caller(&session, operation).await,
        WorkerRole::Router => router_control(),
        WorkerRole::RecordCounterCaller
        | WorkerRole::RecordCounterRecipient
        | WorkerRole::ScheduledCounterCaller
        | WorkerRole::ScheduledCounterRecipient => {
            unreachable!("counter roles retain their own Application/session custody")
        }
    };
    let session_close = session.close().await;
    Ok(TransportReceipts {
        role,
        worker: outcome,
        session_close,
    })
}

fn router_control() -> Result<()> {
    observe(&TransportObservation::Ready {
        role: WorkerRole::Router,
    })?;
    if let Some(line) = io::stdin().lock().lines().next() {
        match control_command(&line?)? {
            ControlCommand::Exit => {}
            ControlCommand::Run
            | ControlCommand::Wait
            | ControlCommand::Cancel
            | ControlCommand::Release => return Err(invalid_control("router requires EXIT").into()),
        }
    }
    Ok(())
}

fn main() -> Result<TransportReceipts> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments.len() != 5 {
        return Err(invalid_control(
            "expected role layout operation tls-address certificate-directory",
        )
        .into());
    }
    let role = match arguments[0].as_str() {
        "router" => WorkerRole::Router,
        "recipient" => WorkerRole::Recipient,
        "caller" => WorkerRole::Caller,
        "record_counter_caller" => WorkerRole::RecordCounterCaller,
        "record_counter_recipient" => WorkerRole::RecordCounterRecipient,
        "scheduled_counter_caller" => WorkerRole::ScheduledCounterCaller,
        "scheduled_counter_recipient" => WorkerRole::ScheduledCounterRecipient,
        _ => return Err(invalid_control("unknown worker role").into()),
    };
    let layout = match arguments[1].as_str() {
        "peer" => ConnectionLayout::Peer,
        "router_client" => ConnectionLayout::RouterClient,
        _ => return Err(invalid_control("unknown connection layout").into()),
    };
    let operation = match arguments[2].as_str() {
        "query" => TransportOperation::Query,
        "publication" => TransportOperation::Publication,
        _ => return Err(invalid_control("unknown transport operation").into()),
    };
    let config = configuration(role, layout, &arguments[3], Path::new(&arguments[4]))?;
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(execute(role, operation, config))
}
