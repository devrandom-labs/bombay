use super::*;
use bombay::ActorInterface;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
use zenoh::{
    Config,
    bytes::ZBytes,
    query::{ConsolidationMode, Query, QueryTarget, Reply},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemoteCounterRole {
    Router,
    Recipient,
    ChildRecipient,
    Caller,
}
#[derive(Clone, Copy)]
enum RemoteCounterLayout {
    Peer,
    RouterClient,
}
#[derive(Default)]
struct RawCounterDiscards {
    full: AtomicUsize,
    closed: AtomicUsize,
}

fn counter_native_config(role: RemoteCounterRole) -> Config {
    let layout = match env::var("BOMBAY_COUNTER_LAYOUT").as_deref() {
        Ok("peer") => RemoteCounterLayout::Peer,
        Ok("router") => RemoteCounterLayout::RouterClient,
        _ => panic!("explicit layout"),
    };
    let address = env::var("BOMBAY_COUNTER_ADDRESS").expect("explicit TLS address");
    let certificates = env::var("BOMBAY_COUNTER_CERTIFICATES").expect("explicit deployment roots");
    let owner = Path::new(&certificates);
    let (mode, listen, connect) = match (role, layout) {
        (RemoteCounterRole::Router, RemoteCounterLayout::RouterClient) => {
            ("router", vec![address], vec![])
        }
        (
            RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient,
            RemoteCounterLayout::Peer,
        ) => ("peer", vec![address], vec![]),
        (RemoteCounterRole::Caller, RemoteCounterLayout::Peer) => ("peer", vec![], vec![address]),
        (
            RemoteCounterRole::Recipient
            | RemoteCounterRole::ChildRecipient
            | RemoteCounterRole::Caller,
            RemoteCounterLayout::RouterClient,
        ) => ("client", vec![], vec![address]),
        _ => panic!("explicit supported deployment role and layout"),
    };
    let certificate_owner = match role {
        RemoteCounterRole::Router => "router",
        RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient => "recipient",
        RemoteCounterRole::Caller => "caller",
    };
    Config::from_json5(
        &serde_json::json!({"mode": mode,
            "connect":{"endpoints":connect,"timeout_ms":2000,"exit_on_failure":true},
            "listen":{"endpoints":listen},
            "scouting":{"multicast":{"enabled":false,"listen":false},"gossip":{"enabled":false}},
            "transport":{"link":{"protocols":["tls"],"tls":{
            "root_ca_certificate":owner.join("root-ca.pem"),
            "listen_private_key":owner.join(format!("{certificate_owner}-private-key.pem")),
            "listen_certificate":owner.join(format!("{certificate_owner}-certificate.pem")),
            "connect_private_key":owner.join(format!("{certificate_owner}-private-key.pem")),
            "connect_certificate":owner.join(format!("{certificate_owner}-certificate.pem")),
            "enable_mtls":true,"verify_name_on_connect":true,"use_public_pki":false}}}
        })
        .to_string(),
    )
    .expect("selected configured TLS profile")
}
struct CounterNativeReadFailure {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
    protected_read: io::Result<()>,
    proof_read: io::Result<()>,
}
fn counter_native_record(
    payload: &ZBytes,
    proof: &ZBytes,
) -> Option<Result<CounterRequest, CounterNativeReadFailure>> {
    if payload.len() > 8192
        || proof.len() > 64
        || payload
            .len()
            .checked_add(proof.len())
            .and_then(|n| n.checked_mul(2))
            .is_none_or(|n| n > 16512)
    {
        return None;
    }
    let mut protected = vec![0; payload.len()].into_boxed_slice();
    let protected_read = payload.reader().read_exact(&mut protected);
    let mut proof_bytes = vec![0; proof.len()].into_boxed_slice();
    let proof_read = proof.reader().read_exact(&mut proof_bytes);
    Some(match (protected_read, proof_read) {
        (Ok(()), Ok(())) => Ok(CounterRequest {
            protected,
            proof: proof_bytes,
        }),
        (protected_read, proof_read) => Err(CounterNativeReadFailure {
            protected,
            proof: proof_bytes,
            protected_read,
            proof_read,
        }),
    })
}
fn counter_control(observation: &str) {
    println!("{observation}");
    let flushed = io::stdout().flush();
    flushed.expect("controller observation publication");
}
fn counter_release() -> (String, io::Result<usize>) {
    let mut release = String::new();
    let acquired = io::stdin().lock().take(65).read_line(&mut release);
    (release, acquired)
}
#[test]
#[ignore = "requires controlled TLS subprocess fixture"]
fn remote_counter_query_preserves_admission_and_processing() {
    let role = match env::var("BOMBAY_COUNTER_ROLE").as_deref() {
        Ok("router") => RemoteCounterRole::Router,
        Ok("recipient") => RemoteCounterRole::Recipient,
        Ok("child-recipient") => RemoteCounterRole::ChildRecipient,
        Ok("caller") => RemoteCounterRole::Caller,
        _ => panic!("explicit controlled subprocess role"),
    };
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("explicit fixture host");
    if role == RemoteCounterRole::Router {
        host.block_on(async {
            let session = zenoh::open(counter_native_config(role))
                .await
                .expect("router Session");
            counter_control("READY");
            let released = counter_release();
            let closed = session.close().await;
            assert!(
                matches!(
                    (&released.1, released.0.as_str()),
                    (Ok(_), "release-router\n")
                ) && closed.is_ok()
            );
        });
        return;
    }
    let profile = match env::var("BOMBAY_COUNTER_PROVIDER").as_deref() {
        Ok("recorded") => CounterProofProfile::RecordedTest,
        Ok("scheduled") => CounterProofProfile::ScheduledTest,
        _ => panic!("explicit provider"),
    };
    let mut authority = AssignmentAuthority {
        host: &host,
        runtime_generation: 23,
        grant_issuer: "grant-controller",
        authority_generation: 7,
        permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
        next: Some(29),
    };
    let configured = match role {
        RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient => None,
        RemoteCounterRole::Caller => {
            let runtime_generation = env::var("BOMBAY_COUNTER_RUNTIME_GENERATION")
                .expect("trusted issued runtime projection")
                .parse()
                .expect("configured runtime generation");
            let actor_generation = env::var("BOMBAY_COUNTER_ACTOR_GENERATION")
                .expect("trusted issued actor projection")
                .parse()
                .expect("configured actor generation");
            Some((
                Binding {
                    deployment: "test-deployment",
                    caller: "caller",
                    controller: "controller",
                    actor: "counter",
                    host: "counter-host",
                    protocol: "counter-protocol",
                    runtime_generation,
                    actor_generation,
                },
                Instant::now()
                    .checked_add(Duration::from_secs(60))
                    .expect("finite observation interval"),
            ))
        }
        RemoteCounterRole::Router => unreachable!("router returned before actor capture"),
    };
    match profile {
        CounterProofProfile::RecordedTest => host.block_on(counter_native_applications(
            role,
            &mut authority,
            configured,
            profile,
            &record_provider::verify,
            record_provider::projection,
            record_provider::meaning,
            record_provider::endpoints,
        )),
        CounterProofProfile::ScheduledTest => host.block_on(counter_native_applications(
            role,
            &mut authority,
            configured,
            profile,
            &scheduled_provider::verify,
            scheduled_provider::projection,
            scheduled_provider::meaning,
            scheduled_provider::endpoints,
        )),
    }
}
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "One typed two-provider native crossing retains every Query, Reply, Application and notification original before independent cleanup oracles."
)]
async fn counter_native_applications<R>(
    role: RemoteCounterRole,
    authority: &mut AssignmentAuthority<'_>,
    configured: Option<(Binding, Instant)>,
    profile: CounterProofProfile,
    provider: &impl AsyncFn(
        &CounterRequest,
        &[ProtectedRecord],
        AuthorityFreshness,
        ProviderAvailability,
        CounterProofProfile,
        CounterProofPurpose,
        &str,
        &str,
        &protected::Limits,
    ) -> Result<R, VerificationError>,
    projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
    meaning: fn(&R) -> CounterProtectedMeaning,
    endpoints: fn(&R) -> (&'static str, &'static str),
) {
    let challenged = match env::var("BOMBAY_COUNTER_EVIDENCE_SOURCE").as_deref() {
        Ok("target") => None,
        Ok("sibling-processing") => Some(CounterProofPurpose::IncrementProcessed),
        Ok("sibling-read") => Some(CounterProofPurpose::ReadReply),
        _ => panic!("explicit controlled native evidence source"),
    };
    assert!(
        !matches!((role, challenged), (RemoteCounterRole::Recipient, Some(_))),
        "root numeric origins cannot establish a same-space producer challenge"
    );
    let session = zenoh::open(counter_native_config(role))
        .await
        .expect("native Session");
    let exchange = async |authority: &AssignmentAuthority<'_>,
                          binding: Binding,
                          deadline: Instant,
                          records: Vec<ProtectedRecord>,
                          target: EstablishedRecipient<Counter>,
                          target_origin: MailAddr,
                          original_stop: &CounterStopAuthority,
                          sibling: EstablishedRecipient<Counter>,
                          sibling_origin: MailAddr,
                          sibling_stop: &CounterStopAuthority,
                          interface: ActorInterface<()>,
                          sibling_requests: ExternalActor<CounterRequests>,
                          mut sibling_values: ExternalActor<CounterValue>,
                          mut requests: ExternalActor<CounterRequests>| {
        let mut values = interface
            .external::<CounterValue>()
            .expect("exact target audit port");
        let mut processing = interface
            .external::<CounterProcessed>()
            .expect("processing port");
        let mut original_retirement = pin!(original_stop.retirement());
        let mut authorization = match role {
            RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient => {
                let mut authorization = CounterAuthorization::new(
                    StartupMode::ExplicitTest,
                    Some((binding, deadline, target.clone())),
                    authority.grant_issuer,
                    authority.authority_generation,
                    authority.permitted_operations,
                    projection,
                    profile,
                    CounterLimits {
                        incoming: NonZeroUsize::new(2).unwrap(),
                        retained: NonZeroUsize::new(3).unwrap(),
                        individual: NonZeroUsize::new(131_072).unwrap(),
                        total: NonZeroUsize::new(262_144).unwrap(),
                        verification_maximum: 65_536,
                    },
                )
                .expect("finite authorization");
                authorization.processing = Some(processing.recipient());
                authorization.stop = Some(match original_stop {
                    CounterStopAuthority::Root(root) => CounterStopAuthority::Root(root.clone()),
                    CounterStopAuthority::Child(child) => {
                        CounterStopAuthority::Child(child.clone())
                    }
                });
                Some(authorization)
            }
            RemoteCounterRole::Caller => None,
            RemoteCounterRole::Router => unreachable!("router has its own Session scope"),
        };
        let (native_queries, mut received_queries) = mpsc::channel::<Query>(2);
        let query_dispositions = Arc::new(RawCounterDiscards::default());
        let reply_dispositions = Arc::new(RawCounterDiscards::default());
        let queryable = match role {
            RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient => {
                let dispositions = query_dispositions.clone();
                Some(
                    session
                        .declare_queryable("bombay/counter/protected")
                        .complete(true)
                        .callback(move |query| match native_queries.try_send(query) {
                            Ok(()) => (),
                            Err(mpsc::error::TrySendError::Full(original)) => {
                                dispositions.full.fetch_add(1, Ordering::Relaxed);
                                drop(original);
                            }
                            Err(mpsc::error::TrySendError::Closed(original)) => {
                                dispositions.closed.fetch_add(1, Ordering::Relaxed);
                                drop(original);
                            }
                        })
                        .await
                        .expect("native queryable"),
                )
            }
            RemoteCounterRole::Caller => None,
            RemoteCounterRole::Router => unreachable!("router scope"),
        };
        let mut native_records = Vec::new();
        let mut declarations = Vec::new();
        let mut request_reads = Vec::new();
        let mut query_releases = Vec::new();
        let mut reservations = Vec::new();
        let mut outbound_records = Vec::new();
        let mut outbound_receipts = Vec::new();
        let mut emissions = Vec::new();
        let mut invocations = Vec::new();
        let mut verified = Vec::new();
        let mut completions = Vec::new();
        let mut reply_authorities = Vec::new();
        let mut acknowledgements = Vec::new();
        let mut requester_terminations = Vec::new();
        let mut requester_receipts = Vec::new();
        let mut local_requester_native = None;
        let mut local_receipts = None;
        let mut processing_reply = None;
        let mut processing_challenges = Vec::new();
        let mut read_challenges = Vec::new();
        let mut reports = Vec::new();
        let mut admissions = Vec::new();
        let mut read_ports = Vec::new();
        let mut joined_report = None;
        let mut original_target_report = None;
        let mut target_after_refusal = None;
        let mut stop_grant = None;
        let mut pending = None;
        let requester_service = requests.recipient();
        let requester_counter = target.clone();
        let requester_reads = values.recipient();
        let execute_requester = || {
            let (publication, ports) = oneshot::channel();
            let (permitted, permission) = oneshot::channel();
            let requester = CounterRequester {
                service: requester_service.clone(),
                counter: requester_counter.clone(),
                replies: requester_reads.clone(),
                state: CounterRequesterState::Ready,
            };
            let checked = authority.check_host();
            checked.expect("actual local Requester capture host");
            let (execution, receiving) = Application::new(requester.stop_on_shutdown())
                .execute_with::<_, _, Never, (), _, _>(|application| async move {
                    let publication = publication.send((
                        application.lifecycle(),
                        application.root().established_recipient(),
                        application
                            .interface(())
                            .external::<CounterRequests>()
                            .expect("requester sender"),
                    ));
                    let permission = permission.await;
                    (publication, permission)
                })
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("existing requester");
                });
            (execution, receiving, ports, permitted)
        };
        if role == RemoteCounterRole::Caller {
            let querier = session
                .declare_querier("bombay/counter/protected")
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .await
                .expect("command-owned Querier");
            while !querier
                .matching_status()
                .await
                .expect("native route observation")
                .matching()
            {
                tokio::task::yield_now().await;
            }
            for (ordinal, sequence) in [0usize, 0, 1, 2, 3, 3].into_iter().enumerate() {
                let scope = records[sequence].scope;
                let (
                    requester_execution,
                    requester_receiving,
                    requester_ports,
                    requester_permitted,
                ) = execute_requester();
                let request = async {
                    let (requester_lifecycle, requester, requester_sender) =
                        requester_ports.await.expect("actual requester publication");
                    let begun = requester_sender
                        .send(
                            &requester,
                            CounterRequesterMessage::BeginProtected {
                                correlation: ordinal as u64,
                                scope,
                            },
                        )
                        .await;
                    let triggered = requests.receive().await;
                    let (native_replies, mut replies) = mpsc::channel::<Reply>(2);
                    let dispositions = reply_dispositions.clone();
                    let issued = querier
                        .get()
                        .payload(ZBytes::from(records[sequence].protected.as_ref()))
                        .attachment(ZBytes::from(records[sequence].proof.as_ref()))
                        .callback(move |reply| match native_replies.try_send(reply) {
                            Ok(()) => (),
                            Err(mpsc::error::TrySendError::Full(original)) => {
                                dispositions.full.fetch_add(1, Ordering::Relaxed);
                                drop(original);
                            }
                            Err(mpsc::error::TrySendError::Closed(original)) => {
                                dispositions.closed.fetch_add(1, Ordering::Relaxed);
                                drop(original);
                            }
                        })
                        .await;
                    invocations.push(issued);
                    if ordinal == 0 {
                        pending = Some(replies.recv().now_or_never());
                        let (local_execution, local_receiving, local_ports, local_permitted) =
                            execute_requester();
                        let local_exchange = async {
                            let (local_lifecycle, local_requester, local_sender) = local_ports
                                .await
                                .expect("actual independent local requester");
                            let first = requests.send(&target, CounterMessage::Increment).await;
                            let second = requests.send(&target, CounterMessage::Increment).await;
                            let begun = local_sender
                                .send(&local_requester, CounterRequesterMessage::Begin(9))
                                .await;
                            let trigger = requests.receive().await;
                            let local_status = match (&first, &second) {
                                (Ok(()), Ok(())) => CounterStatus::IncrementAdmitted,
                                _ => CounterStatus::PossiblyAdmitted,
                            };
                            let actual_correlation =
                                match trigger.as_ref().map(|received| &received.message) {
                                    Some(CounterServiceInput::Verify(correlation)) => {
                                        Some(*correlation)
                                    }
                                    _ => None,
                                };
                            let completed = if actual_correlation == Some(9) {
                                Some(
                                    local_sender
                                        .send(
                                            &local_requester,
                                            CounterRequesterMessage::VerificationCompleted(
                                                CounterReport {
                                                    correlation: 9,
                                                    status: local_status,
                                                },
                                            ),
                                        )
                                        .await,
                                )
                            } else {
                                None
                            };
                            let consumed = if completed.as_ref().is_some_and(Result::is_ok) {
                                requests.receive().await
                            } else {
                                None
                            };
                            let read = if local_status == CounterStatus::IncrementAdmitted
                                && completed.as_ref().is_some_and(Result::is_ok)
                            {
                                values.receive().await
                            } else {
                                None
                            };
                            let observed = match read.as_ref() {
                                Some(read) => Some(
                                    local_sender
                                        .send(
                                            &local_requester,
                                            CounterRequesterMessage::ReadObserved {
                                                correlation: 9,
                                                value: read.message,
                                            },
                                        )
                                        .await,
                                ),
                                None => None,
                            };
                            let acknowledged = if observed.as_ref().is_some_and(Result::is_ok) {
                                requests.receive().await
                            } else {
                                None
                            };
                            let stopped = local_lifecycle.request_shutdown();
                            let permitted = local_permitted.send(());
                            (
                                first,
                                second,
                                begun,
                                trigger,
                                completed,
                                consumed,
                                read,
                                observed,
                                acknowledged,
                                stopped,
                                permitted,
                            )
                        };
                        let (local_execution, local_exchange) =
                            tokio::join!(local_execution, local_exchange);
                        local_requester_native = Some(local_receiving.await);
                        local_receipts = Some((local_execution, local_exchange));

                        if matches!(&pending, Some(None))
                            && local_receipts.as_ref().is_some_and(|((), receipts)| {
                                receipts.6.as_ref().is_some_and(|read| read.message == 2)
                            })
                        {
                            counter_control("PENDING_LOCAL_READ2");
                        } else {
                            counter_control("LOCAL_READ_UNESTABLISHED");
                        }
                    }
                    while let Some(reply) = replies.recv().await {
                        let copied = reply.result().as_ref().ok().and_then(|sample| {
                            sample
                                .attachment()
                                .and_then(|proof| counter_native_record(sample.payload(), proof))
                        });
                        native_records.push((reply, copied));
                        let Some(Ok(original)) = &native_records.last().expect("native original").1
                        else {
                            continue;
                        };
                        declarations.push(protected::decode(original, &counter_test_json_limits()));
                        let Ok(declaration) =
                            declarations.last().expect("original declaration result")
                        else {
                            continue;
                        };
                        let purpose = match declaration {
                            protected::Message::Request(_) => CounterProofPurpose::Request,
                            protected::Message::Refused(_) => CounterProofPurpose::Refused,
                            protected::Message::MailboxAdmitted(_) => {
                                CounterProofPurpose::MailboxAdmitted
                            }
                            protected::Message::IncrementProcessed(_) => {
                                CounterProofPurpose::IncrementProcessed
                            }
                            protected::Message::ReadReply(_) => CounterProofPurpose::ReadReply,
                            protected::Message::StopAccepted(_) => {
                                CounterProofPurpose::StopAccepted
                            }
                            protected::Message::RetirementReport(_) => {
                                CounterProofPurpose::RetirementReport
                            }
                        };
                        let verified_reply = provider(
                            original,
                            &records,
                            AuthorityFreshness::Current,
                            ProviderAvailability::Available,
                            profile,
                            purpose,
                            binding.host,
                            binding.caller,
                            &counter_test_json_limits(),
                        )
                        .await;
                        verified.push(verified_reply);
                        let Ok(receipt) = verified.last().expect("original provider result") else {
                            continue;
                        };
                        let current = counter_protected_reply(
                            binding,
                            authority.grant_issuer,
                            authority.authority_generation,
                            profile,
                            receipt,
                            scope,
                            purpose,
                            projection,
                            meaning,
                            endpoints,
                            &[
                                CounterProofPurpose::Refused,
                                CounterProofPurpose::MailboxAdmitted,
                                CounterProofPurpose::IncrementProcessed,
                                CounterProofPurpose::ReadReply,
                                CounterProofPurpose::StopAccepted,
                                CounterProofPurpose::RetirementReport,
                            ],
                        );
                        reply_authorities.push(current);
                        if let Ok(fact) = current {
                            let sent = requester_sender
                                .send(
                                    &requester,
                                    CounterRequesterMessage::ProtectedReply {
                                        correlation: ordinal as u64,
                                        scope,
                                        meaning: fact,
                                    },
                                )
                                .await;
                            completions.push((current, sent));
                            let acknowledged = if completions
                                .last()
                                .expect("completion original")
                                .1
                                .is_ok()
                            {
                                tokio::select! {
                                    biased;
                                    received = requests.receive() => Some(received),
                                    ended = requester_lifecycle.termination() => { requester_terminations.push(ended); None }
                                }
                            } else {
                                None
                            };
                            if ordinal == 3
                                && fact == CounterProtectedMeaning::Refused(CounterRefusal::Grant)
                                && matches!(&acknowledged, Some(Some(received)) if matches!(received.message,
                                        CounterServiceInput::ProtectedConsumed { correlation: 3, purpose: CounterProofPurpose::Refused }))
                            {
                                counter_control("SEND_ONLY_REFUSED");
                            }
                            acknowledgements.push(acknowledged);
                        }
                    }
                    if ordinal == 3 {
                        counter_control("STOP_REFUSAL_SETTLED");
                    }
                    let stopped = requester_lifecycle.request_shutdown();
                    let released = requester_permitted.send(());
                    (begun, triggered, stopped, released)
                };
                let ((), request) = tokio::join!(requester_execution, request);
                let native = requester_receiving.await;
                requester_receipts.push((ordinal, sequence, native, request));
            }
            invocations.push(querier.undeclare().await);
        } else {
            counter_control("READY");
            for ordinal in 0..6 {
                if ordinal == 4 {
                    let release = counter_release();
                    if let (Ok(_), "grant-stop\n") = (&release.1, release.0.as_str()) {
                        authorization
                            .as_mut()
                            .expect("recipient authority")
                            .permitted_operations =
                            &[CounterOperation::Read, CounterOperation::Stop];
                    }
                    stop_grant = Some(release);
                }
                let query = received_queries
                    .recv()
                    .await
                    .expect("actual native request");
                if ordinal == 0 {
                    counter_control("RECEIVED");
                    query_releases.push(counter_release());
                }
                let copied = query.payload().and_then(|payload| {
                    query
                        .attachment()
                        .and_then(|proof| counter_native_record(payload, proof))
                });
                let mut request_values = interface
                    .external::<CounterValue>()
                    .expect("request-specific Read port");
                let mut read_reply = None;
                match copied {
                    Some(Ok(original)) => {
                        let authorization = authorization
                            .as_mut()
                            .expect("actual recipient admission authority");
                        reservations.push(authorization.reserve(original));
                        let Ok(correlation) = reservations.last().expect("original reservation")
                        else {
                            drop(query);
                            continue;
                        };
                        let correlation = *correlation;
                        let position = authorization
                            .pending
                            .iter()
                            .position(|p| p.correlation == correlation)
                            .expect("actual pending ownership");
                        acquire_verification(
                            &mut authorization.pending[position],
                            async |original| {
                                provider(
                                    original,
                                    &records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    profile,
                                    CounterProofPurpose::Request,
                                    binding.caller,
                                    binding.host,
                                    &counter_test_json_limits(),
                                )
                                .await
                            },
                        )
                        .await;
                        let Verification::Acquired(Ok(receipt)) =
                            &authorization.pending[position].verification
                        else {
                            admissions.push(authorization.consume(correlation));
                            drop(query);
                            continue;
                        };
                        let scope = *projection(receipt).0;
                        authorization.associate(position, &requests, &request_values.recipient());
                        authorization.attempt(correlation, &requests);
                        while authorization.report(correlation).is_none() {
                            tokio::task::yield_now().await;
                            authorization.attempt(correlation, &requests);
                        }
                        let report = authorization
                            .report(correlation)
                            .expect("actual settled report");
                        reports.push(report);
                        admissions.push(authorization.consume(correlation));
                        {
                            let mut emit = async |fact: CounterProtectedMeaning| {
                                outbound_records.push(counter_record(
                                    scope,
                                    profile,
                                    fact,
                                    &[43],
                                    &mut [0; 8192],
                                ));
                                let Ok(record) =
                                    outbound_records.last().expect("original encoding")
                                else {
                                    return;
                                };
                                outbound_receipts.push((counter_request(record), None));
                                let original = &outbound_receipts
                                    .last()
                                    .expect("raw reply before proof wait")
                                    .0;
                                let receipt = provider(
                                    original,
                                    &records,
                                    AuthorityFreshness::Current,
                                    ProviderAvailability::Available,
                                    profile,
                                    fact.purpose(),
                                    binding.host,
                                    binding.caller,
                                    &counter_test_json_limits(),
                                )
                                .await;
                                outbound_receipts.last_mut().expect("raw reply owner").1 =
                                    Some(receipt);
                                let Ok(receipt) = outbound_receipts
                                    .last()
                                    .expect("proof owner")
                                    .1
                                    .as_ref()
                                    .expect("actual proof result")
                                else {
                                    return;
                                };
                                let current = authorization.protected_reply(
                                    receipt,
                                    scope,
                                    fact.purpose(),
                                    meaning,
                                    endpoints,
                                    &[
                                        CounterProofPurpose::Refused,
                                        CounterProofPurpose::MailboxAdmitted,
                                        CounterProofPurpose::IncrementProcessed,
                                        CounterProofPurpose::ReadReply,
                                        CounterProofPurpose::StopAccepted,
                                        CounterProofPurpose::RetirementReport,
                                    ],
                                );
                                emissions.push(current);
                                if current == Ok(fact) {
                                    invocations.push(
                                        query
                                            .reply(
                                                "bombay/counter/protected",
                                                ZBytes::from(record.protected.as_ref()),
                                            )
                                            .attachment(ZBytes::from(record.proof.as_ref()))
                                            .await,
                                    );
                                }
                            };
                            match report.status {
                                CounterStatus::IncrementAdmitted => {
                                    emit(CounterProtectedMeaning::IncrementAdmitted).await;
                                    if processing_reply.is_none() {
                                        processing_reply = processing.receive().await;
                                    }
                                    let processed = match (
                                        challenged,
                                        admissions
                                            .last()
                                            .and_then(Option::as_ref)
                                            .map(|p| &p.verification),
                                    ) {
                                        (
                                            Some(CounterProofPurpose::IncrementProcessed),
                                            Some(Verification::Native {
                                                delivery: CounterDelivery::IncrementAdmitted,
                                                ..
                                            }),
                                        ) => {
                                            let sent = sibling_requests
                                                .send(
                                                    &sibling,
                                                    CounterMessage::IncrementAndReply {
                                                        request_sequence: scope.identity.sequence,
                                                        reply_to: processing.recipient(),
                                                    },
                                                )
                                                .await;
                                            let received = match &sent {
                                                Ok(()) => processing.receive().await,
                                                Err(_) => None,
                                            };
                                            processing_challenges.push((sent, received));
                                            processing_challenges
                                                .last()
                                                .expect("actual alien processing originals")
                                                .1
                                                .as_ref()
                                        }
                                        _ => processing_reply.as_ref(),
                                    };
                                    if let Some(processed) = processed
                                        && processed.from == target_origin
                                        && processed.message.request_sequence
                                            == scope.identity.sequence
                                    {
                                        emit(CounterProtectedMeaning::IncrementProcessed).await;
                                    }
                                }
                                CounterStatus::ReadAdmitted => {
                                    emit(CounterProtectedMeaning::ReadAdmitted).await;
                                    read_reply = request_values.receive().await;
                                    let candidate = match challenged {
                                        Some(CounterProofPurpose::ReadReply) => {
                                            let incremented = sibling_requests
                                                .send(&sibling, CounterMessage::Increment)
                                                .await;
                                            let sent = sibling_requests
                                                .send(
                                                    &sibling,
                                                    CounterMessage::Read(
                                                        request_values.recipient(),
                                                    ),
                                                )
                                                .await;
                                            let received = match &sent {
                                                Ok(()) => request_values.receive().await,
                                                Err(_) => None,
                                            };
                                            read_challenges.push((incremented, sent, received));
                                            read_challenges
                                                .last()
                                                .expect("actual alien Read originals")
                                                .2
                                                .as_ref()
                                        }
                                        _ => read_reply.as_ref(),
                                    };
                                    if let Some(read) = candidate
                                        && read.from == target_origin
                                    {
                                        emit(CounterProtectedMeaning::ReadReply(read.message))
                                            .await;
                                    }
                                }
                                CounterStatus::Refused(refusal) => {
                                    emit(CounterProtectedMeaning::Refused(refusal)).await;
                                }
                                CounterStatus::StopAccepted => {
                                    emit(CounterProtectedMeaning::StopAccepted).await;
                                    if joined_report.is_none() {
                                        joined_report = Some(
                                            authorization
                                                .stop
                                                .as_ref()
                                                .expect("actual Stop capability")
                                                .retirement()
                                                .await,
                                        );
                                        original_target_report =
                                            original_retirement.as_mut().now_or_never();
                                    }
                                    if let Some(report) = joined_report {
                                        emit(CounterProtectedMeaning::RetirementReport {
                                            retirement: report.retirement(),
                                            failures: report.failures(),
                                        })
                                        .await;
                                    }
                                }
                                CounterStatus::StopRejected(_)
                                | CounterStatus::PossiblyAdmitted
                                | CounterStatus::Closed => (),
                            }
                        }
                        if ordinal == 3 {
                            let sent = requests
                                .send(&target, CounterMessage::Read(values.recipient()))
                                .await;
                            let read = match &sent {
                                Ok(()) => values.receive().await,
                                Err(_) => None,
                            };
                            target_after_refusal = Some((sent, read));
                        }
                    }
                    original => request_reads.push(original),
                }
                read_ports.push((request_values, read_reply));
                drop(query); // Release native FINAL only after the original reply Results are retained.
            }
        }
        let read_sent = requests
            .send(&target, CounterMessage::Read(values.recipient()))
            .await;
        let read = match &read_sent {
            Ok(()) => values.receive().await,
            Err(_) => None,
        };
        let sibling_increment = sibling_requests
            .send(&sibling, CounterMessage::Increment)
            .await;
        let sibling_read = sibling_requests
            .send(&sibling, CounterMessage::Read(sibling_values.recipient()))
            .await;
        let sibling_value = match &sibling_read {
            Ok(()) => sibling_values.receive().await,
            Err(_) => None,
        };
        let undeclared = match queryable {
            Some(queryable) => Some(queryable.undeclare().await),
            None => None,
        };
        let stopped = original_stop.request_shutdown();
        let sibling_stopped = sibling_stop.request_shutdown();
        (
            binding,
            target_origin,
            sibling_origin,
            records,
            authorization,
            native_records,
            declarations,
            request_reads,
            query_releases,
            reservations,
            outbound_records,
            outbound_receipts,
            emissions,
            invocations,
            verified,
            completions,
            acknowledgements,
            reply_authorities,
            requester_terminations,
            requester_receipts,
            local_requester_native,
            local_receipts,
            processing_reply,
            processing_challenges,
            read_challenges,
            reports,
            admissions,
            read_ports,
            joined_report,
            original_target_report,
            target_after_refusal,
            stop_grant,
            pending,
            read_sent,
            read,
            sibling_increment,
            sibling_read,
            sibling_value,
            undeclared,
            stopped,
            sibling_stopped,
            query_dispositions,
            reply_dispositions,
        )
    };
    let service = match role {
        RemoteCounterRole::ChildRecipient => {
            let (published, ports) = oneshot::channel();
            let (permit, permission) = oneshot::channel();
            let checked = authority.check_host();
            checked.expect("actual parent capture host");
            let (execution, receiving) =
                Application::new(CounterParent::default().stop_on_shutdown())
                    .execute_with::<_, _, CounterChildrenTerminal, _, _, _>(
                        |application| async move {
                            let interface = application.interface(());
                            let publication = published.send((
                                application.lifecycle(),
                                application.root().established_recipient(),
                                interface.clone(),
                                interface.external::<CounterExports>().unwrap(),
                                interface.external::<CounterRequests>().unwrap(),
                            ));
                            let permission = permission.await;
                            (publication, permission)
                        },
                    )
                    .unwrap_or_else(|original| {
                        drop(original);
                        panic!("actual parent construction");
                    });
            let work = async {
                let (lifecycle, parent, interface, mut exports, requests) =
                    ports.await.expect("actual parent publication");
                let exported = exports.send(&parent, exports.recipient()).await;
                let children = exports.receive().await;
                let CounterChildren { target, sibling } =
                    &children.as_ref().expect("real child export").message;
                let mut target_export = CounterStopExport::default();
                let mut sibling_export = CounterStopExport::default();
                target.clone().interpret_actor(&mut target_export);
                sibling.clone().interpret_actor(&mut sibling_export);
                let target_origin = target.recipient().interpret(&mut target_export);
                let sibling_origin = sibling.recipient().interpret(&mut sibling_export);
                let installed = target_export.installed.expect("real target installation");
                let sibling_installed =
                    sibling_export.installed.expect("real sibling installation");
                let target = target.recipient();
                let sibling = sibling.recipient();
                let stop = CounterStopAuthority::Child(installed);
                let sibling_stop = CounterStopAuthority::Child(sibling_installed);
                let (binding, deadline) =
                    authority.issue().expect("actual captured child assignment");
                counter_control(&format!(
                    "BOUND {} {}",
                    binding.runtime_generation, binding.actor_generation
                ));
                let records = counter_native_records(
                    binding,
                    authority.grant_issuer,
                    authority.authority_generation,
                    profile,
                );
                let sibling_requests = interface.external::<CounterRequests>().unwrap();
                let sibling_values = interface.external::<CounterValue>().unwrap();
                let service = exchange(
                    authority,
                    binding,
                    deadline,
                    records,
                    target,
                    target_origin,
                    &stop,
                    sibling,
                    sibling_origin,
                    &sibling_stop,
                    interface,
                    sibling_requests,
                    sibling_values,
                    requests,
                )
                .await;
                let child_retirements = tokio::join!(stop.retirement(), sibling_stop.retirement());
                let stopped = lifecycle.request_shutdown();
                let released = permit.send(());
                (
                    service,
                    exported,
                    children,
                    stopped,
                    released,
                    exports,
                    child_retirements,
                )
            };
            let ((), work) = tokio::join!(execution, work);
            let native = receiving.await;
            let (service, exported, children, stopped, released, exports, child_retirements) = work;
            let (
                ApplicationOutcome::Completed {
                    output: (publication, permission),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
            ) = native
            else {
                panic!("all original parent Work/native/two-notification products");
            };
            let ActorRetirement::Completed {
                descendants,
                child_failures,
                completion,
                capability_failures,
                retirement_failures,
                ..
            } = &retirement
            else {
                panic!("original parent retirement");
            };
            assert!(
                publication.is_ok()
                    && permission.is_ok()
                    && exported.is_ok()
                    && children.is_some()
                    && stopped.is_ok()
                    && released.is_ok()
            );
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            assert_eq!(*completion, Completion::Stopped);
            assert!(
                child_failures.0.is_empty()
                    && capability_failures.is_empty()
                    && retirement_failures.is_empty()
            );
            assert_eq!(descendants.len(), 2);
            for child in descendants {
                let CounterChildrenTerminal::Child { terminal, .. } = child else {
                    panic!("original child terminal");
                };
                let ActorRetirement::Completed {
                    completion,
                    capability_failures,
                    retirement_failures,
                    ..
                } = terminal
                else {
                    panic!("child complete retirement");
                };
                assert_eq!(*completion, Completion::Stopped);
                assert!(capability_failures.is_empty() && retirement_failures.is_empty());
            }
            assert_eq!(
                child_retirements.0.retirement(),
                RetirementAssessment::Established
            );
            assert_eq!(
                child_retirements.1.retirement(),
                RetirementAssessment::Established
            );
            drop((retirement, children, exports));
            service
        }
        RemoteCounterRole::Recipient | RemoteCounterRole::Caller => {
            let execute_counter = || {
                let (published, ports) = oneshot::channel();
                let (permit, permission) = oneshot::channel();
                let checked = authority.check_host();
                checked.expect("actual local Counter capture host");
                let (execution, receiving) = Application::new(Counter::new().stop_on_shutdown())
                    .execute_with::<_, _, Never, (), _, _>(|application| async move {
                        let interface = application.interface(());
                        let publication = published.send((
                            application.lifecycle(),
                            application.root().address(),
                            application.root().established_recipient(),
                            interface.clone(),
                            interface.external::<CounterRequests>().unwrap(),
                        ));
                        let permission = permission.await;
                        (publication, permission)
                    })
                    .unwrap_or_else(|original| {
                        drop(original);
                        panic!("actual entered Counter Application");
                    });
                (execution, receiving, ports, permit)
            };
            let (execution, receiving, ports, permit) = execute_counter();
            let (sibling_execution, sibling_receiving, sibling_ports, sibling_permit) =
                execute_counter();
            let work = async {
                let (actual, actual_sibling) = tokio::join!(ports, sibling_ports);
                let (lifecycle, target_origin, target, interface, requests) =
                    actual.expect("root publication");
                let (
                    sibling_lifecycle,
                    sibling_origin,
                    sibling,
                    sibling_interface,
                    sibling_requests,
                ) = actual_sibling.expect("sibling publication");
                // Each exact root has a separate address space; keep its real sibling ports.
                let sibling_values = sibling_interface.external::<CounterValue>().unwrap();
                let (binding, deadline) = match role {
                    RemoteCounterRole::Recipient => {
                        let issued = authority.issue().expect("actual captured root assignment");
                        counter_control(&format!(
                            "BOUND {} {}",
                            issued.0.runtime_generation, issued.0.actor_generation
                        ));
                        issued
                    }
                    RemoteCounterRole::Caller => configured.expect("trusted remote projection"),
                    _ => unreachable!("root composition roles"),
                };
                let records = counter_native_records(
                    binding,
                    authority.grant_issuer,
                    authority.authority_generation,
                    profile,
                );
                let stop = CounterStopAuthority::Root(lifecycle);
                let sibling_stop = CounterStopAuthority::Root(sibling_lifecycle);
                let service = exchange(
                    authority,
                    binding,
                    deadline,
                    records,
                    target,
                    target_origin,
                    &stop,
                    sibling,
                    sibling_origin,
                    &sibling_stop,
                    interface,
                    sibling_requests,
                    sibling_values,
                    requests,
                )
                .await;
                let released = permit.send(());
                let sibling_released = sibling_permit.send(());
                (service, released, sibling_released)
            };
            let ((), (), work) = tokio::join!(execution, sibling_execution, work);
            let (native, sibling_native) = tokio::join!(receiving, sibling_receiving);
            let (service, released, sibling_released) = work;
            for actual in [native, sibling_native] {
                let (
                    ApplicationOutcome::Completed {
                        output: (publication, permission),
                        cleanup: Ok(()),
                    },
                    Ok((origin, retirement)),
                    Ok(ActorNotificationReceipts {
                        termination: Ok(()),
                        retirement: Ok(()),
                    }),
                ) = actual
                else {
                    panic!("all original root Work/native/two-notification products");
                };
                let _retired = completed_actor(origin, retirement);
                assert!(publication.is_ok() && permission.is_ok());
            }
            assert!(released.is_ok() && sibling_released.is_ok());
            service
        }
        RemoteCounterRole::Router => unreachable!("router owns no Application"),
    };
    let closed = session.close().await;
    let (
        binding,
        target_origin,
        sibling_origin,
        records,
        authorization,
        native_records,
        declarations,
        request_reads,
        query_releases,
        reservations,
        outbound_records,
        outbound_receipts,
        emissions,
        invocations,
        verified,
        completions,
        acknowledgements,
        reply_authorities,
        requester_terminations,
        requester_receipts,
        local_requester_native,
        local_receipts,
        processing_reply,
        processing_challenges,
        read_challenges,
        reports,
        admissions,
        read_ports,
        joined_report,
        original_target_report,
        target_after_refusal,
        stop_grant,
        pending,
        read_sent,
        read,
        sibling_increment,
        sibling_read,
        sibling_value,
        undeclared,
        stopped,
        sibling_stopped,
        query_dispositions,
        reply_dispositions,
    ) = service;
    counter_control("NATIVE_ACQUIRED");
    let mut retired_requesters = Vec::new();
    for (ordinal, sequence, native, original) in requester_receipts {
        let (
            ApplicationOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = native
        else {
            panic!("all original Requester Work/native/two-notification products");
        };
        let requester = completed_actor(origin, retirement).into_inner();
        retired_requesters.push((
            ordinal,
            sequence,
            requester,
            publication,
            permission,
            original,
        ));
    }
    let grant_expectation = match env::var("BOMBAY_COUNTER_GRANT_EXPECTATION").as_deref() {
        Ok("accepted") => CounterProtectedMeaning::StopAccepted,
        Ok("refused") => CounterProtectedMeaning::Refused(CounterRefusal::Grant),
        _ => panic!("explicit trusted campaign expectation"),
    };
    for (ordinal, sequence, requester, _, _, _) in &retired_requesters {
        let CounterRequesterState::Protected {
            correlation,
            scope,
            receipts,
        } = &requester.state
        else {
            panic!("actual typed Requester");
        };
        assert_eq!(*correlation, *ordinal as u64);
        assert_eq!(*scope, records[*sequence].scope);
        let mut expected = match sequence {
            0 => vec![
                CounterProtectedMeaning::IncrementAdmitted,
                CounterProtectedMeaning::IncrementProcessed,
            ],
            1 => vec![
                CounterProtectedMeaning::ReadAdmitted,
                CounterProtectedMeaning::ReadReply(1),
            ],
            2 => vec![CounterProtectedMeaning::Refused(CounterRefusal::Grant)],
            3 => match grant_expectation {
                CounterProtectedMeaning::StopAccepted => vec![
                    CounterProtectedMeaning::StopAccepted,
                    CounterProtectedMeaning::RetirementReport {
                        retirement: RetirementAssessment::Established,
                        failures: ActorFailureAssessment::NoFailuresFound,
                    },
                ],
                _ => vec![CounterProtectedMeaning::Refused(CounterRefusal::Grant)],
            },
            _ => unreachable!("finite command fixture"),
        };
        match (challenged, ordinal, sequence) {
            (Some(CounterProofPurpose::IncrementProcessed), 0, 0) => {
                assert!(
                    !receipts.contains(&CounterProtectedMeaning::IncrementProcessed),
                    "must not protect an alien native processing receipt"
                );
                expected.retain(|fact| *fact != CounterProtectedMeaning::IncrementProcessed);
            }
            (Some(CounterProofPurpose::ReadReply), _, 1) => {
                assert!(
                    !receipts.contains(&CounterProtectedMeaning::ReadReply(1)),
                    "must not protect an alien native Read receipt"
                );
                expected.retain(|fact| *fact != CounterProtectedMeaning::ReadReply(1));
            }
            _ => (),
        }
        for fact in &expected {
            assert!(
                receipts.contains(fact),
                "native-closed query lost required protected Read/Stop fact: {fact:?}"
            );
        }
        assert_eq!(receipts.len(), expected.len());
    }
    if matches!(
        role,
        RemoteCounterRole::Recipient | RemoteCounterRole::ChildRecipient
    ) && let Some(report) = joined_report
    {
        assert_eq!(
            original_target_report, joined_report,
            "protected Stop must retire the independently captured original target before Work finishes"
        );
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    }
    // Cardinalities and positive controls follow the independent typed membership oracles.
    assert!(closed.is_ok() && invocations.iter().all(Result::is_ok));
    assert!(undeclared.as_ref().is_none_or(Result::is_ok));
    assert!(sibling_increment.is_ok() && sibling_read.is_ok() && sibling_stopped.is_ok());
    let sibling_value = sibling_value.expect("independent sibling progress");
    assert_eq!(sibling_value.from, sibling_origin);
    assert_eq!(
        sibling_value.message,
        match (role, challenged) {
            (
                RemoteCounterRole::ChildRecipient,
                Some(CounterProofPurpose::IncrementProcessed | CounterProofPurpose::ReadReply),
            ) => 2,
            _ => 1,
        }
    );
    if role == RemoteCounterRole::Caller {
        assert!(matches!(pending, Some(None)));
        assert!(
            authorization.is_none(),
            "caller cannot assign a remote Binding to its local target"
        );
        assert!(stopped.is_ok() && read_sent.is_ok());
        let read = read.expect("caller actual local count");
        assert_eq!(read.from, target_origin);
        assert_eq!(read.message, 2);
        let expected = match (grant_expectation, challenged) {
            (CounterProtectedMeaning::StopAccepted, None) => 11,
            (CounterProtectedMeaning::StopAccepted, Some(_)) => 10,
            (_, None) => 9,
            (_, Some(_)) => 8,
        };
        assert_eq!(native_records.len(), expected);
        assert_eq!(verified.len(), expected);
        assert_eq!(completions.len(), expected);
        assert_eq!(acknowledgements.len(), expected);
        assert_eq!(retired_requesters.len(), 6);
    } else {
        let processing_reply = processing_reply.expect("actual processing receipt");
        assert_eq!(processing_reply.from, target_origin);
        assert_eq!(processing_reply.message.request_sequence, 0);
        for (sent, alien) in &processing_challenges {
            assert!(sent.is_ok());
            let alien = alien.as_ref().expect("actual alien processing receive");
            assert_eq!(alien.from, sibling_origin);
            assert_ne!(alien.from, processing_reply.from);
            assert_eq!(
                alien.message.request_sequence,
                processing_reply.message.request_sequence
            );
        }
        for (incremented, sent, alien) in &read_challenges {
            assert!(incremented.is_ok() && sent.is_ok());
            let alien = alien.as_ref().expect("actual alien Read receive");
            let original = read_ports[2]
                .1
                .as_ref()
                .expect("original acquired target Read");
            assert_eq!(alien.from, sibling_origin);
            assert_ne!(alien.from, original.from);
            assert_eq!(alien.message, original.message);
        }
        assert!(
            query_releases
                .iter()
                .all(|r| matches!((&r.1, r.0.as_str()), (Ok(_), "release-query\n")))
        );
        let Some((sent, post_refusal)) = target_after_refusal else {
            panic!("actual post-refusal Read");
        };
        assert!(sent.is_ok());
        let post_refusal = post_refusal.expect("target useful after send-only refusal");
        assert_eq!(post_refusal.from, target_origin);
        assert_eq!(post_refusal.message, 1);
        for (_, reply) in &read_ports {
            if let Some(reply) = reply {
                assert_eq!(reply.from, target_origin);
            }
        }
        assert!(outbound_records.iter().all(Result::is_ok));
        assert!(
            outbound_receipts
                .iter()
                .all(|(_, result)| result.as_ref().is_some_and(Result::is_ok))
        );
        assert!(
            reports
                .iter()
                .any(|r| r.status == CounterStatus::Refused(CounterRefusal::Grant))
        );
        let authorization = authorization.as_ref().expect("actual recipient authority");
        assert_eq!(reservations.len(), 6);
        assert!(reservations.iter().all(Result::is_ok));
        assert_eq!(admissions.len(), 6);
        assert_eq!(read_ports.len(), 6);
        if grant_expectation == CounterProtectedMeaning::StopAccepted {
            assert!(matches!(stop_grant.as_ref(), Some((line, Ok(_))) if line == "grant-stop\n"));
            assert_eq!(stopped, Err(ShutdownRejection::AlreadyStopped));
            assert_eq!(authorization.retained.len(), 3);
            assert!(
                matches!(
                    admissions[5].as_ref().map(|p| &p.verification),
                    Some(Verification::Duplicate { .. })
                ),
                "duplicate Stop reuses existing native outcome"
            );
            assert_eq!(reports[4].status, CounterStatus::StopAccepted);
            assert_eq!(reports[5].status, CounterStatus::StopAccepted);
        } else {
            assert!(
                stop_grant
                    .as_ref()
                    .is_some_and(|r| !matches!((&r.1, r.0.as_str()), (Ok(_), "grant-stop\n")))
            );
            assert!(stopped.is_ok() && read_sent.is_ok());
            let read = read.expect("ungranted original target remains useful");
            assert_eq!(read.from, target_origin);
            assert_eq!(read.message, 1);
            assert_eq!(
                reports[4].status,
                CounterStatus::Refused(CounterRefusal::Grant)
            );
            assert_eq!(
                reports[5].status,
                CounterStatus::Refused(CounterRefusal::Grant)
            );
            assert_eq!(authorization.retained.len(), 2);
        }
        assert!(emissions.iter().all(Result::is_ok));
    }
    for (_, _, _, publication, permission, (begun, triggered, stopped, released)) in
        &retired_requesters
    {
        assert!(
            publication.is_ok()
                && permission.is_ok()
                && begun.is_ok()
                && stopped.is_ok()
                && released.is_ok()
        );
        assert!(matches!(
            triggered.as_ref().map(|r| &r.message),
            Some(CounterServiceInput::Verify(_))
        ));
    }
    assert!(
        verified.iter().all(Result::is_ok)
            && completions
                .iter()
                .all(|(current, sent)| current.is_ok() && sent.is_ok())
    );
    assert!(declarations.iter().all(Result::is_ok) && reply_authorities.iter().all(Result::is_ok));
    assert_eq!(requester_terminations.len(), 0);
    for acknowledged in &acknowledgements {
        assert!(
            matches!(acknowledged, Some(Some(received)) if matches!(received.message, CounterServiceInput::ProtectedConsumed { .. }))
        );
    }
    for failure in native_records
        .iter()
        .filter_map(|(_, copied)| copied.as_ref().and_then(|read| read.as_ref().err()))
        .chain(
            request_reads
                .iter()
                .filter_map(|copied| copied.as_ref().and_then(|read| read.as_ref().err())),
        )
    {
        assert!(failure.protected_read.is_err() || failure.proof_read.is_err());
        assert!(failure.protected.len() <= 8192 && failure.proof.len() <= 64);
    }
    if let Some(local) = local_requester_native {
        let (
            ApplicationOutcome::Completed {
                output: (publication, permission),
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
        ) = local
        else {
            panic!("independent local requester native and notifications")
        };
        let local = completed_actor(origin, retirement).into_inner();
        let CounterRequesterState::Complete {
            observation: Some(2),
            ..
        } = local.state
        else {
            panic!("actual local Actions Read2")
        };
        assert!(publication.is_ok() && permission.is_ok());
        let Some((
            (),
            (
                first,
                second,
                begun,
                trigger,
                completed,
                consumed,
                read,
                observed,
                acknowledged,
                stopped,
                permitted,
            ),
        )) = local_receipts
        else {
            panic!("all actual local requester receipts")
        };
        assert!(
            first.is_ok()
                && second.is_ok()
                && begun.is_ok()
                && completed.as_ref().is_some_and(Result::is_ok)
                && observed.as_ref().is_some_and(Result::is_ok)
                && stopped.is_ok()
                && permitted.is_ok()
        );
        let read = read.expect("native local Read2");
        assert_eq!(read.message, 2);
        assert!(matches!(
            trigger.as_ref().map(|r| &r.message),
            Some(CounterServiceInput::Verify(9))
        ));
        assert!(matches!(
            acknowledged.as_ref().map(|r| &r.message),
            Some(CounterServiceInput::ReadObserved {
                correlation: 9,
                value: 2
            })
        ));
        assert!(matches!(
            consumed.as_ref().map(|r| &r.message),
            Some(CounterServiceInput::ReportConsumed(9))
        ));
    }

    assert_eq!(binding, records[0].scope.identity.binding);
    assert_eq!(query_dispositions.full.load(Ordering::Relaxed), 0);
    assert_eq!(query_dispositions.closed.load(Ordering::Relaxed), 0);
    assert_eq!(reply_dispositions.full.load(Ordering::Relaxed), 0);
    assert_eq!(reply_dispositions.closed.load(Ordering::Relaxed), 0);
    counter_control("FINISHED");
}

fn counter_native_records(
    binding: Binding,
    issuer: &'static str,
    generation: u64,
    profile: CounterProofProfile,
) -> Vec<ProtectedRecord> {
    let mut records = configured_protected_counter(binding, issuer, generation, profile);
    let refused = counter_record(
        records[3].scope,
        profile,
        CounterProtectedMeaning::Refused(CounterRefusal::Grant),
        &[43],
        &mut [0; 8192],
    )
    .expect("independently registered alternate Stop refusal");
    records.push(refused);
    records
}

use super::stop_admission::{
    CounterChildren, CounterChildrenTerminal, CounterExports, CounterParent, CounterStopExport,
};
use core::pin::pin;

#[test]
fn child_stop_keeps_original_target_and_sibling_for_recorded_proof() {
    counter_child_stop(
        CounterProofProfile::RecordedTest,
        &record_provider::verify,
        record_provider::projection,
    );
}
#[test]
fn child_stop_keeps_original_target_and_sibling_for_scheduled_proof() {
    counter_child_stop(
        CounterProofProfile::ScheduledTest,
        &scheduled_provider::verify,
        scheduled_provider::projection,
    );
}
#[expect(
    clippy::too_many_lines,
    reason = "Acquire all actual child, parent, verification and notification originals before exact-target and sibling assertions."
)]
fn counter_child_stop<R>(
    profile: CounterProofProfile,
    provider: &impl AsyncFn(
        &CounterRequest,
        &[ProtectedRecord],
        AuthorityFreshness,
        ProviderAvailability,
        CounterProofProfile,
        CounterProofPurpose,
        &str,
        &str,
        &protected::Limits,
    ) -> Result<R, VerificationError>,
    projection: fn(&R) -> (&Scope, CounterProofProfile, CounterProofPurpose),
) {
    let host = Builder::new_current_thread().enable_all().build().unwrap();
    let acquired = host.block_on(async {
        let mut authority = AssignmentAuthority {
            host: &host,
            runtime_generation: 1,
            grant_issuer: "grant-controller",
            authority_generation: 7,
            permitted_operations: &[CounterOperation::Increment, CounterOperation::Read],
            next: Some(0),
        };
        let (published, ports) = oneshot::channel();
        let (permit, permission) = oneshot::channel();
        let checked = authority.check_host();
        checked.expect("actual parent Application capture host");
        let (execution, receiving) = Application::new(CounterParent::default().stop_on_shutdown())
            .execute_with::<_, _, CounterChildrenTerminal, _, _, _>(|application| async move {
                let interface = application.interface(());
                let publication = published.send((
                    application.lifecycle(),
                    application.root().established_recipient(),
                    interface.external::<CounterExports>().unwrap(),
                    interface.external::<CounterRequests>().unwrap(),
                    interface.external::<CounterValue>().unwrap(),
                ));
                let continued = permission.await;
                (publication, continued)
            })
            .unwrap_or_else(|original| {
                drop(original);
                panic!("actual parent construction");
            });
        let service = async {
            let (parent_lifecycle, parent, mut exports, requests, mut values) =
                ports.await.unwrap();
            let exported = exports.send(&parent, exports.recipient()).await;
            let children = exports.receive().await;
            let CounterChildren { target, sibling } = &children.as_ref().unwrap().message;
            let mut target_export = CounterStopExport::default();
            let mut sibling_export = CounterStopExport::default();
            target.clone().interpret_actor(&mut target_export);
            sibling.clone().interpret_actor(&mut sibling_export);
            let target = target.recipient();
            let sibling = sibling.recipient();
            let installed = target_export.installed.unwrap();
            let sibling_installed = sibling_export.installed.unwrap();
            let mut original_retirement = pin!(installed.retirement());
            let issued = authority
                .issue()
                .expect("captured child actual-host assignment");
            let (binding, deadline) = issued;
            let configured = configured_protected_counter(
                binding,
                authority.grant_issuer,
                authority.authority_generation,
                profile,
            );
            let mut authorization = CounterAuthorization::new(
                StartupMode::ExplicitTest,
                Some((binding, deadline, target.clone())),
                authority.grant_issuer,
                authority.authority_generation,
                authority.permitted_operations,
                projection,
                profile,
                CounterLimits {
                    incoming: NonZeroUsize::new(2).unwrap(),
                    retained: NonZeroUsize::new(2).unwrap(),
                    individual: NonZeroUsize::new(131_072).unwrap(),
                    total: NonZeroUsize::new(262_144).unwrap(),
                    verification_maximum: 65_536,
                },
            )
            .unwrap();
            authorization.stop = Some(CounterStopAuthority::Child(installed.clone()));
            let incremented = values.send(&target, CounterMessage::Increment).await;
            let read = values
                .send(&target, CounterMessage::Read(values.recipient()))
                .await;
            let target_before = values.receive().await;
            let mut reports = Vec::new();
            let mut originals = Vec::new();
            let mut target_after_refusal = None;
            for record in &configured[2..4] {
                let original = counter_request(record);
                let pointers = (original.protected.as_ptr(), original.proof.as_ptr());
                let correlation = authorization.reserve(original).unwrap_or_else(|original| {
                    drop(original);
                    panic!("bounded Stop reservation");
                });
                let position = authorization
                    .pending
                    .iter()
                    .position(|pending| pending.correlation == correlation)
                    .unwrap();
                acquire_verification(&mut authorization.pending[position], async |original| {
                    provider(
                        original,
                        &configured,
                        AuthorityFreshness::Current,
                        ProviderAvailability::Available,
                        profile,
                        CounterProofPurpose::Request,
                        binding.caller,
                        binding.host,
                        &counter_test_json_limits(),
                    )
                    .await
                })
                .await;
                authorization.associate(position, &requests, &values.recipient());
                authorization.attempt(correlation, &requests);
                reports.push(authorization.report(correlation));
                originals.push((authorization.consume(correlation), pointers));
                if record.scope.identity.sequence == 2 {
                    let read = values
                        .send(&target, CounterMessage::Read(values.recipient()))
                        .await;
                    let reply = values.receive().await;
                    target_after_refusal = Some((read, reply));
                    authorization.permitted_operations = &[CounterOperation::Stop];
                }
            }
            let report = authorization.stop.as_ref().unwrap().retirement().await;
            let original_target_report = original_retirement.as_mut().now_or_never();
            let sibling_increment = values.send(&sibling, CounterMessage::Increment).await;
            let sibling_read = values
                .send(&sibling, CounterMessage::Read(values.recipient()))
                .await;
            let sibling_reply = match &sibling_read {
                Ok(()) => values.receive().await,
                Err(_) => None,
            };
            let remaining_target_stop = installed.request_shutdown(Ingress::<_, Here>::new());
            let remaining_sibling_stop =
                sibling_installed.request_shutdown(Ingress::<_, Here>::new());
            let parent_stop = parent_lifecycle.request_shutdown();
            let released = permit.send(());
            (
                exported,
                children,
                authorization,
                issued,
                incremented,
                read,
                target_before,
                reports,
                originals,
                target_after_refusal,
                report,
                original_target_report,
                sibling_increment,
                sibling_read,
                sibling_reply,
                remaining_target_stop,
                remaining_sibling_stop,
                parent_stop,
                released,
                requests,
                exports,
                values,
            )
        };
        let (execution, observations) =
            tokio::join!(AssertUnwindSafe(execution).catch_unwind(), service);
        let native = receiving.await;
        (execution, native, observations)
    });
    let (execution, native, observations) = acquired;
    let (
        exported,
        children,
        authorization,
        issued,
        incremented,
        read,
        target_before,
        reports,
        originals,
        target_after_refusal,
        report,
        original_target_report,
        sibling_increment,
        sibling_read,
        sibling_reply,
        remaining_target_stop,
        remaining_sibling_stop,
        parent_stop,
        released,
        requests,
        exports,
        values,
    ) = observations;
    let (
        ApplicationOutcome::Completed {
            output: (publication, continued),
            cleanup: Ok(()),
        },
        Ok((origin, parent_terminal)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = native
    else {
        panic!("all original parent Work/native/two-notification products");
    };
    assert!(execution.is_ok() && publication.is_ok() && continued.is_ok());
    assert_eq!(
        original_target_report,
        Some(report),
        "service Stop must retire the independently captured original child"
    );
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
    assert!(exported.is_ok() && children.is_some() && incremented.is_ok() && read.is_ok());
    let target_before = target_before.unwrap();
    assert_eq!(target_before.message, 1);
    let Some((Ok(()), Some(target_after_refusal))) = target_after_refusal else {
        panic!("send-only refusal keeps the original child useful");
    };
    assert_eq!(
        (target_after_refusal.from, target_after_refusal.message),
        (target_before.from, 1)
    );
    assert!(matches!(
        reports.as_slice(),
        [
            Some(CounterReport {
                status: CounterStatus::Refused(CounterRefusal::Grant),
                ..
            }),
            Some(CounterReport {
                status: CounterStatus::StopAccepted,
                ..
            })
        ]
    ));
    assert!(sibling_increment.is_ok() && sibling_read.is_ok());
    let sibling_reply = sibling_reply.unwrap();
    assert_eq!(sibling_reply.message, 1);
    assert_ne!(sibling_reply.from, target_before.from);
    assert_eq!(
        remaining_target_stop,
        Err(ShutdownRejection::AlreadyStopped)
    );
    assert_eq!(remaining_sibling_stop, Ok(()));
    assert_eq!(parent_stop, Ok(()));
    assert!(released.is_ok());
    let ActorRetirement::Completed {
        descendants,
        child_failures,
        completion,
        capability_failures,
        retirement_failures,
        ..
    } = &parent_terminal
    else {
        panic!("original completed creator outcome");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(
        child_failures.0.is_empty()
            && capability_failures.is_empty()
            && retirement_failures.is_empty()
    );
    assert_eq!(*completion, Completion::Stopped);
    let mut addresses = Vec::new();
    for child in descendants {
        let CounterChildrenTerminal::Child { origin, terminal } = child else {
            panic!("original child terminal projection");
        };
        let ActorRetirement::Completed {
            completion,
            capability_failures,
            retirement_failures,
            ..
        } = terminal
        else {
            panic!("original completed child");
        };
        assert_eq!(*completion, Completion::Stopped);
        assert!(capability_failures.is_empty() && retirement_failures.is_empty());
        addresses.push(origin.address());
    }
    assert_eq!(addresses.len(), 2);
    assert!(addresses.contains(&target_before.from) && addresses.contains(&sibling_reply.from));
    for (original, pointers) in &originals {
        let original = original.as_ref().unwrap();
        assert_eq!(
            (
                original.original.protected.as_ptr(),
                original.original.proof.as_ptr()
            ),
            *pointers
        );
    }
    drop((
        authorization,
        issued,
        parent_terminal,
        originals,
        requests,
        exports,
        values,
    ));
}
