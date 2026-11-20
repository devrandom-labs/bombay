//! Native retirement and connection changes conserve actor work and original results.
use super::counter::{CounterError, CounterIncremented, CounterProcessed, CounterValue};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationScope};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::proxy::{ProxyControl, ProxyDiagnostic, ProxyOutcome, StableProxy};
use crate::remote::{
    Attempt, CredentialSource, Export, ExportLimits, Network, NetworkLayout, RefusalRetention,
    reply, stop, submission,
};
use crate::worker::{StartupPlan, WorkerStartup};
use crate::{
    ActorFailureAssessment, ActorRetirement, ActorRetirementReport, Completion, MailAddr,
    RetirementAssessment, RetirementOutcome, TrySendError,
};
use behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Births, ChildChoice,
    ChildCreationOutcome, ChildHead, ChildInput, ChildOccurrence, ChildReport, ChildRole,
    ChildTail, CreateChild, CreationEvent, CreationSequence, CreationSettlement,
    CreationSettlements, Creations, CreationsSettled, DeclaredChildOccurrence, EstablishedActor,
    EstablishedDelivery, EstablishedRecipient, EventIngress, Here, Ingress, InitializationTurn,
    Inside, ItemSettlement, MessageProtocol, Never, Protocol, RecoverEvent, SettledItem, Step,
    User, UserEvent,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested, StopOnShutdown};
use core::pin::pin;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::mem::take;
use std::num::NonZeroUsize;
use std::panic::panic_any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use zenoh::Session;
use zenoh::handlers::FifoChannel;
use zenoh::query::ConsolidationMode;
use zenoh::sample::{Locality, SampleKind};
use zenoh_config::WhatAmI;

#[path = "../../../../tests/application_clients/node_exports/actors.rs"]
#[expect(
    dead_code,
    reason = "reuse the pure Creator and topology without installing their separate Entity-family fixture"
)]
mod actors;
mod notification;
mod withdrawal;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum Command {
    Increment,
    Read,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum RetirementCommand {
    Stop,
}

#[derive(Debug)]
enum StopPublication<Reply> {
    BeforeSibling(Reply),
    AfterSibling(Reply),
}

#[derive(Debug, PartialEq, Eq)]
struct StartupCounterValue {
    value: u64,
    initializations: u64,
}

type StartupCounterValues = MessageProtocol<MailAddr, StartupCounterValue>;
type StartupCounterStarted = MessageProtocol<MailAddr, u64>;

enum StartupCounterMessage {
    Increment {
        request_sequence: u64,
        reply_to: EstablishedRecipient<CounterProcessed>,
    },
    Read(EstablishedRecipient<StartupCounterValues>),
}

struct StartupCounter {
    value: u64,
    initializations: u64,
    started: EstablishedRecipient<StartupCounterStarted>,
}

#[bombay::actor(
    sends = {
        startups: Vec<EstablishedDelivery<StartupCounterStarted>>,
        values: Vec<EstablishedDelivery<StartupCounterValues>>,
        processed: Vec<EstablishedDelivery<CounterProcessed>>,
    },
    error = CounterError,
)]
impl StartupCounter {
    fn init(&mut self) -> BehaviorActed<Self> {
        self.initializations = self
            .initializations
            .checked_add(1)
            .ok_or(CounterError::Overflow)?;
        Ok(Actions::cont().send_startups(EstablishedDelivery::new(
            self.started.clone(),
            self.initializations,
        )))
    }

    fn receive(&mut self, message: StartupCounterMessage) -> BehaviorActed<Self> {
        match message {
            StartupCounterMessage::Increment {
                request_sequence,
                reply_to,
            } => {
                self.value = self.value.checked_add(1).ok_or(CounterError::Overflow)?;
                Ok(Actions::cont().send_processed(EstablishedDelivery::new(
                    reply_to,
                    CounterIncremented { request_sequence },
                )))
            }
            StartupCounterMessage::Read(reply_to) => {
                Ok(Actions::cont().send_values(EstablishedDelivery::new(
                    reply_to,
                    StartupCounterValue {
                        value: self.value,
                        initializations: self.initializations,
                    },
                )))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
enum ReconnectOperation {
    Increment,
}

const RECONNECT_COUNTER: &str = "bombay/reconnect/counter";

fn router_client(address: &str, role: &EndpointRole) -> Network {
    let mut network = configuration(address, role);
    let (NetworkLayout::Peer {
        connect: Some(connect),
        listen: None,
    }
    | NetworkLayout::Peer {
        connect: None,
        listen: Some(connect),
    }) = network.layout
    else {
        panic!("one explicit TLS direction supplies this client's identity");
    };
    network.layout = NetworkLayout::RouterClient { connect };
    network
}

async fn reconnect_router(address: &str) -> (Session, String) {
    let mut network = configuration(address, &EndpointRole::Receiver);
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let NetworkLayout::Peer {
        listen: Some(endpoint),
        ..
    } = &mut network.layout
    else {
        panic!("the independent Router owns one TLS listener");
    };
    endpoint.certificate = CredentialSource::File(certificates.join("router-certificate.pem"));
    endpoint.private_key = CredentialSource::File(certificates.join("router-private-key.pem"));
    let mut configuration = network.to_configuration().unwrap();
    configuration.set_mode(Some(WhatAmI::Router)).unwrap();
    let router = zenoh::open(configuration)
        .await
        .expect("the independent TLS Router opens");
    let [locator]: [_; 1] = router
        .info()
        .locators()
        .await
        .try_into()
        .expect("the actual Router owns exactly one TLS listener");
    (router, locator.to_string())
}

#[test]
#[expect(
    clippy::large_futures,
    reason = "this finite trace retains its measured approximately 31 KiB concrete future on the test host stack"
)]
fn automatic_router_reconnect_preserves_initialization_and_acknowledged_replay() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the real reconnect host opens");
    host.block_on(async {
        tokio::time::timeout(Duration::from_secs(40), reconnect_counter())
            .await
            .expect("a watchdog supplies no reconnect, initialization or replay evidence");
    });
}

#[expect(
    clippy::too_many_lines,
    clippy::result_large_err,
    clippy::await_holding_refcell_ref,
    clippy::large_futures,
    reason = "one finite trace retains the measured 20728-byte Node future, original requests, typed results and native cleanup on the test stack; its proof table stays immutable during verification"
)]
async fn reconnect_counter() {
    let (router, address) = reconnect_router("tls/localhost:0").await;
    let old_router = router.zid().to_string();
    let caller = zenoh::open(
        router_client(&address, &EndpointRole::Caller)
            .to_configuration()
            .unwrap(),
    )
    .await
    .expect("the one caller Session connects to the controlled Router");
    let caller_id = caller.zid().to_string();
    let scope = ApplicationScope::new();
    let mut startups = scope.external::<StartupCounterStarted>().unwrap();
    let node = scope
        .with_root(
            StartupCounter {
                value: 0,
                initializations: 0,
                started: startups.recipient(),
            }
            .stop_on_shutdown(),
        )
        .open_node(router_client(&address, &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the one receiving Node connects to the Router"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let events = session
        .info()
        .transport_events_listener()
        .history(true)
        .with(FifoChannel::new(8))
        .await
        .expect("the receiver observes its actual transports");
    let caller_events = caller
        .info()
        .transport_events_listener()
        .history(true)
        .with(FifoChannel::new(8))
        .await
        .expect("the caller observes its actual transports");
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let claims = RefCell::new(None);
    let prepared = RefCell::new(None);
    let first_startup = RefCell::new(None);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "reconnect-counter",
        host: "receiver",
        protocol: "reconnect-counter",
        target_assignment: "one-live-root",
        logical_session: 503,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let current =
        |receipt: &recorded::Receipt<ReconnectOperation>, runtime: &str, target: MailAddr| {
            let claims = claims.borrow();
            let actual = claims.as_ref().ok_or(VerificationError::Invalid)?;
            current_scope(
                recorded::projection(receipt),
                actual,
                &ReconnectOperation::Increment,
                Purpose::Request,
                deadline,
            )?;
            if actual.runtime_generation.as_ref() != runtime
                || actual.actor_generation != target.0
                || recorded::identity(receipt).0 != &binding
            {
                return Err(VerificationError::Invalid);
            }
            Ok(claims)
        };
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        RECONNECT_COUNTER,
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
            refusals: RefusalRetention::Retain,
        },
        limits(),
        |bytes: &[u8]| serde_json::from_slice::<ReadRequest<ReconnectOperation>>(bytes),
        reply(
            |request: ReadRequest<ReconnectOperation>| {
                let ReconnectOperation::Increment = request.operation;
                Ok::<_, ReadRequest<ReconnectOperation>>(move |reply_to| {
                    StartupCounterMessage::Increment {
                        request_sequence: request.bombay.request_sequence,
                        reply_to,
                    }
                })
            },
            |receipt: &recorded::Receipt<ReconnectOperation>,
             original: &User<MailAddr, CounterIncremented>| {
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::Processed,
                    operation: &ReconnectOperation::Increment,
                    value: &original.message.request_sequence,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<ReconnectOperation>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records.borrow(),
                binding,
            )
            .await
        },
        async |bytes: &[u8], _: &recorded::Receipt<ReconnectOperation>| {
            let proof: Box<[u8]> = [251, 253].into();
            protected_replies.borrow_mut().push(ProtectedMessage {
                protected: bytes.into(),
                proof: proof.clone(),
            });
            Ok::<_, Never>(proof)
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        &current,
        &current,
    );
    let mut node = node.exports(export, async |application| {
        let started = startups
            .receive()
            .await
            .expect("the actual initialization Action arrives");
        *first_startup.borrow_mut() = Some(started);
        let target = application.root().established_recipient();
        let mut actual = metadata(
            ProviderProfile::Recorded,
            "reconnect-counter",
            caller_id.clone(),
            runtime.clone(),
        );
        actual.actor_generation = application.actor_address(&target).unwrap().0;
        let original = ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &ReconnectOperation::Increment,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([241_u8, 247]),
        };
        records.borrow_mut().push(ProtectedMessage {
            protected: original.protected.clone(),
            proof: original.proof.clone(),
        });
        *prepared.borrow_mut() = Some(original);
        *claims.borrow_mut() = Some(actual);
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let caller = &caller;
            let router = &router;
            let session = &session;
            let address = &address;
            let prepared = &prepared;
            async move {
                let original = prepared.borrow_mut().take().unwrap();
                let initial = (
                    events.recv_async().await.unwrap(),
                    caller_events.recv_async().await.unwrap(),
                );
                let links_before = (
                    session.info().links().await.collect::<Vec<_>>(),
                    caller.info().links().await.collect::<Vec<_>>(),
                );
                let querier = caller.declare_querier(RECONNECT_COUNTER).await.unwrap();
                let matching = querier
                    .matching_listener()
                    .with(FifoChannel::new(8))
                    .await
                    .unwrap();
                let first_matching = loop {
                    let status = matching.recv_async().await.unwrap();
                    if status.matching() {
                        break status;
                    }
                };
                let matching_closed = matching.undeclare().await;
                let querier_closed = querier.undeclare().await;
                let receiving = caller
                    .get(RECONNECT_COUNTER)
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(30))
                    .await
                    .expect("the first authorized native Query starts");
                let first = receiving.recv_async().await;
                let first_end = receiving.recv_async().await;
                let mut values = application.client::<StartupCounterValues>().unwrap();
                let read_before = values
                    .send(StartupCounterMessage::Read(values.recipient()))
                    .await;
                let before = values.receive().await;
                let router_closed = router.close().await;
                let deleted = (
                    events.recv_async().await.unwrap(),
                    caller_events.recv_async().await.unwrap(),
                );
                let links_down = (
                    session.info().links().await.collect::<Vec<_>>(),
                    caller.info().links().await.collect::<Vec<_>>(),
                );
                let live_before_reconnect = (
                    session.is_closed(),
                    caller.is_closed(),
                    session.zid().to_string(),
                    caller.zid().to_string(),
                );
                let mut processed = application.client::<CounterProcessed>().unwrap();
                let local_increment = processed
                    .send(StartupCounterMessage::Increment {
                        request_sequence: 509,
                        reply_to: processed.recipient(),
                    })
                    .await;
                let local_processed = processed.receive().await;
                let read_down = values
                    .send(StartupCounterMessage::Read(values.recipient()))
                    .await;
                let down = values.receive().await;
                let (replacement, rebound) = reconnect_router(address).await;
                let new_router = replacement.zid().to_string();
                let restored = (
                    events.recv_async().await.unwrap(),
                    caller_events.recv_async().await.unwrap(),
                );
                let links_after = (
                    session.info().links().await.collect::<Vec<_>>(),
                    caller.info().links().await.collect::<Vec<_>>(),
                );
                let querier = caller.declare_querier(RECONNECT_COUNTER).await.unwrap();
                let matching = querier
                    .matching_listener()
                    .with(FifoChannel::new(8))
                    .await
                    .unwrap();
                let last_matching = loop {
                    let status = matching.recv_async().await.unwrap();
                    if status.matching() {
                        break status;
                    }
                };
                let last_matching_closed = matching.undeclare().await;
                let last_querier_closed = querier.undeclare().await;
                let replay = caller
                    .get(RECONNECT_COUNTER)
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(30))
                    .await
                    .expect(
                        "the acknowledged exact identity is explicitly replayed after reconnect",
                    );
                let second = replay.recv_async().await;
                let second_end = replay.recv_async().await;
                let read_after = values
                    .send(StartupCounterMessage::Read(values.recipient()))
                    .await;
                let after = values.receive().await;
                let live_after = (
                    session.is_closed(),
                    caller.is_closed(),
                    session.zid().to_string(),
                    caller.zid().to_string(),
                );
                let events_closed = events.undeclare().await;
                let caller_events_closed = caller_events.undeclare().await;
                let caller_closed = caller.close().await;
                let shutdown = application.shutdown::<Here>();
                let retired = application.lifecycle().retirement().await;
                (
                    original,
                    (
                        initial,
                        deleted,
                        restored,
                        links_before,
                        links_down,
                        links_after,
                        old_router,
                        new_router,
                        rebound,
                        live_before_reconnect,
                        live_after,
                    ),
                    (
                        first_matching,
                        matching_closed,
                        querier_closed,
                        last_matching,
                        last_matching_closed,
                        last_querier_closed,
                    ),
                    (first, first_end, second, second_end),
                    (
                        read_before,
                        before,
                        local_increment,
                        local_processed,
                        read_down,
                        down,
                        read_after,
                        after,
                    ),
                    (
                        router_closed,
                        caller_closed,
                        events_closed,
                        caller_events_closed,
                        shutdown,
                        retired,
                    ),
                    replacement,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the one Node returns its actual joined result"));
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the one Node retains all native result fields"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((original, connections, matching, replies, observations, closure, replacement), cleanup) =
        work.try_into_completed().unwrap_or_else(|_| {
            panic!("all reconnect observations survive actual Application cleanup")
        });
    let replacement_closed = replacement.close().await;
    let mut notices = vec![first_startup.into_inner().unwrap()];
    startups.close_admission();
    while let Some(original) = startups.receive().await {
        notices.push(original);
    }
    let (origin, native) = native.expect("the original native root remains acquired");
    let (
        initial,
        deleted,
        restored,
        links_before,
        links_down,
        links_after,
        old_router,
        new_router,
        rebound,
        live_down,
        live_after,
    ) = connections;
    let (
        first_matching,
        matching_closed,
        querier_closed,
        last_matching,
        last_matching_closed,
        last_querier_closed,
    ) = matching;
    let (first, first_end, second, second_end) = replies;
    let (read_before, before, local_increment, local_processed, read_down, down, read_after, after) =
        observations;
    let (router_closed, caller_closed, events_closed, caller_events_closed, shutdown, retired) =
        closure;
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    let [first_request, second_request] = <[_; 2]>::try_from(requests)
        .unwrap_or_else(|_| panic!("both original protected requests remain retained"));
    let first_request = first_request
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the first request producer ended"));
    let second_request = second_request
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the replay producer ended"));
    cleanup.expect("the actual Application cleanup joins");
    closed.expect("the receiving Session closes once");
    router_closed.expect("the original Router closes normally");
    caller_closed.expect("the same caller Session closes after replay");
    replacement_closed.expect("the replacement Router closes after the receiving Node");
    events_closed.expect("the native receiver listener is undeclared");
    caller_events_closed.expect("the native caller listener is undeclared");
    matching_closed.expect("the first matching listener is undeclared");
    querier_closed.expect("the first querier is undeclared");
    last_matching_closed.expect("the restored matching listener is undeclared");
    last_querier_closed.expect("the restored querier is undeclared");
    assert!(first_matching.matching() && last_matching.matching());
    assert!(
        read_before.is_ok()
            && read_down.is_ok()
            && read_after.is_ok()
            && local_increment.is_ok()
            && shutdown.is_ok()
    );
    let before = before.expect("the independent initial Read returns");
    let down = down.expect("the actor progresses while both transport clients are disconnected");
    let after = after.expect("the independent final Read returns");
    let sequence = claims.borrow().as_ref().unwrap().request_sequence;
    let admitted = [&first_request.4, &second_request.4].map(|row| {
        row.as_ref().and_then(
            |(_, _, admission, received, _)| match (admission, received) {
                (Attempt::Returned(Ok(())), Some(Some(original))) => {
                    Some((original.from, original.message.request_sequence))
                }
                _ => None,
            },
        )
    });
    let expected_startups = [User::new(origin.address(), 1)];
    // All real business and initialization observations precede tag/count oracles.
    assert_eq!(
        (
            admitted,
            [
                (&before.message, before.from),
                (&down.message, down.from),
                (&after.message, after.from)
            ],
            notices.as_slice(),
        ),
        (
            [Some((origin.address(), sequence)), None],
            [
                (
                    &StartupCounterValue {
                        value: 1,
                        initializations: 1
                    },
                    origin.address()
                ),
                (
                    &StartupCounterValue {
                        value: 2,
                        initializations: 1
                    },
                    origin.address()
                ),
                (
                    &StartupCounterValue {
                        value: 2,
                        initializations: 1
                    },
                    origin.address()
                ),
            ],
            expected_startups.as_slice(),
        ),
    );
    let local_processed =
        local_processed.expect("the actual disconnected local operation is processed");
    assert_eq!(
        local_processed,
        User::new(
            origin.address(),
            CounterIncremented {
                request_sequence: 509
            }
        )
    );
    for event in [&initial.0, &initial.1] {
        assert_eq!(event.kind(), SampleKind::Put);
        assert_eq!(event.transport().zid().to_string(), old_router);
    }
    for event in [&deleted.0, &deleted.1] {
        assert_eq!(event.kind(), SampleKind::Delete);
        assert_eq!(event.transport().zid().to_string(), old_router);
    }
    for event in [&restored.0, &restored.1] {
        assert_eq!(event.kind(), SampleKind::Put);
        assert_eq!(event.transport().zid().to_string(), new_router);
    }
    assert_ne!(old_router, new_router);
    assert_eq!(rebound, address);
    for (links, expected_router) in [
        (&links_before.0, &old_router),
        (&links_before.1, &old_router),
        (&links_after.0, &new_router),
        (&links_after.1, &new_router),
    ] {
        let [link] = links.as_slice() else {
            panic!("each Client owns one actual TLS Router link");
        };
        assert_eq!(link.zid().to_string(), *expected_router);
        assert!(
            link.src().to_string().starts_with("tls/")
                && link.dst().to_string().starts_with("tls/")
        );
    }
    assert!(links_down.0.is_empty() && links_down.1.is_empty());
    assert_eq!(
        live_down,
        (false, false, runtime.clone(), caller_id.clone())
    );
    assert_eq!(live_after, (false, false, runtime, caller_id));
    assert_eq!(retired.retirement(), RetirementAssessment::Established);
    assert_eq!(retired.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(report, Some(retired));
    let notifications = notifications.expect("both actor notification receipts remain");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(
        matches!(bound, Some(Ok(_)))
            && matches!(startup, Some(Ok(())))
            && matches!(withdrawal, Some(Ok(())))
    );
    assert!(diagnostics.is_empty() && polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none() && session.is_closed());
    let first = first.expect("the first processed operation has a real reply");
    let second = second.expect("the acknowledged replay has a real cached reply");
    let mut protected = Vec::new();
    for reply in [&first, &second] {
        let native = reply.result();
        let sample = native
            .as_ref()
            .expect("the actual native reply is successful");
        protected.push(ProtectedMessage {
            protected: sample.payload().to_bytes().as_ref().into(),
            proof: sample
                .attachment()
                .expect("the actual protected reply has its proof")
                .to_bytes()
                .as_ref()
                .into(),
        });
    }
    assert_eq!(
        (&protected[0].protected, &protected[0].proof),
        (&protected[1].protected, &protected[1].proof)
    );
    for reply in &protected {
        let verified =
            recorded::verify::<ReconnectOperation>(reply, &protected_replies.borrow(), binding)
                .await
                .expect("both actual replies verify against the original provider publication");
        let value: ReadReply<u64, ReconnectOperation> =
            serde_json::from_slice(&reply.protected).unwrap();
        assert_eq!(value.bombay, recorded::projection(&verified).bombay);
        assert_eq!(value.kind, Purpose::Processed);
        assert_eq!(value.operation, ReconnectOperation::Increment);
        assert_eq!(value.value, value.bombay.request_sequence);
    }
    assert!(first_end.is_err() && second_end.is_err());
    assert_eq!(first_request.0, second_request.0);
    assert_eq!(first_request.1, second_request.1);
    assert_eq!(
        (first_request.1.0.as_ref(), first_request.1.1.as_ref()),
        (original.protected.as_ref(), original.proof.as_ref())
    );
    assert!(matches!(first_request.2, Some(Ok(_))) && matches!(second_request.2, Some(Ok(_))));
    let (constructor, client, admission, received, tail) = first_request
        .4
        .expect("the original native operation is retained");
    assert!(
        constructor.is_none()
            && matches!(client, Attempt::Returned(Ok(_)))
            && matches!(admission, Attempt::Returned(Ok(())))
    );
    let received = received
        .expect("the original native receive completed")
        .expect("the original processing User remains");
    let actual = claims.into_inner().unwrap();
    assert_eq!(
        received,
        User::new(
            origin.address(),
            CounterIncremented {
                request_sequence: actual.request_sequence
            }
        )
    );
    assert!(matches!(tail.as_slice(), [None]));
    assert!(
        second_request.3.is_none()
            && second_request.4.is_none()
            && second_request.5.is_none()
            && second_request.6.is_none()
    );
    assert_eq!((first_request.7.len(), second_request.7.len()), (1, 1));
    assert!(
        first_request.7.iter().all(Result::is_ok) && second_request.7.iter().all(Result::is_ok)
    );
    assert!(
        matches!(first_request.8, (None, None, None))
            && matches!(second_request.8, (None, None, None))
    );
    assert!(
        matches!(first_request.9, (None, None, None, None, None, None))
            && matches!(second_request.9, (None, None, None, None, None, None))
    );
    let ActorRetirement::Retained {
        custody,
        outcome: RetirementOutcome::Completed { .. },
    } = native
    else {
        panic!("the one actual counter completes its original native retirement");
    };
    assert_eq!(
        (
            custody.behavior.base().value,
            custody.behavior.base().initializations
        ),
        (2, 1)
    );
    assert!(
        custody.child_failures.is_empty()
            && custody.descendants.is_empty()
            && custody.capability_failures.is_empty()
    );
    assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
}

#[derive(Clone, Copy)]
enum StartupCompletion {
    Ready,
    Panicked,
}

struct HeldCounterStartup {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    resource: OwnedSemaphorePermit,
    readiness: Box<[u8]>,
    completion: StartupCompletion,
}

impl StartupPlan for HeldCounterStartup {
    type Ready = Box<[u8]>;
    type Rejection = oneshot::error::RecvError;

    async fn start(self) -> Result<Self::Ready, Self::Rejection> {
        let published = self.reached.send(());
        published.expect("the binding observes this actual target-owned startup");
        let released = self.release.await;
        drop(self.resource);
        released?;
        match self.completion {
            StartupCompletion::Ready => Ok(self.readiness),
            StartupCompletion::Panicked => panic_any(self.readiness),
        }
    }
}

type CounterProxy = StableProxy<Counter, HeldCounterStartup>;
type CounterTarget = StopOnShutdown<CounterProxy>;
type CounterSibling = StopOnShutdown<Counter>;
type CounterFamily = ChildChoice<CounterTarget, ChildChoice<CounterSibling, Never>>;
type CounterCreations = <Births<CounterFamily> as CreationSettlements<MailAddr>>::Settlements;
type CounterExports = MessageProtocol<MailAddr, CounterTargets>;

struct CounterTargets {
    proxy: EstablishedActor<CounterTarget>,
    sibling: EstablishedActor<CounterSibling>,
}

struct ProxyChild;
impl ChildRole<CounterOwner> for ProxyChild {
    type Child = CounterTarget;
    type Position = ChildHead;
}
impl ChildOccurrence<CounterOwner> for ProxyChild {
    type Resolution = DeclaredChildOccurrence;
}

struct SiblingChild;
impl ChildRole<CounterOwner> for SiblingChild {
    type Child = CounterSibling;
    type Position = ChildTail<ChildHead>;
}
impl ChildOccurrence<CounterOwner> for SiblingChild {
    type Resolution = DeclaredChildOccurrence;
}

#[derive(behavior_macros::SendProduct)]
struct CounterOwnerSends<Starts, Exports> {
    starts: Starts,
    exports: Exports,
}

enum CounterOwnerCommand {
    Exports(EstablishedRecipient<CounterExports>),
    Start(HeldCounterStartup),
}

enum CounterOwnerEvent {
    Children(CreationEvent<MailAddr, CounterFamily, CounterOwnerCommand>),
    Outcome(ChildReport<ProxyOutcome<Counter, HeldCounterStartup>>),
    Diagnostic(ChildReport<ProxyDiagnostic<Counter, HeldCounterStartup>>),
}

impl UserEvent for CounterOwnerEvent {
    type Addr = MailAddr;
    type Message = CounterOwnerCommand;

    fn user(from: MailAddr, message: Self::Message) -> Self {
        Self::Children(CreationEvent::User(User::new(from, message)))
    }

    fn into_user(self) -> Result<User<MailAddr, Self::Message>, Self> {
        match self {
            Self::Children(CreationEvent::User(original)) => Ok(original),
            other => Err(other),
        }
    }
}

impl EventIngress<Births<CounterFamily>, CreationsSettled<MailAddr, CounterFamily>>
    for CounterOwnerEvent
{
    fn ingress(original: CreationsSettled<MailAddr, CounterFamily>) -> Self {
        Self::Children(CreationEvent::Settlements(original))
    }
}

impl EventIngress<Births<CounterFamily>, ChildReport<ProxyOutcome<Counter, HeldCounterStartup>>>
    for CounterOwnerEvent
{
    fn ingress(original: ChildReport<ProxyOutcome<Counter, HeldCounterStartup>>) -> Self {
        Self::Outcome(original)
    }
}

impl EventIngress<Births<CounterFamily>, ChildReport<ProxyDiagnostic<Counter, HeldCounterStartup>>>
    for CounterOwnerEvent
{
    fn ingress(original: ChildReport<ProxyDiagnostic<Counter, HeldCounterStartup>>) -> Self {
        Self::Diagnostic(original)
    }
}

struct CounterOwner {
    creations: Vec<CounterCreations>,
    outcomes: Vec<ChildReport<ProxyOutcome<Counter, HeldCounterStartup>>>,
    diagnostics: Vec<ChildReport<ProxyDiagnostic<Counter, HeldCounterStartup>>>,
}

impl Protocol for CounterOwner {
    type Addr = MailAddr;
    type Msg = CounterOwnerCommand;
}

impl BehaviorBase for CounterOwner {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for CounterOwner {
    type Protocol = Self;
    type Event = CounterOwnerEvent;
    type Sends = CounterOwnerSends<
        Vec<
            ChildInput<
                CounterTarget,
                CounterProxy,
                ProxyControl<Counter, HeldCounterStartup>,
                ProxyChild,
            >,
        >,
        Vec<EstablishedDelivery<CounterExports>>,
    >;
    type Birth = Births<CounterFamily>;
    type Ph = Never;
    type Error = Never;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let proxy = sequence.issue().expect("two actual child creations fit");
        let sibling = sequence.issue().expect("two actual child creations fit");
        Ok(Actions::new(
            CounterOwnerSends {
                starts: Vec::new(),
                exports: Vec::new(),
            },
            Creations::one(CreateChild::birth(
                proxy,
                ChildChoice::Head(CounterProxy::new().stop_on_shutdown()),
            ))
            .and(CreateChild::birth(
                sibling,
                ChildChoice::Tail(ChildChoice::Head(Counter::new().stop_on_shutdown())),
            )),
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            CounterOwnerEvent::Children(CreationEvent::Settlements(settled)) => {
                let original = settled.into_settlement();
                self.creations.push(original);
                Ok(Actions::cont())
            }
            CounterOwnerEvent::Children(CreationEvent::User(User {
                message: CounterOwnerCommand::Exports(recipient),
                ..
            })) => {
                let mut proxy = None;
                let mut sibling = None;
                for original in &self.creations {
                    if let CreationSettlement::Settled(children) = original {
                        for child in children {
                            match child {
                                SettledItem::Attempted(ItemSettlement::Accepted(
                                    ChildChoice::Head(ChildCreationOutcome::Established(created)),
                                )) => proxy = Some(created.actor()),
                                SettledItem::Attempted(ItemSettlement::Accepted(
                                    ChildChoice::Tail(ChildChoice::Head(
                                        ChildCreationOutcome::Established(created),
                                    )),
                                )) => sibling = Some(created.actor()),
                                _ => {}
                            }
                        }
                    }
                }
                let (Some(proxy), Some(sibling)) = (proxy, sibling) else {
                    return Ok(Actions::stop());
                };
                Ok(Actions::new(
                    CounterOwnerSends {
                        starts: Vec::new(),
                        exports: vec![EstablishedDelivery::new(
                            recipient,
                            CounterTargets { proxy, sibling },
                        )],
                    },
                    Creations::empty(),
                    Step::Continue,
                ))
            }
            CounterOwnerEvent::Children(CreationEvent::User(User {
                message: CounterOwnerCommand::Start(startup),
                ..
            })) => {
                let creation = self
                    .creations
                    .iter()
                    .find_map(|original| {
                        let CreationSettlement::Settled(children) = original else {
                            return None;
                        };
                        children.iter().find_map(|child| match child {
                            SettledItem::Attempted(ItemSettlement::Accepted(
                                ChildChoice::Head(ChildCreationOutcome::Established(created)),
                            )) => Some(created.id()),
                            _ => None,
                        })
                    })
                    .expect("the earlier genuine export supplied the committed proxy creation");
                Ok(Actions::new(
                    CounterOwnerSends {
                        starts: vec![ChildInput::after(
                            creation,
                            ProxyControl::start_with(Counter::new(), startup),
                        )],
                        exports: Vec::new(),
                    },
                    Creations::empty(),
                    Step::Continue,
                ))
            }
            CounterOwnerEvent::Outcome(original) => {
                self.outcomes.push(original);
                Ok(Actions::cont())
            }
            CounterOwnerEvent::Diagnostic(original) => {
                self.diagnostics.push(original);
                Ok(Actions::cont())
            }
        }
    }
}

crate::topology! {
    held_counter_startup {
        root: CounterOwner { Proxy(ProxyChild) => proxy; Counter(SiblingChild) => sibling; }
        proxy { Worker => worker; }
        worker {}
        sibling {}
    }
}

#[test]
#[expect(
    clippy::large_futures,
    reason = "the finite owned-startup test retains its concrete future on the test stack"
)]
fn remote_stop_waits_for_target_owned_startup_after_termination() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the actual remote retirement host opens");
    host.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(40),
            stop_with_held_startup(StartupCompletion::Ready),
        )
        .await
        .expect("the watchdog establishes no stop, join or resource evidence");
    });
}

#[test]
#[expect(
    clippy::large_futures,
    reason = "the finite owned-startup test retains its concrete future on the test stack"
)]
fn remote_stop_retains_joined_startup_failure_and_sibling_progress() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the actual failed-startup retirement host opens");
    host.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(40),
            stop_with_held_startup(StartupCompletion::Panicked),
        )
        .await
        .expect("the watchdog establishes no failure custody or retirement evidence");
    });
}

#[expect(
    clippy::result_large_err,
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one remote stop trace conserves original startup/native/notification custody, and the configured proof table is immutable during actual verification"
)]
async fn stop_with_held_startup(completion: StartupCompletion) {
    let resources = Arc::new(Semaphore::new(1));
    let resource = Arc::clone(&resources)
        .try_acquire_owned()
        .expect("the actual startup owns the sole resource");
    let readiness: Box<[u8]> = [211, 223].into();
    let readiness_allocation = readiness.as_ptr();
    let (reached, observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let startup = HeldCounterStartup {
        reached,
        release: released,
        resource,
        readiness,
        completion,
    };
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let claims = RefCell::new(None);
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "counter-proxy",
        host: "receiver",
        protocol: "counter-proxy",
        target_assignment: "root",
        logical_session: 487,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let current =
        |receipt: &recorded::Receipt<RetirementCommand>, runtime: &str, target: MailAddr| {
            let scope = recorded::projection(receipt);
            let actual = claims.borrow();
            let actual = actual
                .as_ref()
                .expect("the actual binding supplied its metadata");
            current_scope(
                scope,
                actual,
                &RetirementCommand::Stop,
                Purpose::Request,
                deadline,
            )?;
            if scope.bombay.runtime_generation.as_ref() == runtime
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
        "bombay/held-startup/stop",
        ExportLimits {
            queued_queries: maximum,
            retained_queries: maximum,
            retained_requests: maximum,
            diagnostics: maximum,
            refusals: RefusalRetention::Retain,
        },
        limits(),
        |bytes: &[u8]| {
            serde_json::from_slice::<ReadRequest<RetirementCommand>>(bytes)
                .map(|request| request.operation)
        },
        stop::<_, Here, _, _>(
            |RetirementCommand::Stop| Ok::<_, RetirementCommand>(()),
            |receipt: &recorded::Receipt<RetirementCommand>,
             attempted: &Attempt<Result<(), ShutdownRejection>>,
             retired: &Option<ActorRetirementReport>| {
                if !matches!(attempted, Attempt::Returned(Ok(())))
                    || !retired.as_ref().is_some_and(|report| {
                        report.retirement() == RetirementAssessment::Established
                    })
                {
                    return Err(VerificationError::Invalid);
                }
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::Stopped,
                    operation: &RetirementCommand::Stop,
                    value: &(),
                })
                .map(Vec::into_boxed_slice)
                .map_err(VerificationError::Declaration)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            recorded::verify::<RetirementCommand>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records.borrow(),
                binding,
            )
            .await
        },
        async |bytes: &[u8], receipt: &recorded::Receipt<RetirementCommand>| {
            let reply: ReadReply<(), RetirementCommand> =
                serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
            if reply.bombay != recorded::projection(receipt).bombay
                || reply.kind != Purpose::Stopped
                || reply.operation != RetirementCommand::Stop
            {
                return Err(VerificationError::Invalid);
            }
            let proof: Box<[u8]> = [227, 229].into();
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
    let node = Application::with_topology(
        CounterOwner {
            creations: Vec::new(),
            outcomes: Vec::new(),
            diagnostics: Vec::new(),
        }
        .stop_on_shutdown(),
        held_counter_startup::Topology::new(),
    )
    .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
    .await
    .unwrap_or_else(|_| panic!("the target owns its actual native Node"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let (configured, input) = oneshot::channel();
    let mut node = node.exports(export, async |application| {
        let mut receiver = application.client::<CounterExports>().unwrap();
        let requested = receiver
            .send(CounterOwnerCommand::Exports(receiver.recipient()))
            .await;
        requested.unwrap_or_else(|_| panic!("the real parent export request is admitted"));
        let targets = receiver
            .receive()
            .await
            .expect("the real parent exports both actual child capabilities")
            .message;
        let sender = application
            .sender()
            .expect("the outside binding starts the actual child through its parent");
        let started = sender.send(CounterOwnerCommand::Start(startup)).await;
        let reached = observed.await;
        let target = targets.proxy.clone();
        let mut actual = metadata(
            ProviderProfile::Recorded,
            "counter-proxy",
            runtime.clone(),
            runtime,
        );
        actual.actor_generation = application.actor_address(&target.recipient()).unwrap().0;
        let protected = serde_json::to_vec(&ReadRequestRef {
            bombay: &actual,
            kind: Purpose::Request,
            operation: &RetirementCommand::Stop,
        })
        .unwrap()
        .into_boxed_slice();
        let proof: Box<[u8]> = [233, 239].into();
        records.borrow_mut().push(ProtectedMessage {
            protected: protected.clone(),
            proof: proof.clone(),
        });
        *claims.borrow_mut() = Some(actual);
        let supplied = configured.send((
            started,
            reached,
            targets,
            ProtectedMessage { protected, proof },
        ));
        supplied.unwrap_or_else(|_| panic!("Work retains actual startup and request facts"));
        Ok::<_, VerificationError>(target)
    });
    let outcome = node
        .run_with(|application| {
            let session = &session;
            let resources = &resources;
            async move {
                let (started, reached, targets, original) = input.await.unwrap();
                let target = targets
                    .proxy
                    .clone()
                    .interpret_actor(&mut ExtractLocalEndpoint);
                let target_address = application
                    .actor_address(&targets.proxy.recipient())
                    .unwrap();
                let sibling_address = application
                    .actor_address(&targets.sibling.recipient())
                    .unwrap();
                let held_before = resources.available_permits();
                let receiving = session
                    .get("bombay/held-startup/stop")
                    .allowed_destination(Locality::SessionLocal)
                    .payload(original.protected.to_vec())
                    .attachment(original.proof.to_vec())
                    .consolidation(ConsolidationMode::None)
                    .timeout(Duration::from_secs(30))
                    .await
                    .expect("the actual native Stop query starts");
                let termination = target.recipient().termination().await;
                let before = target.retirement().now_or_never();
                let held_after_termination = resources.available_permits();
                let sender = application
                    .sender_to(targets.proxy.recipient())
                    .unwrap_or_else(|_| {
                        panic!("the closed exact child retains its sender contract")
                    });
                let closed = sender.try_send(CounterMessage::Increment);
                let repeated = target.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
                let (pending, sibling, after, held_after_progress, released, published) = {
                    let mut reply = pin!(receiving.recv_async());
                    let pending = reply
                        .as_mut()
                        .now_or_never()
                        .map(StopPublication::BeforeSibling);
                    let sibling = {
                        let mut processed = application
                            .client_to::<_, CounterProcessed>(targets.sibling.recipient())
                            .unwrap_or_else(|_| {
                                panic!("the actual same-parent sibling client allocates")
                            });
                        let increment = processed
                            .send(CounterMessage::IncrementAndReply {
                                request_sequence: 491,
                                reply_to: processed.recipient(),
                            })
                            .await;
                        let processing = processed.receive().await;
                        let mut values = application
                            .client_to::<_, CounterValue>(targets.sibling.recipient())
                            .unwrap_or_else(|_| {
                                panic!("the actual same-parent sibling Read client allocates")
                            });
                        let read = values.send(CounterMessage::Read(values.recipient())).await;
                        let value = values.receive().await;
                        (increment, processing, read, value)
                    };
                    let after = target.retirement().now_or_never();
                    let pending = pending.or_else(|| {
                        reply
                            .as_mut()
                            .now_or_never()
                            .map(StopPublication::AfterSibling)
                    });
                    let held_after_progress = resources.available_permits();
                    let released = release.send(());
                    let (pending, published) = match pending {
                        Some(original) => (Some(original), None),
                        None => (None, Some(reply.await)),
                    };
                    (
                        pending,
                        sibling,
                        after,
                        held_after_progress,
                        released,
                        published,
                    )
                };
                let ending = receiving.recv_async().await;
                let retired = target.retirement().await;
                let replayed = target.retirement().await;
                let permits = resources.available_permits();
                let shutdown = application.shutdown::<Here>();
                (
                    started,
                    reached,
                    original,
                    held_before,
                    termination,
                    before,
                    held_after_termination,
                    closed,
                    repeated,
                    pending,
                    sibling,
                    after,
                    held_after_progress,
                    released,
                    published,
                    ending,
                    retired,
                    replayed,
                    permits,
                    target_address,
                    sibling_address,
                    shutdown,
                )
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the real target Node and joined cleanup return"));
    let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("all original Node results remain acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let (
        (
            started,
            reached,
            original,
            held_before,
            termination,
            before,
            held_after_termination,
            refused,
            repeated,
            pending,
            sibling,
            after,
            held_after_progress,
            released,
            published,
            ending,
            retired,
            replayed,
            permits,
            target_address,
            sibling_address,
            shutdown,
        ),
        cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete Work observations survive cleanup"));
    cleanup.expect("the actual target Application cleanup joins");
    closed.expect("the actual target Node Session closes");
    let (origin, mut native) = native.expect("the original joined parent and both children remain");
    let report = report.expect("the parent root report remains independent");
    let notifications = notifications.expect("both actual parent notification receipts remain");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    let (increment, processing, read, value) = sibling;
    assert!(started.is_ok() && reached.is_ok() && released.is_ok());
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(
        (
            held_before,
            held_after_termination,
            held_after_progress,
            permits
        ),
        (0, 0, 0, 1)
    );
    assert!(
        before.is_none() && after.is_none(),
        "joined completion cannot precede owned startup settlement"
    );
    assert!(
        pending.is_none(),
        "the native Stop query cannot finish while the target-owned resource is held: {pending:?}"
    );
    assert!(matches!(
        refused,
        Err(TrySendError::Closed(CounterMessage::Increment))
    ));
    assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopped));
    assert_eq!(retired, replayed);
    assert_eq!(retired.retirement(), RetirementAssessment::Established);
    let failures = match completion {
        StartupCompletion::Ready => ActorFailureAssessment::NoFailuresFound,
        StartupCompletion::Panicked => ActorFailureAssessment::FailuresFound,
    };
    assert_eq!(retired.failures(), failures);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), failures);
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(matches!(bound, Some(Ok(_))) && matches!(startup, Some(Ok(()))));
    assert!(matches!(withdrawal, Some(Ok(()))) && diagnostics.is_empty() && session.is_closed());
    assert!(increment.is_ok() && read.is_ok() && shutdown.is_ok());
    let processing = processing.expect("the sibling's actual increment processing returns");
    assert_eq!(processing.from, sibling_address);
    assert_eq!(
        processing.message,
        CounterIncremented {
            request_sequence: 491
        }
    );
    let value = value.expect("the independent sibling Read returns");
    assert_eq!((value.from, value.message), (sibling_address, 1));
    let reply = published
        .expect("the native Stop result is acquired only after release")
        .expect("the authorized exact Stop publishes its actual joined result");
    let result = reply.result();
    let sample = result
        .as_ref()
        .expect("the actual Stop publication is successful");
    let reply = ProtectedMessage {
        protected: sample.payload().to_bytes().as_ref().into(),
        proof: sample
            .attachment()
            .expect("the actual reply proof remains")
            .to_bytes()
            .as_ref()
            .into(),
    };
    let reply_receipt =
        recorded::verify::<RetirementCommand>(&reply, &protected_replies.borrow(), binding)
            .await
            .expect(
                "the exact native reply bytes/proof verify against the actual provider publication",
            );
    assert_eq!(recorded::projection(&reply_receipt).kind, Purpose::Stopped);
    assert_eq!(
        recorded::projection(&reply_receipt).operation,
        RetirementCommand::Stop
    );
    assert!(ending.is_err());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Retained {
        custody,
        outcome: RetirementOutcome::Completed { .. },
    } = &mut native
    else {
        panic!("the complete stopped parent native owner survives");
    };
    assert!(custody.child_failures.is_empty() && custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty() && custody.retirement_failures.is_empty());
    assert_eq!(custody.behavior.base().creations.len(), 1);
    assert!(
        custody.behavior.base().outcomes.is_empty()
            && custody.behavior.base().diagnostics.is_empty()
    );
    assert_eq!(custody.descendants.len(), 2);
    let mut proxies = Vec::new();
    let mut siblings = Vec::new();
    for child in &mut custody.descendants {
        match child {
            held_counter_startup::RootRetirements::Proxy { origin, retirement } => {
                proxies.push((origin, retirement));
            }
            held_counter_startup::RootRetirements::Counter { origin, retirement } => {
                siblings.push((origin, retirement));
            }
        }
    }
    let [(proxy_origin, proxy)] = <[_; 1]>::try_from(proxies)
        .unwrap_or_else(|_| panic!("one exact stopped proxy is retained by the real parent"));
    let [(sibling_origin, sibling)] = <[_; 1]>::try_from(siblings)
        .unwrap_or_else(|_| panic!("one distinct actual sibling is retained by the same parent"));
    assert_eq!(proxy_origin.address(), target_address);
    assert_eq!(sibling_origin.address(), sibling_address);
    assert_ne!(target_address, sibling_address);
    assert_ne!(target_address, origin.address());
    assert!(matches!(
        sibling.as_ref(),
        ActorRetirement::Retained {
            outcome: RetirementOutcome::OwnerCancelled { .. } | RetirementOutcome::Completed { .. },
            ..
        }
    ));
    let sibling_custody = sibling
        .custody()
        .expect("the same-parent sibling's native original remains");
    assert!(sibling_custody.descendants.is_empty() && sibling_custody.child_failures.is_empty());
    assert!(
        sibling_custody.capability_failures.is_empty()
            && sibling_custody.additional_failures.is_empty()
    );
    assert!(sibling_custody.retirement_failures.is_empty());
    assert_eq!(proxy.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Retained {
        custody: proxy_custody,
        outcome: RetirementOutcome::Completed { .. },
    } = proxy.as_mut()
    else {
        panic!("the exact target proxy remains its completed original");
    };
    assert!(proxy_custody.child_failures.is_empty());
    match completion {
        StartupCompletion::Ready => assert_eq!(proxy_custody.capability_failures.len(), 0),
        StartupCompletion::Panicked => assert_eq!(proxy_custody.capability_failures.len(), 1),
    }
    assert!(
        proxy_custody.additional_failures.is_empty()
            && proxy_custody.retirement_failures.is_empty()
    );
    let [child] = proxy_custody.descendants.as_slice() else {
        panic!("the target retains its one actual worker");
    };
    assert_ne!(child.origin.address(), target_address);
    let ActorRetirement::Retained {
        custody: child_custody,
        outcome: RetirementOutcome::Completed { .. } | RetirementOutcome::OwnerCancelled { .. },
    } = child.retirement.as_ref()
    else {
        panic!("the exact Counter child completed normal owned retirement");
    };
    assert!(child_custody.descendants.is_empty() && child_custody.child_failures.is_empty());
    assert!(
        child_custody.capability_failures.is_empty()
            && child_custody.additional_failures.is_empty()
    );
    assert!(child_custody.retirement_failures.is_empty());
    let controls = take(&mut proxy_custody.control);
    let receipts: Vec<_> = controls
        .into_iter()
        .map(|event| {
            <<CounterTarget as Behavior>::Event as RecoverEvent<
                WorkerStartup<Counter, HeldCounterStartup>,
                Inside<Here>,
            >>::recover(event)
            .map(|startup| (startup.worker(), startup.into_ready()))
        })
        .collect();
    let readiness: Vec<_> = receipts
        .iter()
        .filter_map(|receipt| receipt.as_ref().ok())
        .filter_map(|(worker, ready)| ready.as_ref().ok().map(|ready| (worker, ready)))
        .collect();
    match completion {
        StartupCompletion::Ready => {
            let [readiness] = readiness.as_slice() else {
                panic!("one original ready result returns to its stopped owner");
            };
            let (worker, ready) = *readiness;
            assert_eq!(worker.creation(), child.origin.creation_id());
            assert_eq!(ready.as_ptr(), readiness_allocation);
            assert_eq!(ready.as_ref(), [211, 223]);
        }
        StartupCompletion::Panicked => {
            assert_eq!(readiness.len(), 0);
            let failure = proxy_custody
                .capability_failures
                .pop()
                .expect("the joined target retains its one real startup task failure");
            assert!(failure.is_panic());
            let original = failure
                .try_into_panic()
                .expect("the actual joined startup task panicked");
            let original = original
                .downcast::<Box<[u8]>>()
                .expect("the original startup readiness allocation is the panic cause");
            assert_eq!(original.as_ptr(), readiness_allocation);
            assert_eq!(original.as_ref().as_ref(), [211, 223]);
        }
    }
    let [request] = <[_; 1]>::try_from(requests)
        .unwrap_or_else(|_| panic!("one complete native Stop request remains"));
    let (
        key,
        raw,
        verified,
        decoded,
        operation,
        encoded,
        protected,
        publications,
        refusals,
        panics,
    ) = request
        .try_into_parts()
        .unwrap_or_else(|_| panic!("all actual request producers ended"));
    assert!(key.is_some() && decoded.is_none());
    assert_eq!(
        (raw.0.as_ref(), raw.1.as_ref()),
        (original.protected.as_ref(), original.proof.as_ref())
    );
    let receipt = verified.expect("the actual request provider ran").unwrap();
    assert_eq!(recorded::projection(&receipt).kind, Purpose::Request);
    assert_eq!(
        recorded::projection(&receipt).operation,
        RetirementCommand::Stop
    );
    let (admission, observed) = operation.expect("the actual Stop contract was selected");
    assert!(matches!(admission, Attempt::Returned(Ok(()))));
    assert_eq!(observed, Some(retired));
    let encoded = encoded.expect("the joined result was encoded").unwrap();
    let protected = protected
        .expect("the joined encoding was protected")
        .unwrap();
    assert_eq!(
        (encoded.as_ref(), protected.as_ref()),
        (reply.protected.as_ref(), reply.proof.as_ref())
    );
    assert_eq!(publications.len(), 1);
    assert!(publications.iter().all(Result::is_ok));
    assert!(matches!(refusals, (None, None, None)));
    assert!(matches!(panics, (None, None, None, None, None, None)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_target_replays_while_work_remains_live() {
    tokio::time::timeout(Duration::from_secs(40), replay_after_retirement())
        .await
        .expect("actual replay and joined cleanup finish within the watchdog");
}

#[expect(
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one causal retirement and replay trace retains all cleanup originals; the configured verification table stays immutable during its provider wait"
)]
async fn replay_after_retirement() {
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let maximum = NonZeroUsize::new(2).unwrap();
    let export = Export::new(
        "bombay/retired-replay",
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
            serde_json::from_slice::<ReadRequest<Command>>(bytes).map(|request| request.operation)
        },
        submission(
            |command| match command {
                Command::Increment => Ok(|| {
                    constructed.set(constructed.get() + 1);
                    CounterMessage::Increment
                }),
                command @ Command::Read => Err(command),
            },
            |_: &recorded::Receipt<Command>,
             admission: &Attempt<Result<(), TrySendError<CounterMessage>>>| {
                encoded.set(encoded.get() + 1);
                match admission {
                    Attempt::Returned(Ok(())) => Ok::<_, ()>(Box::<[u8]>::from([59_u8])),
                    _ => Err(()),
                }
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            let original = ProtectedMessage {
                protected: bytes.into(),
                proof: proof.into(),
            };
            recorded::verify::<Command>(
                &original,
                &records.borrow(),
                Binding {
                    deployment: "two-node-read-study",
                    caller: "caller",
                    controller: "owner",
                    actor: "counter",
                    host: "receiver",
                    protocol: "counter",
                    target_assignment: "root",
                    logical_session: 331,
                },
            )
            .await
        },
        async |_: &[u8], _: &recorded::Receipt<Command>| {
            protected.set(protected.get() + 1);
            Ok::<_, Never>(Box::<[u8]>::from([61_u8]))
        },
        |left, right| recorded::identity(left) == recorded::identity(right),
        |_: &recorded::Receipt<Command>, _: &str, _: MailAddr| Ok::<_, Never>(()),
        |_: &recorded::Receipt<Command>, _: &str, _: MailAddr| Ok::<_, Never>(()),
    );
    let node = Application::new(Counter::new().stop_on_shutdown())
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|_| panic!("the actual cold Node opens"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let mut node = node.exports(export, async |application| {
        let target = application.root().established_recipient();
        let mut claims = metadata(
            ProviderProfile::Recorded,
            "counter",
            runtime.clone(),
            runtime,
        );
        claims.actor_generation = application.actor_address(&target).unwrap().0;
        records.borrow_mut().push(ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &claims,
                kind: Purpose::Request,
                operation: Command::Increment,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([53_u8]),
        });
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let records = &records;
            let session = &session;
            async move {
                let original = {
                    let records = records.borrow();
                    let original = &records[0];
                    (original.protected.to_vec(), original.proof.to_vec())
                };
                let mut endings = Vec::new();
                let mut replies = Vec::new();
                let mut retirement = None;
                let mut shutdown = None;
                for _ in 0..2 {
                    let incoming = session
                        .get("bombay/retired-replay")
                        .allowed_destination(Locality::SessionLocal)
                        .payload(original.0.clone())
                        .attachment(original.1.clone())
                        .consolidation(ConsolidationMode::None)
                        .timeout(Duration::from_secs(5))
                        .await
                        .expect("the actual native query is issued");
                    let mut received = Vec::new();
                    let ending = loop {
                        match incoming.recv_async().await {
                            Ok(reply) => received.push(reply),
                            Err(ending) => break ending,
                        }
                    };
                    endings.push(ending);
                    replies.push(received);
                    if retirement.is_none() {
                        shutdown = Some(application.shutdown());
                        retirement = Some(application.lifecycle().retirement().await);
                    }
                }
                (replies, endings, shutdown, retirement)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual cold Node execution starts"));
    let (export, binding, close, polling, disposal, withdrawal_polling, withdrawal_disposal) = node
        .into_parts()
        .unwrap_or_else(|_| panic!("the actual Node finalization remains acquired"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((replies, endings, shutdown, retirement), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("Work completes after both real query streams"));
    cleanup.expect("actual Application cleanup joins");
    close.expect("actual Node Session close succeeds");
    let (origin, native) = native.expect("the complete original Counter remains acquired");
    let notifications = notifications.expect("both actual notifications are acquired");
    let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
    assert!(polling.is_none() && disposal.is_none());
    assert!(withdrawal_polling.is_none() && withdrawal_disposal.is_none());
    assert!(matches!(binding, Some(Ok(_))));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(diagnostics.is_empty());
    assert_eq!(endings.len(), 2);
    assert_eq!(shutdown, Some(Ok(())));
    let retirement =
        retirement.expect("the actual root retirement was observed before the duplicate");
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    assert_eq!(
        retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(report, Some(retirement));
    assert_eq!(native.completion(), Some(&Completion::Stopped));
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(native.custody().is_some());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert_eq!(replies.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
    for reply in replies.iter().flatten() {
        let received = reply.result();
        let sample = received
            .as_ref()
            .expect("the actual cached reply is successful");
        let payload = sample.payload().to_bytes();
        let attachment = sample
            .attachment()
            .expect("the actual provider attachment remains")
            .to_bytes();
        assert_eq!(payload.as_ref(), [59]);
        assert_eq!(attachment.as_ref(), [61]);
    }
    assert_eq!(
        (
            decoded.get(),
            constructed.get(),
            encoded.get(),
            protected.get()
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(requests.len(), 2);
    let [first, second] = <[_; 2]>::try_from(requests)
        .unwrap_or_else(|_| panic!("two actual request originals remain retained"));
    let first = first
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the canonical request releases its producer loans"));
    let second = second
        .try_into_parts()
        .unwrap_or_else(|_| panic!("the duplicate request releases its producer loans"));
    assert!(first.0.is_some());
    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert!(matches!(first.2, Some(Ok(_))) && matches!(second.2, Some(Ok(_))));
    let (_, _, admitted) = first.4.expect("the first native attempt is retained");
    assert!(matches!(admitted, Attempt::Returned(Ok(()))));
    assert!(second.4.is_none() && second.5.is_none() && second.6.is_none());
    assert!(first.7.iter().all(Result::is_ok) && second.7.iter().all(Result::is_ok));
    assert_eq!((first.7.len(), second.7.len()), (1, 1));
    assert!(matches!(first.8, (None, None, None)) && matches!(second.8, (None, None, None)));
    assert!(matches!(first.9, (None, None, None, None, None, None)));
    assert!(matches!(second.9, (None, None, None, None, None, None)));
}
