//! Native retirement and connection changes conserve actor work and original results.
use super::super::Binding as NodeBinding;
use super::counter::{CounterError, CounterIncremented, CounterProcessed, CounterValue};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::{Application, ApplicationScope, MailboxCapacity, WorkOutcome};
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
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::Poll;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::mem::take;
use std::num::NonZeroUsize;
use std::panic::panic_any;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
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

#[test]
fn shutdown_conserves_saturated_counter_admissions_and_the_pending_original() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("the supported Node host has a single actor worker")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(15), saturated_counter_shutdown())
                .await
                .expect("the watchdog supplies no shutdown or delivery evidence");
        });
}

#[expect(
    clippy::too_many_lines,
    reason = "one causal witness retains original sends, reply Users, unread commands and separate cleanup results"
)]
async fn saturated_counter_shutdown() {
    let scope = ApplicationScope::new()
        .mailbox_capacity(MailboxCapacity::new(2).expect("two actual actor mailbox slots"));
    let mut startups = scope.external::<StartupCounterStarted>().unwrap();
    let started = startups.recipient();
    let mut node = scope
        .with_root(
            StartupCounter {
                value: 0,
                initializations: 0,
                started: started.clone(),
            }
            .stop_on_shutdown(),
        )
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|(_, _, cause)| panic!("the actual Node opens: {cause:?}"));
    let session = node.session.clone();
    let outcome = node
        .run_with(|application| async move {
            let startup = startups.receive().await;
            let first = application.client::<CounterProcessed>().unwrap();
            let second = application.client::<CounterProcessed>().unwrap();
            let third = application.client::<CounterProcessed>().unwrap();
            let issued = [
                (601, first.address(), first.recipient()),
                (607, second.address(), second.recipient()),
                (613, third.address(), third.recipient()),
            ];
            let [first_command, second_command, third_command] =
                issued.each_ref().map(|(request_sequence, _, reply_to)| {
                    StartupCounterMessage::Increment {
                        request_sequence: *request_sequence,
                        reply_to: reply_to.clone(),
                    }
                });
            let (release_worker, worker_release) = mpsc::channel();
            let (announce_worker, worker_entered) = oneshot::channel();
            let worker = tokio::spawn(async move {
                let announced = announce_worker.send(());
                let release = worker_release.recv_timeout(Duration::from_secs(5));
                (announced, release)
            });
            let entered = worker_entered.await;
            // Work runs on the block_on thread. The sole actor worker cannot
            // drain a message until Work releases it, including during shutdown.
            let first_admission = first.try_send(first_command);
            let second_admission = second.try_send(second_command);
            let (
                before_release,
                after_release,
                shutdown,
                repeated,
                early_retirement,
                released,
                worker,
            ) = {
                let mut sending = pin!(third.send(third_command));
                let before_release =
                    poll_fn(|context| Poll::Ready(sending.as_mut().poll(context))).await;
                let shutdown = application.shutdown::<Here>();
                let repeated = application.shutdown::<Here>();
                let early_retirement = application.retirement().now_or_never();
                let released = release_worker.send(());
                let worker = worker.await;
                let after_release = match &before_release {
                    Poll::Pending => Some(sending.await),
                    Poll::Ready(_) => None,
                };
                (
                    before_release,
                    after_release,
                    shutdown,
                    repeated,
                    early_retirement,
                    released,
                    worker,
                )
            };
            let termination = application.termination().await;
            let retirement = application.retirement().await;
            let mut replies = Vec::new();
            for mut client in [first, second, third] {
                client.close_admission();
                let mut users = Vec::new();
                let eof = loop {
                    match client.receive().await {
                        Some(original) => users.push(original),
                        original @ None => break original,
                    }
                };
                replies.push((client.address(), client.recipient(), users, eof));
            }
            startups.close_admission();
            let mut startup_tail = Vec::new();
            let startup_eof = loop {
                match startups.receive().await {
                    Some(original) => startup_tail.push(original),
                    original @ None => break original,
                }
            };
            (
                (startup, startup_tail, startup_eof),
                issued,
                (entered, released, worker),
                (
                    first_admission,
                    second_admission,
                    before_release,
                    after_release,
                ),
                (
                    shutdown,
                    repeated,
                    early_retirement,
                    termination,
                    retirement,
                ),
                replies,
            )
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Node and Application run"));
    let closed = node.close().await;
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let ((startups, issued, worker, admissions, lifecycle, replies), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the complete Work observations survive cleanup"));
    let (origin, native) = native.expect("the original joined root returns");
    let notifications = notifications.expect("both original notification receipts return");
    let ActorRetirement::Retained {
        custody,
        outcome:
            RetirementOutcome::Completed {
                completion: Completion::Stopped,
                ..
            },
    } = native
    else {
        panic!("the completed retirement retains original counter and unread mailbox");
    };

    cleanup.expect("Application cleanup joins");
    closed.expect("the real Node Session closes");
    assert!(session.is_closed());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    let (entered, released, worker) = worker;
    let (announced, worker_release) = worker.expect("the actual actor worker hold joins");
    assert!(entered.is_ok() && announced.is_ok() && released.is_ok() && worker_release.is_ok());
    let (shutdown, repeated, early_retirement, termination, retirement) = lifecycle;
    assert_eq!(shutdown, Ok(()));
    assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
    assert!(early_retirement.is_none());
    assert_eq!(termination, Ok(Exit::Normal));
    assert_eq!(report, Some(retirement));
    assert_eq!(retirement.retirement(), RetirementAssessment::Established);
    assert_eq!(
        retirement.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(
        startups,
        (Some(User::new(origin.address(), 1)), Vec::new(), None)
    );

    let (first_admission, second_admission, before_release, after_release) = admissions;
    assert!(first_admission.is_ok() && second_admission.is_ok());
    let pending = matches!(&before_release, Poll::Pending);
    let ((Poll::Pending, Some(third)) | (Poll::Ready(third), None)) =
        (before_release, after_release)
    else {
        panic!("the ordinary send returns exactly once");
    };
    let third_admissions = match third {
        Ok(()) => 1,
        Err(refused) => {
            let StartupCounterMessage::Increment {
                request_sequence,
                reply_to,
            } = refused.into_message()
            else {
                panic!("the original refused command remains an Increment");
            };
            assert_eq!((request_sequence, &reply_to), (issued[2].0, &issued[2].2));
            0
        }
    };
    let mut accounted = [0; 3];
    let mut processed = 0;
    assert_eq!(replies.len(), issued.len());
    for (index, (address, reply_to, users, eof)) in replies.into_iter().enumerate() {
        let (request_sequence, expected_address, expected_reply) = &issued[index];
        assert_eq!((&address, &reply_to), (expected_address, expected_reply));
        assert!(eof.is_none());
        for original in users {
            assert_eq!(
                original,
                User::new(
                    origin.address(),
                    CounterIncremented {
                        request_sequence: *request_sequence,
                    },
                ),
            );
            accounted[index] += 1;
            processed += 1;
        }
    }
    for original in custody.user {
        let StartupCounterMessage::Increment {
            request_sequence,
            reply_to,
        } = original.message
        else {
            panic!("each unread original is an issued Increment");
        };
        let index = issued
            .iter()
            .position(|(expected_sequence, sender, expected_reply)| {
                (*expected_sequence, *sender, expected_reply)
                    == (request_sequence, original.from, &reply_to)
            })
            .expect("unread custody preserves the complete original User");
        accounted[index] += 1;
    }
    let counter = custody.behavior.base();
    assert_eq!((counter.value, counter.initializations), (processed, 1));
    assert_eq!(&counter.started, &started);
    assert!(
        custody.control.is_empty()
            && custody.descendants.is_empty()
            && custody.child_failures.is_empty()
    );
    assert!(
        custody.capability_failures.is_empty()
            && custody.additional_failures.is_empty()
            && custody.retirement_failures.is_empty()
    );
    assert_eq!(accounted, [1, 1, third_admissions]);
    assert!(
        pending && third_admissions == 0,
        "the third ordinary send must be Pending under capacity two and return its original after shutdown"
    );
}

#[test]
fn failed_initialization_leaves_no_ready_export() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the supported Node host opens");
    host.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), startup_exports(u64::MAX))
            .await
            .expect("a watchdog supplies no initialization or readiness evidence");
    });
}

#[test]
fn committed_initialization_serves_a_protected_read() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the supported Node host opens");
    host.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), startup_exports(0))
            .await
            .expect("a watchdog supplies no initialization or readiness evidence");
    });
}

#[expect(
    clippy::too_many_lines,
    reason = "one concrete startup trace retains original initialization, declaration, protected reply and joined cleanup outcomes"
)]
async fn startup_exports(initializations: u64) {
    let scope = ApplicationScope::new();
    let mut startups = scope.external::<StartupCounterStarted>().unwrap();
    let started = startups.recipient();
    let node = scope
        .with_root(
            StartupCounter {
                value: 0,
                initializations,
                started: started.clone(),
            }
            .stop_on_shutdown(),
        )
        .open_node(configuration("tls/localhost:0", &EndpointRole::Receiver))
        .await
        .unwrap_or_else(|(_, _, cause)| panic!("the actual Node opens: {cause:?}"));
    let runtime = node.runtime_id();
    let session = node.session.clone();
    let binding = Binding {
        deployment: "two-node-read-study",
        caller: "caller",
        controller: "owner",
        actor: "startup-counter",
        host: "receiver",
        protocol: "startup-counter",
        target_assignment: "one-live-root",
        logical_session: 557,
    };
    let claims = RefCell::new(None);
    let records = RefCell::new(Vec::<ProtectedMessage>::new());
    let protected_replies = RefCell::new(Vec::<ProtectedMessage>::new());
    let bindings = Cell::new(0);
    let calls = Cell::new(0);
    let verified = Cell::new(0);
    let decoded = Cell::new(0);
    let constructed = Cell::new(0);
    let encoded = Cell::new(0);
    let protected = Cell::new(0);
    let deadline = Instant::now() + Duration::from_secs(10);
    let current = |receipt: &recorded::Receipt<Command>, runtime: &str, target: MailAddr| {
        let claims = claims.borrow();
        let actual = claims.as_ref().ok_or(VerificationError::Invalid)?;
        current_scope(
            recorded::projection(receipt),
            actual,
            &Command::Read,
            Purpose::Request,
            deadline,
        )?;
        if actual.runtime_generation.as_ref() != runtime
            || actual.actor_generation != target.0
            || recorded::identity(receipt).0 != &binding
        {
            return Err(VerificationError::Invalid);
        }
        Ok(())
    };
    let maximum = NonZeroUsize::new(1).unwrap();
    let export = Export::new(
        "bombay/startup-counter",
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
        reply(
            |command| match command {
                Command::Read => Ok(|reply_to| {
                    constructed.set(constructed.get() + 1);
                    StartupCounterMessage::Read(reply_to)
                }),
                original @ Command::Increment => Err(original),
            },
            |receipt: &recorded::Receipt<Command>,
             original: &User<MailAddr, StartupCounterValue>| {
                encoded.set(encoded.get() + 1);
                serde_json::to_vec(&ReadReplyRef {
                    bombay: &recorded::projection(receipt).bombay,
                    kind: Purpose::ReadReply,
                    operation: &Command::Read,
                    value: &original.message.value,
                })
                .map(Vec::into_boxed_slice)
            },
        ),
        async |bytes: &[u8], proof: &[u8]| {
            verified.set(verified.get() + 1);
            recorded::verify::<Command>(
                &ProtectedMessage {
                    protected: bytes.into(),
                    proof: proof.into(),
                },
                &records.borrow(),
                binding,
            )
            .into_inner()
        },
        async |bytes: &[u8], _: &recorded::Receipt<Command>| {
            protected.set(protected.get() + 1);
            let proof: Box<[u8]> = [179, 181].into();
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
        bindings.set(bindings.get() + 1);
        let target = application.root().established_recipient();
        let mut actual = metadata(
            ProviderProfile::Recorded,
            "startup-counter",
            runtime.clone(),
            runtime,
        );
        actual.actor_generation = application.actor_address(&target).unwrap().0;
        records.borrow_mut().push(ProtectedMessage {
            protected: serde_json::to_vec(&ReadRequestRef {
                bombay: &actual,
                kind: Purpose::Request,
                operation: &Command::Read,
            })
            .unwrap()
            .into_boxed_slice(),
            proof: Box::from([173_u8]),
        });
        *claims.borrow_mut() = Some(actual);
        Ok::<_, Never>(target)
    });
    let outcome = node
        .run_with(|application| {
            let calls = &calls;
            let records = &records;
            let session = &session;
            async move {
                calls.set(calls.get() + 1);
                let (bytes, proof) = {
                    let records = records.borrow();
                    (records[0].protected.to_vec(), records[0].proof.to_vec())
                };
                let incoming = session
                    .get("bombay/startup-counter")
                    .allowed_destination(Locality::SessionLocal)
                    .consolidation(ConsolidationMode::None)
                    .payload(bytes)
                    .attachment(proof)
                    .timeout(Duration::from_secs(5))
                    .await
                    .expect("the actual protected Query starts");
                let mut replies = Vec::new();
                let ending = loop {
                    match incoming.recv_async().await {
                        Ok(original) => replies.push(original),
                        original @ Err(_) => break original,
                    }
                };
                let shutdown = application.shutdown();
                (replies, ending, shutdown)
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Node execution is issued"));
    let (work, report, native, notifications, ()) = outcome.into_parts();
    let closed = node
        .closed
        .take()
        .expect("the actual Node close result is retained");
    let bound = node.binding.take();
    let (inputs, declarations, startup, withdrawal, diagnostics, requests) =
        node.exports.into_parts();
    drop((inputs, declarations));
    startups.close_admission();
    let mut notices = Vec::new();
    while let Some(original) = startups.receive().await {
        notices.push(original);
    }
    let (origin, native) =
        native.expect("the original root is joined even after initialization rejection");
    let custody = native
        .custody()
        .expect("actual original state and runtime custody survive");
    let original = custody.behavior.base();
    // Cleanup and all native originals precede this independent readiness oracle.
    // Early declaration must reach this with Some(Ok), not a watchdog failure.
    let observed = (
        (original.value, original.initializations),
        bindings.get(),
        calls.get(),
        notices.len(),
        startup.as_ref().map(Result::is_ok),
        requests.len(),
    );
    match initializations {
        u64::MAX => assert_eq!(observed, ((0, u64::MAX), 0, 0, 0, None, 0)),
        0 => assert_eq!(observed, ((0, 1), 1, 1, 1, Some(true), 1)),
        _ => panic!(
            "only the existing overflow boundary and its genuine startup control are selected"
        ),
    }
    closed.expect("the actual Node Session closes");
    work.cleanup()
        .expect("actual cleanup has returned")
        .as_ref()
        .expect("actual Application cleanup joins");
    assert!(session.is_closed());
    assert!(node.poll_panic.is_none() && node.disposal_panic.is_none());
    assert!(node.withdrawal_poll_panic.is_none() && node.withdrawal_disposal_panic.is_none());
    let notifications = notifications.expect("both original notification receipts remain");
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    let report = report.expect("the actual native root report remains");
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(&original.started, &started);
    assert!(custody.user.is_empty() && custody.control.is_empty());
    assert!(custody.descendants.is_empty() && custody.child_failures.is_empty());
    assert!(
        custody.capability_failures.is_empty()
            && custody.additional_failures.is_empty()
            && custody.retirement_failures.is_empty()
    );
    assert!(diagnostics.is_empty());
    match initializations {
        u64::MAX => {
            assert!(matches!(&bound, Some(NodeBinding::Uninvoked(_))));
            let WorkOutcome::NotInvoked {
                work: _unused,
                startup_error,
                cleanup,
            } = work
            else {
                panic!("actual initialization failure retains its uninvoked Work");
            };
            assert!(startup_error.is_some() && cleanup.is_ok());
            assert!(matches!(
                &native,
                ActorRetirement::Retained {
                    outcome: RetirementOutcome::InitializationRejected {
                        error: CounterError::Overflow,
                        ..
                    },
                    ..
                }
            ));
            assert_eq!(report.failures(), ActorFailureAssessment::FailuresFound);
            assert!(startup.is_none() && withdrawal.is_none());
            assert_eq!(
                (
                    verified.get(),
                    decoded.get(),
                    constructed.get(),
                    encoded.get(),
                    protected.get()
                ),
                (0, 0, 0, 0, 0)
            );
            assert!(records.borrow().is_empty() && protected_replies.borrow().is_empty());
        }
        0 => {
            assert!(matches!(&bound, Some(NodeBinding::Invoked(Some(Ok(_))))));
            assert_eq!(notices, [User::new(origin.address(), 1)]);
            assert_eq!(native.completion(), Some(&Completion::Stopped));
            assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
            assert!(matches!(withdrawal, Some(Ok(()))));
            let ((replies, ending, shutdown), cleanup) = work
                .try_into_completed()
                .unwrap_or_else(|_| panic!("actual initialized Work and Query EOF return"));
            assert!(ending.is_err() && shutdown.is_ok() && cleanup.is_ok());
            assert_eq!(replies.len(), 1);
            let result = replies[0].result();
            let sample = result
                .as_ref()
                .expect("the real Read publishes successfully");
            let response = ProtectedMessage {
                protected: sample.payload().to_bytes().as_ref().into(),
                proof: sample
                    .attachment()
                    .expect("the actual reply proof remains")
                    .to_bytes()
                    .as_ref()
                    .into(),
            };
            let receipt =
                recorded::verify::<Command>(&response, &protected_replies.borrow(), binding)
                    .into_inner()
                    .expect("the whole actual response authenticates against its provider");
            let value: ReadReply<u64, Command> =
                serde_json::from_slice(&response.protected).unwrap();
            assert_eq!(
                (&value.bombay, value.kind, &value.operation, value.value),
                (
                    &recorded::projection(&receipt).bombay,
                    Purpose::ReadReply,
                    &Command::Read,
                    0
                )
            );
            assert_eq!(
                (
                    verified.get(),
                    decoded.get(),
                    constructed.get(),
                    encoded.get(),
                    protected.get()
                ),
                (1, 1, 1, 1, 1)
            );
            let [request] = <[_; 1]>::try_from(requests)
                .unwrap_or_else(|_| panic!("the one actual request remains"));
            let (
                key,
                raw,
                verified,
                input,
                native,
                encoding,
                protection,
                publications,
                refusals,
                panics,
            ) = request
                .try_into_parts()
                .unwrap_or_else(|_| panic!("actual producer loans return before inspection"));
            assert!(key.is_some() && input.is_none());
            let records = records.borrow();
            assert_eq!((&raw.0, &raw.1), (&records[0].protected, &records[0].proof));
            let verified = verified
                .expect("the actual verifier returns")
                .expect("the exact configured original authenticates");
            let declared: ReadRequest<Command> = serde_json::from_slice(&raw.0).unwrap();
            assert_eq!(
                recorded::identity(&verified),
                (&binding, declared.bombay.request_sequence)
            );
            assert_eq!(&recorded::projection(&verified).bombay, &declared.bombay);
            let (constructor, client, admission, received, tail) =
                native.expect("the actual native Read remains");
            assert!(
                constructor.is_none()
                    && matches!(&client, Attempt::Returned(Ok(_)))
                    && matches!(&admission, Attempt::Returned(Ok(())))
            );
            assert_eq!(
                received,
                Some(Some(User::new(
                    origin.address(),
                    StartupCounterValue {
                        value: 0,
                        initializations: 1
                    }
                )))
            );
            assert!(matches!(tail.as_slice(), [None]));
            let encoding = encoding.expect("the actual encoder returns").unwrap();
            let protection = protection.expect("the actual protector returns").unwrap();
            assert_eq!(encoding, response.protected);
            assert_eq!(protection, response.proof);
            assert!(matches!(publications.as_slice(), [Ok(())]));
            assert!(
                matches!(refusals, (None, None, None))
                    && matches!(panics, (None, None, None, None, None, None))
            );
        }
        _ => unreachable!("the original configured count was checked above"),
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

#[path = "../../../../../../examples/remote-counter/src/deployment.rs"]
#[expect(
    dead_code,
    reason = "the campaign reuses the compatibility policy from the example's complete deployment module"
)]
mod campaign_deployment;
mod campaign {
    use super::campaign_deployment::{Incompatibility, RequiredTransport, compatible};
    use super::{
        Binding, Counter, CounterMessage, CounterProcessed, CounterValue, EndpointRole,
        HeldCounterStartup, ProtectedMessage, ProviderProfile, Purpose, ReadReplyRef, ReadRequest,
        ReadRequestRef, ReconnectOperation, StartupCompletion, VerificationError, current_scope,
        limits, metadata, reconnect_router, recorded, router_client,
    };
    use crate::actors::ActorExt;
    use crate::application::ApplicationScope;
    use crate::diagnostic::DiagnosticDisposition;
    use crate::lifecycle::ActorDrainPolicy;
    use crate::remote::{Attempt, Export, ExportLimits, Metadata, RefusalRetention, reply};
    use crate::supervisor::roster::{
        self, FailureReaction, FixedLifecycle, FixedLifecycleEvent, FixedSupervisor,
        FixedSupervisorEvent, Recovery,
    };
    use crate::worker::{OrderedRoles, StartupLimit, WorkerSubmission};
    use crate::{ActorFailureAssessment, Completion, MailAddr, RetirementAssessment};
    use behavior::{
        ActiveTurn, Behavior, BehaviorActed, BehaviorBase, EstablishedDelivery,
        EstablishedRecipient, InitializationTurn, ItemSettlement, MessageProtocol, Never,
        SettledItem, Step, delegate_transition, initialize,
    };
    use behavior_actors::composition::replies::ExactReplies;
    use behavior_actors::{TimerElapsed, TimerScheduled};
    use futures_util::FutureExt;
    use std::cell::{Cell, OnceCell, RefCell};
    use std::convert::Infallible;
    use std::iter;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::runtime::Builder;
    use tokio::sync::{Semaphore, oneshot};
    use zenoh::handlers::FifoChannel;
    use zenoh::query::ConsolidationMode;
    use zenoh::sample::SampleKind;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CounterRole {
        Serving,
        Held,
    }

    type CounterLifecycle =
        MessageProtocol<MailAddr, FixedLifecycle<CounterRole, Counter, HeldCounterStartup>>;
    type Supervisor = FixedSupervisor<
        CounterRole,
        Counter,
        HeldCounterStartup,
        Never,
        Infallible,
        EstablishedRecipient<CounterLifecycle>,
        ExactReplies,
        ExactReplies,
    >;

    #[derive(Debug)]
    enum CampaignGuard {
        Incompatible(Incompatibility),
        Verification(VerificationError),
    }

    type DrainElapsed = MessageProtocol<MailAddr, TimerElapsed>;

    #[derive(behavior_macros::SendProduct)]
    struct DrainEffects<SupervisorSends, ElapsedSends> {
        supervisor: SupervisorSends,
        elapsed: ElapsedSends,
    }

    // The actual supervisor still decides when its drain expires. The test adds
    // one typed send so Work can release the held resource at that exact point.
    struct DrainingCounters {
        supervisor: Supervisor,
        first_schedule: Option<TimerScheduled>,
        first_elapsed_stop: Option<TimerElapsed>,
        elapsed: EstablishedRecipient<DrainElapsed>,
    }

    impl BehaviorBase for DrainingCounters {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    impl Behavior for DrainingCounters {
        type Protocol = <Supervisor as Behavior>::Protocol;
        type Event = <Supervisor as Behavior>::Event;
        type Sends =
            DrainEffects<<Supervisor as Behavior>::Sends, Vec<EstablishedDelivery<DrainElapsed>>>;
        type Ph = <Supervisor as Behavior>::Ph;
        type Error = <Supervisor as Behavior>::Error;
        type Birth = <Supervisor as Behavior>::Birth;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            initialize(&mut self.supervisor).map(|actions| {
                actions.map_sends(|supervisor| DrainEffects {
                    supervisor,
                    elapsed: Vec::new(),
                })
            })
        }

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            let elapsed = match &event {
                FixedSupervisorEvent::RestartElapsed(elapsed) => Some(*elapsed),
                _ => None,
            };
            if let FixedSupervisorEvent::RestartScheduleSettled(SettledItem::Attempted(
                ItemSettlement::Accepted(scheduled),
            )) = &event
            {
                self.first_schedule.get_or_insert(*scheduled);
            }
            let actions = delegate_transition(&mut self.supervisor, event)?;
            let mut notice = Vec::new();
            if let (Some(elapsed), Step::Stop(_)) = (elapsed, &actions.become_)
                && self.first_elapsed_stop.is_none()
            {
                self.first_elapsed_stop = Some(elapsed);
                notice.push(EstablishedDelivery::new(self.elapsed.clone(), elapsed));
            }
            Ok(actions.map_sends(|supervisor| DrainEffects {
                supervisor,
                elapsed: notice,
            }))
        }
    }

    bombay::topology! {
        draining_counters {
            root: DrainingCounters { Proxy => proxy; }
            proxy { Worker => worker; }
            worker {}
        }
    }

    #[test]
    #[expect(
        clippy::large_futures,
        reason = "the finite local watchdog retains the measured combined-test future on the test stack"
    )]
    fn incompatibility_router_loss_and_unavailable_provider_precede_real_drain_expiry() {
        let stage = Cell::new("bind TLS Router");
        let host = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        host.block_on(async {
            tokio::time::timeout(Duration::from_secs(90), combined_failures(&stage))
                .await
                .unwrap_or_else(|_| {
                    panic!("combined retirement fixture stalled at {}", stage.get())
                });
        });
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one causal trace retains the original provider, transport, actor and retirement outcomes"
    )]
    #[expect(
        clippy::large_futures,
        reason = "Node Work retains the original finite campaign observations until the complete native join"
    )]
    #[expect(
        clippy::result_large_err,
        reason = "the standard reply selector returns the original typed request on refusal"
    )]
    async fn combined_failures(stage: &Cell<&str>) {
        const COUNTER: &str = "bombay/campaign/draining-counter";
        let sequences = [607_u64, 613, 617, 619];
        let (router, address) = reconnect_router("tls/localhost:0").await;
        let old_router = router.zid().to_string();
        stage.set("open caller Session");
        let caller = zenoh::open(
            router_client(&address, &EndpointRole::Caller)
                .to_configuration()
                .unwrap(),
        )
        .await
        .expect("one caller Session connects to the actual TLS Router");
        let caller_id = caller.zid().to_string();
        let scope = ApplicationScope::new();
        let mut lifecycle = scope.external::<CounterLifecycle>().unwrap();
        let mut processed = scope.external::<CounterProcessed>().unwrap();
        let mut values = scope.external::<CounterValue>().unwrap();
        let mut elapsed = scope.external::<DrainElapsed>().unwrap();
        let resources = Arc::new(Semaphore::new(2));
        let (serving_reached, serving_observed) = oneshot::channel();
        let (serving_release, serving_released) = oneshot::channel();
        let (held_reached, held_observed) = oneshot::channel();
        let (held_release, held_released) = oneshot::channel();
        let mut serving = Some(HeldCounterStartup {
            reached: serving_reached,
            release: serving_released,
            resource: resources.clone().try_acquire_owned().unwrap(),
            readiness: [31, 37].into(),
            completion: StartupCompletion::Ready,
        });
        let mut held = Some(HeldCounterStartup {
            reached: held_reached,
            release: held_released,
            resource: resources.clone().try_acquire_owned().unwrap(),
            readiness: [41, 43].into(),
            completion: StartupCompletion::Ready,
        });
        let released_serving = serving_release.send(());
        let supervisor: Supervisor = roster::new(
            move |role: &CounterRole| {
                let startup = match role {
                    CounterRole::Serving => serving.take(),
                    CounterRole::Held => held.take(),
                }
                .expect("each original initial role owns one startup plan");
                Ok::<_, Infallible>(WorkerSubmission::with_startup(Counter::new(), startup))
            },
            OrderedRoles::new(CounterRole::Serving, [CounterRole::Held]).unwrap(),
            roster::Config {
                startup: StartupLimit::new(2).unwrap(),
                recovery: Recovery::temporary(),
                failure: FailureReaction::StopSupervisor,
                shutdown: ActorDrainPolicy::RetireActorGraphAfter {
                    deadline: Duration::from_millis(20),
                },
                diagnostics: DiagnosticDisposition::terminate(),
            },
        )
        .publish_lifecycle(lifecycle.recipient())
        .build()
        .unwrap_or_else(|_| panic!("both actual initial Counter submissions remain owned"));
        let root = DrainingCounters {
            supervisor,
            first_schedule: None,
            first_elapsed_stop: None,
            elapsed: elapsed.recipient(),
        }
        .stop_on_shutdown();
        let maximum = NonZeroUsize::new(sequences.len()).unwrap();
        let mut network = router_client(&address, &EndpointRole::Receiver);
        network.max_retained_queries = maximum;
        let node = scope
            .with_topology(root, draining_counters::Topology::new())
            .open_node(network)
            .await
            .unwrap_or_else(|_| panic!("the real supervisor Node connects to the Router"));
        let runtime = node.runtime_id();
        let session = node.session.clone();
        let receiver_events = session
            .info()
            .transport_events_listener()
            .history(true)
            .with(FifoChannel::new(8))
            .await
            .unwrap();
        let caller_events = caller
            .info()
            .transport_events_listener()
            .history(true)
            .with(FifoChannel::new(8))
            .await
            .unwrap();
        let records = OnceCell::<Vec<ProtectedMessage>>::new();
        let reply_records = RefCell::new(Vec::<ProtectedMessage>::new());
        let configured_claims = RefCell::new(Vec::<Metadata>::new());
        let prepared = RefCell::new(None);
        let readiness = RefCell::new(None);
        let (provider_response, provider_receiving) =
            oneshot::channel::<recorded::Receipt<ReconnectOperation>>();
        let provider_receiving = RefCell::new(Some(provider_receiving));
        let binding = Binding {
            deployment: "two-node-read-study",
            caller: "caller",
            controller: "owner",
            actor: "campaign-counter",
            host: "receiver",
            protocol: "campaign-counter",
            target_assignment: "same-serving-initial-child",
            logical_session: 601,
        };
        let deadline = Instant::now() + Duration::from_secs(90);
        let current =
            |receipt: &recorded::Receipt<ReconnectOperation>, runtime: &str, target: MailAddr| {
                let scope = recorded::projection(receipt);
                let configured = configured_claims.borrow();
                let actual = configured
                    .iter()
                    .find(|claims| claims.request_sequence == scope.bombay.request_sequence)
                    .ok_or(CampaignGuard::Verification(VerificationError::Invalid))?;
                current_scope(
                    scope,
                    actual,
                    &ReconnectOperation::Increment,
                    Purpose::Request,
                    deadline,
                )
                .map_err(CampaignGuard::Verification)?;
                if actual.runtime_generation.as_ref() != runtime
                    || actual.actor_generation != target.0
                    || recorded::identity(receipt).0 != &binding
                {
                    return Err(CampaignGuard::Verification(VerificationError::Invalid));
                }
                // The trusted per-request claims intentionally retain wire=2 for
                // the incompatible input. No independent equality gate masks this
                // one application-owned compatibility decision.
                compatible(
                    &scope.bombay,
                    1,
                    1,
                    &configured[0].provider_profile,
                    RequiredTransport::Tls,
                )
                .map_err(CampaignGuard::Incompatible)
            };
        let export = Export::new(
            COUNTER,
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
                    Ok::<_, ReadRequest<ReconnectOperation>>(move |reply_to| {
                        CounterMessage::IncrementAndReply {
                            request_sequence: request.bombay.request_sequence,
                            reply_to,
                        }
                    })
                },
                |receipt: &recorded::Receipt<ReconnectOperation>,
                 original: &behavior::User<MailAddr, super::CounterIncremented>| {
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
                let request: ReadRequest<ReconnectOperation> =
                    serde_json::from_slice(bytes).map_err(VerificationError::Declaration)?;
                if request.bombay.request_sequence == sequences[2] {
                    let receiving = provider_receiving
                        .borrow_mut()
                        .take()
                        .expect("one real provider response transaction belongs to this input");
                    return receiving.await.map_err(VerificationError::Unavailable);
                }
                recorded::verify::<ReconnectOperation>(
                    &ProtectedMessage {
                        protected: bytes.into(),
                        proof: proof.into(),
                    },
                    records
                        .get()
                        .expect("binding publishes the original request records once"),
                    binding,
                )
                .await
            },
            async |bytes: &[u8], _: &recorded::Receipt<ReconnectOperation>| {
                let proof: Box<[u8]> = [47, 53].into();
                reply_records.borrow_mut().push(ProtectedMessage {
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
            stage.set("serving startup signal");
            let serving_started = serving_observed.await;
            stage.set("held startup signal");
            let held_started = held_observed.await;
            stage.set("serving lifecycle notification");
            let original_notice = lifecycle
                .receive()
                .await
                .expect("only the released initial role can become ready");
            let target = match original_notice.message.event() {
                FixedLifecycleEvent::Started {
                    role: CounterRole::Serving,
                    proxy,
                } => proxy.clone(),
                _ => panic!("the unresolved initial role cannot supply a serving capability"),
            };
            let serving = target.recipient();
            let actor = application.actor_address(&serving).unwrap();
            let mut originals = Vec::new();
            let mut configured_records = Vec::new();
            for sequence in sequences {
                let mut claims = metadata(
                    ProviderProfile::Recorded,
                    "campaign-counter",
                    caller_id.clone(),
                    runtime.clone(),
                );
                claims.actor_generation = actor.0;
                claims.request_sequence = sequence;
                if sequence == sequences[1] {
                    claims.wire_version = 2;
                }
                let original = ProtectedMessage {
                    protected: serde_json::to_vec(&ReadRequestRef {
                        bombay: &claims,
                        kind: Purpose::Request,
                        operation: &ReconnectOperation::Increment,
                    })
                    .unwrap()
                    .into_boxed_slice(),
                    proof: sequence.to_be_bytes().into(),
                };
                configured_records.push(ProtectedMessage {
                    protected: original.protected.clone(),
                    proof: original.proof.clone(),
                });
                configured_claims.borrow_mut().push(claims);
                originals.push(original);
            }
            records
                .set(configured_records)
                .unwrap_or_else(|_| panic!("the one binding publishes one original request table"));
            *prepared.borrow_mut() = Some((serving, originals));
            *readiness.borrow_mut() = Some((serving_started, held_started, original_notice));
            Ok::<_, Never>(target)
        });
        let outcome = node
            .run_with(|application| {
                let caller = &caller;
                let router = &router;
                let session = &session;
                let address = &address;
                let prepared = &prepared;
                let resources = &resources;
                async move {
                    let (serving, originals) = prepared.borrow_mut().take().unwrap();
                    let target = application.actor_address(&serving).unwrap();
                    stage.set("initial transport events");
                    let initial = (
                        receiver_events.recv_async().await.unwrap(),
                        caller_events.recv_async().await.unwrap(),
                    );
                    let links_before = (
                        session.info().links().await.collect::<Vec<_>>(),
                        caller.info().links().await.collect::<Vec<_>>(),
                    );
                    let querier = caller.declare_querier(COUNTER).await.unwrap();
                    let matching = querier
                        .matching_listener()
                        .with(FifoChannel::new(8))
                        .await
                        .unwrap();
                    stage.set("first export visibility");
                    let first_matching = loop {
                        let actual = matching.recv_async().await.unwrap();
                        if actual.matching() {
                            break actual;
                        }
                    };
                    let matching_closed = matching.undeclare().await;
                    let querier_closed = querier.undeclare().await;
                    let request = async |original: &ProtectedMessage| {
                        let replies = caller
                            .get(COUNTER)
                            .payload(original.protected.to_vec())
                            .attachment(original.proof.to_vec())
                            .consolidation(ConsolidationMode::None)
                            .timeout(Duration::from_secs(20))
                            .await
                            .unwrap();
                        let mut received = Vec::new();
                        let ending = loop {
                            match replies.recv_async().await {
                                Ok(original) => received.push(original),
                                Err(ending) => break ending,
                            }
                        };
                        (received, ending)
                    };
                    let mut read = async || {
                        let sent = values
                            .send(&serving, CounterMessage::Read(values.recipient()))
                            .await;
                        let original = values.receive().await;
                        (sent, original)
                    };
                    stage.set("first remote request and local Read");
                    let first = request(&originals[0]).await;
                    let before = read().await;
                    stage.set("incompatible request and local Read");
                    let incompatible = request(&originals[1]).await;
                    let after_incompatible = read().await;
                    stage.set("Router close and transport deletion");
                    let router_closed = router.close().await;
                    let deleted = (
                        receiver_events.recv_async().await.unwrap(),
                        caller_events.recv_async().await.unwrap(),
                    );
                    let links_down = (
                        session.info().links().await.collect::<Vec<_>>(),
                        caller.info().links().await.collect::<Vec<_>>(),
                    );
                    let live_down = (
                        session.is_closed(),
                        caller.is_closed(),
                        session.zid().to_string(),
                        caller.zid().to_string(),
                    );
                    stage.set("disconnected local increment and Read");
                    let local_increment = processed
                        .send(
                            &serving,
                            CounterMessage::IncrementAndReply {
                                request_sequence: 621,
                                reply_to: processed.recipient(),
                            },
                        )
                        .await;
                    let local_processed = processed.receive().await;
                    let down = read().await;
                    stage.set("Router replacement and restored transport events");
                    let (replacement, rebound) = reconnect_router(address).await;
                    let new_router = replacement.zid().to_string();
                    let restored = (
                        receiver_events.recv_async().await.unwrap(),
                        caller_events.recv_async().await.unwrap(),
                    );
                    let links_after = (
                        session.info().links().await.collect::<Vec<_>>(),
                        caller.info().links().await.collect::<Vec<_>>(),
                    );
                    let querier = caller.declare_querier(COUNTER).await.unwrap();
                    let matching = querier
                        .matching_listener()
                        .with(FifoChannel::new(8))
                        .await
                        .unwrap();
                    stage.set("restored export visibility");
                    let last_matching = loop {
                        let actual = matching.recv_async().await.unwrap();
                        if actual.matching() {
                            break actual;
                        }
                    };
                    let last_matching_closed = matching.undeclare().await;
                    let last_querier_closed = querier.undeclare().await;
                    // This is actual provider transaction loss. It is not an Invalid
                    // credential or a manufactured unavailable status.
                    stage.set("unavailable provider request and local Read");
                    drop(provider_response);
                    let unavailable = request(&originals[2]).await;
                    let after_unavailable = read().await;
                    stage.set("fresh remote request and local Read");
                    let fresh = request(&originals[3]).await;
                    let after_fresh = read().await;
                    let live_after = (
                        session.is_closed(),
                        caller.is_closed(),
                        session.zid().to_string(),
                        caller.zid().to_string(),
                    );
                    stage.set("close caller and transport listeners");
                    let receiver_events_closed = receiver_events.undeclare().await;
                    let caller_events_closed = caller_events.undeclare().await;
                    let caller_closed = caller.close().await;
                    let sender = application.sender().unwrap();
                    stage.set("submit supervisor shutdown");
                    let shutdown = sender.send(roster::FixedCommand::shutdown()).await;
                    stage.set("receive the exact elapsed drain notification");
                    let elapsed_notice = elapsed.receive().await;
                    let held_at_expiry = resources.available_permits();
                    let terminated_before_release =
                        application.lifecycle().termination().now_or_never();
                    let joined_before_release = application.lifecycle().retirement().now_or_never();
                    stage.set("release held startup and join retirement");
                    let released_held = held_release.send(());
                    let termination = application.lifecycle().termination().await;
                    let retired = application.lifecycle().retirement().await;
                    let replayed = application.lifecycle().retirement().await;
                    stage.set("finish owning Node cleanup");
                    let permits_after_join = resources.available_permits();
                    (
                        originals,
                        target,
                        [first, incompatible, unavailable, fresh],
                        [
                            before,
                            after_incompatible,
                            down,
                            after_unavailable,
                            after_fresh,
                        ],
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
                            live_down,
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
                        (local_increment, local_processed),
                        (
                            router_closed,
                            caller_closed,
                            receiver_events_closed,
                            caller_events_closed,
                            shutdown,
                            elapsed_notice,
                            termination,
                            held_at_expiry,
                            terminated_before_release,
                            joined_before_release,
                            released_held,
                            retired,
                            replayed,
                            permits_after_join,
                        ),
                        replacement,
                    )
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the same Node retains the complete combined campaign"));
        let (export, bound, closed, polling, disposal, withdrawal_polling, withdrawal_disposal) =
            node.into_parts().unwrap_or_else(|_| {
                panic!("the Node returns every original close and driver field")
            });
        let (work, report, native, notifications, ()) = outcome.into_parts();
        let (
            (originals, target, replies, reads, connections, matching, local, closure, replacement),
            cleanup,
        ) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("all combined observations survive the actual cleanup"));
        let replacement_closed = replacement.close().await;
        let (_, native) =
            native.expect("the original supervisor and both initial children remain acquired");
        let (_, _, startup, withdrawal, diagnostics, requests) = export.into_parts();
        let reply_records = reply_records.into_inner();
        lifecycle.close_admission();
        let mut remaining_notices = Vec::new();
        while let Some(original) = lifecycle.receive().await {
            remaining_notices.push(original);
        }
        let (serving_started, held_started, original_notice) = readiness.into_inner().unwrap();
        let mut started_roles = Vec::new();
        let mut restarted_roles = Vec::new();
        for original in iter::once(&original_notice).chain(&remaining_notices) {
            match original.message.event() {
                FixedLifecycleEvent::Started { role, .. } => started_roles.push(*role),
                FixedLifecycleEvent::Restarted { role, .. } => restarted_roles.push(*role),
                _ => {}
            }
        }
        let mut rows = Vec::new();
        for request in requests {
            rows.push(
                request
                    .try_into_parts()
                    .unwrap_or_else(|_| panic!("all native request producers ended")),
            );
        }
        cleanup.expect("the original actor graph joins after resource release");
        closed.expect("the actual receiving Session closes");
        replacement_closed.expect("the replacement Router closes after the Node");
        let (
            router_closed,
            caller_closed,
            receiver_events_closed,
            caller_events_closed,
            shutdown,
            elapsed_notice,
            termination,
            held_at_expiry,
            terminated_before_release,
            joined_before_release,
            released_held,
            retired,
            replayed,
            permits_after_join,
        ) = closure;
        router_closed.expect("the original transport hop closed");
        caller_closed.expect("the one caller Session closed");
        receiver_events_closed.expect("the receiver transport listener was undeclared");
        caller_events_closed.expect("the caller transport listener was undeclared");
        let custody = native
            .custody()
            .expect("the native body retains the actual deadline observation");
        let observed = custody.behavior.base();
        let native_admissions = rows
            .iter()
            .map(|row| {
                let request: ReadRequest<ReconnectOperation> =
                    serde_json::from_slice(&row.1.0).unwrap();
                let admitted = row.4.as_ref().and_then(|(_, _, admission, received, _)| {
                    match (admission, received) {
                        (Attempt::Returned(Ok(())), Some(Some(original))) => {
                            Some((original.from, original.message.request_sequence))
                        }
                        _ => None,
                    }
                });
                (request.bombay.request_sequence, admitted)
            })
            .collect::<Vec<_>>();
        let actual_values = reads.each_ref().map(|(_, original)| {
            original
                .as_ref()
                .map(|original| (original.from, original.message))
        });
        let actual_replies = replies.each_ref().map(|(native, _)| {
            native
                .iter()
                .map(|original| {
                    let reply = original.result();
                    reply
                        .as_ref()
                        .ok()
                        .and_then(|sample| {
                            serde_json::from_slice::<super::ReadReply<u64, ReconnectOperation>>(
                                &sample.payload().to_bytes(),
                            )
                            .ok()
                        })
                        .map(|reply| (reply.bombay.request_sequence, reply.value))
                })
                .collect::<Vec<_>>()
        });
        let timer = match (observed.first_schedule, observed.first_elapsed_stop) {
            (Some(scheduled), Some(elapsed)) => Some((
                scheduled.id == elapsed.id,
                scheduled.generation == elapsed.generation,
            )),
            _ => None,
        };
        let proxy = custody
            .descendants
            .iter()
            .find(|child| child.origin.address() == target)
            .expect("the same original serving proxy remains in the actual native graph");
        let proxy_custody = proxy
            .retirement
            .custody()
            .expect("the actual stable proxy custody remains");
        let [worker] = proxy_custody.descendants.as_slice() else {
            panic!("one initial serving Counter is retained; the Router never restarts this actor");
        };
        let worker_address = worker.origin.address();
        let actual = (
            native_admissions.as_slice(),
            actual_values,
            actual_replies,
            timer,
            held_at_expiry,
            joined_before_release.is_some(),
            permits_after_join,
            started_roles.as_slice(),
            restarted_roles.as_slice(),
        );
        assert_eq!(
            actual,
            (
                [
                    (sequences[0], Some((worker_address, sequences[0]))),
                    (sequences[1], None),
                    (sequences[2], None),
                    (sequences[3], Some((worker_address, sequences[3])))
                ]
                .as_slice(),
                [
                    Some((worker_address, 1)),
                    Some((worker_address, 1)),
                    Some((worker_address, 2)),
                    Some((worker_address, 2)),
                    Some((worker_address, 3))
                ],
                [
                    vec![Some((sequences[0], sequences[0]))],
                    vec![],
                    vec![],
                    vec![Some((sequences[3], sequences[3]))]
                ],
                Some((true, true)),
                1,
                false,
                2,
                [CounterRole::Serving].as_slice(),
                &[] as &[CounterRole],
            )
        );
        let elapsed_notice = elapsed_notice.expect("Work received the exact typed drain event");
        assert_eq!(elapsed_notice.from, MailAddr::APPLICATION_ROOT);
        assert_eq!(Some(elapsed_notice.message), observed.first_elapsed_stop);
        assert!(terminated_before_release.is_none());
        // Every typed rejection is inspected only after the independent complete
        // native-admission, state, actual-expiry and retirement-frontier oracle.
        assert!(matches!(
            &rows[1].8.0,
            Some(CampaignGuard::Incompatible(Incompatibility::Wire {
                required: 1,
                actual: 2
            }))
        ));
        assert!(matches!(
            &rows[2].2,
            Some(Err(VerificationError::Unavailable(_)))
        ));
        for (row, original) in rows.iter().zip(&originals) {
            if let Some(CampaignGuard::Verification(cause)) = &row.8.0 {
                panic!("unexpected current-authority refusal: {cause:?}");
            }
            assert_eq!((&row.1.0, &row.1.1), (&original.protected, &original.proof));
            assert!(matches!(&row.9, (None, None, None, None, None, None)));
        }
        assert_eq!(rows.len(), sequences.len());

        for index in [0_usize, 3] {
            let native_reply = &replies[index].0[0];
            let reply = native_reply.result();
            let sample = reply.as_ref().unwrap();
            let actual = ProtectedMessage {
                protected: sample.payload().to_bytes().as_ref().into(),
                proof: sample.attachment().unwrap().to_bytes().as_ref().into(),
            };
            let receipt = recorded::verify::<ReconnectOperation>(&actual, &reply_records, binding)
                .await
                .unwrap();
            assert_eq!(recorded::projection(&receipt).kind, Purpose::Processed);
            assert_eq!(
                recorded::projection(&receipt).bombay.request_sequence,
                sequences[index]
            );
        }
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
        for (links, expected) in [
            (&links_before.0, &old_router),
            (&links_before.1, &old_router),
            (&links_after.0, &new_router),
            (&links_after.1, &new_router),
        ] {
            let [link] = links.as_slice() else {
                panic!("each live client has one actual TLS Router link");
            };
            assert_eq!(link.zid().to_string(), *expected);
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
        let (
            first_matching,
            matching_closed,
            querier_closed,
            last_matching,
            last_matching_closed,
            last_querier_closed,
        ) = matching;
        assert!(first_matching.matching() && last_matching.matching());
        matching_closed.unwrap();
        querier_closed.unwrap();
        last_matching_closed.unwrap();
        last_querier_closed.unwrap();
        let (local_increment, local_processed) = local;
        assert!(local_increment.is_ok());
        assert_eq!(
            local_processed
                .as_ref()
                .map(|original| (original.from, original.message.request_sequence)),
            Some((worker_address, 621))
        );
        assert!(reads.iter().all(|(sent, _)| sent.is_ok()));
        assert!(
            released_serving.is_ok()
                && serving_started.is_ok()
                && held_started.is_ok()
                && released_held.is_ok()
        );
        assert!(matches!(
            original_notice.message.event(),
            FixedLifecycleEvent::Started {
                role: CounterRole::Serving,
                ..
            }
        ));
        assert!(shutdown.is_ok());
        assert_eq!(termination, Ok(behavior_actors::Exit::Normal));
        assert_eq!(native.completion(), Some(&Completion::Stopped));
        assert_eq!(retired, replayed);
        assert_eq!(retired.retirement(), RetirementAssessment::Established);
        assert_eq!(retired.failures(), ActorFailureAssessment::NoFailuresFound);
        assert_eq!(report, Some(retired));
        let notifications = notifications.unwrap();
        assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
        assert!(
            matches!(&bound, Some(Ok(_)))
                && matches!(&startup, Some(Ok(())))
                && matches!(&withdrawal, Some(Ok(())))
        );
        assert!(
            diagnostics.is_empty()
                && polling.is_none()
                && disposal.is_none()
                && withdrawal_polling.is_none()
                && withdrawal_disposal.is_none()
        );
        assert!(session.is_closed());
    }
}
