//! A retained result remains available after target retirement while Work is live.
use super::counter::{CounterIncremented, CounterProcessed, CounterValue};
use super::protocol::{
    Binding, EndpointRole, ProtectedMessage, ProviderProfile, Purpose, ReadReply, ReadReplyRef,
    ReadRequest, ReadRequestRef, VerificationError, configuration, current_scope, limits, metadata,
    recorded,
};
use super::{Counter, CounterMessage};
use crate::actors::ActorExt;
use crate::application::Application;
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::proxy::{ProxyControl, ProxyDiagnostic, ProxyOutcome, StableProxy};
use crate::remote::{Attempt, Export, ExportLimits, RefusalRetention, stop, submission};
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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use zenoh::query::ConsolidationMode;
use zenoh::sample::Locality;

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

struct HeldCounterStartup {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    resource: OwnedSemaphorePermit,
    readiness: Box<[u8]>,
}

impl StartupPlan for HeldCounterStartup {
    type Ready = Box<[u8]>;
    type Rejection = oneshot::error::RecvError;

    async fn start(self) -> Result<Self::Ready, Self::Rejection> {
        let published = self.reached.send(());
        published.expect("the binding observes this actual target-owned startup");
        let released = self.release.await;
        drop(self.resource);
        released.map(|()| self.readiness)
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
    reason = "the finite owned-startup test retains its measured 16 KiB concrete future on the test stack"
)]
fn remote_stop_waits_for_target_owned_startup_after_termination() {
    let host = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the actual remote retirement host opens");
    host.block_on(async {
        tokio::time::timeout(Duration::from_secs(40), stop_with_held_startup())
            .await
            .expect("the watchdog establishes no stop, join or resource evidence");
    });
}

#[expect(
    clippy::result_large_err,
    clippy::too_many_lines,
    clippy::await_holding_refcell_ref,
    reason = "one remote stop trace conserves original startup/native/notification custody, and the configured proof table is immutable during actual verification"
)]
async fn stop_with_held_startup() {
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
    assert_eq!(retired.failures(), ActorFailureAssessment::NoFailuresFound);
    assert_eq!(report.retirement(), RetirementAssessment::Established);
    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
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
    assert!(
        proxy_custody.child_failures.is_empty() && proxy_custody.capability_failures.is_empty()
    );
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
    let [readiness] = readiness.as_slice() else {
        panic!("one original ready result returns to its stopped owner");
    };
    let (worker, ready) = *readiness;
    assert_eq!(worker.creation(), child.origin.creation_id());
    assert_eq!(ready.as_ptr(), readiness_allocation);
    assert_eq!(ready.as_ref(), [211, 223]);
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
