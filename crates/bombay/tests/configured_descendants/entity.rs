//! Entity hydration uses the same configured host and preserves failed startup.
use super::{
    Config, Document, DocumentChildrenNext, DocumentReady, FileAccess, Files, ManagedDocument,
    OpenFile, file_request, inspect_file,
};
use behavior_actors::ShutdownRequested;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, ChildCreationOutcome, ClassifySettlement, CreateChild,
    CreationKind, CreationSequence, CreationSettlement, Creations, EstablishedDelivery,
    EstablishedRecipient, EventLayer, InterpretationProgress, InterpreterRequests, ItemSettlement,
    Never, NoSends, SettledItem, SettlementStatus, Step,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityShutdown, Refusal, RetirementMode,
};
use bombay::prelude::{Completion, Exit, StopOnShutdown};
use bombay::{
    ActorFailureAssessment, ActorRetirement, ActorRetirementReport, ApplicationScope,
    InterpreterFactory, MailAddr, RetirementAssessment, RetirementSchema,
};
use core::num::NonZeroUsize;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::resume_unwind;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::task::JoinError;

struct Catalog;
#[bombay::actor(message = Never)]
impl Catalog {}

struct Ledger {
    original: Box<[u8]>,
    file: Option<OpenFile>,
    document: Option<ManagedDocument>,
    ready: EstablishedRecipient<DocumentReady>,
    commands: Vec<Box<[u8]>>,
}

#[bombay::actor(
    sends = {
        files: InterpreterRequests<OpenFile>,
        ready: Vec<EstablishedDelivery<DocumentReady>>,
    },
    births = { document: ManagedDocument },
    creation_settlements = retain_for_retirement,
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor returns the actual typed Actions carrier"
)]
impl Ledger {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one original document birth fits");
        let document = self.document.take().expect("the original document");
        let file = self.file.take().expect("the original ledger File request");
        Ok(
            Actions::create(Creations::one(CreateChild::birth(id, document)))
                .with_send(file)
                .with_send(EstablishedDelivery::new(
                    self.ready.clone(),
                    self.original[0],
                )),
        )
    }

    fn receive(&mut self, command: Box<[u8]>) -> BehaviorActed<Self> {
        let reply = command[0];
        self.commands.push(command);
        Ok(Actions::cont().send_ready(EstablishedDelivery::new(self.ready.clone(), reply)))
    }
}

struct LedgerConfig {
    files: Config,
    attempts: Arc<AtomicUsize>,
    constructors: Mutex<VecDeque<Result<(), Box<[u8; 2]>>>>,
}

impl InterpreterFactory for LedgerConfig {
    type Interpreter = Files;
    fn create(&self) -> Files {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let constructor = self
            .constructors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .expect("the explicit construction plan covers only the real actors");
        match constructor {
            Ok(()) => self.files.create(),
            Err(cause) => resume_unwind(cause),
        }
    }
}

type LedgerRetirement = ActorRetirement<
    StopOnShutdown<Ledger>,
    library::LedgersRetirements,
    Vec<<library::LedgersRetirements as RetirementSchema>::Failure>,
>;
type LedgerFailure = EntityActivationError<
    u64,
    StopOnShutdown<Ledger>,
    library::LedgersRetirements,
    Vec<<library::LedgersRetirements as RetirementSchema>::Failure>,
>;
type LedgerRetirements = Arc<
    Mutex<
        Vec<(
            EntityId<u64>,
            ActivationId,
            Result<LedgerRetirement, JoinError>,
            ActorRetirementReport,
        )>,
    >,
>;
type LedgerFailures = Arc<Mutex<Vec<(EntityId<u64>, ActivationId, LedgerFailure)>>>;
type LedgerRefusals = Arc<Mutex<Vec<(EntityId<u64>, AdmissionFailure<Box<[u8]>>)>>>;
type LedgerDrains = Arc<Mutex<Vec<(EntityId<u64>, ActivationId, DrainFailure)>>>;

struct Ledgers {
    roots: Mutex<BTreeMap<u64, Ledger>>,
    retirements: LedgerRetirements,
    failures: LedgerFailures,
    refusals: LedgerRefusals,
    drains: LedgerDrains,
}

impl EntityDefinition for Ledgers {
    type Id = u64;
    type Behavior = StopOnShutdown<Ledger>;
    type Hosts = library::Topology;
    type HydrationError = u64;
    type Terminal = library::LedgersRetirements;
    type ChildFailures = Vec<<Self::Terminal as RetirementSchema>::Failure>;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "transfer the original Ledger only when hydration is polled"
    )]
    async fn hydrate(&self, id: EntityId<u64>) -> Result<Self::Behavior, u64> {
        self.roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id.get())
            .map(ActorExt::stop_on_shutdown)
            .ok_or(*id.get())
    }
    fn activation_failed(
        &self,
        id: EntityId<u64>,
        activation: ActivationId,
        failure: LedgerFailure,
    ) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, activation, failure));
    }
    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<Box<[u8]>>) {
        self.refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, failure));
    }
    fn forced_retirement(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        failure: DrainFailure,
    ) {
        self.drains
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, activation, failure));
    }
    fn retired(
        &self,
        id: &EntityId<u64>,
        activation: ActivationId,
        retirement: Result<LedgerRetirement, JoinError>,
        report: ActorRetirementReport,
    ) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, activation, retirement, report));
    }
}

#[derive(Clone, Copy)]
struct LedgersRole;

bombay::topology! {
    library {
        interpreter: LedgerConfig;
        root: Catalog {}
        hosts { ledgers: Ledger; }
        families { ledgers: Ledgers; }
        ledgers { Document(LedgerChildrenDocument) => document; }
        document: ManagedDocument { Next(DocumentChildrenNext) => document; }
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "two nominally identical configured Entity applications preserve success, startup refusal and every original result"
)]
async fn entity_hydration_keeps_configured_services_and_original_constructor_failure() {
    for _ in 0..2 {
        let scope = ApplicationScope::new();
        let mut replies = scope
            .external::<DocumentReady>()
            .expect("the original early File/command recipient");
        let ready = replies.recipient();
        let live_bytes = Box::<[u8]>::from([181]);
        let child_bytes = Box::<[u8]>::from([191]);
        let grandchild_bytes = Box::<[u8]>::from([193]);
        let live_pointer = live_bytes.as_ptr() as usize;
        let child_pointer = child_bytes.as_ptr() as usize;
        let grandchild_pointer = grandchild_bytes.as_ptr() as usize;
        let refused_bytes = Box::<[u8]>::from([197]);
        let unborn_bytes = Box::<[u8]>::from([199]);
        let leaf_bytes = Box::<[u8]>::from([211]);
        let refused_pointer = refused_bytes.as_ptr() as usize;
        let unborn_pointer = unborn_bytes.as_ptr() as usize;
        let leaf_pointer = leaf_bytes.as_ptr() as usize;
        let refused_file = file_request();
        let refused_path_pointer =
            refused_file.path.as_os_str().as_encoded_bytes().as_ptr() as usize;
        let expected_refused_path = refused_file.path.clone();
        let unborn_file = file_request();
        let unborn_path_pointer = unborn_file.path.as_os_str().as_encoded_bytes().as_ptr() as usize;
        let expected_unborn_path = unborn_file.path.clone();
        let leaf_file = file_request();
        let leaf_path_pointer = leaf_file.path.as_os_str().as_encoded_bytes().as_ptr() as usize;
        let expected_leaf_path = leaf_file.path.clone();
        let live = Ledger {
            original: live_bytes,
            file: Some(file_request()),
            ready: ready.clone(),
            commands: vec![],
            document: Some(
                Document {
                    original: child_bytes,
                    file: Some(file_request()),
                    ready: ready.clone(),
                    next: Some(Box::new(Document {
                        original: grandchild_bytes,
                        file: Some(file_request()),
                        ready: ready.clone(),
                        next: None,
                    })),
                }
                .stop_on_shutdown(),
            ),
        };
        let refused = Ledger {
            original: refused_bytes,
            file: Some(refused_file),
            ready: ready.clone(),
            commands: vec![],
            document: Some(
                Document {
                    original: unborn_bytes,
                    file: Some(unborn_file),
                    ready: ready.clone(),
                    next: Some(Box::new(Document {
                        original: leaf_bytes,
                        file: Some(leaf_file),
                        ready: ready.clone(),
                        next: None,
                    })),
                }
                .stop_on_shutdown(),
            ),
        };
        let retirements = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let refusals = Arc::new(Mutex::new(Vec::new()));
        let drains = Arc::new(Mutex::new(Vec::new()));
        let definition = Ledgers {
            roots: Mutex::new(BTreeMap::from([(1, live), (2, refused)])),
            retirements: Arc::clone(&retirements),
            failures: Arc::clone(&failures),
            refusals: Arc::clone(&refusals),
            drains: Arc::clone(&drains),
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let constructions = Arc::new(AtomicUsize::new(0));
        let visits = Arc::new(Mutex::new(BTreeMap::new()));
        let cause = Box::new([239, 241]);
        let cause_pointer = ptr::from_ref(cause.as_ref()).cast::<()>() as usize;
        let application = scope
            .with_topology(
                Catalog.stop_on_shutdown(),
                library::Topology::new(LedgerConfig {
                    files: Config {
                        label: Box::from(*b"documents"),
                        constructions: Arc::clone(&constructions),
                        visits: Arc::clone(&visits),
                    },
                    attempts: Arc::clone(&attempts),
                    constructors: Mutex::new(VecDeque::from([
                        Ok(()),
                        Ok(()),
                        Ok(()),
                        Ok(()),
                        Err(cause),
                    ])),
                }),
            )
            .entity_family(
                LedgersRole,
                definition,
                DirectoryConfig::default(),
                EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap()),
            );
        let command = Box::<[u8]>::from([223, 227]);
        let command_pointer = command.as_ptr() as usize;
        let rejected = Box::<[u8]>::from([229, 233]);
        let rejected_pointer = rejected.as_ptr() as usize;
        let (work, root, notifications, (family, ())) = application.run_with_entities(|application| async move {
            let ledgers = application.entities(LedgersRole);
            let caller = application.sender_to(ledgers.entity(1))
                .unwrap_or_else(|_| panic!("the typed Entity recipient is established"));
            caller.send(command).await.expect("the actual hydrated Ledger admits its original command");
            let mut reports = BTreeMap::new();
            for _ in 0..4 {
                let report = tokio::time::timeout(Duration::from_secs(5), replies.receive()).await
                    .expect("all real IO and command replies arrive")
                    .expect("the actual recipient remains open");
                let previous = reports.insert(report.message, report.from);
                assert!(previous.is_none());
            }
            assert_eq!(reports.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([181, 191, 193, 223]));
            assert_eq!(reports[&181], reports[&223]);
            let retired = application.retire_entity(LedgersRole, &1).await;
            assert_eq!(retired, Ok(RetirementMode::Graceful));
            let caller = application.sender_to(ledgers.entity(2))
                .unwrap_or_else(|_| panic!("the refused Entity reference still owns its original target"));
            let rejection = caller.send(rejected).await;
            let Err(AdmissionFailure::Refused { command, reason: Refusal::Unavailable }) = rejection else {
                panic!("a constructor panic returns the original customer command before routable birth");
            };
            assert_eq!(command.as_ptr() as usize, rejected_pointer);
            assert_eq!(&*command, &[229, 233]);
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            reports
        }).await.unwrap_or_else(|_| panic!("the configured Entity application enters its native host")).into_parts();
        let (reports, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("both genuine Entity journeys finish"));
        cleanup.expect("the original application joins the complete Entity family");
        let notifications = notifications.expect("independent root notification results");
        notifications
            .termination
            .expect("root termination publication");
        notifications
            .retirement
            .expect("root retirement publication");
        let (origin, root) = root.expect("independent native root retirement");
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        let ActorRetirement::Completed {
            custody,
            completion,
            settlements,
            interpretation,
            source,
        } = root
        else {
            panic!("the actual application root stops normally");
        };
        assert_eq!(completion, Completion::Stopped);
        assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert_eq!(custody.control.len(), 0);
        assert_eq!(custody.user.len(), 0);
        assert_eq!(custody.descendants.len(), 0);
        assert_eq!(custody.child_failures.len(), 0);
        assert_eq!(custody.capability_failures.len(), 0);
        assert_eq!(custody.additional_failures.len(), 0);
        assert_eq!(custody.retirement_failures.len(), 0);
        assert!(custody.received_interpretation.is_none());
        assert!(custody.received_source.is_none());
        assert!(custody.source_index.is_none());
        assert!(custody.acquired_ingress.is_none());
        assert!(custody.terminal_report.is_none());
        assert!(custody.unread_owner_cancellation.is_none());
        let (LedgersRole, family) =
            family.expect("the independent family result is joined before callback inspection");
        let (shutdown, metrics, disposal) = family.into_parts();
        assert!(disposal.is_none());
        let EntityShutdown::Settled {
            represented,
            entities,
        } = shutdown
        else {
            panic!("all actual family slots retire");
        };
        assert_eq!(represented, 0);
        assert_eq!(entities.len(), 0);
        assert_eq!(metrics.activations, 1);
        assert_eq!(metrics.launch_failures, 1);
        assert_eq!(metrics.hydration_failures, 0);
        assert_eq!(metrics.capacity_refusals, 0);
        assert_eq!(metrics.forced_retirements, 0);
        assert_eq!(metrics.peak_hydrations, 1);
        assert_eq!(metrics.residents, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
        assert_eq!(constructions.load(Ordering::SeqCst), 4);
        assert_eq!(
            *visits.lock().unwrap_or_else(PoisonError::into_inner),
            BTreeMap::from([(1, 1), (2, 1), (3, 1)])
        );
        assert_eq!(
            refusals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len(),
            0
        );
        assert_eq!(
            drains.lock().unwrap_or_else(PoisonError::into_inner).len(),
            0
        );
        let mut retired = retirements.lock().unwrap_or_else(PoisonError::into_inner);
        let (id, live_activation, retirement, report) = retired
            .pop()
            .expect("the original live Ledger reaches its consuming callback");
        assert!(retired.is_empty());
        assert_eq!(id, EntityId::new(1));
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        inspect_ledger(
            retirement.expect("the actual actor task is received"),
            &ready,
            &reports,
            live_pointer,
            command_pointer,
            [(child_pointer, 191), (grandchild_pointer, 193)],
        );
        let mut failures = failures.lock().unwrap_or_else(PoisonError::into_inner);
        let (id, failed_activation, failure) = failures
            .pop()
            .expect("the original constructor cause reaches activation_failed");
        assert!(failures.is_empty());
        assert_eq!(id, EntityId::new(2));
        assert_ne!(failed_activation, live_activation);
        let EntityActivationError::Launch {
            retirement,
            retirement_report,
            termination_notification,
        } = failure
        else {
            panic!(
                "constructor panic preserves native launch custody rather than hydration failure"
            );
        };
        termination_notification.expect("actual failed actor termination publication");
        assert_eq!(
            retirement_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            retirement_report.failures(),
            ActorFailureAssessment::FailuresFound
        );
        let ActorRetirement::HostCommitPanicked {
            custody,
            initialization: Some(InterpretationProgress::Original(actions)),
            payload,
        } = retirement
        else {
            panic!("constructor panic occurs before binding ACK and all original actions survive");
        };
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>() as usize,
            cause_pointer
        );
        let ledger = custody.behavior.base();
        assert_eq!(ledger.original.as_ptr() as usize, refused_pointer);
        assert_eq!(&*ledger.original, &[197]);
        assert_eq!(ledger.ready, ready);
        assert_eq!(ledger.commands.len(), 0);
        assert!(ledger.file.is_none());
        assert!(ledger.document.is_none());
        assert_eq!(custody.control.len(), 0);
        assert_eq!(custody.user.len(), 0);
        assert_eq!(custody.descendants.len(), 0);
        assert_eq!(custody.child_failures.len(), 0);
        assert_eq!(custody.capability_failures.len(), 0);
        assert_eq!(custody.additional_failures.len(), 0);
        assert_eq!(custody.retirement_failures.len(), 0);
        assert!(custody.received_interpretation.is_none());
        assert!(custody.received_source.is_none());
        assert!(custody.source_index.is_none());
        assert!(custody.acquired_ingress.is_none());
        assert!(custody.terminal_report.is_none());
        assert!(custody.unread_owner_cancellation.is_none());
        assert!(matches!(actions.become_, Step::Continue));
        assert_eq!(actions.sends.owned, NoSends);
        let mut files = actions.sends.inner.files.into_iter();
        let file = files
            .next()
            .expect("the original hydrated File request remains unopened");
        assert!(files.next().is_none());
        assert_eq!(file.path, expected_refused_path);
        assert_eq!(
            file.path.as_os_str().as_encoded_bytes().as_ptr() as usize,
            refused_path_pointer
        );
        assert!(matches!(file.access, FileAccess::Ready));
        let mut notices = actions.sends.inner.ready.into_iter();
        let notice = notices
            .next()
            .expect("the original ready request was never interpreted");
        assert!(notices.next().is_none());
        assert_eq!(notice.to, ready);
        assert_eq!(notice.message, 197);
        let mut unborn = actions.creates.into_iter();
        let document = unborn
            .next()
            .expect("the original uncommitted child stays in Actions");
        assert!(unborn.next().is_none());
        let (_, document, kind) = document.into_parts();
        assert_eq!(kind, CreationKind::Birth);
        let document = document.base();
        assert_eq!(document.original.as_ptr() as usize, unborn_pointer);
        assert_eq!(&*document.original, &[199]);
        assert_eq!(document.ready, ready);
        let file = document
            .file
            .as_ref()
            .expect("unborn child still owns its unopened file");
        assert_eq!(file.path, expected_unborn_path);
        assert_eq!(
            file.path.as_os_str().as_encoded_bytes().as_ptr() as usize,
            unborn_path_pointer
        );
        assert!(matches!(file.access, FileAccess::Ready));
        let leaf = document
            .next
            .as_ref()
            .expect("the exact unopened grandchild remains with its parent");
        assert_eq!(leaf.original.as_ptr() as usize, leaf_pointer);
        assert_eq!(&*leaf.original, &[211]);
        assert_eq!(leaf.ready, ready);
        assert!(leaf.next.is_none());
        let file = leaf
            .file
            .as_ref()
            .expect("unborn grandchild still owns its unopened file");
        assert_eq!(file.path, expected_leaf_path);
        assert_eq!(
            file.path.as_os_str().as_encoded_bytes().as_ptr() as usize,
            leaf_path_pointer
        );
        assert!(matches!(file.access, FileAccess::Ready));
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "inspect complete owned Entity and descendant custody against independent sender/original-value observations"
)]
fn inspect_ledger(
    retirement: LedgerRetirement,
    ready: &EstablishedRecipient<DocumentReady>,
    reports: &BTreeMap<u8, MailAddr>,
    original: usize,
    command: usize,
    children: [(usize, u8); 2],
) {
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!(
            "on this current-thread host, the owning Entity task requests cancellation before the actor can consume queued shutdown"
        );
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let ledger = custody.behavior.base();
    assert_eq!(ledger.original.as_ptr() as usize, original);
    assert_eq!(&*ledger.original, &[181]);
    assert_eq!(&ledger.ready, ready);
    assert!(ledger.file.is_none());
    assert!(ledger.document.is_none());
    assert_eq!(ledger.commands.len(), 1);
    assert_eq!(ledger.commands[0].as_ptr() as usize, command);
    assert_eq!(&*ledger.commands[0], &[223, 227]);
    assert!(matches!(
        custody.control.as_slice(),
        [EventLayer::Owned(ShutdownRequested)]
    ));
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    let mut files = 0;
    let mut births = Vec::new();
    for settled in settlements {
        assert_eq!(settled.sends.owned, NoSends);
        assert_eq!(settled.sends.inner.ready.len(), 0);
        for file in settled.sends.inner.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("the actual Ledger File remains acquired");
            };
            inspect_file(file);
            files += 1;
        }
        let CreationSettlement::Settled(creations) = settled.creations.into_settlement() else {
            panic!("the original birth lane settled");
        };
        for creation in creations {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::Established(child),
            )) = creation
            else {
                panic!("the child has an actual established receipt");
            };
            births.push((child.id(), child.kind()));
        }
    }
    assert_eq!(files, 1);
    let mut descendants = custody.descendants.into_iter();
    let library::LedgersRetirements { origin, retirement } = descendants
        .next()
        .expect("the exact child retirement remains owned");
    assert!(descendants.next().is_none());
    assert_eq!(births, [(origin.creation_id(), origin.creation_kind())]);
    assert_eq!(origin.creation_kind(), CreationKind::Birth);
    assert_eq!(reports[&191], origin.address());
    let mut addresses = BTreeSet::from([MailAddr::APPLICATION_ROOT, reports[&181]]);
    let fresh = addresses.insert(origin.address());
    assert!(fresh);
    let mut retirement = *retirement;
    let mut expected = children.into_iter();
    loop {
        let (custody, settlements) = match retirement {
            ActorRetirement::OwnerCancelled {
                custody,
                settlements,
                interpretation,
                source,
            } => {
                assert!(interpretation.is_none());
                assert!(source.is_none());
                (custody, settlements)
            }
            _ => panic!("every original recursive child has complete joined retirement"),
        };
        let (original, value) = expected.next().expect("no fabricated child retirement");
        let document = custody.behavior.base();
        assert_eq!(document.original.as_ptr() as usize, original);
        assert_eq!(&*document.original, &[value]);
        assert_eq!(&document.ready, ready);
        assert!(document.file.is_none());
        assert!(document.next.is_none());
        assert_eq!(custody.control.len(), 0);
        assert_eq!(custody.user.len(), 0);
        assert_eq!(custody.child_failures.len(), 0);
        assert_eq!(custody.capability_failures.len(), 0);
        assert_eq!(custody.additional_failures.len(), 0);
        assert_eq!(custody.retirement_failures.len(), 0);
        assert!(custody.received_interpretation.is_none());
        assert!(custody.received_source.is_none());
        assert!(custody.source_index.is_none());
        assert!(custody.acquired_ingress.is_none());
        assert!(custody.terminal_report.is_none());
        assert!(custody.unread_owner_cancellation.is_none());
        assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
        let mut files = 0;
        let mut births = Vec::new();
        for settled in settlements {
            assert_eq!(settled.sends.owned, NoSends);
            assert_eq!(settled.sends.inner.ready.len(), 0);
            for file in settled.sends.inner.files {
                let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                    panic!("the actual descendant File remains acquired");
                };
                inspect_file(file);
                files += 1;
            }
            let CreationSettlement::Settled(creations) = settled.creations.into_settlement() else {
                panic!("all recursive births settled");
            };
            for creation in creations {
                let SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(child),
                )) = creation
                else {
                    panic!("every recursive birth has its original established receipt");
                };
                births.push((child.id(), child.kind()));
            }
        }
        assert_eq!(files, 1);
        let mut descendants = custody.descendants.into_iter();
        let Some(library::DocumentRetirements {
            origin,
            retirement: child,
        }) = descendants.next()
        else {
            assert_eq!(births.len(), 0);
            assert!(expected.next().is_none());
            assert_eq!(addresses.len(), 4);
            return;
        };
        assert!(descendants.next().is_none());
        assert_eq!(births, [(origin.creation_id(), origin.creation_kind())]);
        assert_eq!(origin.creation_kind(), CreationKind::Birth);
        assert_eq!(reports[&193], origin.address());
        let fresh = addresses.insert(origin.address());
        assert!(fresh);
        retirement = *child;
    }
}
