use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, ClassifySettlement, CreateChild, CreationSequence,
    Creations, Never, SettlementStatus,
};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId, EntityShutdown, Passivation,
};
use bombay::prelude::{ActorRetirement, Completion, StopOnShutdown};
use bombay::{
    ActorFailureAssessment, ActorRetirementReport, Application, MailAddr, RetirementAssessment,
    RetirementSchema,
};
use core::num::NonZeroUsize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::Semaphore;

struct Root;
#[bombay::actor(message = Never)]
impl Root {}

#[derive(Debug, PartialEq)]
enum TreeRefusal {
    Initialization,
}
enum TreeStartup {
    Ready,
    Refuse,
}
struct Tree {
    startup: TreeStartup,
    payload: Box<[u8]>,
    next: Option<Box<Tree>>,
    commands: Vec<u64>,
}
#[bombay::actor(
    error = TreeRefusal,
    births = { next: StopOnShutdown<Tree> },
    creation_settlements = retain_for_retirement,
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor transition owns a typed Actions success or error carrier"
)]
impl Tree {
    fn init(&mut self) -> BehaviorActed<Self> {
        match self.startup {
            TreeStartup::Refuse => Err(TreeRefusal::Initialization),
            TreeStartup::Ready => {
                let Some(next) = self.next.take() else {
                    return Ok(Actions::cont());
                };
                let mut sequence = CreationSequence::new();
                let id = sequence.issue().expect("one fresh child request");
                Ok(Actions::create(Creations::one(CreateChild::birth(
                    id,
                    (*next).stop_on_shutdown(),
                ))))
            }
        }
    }
    fn receive(&mut self, command: u64) -> BehaviorActed<Self> {
        self.commands.push(command);
        Ok(Actions::cont())
    }
}

type TreeRetirement = ActorRetirement<
    <Trees as EntityDefinition>::Behavior,
    tree_application::TreeRetirements,
    Vec<<tree_application::TreeRetirements as RetirementSchema>::Failure>,
>;
type TreeActivationFailure = EntityActivationError<
    u64,
    <Trees as EntityDefinition>::Behavior,
    tree_application::TreeRetirements,
    Vec<<tree_application::TreeRetirements as RetirementSchema>::Failure>,
>;
type TreeRetirementFacts = Arc<
    Mutex<
        Vec<(
            EntityId<u64>,
            Result<TreeRetirement, tokio::task::JoinError>,
            ActorRetirementReport,
        )>,
    >,
>;
type TreeActivationFacts = Arc<Mutex<Vec<(EntityId<u64>, TreeActivationFailure)>>>;
struct Trees {
    roots: Mutex<BTreeMap<u64, Tree>>,
    retirements: TreeRetirementFacts,
    failures: TreeActivationFacts,
    refusals: Arc<Mutex<Vec<AdmissionFailure<u64>>>>,
    drains: Arc<Mutex<Vec<DrainFailure>>>,
    retired: Arc<Semaphore>,
}
impl EntityDefinition for Trees {
    type Id = u64;
    type Behavior = StopOnShutdown<Tree>;
    type Hosts = tree_application::Topology;
    type HydrationError = u64;
    type Terminal = tree_application::TreeRetirements;
    type ChildFailures = Vec<<Self::Terminal as RetirementSchema>::Failure>;
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "transfer the exact pre-owned tree only when hydration is polled"
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
        _: ActivationId,
        failure: TreeActivationFailure,
    ) {
        self.failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, failure));
    }
    fn admission_refused(&self, _: EntityId<u64>, failure: AdmissionFailure<u64>) {
        self.refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure);
    }
    fn forced_retirement(&self, _: &EntityId<u64>, _: ActivationId, failure: DrainFailure) {
        self.drains
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(failure);
    }
    fn retired(
        &self,
        id: &EntityId<u64>,
        _: ActivationId,
        retirement: Result<TreeRetirement, tokio::task::JoinError>,
        retirement_report: ActorRetirementReport,
    ) {
        self.retirements
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, retirement, retirement_report));
        self.retired.add_permits(1);
    }
}
#[derive(Clone, Copy)]
struct TreesRole;
bombay::topology! {
    tree_application {
        root: Root {}
        hosts { trees: Tree; }
        families { tree: Trees; }
        tree { Next(TreeChildrenNext) => tree; }
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one complete recursive Entity custody journey preserves every owned actor and independent failure"
)]
fn recursive_hydration_preserves_each_actor_and_original_launch_refusal() {
    let root_payload: Box<[u8]> = Box::from([11]);
    let root_pointer = root_payload.as_ptr();
    let refused_payload: Box<[u8]> = Box::from([13, 17]);
    let refused_pointer = refused_payload.as_ptr();
    let leaf_payload: Box<[u8]> = Box::from([3]);
    let leaf_pointer = leaf_payload.as_ptr();
    let leaf = Tree {
        startup: TreeStartup::Ready,
        payload: leaf_payload,
        next: None,
        commands: vec![],
    };
    let branch_payload: Box<[u8]> = Box::from([5]);
    let branch_pointer = branch_payload.as_ptr();
    let branch = Tree {
        startup: TreeStartup::Ready,
        payload: branch_payload,
        next: Some(Box::new(leaf)),
        commands: vec![],
    };
    let accepted = Tree {
        startup: TreeStartup::Ready,
        payload: root_payload,
        next: Some(Box::new(branch)),
        commands: vec![],
    };
    let refused = Tree {
        startup: TreeStartup::Refuse,
        payload: refused_payload,
        next: None,
        commands: vec![],
    };
    let retirements = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let refusals = Arc::new(Mutex::new(Vec::new()));
    let drains = Arc::new(Mutex::new(Vec::new()));
    let retired = Arc::new(Semaphore::new(0));
    let definition = Trees {
        roots: Mutex::new(BTreeMap::from([(1, accepted), (2, refused)])),
        retirements: Arc::clone(&retirements),
        failures: Arc::clone(&failures),
        refusals: Arc::clone(&refusals),
        drains: Arc::clone(&drains),
        retired: Arc::clone(&retired),
    };
    let application =
        Application::with_topology(Root.stop_on_shutdown(), tree_application::Topology::new())
            .entity_family(
                TreesRole,
                definition,
                DirectoryConfig::default(),
                EntityCapacity::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(2).unwrap()),
            );
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (outcome, root, notifications, (family, ())) = host
        .block_on(application.run_with_entities(|application| async move {
            let trees = application.entities(TreesRole);
            let caller = application
                .sender_to(trees.entity(1))
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("original target retained");
                });
            caller
                .send(19)
                .await
                .expect("actual hydrated root accepts its command");
            let passivation = application.passivate_entity(TreesRole, &1);
            assert_eq!(passivation, Passivation::Begun);
            retired.acquire().await.unwrap().forget();
            let refused = application
                .sender_to(trees.entity(2))
                .unwrap_or_else(|original| {
                    drop(original);
                    panic!("original target retained");
                });
            let rejected = refused.send(23).await;
            match rejected {
                Err(AdmissionFailure::Refused {
                    command,
                    reason: bombay::entity::Refusal::Unavailable,
                }) => assert_eq!(command, 23),
                other => panic!("expected complete launch rejection, got {other:?}"),
            }
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
        }))
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered host");
        })
        .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("complete work");
    });
    assert!(cleanup.is_ok());
    let (origin, root) = root.expect("independent root receiving");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    let notifications = notifications.expect("independent original root notifications");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (TreesRole, family_retirement) = family.expect("independent family receiving");
    let (shutdown, metrics, disposal) = family_retirement.into_parts();
    assert!(disposal.is_none());
    assert!(matches!(
        shutdown,
        EntityShutdown::Settled { represented: 0, .. }
    ));
    assert_eq!(metrics.activations, 1);
    assert_eq!(metrics.launch_failures, 1);
    assert_eq!(metrics.residents, 0);
    assert!(
        refusals
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        drains
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    );
    let mut failures = failures.lock().unwrap_or_else(PoisonError::into_inner);
    let (
        id,
        EntityActivationError::Launch {
            retirement: ActorRetirement::InitializationRejected { custody, error },
            retirement_report,
            termination_notification,
        },
    ) = failures.pop().expect("original launch refusal")
    else {
        panic!("exact root refusal");
    };
    assert_eq!(id, EntityId::new(2));
    assert_eq!(error, TreeRefusal::Initialization);
    assert_eq!(
        retirement_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        retirement_report.failures(),
        ActorFailureAssessment::FailuresFound
    );
    assert!(termination_notification.is_ok());
    assert_eq!(custody.behavior.base().payload.as_ptr(), refused_pointer);
    assert_eq!(&*custody.behavior.base().payload, &[13, 17]);
    assert!(custody.descendants.is_empty());
    assert!(custody.child_failures.is_empty());
    assert!(failures.is_empty());
    let mut retirements = retirements.lock().unwrap_or_else(PoisonError::into_inner);
    let (id, retirement, retirement_report) =
        retirements.pop().expect("actual family tree retirement");
    assert_eq!(id, EntityId::new(1));
    assert_eq!(
        retirement_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        retirement_report.failures(),
        ActorFailureAssessment::NoFailuresFound
    );
    assert!(retirements.is_empty());
    let mut retirement = retirement.expect("native root result");
    assert_eq!(
        retirement.behavior().unwrap().base().payload.as_ptr(),
        root_pointer
    );
    assert_eq!(retirement.behavior().unwrap().base().commands, [19]);
    let mut addresses = BTreeSet::new();
    let mut values = Vec::new();
    let mut pointers = Vec::new();
    loop {
        let custody = match retirement {
            ActorRetirement::Completed {
                custody,
                interpretation,
                source,
                settlements,
                completion,
            } => {
                assert_eq!(completion, Completion::Stopped);
                assert!(interpretation.is_none());
                assert!(source.is_none());
                assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
                custody
            }
            ActorRetirement::OwnerCancelled {
                custody,
                interpretation,
                source,
                settlements,
            } => {
                assert!(interpretation.is_none());
                assert!(source.is_none());
                assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
                custody
            }
            _ => panic!("complete recursive retirement required"),
        };
        assert!(custody.child_failures.is_empty());
        assert!(custody.additional_failures.is_empty());
        assert!(custody.capability_failures.is_empty());
        assert!(custody.retirement_failures.is_empty());
        values.push(custody.behavior.base().payload[0]);
        pointers.push(custody.behavior.base().payload.as_ptr());
        assert!(custody.received_interpretation.is_none());
        assert!(custody.received_source.is_none());
        assert!(custody.source_index.is_none());
        assert!(custody.acquired_ingress.is_none());
        assert!(custody.terminal_report.is_none());
        assert_eq!(custody.user, []);
        let mut descendants = custody.descendants;
        if descendants.is_empty() {
            break;
        }
        assert_eq!(descendants.len(), 1);
        let tree_application::TreeRetirements {
            origin,
            retirement: child,
        } = descendants.pop().unwrap();
        let fresh = addresses.insert(origin.address());
        assert!(fresh);
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(
            origin.creation_kind(),
            bombay::behavior::CreationKind::Birth
        );
        retirement = *child;
    }
    assert_eq!(values, [11, 5, 3]);
    assert_eq!(pointers, [root_pointer, branch_pointer, leaf_pointer]);
    assert_eq!(addresses.len(), 2);
}
