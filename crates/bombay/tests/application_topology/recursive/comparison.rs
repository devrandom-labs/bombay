use super::{
    ManagedLeaf, ManagedNode, Node, NodeError, NodeStartup, Root, RootStartup, Trace, garden,
    root_deadline_reached, stopped_root_deadline_reached,
};
use bombay::actors::ActorExt;
use bombay::behavior::{
    Behavior, BehaviorBase, BehaviorSettlements, ChildCreationOutcome, ChildHead, ChildTail,
    ClassifySettlement, CreateChild, CreationId, CreationKind, CreationRejection, CreationSequence,
    CreationSettlement, ItemSettlement, Never, Protocol, RetirementBirths, RoutedCreation,
    SettledItem, SettlementStatus,
};
use bombay::lifecycle::StopOnShutdown;
use bombay::prelude::{Exit, TimerId};
use bombay::{
    ActorCustody, ActorRetirement, ActorSpace, Application, ApplicationTopology, ChildFailure,
    ChildOrigin, Completion, EstablishNativeChild, MailAddr, NativeChild, NativeHost, PendingChild,
    ProjectTerminal, RetirementSchema, RootRegistration, Runtime, WorkOutcome,
};
use core::future::Future;
use core::ops::ControlFlow;
use core::time::Duration;
use std::collections::HashSet;

struct GardenSpaces {
    root: ActorSpace<<Root as Behavior>::Protocol>,
}

impl NativeHost for GardenSpaces {
    type Runtime = Runtime;
    fn runtime(&self) -> &Self::Runtime {
        const RUNTIME: Runtime = Runtime::new();
        &RUNTIME
    }
}
struct BranchRetirement {
    origin: ChildOrigin<Root, ChildHead>,
    retirement: Box<ActorRetirement<ManagedNode, NodeRetirement, Vec<NodeFailure>>>,
}
enum NodeRetirement {
    Next {
        origin: ChildOrigin<Node, ChildHead>,
        retirement: Box<ActorRetirement<ManagedNode, NodeRetirement, Vec<NodeFailure>>>,
    },
    Audit {
        origin: ChildOrigin<Node, ChildTail<ChildHead>>,
        retirement: Box<ActorRetirement<ManagedLeaf, Never, Vec<Never>>>,
    },
}
enum NodeFailure {
    Next(ChildFailure<ChildOrigin<Node, ChildHead>, ManagedNode>),
    Audit(ChildFailure<ChildOrigin<Node, ChildTail<ChildHead>>, ManagedLeaf>),
}
impl From<ChildFailure<ChildOrigin<Node, ChildHead>, ManagedNode>> for NodeFailure {
    fn from(original: ChildFailure<ChildOrigin<Node, ChildHead>, ManagedNode>) -> Self {
        Self::Next(original)
    }
}
impl From<ChildFailure<ChildOrigin<Node, ChildTail<ChildHead>>, ManagedLeaf>> for NodeFailure {
    fn from(original: ChildFailure<ChildOrigin<Node, ChildTail<ChildHead>>, ManagedLeaf>) -> Self {
        Self::Audit(original)
    }
}
impl RetirementSchema for NodeRetirement {
    type Failure = NodeFailure;
}
impl RetirementSchema for BranchRetirement {
    type Failure = ChildFailure<ChildOrigin<Root, ChildHead>, ManagedNode>;
}
impl RootRegistration<<Root as Behavior>::Protocol> for GardenSpaces {
    fn root_space(&self) -> &ActorSpace<<Root as Behavior>::Protocol> {
        &self.root
    }
}
impl<C> ApplicationTopology<C, ()> for GardenSpaces
where
    C: BehaviorBase<Base = Root>
        + Behavior<Protocol = <Root as Behavior>::Protocol, Birth = RetirementBirths<ManagedNode>>,
{
    type Descendants = BranchRetirement;
}
impl ProjectTerminal<ChildOrigin<Root, ChildHead>, ManagedNode> for BranchRetirement {
    type Descendants = NodeRetirement;
    fn project(
        origin: ChildOrigin<Root, ChildHead>,
        retirement: ActorRetirement<ManagedNode, NodeRetirement, Vec<NodeFailure>>,
    ) -> Self {
        Self {
            origin,
            retirement: Box::new(retirement),
        }
    }
}
impl ProjectTerminal<ChildOrigin<Node, ChildHead>, ManagedNode> for NodeRetirement {
    type Descendants = NodeRetirement;
    fn project(
        origin: ChildOrigin<Node, ChildHead>,
        retirement: ActorRetirement<ManagedNode, NodeRetirement, Vec<NodeFailure>>,
    ) -> Self {
        Self::Next {
            origin,
            retirement: Box::new(retirement),
        }
    }
}
impl ProjectTerminal<ChildOrigin<Node, ChildTail<ChildHead>>, ManagedLeaf> for NodeRetirement {
    type Descendants = Never;
    fn project(
        origin: ChildOrigin<Node, ChildTail<ChildHead>>,
        retirement: ActorRetirement<ManagedLeaf, Never, Vec<Never>>,
    ) -> Self {
        Self::Audit {
            origin,
            retirement: Box::new(retirement),
        }
    }
}

impl<C> NativeChild<C, ChildHead, ManagedNode, BranchRetirement, NodeRetirement> for GardenSpaces
where
    C: BehaviorBase<Base = Root>
        + Behavior<Protocol = <Root as Behavior>::Protocol, Birth = RetirementBirths<ManagedNode>>,
    Self: EstablishNativeChild<C, ManagedNode, NodeRetirement>,
{
    fn registration(&self) -> ActorSpace<<ManagedNode as Behavior>::Protocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            C,
            Self,
            ChildHead,
            ManagedNode,
            BranchRetirement,
            NodeRetirement,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedNode>,
            ChildCreationOutcome<ManagedNode, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        BranchRetirement:
            ProjectTerminal<Origin, ManagedNode, Descendants = NodeRetirement> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<C, ManagedNode, NodeRetirement>>::establish_native_child::<
            ChildHead,
            BranchRetirement,
            Origin,
            OriginFn,
        >(pending)
    }
}

impl NativeChild<ManagedNode, ChildHead, ManagedNode, NodeRetirement, NodeRetirement>
    for GardenSpaces
{
    fn registration(&self) -> ActorSpace<<ManagedNode as Behavior>::Protocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            ManagedNode,
            Self,
            ChildHead,
            ManagedNode,
            NodeRetirement,
            NodeRetirement,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedNode>,
            ChildCreationOutcome<ManagedNode, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        NodeRetirement:
            ProjectTerminal<Origin, ManagedNode, Descendants = NodeRetirement> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<ManagedNode, ManagedNode, NodeRetirement>>::establish_native_child::<
            ChildHead, NodeRetirement, Origin, OriginFn,
        >(pending)
    }
}

impl NativeChild<ManagedNode, ChildTail<ChildHead>, ManagedLeaf, NodeRetirement, Never>
    for GardenSpaces
{
    fn registration(&self) -> ActorSpace<<ManagedLeaf as Behavior>::Protocol> {
        ActorSpace::new()
    }
    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            ManagedNode,
            Self,
            ChildTail<ChildHead>,
            ManagedLeaf,
            NodeRetirement,
            Never,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedLeaf>,
            ChildCreationOutcome<ManagedLeaf, ChildTail<ChildHead>>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        NodeRetirement: ProjectTerminal<Origin, ManagedLeaf, Descendants = Never> + Send + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<ManagedNode, ManagedLeaf, Never>>::establish_native_child::<
            ChildTail<ChildHead>,
            NodeRetirement,
            Origin,
            OriginFn,
        >(pending)
    }
}

#[derive(Debug, PartialEq)]
enum GardenRetirement {
    Node {
        trace: Trace,
        readiness: Vec<Vec<u8>>,
        children: Vec<Self>,
    },
    Audit {
        payload: Vec<u8>,
    },
}
fn inspect_clean_custody<B, D, F>(custody: &ActorCustody<B, D, Vec<F>>)
where
    B: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    assert!(custody.child_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.user.is_empty());
    // Read cancellation/exhaustion lanes independently, without comparing scheduler choices.
    if let Some(ControlFlow::Break(original)) = &custody.acquired_ingress {
        assert!(original.is_ok());
    }
}
fn observe_node<D, F>(retirement: &ActorRetirement<ManagedNode, D, Vec<F>>) -> (&Node, &Vec<D>) {
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the root owns the complete cancelled recursive node");
    };
    inspect_clean_custody(custody);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.behavior.base().pending.is_empty());
    for settlement in settlements {
        assert_eq!(settlement.settlement_status(), SettlementStatus::Accepted);
    }
    (custody.behavior.base(), &custody.descendants)
}
fn observe_audit(retirement: &ActorRetirement<ManagedLeaf, Never, Vec<Never>>) -> GardenRetirement {
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("both graphs own the original audit actor");
    };
    inspect_clean_custody(custody);
    assert_eq!(custody.descendants.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert!(settlements.is_empty());
    GardenRetirement::Audit {
        payload: custody.behavior.base().payload.to_vec(),
    }
}
fn observe_origin<Owner, Role>(
    origin: &ChildOrigin<Owner, Role>,
    addresses: &mut HashSet<MailAddr>,
) {
    let unique = addresses.insert(origin.address());
    assert!(
        unique,
        "each actor retains a distinct native binding inside its own run"
    );
    assert_eq!(origin.creation_kind(), CreationKind::Birth);
}
fn manual_nodes(
    retirement: &ActorRetirement<ManagedNode, NodeRetirement, Vec<NodeFailure>>,
    addresses: &mut HashSet<MailAddr>,
) -> GardenRetirement {
    if let ActorRetirement::OwnerCancelled { custody, .. } = retirement
        && let Some(original) = custody.child_failures.first()
    {
        match original {
            NodeFailure::Next(original) => {
                panic!("unexpected exact Next failure: {original:?}")
            }
            NodeFailure::Audit(original) => {
                panic!("unexpected exact Audit failure: {original:?}")
            }
        }
    }
    let (node, children) = observe_node(retirement);
    let children = children
        .iter()
        .map(|child| match child {
            NodeRetirement::Next { origin, retirement } => {
                observe_origin(origin, addresses);
                manual_nodes(retirement, addresses)
            }
            NodeRetirement::Audit { origin, retirement } => {
                observe_origin(origin, addresses);
                observe_audit(retirement)
            }
        })
        .collect();
    let mut readiness: Vec<_> = node
        .readiness
        .iter()
        .map(|original| original.to_vec())
        .collect();
    readiness.sort();
    GardenRetirement::Node {
        trace: Trace {
            nodes: node.trace.nodes,
            audits: node.trace.audits,
        },
        readiness,
        children,
    }
}
fn generated_nodes(
    retirement: &ActorRetirement<
        ManagedNode,
        garden::NodeRetirements,
        Vec<garden::NodeChildFailure>,
    >,
    addresses: &mut HashSet<MailAddr>,
) -> GardenRetirement {
    let (node, children) = observe_node(retirement);
    let children = children
        .iter()
        .map(|child| match child {
            garden::NodeRetirements::Next { origin, retirement } => {
                observe_origin(origin, addresses);
                generated_nodes(retirement, addresses)
            }
            garden::NodeRetirements::Audit { origin, retirement } => {
                observe_origin(origin, addresses);
                observe_audit(retirement)
            }
        })
        .collect();
    let mut readiness: Vec<_> = node
        .readiness
        .iter()
        .map(|original| original.to_vec())
        .collect();
    readiness.sort();
    GardenRetirement::Node {
        trace: Trace {
            nodes: node.trace.nodes,
            audits: node.trace.audits,
        },
        readiness,
        children,
    }
}
fn inspect_root<B, D, F>(retirement: &ActorRetirement<B, D, Vec<F>>) -> &Vec<D>
where
    B: BehaviorBase<Base = Root>
        + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Settlements: ClassifySettlement,
{
    let ActorRetirement::Completed {
        custody,
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the original root must complete its actual reports");
    };
    inspect_clean_custody(custody);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(
        custody.behavior.base().trace,
        Some(Trace {
            nodes: 4,
            audits: 3
        })
    );
    assert_eq!(custody.behavior.base().readiness.len(), 1);
    assert_eq!(custody.behavior.base().readiness[0].as_ref(), &[197, 199]);
    assert_eq!(completion, &Completion::Stopped);
    &custody.descendants
}
fn growing_garden_root() -> Root {
    Root {
        startup: Some(RootStartup::Grow { remaining: 3 }),
        trace: None,
        readiness: Vec::new(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn generated_recursive_garden_matches_manual_native_custody() {
    let generated = Application::with_topology(
        growing_garden_root()
            .with_one_shot(TimerId(37), Duration::from_secs(10), root_deadline_reached)
            .stop_on_shutdown(),
        garden::Topology::new(),
    )
    .run_with(|application| async move { application.termination().await })
    .await;
    let manual = Application::with_topology(
        growing_garden_root()
            .with_one_shot(TimerId(37), Duration::from_secs(10), root_deadline_reached)
            .stop_on_shutdown(),
        GardenSpaces {
            root: ActorSpace::new(),
        },
    )
    .run_with(|application| async move { application.termination().await })
    .await;
    let (generated_work, generated_native, generated_notifications, ()) = generated
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the existing generated garden starts");
        })
        .into_parts();
    let (manual_work, manual_native, manual_notifications, ()) = manual
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the handwritten public garden starts");
        })
        .into_parts();
    let (generated_termination, generated_cleanup) = generated_work
        .try_into_completed()
        .unwrap_or_else(|original| {
            drop(original);
            panic!("generated garden Work remains acquired");
        });
    let (manual_termination, manual_cleanup) =
        manual_work.try_into_completed().unwrap_or_else(|original| {
            drop(original);
            panic!("handwritten garden Work remains acquired");
        });
    for cleanup in [generated_cleanup, manual_cleanup] {
        assert!(cleanup.is_ok());
    }
    assert_eq!(generated_termination, Ok(Exit::Normal));
    assert_eq!(manual_termination, Ok(Exit::Normal));
    for notifications in [generated_notifications, manual_notifications] {
        let notifications = notifications.unwrap();
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    }
    let (generated_origin, generated_native) = generated_native.unwrap();
    let (manual_origin, manual_native) = manual_native.unwrap();
    let generated_address = generated_origin.address();
    let manual_address = manual_origin.address();
    compare_garden_retirements(
        (generated_address, &generated_native),
        (manual_address, &manual_native),
    );
}

#[expect(
    clippy::type_complexity,
    reason = "compare both complete native products without replacing their distinct owning schemas"
)]
fn compare_garden_retirements<B>(
    generated: (
        MailAddr,
        &ActorRetirement<
            B,
            garden::RootRetirements,
            Vec<ChildFailure<ChildOrigin<Root, ChildHead>, ManagedNode>>,
        >,
    ),
    manual: (
        MailAddr,
        &ActorRetirement<
            B,
            BranchRetirement,
            Vec<ChildFailure<ChildOrigin<Root, ChildHead>, ManagedNode>>,
        >,
    ),
) where
    B: BehaviorBase<Base = Root>
        + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    B::Settlements: ClassifySettlement,
{
    let generated_children = inspect_root(generated.1);
    let manual_children = inspect_root(manual.1);
    assert_eq!(generated_children.len(), 1);
    assert_eq!(manual_children.len(), 1);
    let mut generated_addresses = HashSet::from([generated.0]);
    let mut manual_addresses = HashSet::from([manual.0]);
    observe_origin(&generated_children[0].origin, &mut generated_addresses);
    observe_origin(&manual_children[0].origin, &mut manual_addresses);
    let generated = generated_nodes(&generated_children[0].retirement, &mut generated_addresses);
    let manual = manual_nodes(&manual_children[0].retirement, &mut manual_addresses);
    assert_eq!(
        generated, manual,
        "generated recursion must preserve every exact role/state/report"
    );
    assert_eq!(generated_addresses.len(), 8);
    assert_eq!(manual_addresses.len(), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn generated_outer_timer_matches_manual_native_custody() {
    let generated = Application::with_topology(
        growing_garden_root().stop_on_shutdown().with_one_shot(
            TimerId(37),
            Duration::from_secs(10),
            stopped_root_deadline_reached,
        ),
        garden::Topology::new(),
    )
    .run_with(|application| async move { application.termination().await })
    .await;
    let manual = Application::with_topology(
        growing_garden_root().stop_on_shutdown().with_one_shot(
            TimerId(37),
            Duration::from_secs(10),
            stopped_root_deadline_reached,
        ),
        GardenSpaces {
            root: ActorSpace::new(),
        },
    )
    .run_with(|application| async move { application.termination().await })
    .await;
    let (generated_work, generated_native, generated_notifications, ()) = generated
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the generated outer timer starts");
        })
        .into_parts();
    let (manual_work, manual_native, manual_notifications, ()) = manual
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the handwritten outer timer starts");
        })
        .into_parts();
    let (generated_termination, generated_cleanup) = generated_work
        .try_into_completed()
        .unwrap_or_else(|original| {
            drop(original);
            panic!("generated outer-timer Work remains acquired");
        });
    let (manual_termination, manual_cleanup) =
        manual_work.try_into_completed().unwrap_or_else(|original| {
            drop(original);
            panic!("handwritten outer-timer Work remains acquired");
        });
    for cleanup in [generated_cleanup, manual_cleanup] {
        assert!(cleanup.is_ok());
    }
    assert_eq!(generated_termination, Ok(Exit::Normal));
    assert_eq!(manual_termination, Ok(Exit::Normal));
    for notifications in [generated_notifications, manual_notifications] {
        let notifications = notifications.unwrap();
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    }
    let (generated_origin, generated_native) = generated_native.unwrap();
    let (manual_origin, manual_native) = manual_native.unwrap();
    let generated_address = generated_origin.address();
    let manual_address = manual_origin.address();
    compare_garden_retirements(
        (generated_address, &generated_native),
        (manual_address, &manual_native),
    );
}

fn refused_garden_root() -> (Root, CreationId, *const u8) {
    let payload: Box<[u8]> = Box::from([7, 13]);
    let allocation = payload.as_ptr();
    let mut sequence = CreationSequence::new();
    let id = sequence
        .issue()
        .expect("the actual singleton creation fits");
    let creation = CreateChild::birth(
        id,
        Node {
            startup: NodeStartup::Refuse { payload },
            pending: HashSet::new(),
            trace: Trace {
                nodes: 1,
                audits: 0,
            },
            readiness: Vec::new(),
        }
        .stop_on_shutdown(),
    );
    (
        Root {
            startup: Some(RootStartup::Refuse { creation }),
            trace: None,
            readiness: Vec::new(),
        },
        id,
        allocation,
    )
}
fn inspect_refused_garden<D, F>(
    retirement: ActorRetirement<StopOnShutdown<Root>, D, Vec<F>>,
    original_id: CreationId,
    allocation: *const u8,
) -> Vec<u8> {
    let ActorRetirement::Completed {
        custody,
        settlements,
        interpretation,
        source,
        ..
    } = retirement
    else {
        panic!("the original finite refused creation remains in root custody");
    };
    inspect_clean_custody(&custody);
    assert_eq!(custody.descendants.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let mut refused = Vec::new();
    for action in settlements {
        let CreationSettlement::Settled(children) = action.creations.into_settlement() else {
            panic!("both graphs must retain the routed creation settlement");
        };
        for child in children {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::InitializationRejected { creation, error },
            )) = child
            else {
                panic!("both graphs must return the exact initialization refusal");
            };
            assert_eq!(error, NodeError::InitializationRefused);
            let returned_id = creation.id();
            assert_eq!(returned_id, original_id);
            let (request, _route) = creation.into_parts();
            let (id, node, kind) = request.into_parts();
            assert_eq!(id, original_id);
            assert_eq!(kind, CreationKind::Birth);
            let NodeStartup::Refuse { payload } = node.into_inner().startup else {
                panic!("the original refused actor definition must survive");
            };
            assert_eq!(payload.as_ptr(), allocation);
            refused.push(payload.to_vec());
        }
    }
    assert_eq!(refused.len(), 1);
    refused.pop().unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn generated_singleton_matches_manual_original_initialization_refusal() {
    let (generated_root, generated_id, generated_allocation) = refused_garden_root();
    let (manual_root, manual_id, manual_allocation) = refused_garden_root();
    let generated =
        Application::with_topology(generated_root.stop_on_shutdown(), garden::Topology::new())
            .run()
            .await;
    let manual = Application::with_topology(
        manual_root.stop_on_shutdown(),
        GardenSpaces {
            root: ActorSpace::new(),
        },
    )
    .run()
    .await;
    let (generated_work, generated_native, generated_notifications, ()) = generated
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the generated singleton owns startup refusal");
        })
        .into_parts();
    let (manual_work, manual_native, manual_notifications, ()) = manual
        .unwrap_or_else(|original| {
            drop(original);
            panic!("the manual singleton owns startup refusal");
        })
        .into_parts();
    let WorkOutcome::NotInvoked {
        cleanup: generated_cleanup,
        ..
    } = generated_work
    else {
        panic!("the generated root must not fabricate caller invocation");
    };
    let WorkOutcome::NotInvoked {
        cleanup: manual_cleanup,
        ..
    } = manual_work
    else {
        panic!("the handwritten root must not fabricate caller invocation");
    };
    for cleanup in [generated_cleanup, manual_cleanup] {
        assert!(cleanup.is_ok());
    }
    for notifications in [generated_notifications, manual_notifications] {
        let notifications = notifications.unwrap();
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    }
    let (_, generated_native) = generated_native.unwrap();
    let (_, manual_native) = manual_native.unwrap();
    let generated = inspect_refused_garden(generated_native, generated_id, generated_allocation);
    let manual = inspect_refused_garden(manual_native, manual_id, manual_allocation);
    assert_eq!(generated, manual);
    assert_eq!(generated, [7, 13]);
}
