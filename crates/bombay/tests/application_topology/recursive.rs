//! Complete recursive creation, report and retirement journeys through public topology.
use behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, BirthMode, ChildChoice, ChildHead,
    ChildReport, ChildRole, ChildTail, ClassifySettlement, CreateChild, CreationKind,
    CreationSequence, Creations, EventIngress, InterpreterRequests, ItemSettlement,
    MessageProtocol, Never, NoBirths, NoSends, ReportToParent, RetirementBirths, SettledItem,
    SettlementStatus, Step, Stopped, User, UserEvent,
};
use bombay::Application;
use bombay::MailAddr;
use bombay::actors::ActorExt;
use bombay::lifecycle::StopOnShutdown;
use bombay::{ActorCustody, ActorRetirement};
use core::time::Duration;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
struct Trace {
    nodes: usize,
    audits: usize,
}

struct Readiness {
    payload: Box<[u8]>,
}
type Reports = <Leaf as Behavior>::Sends;
enum ReportEvent {
    Child(ChildReport<Trace>),
    Ready(ChildReport<Readiness>),
}
impl UserEvent for ReportEvent {
    type Addr = MailAddr;
    type Message = Never;
    fn user(_: MailAddr, never: Never) -> Self {
        match never {}
    }
    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}
impl<Source: BirthMode> EventIngress<Source, ChildReport<Trace>> for ReportEvent {
    fn ingress(report: ChildReport<Trace>) -> Self {
        Self::Child(report)
    }
}
impl<Source: BirthMode> EventIngress<Source, ChildReport<Readiness>> for ReportEvent {
    fn ingress(report: ChildReport<Readiness>) -> Self {
        Self::Ready(report)
    }
}

#[derive(Debug, PartialEq)]
enum NodeError {
    InitializationRefused,
}
enum NodeStartup {
    Grow { remaining: usize },
    Refuse { payload: Box<[u8]> },
}
struct Node {
    startup: NodeStartup,
    pending: HashSet<behavior::CreationId>,
    trace: Trace,
    readiness: Vec<Box<[u8]>>,
}
impl Node {
    fn grow(remaining: usize) -> Self {
        Self {
            startup: NodeStartup::Grow { remaining },
            pending: HashSet::new(),
            trace: Trace {
                nodes: 1,
                audits: 0,
            },
            readiness: Vec::new(),
        }
    }
}
struct Leaf {
    payload: Box<[u8]>,
}
impl BehaviorBase for Node {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
type ManagedNode = StopOnShutdown<Node>;
type ManagedLeaf = StopOnShutdown<behavior_actors::OneShot<Leaf>>;
type NodeChildren = ChildChoice<ManagedNode, ChildChoice<ManagedLeaf, Never>>;
struct Next;
struct Audit;
impl ChildRole<Node> for Next {
    type Child = ManagedNode;
    type Position = ChildHead;
}
impl behavior::ChildOccurrence<Node> for Next {
    type Resolution = behavior::DeclaredChildOccurrence;
}
impl ChildRole<Node> for Audit {
    type Child = ManagedLeaf;
    type Position = ChildTail<ChildHead>;
}
impl behavior::ChildOccurrence<Node> for Audit {
    type Resolution = behavior::DeclaredChildOccurrence;
}
impl Behavior for Node {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = ReportEvent;
    type Sends = Reports;
    type Ph = Never;
    type Error = NodeError;
    type Birth = RetirementBirths<NodeChildren>;
    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        let remaining = match self.startup {
            NodeStartup::Grow { remaining } => remaining,
            NodeStartup::Refuse { .. } => return Err(NodeError::InitializationRefused),
        };
        let readiness = ReportToParent::new(Readiness {
            payload: Box::from([197, 199]),
        });
        if remaining == 0 {
            return Ok(Actions::cont()
                .with_send(readiness)
                .with_send(ReportToParent::new(Trace {
                    nodes: 1,
                    audits: 0,
                })));
        }
        let mut sequence = CreationSequence::new();
        let next = sequence.issue().expect("two creations fit");
        let audit = sequence.issue().expect("two creations fit");
        self.pending.extend([next, audit]);
        let children = Creations::one(CreateChild::birth(
            next,
            ChildChoice::Head(Node::grow(remaining - 1).stop_on_shutdown()),
        ))
        .and(CreateChild::birth(
            audit,
            ChildChoice::Tail(ChildChoice::Head(
                Leaf {
                    payload: Box::from([13]),
                }
                .with_one_shot(Duration::from_secs(60), leaf_deadline_reached)
                .stop_on_shutdown(),
            )),
        ));
        Ok(Actions::create(children).with_send(readiness))
    }
    fn transition(&mut self, _: ActiveTurn, event: ReportEvent) -> BehaviorActed<Self> {
        let report = match event {
            ReportEvent::Child(report) => report,
            ReportEvent::Ready(report) => {
                self.readiness.push(report.report.payload);
                return Ok(Actions::cont());
            }
        };
        if self.pending.remove(&report.child) {
            self.trace.nodes += report.report.nodes;
            self.trace.audits += report.report.audits;
            if self.pending.is_empty() {
                return Ok(Actions::cont().with_send(ReportToParent::new(Trace {
                    nodes: self.trace.nodes,
                    audits: self.trace.audits,
                })));
            }
        }
        Ok(Actions::cont())
    }
}
#[bombay::actor(message = Never, sends = {
    readiness: InterpreterRequests<ReportToParent<Readiness>>,
    summaries: InterpreterRequests<ReportToParent<Trace>>,
})]
impl Leaf {
    #[expect(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the actor initialization contract retains its receiver and controlled error sum"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont()
            .with_send(ReportToParent::new(Readiness {
                payload: Box::from([211, 223]),
            }))
            .with_send(ReportToParent::new(Trace {
                nodes: 0,
                audits: 1,
            })))
    }
}

enum RootReportEvent {
    Child(ChildReport<Trace>),
    Ready(ChildReport<Readiness>),
}
impl UserEvent for RootReportEvent {
    type Addr = MailAddr;
    type Message = Never;
    fn user(_: MailAddr, never: Never) -> Self {
        match never {}
    }
    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}
impl<Source: BirthMode> EventIngress<Source, ChildReport<Trace>> for RootReportEvent {
    fn ingress(report: ChildReport<Trace>) -> Self {
        Self::Child(report)
    }
}
impl<Source: BirthMode> EventIngress<Source, ChildReport<Readiness>> for RootReportEvent {
    fn ingress(report: ChildReport<Readiness>) -> Self {
        Self::Ready(report)
    }
}

enum RootStartup {
    Grow {
        remaining: usize,
    },
    Refuse {
        creation: CreateChild<MailAddr, ManagedNode>,
    },
}
struct Root {
    startup: Option<RootStartup>,
    trace: Option<Trace>,
    readiness: Vec<Box<[u8]>>,
}
impl BehaviorBase for Root {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
struct Branch;
impl ChildRole<Root> for Branch {
    type Child = ManagedNode;
    type Position = ChildHead;
}
impl behavior::ChildOccurrence<Root> for Branch {
    type Resolution = behavior::DeclaredChildOccurrence;
}
impl Behavior for Root {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = RootReportEvent;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = RetirementBirths<ManagedNode>;
    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        let (creation, become_) = match self.startup.take().expect("root initialization once") {
            RootStartup::Grow { remaining } => {
                let mut sequence = CreationSequence::new();
                let id = sequence.issue().expect("one child fits");
                (
                    CreateChild::birth(id, Node::grow(remaining).stop_on_shutdown()),
                    Step::Continue,
                )
            }
            RootStartup::Refuse { creation } => (creation, Step::Stop(Stopped)),
        };
        Ok(Actions::new(NoSends, Creations::one(creation), become_))
    }
    fn transition(&mut self, _: ActiveTurn, event: RootReportEvent) -> BehaviorActed<Self> {
        let report = match event {
            RootReportEvent::Child(report) => report,
            RootReportEvent::Ready(report) => {
                self.readiness.push(report.report.payload);
                return Ok(Actions::cont());
            }
        };
        self.trace = Some(report.report);
        Ok(Actions::stop())
    }
}

bombay::topology! {
    garden {
        root: Root { Branch(Branch) => node; }
        node for Node { Next(Next) => node; Audit(Audit) => leaf; }
        leaf {}
    }
}

use garden::{NodeRetirements, RootRetirements, Topology as Spaces};
type NodeFailures = Vec<garden::NodeChildFailure>;

fn inspect_node(
    retirement: ActorRetirement<ManagedNode, NodeRetirements, NodeFailures>,
    actual: &mut Trace,
) {
    let ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                behavior,
                descendants,
                child_failures: failures,
                received_interpretation,
                received_source,
                ..
            },
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("parent cancellation owns every exact node")
    };
    assert_eq!(failures.len(), 0);
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let node = behavior.into_inner();
    assert_eq!(node.pending.len(), 0);
    let mut reports: Vec<_> = node.readiness.iter().map(AsRef::as_ref).collect();
    reports.sort_unstable();
    if descendants.is_empty() {
        assert_eq!(reports.len(), 0);
    } else {
        assert_eq!(reports, [&[197, 199][..], &[211, 223][..]]);
    }
    actual.nodes += 1;
    let statuses: Vec<_> = settlements
        .iter()
        .map(ClassifySettlement::settlement_status)
        .collect();
    assert!(
        statuses
            .iter()
            .all(|status| *status == SettlementStatus::Accepted)
    );
    for descendant in descendants {
        match descendant {
            NodeRetirements::Next { origin, retirement } => {
                assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
                assert_eq!(origin.creation_kind(), CreationKind::Birth);
                inspect_node(*retirement, actual);
            }
            NodeRetirements::Audit { origin, retirement } => {
                assert_eq!(origin.creation_kind(), CreationKind::Birth);
                let ActorRetirement::OwnerCancelled {
                    custody:
                        ActorCustody {
                            behavior,
                            descendants,
                            child_failures,
                            received_interpretation,
                            received_source,
                            ..
                        },
                    settlements,
                    interpretation,
                    source,
                } = *retirement
                else {
                    panic!("audit retains original leaf")
                };
                assert_eq!(descendants, [] as [Never; 0]);
                assert_eq!(child_failures.len(), 0);
                assert!(received_interpretation.is_none());
                assert!(received_source.is_none());
                assert!(interpretation.is_none());
                assert!(source.is_none());
                assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
                assert_eq!(settlements.len(), 0);
                assert_eq!(behavior.base().payload.len(), 1);
                actual.audits += 1;
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn recursive_reports_return_full_native_retirement() {
    pure_alias_definitions();
    let (outcome, _root_report, native, notifications, ()) = Application::with_topology(
        Root {
            startup: Some(RootStartup::Grow { remaining: 3 }),
            trace: None,
            readiness: Vec::new(),
        }
        .with_one_shot(Duration::from_secs(10), root_deadline_reached)
        .stop_on_shutdown(),
        Arc::new(Spaces::new()),
    )
    .run_with(|application| async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            application.lifecycle().termination(),
        )
        .await
        .expect("actual reports complete recursive root")
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("native host entered")
    })
    .into_parts();
    let (_, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("caller work completed")
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (_, retirement) = native.expect("native root joined");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                descendants,
                child_failures: failures,
                ..
            },
        settlements,
        ..
    } = retirement
    else {
        panic!("root completed actual report")
    };
    assert_eq!(failures.len(), 0);
    assert_eq!(behavior.base().readiness.len(), 1);
    assert_eq!(behavior.base().readiness[0].as_ref(), &[197, 199]);
    assert_eq!(
        behavior.base().trace,
        Some(Trace {
            nodes: 4,
            audits: 3
        })
    );
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(descendants.len(), 1);
    let mut actual = Trace {
        nodes: 0,
        audits: 0,
    };
    for RootRetirements { origin, retirement } in descendants {
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        inspect_node(*retirement, &mut actual);
    }
    assert_eq!(
        actual,
        Trace {
            nodes: 4,
            audits: 3
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recursive_reports_keep_custody_with_outer_timer() {
    pure_alias_definitions();
    let (outcome, _root_report, native, notifications, ()) = Application::with_topology(
        Root {
            startup: Some(RootStartup::Grow { remaining: 3 }),
            trace: None,
            readiness: Vec::new(),
        }
        .stop_on_shutdown()
        .with_one_shot(Duration::from_secs(10), stopped_root_deadline_reached),
        Arc::new(Spaces::new()),
    )
    .run_with(|application| async move {
        tokio::time::timeout(
            Duration::from_secs(10),
            application.lifecycle().termination(),
        )
        .await
        .expect("actual reports complete recursive root")
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("native host entered")
    })
    .into_parts();
    let (_, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("caller work completed")
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (_, retirement) = native.expect("native root joined");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                descendants,
                child_failures: failures,
                ..
            },
        settlements,
        ..
    } = retirement
    else {
        panic!("root completed actual report")
    };
    assert_eq!(failures.len(), 0);
    assert_eq!(behavior.base().readiness.len(), 1);
    assert_eq!(behavior.base().readiness[0].as_ref(), &[197, 199]);
    assert_eq!(
        behavior.base().trace,
        Some(Trace {
            nodes: 4,
            audits: 3
        })
    );
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(descendants.len(), 1);
    let mut actual = Trace {
        nodes: 0,
        audits: 0,
    };
    for RootRetirements { origin, retirement } in descendants {
        assert_ne!(origin.address(), MailAddr::APPLICATION_ROOT);
        inspect_node(*retirement, &mut actual);
    }
    assert_eq!(
        actual,
        Trace {
            nodes: 4,
            audits: 3
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recursive_scope_returns_original_initialization_refusal() {
    let payload: Box<[u8]> = Box::from([7, 13]);
    let allocation = payload.as_ptr();
    let mut sequence = CreationSequence::new();
    let original_id = sequence.issue().expect("one exact refused child fits");
    let creation = CreateChild::birth(
        original_id,
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
    let (outcome, _root_report, native, notifications, ()) = Application::with_topology(
        Root {
            startup: Some(RootStartup::Refuse { creation }),
            trace: None,
            readiness: Vec::new(),
        }
        .stop_on_shutdown(),
        Arc::new(Spaces::new()),
    )
    .run()
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("native host entered")
    })
    .into_parts();
    let bombay::WorkOutcome::NotInvoked {
        cleanup: Ok(()), ..
    } = outcome
    else {
        drop(outcome);
        panic!("no fabricated caller work")
    };
    let notifications = notifications.expect("both original startup notification receipts survive");
    notifications
        .termination
        .expect("actual startup termination notification");
    notifications
        .retirement
        .expect("actual joined startup retirement notification");
    let (_, retirement) = native.expect("exact original startup root retirement");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                descendants,
                child_failures: failures,
                ..
            },
        settlements,
        ..
    } = retirement
    else {
        panic!("exact startup refusal settled")
    };
    assert_eq!(descendants.len(), 0);
    assert_eq!(failures.len(), 0);
    let mut refusals = 0;
    for action in settlements {
        let behavior::CreationSettlement::Settled(children) = action.creations.into_settlement()
        else {
            panic!("request routed")
        };
        for child in children {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                behavior::ChildCreationOutcome::InitializationRejected { creation, error },
            )) = child
            else {
                panic!("original refused child returned")
            };
            assert_eq!(error, NodeError::InitializationRefused);
            let returned_id = creation.id();
            assert_eq!(returned_id, original_id);
            let (request, route) = creation.into_parts();
            let (id, node, kind) = request.into_parts();
            assert_eq!(id, original_id);
            assert_ne!(route, id.get());
            assert_eq!(kind, CreationKind::Birth);
            let NodeStartup::Refuse { payload } = node.into_inner().startup else {
                panic!("exact original definition")
            };
            assert_eq!(payload.as_ptr(), allocation);
            assert_eq!(&*payload, &[7, 13]);
            refusals += 1;
        }
    }
    assert_eq!(refusals, 1);
}

// Universal pure metadata remains separate from native closed graph eligibility.
struct Foreign {
    payload: Box<[u8]>,
}
impl Behavior for Foreign {
    type Protocol = MessageProtocol<behavior::MailAddr, Never>;
    type Event = User<behavior::MailAddr, Never>;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}
type ForeignServices = behavior::Births<Foreign>;
struct ForeignParent {
    child: Foreign,
}
#[bombay::actor(message = Never, births = ForeignServices, creation_settlements = retain_for_retirement)]
impl ForeignParent {}
type Services<Child> = behavior::Births<ChildChoice<Child, ChildChoice<ManagedLeaf, Never>>>;
struct GenericParent<Child> {
    child: Child,
}
#[bombay::actor(message = Never, births = Services<Child>, creation_settlements = retain_for_retirement)]
impl<Child: Behavior> GenericParent<Child> {}
fn pure_alias_definitions() {
    let payload: Box<[u8]> = Box::from([17, 23]);
    let allocation = payload.as_ptr();
    let mut foreign = ForeignParent {
        child: Foreign { payload },
    };
    let actions = behavior::initialize(&mut foreign)
        .expect("pure foreign aliases initialize without native execution authority");
    assert_eq!(actions.creates.len(), 0);
    assert_eq!(foreign.child.payload.as_ptr(), allocation);
    let mut generic = GenericParent {
        child: foreign.child,
    };
    let actions = behavior::initialize(&mut generic)
        .expect("pure generic heterogeneous aliases retain original child definitions");
    assert_eq!(actions.creates.len(), 0);
    assert_eq!(generic.child.payload.as_ptr(), allocation);
    assert_eq!(&*generic.child.payload, &[17, 23]);
}

fn root_deadline_reached(
    _: &mut Root,
) -> Actions<MailAddr, Never, <Root as Behavior>::Sends, <Root as Behavior>::Birth> {
    Actions::stop()
}
fn leaf_deadline_reached(
    _: &mut Leaf,
) -> Actions<MailAddr, Never, <Leaf as Behavior>::Sends, <Leaf as Behavior>::Birth> {
    Actions::stop()
}

fn stopped_root_deadline_reached(
    _: &mut StopOnShutdown<Root>,
) -> Actions<MailAddr, Never, <StopOnShutdown<Root> as Behavior>::Sends, <Root as Behavior>::Birth>
{
    Actions::stop()
}

#[path = "recursive/comparison.rs"]
mod comparison;
