//! Inferred recursive creation, exact refusal and borrowed/owned nominal role custody.

use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::Children;
use bombay::behavior::{Actions, BehaviorActed, CreateChild, CreationSequence, Creations, Never};
use bombay::lifecycle::StopOnShutdown;
use bombay::{ActorRetirement, Application};

enum NodeStartup {
    Grow { remaining: u32 },
    Refuse { payload: Box<[u8]> },
}

#[derive(Debug, PartialEq)]
enum NodeError {
    InitializationRefused,
}

struct Node {
    startup: NodeStartup,
}

#[bombay::actor(
    message = Never,
    error = NodeError,
    births = { next: StopOnShutdown<Node> },
    creation_settlements = retain_for_retirement,
)]
impl Node {
    fn init(&mut self) -> BehaviorActed<Self> {
        let remaining = match &self.startup {
            NodeStartup::Grow { remaining } => *remaining,
            NodeStartup::Refuse { .. } => return Err(NodeError::InitializationRefused),
        };
        if remaining == 0 {
            return Ok(Actions::cont());
        }
        let mut creations = CreationSequence::new();
        let id = creations.issue().expect("one creation fits the namespace");
        let child = Node {
            startup: NodeStartup::Grow {
                remaining: remaining - 1,
            },
        }
        .stop_on_shutdown();
        Ok(Actions::create(Creations::one(CreateChild::birth(
            id, child,
        ))))
    }
}

bombay::topology! {
    recursive_application {
        root: Node { Next(NodeChildrenNext) => node; }
        node: StopOnShutdown<Node> { Next(NodeChildrenNext) => node; }
    }
}

async fn nominal_recursive_creations() {
    let (outcome, native, notifications, ()) = Application::with_topology(
        Node {
            startup: NodeStartup::Grow { remaining: 3 },
        }
        .stop_on_shutdown(),
        recursive_application::Topology::new(),
    )
    .run_with(|application| async move {
        application
            .lifecycle()
            .request_shutdown()
            .expect("shutdown admitted");
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("entered host");
    }) .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work completed");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("actual cleanup acquired");
    let ActorRetirement::Completed {
        custody: ActorCustody { descendants, .. },
        ..
    } = retirement
    else {
        panic!("root completed");
    };
    let mut depth = 0;
    let [recursive_application::RootRetirements { origin, retirement }] = descendants
        .try_into()
        .unwrap_or_else(|_| panic!("one original direct child"));
    assert_ne!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    let ActorRetirement::OwnerCancelled {
        custody: ActorCustody { descendants, .. },
        ..
    } = *retirement
    else {
        panic!("original direct child custody");
    };
    depth += 1;
    let mut remaining = descendants;
    while let Some(recursive_application::NodeRetirements { origin, retirement }) = remaining.pop()
    {
        assert_ne!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
        depth += 1;
        let ActorRetirement::OwnerCancelled {
            custody: ActorCustody { descendants, .. },
            ..
        } = *retirement
        else {
            panic!("descendant retains parent retirement");
        };
        remaining.extend(descendants);
    }
    assert_eq!(depth, 3);
}

type ManagedNode = StopOnShutdown<Node>;

struct Parent {
    child: Option<CreateChild<bombay::MailAddr, ManagedNode>>,
}

#[derive(Debug)]
enum ParentError {
    ChildAlreadyTransferred,
}

#[bombay::actor(message = Never, error = ParentError, births = { branch: ManagedNode }, creation_settlements = retain_for_retirement)]
impl Parent {
    fn init(&mut self) -> BehaviorActed<Self> {
        let child = self
            .child
            .take()
            .ok_or(ParentError::ChildAlreadyTransferred)?;
        Ok(Actions::create(Creations::one(child)))
    }
}

bombay::topology! {
    parent_application {
        root: Parent { Branch(ParentChildrenBranch) => node; }
        node: StopOnShutdown<Node> { Next(NodeChildrenNext) => node; }
    }
}

async fn initialization_rejection_returns_the_original_child_and_work() {
    let payload: Box<[u8]> = Box::from([7, 13]);
    let allocation = payload.as_ptr();
    let child = Node {
        startup: NodeStartup::Refuse { payload },
    }
    .stop_on_shutdown();
    let mut sequence = CreationSequence::new();
    let id = sequence.issue().expect("one staged creation fits");
    let root = Parent {
        child: Some(CreateChild::birth(id, child)),
    }
    .stop_on_shutdown();
    let work = Box::new(String::from("root work survived"));
    let work_allocation = work.as_ptr();
    let (outcome, native, notifications, ()) = Application::with_topology(root, parent_application::Topology::new())
        .run_with(move |application| async move {
            application
                .lifecycle()
                .request_shutdown()
                .expect("root shutdown admitted");
            work
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered host");
        }) .into_parts();
    let (work, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("root work completed beside child failure");
    });
    assert_eq!(work.as_ptr(), work_allocation);
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("joined root");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                descendants,
                child_failures,
                ..
            },
        settlements,
        ..
    } = retirement
    else {
        panic!("child rejection does not invent a failed root");
    };
    assert!(descendants.is_empty());
    assert!(child_failures.is_empty());
    let mut rejected_children = 0;
    for action in settlements {
        let bombay::behavior::CreationSettlement::Settled(creations) =
            action.creations.into_settlement()
        else {
            panic!("the creation batch was routed");
        };
        for child in creations {
            let bombay::behavior::SettledItem::Attempted(
                bombay::behavior::ItemSettlement::Accepted(
                    bombay::behavior::ChildCreationOutcome::InitializationRejected {
                        creation,
                        error,
                    },
                ),
            ) = child
            else {
                panic!("the exact failed child returns in its accepted runtime reply");
            };
            assert_eq!(error, NodeError::InitializationRefused);
            let (creation, _route) = creation.into_parts();
            let (nonce, child, kind) = creation.into_parts();
            assert_eq!(nonce, id);
            assert_eq!(kind, bombay::behavior::CreationKind::Birth);
            let NodeStartup::Refuse { payload } = child.into_inner().startup else {
                panic!("the original definition survives");
            };
            assert_eq!(payload.as_ptr(), allocation);
            assert_eq!(&*payload, &[7, 13]);
            rejected_children += 1;
        }
    }
    assert_eq!(rejected_children, 1);
}

async fn different_parent_keeps_its_role_and_recursive_descendants() {
    let child = Node {
        startup: NodeStartup::Grow { remaining: 2 },
    }
    .stop_on_shutdown();
    let mut sequence = CreationSequence::new();
    let id = sequence.issue().expect("one staged creation fits");
    let root = Parent {
        child: Some(CreateChild::birth(id, child)),
    }
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = Application::with_topology(root, parent_application::Topology::new())
        .run_with(|application| async move {
            application
                .lifecycle()
                .request_shutdown()
                .expect("root shutdown admitted");
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered host");
        }) .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work returned");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("joined root");
    let ActorRetirement::Completed {
        custody: ActorCustody {
            mut descendants, ..
        },
        ..
    } = retirement
    else {
        panic!("parent completed");
    };
    let Some(parent_application::RootRetirements { origin, retirement }) = descendants.pop() else {
        panic!("the direct child's parent role survives");
    };
    assert_ne!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    assert!(descendants.is_empty());
    let ActorRetirement::OwnerCancelled {
        custody: ActorCustody { descendants, .. },
        ..
    } = *retirement
    else {
        panic!("the child retains cancellation custody");
    };
    let mut depth = 0;
    let mut descendants = descendants;
    while let Some(parent_application::NodeRetirements { origin, retirement }) = descendants.pop() {
        assert_ne!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    descendants: children,
                    ..
                },
            ..
        } = *retirement
        else {
            panic!("the descendant retains cancellation custody");
        };
        descendants.extend(children);
        depth += 1;
    }
    assert_eq!(depth, 2);
}

async fn born_roles_select_the_original_parent_and_recursive_retirement() {
    let mut sequence = CreationSequence::new();
    let id = sequence.issue().expect("one actual parent birth fits");
    let root = Parent {
        child: Some(CreateChild::birth(
            id,
            Node {
                startup: NodeStartup::Grow { remaining: 2 },
            }
            .stop_on_shutdown(),
        )),
    }
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = Application::with_topology(root, parent_application::Topology::new())
        .run_with(|application| async move {
            application
                .lifecycle()
                .request_shutdown()
                .expect("shutdown admitted");
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered host");
        }) .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work completed");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("complete parent retirement");
    let ActorRetirement::Completed {
        custody: ActorCustody {
            mut descendants, ..
        },
        ..
    } = retirement
    else {
        panic!("parent completed");
    };
    let report = descendants.pop().expect("direct child retired");
    assert!(descendants.is_empty());
    let before = (
        report.origin,
        core::ptr::from_ref(report.retirement.as_ref()),
    );
    let parent_application::RootRetirements { origin, retirement } = report;
    assert_eq!((origin, core::ptr::from_ref(retirement.as_ref())), before);
    let ActorRetirement::OwnerCancelled {
        custody: ActorCustody { descendants, .. },
        ..
    } = *retirement
    else {
        panic!("the original child was cancelled by its owner");
    };
    let mut descendants = descendants;
    let mut depth = 0;
    while let Some(report) = descendants.pop() {
        let before = (
            report.origin,
            core::ptr::from_ref(report.retirement.as_ref()),
        );
        let parent_application::NodeRetirements { origin, retirement } = report;
        assert_eq!((origin, core::ptr::from_ref(retirement.as_ref())), before);
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    descendants: children,
                    ..
                },
            ..
        } = *retirement
        else {
            panic!("recursive cancellation custody stays intact");
        };
        descendants.extend(children);
        depth += 1;
    }
    assert_eq!(depth, 2);
}

struct Siblings {
    children: Option<[CreateChild<bombay::MailAddr, ManagedNode>; 2]>,
}

#[bombay::actor(message = Never, error = ParentError,
    births = { primary: ManagedNode, replica: ManagedNode },
    creation_settlements = retain_for_retirement)]
impl Siblings {
    fn init(&mut self) -> BehaviorActed<Self> {
        let [primary, replica] = self
            .children
            .take()
            .ok_or(ParentError::ChildAlreadyTransferred)?;
        Ok(Actions::create(
            Children::new()
                .create(primary)
                .create(replica)
                .into_creates(),
        ))
    }
}

bombay::topology! {
    sibling_application {
        root: Siblings {
            Primary(SiblingsChildrenPrimary) => node;
            Replica(SiblingsChildrenReplica) => node;
        }
        node: StopOnShutdown<Node> { Next(NodeChildrenNext) => node; }
    }
}

async fn born_roles_preserve_the_unmatched_sibling_with_the_same_actor_type() {
    let mut sequence = CreationSequence::new();
    let primary = sequence.issue().expect("two sibling requests fit");
    let replica = sequence.issue().expect("two sibling requests fit");
    let root = Siblings {
        children: Some([
            CreateChild::birth(
                primary,
                Node {
                    startup: NodeStartup::Grow { remaining: 0 },
                }
                .stop_on_shutdown(),
            ),
            CreateChild::birth(
                replica,
                Node {
                    startup: NodeStartup::Grow { remaining: 0 },
                }
                .stop_on_shutdown(),
            ),
        ]),
    }
    .stop_on_shutdown();
    let (outcome, native, notifications, ()) = Application::with_topology(root, sibling_application::Topology::new())
        .run_with(|application| async move {
            application
                .lifecycle()
                .request_shutdown()
                .expect("shutdown admitted");
        })
        .await
        .unwrap_or_else(|original| {
            drop(original);
            panic!("entered host");
        }) .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work completed");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("complete sibling retirement");
    let ActorRetirement::Completed {
        custody: ActorCustody { descendants, .. },
        ..
    } = retirement
    else {
        panic!("parent completed");
    };
    assert_eq!(descendants.len(), 2);
    let mut primary_addresses = Vec::new();
    let mut replica_addresses = Vec::new();
    for report in descendants {
        let primary_before = match &report {
            sibling_application::RootRetirements::Primary { origin, retirement } => {
                Some((*origin, core::ptr::from_ref(retirement.as_ref())))
            }
            _ => None,
        };
        let replica_before = match &report {
            sibling_application::RootRetirements::Replica { origin, retirement } => {
                Some((*origin, core::ptr::from_ref(retirement.as_ref())))
            }
            _ => None,
        };
        let report = match report {
            sibling_application::RootRetirements::Primary { origin, retirement } => {
                assert_eq!(
                    primary_before,
                    Some((origin, core::ptr::from_ref(&*retirement)))
                );
                assert!(replica_before.is_none());
                primary_addresses.push(origin.address());
                let ActorRetirement::OwnerCancelled {
                    custody: ActorCustody { descendants, .. },
                    ..
                } = *retirement
                else {
                    panic!("the original primary was cancelled");
                };
                assert!(descendants.is_empty());
                continue;
            }
            original => {
                assert!(primary_before.is_none());
                assert_eq!(
                    replica_before,
                    (match &original {
                        sibling_application::RootRetirements::Replica { origin, retirement } =>
                            Some((*origin, core::ptr::from_ref(retirement.as_ref()))),
                        _ => None,
                    })
                );
                original
            }
        };
        let (origin, retirement) = match report {
            sibling_application::RootRetirements::Replica { origin, retirement } => {
                (origin, retirement)
            }
            original => {
                drop(original);
                panic!("the unmatched sibling retains its actual role");
            }
        };
        assert_eq!(
            replica_before,
            Some((origin, core::ptr::from_ref(&*retirement)))
        );
        replica_addresses.push(origin.address());
        let ActorRetirement::OwnerCancelled {
            custody: ActorCustody { descendants, .. },
            ..
        } = *retirement
        else {
            panic!("the original replica was cancelled");
        };
        assert!(descendants.is_empty());
    }
    assert_eq!(primary_addresses.len(), 1);
    assert_eq!(replica_addresses.len(), 1);
    assert_ne!(primary_addresses[0], replica_addresses[0]);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    nominal_recursive_creations().await;
    initialization_rejection_returns_the_original_child_and_work().await;
    different_parent_keeps_its_role_and_recursive_descendants().await;
    born_roles_select_the_original_parent_and_recursive_retirement().await;
    born_roles_preserve_the_unmatched_sibling_with_the_same_actor_type().await;
}
