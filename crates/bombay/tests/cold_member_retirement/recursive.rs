//! Inferred recursive creation, exact refusal and borrowed/owned nominal role custody.

#[path = "../../../../examples/counter/src/counter.rs"]
mod counter;
use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{Actions, BehaviorActed, CreateChild, CreationSequence, Creations, Never};
use bombay::lifecycle::StopOnShutdown;
use bombay::{ActorRetirement, Application};
use counter::{Counter, CounterMessage};

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

struct Canopy;
bombay::topology! {
    forest {
        root: Counter {}
        members { Canopy(Canopy) => node; }
        node for Node { Next(NodeChildrenNext) => node; }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cold_recursive_creation_retains_every_original_descendant() {
    let outcome =
        Application::with_topology(Counter::new().stop_on_shutdown(), forest::Topology::new())
            .child(
                Canopy,
                Node {
                    startup: NodeStartup::Grow { remaining: 3 },
                }
                .stop_on_shutdown(),
            )
            .run_with(|application| async move {
                let mut caller = application.client().expect("fresh customer");
                caller
                    .send(CounterMessage::Increment)
                    .await
                    .expect("increment admitted");
                caller
                    .send(CounterMessage::Read(caller.recipient()))
                    .await
                    .expect("read admitted");
                let reply = caller.receive().await.expect("original reply");
                assert_eq!(reply.message, 1);
                application.request_shutdown().expect("shutdown admitted");
            })
            .await
            .unwrap_or_else(|original| {
                drop(original);
                panic!("entered host");
            });
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work returned");
    });
    let (_, retirement) = cleanup.expect("root joined");
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                descendants,
                child_failures,
                ..
            },
        ..
    } = retirement
    else {
        panic!("root completed");
    };
    drop(behavior);
    assert!(child_failures.is_empty());
    let [report] = <[_; 1]>::try_from(descendants).unwrap_or_else(|original| {
        drop(original);
        panic!("one cold tree");
    });
    let forest::RootRetirements { origin, retirement } = report;
    assert_eq!(
        origin.creation_kind(),
        bombay::behavior::CreationKind::Birth
    );
    let mut remaining = vec![*retirement];
    let mut addresses = std::collections::HashSet::new();
    addresses.insert(origin.address());
    let mut depth = 0;
    while let Some(retirement) = remaining.pop() {
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    behavior,
                    descendants,
                    child_failures,
                    ..
                },
            ..
        } = retirement
        else {
            panic!("original node cancellation retained");
        };
        assert!(matches!(
            behavior.into_inner().startup,
            NodeStartup::Grow { .. }
        ));
        assert!(child_failures.is_empty());
        depth += 1;
        for forest::NodeRetirements { origin, retirement } in descendants {
            assert_eq!(
                origin.creation_kind(),
                bombay::behavior::CreationKind::Birth
            );
            assert!(addresses.insert(origin.address()));
            remaining.push(*retirement);
        }
    }
    assert_eq!(depth, 4);
    assert_eq!(addresses.len(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn cold_initialization_refusal_returns_the_original_definition_and_independent_work() {
    let payload: Box<[u8]> = Box::from([7, 13]);
    let allocation = payload.as_ptr();
    let work = Box::new(String::from("counter work survived"));
    let work_allocation = work.as_ptr();
    let outcome =
        Application::with_topology(Counter::new().stop_on_shutdown(), forest::Topology::new())
            .child(
                Canopy,
                Node {
                    startup: NodeStartup::Refuse { payload },
                }
                .stop_on_shutdown(),
            )
            .run_with(move |application| async move {
                let mut caller = application.client().expect("fresh customer");
                caller
                    .send(CounterMessage::Increment)
                    .await
                    .expect("increment admitted");
                caller
                    .send(CounterMessage::Read(caller.recipient()))
                    .await
                    .expect("read admitted");
                let reply = caller
                    .receive()
                    .await
                    .expect("counter reply survives independent child refusal");
                assert_eq!(reply.message, 1);
                application.request_shutdown().expect("shutdown admitted");
                work
            })
            .await
            .unwrap_or_else(|original| {
                drop(original);
                panic!("entered host");
            });
    let (work, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work returned independently");
    });
    assert_eq!(work.as_ptr(), work_allocation);
    let (_, retirement) = cleanup.expect("joined root");
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
        panic!("child refusal does not replace root completion");
    };
    assert!(descendants.is_empty());
    assert!(child_failures.is_empty());
    let mut returned = 0;
    for action in settlements {
        let bombay::behavior::CreationSettlement::Settled(creations) =
            action.creations.into_settlement()
        else {
            panic!("creation batch routed");
        };
        for child in creations {
            let bombay::behavior::SettledItem::Attempted(
                bombay::behavior::ItemSettlement::Accepted(bombay::behavior::ChildChoice::Head(
                    bombay::behavior::ChildCreationOutcome::InitializationRejected {
                        creation,
                        error,
                    },
                )),
            ) = child
            else {
                panic!("original refusal returned");
            };
            assert_eq!(error, NodeError::InitializationRefused);
            let (creation, _) = creation.into_parts();
            let (_, child, kind) = creation.into_parts();
            assert_eq!(kind, bombay::behavior::CreationKind::Birth);
            let NodeStartup::Refuse { payload } = child.into_inner().startup else {
                panic!("original startup returned");
            };
            assert_eq!(payload.as_ptr(), allocation);
            assert_eq!(&*payload, &[7, 13]);
            returned += 1;
        }
    }
    assert_eq!(returned, 1);
}
