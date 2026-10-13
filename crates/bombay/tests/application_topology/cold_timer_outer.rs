use behavior::{
    BehaviorBase, ChildDelivery, ChildProduct, CreateChild, CreationSequence, Creations,
};
use bombay::actors::ActorExt;
use bombay::prelude::*;
use core::time::Duration;
use std::collections::BTreeSet;
use tokio::runtime::Builder;

#[derive(Debug, PartialEq)]
enum WorkerCommand {
    Increment,
}
struct Worker {
    original: Box<[u8]>,
}
#[bombay::actor]
impl Worker {
    fn receive(&mut self, _: WorkerCommand) -> BehaviorActed<Self> {
        self.original[0] += 1;
        Ok(Actions::cont())
    }
}
type ManagedWorker = StopOnShutdown<Worker>;
struct Root {
    born: Option<ManagedWorker>,
}
#[bombay::actor(
    message = Never,
    sends = { commands: Vec<ChildDelivery<Worker, RootChildrenBorn>> },
    births = { born: ManagedWorker },
    creation_settlements = retain_for_retirement,
)]
impl Root {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the actor initialization contract returns the exact controlled error sum"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one original birth");
        let child = self.born.take().expect("original birth occurs once");
        Ok(
            Actions::create(Creations::one(CreateChild::birth(id, child)))
                .send_commands(ChildDelivery::after(id, WorkerCommand::Increment)),
        )
    }
}
struct BackgroundWorker;
struct AuditWorker;
bombay::topology! {
    workshop {
        root: Root { Born(RootChildrenBorn) => worker; }
        members {
            Background(BackgroundWorker) => worker;
            Audit(AuditWorker) => worker;
        }
        worker: ManagedWorker {}
    }
}

fn cold_child_origin<OriginalRoot, Product, Role>(
    actor: &ApplicationBehavior<OriginalRoot, Product>,
    origin: ChildOrigin<OriginalRoot, Role>,
) -> (MailAddr, u64)
where
    OriginalRoot: BehaviorBase<Base = Root>,
    Product: ChildProduct<MailAddr>,
{
    assert!(
        actor.base().born.is_none(),
        "the original root initialized its actual birth"
    );
    (origin.address(), origin.nonce())
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "One causal trace preserves original actors and both notification receipts before checking complete child custody."
)]
fn cold_children_keep_original_root_through_timer_outer() {
    let first = vec![1].into_boxed_slice();
    let second = vec![10].into_boxed_slice();
    let third = vec![20].into_boxed_slice();
    let originals = BTreeSet::from([
        first.as_ptr() as usize,
        second.as_ptr() as usize,
        third.as_ptr() as usize,
    ]);
    let application = Application::with_topology(
        Root {
            born: Some(Worker { original: first }.stop_on_shutdown()),
        }
        .stop_on_shutdown()
        .with_one_shot(TimerId(53), Duration::from_secs(60), |_| Actions::stop()),
        workshop::Topology::new(),
    )
    .child(
        BackgroundWorker,
        Worker { original: second }.stop_on_shutdown(),
    )
    .child(AuditWorker, Worker { original: third }.stop_on_shutdown());
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller-owned host");
    let (outcome, native, notifications) = host
        .block_on(application.run_with(|application| async move {
            let shutdown = application.lifecycle().request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.lifecycle().termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
        }))
        .unwrap_or_else(|original| {
            drop(original);
            panic!("original declaration retained on missing host");
        });
    drop(host);
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("complete original outcome");
    });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications
        .termination
        .expect("actual ordinary termination notification");
    notifications
        .retirement
        .expect("actual joined retirement notification");
    let (root_origin, retirement) = native.expect("independent root custody");
    assert_eq!(root_origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(retirement.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed { custody, .. } = retirement else {
        panic!("root completes")
    };
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.descendants.len(), 3);
    let mut returned = BTreeSet::new();
    let mut addresses = BTreeSet::new();
    let mut roles = BTreeSet::new();
    for child in custody.descendants {
        let (role, origin, retirement) = match child {
            workshop::RootRetirements::Born { origin, retirement } => {
                ("born", (origin.address(), origin.nonce()), retirement)
            }
            workshop::RootRetirements::Background { origin, retirement } => (
                "background",
                cold_child_origin(&custody.behavior, origin),
                retirement,
            ),
            workshop::RootRetirements::Audit { origin, retirement } => (
                "audit",
                cold_child_origin(&custody.behavior, origin),
                retirement,
            ),
        };
        let inserted_role = roles.insert(role);
        assert!(inserted_role);
        let inserted_address = addresses.insert(origin);
        assert!(
            inserted_address,
            "each occurrence has its original fresh binding"
        );
        let ActorRetirement::OwnerCancelled { custody, .. } = *retirement else {
            panic!("root retirement cancels original worker")
        };
        let inserted_original = returned.insert(custody.behavior.base().original.as_ptr() as usize);
        assert!(inserted_original);
        assert_eq!(custody.descendants.len(), 0);
        assert_eq!(custody.child_failures.len(), 0);
        if role == "born" {
            let accepted_increment =
                usize::from(custody.behavior.base().original[0] - 1) + custody.user.len();
            assert_eq!(
                accepted_increment, 1,
                "accepted same-action send is processed or retained exactly once"
            );
        } else {
            assert_eq!(custody.user.len(), 0);
        }
    }
    assert_eq!(returned, originals);
    assert_eq!(roles, BTreeSet::from(["born", "background", "audit"]));
}
