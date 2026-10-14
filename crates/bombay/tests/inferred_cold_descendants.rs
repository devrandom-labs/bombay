use bombay::ApplicationScope;
use bombay::behavior::{
    BehaviorBase, ChildDelivery, CreateChild, CreationSequence, Creations, MessageProtocol,
};
use bombay::prelude::*;
use std::ptr;

type Count = MessageProtocol<MailAddr, u64>;

enum WorkerCommand {
    Read(EstablishedRecipient<Count>),
}

struct Worker {
    value: u64,
    payload: Box<[u8]>,
}

#[bombay::actor(sends = { counts: Vec<EstablishedDelivery<Count>> })]
impl Worker {
    fn receive(&mut self, command: WorkerCommand) -> BehaviorActed<Self> {
        let WorkerCommand::Read(reply) = command;
        Ok(Actions::cont().with_send(EstablishedDelivery::new(reply, self.value)))
    }
}

struct Branch {
    worker: Option<StopOnShutdown<Worker>>,
    reply: EstablishedRecipient<Count>,
}

#[bombay::actor(
    message = Never,
    births = { worker: StopOnShutdown<Worker> },
    creation_settlements = retain_for_retirement,
    sends = { commands: Vec<ChildDelivery<Worker, BranchChildrenWorker>> },
)]
impl Branch {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "initialization uses the owning typed actor Actions carrier"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one fresh worker request");
        let worker = self
            .worker
            .take()
            .expect("the original worker is created once");
        Ok(
            Actions::create(Creations::one(CreateChild::birth(id, worker))).send_commands(
                ChildDelivery::after(id, WorkerCommand::Read(self.reply.clone())),
            ),
        )
    }
}

struct Entrance;

#[bombay::actor(message = Never)]
impl Entrance {}

struct BranchRole;

bombay::topology! {
    branches {
        root: Entrance {}
        members { Branch(BranchRole) => branch; }
        branch { Worker => worker; }
        worker {}
        hosts { count: Count; }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inferred_cold_branch_creates_worker_and_keeps_native_reply_and_complete_custody() {
    let scope = ApplicationScope::new();
    let mut counts = scope
        .external::<Count>()
        .expect("the real reply endpoint starts");
    let payload = vec![41, 43].into_boxed_slice();
    let identity = payload.as_ptr();
    let application = scope
        .with_topology(Entrance.stop_on_shutdown(), branches::Topology::new())
        .child(
            BranchRole,
            Branch {
                worker: Some(Worker { value: 42, payload }.stop_on_shutdown()),
                reply: counts.recipient(),
            }
            .stop_on_shutdown(),
        );
    let outcome = application
        .run_with(|application| async move {
            let count = counts
                .receive()
                .await
                .expect("fresh child creation precedes its same-action read");
            assert_eq!(count.message, 42);
            assert_ne!(count.from, counts.address());
            assert_ne!(count.from, application.root().address());
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            count.from
        })
        .await
        .unwrap_or_else(|_| panic!("the ordinary cold-member application starts"));
    let (worker_address, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the full cold-member subtree joins"));
    assert!(retirement.cleanup().is_ok());
    let (_, root) = retirement
        .retirement()
        .as_ref()
        .expect("the original root remains owned");
    let root = root
        .custody()
        .expect("complete root custody retains its cold branch");
    assert_eq!(root.descendants.len(), 1);
    let branches::RootRetirements {
        retirement: branch, ..
    } = &root.descendants[0];
    let branch = branch.custody().expect("the original branch remains owned");
    assert!(branch.behavior.base().worker.is_none());
    assert_eq!(branch.descendants.len(), 1);
    let branches::BranchRetirements {
        origin,
        retirement: worker,
    } = &branch.descendants[0];
    assert_eq!(origin.address(), worker_address);
    let worker = worker
        .behavior()
        .expect("the actual fresh child retains its original state")
        .base();
    assert!(ptr::eq(worker.payload.as_ptr(), identity));
    assert_eq!(&*worker.payload, &[41, 43]);
    assert_eq!(worker.value, 42);
}
