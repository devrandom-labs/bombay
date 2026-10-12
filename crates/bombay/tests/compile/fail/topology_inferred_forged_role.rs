use bombay::actors::ActorExt;
use bombay::behavior::Never;
struct FirstWorker;
#[bombay::actor(message = Never)]
impl FirstWorker {}
struct SecondWorker;
#[bombay::actor(message = Never)]
impl SecondWorker {}
struct LeftBranch;
#[bombay::actor(message = Never, births = { worker: FirstWorker }, creation_settlements = retain_for_retirement)]
impl LeftBranch {}
struct RightBranch;
#[bombay::actor(message = Never, births = { worker: FirstWorker }, creation_settlements = retain_for_retirement)]
impl RightBranch {}
struct MisdeclaredWorker;
impl bombay::behavior::ChildPosition<FirstWorker, SecondWorker> for bombay::behavior::ChildHead {}
impl bombay::behavior::ChildRole<RightBranch> for MisdeclaredWorker {
    type Child = SecondWorker;
    type Position = bombay::behavior::ChildHead;
}
struct Root;
#[bombay::actor(message = Never, births = { left: LeftBranch, right: RightBranch }, creation_settlements = retain_for_retirement)]
impl Root {}
bombay::topology! {
    application {
        root: Root { Left(RootChildrenLeft) => left; Right(RootChildrenRight) => right; }
        left { Worker => worker; }
        right { Worker(MisdeclaredWorker) => worker; }
        worker {}
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result =
        bombay::Application::with_topology(Root.stop_on_shutdown(), application::Topology::new())
            .run()
            .await;
    drop(result);
}
