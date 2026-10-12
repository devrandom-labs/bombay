use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorActed, Never};

struct Worker;
#[bombay::actor(message = Never)]
impl Worker {}
struct LeftBranch;
#[bombay::actor(message = Never, births = { worker: Worker }, creation_settlements = retain_for_retirement)]
impl LeftBranch {}
struct RightBranch;
#[bombay::actor(message = Never, births = { worker: Worker }, creation_settlements = retain_for_retirement)]
impl RightBranch {}
struct Root;
#[bombay::actor(message = Never, births = { left: LeftBranch, right: RightBranch }, creation_settlements = retain_for_retirement)]
impl Root {
    #[expect(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the actor initialization contract requires self and its exact typed success/error carrier"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(bombay::behavior::Actions::stop())
    }
}

bombay::topology! {
    left_first {
        root: Root { Left(RootChildrenLeft) => left; Right(RootChildrenRight) => right; }
        left { Worker => worker; }
        right { Worker => worker; }
        worker {}
    }
}
bombay::topology! {
    right_first {
        worker {}
        right { Worker => worker; }
        root: Root { Left(RootChildrenLeft) => left; Right(RootChildrenRight) => right; }
        left { Worker => worker; }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn matching_incoming_births_preserve_declaration_order_independence() {
    let left =
        bombay::Application::with_topology(Root.stop_on_shutdown(), left_first::Topology::new())
            .run()
            .await;
    let right =
        bombay::Application::with_topology(Root.stop_on_shutdown(), right_first::Topology::new())
            .run()
            .await;
    let left = left.unwrap_or_else(|original| {
        drop(original);
        panic!("left declaration keeps its original application");
    });
    let right = right.unwrap_or_else(|original| {
        drop(original);
        panic!("right declaration keeps its original application");
    });
    let left = left.cleanup();
    let right = right.cleanup();
    assert!(matches!(
        left,
        Some(Ok((_, bombay::ActorRetirement::Completed { .. })))
    ));
    assert!(matches!(
        right,
        Some(Ok((_, bombay::ActorRetirement::Completed { .. })))
    ));
}
