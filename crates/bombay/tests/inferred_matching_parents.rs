use bombay::ActorNotificationReceipts;
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
    let (left_outcome, left_native, left_notifications, ()) = left.into_parts();
    let (right_outcome, right_native, right_notifications, ()) = right.into_parts();
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = left_notifications
    else {
        panic!("both original left notifications succeed");
    };
    let Ok(ActorNotificationReceipts {
        termination: Ok(()),
        retirement: Ok(()),
    }) = right_notifications
    else {
        panic!("both original right notifications succeed");
    };
    let left = left_outcome.cleanup();
    let right = right_outcome.cleanup();
    assert!(matches!(left, Some(Ok(()))));
    assert!(matches!(right, Some(Ok(()))));
    assert!(matches!(
        left_native,
        Ok((_, bombay::ActorRetirement::Completed { .. }))
    ));
    assert!(matches!(
        right_native,
        Ok((_, bombay::ActorRetirement::Completed { .. }))
    ));
}
