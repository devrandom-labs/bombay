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
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(bombay::behavior::Actions::stop())
    }
}

bombay::topology! {
    application {
        root { Left(RootChildrenLeft) => left; Right(RootChildrenRight) => right; }
        left { Worker => worker; }
        right { Worker => worker; }
        worker {}
    }
}
