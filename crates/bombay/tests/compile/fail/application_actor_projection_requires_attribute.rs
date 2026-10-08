use bombay::behavior::Never;
use bombay::prelude::{
    ActorExt, ChildOrigin, ActorRetirement, StopOnShutdown, TerminalProjection,
};

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

struct LifecycleActor;

#[bombay::actor(message = Never)]
impl LifecycleActor {}

struct Lifecycle;

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Lifecycle {
        origin: ChildOrigin<StopOnShutdown<Root>, Lifecycle>,
        terminal: ActorRetirement<StopOnShutdown<LifecycleActor>, Self, ()>,
    },
}

fn main() {
    let _root = Root.stop_on_shutdown();
}
