use bombay::behavior::Never;
use bombay::{ActorRetirement, ChildOrigin, TerminalProjection};

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct OtherWorker;

#[bombay::actor(message = Never)]
impl OtherWorker {}

struct Parent;

#[bombay::actor(
    message = Never,
    births = { worker: Worker },
    creation_settlements = retain_for_retirement,
)]
impl Parent {}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    #[declared_child(Parent, ParentChildrenWorker, OtherWorker)]
    Worker {
        origin: ChildOrigin<Parent, ParentChildrenWorker>,
        terminal: ActorRetirement<OtherWorker, Self>,
    },
}

fn main() {}
