use bombay::behavior::{ChildRole, Never};
use bombay::{ActorOrigin, ActorRetirement, ProjectTerminal, TerminalProjection};

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Parent;

#[bombay::actor(
    message = Never,
    births = {
        primary: Worker,
        replica: Worker,
    },
)]
impl Parent {}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Replica {
        origin: ActorOrigin<Parent, ParentChildrenReplica>,
        terminal: ActorRetirement<Worker, Self>,
    },
}

type PrimaryPosition = <ParentChildrenPrimary as ChildRole<Parent>>::Position;

fn require_wrong_role<T>()
where
    T: ProjectTerminal<
            ActorOrigin<Parent, PrimaryPosition>,
            ActorRetirement<Worker, ApplicationTerminal>,
        >,
{
}

fn main() {
    require_wrong_role::<ApplicationTerminal>();
}
