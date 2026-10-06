use bombay::behavior::{ChildRole, Never};
use bombay::{ChildOrigin, ActorRetirement, ProjectTerminal, TerminalProjection};

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Parent;

#[bombay::actor(
    message = Never,
    births = {
        primary: Worker,
        replica: Worker,
    }, creation_settlements = retain_for_retirement,
)]
impl Parent {}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    #[declared_child(Parent, ParentChildrenReplica, Worker)]
    Replica {
        origin: ChildOrigin<Parent, ParentChildrenReplica>,
        terminal: ActorRetirement<Worker, Self, ()>,
    },
}

type PrimaryPosition = <ParentChildrenPrimary as ChildRole<Parent>>::Position;

fn require_wrong_role<T>()
where
    T: ProjectTerminal<
            ChildOrigin<Parent, PrimaryPosition>,
            ActorRetirement<Worker, ApplicationTerminal, ()>,
        >,
{
}

fn main() {
    require_wrong_role::<ApplicationTerminal>();
}
