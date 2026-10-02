use bombay::behavior::{ChildRole, Never};
use bombay::ChildOrigin;

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
    creation_settlements = retain_for_retirement,
)]
impl Parent {}

type PrimaryPosition = <ParentChildrenPrimary as ChildRole<Parent>>::Position;

fn change_role(
    origin: ChildOrigin<Parent, PrimaryPosition>,
) -> ChildOrigin<Parent, ParentChildrenReplica> {
    origin.into_declared_child::<ParentChildrenReplica>()
}

fn main() {}
