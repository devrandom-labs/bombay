use bombay::prelude::*;
use bombay::behavior::{Behavior, CreationSequence};

struct Worker;

#[bombay::behavior::behavior(addr = MailAddr, message = u8)]
impl Worker {
    fn receive(&mut self, _: MailAddr, _: u8) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct System;

#[bombay::behavior::behavior(
    addr = MailAddr,
    message = Never,
    births = { workers: Worker },
    creation_settlements = retain_for_retirement,
)]
impl System {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn accepts_child<Parent, Role>(_: Role, _: Role::Child)
where
    Parent: Behavior,
    Role: ChildRole<Parent>,
{
}

fn main() {
    accepts_child::<System, _>(SystemChild::Workers, Worker);
    let mut sequence = CreationSequence::new();
    let creation = sequence
        .issue()
        .expect("the worker creation ID exists");
    let delivery = ChildDelivery::<Worker, SystemChildrenWorkers>::after(creation, 9);
    let creations = Children::<MailAddr>::new()
        .child(creation, Worker)
        .into_creates();
    let staged = creations
        .into_iter()
        .next()
        .expect("the worker creation is retained");

    assert_eq!(delivery.creation, creation);
    assert_eq!(delivery.message, 9);
    assert_eq!(staged.id(), creation);
}
