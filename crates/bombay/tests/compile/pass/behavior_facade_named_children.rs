use bombay::prelude::*;
use bombay::behavior::{Behavior, ChildRoute, ObserveChild, ObserveCreation, ShutdownChild};

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
    let route = ChildRoute::<Worker, SystemChildrenWorkers>::new(7);
    let delivery = ChildDelivery::<Worker, SystemChildrenWorkers>::at(route, 9);
    let creation_observation = ObserveCreation::<MailAddr, SystemChildrenWorkers>::at(route);
    let child_observation = ObserveChild::<MailAddr, SystemChildrenWorkers>::at(route);
    let shutdown = ShutdownChild::<Worker, SystemChildrenWorkers>::at(route);
    let creations = Children::<MailAddr>::new()
        .create(route.birth(Worker))
        .into_creates()
        .expect("one declared child route cannot collide");
    let staged = creations
        .into_iter()
        .next()
        .expect("the worker creation is retained");

    assert_eq!(delivery.nonce, route.nonce());
    assert_eq!(delivery.message, 9);
    assert_eq!(staged.nonce, route.nonce());
    assert_eq!(creation_observation.nonce, route.nonce());
    assert_eq!(child_observation.nonce, route.nonce());
    assert_eq!(shutdown.nonce, route.nonce());
}
