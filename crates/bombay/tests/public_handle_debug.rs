use core::fmt::Debug;

use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorActed, MessageProtocol, Never, NoBirths, User,
};
use bombay::{
    ActorInterface, ActorRef, App, ApplicationHandle, ApplicationLifecycle, ExternalActor,
    InstalledActor, MailAddr,
};

type PublicProtocol = MessageProtocol<MailAddr, ()>;

struct InstalledBehavior;

impl Behavior for InstalledBehavior {
    type Protocol = PublicProtocol;
    type Event = User<MailAddr, ()>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(bombay::behavior::Actions::cont())
    }
}

fn exposes_debug<Capability: Debug>() {}

#[test]
fn ordinary_public_handles_have_debug_without_exposing_private_capabilities() {
    exposes_debug::<ActorRef<PublicProtocol>>();
    exposes_debug::<InstalledActor<InstalledBehavior>>();
    exposes_debug::<ActorInterface<()>>();
    exposes_debug::<ExternalActor<PublicProtocol>>();
    exposes_debug::<ApplicationHandle<PublicProtocol, ()>>();
    exposes_debug::<ApplicationLifecycle<PublicProtocol, ()>>();
    exposes_debug::<App<(), ()>>();
}
