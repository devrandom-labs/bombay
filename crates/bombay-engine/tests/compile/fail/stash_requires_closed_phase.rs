use behavior::{
    Actions, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths, Stash, User,
};

struct OpenPhase;

impl Behavior for OpenPhase {
    type Protocol = behavior::MessageProtocol<MailAddr, ()>;
    type Event = User<MailAddr, ()>;
    type Sends = Vec<Never>;
    type Ph = u8;
    type Error = core::convert::Infallible;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn main() {
    let _ = Stash::new(OpenPhase, |_| behavior::StashRoute::Deliver);
}
