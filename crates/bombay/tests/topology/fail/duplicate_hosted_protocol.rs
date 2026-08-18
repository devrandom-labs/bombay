use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths,
    User,
};
use bombay::application;

struct Root;
struct Child;

impl Behavior for Root {
    type Protocol = behavior::MessageProtocol<MailAddr, ()>;
    type Event = User<MailAddr, ()>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

impl Behavior for Child {
    type Protocol = behavior::MessageProtocol<MailAddr, ()>;
    type Event = User<MailAddr, ()>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

type ChildProtocol = behavior::MessageProtocol<MailAddr, ()>;
type ChildAlias = ChildProtocol;

application! {
    topology BadTopology for Root {
        hosted { ChildProtocol, ChildAlias }
    }
}

fn main() {}
