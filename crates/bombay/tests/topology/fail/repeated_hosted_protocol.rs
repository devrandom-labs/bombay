use bombay::application;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths,
    User,
};

struct Root;
struct Child;

macro_rules! inert {
    ($actor:ty) => {
        impl Behavior for $actor {
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
    };
}

inert!(Root);
inert!(Child);

application! {
    topology BadTopology for Root {
        hosted {
            behavior::MessageProtocol<MailAddr, ()>,
            behavior::MessageProtocol<MailAddr, ()>,
        }
    }
}

fn main() {}
