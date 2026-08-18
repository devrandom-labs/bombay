use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, InitializationTurn, MailAddr, Never, NoBirths,
    User,
};
use bombay::{Application, application};

struct Root;
struct Child;

macro_rules! behavior {
    ($name:ty, $message:ty) => {
        impl Behavior for $name {
            type Protocol = behavior::MessageProtocol<MailAddr, $message>;
            type Event = User<MailAddr, $message>;
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

behavior!(Root, ());
behavior!(Child, u8);
application! {
    topology TestTopology for Root {
        hosted { behavior::MessageProtocol<MailAddr, u8> }
    }
}

fn requires_application<T: Application>() {}

fn main() {
    requires_application::<Root>();
}
