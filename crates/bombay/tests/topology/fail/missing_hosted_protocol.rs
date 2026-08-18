use bombay::prelude::*;

struct Child;

impl Behavior for Child {
    type Protocol = behavior::MessageProtocol<MailAddr, u8>;
    type Event = User<MailAddr, u8>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

struct Root;

impl Behavior for Root {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<StopOnShutdown<Child>>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::new(
            Vec::new(),
            vec![Create::birth(1, StopOnShutdown::new(Child))],
            Step::Stop(Stopped),
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

application! {
    topology MissingChildTopology for Root {
        hosted {}
    }
}

#[bombay::main]
fn main() {
    Root
}
