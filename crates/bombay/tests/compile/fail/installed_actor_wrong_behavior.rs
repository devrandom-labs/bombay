use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorActed, EstablishedActor, Never, NoBirths, Protocol, User,
};
use bombay::{InstalledActor, MailAddr};

struct Wire;

impl Protocol for Wire {
    type Addr = MailAddr;
    type Msg = Never;
}

struct Primary;

impl Behavior for Primary {
    type Protocol = Wire;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct Secondary;

impl Behavior for Secondary {
    type Protocol = Wire;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

fn wrong_behavior(installed: InstalledActor<Primary>) -> EstablishedActor<Secondary> {
    EstablishedActor::issued(installed)
}

fn main() {}
