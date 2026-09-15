use core::marker::PhantomData;

use bombay::prelude::*;
use bombay::behavior::Behavior;

struct Replies;

impl Protocol for Replies {
    type Addr = MailAddr;
    type Msg = u64;
}

struct Envelope<T>(PhantomData<T>);
struct GenericReplies<T>(PhantomData<T>);

impl<T> Protocol for GenericReplies<T> {
    type Addr = MailAddr;
    type Msg = Envelope<T>;
}

struct Counter {
    value: u64,
}

#[bombay::behavior::behavior(
    addr = MailAddr,
    message = u8,
    sends = { replies: Vec<Delivery<Replies>> },
)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "the owning Behavior fold has a typed sender and controlled-error result"
)]
impl Counter {
    fn receive(&mut self, _: MailAddr, message: u8) -> BehaviorActed<Self> {
        self.value += u64::from(message);
        Ok(Actions::cont().send_replies(Delivery::new(
            Recipient::global(MailAddr(9)),
            self.value,
        )))
    }
}

fn require_protocol<P: Protocol<Addr = MailAddr, Msg = u64>>() {}
fn require_generic_protocol<P: Protocol<Addr = MailAddr, Msg = Envelope<u8>>>() {}
fn require_behavior<B: Behavior<Protocol = Counter>>() {}

struct Child;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unused_self,
    reason = "the owning Behavior fold requires the impossible typed receiver"
)]
impl Child {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

struct Parent;

#[bombay::behavior::behavior(
    addr = MailAddr,
    message = Never,
    births = { child: Child },
)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unused_self,
    reason = "the owning Behavior fold requires the impossible typed receiver"
)]
impl Parent {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn main() {
    require_protocol::<Replies>();
    require_generic_protocol::<GenericReplies<u8>>();
    require_behavior::<Counter>();
    let _child_role = ParentChild::Child;
}
