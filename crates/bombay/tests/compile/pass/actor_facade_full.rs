use core::marker::PhantomData;

use bombay::actors::ActorExt;
use bombay::behavior::{Behavior, ChildRole};
use bombay::prelude::*;

struct Reply;

impl Protocol for Reply {
    type Addr = MailAddr;
    type Msg = u64;
}

struct GenericState<T> {
    marker: PhantomData<T>,
    total: u64,
}

#[bombay::actor]
impl<T: Send + 'static> GenericState<T> {
    fn receive(&mut self, value: u8) -> BehaviorActed<Self> {
        self.total += u64::from(value);
        Ok(Actions::cont())
    }
}

#[derive(Debug)]
struct ReplyError;

struct SenderAware;

#[bombay::actor(
    sends = { replies: Vec<Delivery<Reply>> },
    error = ReplyError,
)]
impl SenderAware {
    fn receive(&mut self, from: MailAddr, value: u64) -> BehaviorActed<Self> {
        let _ = self;
        Ok(Actions::cont().send_replies(Delivery::new(
            Recipient::global(from),
            value,
        )))
    }
}

struct Child;

#[bombay::actor(message = Never)]
impl Child {}

struct Parent;

#[bombay::actor(
    message = Never,
    births = {
        primary: Child,
        fallback: Child,
    },
)]
impl Parent {}

fn same_type<T>(_: &T, _: &T) {}

fn accepts_role<R: ChildRole<Parent, Child = Child>>(_: R) {}

fn main() {
    let state = GenericState::<u8> {
        marker: PhantomData,
        total: 0,
    };
    let wrapped = state.stop_on_shutdown();
    same_type(&wrapped, &StopOnShutdown::new(GenericState::<u8> {
        marker: PhantomData,
        total: 0,
    }));
    accepts_role(ParentChild::Primary);
    accepts_role(ParentChild::Fallback);

    fn accepts_behavior<B: Behavior>() {}
    accepts_behavior::<SenderAware>();
}
