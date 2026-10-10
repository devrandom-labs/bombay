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
    sends = pub(crate) { replies: Vec<Delivery<Reply>> },
    error = ReplyError,
)]
impl SenderAware {
    fn receive(&mut self, from: MailAddr, value: u64) -> BehaviorActed<Self> {
        let _ = self;
        Ok(Actions::cont().send_replies(Delivery::new(Recipient::global(from), value)))
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
    creation_settlements = retain_for_retirement,
)]
impl Parent {}

// Declaring a child's reports does not promise a runnable local interpreter.
type Proxy = bombay::atomic::StableProxy<Child, bombay::atomic::ImmediateActivation>;

struct ProxyParent;

#[bombay::actor(
    message = Never,
    births = { service: Proxy },
    creation_settlements = retain_for_retirement,
)]
impl ProxyParent {}

// Existing birth modes retain the same pure contract, including closed sums.
type Services = bombay::behavior::Births<Proxy>;
type MixedServices = bombay::behavior::Births<
    bombay::behavior::ChildChoice<Child, bombay::behavior::ChildChoice<Proxy, Never>>,
>;

struct ServiceParent;
#[bombay::actor(message = Never, births = Services, creation_settlements = retain_for_retirement)]
impl ServiceParent {}

struct MixedServiceParent;
#[bombay::actor(message = Never, births = MixedServices, creation_settlements = retain_for_retirement)]
impl MixedServiceParent {}

// Sender inference remains usable for pure Behavior authoring without a local host.
struct SenderIndependent {
    total: u64,
}

#[bombay::actor]
impl SenderIndependent {
    fn receive(&mut self, from: bombay::behavior::MailAddr, value: u8) -> BehaviorActed<Self> {
        self.total += from.0 + u64::from(value);
        Ok(Actions::cont())
    }
}

fn same_type<T>(_: &T, _: &T) {}

fn accepts_role<R: ChildRole<Parent, Child = Child>>(_: R) {}

fn main() {
    let state = GenericState::<u8> {
        marker: PhantomData,
        total: 0,
    };
    let wrapped = state.stop_on_shutdown();
    same_type(
        &wrapped,
        &StopOnShutdown::new(GenericState::<u8> {
            marker: PhantomData,
            total: 0,
        }),
    );
    accepts_role(ParentChild::Primary);
    accepts_role(ParentChild::Fallback);

    let mut proxy_parent = ProxyParent;
    let pure_parent = bombay::behavior::initialize(&mut proxy_parent)
        .expect("declared birth capabilities preserve pure authoring");
    assert!(pure_parent.creates.is_empty());
    assert!(matches!(pure_parent.sends, bombay::behavior::NoSends));
    assert!(matches!(pure_parent.become_, Step::Continue));

    let services = bombay::behavior::initialize(&mut ServiceParent)
        .expect("an existing birth mode preserves pure authoring");
    assert!(services.creates.is_empty());
    assert!(matches!(services.sends, bombay::behavior::NoSends));
    assert!(matches!(services.become_, Step::Continue));

    let mixed_services = bombay::behavior::initialize(&mut MixedServiceParent)
        .expect("a heterogeneous birth mode preserves pure authoring");
    assert!(mixed_services.creates.is_empty());
    assert!(matches!(mixed_services.sends, bombay::behavior::NoSends));
    assert!(matches!(mixed_services.become_, Step::Continue));

    fn accepts_behavior<B: Behavior>() {}
    accepts_behavior::<SenderAware>();

    let mut sender_independent = SenderIndependent { total: 0 };
    let initialization = bombay::behavior::initialize(&mut sender_independent)
        .expect("the pure actor initializes without a runtime");
    let transition = bombay::behavior::delegate_transition(
        &mut sender_independent,
        bombay::behavior::User::new(bombay::behavior::MailAddr(5), 7),
    )
    .expect("the inferred sender stays in its owning address domain");
    let expected = bombay::behavior::Actions::<
        bombay::behavior::MailAddr,
        bombay::behavior::Never,
        bombay::behavior::NoSends,
        bombay::behavior::NoBirths,
    >::cont();
    assert_eq!(initialization, expected);
    assert_eq!(transition, expected);
    assert_eq!(sender_independent.total, 12);
}
