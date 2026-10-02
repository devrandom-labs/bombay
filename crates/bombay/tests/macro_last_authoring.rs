//! Macro-last comparison over one complete level-1 transition surface.
//!
//! The retained owning-macro and explicit forms below own the same state,
//! public protocol, initialization, typed reply, continuation, stop, and
//! domain error. The test compares public observable Actions rather than
//! private generated fields or token snapshots. A static builder records why
//! plain closure inference is not a complete named-effect replacement.

use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, BirthMode, NoBirths, SendEffects, SendsFor, User,
    delegate_transition, initialize,
};
use bombay::prelude::*;

struct CounterValue;

impl Protocol for CounterValue {
    type Addr = MailAddr;
    type Msg = u64;
}

enum CounterMessage {
    Increment,
    Read(Recipient<CounterValue>),
    Stop,
}

#[derive(Debug, PartialEq, Eq)]
struct CounterOverflow;

mod owner {
    use super::*;

    pub(super) struct Counter {
        pub(super) value: u64,
    }

    #[bombay::behavior::behavior(
        addr = MailAddr,
        message = CounterMessage,
        sends = pub(crate) { values: Vec<Delivery<CounterValue>> },
        error = CounterOverflow,
    )]
    impl Counter {
        #[allow(
            clippy::needless_pass_by_value,
            reason = "the owning Behavior fold receives the typed sender by value"
        )]
        pub(super) fn receive(
            &mut self,
            _: MailAddr,
            message: CounterMessage,
        ) -> BehaviorActed<Self> {
            match message {
                CounterMessage::Increment => {
                    self.value = self.value.checked_add(1).ok_or(CounterOverflow)?;
                    Ok(Actions::cont())
                }
                CounterMessage::Read(reply_to) => {
                    Ok(Actions::cont().send_values(Delivery::new(reply_to, self.value)))
                }
                CounterMessage::Stop => Ok(Actions::stop()),
            }
        }
    }
}

mod facade {
    use super::*;

    pub(super) struct Counter {
        pub(super) value: u64,
    }

    #[bombay::actor(
        sends = pub(crate) { values: Vec<Delivery<CounterValue>> },
        error = CounterOverflow,
    )]
    impl Counter {
        pub(super) fn receive(&mut self, message: CounterMessage) -> BehaviorActed<Self> {
            match message {
                CounterMessage::Increment => {
                    self.value = self.value.checked_add(1).ok_or(CounterOverflow)?;
                    Ok(Actions::cont())
                }
                CounterMessage::Read(reply_to) => {
                    Ok(Actions::cont().send_values(Delivery::new(reply_to, self.value)))
                }
                CounterMessage::Stop => Ok(Actions::stop()),
            }
        }
    }
}

struct ExplicitCounter {
    value: u64,
}

impl Protocol for ExplicitCounter {
    type Addr = MailAddr;
    type Msg = CounterMessage;
}

impl Behavior for ExplicitCounter {
    type Protocol = Self;
    type Event = User<MailAddr, CounterMessage>;
    type Sends = Vec<Delivery<CounterValue>>;
    type Ph = Never;
    type Error = CounterOverflow;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {
            CounterMessage::Increment => {
                self.value = self.value.checked_add(1).ok_or(CounterOverflow)?;
                Ok(Actions::cont())
            }
            CounterMessage::Read(reply_to) => {
                Ok(Actions::send(vec![Delivery::new(reply_to, self.value)]))
            }
            CounterMessage::Stop => Ok(Actions::stop()),
        }
    }
}

impl BehaviorBase for ExplicitCounter {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

mod plain_builder_candidate {
    use core::marker::PhantomData;

    use super::*;

    pub(super) struct Define<P>(PhantomData<fn() -> P>);

    impl<P: Protocol> Define<P> {
        pub(super) fn state<S>(state: S) -> Builder<P, S> {
            Builder {
                state,
                protocol: PhantomData,
            }
        }
    }

    pub(super) struct Builder<P, S> {
        state: S,
        protocol: PhantomData<fn() -> P>,
    }

    impl<P: Protocol, S> Builder<P, S> {
        pub(super) fn receive<Ph, Sends, Birth, Error, Receive>(
            self,
            receive: Receive,
        ) -> Fold<S, P, Ph, Sends, Birth, Error, Receive>
        where
            Sends: SendEffects + SendsFor<User<P::Addr, P::Msg>>,
            Birth: BirthMode,
            Receive:
                FnMut(&mut S, P::Addr, P::Msg) -> Result<Actions<P::Addr, Ph, Sends, Birth>, Error>,
        {
            Fold {
                state: self.state,
                receive,
                protocol: PhantomData,
                phase: PhantomData,
                sends: PhantomData,
                birth: PhantomData,
                error: PhantomData,
            }
        }
    }

    pub(super) struct Fold<S, P, Ph, Sends, Birth, Error, Receive> {
        state: S,
        receive: Receive,
        protocol: PhantomData<fn() -> P>,
        phase: PhantomData<fn() -> Ph>,
        sends: PhantomData<fn() -> Sends>,
        birth: PhantomData<fn() -> Birth>,
        error: PhantomData<fn() -> Error>,
    }

    impl<S, P, Ph, Sends, Birth, Error, Receive> Behavior
        for Fold<S, P, Ph, Sends, Birth, Error, Receive>
    where
        P: Protocol,
        Sends: SendEffects + SendsFor<User<P::Addr, P::Msg>>,
        Birth: BirthMode,
        Receive:
            FnMut(&mut S, P::Addr, P::Msg) -> Result<Actions<P::Addr, Ph, Sends, Birth>, Error>,
    {
        type Protocol = P;
        type Event = User<P::Addr, P::Msg>;
        type Sends = Sends;
        type Ph = Ph;
        type Error = Error;
        type Birth = Birth;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            (self.receive)(&mut self.state, event.from, event.message)
        }
    }

    impl<S, P, Ph, Sends, Birth, Error, Receive> BehaviorBase
        for Fold<S, P, Ph, Sends, Birth, Error, Receive>
    {
        type Base = S;

        fn base(&self) -> &Self::Base {
            &self.state
        }
    }
}

struct BuilderCounter;

impl Protocol for BuilderCounter {
    type Addr = MailAddr;
    type Msg = CounterMessage;
}

struct OwnerPathCounter;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl OwnerPathCounter {
    #[allow(
        clippy::unused_self,
        reason = "the owning Behavior fold requires the impossible receiver"
    )]
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[test]
fn owning_and_explicit_forms_have_the_same_initialization_and_domain_error() {
    let mut owner = owner::Counter { value: u64::MAX };
    let mut facade = facade::Counter { value: u64::MAX };
    let mut explicit = ExplicitCounter { value: u64::MAX };

    let owner_init = initialize(&mut owner).expect("default initialization succeeds");
    let facade_init = initialize(&mut facade).expect("default initialization succeeds");
    let explicit_init = initialize(&mut explicit).expect("default initialization succeeds");
    let expected_init: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::cont();
    let owner_init_values = owner_init.map_sends(|sends| sends.values);
    let facade_init_values = facade_init.map_sends(|sends| sends.values);
    assert!(owner_init_values == expected_init);
    assert!(facade_init_values == expected_init);
    assert!(explicit_init == expected_init);

    let owner_overflow = owner.receive(MailAddr(1), CounterMessage::Increment);
    assert!(matches!(owner_overflow, Err(CounterOverflow)));
    let facade_overflow = delegate_transition(
        &mut facade,
        User::new(MailAddr(1), CounterMessage::Increment),
    );
    let explicit_overflow = delegate_transition(
        &mut explicit,
        User::new(MailAddr(1), CounterMessage::Increment),
    );
    assert!(matches!(facade_overflow, Err(CounterOverflow)));
    assert!(matches!(explicit_overflow, Err(CounterOverflow)));
    assert_eq!(owner.value, u64::MAX);
    assert_eq!(facade.value, u64::MAX);
    assert_eq!(explicit.value, u64::MAX);
}

#[test]
fn owning_and_explicit_forms_preserve_increment_reply_and_stop() {
    let mut owner = owner::Counter { value: 0 };
    let mut facade = facade::Counter { value: 0 };
    let mut explicit = ExplicitCounter { value: 0 };

    let owner_increment = owner
        .receive(MailAddr(1), CounterMessage::Increment)
        .expect("increment succeeds");
    let facade_increment = delegate_transition(
        &mut facade,
        User::new(MailAddr(1), CounterMessage::Increment),
    )
    .expect("increment succeeds");
    let explicit_increment = delegate_transition(
        &mut explicit,
        User::new(MailAddr(1), CounterMessage::Increment),
    )
    .expect("increment succeeds");
    let expected_increment: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::cont();
    let owner_increment_values = owner_increment.map_sends(|sends| sends.values);
    let facade_increment_values = facade_increment.map_sends(|sends| sends.values);
    assert!(owner_increment_values == expected_increment);
    assert!(facade_increment_values == expected_increment);
    assert!(explicit_increment == expected_increment);
    assert_eq!((owner.value, facade.value, explicit.value), (1, 1, 1));

    let recipient = Recipient::global(MailAddr(2));
    let owner_read = owner
        .receive(MailAddr(1), CounterMessage::Read(recipient))
        .expect("read succeeds");
    let facade_read = delegate_transition(
        &mut facade,
        User::new(MailAddr(1), CounterMessage::Read(recipient)),
    )
    .expect("read succeeds");
    let explicit_read = delegate_transition(
        &mut explicit,
        User::new(MailAddr(1), CounterMessage::Read(recipient)),
    )
    .expect("read succeeds");

    let expected_read: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::send(vec![Delivery::new(recipient, 1)]);
    let owner_read_values = owner_read.map_sends(|sends| sends.values);
    let facade_read_values = facade_read.map_sends(|sends| sends.values);
    assert!(owner_read_values == expected_read);
    assert!(facade_read_values == expected_read);
    assert!(explicit_read == expected_read);

    let owner_stop = owner
        .receive(MailAddr(1), CounterMessage::Stop)
        .expect("stop succeeds");
    let facade_stop =
        delegate_transition(&mut facade, User::new(MailAddr(1), CounterMessage::Stop))
            .expect("stop succeeds");
    let explicit_stop =
        delegate_transition(&mut explicit, User::new(MailAddr(1), CounterMessage::Stop))
            .expect("stop succeeds");
    let expected_stop: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::stop();
    let owner_stop_values = owner_stop.map_sends(|sends| sends.values);
    let facade_stop_values = facade_stop.map_sends(|sends| sends.values);
    assert!(owner_stop_values == expected_stop);
    assert!(facade_stop_values == expected_stop);
    assert!(explicit_stop == expected_stop);
}

#[test]
fn owning_macro_receiver_mismatch_is_rejected_at_the_authored_method() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/behavior_owner_bad_receiver.rs");
}

#[test]
fn plain_builder_is_static_but_cannot_name_general_send_or_birth_products() {
    struct State {
        value: u64,
    }
    type Folded =
        Result<Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths>, CounterOverflow>;

    let mut counter = plain_builder_candidate::Define::<BuilderCounter>::state(State { value: 0 })
        .receive(|state, _: MailAddr, message| -> Folded {
            match message {
                CounterMessage::Increment => {
                    state.value = state.value.checked_add(1).ok_or(CounterOverflow)?;
                    Ok(Actions::cont())
                }
                CounterMessage::Read(reply_to) => {
                    Ok(Actions::send(vec![Delivery::new(reply_to, state.value)]))
                }
                CounterMessage::Stop => Ok(Actions::stop()),
            }
        });

    let increment = delegate_transition(
        &mut counter,
        User::new(MailAddr(1), CounterMessage::Increment),
    )
    .expect("increment succeeds");
    let expected_increment: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::cont();
    assert!(increment == expected_increment);
    let actions = delegate_transition(
        &mut counter,
        User::new(
            MailAddr(1),
            CounterMessage::Read(Recipient::global(MailAddr(2))),
        ),
    )
    .expect("read succeeds");

    assert_eq!(counter.base().value, 1);
    let expected_read: Actions<MailAddr, Never, Vec<Delivery<CounterValue>>, NoBirths> =
        Actions::send(vec![Delivery::new(Recipient::global(MailAddr(2)), 1)]);
    assert!(actions == expected_read);
}

#[test]
fn nested_owner_macro_is_available_beside_the_behavior_module() {
    fn accepts_behavior<B: Behavior>() {}

    accepts_behavior::<OwnerPathCounter>();
    let _: Option<bombay::behavior::Never> = None;
}
