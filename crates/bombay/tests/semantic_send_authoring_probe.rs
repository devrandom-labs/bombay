//! Regression coverage for the owning Behavior macro's semantic action methods.
//!
//! This intentionally consumes the generated extension trait rather than
//! recreating it in Bombay. The test is an integration-level contract check:
//! every action verdict keeps its original creation and next-behavior legs
//! while appending only to the declared send lane.

use bombay::behavior::{Births, NoBirths, SendEffects, Stopped};
use bombay::prelude::*;

struct CounterValue;

impl Protocol for CounterValue {
    type Addr = MailAddr;
    type Msg = u64;
}

#[allow(dead_code, reason = "macro expansion subject for the authoring probe")]
struct Counter;

#[bombay::behavior::behavior(
    addr = MailAddr,
    message = Never,
    sends = { values: Vec<Delivery<CounterValue>> },
)]
impl Counter {
    #[allow(
        dead_code,
        clippy::unused_self,
        reason = "generated Behavior requires the receive fold"
    )]
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[test]
fn semantic_method_is_exactly_the_existing_typed_send_product() {
    let recipient = Recipient::global(MailAddr(9));
    let delivery = Delivery::new(recipient, 41);

    let ergonomic: Actions<MailAddr, Never, CounterSends, NoBirths> =
        Actions::cont().send_values(delivery.clone());

    let mut sends = CounterSends::empty();
    sends.send(delivery);
    let explicit: Actions<MailAddr, Never, CounterSends, NoBirths> = Actions::send(sends);

    assert_eq!(ergonomic.sends.values.len(), 1);
    assert_eq!(explicit.sends.values.len(), 1);
    assert_eq!(ergonomic.sends.values[0].message, 41);
    assert_eq!(ergonomic.sends.values[0].to, explicit.sends.values[0].to);
    assert_eq!(ergonomic.creates, explicit.creates);
    assert_eq!(ergonomic.become_, explicit.become_);
}

#[test]
fn semantic_method_preserves_every_action_verdict() {
    let recipient = Recipient::global(MailAddr(9));
    let delivery = |value| Delivery::new(recipient, value);

    let continued: Actions<MailAddr, Never, CounterSends, NoBirths> =
        Actions::cont().send_values(delivery(1));
    assert_eq!(continued.become_, Step::Continue);

    let changed: Actions<MailAddr, u8, CounterSends, NoBirths> =
        Actions::goto(7).send_values(delivery(2));
    assert_eq!(changed.become_, Step::Goto(7));

    let stopped: Actions<MailAddr, u8, CounterSends, NoBirths> =
        Actions::stop().send_values(delivery(3));
    assert_eq!(stopped.become_, Step::Stop(Stopped));

    let created: Actions<MailAddr, Never, CounterSends, Births<()>> = Actions::create(Vec::new())
        .send_values(delivery(4))
        .send_values(delivery(5));
    assert!(created.creates.is_empty());
    assert_eq!(created.sends.values.len(), 2);
    assert_eq!(created.become_, Step::Continue);
}
