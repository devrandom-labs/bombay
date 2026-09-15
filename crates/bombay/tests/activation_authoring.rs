use bombay::prelude::*;
use bombay::testing::InfallibleResultExt;

struct Marker;

impl Protocol for Marker {
    type Addr = MailAddr;
    type Msg = u8;
}

struct Counter {
    value: u8,
}

#[bombay::behavior::behavior(
    addr = MailAddr,
    message = u8,
    sends = { markers: Vec<Delivery<Marker>> },
)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "the owning Behavior fold has a typed sender and controlled-error result"
)]
impl Counter {
    fn receive(&mut self, _: MailAddr, value: u8) -> BehaviorActed<Self> {
        self.value += value;
        Ok(Actions::cont().send_markers(Delivery::new(Recipient::global(MailAddr(9)), self.value)))
    }
}

#[test]
fn consuming_activation_preserves_the_explicit_initialization_and_transition_trace() {
    let mut explicit = Counter { value: 2 };
    let explicit_initialization = bombay::behavior::initialize(&mut explicit).infallible();

    let initialized = Counter { value: 2 }.initialize().infallible();
    let active_initialization = initialized.actions;
    let mut active = initialized.behavior;

    assert!(matches!(explicit_initialization.become_, Step::Continue));
    assert!(matches!(active_initialization.become_, Step::Continue));
    assert!(explicit_initialization.sends.markers.is_empty());
    assert!(active_initialization.sends.markers.is_empty());

    let explicit_actions = explicit.receive(MailAddr(3), 4).infallible();
    let active_actions = active.receive(MailAddr(3), 4).infallible();

    assert_eq!(explicit.value, active.value);
    assert_eq!(
        explicit_actions.sends.markers[0].message,
        active_actions.sends.markers[0].message
    );
    assert_eq!(
        explicit_actions.sends.markers[0].to,
        active_actions.sends.markers[0].to
    );
    assert_eq!(explicit_actions.become_, active_actions.become_);
}

#[test]
fn active_behavior_cannot_be_initialized_twice() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/active_cannot_initialize_again.rs");
}
