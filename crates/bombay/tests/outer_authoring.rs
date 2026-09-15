use core::time::Duration;

use bombay::actors::ActorExt;
use bombay::behavior::{NoBirths, NoSends, User, delegate_transition};
use bombay::prelude::*;
use bombay::testing::InfallibleResultExt;
use bombay::timing::ReceiveTimeout;

struct ActorCounter {
    value: u64,
}

#[bombay::actor]
impl ActorCounter {
    fn receive(&mut self, message: u8) -> BehaviorActed<Self> {
        self.value += u64::from(message);
        Ok(Actions::cont())
    }
}

struct OwnerCounter {
    value: u64,
}

#[bombay::behavior::behavior(addr = MailAddr, message = u8)]
impl OwnerCounter {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the owner comparison fold is infallible"
    )]
    fn receive(&mut self, _: MailAddr, message: u8) -> BehaviorActed<Self> {
        self.value += u64::from(message);
        Ok(Actions::cont())
    }
}

fn stop_counter(_: &mut ActorCounter) -> Actions<MailAddr, Never, NoSends, NoBirths> {
    Actions::stop()
}

fn same_type<T>(_: &T, _: &T) {}

#[test]
fn actor_facade_preserves_the_owning_fold_and_enters_exact_templates() {
    let mut actor = ActorCounter { value: 3 };
    let mut owning = OwnerCounter { value: 3 };

    let actor_actions = delegate_transition(&mut actor, User::new(MailAddr(1), 4)).infallible();
    let owning_actions = delegate_transition(&mut owning, User::new(MailAddr(1), 4)).infallible();

    assert_eq!(actor.value, owning.value);
    assert_eq!(actor_actions.become_, owning_actions.become_);
    assert!(actor_actions.sends == owning_actions.sends);

    let actor = ActorCounter { value: 0 };
    let fluent = actor
        .with_receive_timeout(TimerId(1), Duration::from_millis(1), stop_counter)
        .stop_on_shutdown();
    let direct = StopOnShutdown::new(ReceiveTimeout::new(
        ActorCounter { value: 0 },
        TimerId(1),
        Duration::from_millis(1),
        stop_counter,
    ));

    same_type(&fluent, &direct);
}

#[test]
fn actor_facade_compile_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/actor_facade_full.rs");
    cases.compile_fail("tests/compile/fail/actor_bad_arity.rs");
    cases.compile_fail("tests/compile/fail/actor_duplicate_argument.rs");
    cases.compile_fail("tests/compile/fail/actor_message_is_inferred.rs");
    cases.compile_fail("tests/compile/fail/actor_unknown_argument.rs");
}
