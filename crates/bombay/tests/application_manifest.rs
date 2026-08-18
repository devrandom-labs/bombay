use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bombay::prelude::*;

#[test]
fn application_manifest_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/topology/pass/*.rs");
    cases.compile_fail("tests/topology/fail/*.rs");
}

struct Child {
    retired: Arc<AtomicBool>,
}

impl Drop for Child {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::SeqCst);
    }
}

impl Behavior for Child {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

struct Root {
    retired: Arc<AtomicBool>,
}

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
            vec![Create::birth(
                1,
                StopOnShutdown::new(Child {
                    retired: self.retired.clone(),
                }),
            )],
            Step::Stop(Stopped),
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

application! {
    topology OneChildTopology for Root {
        hosted {}
    }
}

#[test]
fn public_run_creates_one_child_value_and_waits_for_its_retirement() {
    let retired = Arc::new(AtomicBool::new(false));

    run(Root {
        retired: retired.clone(),
    })
    .unwrap();

    assert!(retired.load(Ordering::SeqCst));
}

struct TimerRoot;

impl Behavior for TimerRoot {
    type Protocol = behavior::MessageProtocol<MailAddr, Never>;
    type Event = TimedEvent<User<MailAddr, Never>>;
    type Sends = InterpreterRequests<ScheduleAfter>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::new(
            InterpreterRequests::one(ScheduleAfter::new(
                TimerId(1),
                TimerGeneration(1),
                std::time::Duration::from_millis(1),
            )),
            Vec::new(),
            Step::Continue,
        ))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            TimedEvent::Owned(elapsed)
                if elapsed == TimerElapsed::new(TimerId(1), TimerGeneration(1)) =>
            {
                Ok(Actions::stop())
            }
            TimedEvent::Owned(_) => Ok(Actions::cont()),
            TimedEvent::Inner(user) => match user.message {},
        }
    }
}

application! {
    topology TimerTopology for TimerRoot {
        hosted {}
    }
}

#[test]
fn public_run_drives_actor_local_timers_without_an_auxiliary_task() {
    run(TimerRoot).unwrap();
}
