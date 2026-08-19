use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bombay::prelude::*;

struct SingleProtocolActors<P: Protocol>(ActorSpace<P>);

impl<P> Default for SingleProtocolActors<P>
where
    P: Protocol,
    P::Addr: core::hash::Hash,
{
    fn default() -> Self {
        Self(ActorSpace::new())
    }
}

impl<P> Hosts<P> for SingleProtocolActors<P>
where
    P: Protocol,
    P::Addr: core::hash::Hash,
{
    fn space(&self) -> &ActorSpace<P> {
        &self.0
    }
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

#[test]
fn actor_system_creates_one_child_value_and_waits_for_its_retirement() {
    let retired = Arc::new(AtomicBool::new(false));

    App::new(
        Root {
            retired: retired.clone(),
        },
        SingleProtocolActors::default(),
    )
    .run()
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

#[test]
fn actor_system_drives_actor_local_timers_without_an_auxiliary_task() {
    App::new(TimerRoot, SingleProtocolActors::default())
        .run()
        .unwrap();
}

struct ShutdownChildProbe {
    retired: Arc<AtomicBool>,
}

impl Drop for ShutdownChildProbe {
    fn drop(&mut self) {
        assert!(!self.retired.swap(true, Ordering::SeqCst));
    }
}

impl Behavior for ShutdownChildProbe {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

type ManagedShutdownChild = StopOnShutdown<ShutdownChildProbe>;

struct ShutdownRoot {
    retired: Arc<AtomicBool>,
}

impl Behavior for ShutdownRoot {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = Births<ManagedShutdownChild>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::create(vec![Create::birth(
            1,
            StopOnShutdown::new(ShutdownChildProbe {
                retired: self.retired.clone(),
            }),
        )]))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

type CoordinatedShutdownRoot = ShutdownCoordinator<ShutdownRoot, ManagedShutdownChild>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "OneShot reactions use the template's exact fallible Behavior result signature"
)]
fn request_coordinated_shutdown(
    root: &mut CoordinatedShutdownRoot,
) -> BehaviorActed<CoordinatedShutdownRoot> {
    behavior::delegate_transition(root, ShutdownCoordinatorEvent::Requested(ShutdownRequested))
}

#[test]
fn shutdown_child_completion_advances_the_coordinator() {
    let retired = Arc::new(AtomicBool::new(false));
    let plan = ShutdownPlan::new([vec![1]]).unwrap();
    let root = ShutdownCoordinator::new(
        ShutdownRoot {
            retired: retired.clone(),
        },
        plan,
    );
    let application = OneShot::new(
        root,
        TimerId(1),
        std::time::Duration::from_millis(1),
        request_coordinated_shutdown,
    );

    App::new(application, SingleProtocolActors::default())
        .run()
        .unwrap();

    assert!(retired.load(Ordering::SeqCst));
}
