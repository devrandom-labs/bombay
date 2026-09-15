use core::time::Duration;
use std::time::Instant;

use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, Activate, ActiveTurn, Become, Behavior, BehaviorActed, BehaviorBase, Machine, Move,
    Never, NoBirths, Protocol, Stash, StashRoute, Step, StopOnShutdown, Stopped, TimerElapsed,
    TimerGeneration, TimerId, User,
};
use bombay::timing::{Deadline, OneShot, Periodic, ReceiveTimeout};
use bombay::{Application, MailAddr};

mod application_support;

use application_support::{RootTerminal, assert_completed};

const TEST_BOUNDARY: MailAddr = MailAddr(u64::MAX);

struct TimerRoot;

impl Protocol for TimerRoot {
    type Addr = MailAddr;
    type Msg = Never;
}

impl Behavior for TimerRoot {
    type Protocol = Self;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = Never;
    type Birth = NoBirths;

    #[allow(
        clippy::unnecessary_wraps,
        reason = "the foundational fold retains its controlled-error boundary"
    )]
    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event.message {}
    }
}

impl BehaviorBase for TimerRoot {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the owning OneShot reaction retains the wrapped controlled-error boundary"
)]
fn stop_after_timer(_: &mut TimerRoot) -> Actions<MailAddr, Never, Vec<Never>, NoBirths> {
    Actions::stop()
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the owning Deadline reaction retains the wrapped controlled-error boundary"
)]
fn stop_at_deadline(_: &mut TimerRoot) -> Become {
    Step::Stop(Stopped)
}

#[test]
fn one_shot_template_runs_as_an_ordinary_application_root() {
    let root = OneShot::new(
        TimerRoot,
        TimerId(1),
        Duration::from_millis(1),
        stop_after_timer,
    );

    let terminal: RootTerminal<_> = Application::new(root.stop_on_shutdown())
        .run()
        .expect("the timer fires and the template stops normally");
    assert_completed(terminal);
}

#[test]
fn receive_timeout_template_runs_as_an_ordinary_application_root() {
    let root = ReceiveTimeout::new(
        TimerRoot,
        TimerId(2),
        Duration::from_millis(1),
        stop_after_timer,
    );

    let terminal: RootTerminal<_> = Application::new(root.stop_on_shutdown())
        .run()
        .expect("the idle timer fires and the template stops normally");
    assert_completed(terminal);
}

#[test]
fn periodic_template_runs_as_an_ordinary_application_root() {
    let root = Periodic::new(
        TimerRoot,
        TimerId(3),
        Duration::from_millis(1),
        stop_after_timer,
    );

    let terminal: RootTerminal<_> = Application::new(root.stop_on_shutdown())
        .run()
        .expect("the first periodic generation fires and the template stops normally");
    assert_completed(terminal);
}

#[test]
fn deadline_template_runs_as_an_ordinary_application_root() {
    let root = Deadline::new(
        TimerRoot,
        TimerId(4),
        Some(Instant::now() + Duration::from_millis(1)),
        stop_at_deadline,
    );

    let terminal: RootTerminal<_> = Application::new(root.stop_on_shutdown())
        .run()
        .expect("the absolute deadline fires and the template stops normally");
    assert_completed(terminal);
}

#[derive(Clone)]
enum MachineCommand {
    DeferredWork,
    Open,
    Stop,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MachinePhase {
    Closed,
    Open,
}

#[derive(Debug)]
struct MachineInvariant;

fn machine_transition(
    phase: MachinePhase,
    completed: &mut usize,
    command: &MachineCommand,
) -> Result<Move<MachinePhase>, MachineInvariant> {
    match (phase, command) {
        (MachinePhase::Closed, MachineCommand::DeferredWork) => Ok(Move::Defer),
        (MachinePhase::Closed, MachineCommand::Open) => Ok(Move::Goto(MachinePhase::Open)),
        (MachinePhase::Open, MachineCommand::DeferredWork) => {
            *completed += 1;
            Ok(Move::Stay)
        }
        (_, MachineCommand::Stop) if *completed == 1 => Ok(Move::Stop),
        (_, MachineCommand::Stop | MachineCommand::Open) => Err(MachineInvariant),
    }
}

#[test]
fn machine_template_runs_as_an_ordinary_application_root() {
    let root = Machine::<MailAddr, _, _, _, _>::new(0, MachinePhase::Closed, machine_transition);

    let ((), terminal): (_, RootTerminal<_>) = Application::new(root.stop_on_shutdown())
        .run_with(|application| async move {
            application
                .root()
                .send_from(TEST_BOUNDARY, MachineCommand::DeferredWork)
                .await
                .expect("the machine accepts deferred work");
            application
                .root()
                .send_from(TEST_BOUNDARY, MachineCommand::Open)
                .await
                .expect("the machine opens and drains deferred work");
            application
                .root()
                .send_from(TEST_BOUNDARY, MachineCommand::Stop)
                .await
                .expect("the machine accepts stop after draining");
        })
        .expect("the machine reaches its statically defined terminal transition");
    assert_completed(terminal);
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "the owning Stash route contract borrows every message, including Never"
)]
const fn deliver_never(message: &Never) -> StashRoute {
    match *message {}
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the owning timeout reaction retains the wrapped controlled-error boundary"
)]
fn stop_wrapped<B: Behavior>(
    _: &mut B,
) -> Actions<bombay::behavior::BehaviorAddr<B>, Never, B::Sends, B::Birth> {
    Actions::stop()
}

fn same_type<T>(_: &T, _: &T) {}

#[test]
fn every_fluent_method_returns_its_existing_owner_type() {
    let delay = Duration::from_millis(1);

    same_type(
        &TimerRoot.with_stash(deliver_never),
        &Stash::new(TimerRoot, deliver_never),
    );
    same_type(
        &TimerRoot.with_one_shot(TimerId(6), delay, stop_after_timer),
        &OneShot::new(TimerRoot, TimerId(6), delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.with_periodic(TimerId(7), delay, stop_after_timer),
        &Periodic::new(TimerRoot, TimerId(7), delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.with_deadline(TimerId(8), None, stop_at_deadline),
        &Deadline::new(TimerRoot, TimerId(8), None, stop_at_deadline),
    );
    same_type(
        &TimerRoot.with_receive_timeout(TimerId(9), delay, stop_after_timer),
        &ReceiveTimeout::new(TimerRoot, TimerId(9), delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.stop_on_shutdown(),
        &StopOnShutdown::new(TimerRoot),
    );
}

#[test]
fn fluent_template_composition_is_the_exact_existing_wrapper_stack() {
    let delay = Duration::from_millis(1);
    let fluent = TimerRoot
        .with_stash(deliver_never)
        .with_receive_timeout(TimerId(5), delay, stop_wrapped::<Stash<TimerRoot>>)
        .stop_on_shutdown();
    let direct = StopOnShutdown::new(ReceiveTimeout::new(
        Stash::new(TimerRoot, deliver_never),
        TimerId(5),
        delay,
        stop_wrapped::<Stash<TimerRoot>>,
    ));

    same_type(&fluent, &direct);

    let mut fluent = fluent
        .initialize()
        .expect("the fluent stack initializes through the owning wrappers");
    let mut direct = direct
        .initialize()
        .expect("the direct stack initializes through the owning wrappers");
    assert!(fluent.actions.sends == direct.actions.sends);
    assert_eq!(fluent.actions.creates.len(), direct.actions.creates.len());
    assert_eq!(fluent.actions.become_, direct.actions.become_);

    let elapsed = TimerElapsed::new(TimerId(5), TimerGeneration(0));
    let fluent_elapsed = fluent
        .behavior
        .on_path(elapsed)
        .expect("the fluent stack accepts its exact timer generation");
    let direct_elapsed = direct
        .behavior
        .on_path(elapsed)
        .expect("the direct stack accepts its exact timer generation");
    assert!(fluent_elapsed.sends == direct_elapsed.sends);
    assert_eq!(fluent_elapsed.creates.len(), direct_elapsed.creates.len());
    assert_eq!(fluent_elapsed.become_, direct_elapsed.become_);

    let runtime = TimerRoot
        .with_stash(deliver_never)
        .with_receive_timeout(TimerId(5), delay, stop_wrapped::<Stash<TimerRoot>>)
        .stop_on_shutdown();
    let terminal: RootTerminal<_> = Application::new(runtime)
        .run()
        .expect("the exact fluent wrapper stack runs as an ordinary application");
    assert_completed(terminal);
}
