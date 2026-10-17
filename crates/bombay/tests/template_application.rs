use bombay::ActorNotificationReceipts;
use core::time::Duration;
use std::time::Instant;

use behavior_actors::{
    Activate, Machine, Move, OwnedTimerElapsed, StashRoute, StopOnShutdown, TimerGeneration,
    UnlimitedStash,
};
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Become, Behavior, BehaviorActed, BehaviorBase, Never, NoBirths, Protocol,
    Step, Stopped, User,
};
use bombay::timing::{Deadline, OneShot, Periodic, ReceiveTimeout};
use bombay::{ActorRetirement, Application, MailAddr, WorkOutcome};

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

#[tokio::test(flavor = "current_thread")]
async fn one_shot_template_runs_as_an_ordinary_application_root() {
    let root = OneShot::new(TimerRoot, Duration::from_millis(1), stop_after_timer);

    let application_outcome = Application::new(root.stop_on_shutdown())
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        })
        .into_parts();
    if let (
        WorkOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let (
        WorkOutcome::Completed {
            output: None,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: RootTerminal<_> = (origin, retirement);
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn receive_timeout_template_runs_as_an_ordinary_application_root() {
    let root = ReceiveTimeout::new(TimerRoot, Duration::from_millis(1), stop_after_timer);

    let application_outcome = Application::new(root.stop_on_shutdown())
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        })
        .into_parts();
    if let (
        WorkOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let (
        WorkOutcome::Completed {
            output: None,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: RootTerminal<_> = (origin, retirement);
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn periodic_template_runs_as_an_ordinary_application_root() {
    let root = Periodic::new(TimerRoot, Duration::from_millis(1), stop_after_timer);

    let application_outcome = Application::new(root.stop_on_shutdown())
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        })
        .into_parts();
    if let (
        WorkOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let (
        WorkOutcome::Completed {
            output: None,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: RootTerminal<_> = (origin, retirement);
    assert_completed(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn deadline_template_runs_as_an_ordinary_application_root() {
    let root = Deadline::new(
        TimerRoot,
        Some(Instant::now() + Duration::from_millis(1)),
        stop_at_deadline,
    );

    let application_outcome = Application::new(root.stop_on_shutdown())
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        })
        .into_parts();
    if let (
        WorkOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let (
        WorkOutcome::Completed {
            output: None,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: RootTerminal<_> = (origin, retirement);
    assert_completed(terminal, None);
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

#[tokio::test(flavor = "current_thread")]
async fn machine_template_runs_as_an_ordinary_application_root() {
    let root = Machine::<MailAddr, _, _, _, _>::new(0, MachinePhase::Closed, machine_transition);

    let (
        WorkOutcome::Completed {
            output: (),
            cleanup: Ok(()),
        },
        Ok((root_origin, joined_actor)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = Application::new(root.stop_on_shutdown())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the machine reaches its statically defined terminal transition");
        })
        .into_parts()
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: RootTerminal<_> = (
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_completed(terminal, None);
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "the owning Stash route contract borrows every message, including Never"
)]
const fn deliver_never(_: &TimerRoot, message: &Never) -> StashRoute {
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
        &TimerRoot.with_one_shot(delay, stop_after_timer),
        &OneShot::new(TimerRoot, delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.with_periodic(delay, stop_after_timer),
        &Periodic::new(TimerRoot, delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.with_deadline(None, stop_at_deadline),
        &Deadline::new(TimerRoot, None, stop_at_deadline),
    );
    same_type(
        &TimerRoot.with_receive_timeout(delay, stop_after_timer),
        &ReceiveTimeout::new(TimerRoot, delay, stop_after_timer),
    );
    same_type(
        &TimerRoot.stop_on_shutdown(),
        &StopOnShutdown::new(TimerRoot),
    );
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::manual_assert_eq,
    reason = "Compare complete typed transitions without adding Debug requirements to their owning types."
)]
async fn fluent_template_composition_is_the_exact_existing_wrapper_stack() {
    let delay = Duration::from_millis(1);
    let fluent = UnlimitedStash::new(TimerRoot, deliver_never)
        .with_receive_timeout(delay, stop_wrapped::<UnlimitedStash<TimerRoot>>)
        .stop_on_shutdown();
    let direct = StopOnShutdown::new(ReceiveTimeout::new(
        UnlimitedStash::new(TimerRoot, deliver_never),
        delay,
        stop_wrapped::<UnlimitedStash<TimerRoot>>,
    ));

    same_type(&fluent, &direct);

    let mut fluent = fluent
        .initialize()
        .expect("the fluent stack initializes through the owning wrappers");
    let mut direct = direct
        .initialize()
        .expect("the direct stack initializes through the owning wrappers");
    assert!(fluent.actions == direct.actions);

    let elapsed = OwnedTimerElapsed::new(TimerGeneration(0));
    let fluent_elapsed = fluent
        .behavior
        .on_path(elapsed)
        .expect("the fluent stack accepts its exact timer generation");
    let direct_elapsed = direct
        .behavior
        .on_path(elapsed)
        .expect("the direct stack accepts its exact timer generation");
    assert!(fluent_elapsed == direct_elapsed);

    let runtime = UnlimitedStash::new(TimerRoot, deliver_never)
        .with_receive_timeout(delay, stop_wrapped::<UnlimitedStash<TimerRoot>>)
        .stop_on_shutdown();
    let application_outcome = Application::new(runtime)
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        })
        .into_parts();
    if let (
        WorkOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let (
        WorkOutcome::Completed {
            output: None,
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
        (),
    ) = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: RootTerminal<_> = (origin, retirement);
    assert_completed(terminal, None);
}
