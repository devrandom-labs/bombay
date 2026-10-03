use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};

use behavior_actors::{
    FinalizeOnShutdown, ReportTerminalOutcome, RestartDenial, ShutdownRequested,
    SupervisionFailureReason,
};
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, InitializationTurn, InterpreterRequests, NoBirths, User,
};
use bombay::prelude::*;
use bombay::{ActorSpace, App};

mod application_support;

use application_support::{
    RootTerminal as ApplicationTerminal, assert_completed as assert_completed_root, into_root,
};

const TEST_BOUNDARY: MailAddr = MailAddr(u64::MAX);

#[test]
fn run_error_describes_its_exact_public_failure_variant() {
    let failure = RunError::<Never, Never>::Panicked;
    assert_eq!(format!("{failure:?}"), "Panicked");
    assert_eq!(failure.to_string(), "root initialization panicked");
}

#[test]
fn run_error_retains_the_executor_construction_source() {
    let failure = RunError::<Never, Never>::Runtime(io::Error::other("executor unavailable"));
    let source = failure.source().expect("the executor failure has a source");
    assert_eq!(source.to_string(), "executor unavailable");
}

#[derive(Debug, PartialEq, Eq)]
enum RootCommand {
    Stop,
}

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = RootCommand)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::unused_self,
    reason = "the owning Behavior fold has a typed sender and controlled-error result"
)]
impl Root {
    fn receive(&mut self, _: MailAddr, command: RootCommand) -> BehaviorActed<Self> {
        match command {
            RootCommand::Stop => Ok(Actions::stop()),
        }
    }
}

struct StopsDuringInitialization;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unused_self,
    reason = "the owning Behavior fold requires the impossible typed receiver"
)]
impl StopsDuringInitialization {
    #[allow(
        clippy::unnecessary_wraps,
        clippy::unused_self,
        reason = "the Behavior initialization contract requires this exact fallible receiver"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

struct WaitsForApplicationShutdown;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unused_self,
    reason = "the owning Behavior fold requires the impossible typed receiver"
)]
impl WaitsForApplicationShutdown {
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

struct ReportsTerminalOutcome;

#[bombay::actor(
    sends = pub(crate) { terminal_outcome: InterpreterRequests<ReportTerminalOutcome<MailAddr>> },
)]
impl ReportsTerminalOutcome {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn receive(&mut self, command: RootCommand) -> BehaviorActed<Self> {
        let RootCommand::Stop = command;
        Ok(ReportsTerminalOutcomeActions::send_terminal_outcome(
            Actions::stop(),
            ReportTerminalOutcome::new(Ok(Exit::LinkDied(MailAddr(19)))),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuingReportCommand {
    Report,
    Stop,
}

struct ContinuingTerminalReport;

#[bombay::actor(
    sends = pub(crate) { terminal_outcome: InterpreterRequests<ReportTerminalOutcome<MailAddr>> },
)]
impl ContinuingTerminalReport {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn receive(&mut self, command: ContinuingReportCommand) -> BehaviorActed<Self> {
        match command {
            ContinuingReportCommand::Report => {
                Ok(ContinuingTerminalReportActions::send_terminal_outcome(
                    Actions::cont(),
                    ReportTerminalOutcome::new(Ok(Exit::LinkDied(MailAddr(19)))),
                ))
            }
            ContinuingReportCommand::Stop => Ok(Actions::stop()),
        }
    }
}

enum ReportCommitment {
    Stop,
    Continue,
}

struct ReportsSupervisionOutcome {
    commitment: ReportCommitment,
}

#[bombay::actor(
    sends = pub(crate) { terminal_outcome: InterpreterRequests<ReportTerminalOutcome<MailAddr>> },
)]
impl ReportsSupervisionOutcome {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        match self.commitment {
            ReportCommitment::Stop => Ok(Actions::cont()),
            ReportCommitment::Continue => {
                let denial = RestartDenial::BudgetExceeded {
                    restarts_in_window: 2,
                    replacements_requested: 1,
                    maximum_restarts: 2,
                };
                Ok(ReportsSupervisionOutcomeActions::send_terminal_outcome(
                    Actions::cont(),
                    ReportTerminalOutcome::new(Ok(Exit::SupervisionFailed(
                        SupervisionFailureReason::RestartDenied(denial),
                    ))),
                ))
            }
        }
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "the generated fold has a controlled-error boundary"
    )]
    fn receive(&mut self, command: RootCommand) -> BehaviorActed<Self> {
        let RootCommand::Stop = command;
        let denial = RestartDenial::BudgetExceeded {
            restarts_in_window: 2,
            replacements_requested: 1,
            maximum_restarts: 2,
        };
        Ok(ReportsSupervisionOutcomeActions::send_terminal_outcome(
            Actions::stop(),
            ReportTerminalOutcome::new(Ok(Exit::SupervisionFailed(
                SupervisionFailureReason::RestartDenied(denial),
            ))),
        ))
    }
}

struct FinalizationNotice;

impl Protocol for FinalizationNotice {
    type Addr = MailAddr;
    type Msg = FinalizationEvent;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalizationEvent {
    Ready,
    Finalized,
}

struct FinalizationProbe {
    observer: Option<EstablishedRecipient<FinalizationNotice>>,
}

enum FinalizationCommand {
    Observe(EstablishedRecipient<FinalizationNotice>),
}

#[bombay::actor(
    sends = pub(crate) { notices: Vec<EstablishedDelivery<FinalizationNotice>> },
)]
impl FinalizationProbe {
    fn receive(&mut self, command: FinalizationCommand) -> BehaviorActed<Self> {
        let FinalizationCommand::Observe(observer) = command;
        self.observer = Some(observer.clone());
        Ok(Actions::cont()
            .send_notices(EstablishedDelivery::new(observer, FinalizationEvent::Ready)))
    }
}

fn record_finalization(
    probe: &mut FinalizationProbe,
    _: ShutdownRequested,
) -> Actions<MailAddr, Never, FinalizationProbeSends, NoBirths> {
    let observer = probe
        .observer
        .take()
        .expect("the finalization fixture installs one observer before shutdown");
    Actions::cont().send_notices(EstablishedDelivery::new(
        observer,
        FinalizationEvent::Finalized,
    ))
}

struct RejectsInitialization;

#[derive(Debug, PartialEq, Eq)]
struct InitializationFailure(u8);

impl Protocol for RejectsInitialization {
    type Addr = MailAddr;
    type Msg = Never;
}

#[derive(Debug)]
struct RootFailure;

struct FailsAfterActivation;

impl Protocol for FailsAfterActivation {
    type Addr = MailAddr;
    type Msg = RootCommand;
}

impl Behavior for FailsAfterActivation {
    type Protocol = Self;
    type Event = User<MailAddr, RootCommand>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = RootFailure;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let User { message, .. } = event;
        match message {
            RootCommand::Stop => Err(RootFailure),
        }
    }
}

impl BehaviorBase for FailsAfterActivation {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

impl Behavior for RejectsInitialization {
    type Protocol = Self;
    type Event = User<MailAddr, Never>;
    type Sends = Vec<Never>;
    type Ph = Never;
    type Error = InitializationFailure;
    type Birth = NoBirths;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        Err(InitializationFailure(47))
    }

    fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        let User { message, .. } = event;
        match message {}
    }
}

impl BehaviorBase for RejectsInitialization {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

#[test]
fn live_boundary_receives_root_once_and_returns_its_exact_value() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);

    let (rejected, terminal): (_, ApplicationTerminal<_>) =
        Application::new(Root.stop_on_shutdown())
            .run_with(move |application| async move {
                observed.fetch_add(1, Ordering::SeqCst);
                assert_eq!(application.root().address(), MailAddr::APPLICATION_ROOT);
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect("the live root admits its stop command");
                let termination = application.lifecycle().termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect_err("the terminated root rejects the exact command")
                    .into_message()
            })
            .expect("the root terminates normally");

    assert_eq!(rejected, RootCommand::Stop);
    assert_completed_root(terminal);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn boundary_owned_error_remains_an_unaggregated_output() {
    let (output, terminal): (_, ApplicationTerminal<_>) = Application::new(Root.stop_on_shutdown())
        .run_with(|application| async move {
            application
                .root()
                .send_from(TEST_BOUNDARY, RootCommand::Stop)
                .await
                .expect("the live root admits its stop command");
            Err::<Never, _>("boundary refused its own work")
        })
        .expect("the root terminates normally");

    assert_eq!(output, Err("boundary refused its own work"));
    assert_completed_root(terminal);
}

#[test]
fn run_is_the_unit_boundary_specialization() {
    let terminal: ApplicationTerminal<_> =
        match Application::new(StopsDuringInitialization.stop_on_shutdown()).run() {
            Err(RunError::Unpublished(terminal)) => terminal,
            _ => panic!("the direct run must retain the unpublished terminal"),
        };
    assert_completed_root(terminal);
    let terminal: ApplicationTerminal<_> =
        match Application::new(StopsDuringInitialization.stop_on_shutdown())
            .run_with(|_| async { 41_u8 })
        {
            Err(RunError::Unpublished(terminal)) => terminal,
            _ => panic!("the generic boundary must withhold the unpublished root"),
        };
    assert_completed_root(terminal);
}

#[test]
fn application_handle_separates_shutdown_request_from_exact_termination() {
    let (termination, terminal): (_, ApplicationTerminal<_>) =
        Application::new(WaitsForApplicationShutdown.stop_on_shutdown())
            .run_with(|application| async move {
                assert_eq!(application.root().address(), MailAddr(0));
                let handle_description = format!("{application:?}");
                assert!(handle_description.contains("ApplicationHandle"));
                assert!(handle_description.contains("MailAddr(0)"));
                assert!(handle_description.contains("families_type"));

                let lifecycle = application.lifecycle();
                let lifecycle_description = format!("{lifecycle:?}");
                assert!(lifecycle_description.contains("ApplicationLifecycle"));
                assert!(lifecycle_description.contains("MailAddr(0)"));
                let mut termination = pin!(lifecycle.termination());
                {
                    let waker = Waker::noop();
                    let mut context = Context::from_waker(waker);
                    let pending = termination.as_mut().poll(&mut context);
                    assert!(matches!(pending, Poll::Pending));
                }

                let accepted = lifecycle.request_shutdown();
                assert_eq!(accepted, Ok(()));
                let repeated = lifecycle.request_shutdown();
                assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
                let result = termination.await;
                let stopped = lifecycle.request_shutdown();
                (result, stopped)
            })
            .expect("the requested application shutdown terminates normally");

    assert_eq!(termination.0, Ok(Exit::Normal));
    assert_eq!(termination.1, Err(ShutdownRejection::AlreadyStopped));
    assert_completed_root(terminal);
}

#[test]
fn terminal_outcome_report_selects_the_exact_publication_before_stop() {
    let (termination, terminal): (_, ApplicationTerminal<_>) =
        Application::new(ReportsTerminalOutcome.stop_on_shutdown())
            .run_with(|application| async move {
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .unwrap();
                application.lifecycle().termination().await
            })
            .expect("the reported terminal outcome commits before the root stops");

    assert_eq!(termination, Ok(Exit::LinkDied(MailAddr(19))));
    assert_completed_root(terminal);
}

#[test]
fn continuing_report_cannot_select_the_later_stop_outcome() {
    let (termination, terminal): (_, ApplicationTerminal<_>) =
        Application::new(ContinuingTerminalReport.stop_on_shutdown())
            .run_with(|application| async move {
                let root = application.root();
                root.send_from(TEST_BOUNDARY, ContinuingReportCommand::Report)
                    .await
                    .expect("the first report action is admitted");
                root.send_from(TEST_BOUNDARY, ContinuingReportCommand::Stop)
                    .await
                    .expect("the later stop action is admitted");
                application.lifecycle().termination().await
            })
            .expect("the later stop publishes its own normal outcome");

    assert_eq!(termination, Ok(Exit::Normal));
    assert_completed_root(terminal);
}

#[test]
fn supervision_report_selects_the_typed_failure_publication_before_stop() {
    let denial = RestartDenial::BudgetExceeded {
        restarts_in_window: 2,
        replacements_requested: 1,
        maximum_restarts: 2,
    };
    let (termination, terminal): (_, ApplicationTerminal<_>) = Application::new(
        ReportsSupervisionOutcome {
            commitment: ReportCommitment::Stop,
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        application
            .root()
            .send_from(TEST_BOUNDARY, RootCommand::Stop)
            .await
            .unwrap();
        application.lifecycle().termination().await
    })
    .expect("the supervision failure commits before the root stops");

    assert_eq!(
        termination,
        Ok(Exit::SupervisionFailed(
            SupervisionFailureReason::RestartDenied(denial)
        ))
    );
    assert_completed_root(terminal);
}

#[test]
fn supervision_report_from_a_continuing_action_cannot_override_later_shutdown() {
    let (termination, terminal): (_, ApplicationTerminal<_>) = Application::new(
        ReportsSupervisionOutcome {
            commitment: ReportCommitment::Continue,
        }
        .stop_on_shutdown(),
    )
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let shutdown = lifecycle.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        let repeated = lifecycle.request_shutdown();
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        lifecycle.termination().await
    })
    .expect("the continuing report is discarded before the later shutdown action");

    assert_eq!(termination, Ok(Exit::Normal));
    assert_completed_root(terminal);
}

#[test]
fn run_delegates_shutdown_to_the_explicit_root_policy() {
    let (finalization, terminal): (_, ApplicationTerminal<_>) = Application::new(
        FinalizeOnShutdown::new(FinalizationProbe { observer: None }, record_finalization),
    )
    .run_with(|application| async move {
        let interface = application.interface(application.root().established_recipient());
        let lifecycle = application.lifecycle();
        let mut observer = interface
            .external::<FinalizationNotice>()
            .expect("the finalization observer is established");
        observer
            .send(
                interface.api(),
                FinalizationCommand::Observe(observer.recipient()),
            )
            .await
            .expect("the root accepts its finalization observer");
        let ready = observer.receive().await.map(|event| event.message);
        assert!(matches!(ready, Some(FinalizationEvent::Ready)));
        let shutdown = lifecycle.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        let repeated = lifecycle.request_shutdown();
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let finalization = observer.receive().await.map(|event| event.message);
        let termination = lifecycle.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
        finalization
    })
    .expect("the explicit finalization policy receives shutdown and terminates normally");
    assert_eq!(finalization, Some(FinalizationEvent::Finalized));
    assert_completed_root(terminal);
}

#[test]
fn activation_failure_does_not_invoke_the_boundary() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);

    let result: Result<
        (_, ApplicationTerminal<_>),
        RunError<InitializationFailure, ApplicationTerminal<_>>,
    > = Application::new(RejectsInitialization.stop_on_shutdown()).run_with(move |_| async move {
        observed.fetch_add(1, Ordering::SeqCst);
    });

    assert!(matches!(
        result,
        Err(RunError::InitializationRejected(InitializationFailure(47)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn behavior_failure_returns_the_exact_behavior_and_domain_error() {
    let ((), terminal): (_, ApplicationTerminal<_>) =
        Application::new(FailsAfterActivation.stop_on_shutdown())
            .run_with(|application| async move {
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect("the live root admits the failing command");
            })
            .expect("failure after activation is an exact root terminal, not a launch error");

    let (
        origin,
        ActorRetirement::BehaviorFailed {
            behavior: _behavior,
            settlements,
            control,
            user,
            descendants,
            error: RootFailure,
        },
    ) = into_root(terminal)
    else {
        panic!("the root behavior failure must remain an exact typed terminal")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(settlements.len(), 0);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
}

#[test]
fn application_delegates_to_the_explicit_single_space_app() {
    let (ordinary, ordinary_terminal): (_, ApplicationTerminal<_>) =
        Application::new(Root.stop_on_shutdown())
            .run_with(|application| async move {
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect("the ordinary application admits its stop command");
                let termination = application.lifecycle().termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect_err("the ordinary application rejects after termination")
                    .into_message()
            })
            .expect("the ordinary application terminates normally");

    let (explicit, explicit_terminal): (_, ApplicationTerminal<_>) =
        App::new(Root.stop_on_shutdown(), ActorSpace::new())
            .run_with(|application| async move {
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect("the explicit application admits its stop command");
                let termination = application.lifecycle().termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                application
                    .root()
                    .send_from(TEST_BOUNDARY, RootCommand::Stop)
                    .await
                    .expect_err("the explicit application rejects after termination")
                    .into_message()
            })
            .expect("the explicit application terminates normally");

    assert_eq!(ordinary, explicit);
    assert_completed_root(ordinary_terminal);
    assert_completed_root(explicit_terminal);
}

#[test]
fn run_with_protocol_is_compile_checked() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/shutdown_authority.rs");
    cases.compile_fail("tests/compile/fail/run_with_wrong_root_protocol.rs");
    cases.compile_fail("tests/compile/fail/actor_ref_has_no_shutdown.rs");
    cases.compile_fail("tests/compile/fail/application_lifecycle_wrong_event.rs");
    cases.compile_fail("tests/compile/fail/application_handle_has_no_direct_lifecycle.rs");
    cases.compile_fail("tests/compile/fail/app_local_is_not_an_ordinary_path.rs");
}
