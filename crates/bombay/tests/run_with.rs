use bombay::behavior::{NoSends, Step, Stopped};
use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};
#[cfg(all(feature = "axum", unix))]
use std::{env, fs::File, process::Command};
use tokio::runtime::Builder as TokioBuilder;

use behavior_actors::{
    FinalizeOnShutdown, ReportTerminalOutcome, RestartDenial, ShutdownRequested,
    SupervisionFailureReason,
};
use bombay::ProjectTerminal;
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, EventLayer, InitializationTurn, InterpreterRequests,
    NoBirths, User,
};
use bombay::prelude::*;
use bombay::{ActorSpace, App, ApplicationOutcome};

mod application_support;

use application_support::{
    RootTerminal as ApplicationTerminal, assert_completed as assert_completed_root, into_root,
};

const TEST_BOUNDARY: MailAddr = MailAddr(u64::MAX);

#[test]
fn run_error_describes_its_exact_public_failure_variant() {
    let failure = RunError::BlockingInEnteredRuntime;
    assert_eq!(format!("{failure:?}"), "BlockingInEnteredRuntime");
    assert_eq!(
        failure.to_string(),
        "blocking application execution is forbidden inside an entered Tokio runtime"
    );
}

#[test]
fn run_error_retains_the_executor_construction_source() {
    let failure = RunError::Runtime(io::Error::other("executor unavailable"));
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

#[tokio::test(flavor = "current_thread")]
async fn live_boundary_receives_root_once_and_returns_its_exact_value() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);

    let ApplicationOutcome::Completed {
        output: rejected,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(Root.stop_on_shutdown())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the root terminates normally");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(rejected, RootCommand::Stop);
    assert_completed_root(terminal, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn boundary_owned_error_remains_an_unaggregated_output() {
    let ApplicationOutcome::Completed {
        output,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(Root.stop_on_shutdown())
        .run_with(|application| async move {
            application
                .root()
                .send_from(TEST_BOUNDARY, RootCommand::Stop)
                .await
                .expect("the live root admits its stop command");
            Err::<Never, _>("boundary refused its own work")
        })
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the root terminates normally");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(output, Err("boundary refused its own work"));
    assert_completed_root(terminal, None);
}

#[test]
#[expect(
    clippy::drop_non_drop,
    reason = "explicitly release the recovered concrete input at this ownership boundary, before the following retry or failure"
)]
fn run_is_the_unit_boundary_specialization() {
    let terminal = {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the caller constructs its explicit current-thread host");
        let application_outcome = executor
            .block_on(
                Application::new(StopsDuringInitialization.stop_on_shutdown())
                    .run::<_, _, ApplicationTerminal<_>, _>(),
            )
            .unwrap_or_else(|(application, error)| {
                drop(application);
                panic!("the caller owns the application's live entered host: {error}");
            });
        let ApplicationOutcome::NotInvoked {
            work: None,
            startup_error: Some(_startup_error),
            cleanup: Ok((origin, retirement)),
        } = application_outcome
        else {
            drop(application_outcome);
            panic!("the original startup phase and complete joined root remain exact");
        };
        let terminal: ApplicationTerminal<_> = ProjectTerminal::project(origin, retirement);
        terminal
    };
    assert_completed_root(terminal, None);
    let terminal: ApplicationTerminal<_> = {
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the second caller constructs its explicit current-thread host");
        let returned = caller
            .block_on(
                Application::new(StopsDuringInitialization.stop_on_shutdown())
                    .run_with(|_| async { 41_u8 }),
            )
            .unwrap_or_else(|failed| {
                drop(failed);
                panic!("the actual supplied-work caller host is entered");
            });
        let ApplicationOutcome::NotInvoked {
            work,
            startup_error: Some(startup_error),
            cleanup: Ok((origin, retirement)),
        } = returned
        else {
            panic!("the generic boundary must withhold the unpublished root");
        };
        drop((work, startup_error));
        ProjectTerminal::project(origin, retirement)
    };
    assert_completed_root(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn application_handle_separates_shutdown_request_from_exact_termination() {
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(WaitsForApplicationShutdown.stop_on_shutdown())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the requested application shutdown terminates normally");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(termination.0, Ok(Exit::Normal));
    assert_eq!(termination.1, Err(ShutdownRejection::AlreadyStopped));
    assert_completed_root(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_outcome_report_selects_the_exact_publication_before_stop() {
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(ReportsTerminalOutcome.stop_on_shutdown())
        .run_with(|application| async move {
            application
                .root()
                .send_from(TEST_BOUNDARY, RootCommand::Stop)
                .await
                .unwrap();
            application.lifecycle().termination().await
        })
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the reported terminal outcome commits before the root stops");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(termination, Ok(Exit::LinkDied(MailAddr(19))));
    assert_completed_root(terminal, Some(()));
}

#[tokio::test(flavor = "current_thread")]
async fn continuing_report_cannot_select_the_later_stop_outcome() {
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(ContinuingTerminalReport.stop_on_shutdown())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the later stop publishes its own normal outcome");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(termination, Ok(Exit::Normal));
    assert_completed_root(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn supervision_report_selects_the_typed_failure_publication_before_stop() {
    let denial = RestartDenial::BudgetExceeded {
        restarts_in_window: 2,
        replacements_requested: 1,
        maximum_restarts: 2,
    };
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(
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
    .await
    .unwrap_or_else(|failed| {
        drop(failed);
        panic!("the supervision failure commits before the root stops");
    })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(
        termination,
        Ok(Exit::SupervisionFailed(
            SupervisionFailureReason::RestartDenied(denial)
        ))
    );
    assert_completed_root(terminal, Some(()));
}

#[tokio::test(flavor = "current_thread")]
async fn supervision_report_from_a_continuing_action_cannot_override_later_shutdown() {
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(
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
    .await
    .unwrap_or_else(|failed| {
        drop(failed);
        panic!("the continuing report is discarded before the later shutdown action");
    })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(termination, Ok(Exit::Normal));
    assert_completed_root(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn run_delegates_shutdown_to_the_explicit_root_policy() {
    let ApplicationOutcome::Completed {
        output: finalization,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(FinalizeOnShutdown::new(
        FinalizationProbe { observer: None },
        record_finalization,
    ))
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
    .await
    .unwrap_or_else(|failed| {
        drop(failed);
        panic!("the explicit finalization policy receives shutdown and terminates normally");
    })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );
    assert_eq!(finalization, Some(FinalizationEvent::Finalized));
    assert_completed_root(terminal, None);
}

#[tokio::test(flavor = "current_thread")]
async fn activation_failure_does_not_invoke_the_boundary() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);

    let result = Application::new(RejectsInitialization.stop_on_shutdown())
        .run_with(move |_| async move {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the actual caller runtime is entered");
        });

    let ApplicationOutcome::NotInvoked {
        work,
        startup_error: Some(startup_error),
        cleanup: Ok((origin, retirement)),
    } = result
    else {
        panic!("startup refusal returns the unpublished complete root retirement");
    };
    drop((work, startup_error));
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(origin, retirement);
    let (origin, retirement) = into_root(terminal);
    let ActorRetirement::InitializationRejected {
        behavior,
        error,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
        unread_owner_cancellation,
    } = retirement
    else {
        panic!("the exact initialization refusal remains a distinct root outcome");
    };
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(matches!(behavior.base(), RejectsInitialization));
    assert_eq!(error, InitializationFailure(47));
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(descendants.len(), 0);
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop((
        behavior,
        error,
        control,
        user,
        descendants,
        capability_failures,
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn behavior_failure_returns_the_exact_behavior_and_domain_error() {
    let ApplicationOutcome::Completed {
        output: (),
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(FailsAfterActivation.stop_on_shutdown())
        .run_with(|application| async move {
            application
                .root()
                .send_from(TEST_BOUNDARY, RootCommand::Stop)
                .await
                .expect("the live root admits the failing command");
        })
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("failure after activation is an exact root terminal, not a launch error");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    let (
        origin,
        ActorRetirement::BehaviorFailed {
            child_failures: (),
            capability_failures,
            interpretation,
            source,
            additional_failures,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
            terminal_report,
            unread_owner_cancellation,
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
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(settlements.len(), 0);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn application_delegates_to_the_explicit_single_space_app() {
    let ApplicationOutcome::Completed {
        output: ordinary,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::new(Root.stop_on_shutdown())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the ordinary application terminates normally");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let ordinary_terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    let ApplicationOutcome::Completed {
        output: explicit,
        cleanup: Ok((root_origin, joined_actor)),
    } = App::new(Root.stop_on_shutdown(), ActorSpace::new())
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
        .await
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the explicit application terminates normally");
        })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let explicit_terminal: ApplicationTerminal<_> = ProjectTerminal::project(
        root_origin,
        match joined_actor {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the actual application actor task failed: {failure}")
            }
            retirement => retirement,
        },
    );

    assert_eq!(ordinary, explicit);
    assert_completed_root(ordinary_terminal, None);
    assert_completed_root(explicit_terminal, None);
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

struct BlockingRoot {
    entries: Vec<u64>,
}

#[bombay::actor(message = Never)]
impl BlockingRoot {
    #[allow(
        clippy::unnecessary_wraps,
        reason = "the generated Behavior fold owns its exact controlled-error result"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        self.entries.push(257);
        Ok(Actions::stop())
    }
}

fn assert_blocking_root_retirement(
    origin: RootOrigin<StopOnShutdown<BlockingRoot>>,
    retirement: ActorRetirement<StopOnShutdown<BlockingRoot>, Never, ()>,
    allocation: *const u64,
) {
    let ActorRetirement::Completed {
        behavior,
        interpretation,
        source,
        settlements,
        control,
        user,
        descendants,
        child_failures: (),
        capability_failures,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
        unread_owner_cancellation,
        completion,
    } = retirement
    else {
        panic!("the original finite root must return its complete stopped retirement");
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let root = behavior.into_inner();
    assert_eq!(root.entries.as_ptr(), allocation);
    assert_eq!(root.entries, [239, 241, 257]);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(
        control,
        [] as [EventLayer<ShutdownRequested, User<MailAddr, Never>>; 0]
    );
    assert_eq!(user, [] as [User<MailAddr, Never>; 0]);
    assert_eq!(descendants, [] as [Never; 0]);
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(completion, Completion::Stopped);
    let [settlement] = settlements.try_into().unwrap_or_else(|_| {
        panic!("the initialization stop retains one complete settlement");
    });
    assert_eq!(settlement.sends.owned, NoSends);
    assert_eq!(settlement.sends.inner, NoSends);
    assert_eq!(settlement.become_, Step::Stop(Stopped));
    assert!(settlement.creations.is_empty());
}

#[test]
#[expect(
    clippy::drop_non_drop,
    reason = "explicitly discharge the original startup receive failure after the no-publication proof"
)]
fn public_run_blocking_returns_entered_host_inputs_and_retries_both_configurations() {
    let current_starts = Arc::new(AtomicUsize::new(0));
    let current_stops = Arc::new(AtomicUsize::new(0));
    let multi_starts = Arc::new(AtomicUsize::new(0));
    let multi_stops = Arc::new(AtomicUsize::new(0));
    let mut current = TokioBuilder::new_current_thread();
    current.enable_all();
    let started = current_starts.clone();
    let stopped = current_stops.clone();
    current.on_thread_start(move || {
        started.fetch_add(1, Ordering::SeqCst);
    });
    current.on_thread_stop(move || {
        stopped.fetch_add(1, Ordering::SeqCst);
    });
    let mut multi = TokioBuilder::new_multi_thread();
    multi.worker_threads(2).enable_all();
    let started = multi_starts.clone();
    let stopped = multi_stops.clone();
    multi.on_thread_start(move || {
        started.fetch_add(1, Ordering::SeqCst);
    });
    multi.on_thread_stop(move || {
        stopped.fetch_add(1, Ordering::SeqCst);
    });
    for (builder, starts, stops, expected_workers) in [
        (current, current_starts, current_stops, 0),
        (multi, multi_starts, multi_stops, 2),
    ] {
        let mut entries = Vec::with_capacity(3);
        entries.extend([239, 241]);
        let application = Application::new(BlockingRoot { entries }.stop_on_shutdown());
        let allocation = application.root().base().entries.as_ptr();
        let outer = TokioBuilder::new_current_thread()
            .build()
            .expect("the independent entered caller host is valid");
        let entered = outer.enter();
        let refused = application.run_blocking::<_, _, Never, ()>(builder);
        let Err((application, builder, RunError::BlockingInEnteredRuntime)) = refused else {
            panic!("the public blocking boundary must refuse before building or staging");
        };
        let returned_allocation = application.root().base().entries.as_ptr();
        assert_eq!(application.root().base().entries, [239, 241]);
        let starts_before_retry = starts.load(Ordering::SeqCst);
        let stops_before_retry = stops.load(Ordering::SeqCst);
        drop(entered);
        drop(outer);
        let retried = application
            .run_blocking::<_, _, Never, ()>(builder)
            .unwrap_or_else(|_| panic!("the exact returned inputs retry outside the entered host"))
            .unwrap_or_else(|_| panic!("the owned host supplies the actual async executor"));
        let ApplicationOutcome::NotInvoked {
            work: None,
            startup_error: Some(startup_error),
            cleanup: Ok((origin, retirement)),
        } = retried
        else {
            panic!(
                "initialization stop retains absent work, actual startup refusal and native cleanup"
            );
        };
        // run_blocking has joined and dropped its owned host before these oracles.
        assert_eq!(returned_allocation, allocation);
        assert_eq!(starts_before_retry, 0);
        assert_eq!(stops_before_retry, 0);
        assert_eq!(starts.load(Ordering::SeqCst), expected_workers);
        assert_eq!(stops.load(Ordering::SeqCst), expected_workers);
        drop(startup_error);
        assert_blocking_root_retirement(origin, retirement, allocation);
    }
}

#[cfg(all(feature = "axum", unix))]
#[test]
#[ignore = "only the parent invokes this test in its independently descriptor-limited child"]
fn public_run_blocking_recovers_real_build_error_in_limited_child() {
    let mut current = TokioBuilder::new_current_thread();
    current.enable_all();
    let mut multi = TokioBuilder::new_multi_thread();
    multi.worker_threads(2).enable_all();
    for (mut builder, expected_workers) in [(current, 0), (multi, 2)] {
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let started = starts.clone();
        let stopped = stops.clone();
        builder.on_thread_start(move || {
            started.fetch_add(1, Ordering::SeqCst);
        });
        builder.on_thread_stop(move || {
            stopped.fetch_add(1, Ordering::SeqCst);
        });
        let mut entries = Vec::with_capacity(3);
        entries.extend([239, 241]);
        let application = Application::new(BlockingRoot { entries }.stop_on_shutdown());
        let allocation = application.root().base().entries.as_ptr();
        let mut files = Vec::new();
        let exhausted = loop {
            match File::open("/dev/null") {
                Ok(file) => {
                    files.push(file);
                    if files.len() == 256 {
                        drop(files);
                        panic!(
                            "the isolated limit must yield a real descriptor failure within256 opens"
                        );
                    }
                }
                Err(error) => break error,
            }
        };
        let refused = application.run_blocking::<_, _, Never, ()>(builder);
        drop(files);
        let Err((application, builder, RunError::Runtime(source))) = refused else {
            panic!("actual enabled selector construction must refuse before any actor starts");
        };
        let returned_allocation = application.root().base().entries.as_ptr();
        assert_eq!(application.root().base().entries, [239, 241]);
        let starts_before_retry = starts.load(Ordering::SeqCst);
        let stops_before_retry = stops.load(Ordering::SeqCst);
        let retried = application
            .run_blocking::<_, _, Never, ()>(builder)
            .unwrap_or_else(|_| {
                panic!("the same declaration and Builder retry after descriptor recovery")
            })
            .unwrap_or_else(|_| panic!("the owned retry enters its actual executor"));
        let ApplicationOutcome::NotInvoked {
            work: None,
            startup_error: Some(startup_error),
            cleanup: Ok((origin, retirement)),
        } = retried
        else {
            panic!("the finite original root retains its whole native cleanup after retry");
        };
        assert_eq!(source.raw_os_error(), exhausted.raw_os_error());
        assert_eq!(source.raw_os_error(), Some(24));
        assert_eq!(returned_allocation, allocation);
        assert_eq!(starts_before_retry, 0);
        assert_eq!(stops_before_retry, 0);
        assert_eq!(starts.load(Ordering::SeqCst), expected_workers);
        assert_eq!(stops.load(Ordering::SeqCst), expected_workers);
        drop((source, exhausted, startup_error));
        assert_blocking_root_retirement(origin, retirement, allocation);
    }
}

#[cfg(all(feature = "axum", unix))]
#[test]
fn public_run_blocking_recovers_real_build_error_without_limiting_other_tests() {
    let executable = env::current_exe().expect("the actual compiled owning test executable exists");
    let output = Command::new("/bin/sh")
        .args(["-c", "ulimit -n 64 && exec \"$1\" --exact public_run_blocking_recovers_real_build_error_in_limited_child --ignored --nocapture --test-threads=1", "bombay-runtime-construction-child"])
        .arg(executable).output().expect("the independently limited child is launched");
    assert!(
        output.status.success(),
        "child status {:?}; stdout {}; stderr {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("1 passed; 0 failed"));
}
