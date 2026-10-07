use std::panic::panic_any;
use std::sync::Arc;
use std::time::Duration;

use bombay::ChildFailure;
use bombay::ProjectTerminal;
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, ChildChoice, ChildCons, ChildCreationOutcome, ChildHead,
    ChildReport, ChildTail, ClassifySettlement, CreateChild, CreationId, CreationKind,
    CreationSequence, CreationSettlement, Creations, EventIngress, InitializationTurn,
    InterpreterRequests, ItemSettlement, Never, NoChildren, NoSends, ReportToParent,
    RetirementBirths, SettledItem, SettlementStatus, Step, Stopped, User, UserEvent,
};
use bombay::prelude::*;
use bombay::{ActorSpace, App, ApplicationOutcome};

struct Worker;

#[bombay::actor(message = Never)]
impl Worker {}

struct Root;

#[bombay::actor(
    message = Never,
    births = { worker: StopOnShutdown<Worker> },
    creation_settlements = retain_for_retirement,
)]
impl Root {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let first = creations.issue().expect("the first creation ID exists");
        let second = creations.issue().expect("the second creation ID exists");
        Ok(Actions::new(
            NoSends,
            Creations::one(CreateChild::birth(first, Worker.stop_on_shutdown()))
                .and(CreateChild::birth(second, Worker.stop_on_shutdown())),
            Step::Stop(Stopped),
        ))
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<Root>>,
        #[expect(
            clippy::type_complexity,
            reason = "the root retirement retains its exact typed child failures and complete terminal"
        )]
        terminal: ActorRetirement<
            StopOnShutdown<Root>,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<Root, ChildHead>, StopOnShutdown<Worker>>>,
                (),
            ),
        >,
    },
    #[declared_child(Root, RootChildrenWorker, StopOnShutdown<Worker>)]
    Worker {
        origin: ChildOrigin<Root, RootChildrenWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self, ()>,
    },
}

struct ApplicationRoot;

#[bombay::actor(message = Never)]
impl ApplicationRoot {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }
}

struct BackgroundWorker;

struct Auditor;

#[bombay::actor(message = Never)]
impl Auditor {}

struct AuditWorker;

type DeclaredChildren = ChildCons<
    MailAddr,
    StopOnShutdown<Auditor>,
    ChildCons<MailAddr, StopOnShutdown<Worker>, NoChildren>,
>;
type DeclaredApplication = ApplicationBehavior<StopOnShutdown<ApplicationRoot>, DeclaredChildren>;

#[derive(TerminalProjection)]
enum DeclaredApplicationTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<ApplicationRoot>>,
        #[expect(
            clippy::type_complexity,
            reason = "the declared root retirement retains both heterogeneous child failure products"
        )]
        terminal: ActorRetirement<
            DeclaredApplication,
            Self,
            (
                Vec<
                    ChildFailure<
                        ChildOrigin<StopOnShutdown<ApplicationRoot>, AuditWorker>,
                        StopOnShutdown<Auditor>,
                    >,
                >,
                (
                    Vec<
                        ChildFailure<
                            ChildOrigin<StopOnShutdown<ApplicationRoot>, BackgroundWorker>,
                            StopOnShutdown<Worker>,
                        >,
                    >,
                    (),
                ),
            ),
        >,
    },
    #[application_actor]
    BackgroundWorker {
        origin: ChildOrigin<StopOnShutdown<ApplicationRoot>, BackgroundWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self, ()>,
    },
    #[application_actor]
    AuditWorker {
        origin: ChildOrigin<StopOnShutdown<ApplicationRoot>, AuditWorker>,
        terminal: ActorRetirement<StopOnShutdown<Auditor>, Self, ()>,
    },
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    clippy::drop_non_drop,
    reason = "keep the full ordered direct-child custody trace and explicit recovered-input discharge before failure"
)]
async fn root_returns_only_after_owning_ordered_direct_child_terminals() {
    let application_outcome = Application::new(Root.stop_on_shutdown())
        .run::<_, _, ApplicationTerminal, _>()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        });
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok(Ok((origin, Ok(retirement)))),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: ApplicationTerminal = ProjectTerminal::project(origin, retirement);

    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                child_failures: (child_failures, ()),

                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                settlements,
                control,
                user,
                mut descendants,
                completion,
                interpretation: retirement_interpretation,
                source: retirement_source,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
    } = terminal
    else {
        panic!("the root must preserve its completed state and child custody")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(child_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(settlements.len(), 1);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);

    let mut addresses = Vec::new();
    let mut nonces = Vec::new();
    for descendant in descendants.drain(..) {
        let ApplicationTerminal::Worker {
            origin,
            terminal:
                ActorRetirement::OwnerCancelled {
                    child_failures: (),

                    capability_failures,
                    unread_owner_cancellation,
                    behavior: _,
                    settlements,
                    control,
                    user,
                    descendants,
                    interpretation: retirement_interpretation,
                    source: retirement_source,
                    additional_failures: retirement_additional_failures,
                    received_interpretation: retirement_received_interpretation,
                    received_source: retirement_received_source,
                    source_index: retirement_source_index,
                    acquired_ingress: retirement_acquired_ingress,
                    retirement_failures: retirement_native_failures,
                    terminal_report: retirement_terminal_report,
                },
        } = descendant
        else {
            panic!("parent retirement must preserve each exact child cancellation")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_additional_failures.is_empty());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_terminal_report.is_none());
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(settlements.len(), 0);
        assert!(descendants.is_empty());
        addresses.push(origin.address());
        nonces.push(origin.nonce());
    }
    assert!(addresses[0] < addresses[1]);
    assert_ne!(nonces[0], nonces[1]);
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep full heterogeneous declared-role retirements and all original typed custody lanes in one test"
)]
#[expect(
    clippy::drop_non_drop,
    reason = "explicitly release the recovered concrete input at this ownership boundary, before the following retry or failure"
)]
async fn heterogeneous_application_children_are_owned_by_their_declared_roles() {
    let application_outcome = Application::new(ApplicationRoot.stop_on_shutdown())
        .child(BackgroundWorker, Worker.stop_on_shutdown())
        .child(AuditWorker, Auditor.stop_on_shutdown())
        .run::<_, _, DeclaredApplicationTerminal, _>()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        });
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok(Ok((origin, Ok(retirement)))),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: DeclaredApplicationTerminal = ProjectTerminal::project(origin, retirement);

    let DeclaredApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                child_failures: (child_failures, (tail_child_failures, ())),

                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                settlements,
                control,
                user,
                descendants,
                completion,
                interpretation: retirement_interpretation,
                source: retirement_source,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
    } = terminal
    else {
        panic!("the application root must retain its declared child")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(child_failures.is_empty());
    assert!(tail_child_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(settlements.len(), 1);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);

    let mut background = None;
    let mut audit = None;
    for descendant in descendants {
        match descendant {
            DeclaredApplicationTerminal::BackgroundWorker {
                origin,
                terminal:
                    ActorRetirement::OwnerCancelled {
                        child_failures: (),

                        capability_failures,
                        unread_owner_cancellation,
                        behavior: _,
                        settlements,
                        control,
                        user,
                        descendants,
                        interpretation: retirement_interpretation,
                        source: retirement_source,
                        additional_failures: retirement_additional_failures,
                        received_interpretation: retirement_received_interpretation,
                        received_source: retirement_received_source,
                        source_index: retirement_source_index,
                        acquired_ingress: retirement_acquired_ingress,
                        retirement_failures: retirement_native_failures,
                        terminal_report: retirement_terminal_report,
                    },
            } => {
                assert!(retirement_interpretation.is_none());
                assert!(retirement_source.is_none());
                assert!(retirement_additional_failures.is_empty());
                assert!(retirement_received_interpretation.is_none());
                assert!(retirement_received_source.is_none());
                assert!(retirement_source_index.is_none());
                assert!(retirement_acquired_ingress.is_none());
                assert!(retirement_native_failures.is_empty());
                assert!(retirement_terminal_report.is_none());
                assert_eq!(capability_failures.len(), 0);
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(control.len(), 0);
                assert_eq!(user.len(), 0);
                assert_eq!(settlements.len(), 0);
                assert!(descendants.is_empty());
                let previous_background = background.replace(origin);
                assert_eq!(previous_background, None);
            }
            DeclaredApplicationTerminal::AuditWorker {
                origin,
                terminal:
                    ActorRetirement::OwnerCancelled {
                        child_failures: (),

                        capability_failures,
                        unread_owner_cancellation,
                        behavior: _,
                        settlements,
                        control,
                        user,
                        descendants,
                        interpretation: retirement_interpretation,
                        source: retirement_source,
                        additional_failures: retirement_additional_failures,
                        received_interpretation: retirement_received_interpretation,
                        received_source: retirement_received_source,
                        source_index: retirement_source_index,
                        acquired_ingress: retirement_acquired_ingress,
                        retirement_failures: retirement_native_failures,
                        terminal_report: retirement_terminal_report,
                    },
            } => {
                assert!(retirement_interpretation.is_none());
                assert!(retirement_source.is_none());
                assert!(retirement_additional_failures.is_empty());
                assert!(retirement_received_interpretation.is_none());
                assert!(retirement_received_source.is_none());
                assert!(retirement_source_index.is_none());
                assert!(retirement_acquired_ingress.is_none());
                assert!(retirement_native_failures.is_empty());
                assert!(retirement_terminal_report.is_none());
                assert_eq!(capability_failures.len(), 0);
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(control.len(), 0);
                assert_eq!(user.len(), 0);
                assert_eq!(settlements.len(), 0);
                assert!(descendants.is_empty());
                let previous_audit = audit.replace(origin);
                assert_eq!(previous_audit, None);
            }
            _ => panic!("each application child must retain exact cancellation custody"),
        }
    }

    let background = background.expect("the root owns the background worker terminal");
    assert_eq!(background.address(), MailAddr(1));

    let audit = audit.expect("the root owns the audit worker terminal");
    assert_eq!(audit.address(), MailAddr(2));
    assert_ne!(background.nonce(), audit.nonce());
}

struct Grandchild;

#[bombay::actor(message = Never)]
impl Grandchild {}

struct NestedChild;

#[bombay::actor(
    message = Never,
    sends = pub(crate) { birth_reports: InterpreterRequests<ReportToParent<GrandchildBirthInterpreted>> },
    births = { grandchild: StopOnShutdown<Grandchild> },
    creation_settlements = retain_for_retirement,
)]
impl NestedChild {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let grandchild = creations
            .issue()
            .expect("the grandchild creation ID exists");
        Ok(Actions::create(Creations::one(CreateChild::birth(
            grandchild,
            Grandchild.stop_on_shutdown(),
        )))
        .send_birth_reports(ReportToParent::new(GrandchildBirthInterpreted)))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct GrandchildBirthInterpreted;

struct NestedRoot {
    birth_report: Option<ChildReport<GrandchildBirthInterpreted>>,
}

enum NestedRootInput {
    BirthReport(ChildReport<GrandchildBirthInterpreted>),
}

impl UserEvent for NestedRootInput {
    type Addr = MailAddr;
    type Message = Never;

    fn user(_: MailAddr, message: Never) -> Self {
        match message {}
    }

    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}

impl
    EventIngress<
        RetirementBirths<StopOnShutdown<NestedChild>>,
        ChildReport<GrandchildBirthInterpreted>,
    > for NestedRootInput
{
    fn ingress(report: ChildReport<GrandchildBirthInterpreted>) -> Self {
        Self::BirthReport(report)
    }
}

impl Protocol for NestedRoot {
    type Addr = MailAddr;
    type Msg = Never;
}

impl Behavior for NestedRoot {
    type Protocol = Self;
    type Event = NestedRootInput;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = RetirementBirths<StopOnShutdown<NestedChild>>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let child = creations.issue().expect("the child creation ID exists");
        Ok(Actions::create(Creations::one(CreateChild::birth(
            child,
            NestedChild.stop_on_shutdown(),
        ))))
    }

    fn transition(&mut self, _: ActiveTurn, input: Self::Event) -> BehaviorActed<Self> {
        match input {
            NestedRootInput::BirthReport(report) => {
                self.birth_report = Some(report);
                Ok(Actions::stop())
            }
        }
    }
}

impl BehaviorBase for NestedRoot {
    type Base = Self;

    fn base(&self) -> &Self::Base {
        self
    }
}

#[derive(TerminalProjection)]
#[expect(
    clippy::large_enum_variant,
    reason = "retain complete original role retirements without adding a heap owner or changing terminal custody"
)]
enum NestedTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<NestedRoot>>,
        #[expect(
            clippy::type_complexity,
            reason = "the nested root retains its exact child failure product"
        )]
        terminal: ActorRetirement<
            StopOnShutdown<NestedRoot>,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<NestedRoot, ChildHead>, StopOnShutdown<NestedChild>>>,
                (),
            ),
        >,
    },
    #[structural_child]
    Child {
        origin: ChildOrigin<NestedRoot, ChildHead>,
        #[expect(
            clippy::type_complexity,
            reason = "the nested child retains its exact grandchild failure product"
        )]
        terminal: ActorRetirement<
            StopOnShutdown<NestedChild>,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<NestedChild, ChildHead>, StopOnShutdown<Grandchild>>>,
                (),
            ),
        >,
    },
    #[declared_child(NestedChild, NestedChildChildrenGrandchild, StopOnShutdown<Grandchild>)]
    Grandchild {
        origin: ChildOrigin<NestedChild, NestedChildChildrenGrandchild>,
        terminal: ActorRetirement<StopOnShutdown<Grandchild>, Self, ()>,
    },
}

#[tokio::test(flavor = "current_thread")]
async fn privately_bound_child_reports_its_nested_creation_before_parent_retirement() {
    let ApplicationOutcome::Completed {
        output: Some(termination),
        cleanup: Ok(Ok((root_origin, joined_actor))),
    } = App::new(
        NestedRoot { birth_report: None }.stop_on_shutdown(),
        ActorSpace::new(),
    )
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let termination =
            tokio::time::timeout(Duration::from_secs(5), lifecycle.termination()).await;
        if termination.is_err() {
            match lifecycle.request_shutdown() {
                Ok(())
                | Err(ShutdownRejection::AlreadyStopping | ShutdownRejection::AlreadyStopped) => {}
            }
        }
        termination
    })
    .await
    .unwrap_or_else(|failed| {
        drop(failed);
        panic!("the child report must stop the published root");
    })
    else {
        panic!("the original completed output and both cleanup join boundaries remain exact");
    };
    let terminal: NestedTerminal = ProjectTerminal::project(
        root_origin,
        joined_actor.unwrap_or_else(|failure| {
            panic!("the actual application actor task failed: {failure}")
        }),
    );
    let child_termination =
        termination.expect("the child report must complete before the watchdog");
    assert_eq!(child_termination, Ok(Exit::Normal));

    assert_nested_birth_retirement(terminal);
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete recursive retirement oracle checks every owned lane and nested child outcome"
)]
fn assert_nested_birth_retirement(terminal: NestedTerminal) {
    let NestedTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                child_failures: (child_failures, ()),

                capability_failures,
                unread_owner_cancellation,
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
                interpretation: retirement_interpretation,
                source: retirement_source,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
    } = terminal
    else {
        panic!("the root must retain its completed retirement")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(child_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let Some(report) = behavior.base().birth_report.as_ref() else {
        panic!("the child report must reach the root as a typed input")
    };
    assert_eq!(report.report, GrandchildBirthInterpreted);
    assert_eq!(
        settlements
            .iter()
            .map(ClassifySettlement::settlement_status)
            .collect::<Vec<_>>(),
        vec![SettlementStatus::Accepted, SettlementStatus::Accepted]
    );
    assert!(control.is_empty());
    assert_eq!(user.len(), 0);
    assert_eq!(completion, Completion::Stopped);
    let [
        NestedTerminal::Child {
            origin,
            terminal:
                ActorRetirement::OwnerCancelled {
                    child_failures: (child_failures, ()),

                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    control,
                    user,
                    descendants,
                    behavior: _,
                    interpretation: retirement_interpretation,
                    source: retirement_source,
                    additional_failures: retirement_additional_failures,
                    received_interpretation: retirement_received_interpretation,
                    received_source: retirement_received_source,
                    source_index: retirement_source_index,
                    acquired_ingress: retirement_acquired_ingress,
                    retirement_failures: retirement_native_failures,
                    terminal_report: retirement_terminal_report,
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("the root must retain its exact child retirement")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(child_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    let child_address = origin.address();
    assert_ne!(child_address, MailAddr::APPLICATION_ROOT);
    assert_eq!(settlements.len(), 1);
    assert_eq!(
        settlements[0].settlement_status(),
        SettlementStatus::Accepted
    );
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    let [
        NestedTerminal::Grandchild {
            origin,
            terminal:
                ActorRetirement::OwnerCancelled {
                    child_failures: (),

                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    control,
                    user,
                    descendants,
                    behavior: _,
                    interpretation: retirement_interpretation,
                    source: retirement_source,
                    additional_failures: retirement_additional_failures,
                    received_interpretation: retirement_received_interpretation,
                    received_source: retirement_received_source,
                    source_index: retirement_source_index,
                    acquired_ingress: retirement_acquired_ingress,
                    retirement_failures: retirement_native_failures,
                    terminal_report: retirement_terminal_report,
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("the child must retain its exact grandchild retirement")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_ne!(origin.address(), child_address);
    match settlements.as_slice() {
        [] => {}
        [initialization] => {
            assert_eq!(
                initialization.settlement_status(),
                SettlementStatus::Accepted
            );
        }
        _ => panic!("the grandchild cannot interpret more than its initialization"),
    }
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
}

struct PanickingChild {
    initialization_attempts: usize,
    values: Option<Arc<Vec<u64>>>,
}

#[bombay::actor(message = Never)]
impl PanickingChild {
    fn init(&mut self) -> BehaviorActed<Self> {
        self.initialization_attempts += 1;
        let values = self
            .values
            .take()
            .expect("the child owns its original semantic values");
        panic_any(values)
    }
}

struct PanicParent {
    child: CreationId,
    survivor: CreationId,
    values: Option<Arc<Vec<u64>>>,
}

#[bombay::actor(
    message = Never,
    births = {
        child: StopOnShutdown<PanickingChild>,
        survivor: StopOnShutdown<Worker>,
    },
    creation_settlements = retain_for_retirement,
)]
impl PanicParent {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the generated Behavior contract requires the controlled Result even for this successful initializer"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let values = self
            .values
            .take()
            .expect("the parent transfers the original child values once");
        let children = Creations::one(CreateChild::birth(
            self.child,
            ChildChoice::Tail(ChildChoice::Head(
                PanickingChild {
                    initialization_attempts: 0,
                    values: Some(values),
                }
                .stop_on_shutdown(),
            )),
        ))
        .and(CreateChild::birth(
            self.survivor,
            ChildChoice::Head(Worker.stop_on_shutdown()),
        ));
        Ok(Actions::new(NoSends, children, Step::Stop(Stopped)))
    }
}

#[derive(bombay::ActorSpaces)]
struct PanicSpaces {
    #[actor_space(PanicParent)]
    parents: ActorSpace<PanicParent>,
    #[actor_space(PanickingChild)]
    children: ActorSpace<PanickingChild>,
    #[actor_space(Worker)]
    survivors: ActorSpace<Worker>,
}

#[derive(TerminalProjection)]
enum PanicTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<PanicParent>>,
        #[expect(
            clippy::type_complexity,
            reason = "the parent retains independent surviving-worker and panicking-child failure products"
        )]
        terminal: ActorRetirement<
            StopOnShutdown<PanicParent>,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<PanicParent, ChildHead>, StopOnShutdown<Worker>>>,
                (
                    Vec<
                        ChildFailure<
                            ChildOrigin<PanicParent, ChildTail<ChildHead>>,
                            StopOnShutdown<PanickingChild>,
                        >,
                    >,
                    (),
                ),
            ),
        >,
    },
    #[allow(
        dead_code,
        reason = "the declared child never commits after its pure initialization panics"
    )]
    #[declared_child(PanicParent, PanicParentChildrenChild, StopOnShutdown<PanickingChild>)]
    Child {
        origin: ChildOrigin<PanicParent, PanicParentChildrenChild>,
        terminal: ActorRetirement<StopOnShutdown<PanickingChild>, Self, ()>,
    },
    #[declared_child(PanicParent, PanicParentChildrenSurvivor, StopOnShutdown<Worker>)]
    Survivor {
        origin: ChildOrigin<PanicParent, PanicParentChildrenSurvivor>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self, ()>,
    },
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one finite child-panic controller joins all owners before complete custody oracles"
)]
async fn panicking_child_returns_exact_uncommitted_creation() {
    let mut ids = CreationSequence::new();
    let child_id = ids
        .issue()
        .expect("the child's original creation ID exists");
    let survivor_id = ids
        .issue()
        .expect("the sibling's distinct original creation ID exists");
    let values = Arc::new(vec![103, 107, 109]);
    let original_values = Arc::downgrade(&values);
    let original_arc_allocation = Arc::as_ptr(&values);
    let original_vec_allocation = values.as_ptr();
    let parents = ActorSpace::new();
    let children = ActorSpace::new();
    let survivors = ActorSpace::new();
    let spaces = PanicSpaces {
        parents: parents.clone(),
        children: children.clone(),
        survivors: survivors.clone(),
    };
    let parent = PanicParent {
        child: child_id,
        survivor: survivor_id,
        values: Some(values),
    };
    let application_outcome = App::new(parent.stop_on_shutdown(), spaces)
        .run::<PanicTerminal, _>()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        });
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok(Ok((origin, Ok(retirement)))),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let terminal: PanicTerminal = ProjectTerminal::project(origin, retirement);
    let PanicTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                capability_failures,
                unread_owner_cancellation,
                settlements,
                control,
                user,
                mut descendants,
                child_failures: (survivor_failures, (mut child_failures, ())),
                completion,
                interpretation: retirement_interpretation,
                source: retirement_source,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
    } = terminal
    else {
        panic!("the parent completes after joining every surviving child");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    assert_eq!(behavior.base().child, child_id);
    assert_eq!(behavior.base().survivor, survivor_id);
    assert!(behavior.base().values.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(parents.resolve(&origin.address()).is_none());
    assert!(survivor_failures.is_empty());
    let [settlement] = settlements.try_into().unwrap_or_else(|_| {
        panic!("the parent retains one complete initialization settlement");
    });
    assert_eq!(settlement.sends.owned, NoSends);
    assert_eq!(settlement.sends.inner, NoSends);
    assert_eq!(settlement.become_, Step::Stop(Stopped));
    let CreationSettlement::Settled(creations) = settlement.creations.into_settlement() else {
        panic!("the parent retains its original routed creation rows");
    };
    let mut rows = creations.into_iter();
    let native_row = rows
        .next()
        .expect("the native child owns the first original row");
    let survivor_row = rows
        .next()
        .expect("the established sibling owns the second original row");
    let remaining = rows.next();
    assert!(remaining.is_none());
    let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Tail(ChildChoice::Head(
        ChildCreationOutcome::InitializationPanicked { creation },
    )))) = native_row
    else {
        panic!("the pure initialization panic returns its original surviving child");
    };
    assert_eq!(creation.id(), child_id);
    assert_eq!(creation.kind(), CreationKind::Birth);
    let (request, route) = creation.into_parts();
    let (returned_id, child, returned_kind) = request.into_parts();
    assert_eq!(returned_id, child_id);
    assert_eq!(returned_kind, CreationKind::Birth);
    assert_eq!(child.base().initialization_attempts, 1);
    assert!(child.base().values.is_none());
    let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
        ChildCreationOutcome::Established(committed),
    ))) = survivor_row
    else {
        panic!("the later sibling still commits and retains its original receipt");
    };
    let (returned_survivor_id, returned_survivor_kind, actor) = committed.into_parts();
    assert_eq!(returned_survivor_id, survivor_id);
    assert_eq!(returned_survivor_kind, CreationKind::Birth);
    assert_eq!(descendants.len(), 1);
    let survivor_terminal = descendants
        .pop()
        .expect("the sibling is joined exactly once");
    let PanicTerminal::Survivor {
        origin: survivor_origin,
        terminal: survivor_retirement,
    } = survivor_terminal
    else {
        panic!("the surviving terminal retains its declared sibling role");
    };
    // The selected public EstablishedActor deliberately keeps its installed
    // endpoint opaque. Preserve that whole capability without inventing an
    // address getter or inferring an incarnation generation from this address.
    let survivor_recipient = actor.into_recipient();
    assert!(survivors.resolve(&survivor_origin.address()).is_none());
    let ActorRetirement::OwnerCancelled {
        behavior: survivor,
        settlements: survivor_settlements,
        control: survivor_control,
        user: survivor_user,
        descendants: survivor_descendants,
        child_failures: (),
        capability_failures: survivor_capability_failures,
        unread_owner_cancellation: survivor_unread_request,
        interpretation: retirement_interpretation,
        source: retirement_source,
        additional_failures: retirement_additional_failures,
        received_interpretation: retirement_received_interpretation,
        received_source: retirement_received_source,
        source_index: retirement_source_index,
        acquired_ingress: retirement_acquired_ingress,
        retirement_failures: retirement_native_failures,
        terminal_report: retirement_terminal_report,
    } = survivor_retirement
    else {
        panic!("the sibling preserves the actual acquired owner cancellation");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    assert!(retirement_terminal_report.is_none());
    let _: &Worker = survivor.base();
    assert_eq!(survivor_settlements.len(), 0);
    assert_eq!(survivor_control.len(), 0);
    assert_eq!(survivor_user.len(), 0);
    assert!(survivor_descendants.is_empty());
    assert!(survivor_capability_failures.is_empty());
    assert!(survivor_unread_request.is_none());
    assert_eq!(child_failures.len(), 1);
    let failure = child_failures
        .pop()
        .expect("the native payload has one runtime owner");
    let ChildFailure::InitializationPanicked {
        id,
        kind,
        origin: child_origin,
        payload,
        additional_failures: initialization_additional_failures,
        terminal_report: initialization_terminal_report,
        retirement_failures: initialization_retirement_failures,
    } = failure
    else {
        panic!("initialization provenance is distinct from actor and projector task failures");
    };
    assert!(initialization_additional_failures.is_empty());
    assert!(initialization_terminal_report.is_none());
    assert!(initialization_retirement_failures.is_empty());
    assert_eq!(id, child_id);
    assert_eq!(kind, CreationKind::Birth);
    let child_origin: ChildOrigin<PanicParent, PanicParentChildrenChild> =
        child_origin.into_declared_child();
    assert_eq!(child_origin.nonce(), route);
    assert!(children.resolve(&child_origin.address()).is_none());
    assert_eq!(original_values.strong_count(), 1);
    let retained_values = original_values
        .upgrade()
        .expect("the opaque runtime payload retains the original values");
    assert_eq!(Arc::as_ptr(&retained_values), original_arc_allocation);
    assert_eq!(retained_values.as_ptr(), original_vec_allocation);
    assert_eq!(retained_values.as_slice(), [103, 107, 109]);
    drop(retained_values);
    drop(payload);
    drop(survivor_recipient);
    assert_eq!(original_values.strong_count(), 0);
}
