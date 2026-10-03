use std::time::Duration;

use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, ChildCons, ChildCreationOutcome, ChildHead, ChildReport,
    ClassifySettlement, CreateChild, CreationKind, CreationSequence, CreationSettlement, Creations,
    EventIngress, InitializationTurn, InterpreterRequests, ItemSettlement, Never, NoChildren,
    NoSends, ReportToParent, RetirementBirths, SettledItem, SettlementStatus, Step, Stopped, User,
    UserEvent,
};
use bombay::prelude::*;
use bombay::{ActorSpace, App};

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
        terminal: ActorRetirement<StopOnShutdown<Root>, Self>,
    },
    #[declared_child(Root, RootChildrenWorker, StopOnShutdown<Worker>)]
    Worker {
        origin: ChildOrigin<Root, RootChildrenWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self>,
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
        terminal: ActorRetirement<DeclaredApplication, Self>,
    },
    #[application_actor]
    BackgroundWorker {
        origin: ChildOrigin<StopOnShutdown<ApplicationRoot>, BackgroundWorker>,
        terminal: ActorRetirement<StopOnShutdown<Worker>, Self>,
    },
    #[application_actor]
    AuditWorker {
        origin: ChildOrigin<StopOnShutdown<ApplicationRoot>, AuditWorker>,
        terminal: ActorRetirement<StopOnShutdown<Auditor>, Self>,
    },
}

#[test]
fn root_returns_only_after_owning_ordered_direct_child_terminals() {
    let terminal: ApplicationTerminal = match Application::new(Root.stop_on_shutdown()).run() {
        Err(RunError::Unpublished(terminal)) => terminal,
        _ => panic!("an initialization stop must return the unpublished root terminal"),
    };

    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                settlements,
                control,
                user,
                mut descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the root must preserve its completed state and child custody")
    };
    assert!(capability_failures.is_empty());
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
                    capability_failures,
                    unread_owner_cancellation,
                    behavior: _,
                    settlements,
                    control,
                    user,
                    descendants,
                },
        } = descendant
        else {
            panic!("parent retirement must preserve each exact child cancellation")
        };
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

#[test]
fn heterogeneous_application_children_are_owned_by_their_declared_roles() {
    let terminal: DeclaredApplicationTerminal =
        match Application::new(ApplicationRoot.stop_on_shutdown())
            .child(BackgroundWorker, Worker.stop_on_shutdown())
            .child(AuditWorker, Auditor.stop_on_shutdown())
            .run()
        {
            Err(RunError::Unpublished(terminal)) => terminal,
            _ => panic!("an initialization stop must return the unpublished application terminal"),
        };

    let DeclaredApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                settlements,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the application root must retain its declared child")
    };
    assert!(capability_failures.is_empty());
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
                        capability_failures,
                        unread_owner_cancellation,
                        behavior: _,
                        settlements,
                        control,
                        user,
                        descendants,
                    },
            } => {
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
                        capability_failures,
                        unread_owner_cancellation,
                        behavior: _,
                        settlements,
                        control,
                        user,
                        descendants,
                    },
            } => {
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
enum NestedTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<NestedRoot>>,
        terminal: ActorRetirement<StopOnShutdown<NestedRoot>, Self>,
    },
    #[structural_child]
    Child {
        origin: ChildOrigin<NestedRoot, ChildHead>,
        terminal: ActorRetirement<StopOnShutdown<NestedChild>, Self>,
    },
    #[declared_child(NestedChild, NestedChildChildrenGrandchild, StopOnShutdown<Grandchild>)]
    Grandchild {
        origin: ChildOrigin<NestedChild, NestedChildChildrenGrandchild>,
        terminal: ActorRetirement<StopOnShutdown<Grandchild>, Self>,
    },
}

#[test]
fn privately_bound_child_reports_its_nested_creation_before_parent_retirement() {
    let (termination, terminal): (_, NestedTerminal) = App::new(
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
    .expect("the child report must stop the published root");
    let child_termination =
        termination.expect("the child report must complete before the watchdog");
    assert_eq!(child_termination, Ok(Exit::Normal));

    assert_nested_birth_retirement(terminal);
}

fn assert_nested_birth_retirement(terminal: NestedTerminal) {
    let NestedTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                capability_failures,
                unread_owner_cancellation,
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the root must retain its completed retirement")
    };
    assert!(capability_failures.is_empty());
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
                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    control,
                    user,
                    descendants,
                    ..
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("the root must retain its exact child retirement")
    };
    assert!(capability_failures.is_empty());
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
                    capability_failures,
                    unread_owner_cancellation,
                    settlements,
                    control,
                    user,
                    descendants,
                    ..
                },
        },
    ] = descendants.as_slice()
    else {
        panic!("the child must retain its exact grandchild retirement")
    };
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
}

#[bombay::actor(message = Never)]
impl PanickingChild {
    fn init(&mut self) -> BehaviorActed<Self> {
        self.initialization_attempts += 1;
        panic!("child pure initialization panic")
    }
}

struct PanicParent;

#[bombay::actor(
    message = Never,
    births = { child: StopOnShutdown<PanickingChild> },
    creation_settlements = retain_for_retirement,
)]
impl PanicParent {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let id = creations.issue().expect("the child creation ID exists");
        Ok(Actions::new(
            NoSends,
            Creations::one(CreateChild::birth(
                id,
                PanickingChild {
                    initialization_attempts: 0,
                }
                .stop_on_shutdown(),
            )),
            Step::Stop(Stopped),
        ))
    }
}

#[derive(TerminalProjection)]
enum PanicTerminal {
    Root {
        origin: RootOrigin<StopOnShutdown<PanicParent>>,
        terminal: ActorRetirement<StopOnShutdown<PanicParent>, Self>,
    },
    #[allow(
        dead_code,
        reason = "the declared child role never commits after its pure fold panics"
    )]
    #[declared_child(PanicParent, PanicParentChildrenChild, StopOnShutdown<PanickingChild>)]
    Child {
        origin: ChildOrigin<PanicParent, PanicParentChildrenChild>,
        terminal: ActorRetirement<StopOnShutdown<PanickingChild>, Self>,
    },
}

#[test]
fn panicking_child_returns_exact_uncommitted_creation() {
    let terminal: PanicTerminal = match Application::new(PanicParent.stop_on_shutdown()).run() {
        Err(RunError::Unpublished(terminal)) => terminal,
        _ => panic!("the stopping parent must retain its unpublished terminal"),
    };
    let PanicTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                capability_failures,
                unread_owner_cancellation,
                settlements,
                descendants,
                completion,
                ..
            },
        ..
    } = terminal
    else {
        panic!("the parent must complete after settling the rejected creation")
    };
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(completion, Completion::Stopped);
    assert!(descendants.is_empty());
    let [settlement] = settlements.try_into().unwrap_or_else(|_| {
        panic!("the parent must retain exactly one complete initialization settlement")
    });
    let CreationSettlement::Settled(creations) = settlement.creations.into_settlement() else {
        panic!("the parent must retain a routed child settlement")
    };
    let SettledItem::Attempted(ItemSettlement::Accepted(
        ChildCreationOutcome::InitializationPanicked { creation },
    )) = creations
        .into_one()
        .unwrap_or_else(|_| panic!("one child creation must settle"))
    else {
        panic!("the child panic must retain its exact routed creation")
    };
    assert_eq!(creation.id().get(), 1);
    assert_eq!(creation.kind(), CreationKind::Birth);
    let (_, child, _) = creation.into_parts().0.into_parts();
    assert_eq!(child.base().initialization_attempts, 1);
}
