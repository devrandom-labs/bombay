use bombay::ActorCustody;
use core::ptr;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::time::Duration;

use bombay::ChildFailure;
use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, ChildChoice, ChildCreationOutcome, ChildReport,
    ClassifySettlement, CreateChild, CreationId, CreationKind, CreationSequence,
    CreationSettlement, Creations, EventIngress, InitializationTurn, InterpreterRequests,
    ItemSettlement, Never, NoSends, ReportToParent, RetirementBirths, SettledItem,
    SettlementStatus, Step, Stopped, User, UserEvent,
};
use bombay::prelude::*;
use bombay::{Application, ApplicationOutcome, Hosts};

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

bombay::topology! {
    ordered_children {
        root: Root { Worker(RootChildrenWorker) => worker; }
        worker: StopOnShutdown<Worker> {}
    }
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

bombay::topology! {
    declared_children {
        root: ApplicationRoot {}
        members {
            BackgroundWorker(BackgroundWorker) => worker;
            AuditWorker(AuditWorker) => auditor;
        }
        worker: StopOnShutdown<Worker> {}
        auditor: StopOnShutdown<Auditor> {}
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the full ordered direct-child custody trace and explicit recovered-input discharge before failure"
)]
async fn root_returns_only_after_owning_ordered_direct_child_terminals() {
    let application_outcome =
        Application::with_topology(Root.stop_on_shutdown(), ordered_children::Topology::new())
            .run()
            .await
            .unwrap_or_else(|(application, error)| {
                drop(application);
                panic!("the caller owns the application's live entered host: {error}");
            });
    if let ApplicationOutcome::NotInvoked {
        work: _,
        startup_error: _,
        cleanup: Ok((_, ActorRetirement::ActorTaskFailed(_))),
    } = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok((origin, retirement)),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                control,
                user,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
                mut descendants,
            },
        settlements,
        completion,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement
    else {
        panic!("the root must preserve its completed state and child custody")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(capability_failures.len(), 0);
    assert_eq!(child_failures.len(), 0);
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
        let ordered_children::RootRetirements { origin, retirement } = descendant;
        let ActorRetirement::OwnerCancelled {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    behavior: _,
                    control,
                    user,
                    descendants,
                    additional_failures: retirement_additional_failures,
                    received_interpretation: retirement_received_interpretation,
                    received_source: retirement_received_source,
                    source_index: retirement_source_index,
                    acquired_ingress: retirement_acquired_ingress,
                    retirement_failures: retirement_native_failures,
                    terminal_report: retirement_terminal_report,
                },
            settlements,
            interpretation: retirement_interpretation,
            source: retirement_source,
        } = *retirement
        else {
            panic!("parent retirement must preserve each exact child cancellation")
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert_eq!(retirement_additional_failures.len(), 0);
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert_eq!(retirement_native_failures.len(), 0);
        assert!(retirement_terminal_report.is_none());
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(control.len(), 0);
        assert_eq!(user.len(), 0);
        assert_eq!(settlements.len(), 0);
        assert_eq!(descendants.len(), 0);
        assert_eq!(child_failures.len(), 0);
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
async fn heterogeneous_application_children_are_owned_by_their_declared_roles() {
    let application_outcome = Application::with_topology(
        ApplicationRoot.stop_on_shutdown(),
        declared_children::Topology::new(),
    )
    .child(BackgroundWorker, Worker.stop_on_shutdown())
    .child(AuditWorker, Auditor.stop_on_shutdown())
    .run()
    .await
    .unwrap_or_else(|(application, error)| {
        drop(application);
        panic!("the caller owns the application's live entered host: {error}");
    });
    if let ApplicationOutcome::NotInvoked {
        work: _,
        startup_error: _,
        cleanup: Ok((_, ActorRetirement::ActorTaskFailed(_))),
    } = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok((origin, retirement)),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                behavior: _,
                control,
                user,
                descendants,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
        settlements,
        completion,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement
    else {
        panic!("the application root must retain its declared child")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(capability_failures.len(), 0);
    assert_eq!(child_failures.len(), 0);
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
            declared_children::RootRetirements::BackgroundWorker { origin, retirement } => {
                let ActorRetirement::OwnerCancelled {
                    custody:
                        ActorCustody {
                            child_failures,
                            capability_failures,
                            unread_owner_cancellation,
                            behavior: _,
                            control,
                            user,
                            descendants,
                            additional_failures: retirement_additional_failures,
                            received_interpretation: retirement_received_interpretation,
                            received_source: retirement_received_source,
                            source_index: retirement_source_index,
                            acquired_ingress: retirement_acquired_ingress,
                            retirement_failures: retirement_native_failures,
                            terminal_report: retirement_terminal_report,
                        },
                    settlements,
                    interpretation: retirement_interpretation,
                    source: retirement_source,
                } = *retirement
                else {
                    panic!("the cold child preserves its exact owner cancellation");
                };
                assert!(retirement_interpretation.is_none());
                assert!(retirement_source.is_none());
                assert_eq!(retirement_additional_failures.len(), 0);
                assert!(retirement_received_interpretation.is_none());
                assert!(retirement_received_source.is_none());
                assert!(retirement_source_index.is_none());
                assert!(retirement_acquired_ingress.is_none());
                assert_eq!(retirement_native_failures.len(), 0);
                assert!(retirement_terminal_report.is_none());
                assert_eq!(capability_failures.len(), 0);
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(control.len(), 0);
                assert_eq!(user.len(), 0);
                assert_eq!(settlements.len(), 0);
                assert_eq!(descendants.len(), 0);
                assert_eq!(child_failures.len(), 0);
                let previous_background = background.replace(origin);
                assert_eq!(previous_background, None);
            }
            declared_children::RootRetirements::AuditWorker { origin, retirement } => {
                let ActorRetirement::OwnerCancelled {
                    custody:
                        ActorCustody {
                            child_failures,
                            capability_failures,
                            unread_owner_cancellation,
                            behavior: _,
                            control,
                            user,
                            descendants,
                            additional_failures: retirement_additional_failures,
                            received_interpretation: retirement_received_interpretation,
                            received_source: retirement_received_source,
                            source_index: retirement_source_index,
                            acquired_ingress: retirement_acquired_ingress,
                            retirement_failures: retirement_native_failures,
                            terminal_report: retirement_terminal_report,
                        },
                    settlements,
                    interpretation: retirement_interpretation,
                    source: retirement_source,
                } = *retirement
                else {
                    panic!("the cold child preserves its exact owner cancellation");
                };
                assert!(retirement_interpretation.is_none());
                assert!(retirement_source.is_none());
                assert_eq!(retirement_additional_failures.len(), 0);
                assert!(retirement_received_interpretation.is_none());
                assert!(retirement_received_source.is_none());
                assert!(retirement_source_index.is_none());
                assert!(retirement_acquired_ingress.is_none());
                assert_eq!(retirement_native_failures.len(), 0);
                assert!(retirement_terminal_report.is_none());
                assert_eq!(capability_failures.len(), 0);
                assert!(unread_owner_cancellation.is_none());
                assert_eq!(control.len(), 0);
                assert_eq!(user.len(), 0);
                assert_eq!(settlements.len(), 0);
                assert_eq!(descendants.len(), 0);
                assert_eq!(child_failures.len(), 0);
                let previous_audit = audit.replace(origin);
                assert_eq!(previous_audit, None);
            }
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

bombay::topology! {
    nested_children {
        root: NestedRoot { Child => child; }
        child: StopOnShutdown<NestedChild> { Grandchild(NestedChildChildrenGrandchild) => grandchild; }
        grandchild: StopOnShutdown<Grandchild> {}
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one complete nested birth/report and retirement trace preserves every owned lane"
)]
async fn privately_bound_child_reports_its_nested_creation_before_parent_retirement() {
    let ApplicationOutcome::Completed {
        output: termination,
        cleanup: Ok((root_origin, joined_actor)),
    } = Application::with_topology(
        NestedRoot { birth_report: None }.stop_on_shutdown(),
        nested_children::Topology::new(),
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
    let retirement = match joined_actor {
        ActorRetirement::ActorTaskFailed(failure) => {
            panic!("the actual application actor task failed: {failure}")
        }
        retirement => retirement,
    };
    let origin = root_origin;
    let child_termination =
        termination.expect("the child report must complete before the watchdog");
    assert_eq!(child_termination, Ok(Exit::Normal));

    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                behavior,
                control,
                user,
                descendants,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
        settlements,
        completion,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement
    else {
        panic!("the root must retain its completed retirement")
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(capability_failures.len(), 0);
    assert_eq!(child_failures.len(), 0);
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
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(completion, Completion::Stopped);
    let [nested_children::RootRetirements { origin, retirement }] = descendants.as_slice() else {
        panic!("the root retains its exact descendant retirement");
    };
    let ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                control,
                user,
                descendants,
                behavior: _,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
        settlements,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement.as_ref()
    else {
        panic!("the descendant retains its exact owner cancellation");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(capability_failures.len(), 0);
    assert_eq!(child_failures.len(), 0);
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
    let [nested_children::ChildRetirements { origin, retirement }] = descendants.as_slice() else {
        panic!("the child retains its exact descendant retirement");
    };
    let ActorRetirement::OwnerCancelled {
        custody:
            ActorCustody {
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                control,
                user,
                descendants,
                behavior: _,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
        settlements,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement.as_ref()
    else {
        panic!("the descendant retains its exact owner cancellation");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(capability_failures.len(), 0);
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
    assert_eq!(descendants.len(), 0);
    assert_eq!(child_failures.len(), 0);
}

struct PanickingChild {
    initialization_attempts: usize,
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic carrier identity separately from shared payload lifetime."
    )]
    values: Option<Box<Arc<Vec<u64>>>>,
}

#[bombay::actor(message = Never)]
impl PanickingChild {
    fn init(&mut self) -> BehaviorActed<Self> {
        self.initialization_attempts += 1;
        let values = self
            .values
            .take()
            .expect("the child owns its original semantic values");
        resume_unwind(values)
    }
}

struct PanicParent {
    child: CreationId,
    survivor: CreationId,
    #[expect(
        clippy::redundant_allocation,
        reason = "Preserve native panic carrier identity separately from shared payload lifetime."
    )]
    values: Option<Box<Arc<Vec<u64>>>>,
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

bombay::topology! {
    panicking_children {
        root: PanicParent in root {
            Child(PanicParentChildrenChild) => child;
            Survivor(PanicParentChildrenSurvivor) => survivor;
        }
        hosts {
            root: PanicParent;
            children: PanickingChild;
            survivors: Worker;
        }
        child: StopOnShutdown<PanickingChild> in children {}
        survivor: StopOnShutdown<Worker> in survivors {}
    }
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
    let values = Box::new(Arc::new(vec![103_u64, 107, 109]));
    let original_values = Arc::downgrade(values.as_ref());
    let original_carrier = ptr::from_ref(values.as_ref()).cast::<()>();
    let spaces = panicking_children::Topology::new();
    let parents = <_ as Hosts<PanicParent>>::space(&spaces).clone();
    let children = <_ as Hosts<PanickingChild>>::space(&spaces).clone();
    let survivors = <_ as Hosts<Worker>>::space(&spaces).clone();
    let parent = PanicParent {
        child: child_id,
        survivor: survivor_id,
        values: Some(values),
    };
    let application_outcome = Application::with_topology(parent.stop_on_shutdown(), spaces)
        .run()
        .await
        .unwrap_or_else(|(application, error)| {
            drop(application);
            panic!("the caller owns the application's live entered host: {error}");
        });
    if let ApplicationOutcome::NotInvoked {
        work: _,
        startup_error: _,
        cleanup: Ok((_, ActorRetirement::ActorTaskFailed(_))),
    } = &application_outcome
    {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    }
    let ApplicationOutcome::NotInvoked {
        work: None,
        startup_error: Some(_startup_error),
        cleanup: Ok((origin, retirement)),
    } = application_outcome
    else {
        drop(application_outcome);
        panic!("the original startup phase and complete joined root remain exact");
    };
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                capability_failures,
                unread_owner_cancellation,
                control,
                user,
                mut child_failures,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
                mut descendants,
            },
        settlements,
        completion,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = retirement
    else {
        panic!("the parent completes after joining every surviving child");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    assert_eq!(behavior.base().child, child_id);
    assert_eq!(behavior.base().survivor, survivor_id);
    assert!(behavior.base().values.is_none());
    assert_eq!(capability_failures.len(), 0);
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(parents.resolve(&origin.address()).is_none());
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
    let panicking_children::RootRetirements::Survivor {
        origin: survivor_origin,
        retirement: survivor_retirement,
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
        custody:
            ActorCustody {
                behavior: survivor,
                control: survivor_control,
                user: survivor_user,
                descendants: survivor_descendants,
                child_failures: survivor_child_failures,
                capability_failures: survivor_capability_failures,
                unread_owner_cancellation: survivor_unread_request,
                additional_failures: retirement_additional_failures,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                retirement_failures: retirement_native_failures,
                terminal_report: retirement_terminal_report,
            },
        settlements: survivor_settlements,
        interpretation: retirement_interpretation,
        source: retirement_source,
    } = *survivor_retirement
    else {
        panic!("the sibling preserves the actual acquired owner cancellation");
    };
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert_eq!(retirement_additional_failures.len(), 0);
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert_eq!(retirement_native_failures.len(), 0);
    assert!(retirement_terminal_report.is_none());
    let _: &Worker = survivor.base();
    assert_eq!(survivor_settlements.len(), 0);
    assert_eq!(survivor_control.len(), 0);
    assert_eq!(survivor_user.len(), 0);
    assert_eq!(survivor_descendants.len(), 0);
    assert_eq!(survivor_child_failures.len(), 0);
    assert_eq!(survivor_capability_failures.len(), 0);
    assert!(survivor_unread_request.is_none());
    assert_eq!(child_failures.len(), 1);
    let failure = child_failures
        .pop()
        .expect("the native payload has one runtime owner");
    let panicking_children::RootChildFailure::Child(ChildFailure::InitializationPanicked {
        origin: child_origin,
        payload,
        additional_failures: initialization_additional_failures,
        terminal_report: initialization_terminal_report,
        retirement_failures: initialization_retirement_failures,
    }) = failure
    else {
        panic!("initialization provenance is distinct from actor and projector task failures");
    };
    assert_eq!(initialization_additional_failures.len(), 0);
    assert!(initialization_terminal_report.is_none());
    assert_eq!(initialization_retirement_failures.len(), 0);
    assert_eq!(child_origin.creation_id(), child_id);
    assert_eq!(child_origin.creation_kind(), CreationKind::Birth);
    let child_origin: ChildOrigin<PanicParent, PanicParentChildrenChild> =
        child_origin.into_declared_child();
    assert_eq!(child_origin.nonce(), route);
    assert!(children.resolve(&child_origin.address()).is_none());
    assert_eq!(original_values.strong_count(), 1);
    assert_eq!(
        ptr::from_ref(payload.as_ref()).cast::<()>(),
        original_carrier
    );
    drop(payload);
    drop(survivor_recipient);
    assert_eq!(original_values.strong_count(), 0);
}
