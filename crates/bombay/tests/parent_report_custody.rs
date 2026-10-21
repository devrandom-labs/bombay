use bombay::behavior::{
    ActiveTurn, Behavior, BehaviorBase, ChildReport, ClassifySettlement, CreateChild,
    CreationSequence, Creations, EventIngress, InitializationTurn, InterpreterRequests,
    ItemSettlement, Never, NoSends, ParentReportReason, ReportToParent, RetirementBirths,
    SettledItem, SettlementStatus, Stopped, User, UserEvent,
};
use bombay::prelude::*;
use bombay::{ActorCustody, ActorNotificationReceipts, WorkOutcome};

struct ReportChild {
    report: Option<Box<[u8]>>,
}

#[bombay::actor(
    message = Never,
    sends = { reports: InterpreterRequests<ReportToParent<Box<[u8]>>> },
)]
impl ReportChild {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "the owning actor fold requires its BehaviorActed result"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let report = self.report.take().expect("one original child report");
        Ok(Actions::cont().send_reports(ReportToParent::new(report)))
    }
}

struct ReportParent {
    report: Option<Box<[u8]>>,
}

enum ParentReportInput {
    Report(ChildReport<Box<[u8]>>),
}

impl UserEvent for ParentReportInput {
    type Addr = MailAddr;
    type Message = Never;

    fn user(_: MailAddr, message: Never) -> Self {
        match message {}
    }

    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}

impl EventIngress<RetirementBirths<ReportChild>, ChildReport<Box<[u8]>>> for ParentReportInput {
    fn ingress(report: ChildReport<Box<[u8]>>) -> Self {
        Self::Report(report)
    }
}

impl Protocol for ReportParent {
    type Addr = MailAddr;
    type Msg = Never;
}

impl BehaviorBase for ReportParent {
    type Base = Self;

    fn base(&self) -> &Self {
        self
    }
}

impl Behavior for ReportParent {
    type Protocol = Self;
    type Event = ParentReportInput;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = RetirementBirths<ReportChild>;

    fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let child = creations.issue().expect("one real child creation");
        let report = self.report.take().expect("one original report transfer");
        Ok(Actions::new(
            NoSends,
            Creations::one(CreateChild::birth(
                child,
                ReportChild {
                    report: Some(report),
                },
            )),
            Step::Stop(Stopped),
        ))
    }

    fn transition(&mut self, _: ActiveTurn, input: Self::Event) -> BehaviorActed<Self> {
        let ParentReportInput::Report(report) = input;
        self.report = Some(report.report);
        Ok(Actions::cont())
    }
}

bombay::topology! {
    report_application {
        root: ReportParent { Child => child; }
        child: ReportChild {}
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the complete acquired Work/native/notification owners precede the contiguous original-report custody oracle"
)]
async fn early_parent_stop_retains_the_original_refused_child_report() {
    let report: Box<[u8]> = Box::from([7, 13, 17]);
    let report_allocation = report.as_ptr();
    let application = Application::with_topology(
        ReportParent {
            report: Some(report),
        }
        .stop_on_shutdown(),
        report_application::Topology::new(),
    );
    let (execution, receiving) = application
        .execute_with(|application| async move { application.lifecycle().termination().await })
        .unwrap_or_else(|original| panic!("the entered host owns startup: {}", original.2));
    execution.await;
    let (work, _root_report, native, notifications, ()) = receiving.await.into_parts();
    let Ok((root_origin, root_retirement)) = &native else {
        panic!("the parent retains its original native result")
    };
    let ActorRetirement::Completed {
        custody:
            ActorCustody {
                behavior,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
                unread_owner_cancellation,
            },
        interpretation,
        source,
        settlements,
        completion,
    } = root_retirement
    else {
        panic!("the parent must complete after acquiring all child retirement")
    };
    let [
        report_application::RootRetirements {
            origin,
            retirement: terminal,
        },
    ] = descendants.as_slice()
    else {
        panic!("exactly one original child native result remains")
    };
    // The complete Work/native/notification owners above survive this first
    // semantic oracle, including the original host panic on the old interpreter.
    let ActorRetirement::EffectsFailed {
        custody:
            ActorCustody {
                behavior: child,
                control: child_control,
                user: child_user,
                descendants: child_descendants,
                child_failures: child_child_failures,
                capability_failures: child_capability_failures,
                additional_failures: child_additional_failures,
                received_interpretation: child_received_interpretation,
                received_source: child_received_source,
                source_index: child_source_index,
                acquired_ingress: child_acquired_ingress,
                retirement_failures: child_retirement_failures,
                terminal_report: child_terminal_report,
                // The original unread owner request remains borrowed in native custody.
                unread_owner_cancellation: _,
            },
        initialization,
        interpretation: child_interpretation,
        source: child_source,
        settlements: child_settlements,
        error,
    } = terminal.as_ref()
    else {
        panic!("the refused child report must remain a typed EffectsFailed settlement")
    };
    assert_eq!(*error, SettlementFailure::Rejected);
    let [settled] = child_settlements.as_slice() else {
        panic!("one complete child report settlement remains")
    };
    let [SettledItem::Attempted(ItemSettlement::Rejected { item, reason })] =
        settled.sends.reports.as_slice()
    else {
        panic!("the closed parent returns the original ReportToParent")
    };
    assert_eq!(*reason, ParentReportReason::ClosedParentControlLane);
    assert_eq!(item.report.as_ptr(), report_allocation);
    assert_eq!(&*item.report, &[7, 13, 17]);
    assert_eq!(root_origin.address(), MailAddr::APPLICATION_ROOT);
    assert_ne!(origin.address(), root_origin.address());
    assert_eq!(*completion, Completion::Stopped);
    assert!(matches!(
        &notifications,
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        })
    ));
    match &work {
        WorkOutcome::Completed {
            output,
            cleanup: Ok(()),
        } => {
            assert_eq!(*output, Ok(Exit::Normal));
        }
        WorkOutcome::NotInvoked {
            cleanup: Ok(()), ..
        } => {}
        _ => panic!("actual Work retains its complete phase and successful cleanup"),
    }
    assert!(behavior.base().report.is_none());
    assert!(child.report.is_none());
    assert_eq!(settlements.len(), 1);
    assert_eq!(
        settlements[0].settlement_status(),
        SettlementStatus::Accepted
    );
    assert!(control.is_empty());
    assert_eq!(user.as_slice(), []);
    assert!(child_failures.is_empty());
    assert!(capability_failures.is_empty());
    assert!(additional_failures.is_empty());
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(unread_owner_cancellation.is_none());
    assert!(initialization.is_none());
    assert!(child_interpretation.is_none());
    assert!(child_source.is_none());
    assert_eq!(child_control.as_slice(), []);
    assert_eq!(child_user.as_slice(), []);
    assert_eq!(child_descendants.as_slice(), []);
    assert_eq!(child_child_failures.as_slice(), []);
    assert!(child_capability_failures.is_empty());
    assert!(child_additional_failures.is_empty());
    assert!(child_received_interpretation.is_none());
    assert!(child_received_source.is_none());
    assert!(child_source_index.is_none());
    assert!(child_acquired_ingress.is_none());
    assert!(child_retirement_failures.is_empty());
    assert!(child_terminal_report.is_none());
}
