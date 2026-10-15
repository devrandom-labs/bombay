use crate::address::MailAddr;
use crate::local::effects::ApplicationCapabilities;
use crate::termination::{TerminalReportDisposition, Termination, TerminationSelection};
use behavior::{
    ActionItem, Behavior, ChildReport, CreationId, EventIngress, Here, InterpretItem,
    ItemSettlement, ParentReportReason, Protocol, ReportToParent,
};
use behavior_actors::{InstallShutdownPlan, ReportShutdownPlan, ReportTerminalOutcome};
use communication::{ControlClosed, ControlSender};
use core::future::Future;
use core::mem;
use tokio::sync::oneshot;

impl<C, N, P, Bindings, Origins> TerminalReportTransaction
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition) {
        self.terminal_reports.finish(disposition);
    }
}

impl<C, N, P, Bindings, Origins, Report, Path> InterpretItem<ReportToParent<Report>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    P: ParentReporting<Report> + Send,
    Report: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportToParent<Report> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportToParent<Report> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportToParent<Report>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = match self.parent_reports.report(request.into_inner()) {
                Ok(()) => ItemSettlement::Accepted(()),
                Err(report) => ItemSettlement::Rejected {
                    item: ReportToParent::new(report),
                    reason: ParentReportReason::ClosedParentControlLane,
                },
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Path>
    InterpretItem<ReportTerminalOutcome<MailAddr>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportTerminalOutcome<MailAddr> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportTerminalOutcome<MailAddr> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportTerminalOutcome<MailAddr>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                self.terminal_reports.report_outcome(request);
                ItemSettlement::Accepted(())
            };
            *received = Some(settlement);
        }
    }
}

impl<C, N, P, Bindings, Origins, Plan, Path> InterpretItem<ReportShutdownPlan<Plan>, C::Event, Path>
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: EventIngress<Here, InstallShutdownPlan<Plan>>,
    Plan: Send,
    Self: Send,
{
    #[expect(
        clippy::manual_async_fn,
        reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
    )]
    fn interpret_item<'a>(
        &'a mut self,
        input: <ReportShutdownPlan<Plan> as ActionItem>::Input<'a>,
        received: &'a mut Option<<ReportShutdownPlan<Plan> as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        ReportShutdownPlan<Plan>: 'a,
    {
        async move {
            if received.is_some() {
                return;
            }
            let Some(request) = input.take() else {
                return;
            };
            let settlement = {
                match self.control.send(request.into_event()) {
                    Ok(()) => ItemSettlement::Accepted(()),
                    Err(ControlClosed(_)) => {
                        unreachable!("the actor control lane remains live while its effects commit")
                    }
                }
            };
            *received = Some(settlement);
        }
    }
}

pub(crate) trait TerminalReportTransaction {
    fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition);
}

impl LocalTerminalReports {
    pub(crate) const fn new(report: oneshot::Sender<Termination<MailAddr>>) -> Self {
        Self {
            selection: TerminationSelection::new(),
            report: Some(report),
        }
    }

    pub(crate) fn finish(&mut self, disposition: TerminalReportDisposition) {
        self.selection.finish(disposition);
    }

    pub(crate) fn report_outcome(&mut self, report: ReportTerminalOutcome<MailAddr>) {
        let outcome: Termination<MailAddr> = report.outcome;
        self.selection.select(outcome);
    }

    /// Acquire the original one-shot result outside terminal-report disposal.
    /// A refused outcome does not recreate the consumed sender.
    pub(crate) fn receive_retirement(
        &mut self,
        publication: &mut Option<Result<(), Termination<MailAddr>>>,
    ) {
        if publication.is_some() {
            return;
        }
        if matches!(self.selection, TerminationSelection::Selected(_)) && self.report.is_none() {
            return;
        }
        let outcome = match mem::replace(&mut self.selection, TerminationSelection::new()) {
            TerminationSelection::Selected(outcome) => outcome,
            unselected @ TerminationSelection::Unselected => {
                self.selection = unselected;
                drop(self.report.take());
                return;
            }
        };
        let report = self
            .report
            .take()
            .expect("the original selected report owns its sender");
        *publication = Some(report.send(outcome));
        if matches!(publication, Some(Err(_))) {
            unreachable!("the incarnation retirement owns the report receiver");
        }
    }

    /// Retirement is complete when the original selection and sender are gone.
    /// An acquired refusal remains in the outside publication result.
    pub(crate) fn retirement_complete(&self) -> bool {
        self.report.is_none() && !matches!(self.selection, TerminationSelection::Selected(_))
    }
}

pub(crate) trait ParentReporting<Report> {
    fn report(&self, report: Report) -> Result<(), Report>;
}

impl<Event, Child, Position> LocalParentReports<Event, Child, Position> {
    pub(crate) const fn new(child: CreationId, parent: ControlSender<Event>) -> Self {
        Self {
            child,
            parent,
            occurrence: core::marker::PhantomData,
        }
    }

    pub(crate) fn report<Report>(&self, report: Report) -> Result<(), Report>
    where
        Child: Behavior,
        Event: EventIngress<Position, ChildReport<Report>>,
    {
        self.parent
            .send_map(report, |report| {
                Event::ingress(ChildReport::new(self.child, report))
            })
            .map_err(|ControlClosed(report)| report)
    }
}

impl<Event, Child, Position, Report> ParentReporting<Report>
    for LocalParentReports<Event, Child, Position>
where
    Child: Behavior,
    Event: EventIngress<Position, ChildReport<Report>>,
{
    fn report(&self, report: Report) -> Result<(), Report> {
        LocalParentReports::report(self, report)
    }
}

pub(crate) struct LocalTerminalReports {
    selection: TerminationSelection<MailAddr>,
    report: Option<oneshot::Sender<Termination<MailAddr>>>,
}

pub(crate) struct LocalParentReports<Event, Child, Position> {
    child: CreationId,
    parent: ControlSender<Event>,
    occurrence: core::marker::PhantomData<fn() -> (Child, Position)>,
}

#[cfg(test)]
mod parent_conversion_custody {
    use crate::ActorCustody;
    use crate::MailboxCapacity;

    use core::ptr;
    use std::panic::resume_unwind;
    use std::sync::{Arc, Mutex, PoisonError};

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, ChildChoice, ChildCons,
        ChildCreationOutcome, ChildHead, ChildProduct, Children, ComposedEvent, CreationId,
        CreationKind, CreationSequence, CreationSettlement, Creations, EstablishedCreation,
        EstablishedDelivery, EstablishedRecipient, Here, InitializationTurn, InjectEvent,
        Interpretation, InterpretationProgress, InterpreterRequests, ItemSettlement,
        MessageProtocol, Never, NoBirths, NoChildren, NoSends, RetirementBirths, SendEffects,
        SendLayer, SettledItem, Step, Stopped, User, UserEvent,
    };
    use behavior_actors::ObserveEstablishedCreation;
    use bombay_engine::Completion;
    use communication::{Config, Received, mailbox_channel};
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::launch::spawn_local_execution;
    use crate::local::children::StructuralOrigins;
    use crate::local::children::{ChildBindings, ChildBindingsIn};
    use crate::local::effects::ActionInterpreter;
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs, NoParent};
    use crate::local::endpoint::ActorRef;
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::LocalResidual;
    use crate::local::ingress::{Admission, StandardIngress};
    use crate::observe;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};
    use crate::{
        ActorExecutionOutcome, ActorFailureAssessment, ActorRetirementReport, ActorSpace,
        RetirementAssessment,
    };

    static CONVERSION_SERIAL: Mutex<()> = Mutex::new(());
    #[expect(
        clippy::type_complexity,
        reason = "original report and separately allocated native panic carrier have one affine custody"
    )]
    static CONVERSION_CUSTODY: Mutex<
        Option<(
            oneshot::Sender<EstablishedCreation<Child, ChildHead>>,
            Box<Arc<Vec<u8>>>,
        )>,
    > = Mutex::new(None);
    #[expect(
        clippy::type_complexity,
        reason = "child verification keeps its original publication and allocation"
    )]
    static CHILD_RETIREMENT: Mutex<
        Option<(oneshot::Sender<ChildOrigin<Parent, ChildHead>>, usize)>,
    > = Mutex::new(None);

    #[derive(Clone, Copy)]
    enum ParentCommand {
        Inspect,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Notice {
        Occupied(u8),
        BirthsCommitted,
    }

    type NoticeProtocol = MessageProtocol<MailAddr, Notice>;
    type ParentProtocol = MessageProtocol<MailAddr, ParentCommand>;
    type ParentChildren = ChildCons<MailAddr, Child, NoChildren>;
    type ParentSends = SendLayer<
        InterpreterRequests<ObserveEstablishedCreation<Child, ChildHead>>,
        Vec<EstablishedDelivery<NoticeProtocol>>,
    >;

    enum ParentEvent {
        User(User<MailAddr, ParentCommand>),
        CapabilityReturned(Arc<Vec<u8>>),
    }

    impl UserEvent for ParentEvent {
        type Addr = MailAddr;
        type Message = ParentCommand;

        fn user(from: MailAddr, message: ParentCommand) -> Self {
            Self::User(User::new(from, message))
        }

        fn into_user(self) -> Result<User<MailAddr, ParentCommand>, Self> {
            match self {
                Self::User(user) => Ok(user),
                returned @ Self::CapabilityReturned(_) => Err(returned),
            }
        }
    }

    impl ComposedEvent for ParentEvent {
        type Inner = User<MailAddr, ParentCommand>;

        fn from_inner(event: Self::Inner) -> Self {
            Self::User(event)
        }
    }

    impl InjectEvent<EstablishedCreation<Child, ChildHead>, Here> for ParentEvent {
        fn inject_at(report: EstablishedCreation<Child, ChildHead>) -> Self {
            let custody = CONVERSION_CUSTODY
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("the dedicated conversion owns its original publication and payload");
            let (publication, payload) = custody;
            let admitted = publication.send(report);
            match admitted {
                Ok(()) => {}
                Err(rejected) => {
                    drop(rejected);
                    panic!("the original report receiver must remain alive");
                }
            }
            // This application conversion is outside every Behavior fold.
            // No inspection of the resulting native payload is performed.
            resume_unwind(payload);
        }
    }

    struct Child {
        entries: Arc<Vec<u8>>,
    }

    impl Behavior for Child {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = NoSends;
        type Birth = NoBirths;
        type Ph = Never;
        type Error = Never;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::stop())
        }

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    impl BehaviorBase for Child {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    struct Parent {
        entries: Arc<Vec<u8>>,
        child: Option<Child>,
        child_id: CreationId,
        recipient: EstablishedRecipient<NoticeProtocol>,
        inspection: Option<ParentCommand>,
        turns: usize,
        returned: Vec<Arc<Vec<u8>>>,
    }

    impl Behavior for Parent {
        type Protocol = ParentProtocol;
        type Event = ParentEvent;
        type Sends = ParentSends;
        type Birth = RetirementBirths<<ParentChildren as ChildProduct<MailAddr>>::Choice>;
        type Ph = Never;
        type Error = Never;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let child = self.child.take().expect("one original declared child");
            let creates = Children::<MailAddr>::new()
                .child(self.child_id, child)
                .into_creates();
            let deliveries = vec![EstablishedDelivery::new(
                self.recipient.clone(),
                Notice::BirthsCommitted,
            )];
            let requests = match self.inspection.take() {
                Some(ParentCommand::Inspect) => InterpreterRequests::one(
                    ObserveEstablishedCreation::<Child, ChildHead>::new(self.child_id),
                ),
                None => InterpreterRequests::empty(),
            };
            let sends = ParentSends::new(requests, deliveries);
            Ok(Actions::new(sends, creates, Step::Continue))
        }

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                ParentEvent::User(User {
                    message: ParentCommand::Inspect,
                    ..
                }) => {
                    self.turns += 1;
                    let requests =
                        InterpreterRequests::one(
                            ObserveEstablishedCreation::<Child, ChildHead>::new(self.child_id),
                        );
                    let sends = ParentSends::new(requests, Vec::new());
                    Ok(Actions::new(sends, Creations::default(), Step::Continue))
                }
                ParentEvent::CapabilityReturned(entries) => {
                    self.returned.push(entries);
                    Ok(Actions::cont())
                }
            }
        }
    }

    impl BehaviorBase for Parent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    impl crate::RetirementSchema for ChildTerminal {
        type Failure = crate::ChildFailure<ChildOrigin<Parent, ChildHead>, Child>;
    }
    impl crate::NativeChild<Parent, ChildHead, Child, ChildTerminal, Never>
        for ActorSpace<ParentProtocol>
    {
        fn registration(&self) -> crate::ActorSpace<<Child as behavior::Behavior>::Protocol> {
            crate::ActorSpace::new()
        }
        fn establish<Origin, OriginFn>(
            pending: crate::PendingChild<
                '_,
                Parent,
                Self,
                ChildHead,
                Child,
                ChildTerminal,
                Never,
                OriginFn,
            >,
        ) -> impl core::future::Future<
            Output = behavior::ItemSettlement<
                behavior::RoutedCreation<crate::MailAddr, Child>,
                behavior::ChildCreationOutcome<Child, ChildHead>,
                behavior::CreationRejection,
                Never,
            >,
        > + Send
        where
            ChildTerminal:
                crate::ProjectTerminal<Origin, Child, Descendants = Never> + Send + 'static,
            Origin: Send + 'static,
            OriginFn: Fn(crate::MailAddr, u64, behavior::CreationId, behavior::CreationKind) -> Origin
                + Send,
        {
            <Self as crate::EstablishNativeChild<Parent, Child, Never>>::establish_native_child::<
                ChildHead,
                ChildTerminal,
                Origin,
                OriginFn,
            >(pending)
        }
    }
    struct ChildTerminal {
        origin: ChildOrigin<Parent, ChildHead>,
        retirement: ActorRetirement<Child, Never, Vec<Never>>,
    }

    impl ProjectTerminal<ChildOrigin<Parent, ChildHead>, Child> for ChildTerminal {
        type Descendants = Never;
        fn project(
            origin: ChildOrigin<Parent, ChildHead>,
            retirement: ActorRetirement<Child, Never, Vec<Never>>,
        ) -> Self {
            let observation = CHILD_RETIREMENT
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("the original child owns its verification publication");
            // The acknowledgement closes if any complete retirement oracle panics.
            let (publication, allocation) = observation;
            let ActorRetirement::Completed {
                custody:
                    ActorCustody {
                        received_interpretation: retirement_received_interpretation,
                        received_source: retirement_received_source,
                        source_index: retirement_source_index,
                        acquired_ingress: retirement_acquired_ingress,
                        terminal_report: retirement_terminal_report,
                        retirement_failures: retirement_native_failures,
                        additional_failures: retirement_additional_failures,
                        child_failures: native_failures,
                        behavior,
                        control,
                        user,
                        descendants,
                        capability_failures,
                        unread_owner_cancellation,
                    },
                interpretation: retirement_interpretation,
                source: retirement_source,
                settlements,
                completion,
            } = &retirement
            else {
                panic!("the pure child stops in its ordinary initialization");
            };
            assert_eq!(native_failures.as_slice(), []);
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());
            assert!(retirement_additional_failures.is_empty());

            assert_eq!(behavior.entries.as_slice(), [31, 37, 41]);
            assert_eq!(behavior.entries.as_ptr() as usize, allocation);
            assert_eq!(completion, &Completion::Stopped);
            assert_eq!(settlements.len(), 1);
            assert_eq!(control.as_slice(), []);
            assert_eq!(user.as_slice(), []);
            assert_eq!(descendants.as_slice(), []);
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            let settlement = &settlements[0];
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends, NoSends));
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
            let verified = publication.send(origin);
            match verified {
                Ok(()) => {}
                Err(rejected) => panic!("the host retains the child verifier: {rejected:?}"),
            }
            // No await follows publication: on the current-thread runtime this
            // poll returns the complete original terminal before the host resumes.
            Self { origin, retirement }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one complete actor cleanup precedes every ownership assertion"
    )]
    fn assert_parent_conversion_preserves_owned_retirement(inspection: Option<ParentCommand>) {
        let _serial = CONVERSION_SERIAL
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the actual local actor runtime");
        let original_parent = Arc::new(vec![43, 47, 53]);
        let original_child = Arc::new(vec![31, 37, 41]);
        let original_capability = Arc::new(vec![59, 61, 67]);
        let original_payload = Box::new(Arc::new(vec![71_u8, 73, 79]));
        let parent_allocation = original_parent.as_ptr() as usize;
        let child_allocation = original_child.as_ptr() as usize;
        let capability_allocation = original_capability.as_ptr() as usize;
        let payload_carrier = ptr::from_ref(original_payload.as_ref()).cast::<()>();
        let parent_owner = Arc::downgrade(&original_parent);
        let child_owner = Arc::downgrade(&original_child);
        let capability_owner = Arc::downgrade(&original_capability);
        let payload_owner = Arc::downgrade(original_payload.as_ref());
        let payload_before_cleanup = payload_owner.clone();
        let (report_publication, report_receiver) = oneshot::channel();
        let (child_publication, child_receiver) = oneshot::channel();
        *CONVERSION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((report_publication, original_payload));
        *CHILD_RETIREMENT
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((child_publication, child_allocation));
        let mut creations = CreationSequence::new();
        let id = creations.issue().expect("one actual original creation ID");
        let roots = ActorSpace::<ParentProtocol>::new();
        let addresses = ApplicationAddresses::new();
        let notice_address = addresses.allocate().expect("one actual external address");
        let (
            joined,
            task_id,
            observed_origin,
            reported,
            first,
            second,
            third,
            startup,
            payload_count_before_cleanup,
            actor_report,
        ) = runtime.block_on(async move {
            let (_control, owner, mailbox, mut receiver) =
                mailbox_channel::<Never, User<MailAddr, Notice>>(Config::new(2));
            let admission = Arc::new(Admission::new(owner));
            let (_notice_publication, notice_observation) = observe::pair();
            let notice_endpoint = ActorRef::<NoticeProtocol>::external(
                notice_address,
                mailbox,
                Arc::downgrade(&admission),
                notice_observation,
            );
            // Communication rounds the minimum capacity up with floor two.
            // Both original slots are full before the parent starts.
            let earlier_occupied = notice_endpoint
                .send_from(MailAddr::APPLICATION_ROOT, Notice::Occupied(11))
                .await;
            let later_occupied = notice_endpoint
                .send_from(MailAddr::APPLICATION_ROOT, Notice::Occupied(13))
                .await;
            let (capability_ready, capability_started) = oneshot::channel();
            let (release_capability, capability_permission) = oneshot::channel();
            let actor_spaces = Arc::new(roots.clone());
            let parent = Parent {
                entries: original_parent,
                child: Some(Child {
                    entries: original_child,
                }),
                child_id: id,
                recipient: notice_endpoint.established_recipient(),
                inspection,
                turns: 0,
                returned: Vec::new(),
            };
            let (authority, startup, (), task, termination_notification) =
                spawn_local_execution::<Parent, _, StandardIngress, _, _, _>(
                    roots,
                    Config::new(1),
                    MailAddr::APPLICATION_ROOT,
                    parent,
                    #[cfg(feature = "remote")]
                    None,
                    move |control, terminal_reports, timers, observations| {
                        let mut capabilities = ApplicationCapabilities::<
                        Parent,
                        ActorSpace<ParentProtocol>,
                        NoParent,
                        ChildBindings<Parent, ChildTerminal, StructuralOrigins<Parent>>,
                        StructuralOrigins<Parent>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr::APPLICATION_ROOT,
                            actor_spaces: actor_spaces.clone(),
                            allocations: addresses,
mailbox_capacity: MailboxCapacity::default(),
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        ChildBindings::<Parent, ChildTerminal, StructuralOrigins<Parent>>::new_in(
                            actor_spaces.as_ref(),
                        ),
                    );
                        // Advanced runtime/test-host admission only. The exact
                        // existing ActivationTasks owner, not a new work service,
                        // owns this original gate and immutable returned value.
                        capabilities
                            .activation_tasks
                            .as_mut()
                            .expect("live activation tasks remain installed")
                            .spawn(async move {
                                let admitted = capability_ready.send(());
                                match admitted {
                                    Ok(()) | Err(()) => {}
                                }
                                let permission = capability_permission.await;
                                match permission {
                                    Ok(()) | Err(_) => {}
                                }
                                Err(ParentEvent::CapabilityReturned(original_capability))
                            });
                        ActionInterpreter::new(capabilities)
                    },
                    |environment| {
                        let (publication, startup) = oneshot::channel();
                        let environment = environment.publish_with(move |actor| {
                            let admitted = publication.send(actor);
                            match admitted {
                                Ok(()) | Err(_) => {}
                            }
                        });
                        (environment, startup, ())
                    },
                );
            let task_id = task.id();
            let started = capability_started.await;
            let observed_origin = child_receiver.await;
            // The first delivery cannot be admitted before this actual
            // child task and its total eager projector finish their poll.
            let first = receiver.recv().await;
            let second = receiver.recv().await;
            let third = receiver.recv().await;
            let startup = match inspection {
                Some(ParentCommand::Inspect) => startup.await.map(|_| ()),
                None => match startup.await {
                    Ok(actor) => {
                        let admitted = actor
                            .send_from(notice_address, ParentCommand::Inspect)
                            .await;
                        match admitted {
                            Ok(()) => Ok(()),
                            Err(rejected) => match rejected.into_message() {
                                ParentCommand::Inspect => {
                                    panic!("the live parent admits the actual user turn");
                                }
                            },
                        }
                    }
                    Err(error) => Err(error),
                },
            };
            let reported = report_receiver.await;
            // No assertion or negative oracle can retain this gate.
            let payload_count_before_cleanup = payload_before_cleanup.strong_count();
            let released = release_capability.send(());
            match released {
                Ok(()) | Err(()) => {}
            }
            let joined = task.await;
            let notification = termination_notification
                .await
                .expect("the actual termination producer transferred its result");
            let actor_report = ActorRetirementReport::from_joined(&joined, &notification);
            notification.expect("the ordinary termination publication succeeded");
            drop(authority);
            // All control calls/transfers happen before these observations.
            assert!(earlier_occupied.is_ok());
            assert!(later_occupied.is_ok());
            assert!(started.is_ok());
            (
                joined,
                task_id,
                observed_origin,
                reported,
                first,
                second,
                third,
                startup,
                payload_count_before_cleanup,
                actor_report,
            )
        });
        // Gate release and the actual raw root join precede executor destruction;
        // runtime Drop settles remaining detached/aborted task resources.
        drop(runtime);
        let stale_conversion = CONVERSION_CUSTODY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let stale_child = CHILD_RETIREMENT
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop((stale_conversion, stale_child));
        let origin = observed_origin.expect("the actual child completed every typed oracle");
        let report = reported.expect("the original conversion transferred its complete report");
        assert_eq!(report.id(), id);
        assert_eq!(report.kind(), CreationKind::Birth);
        let Ok(committed) = report.into_committed() else {
            panic!("the actual child binding committed");
        };
        let (_, _, actor) = committed.into_parts();
        let recipient = actor.into_recipient();
        let endpoint = recipient.interpret(&mut ExtractLocalEndpoint);
        assert_eq!(endpoint.address(), origin.address());
        match first {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::Occupied(11));
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match second {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::Occupied(13));
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match third {
            Some(Received::User(user)) => {
                assert_eq!(user.from, MailAddr::APPLICATION_ROOT);
                assert_eq!(user.message, Notice::BirthsCommitted);
            }
            Some(Received::Control(never)) => match never {},
            Some(Received::UserLaneClosed) => {
                panic!("the original user lane remains open through retirement")
            }
            None => panic!("the actual external mailbox remains open through retirement"),
        }
        match inspection {
            Some(ParentCommand::Inspect) => assert!(startup.is_err()),
            None => assert!(startup.is_ok()),
        }
        if let Err(error) = &joined {
            assert_eq!(error.id(), task_id);
            assert!(error.is_panic());
        }
        assert_eq!(actor_report.retirement(), RetirementAssessment::Established);
        assert_eq!(
            actor_report.failures(),
            ActorFailureAssessment::FailuresFound
        );
        let parent_retained = parent_owner.upgrade();
        let child_retained = child_owner.upgrade();
        let capability_retained = capability_owner.upgrade();
        let payload_retained = payload_owner.strong_count();
        // The original raw JoinError already preserves its native payload. It
        // does not preserve the outside Parent, child result or capability value.
        assert_eq!(payload_count_before_cleanup, 1);
        assert_eq!(payload_retained, 1);
        assert!(
            joined.is_ok(),
            "conversion must retire the outside parent ownership before returning"
        );
        let Ok(outcome) = &joined else {
            panic!("the joined actor returns its outside owners");
        };
        match (inspection, outcome) {
            (
                Some(ParentCommand::Inspect),
                ActorExecutionOutcome::ActivationPanicked {
                    residual: LocalResidual::Uncommitted { .. },
                    ..
                },
            )
            | (
                None,
                ActorExecutionOutcome::HostExecutionPanicked {
                    residual: LocalResidual::Retired { .. },
                    ..
                },
            ) => {}
            _ => panic!(
                "the originating host operation must retain its exact native panic provenance"
            ),
        }
        let (ActorExecutionOutcome::ActivationPanicked {
            behavior: parent,
            residual,
            additional_failures,
            ..
        }
        | ActorExecutionOutcome::HostExecutionPanicked {
            behavior: parent,
            residual,
            additional_failures,
            ..
        }) = outcome
        else {
            panic!("classification cannot substitute for available actor ownership");
        };
        assert!(additional_failures.is_empty());
        assert_eq!(parent.entries.as_slice(), [43, 47, 53]);
        assert_eq!(parent.entries.as_ptr() as usize, parent_allocation);
        assert!(parent.child.is_none());
        assert_eq!(parent.child_id, id);
        assert!(parent.inspection.is_none());
        assert_eq!(parent.turns, usize::from(inspection.is_none()));
        assert_eq!(parent.returned.len(), 0);
        let parent_endpoint = parent
            .recipient
            .clone()
            .interpret(&mut ExtractLocalEndpoint);
        assert_eq!(parent_endpoint.address(), notice_address);
        let (
            descendants,
            ingress,
            activation_tasks,
            failures,
            unread,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            current,
            operation_failures,
            descendant_report,
        ) = match residual {
            LocalResidual::Uncommitted {
                initialization,
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                operation_failures,
                descendant_report,
            } => (
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                Some(initialization),
                operation_failures,
                descendant_report,
            ),
            LocalResidual::Retired {
                interpretation,
                source,
                settlements: _,
                descendants,
                ingress,
                activation_tasks,
                capability_failures,
                unread_owner_cancellation,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                operation_failures,
                descendant_report,
            } => {
                assert!(source.is_none());
                (
                    descendants,
                    ingress,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    interpretation.as_ref(),
                    operation_failures,
                    descendant_report,
                )
            }
            LocalResidual::Prepared { .. } => {
                panic!("the real conversion began with installed original Actions progress");
            }
        };
        assert_eq!(*operation_failures, ActorFailureAssessment::NoFailuresFound);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::Established
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::NoFailuresFound
        );
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(current.is_some());
        assert!(activation_tasks.is_empty());
        assert_eq!(failures.len(), 0);
        assert!(unread.is_none());
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(ingress.control.len(), 1);
        let ParentEvent::CapabilityReturned(returned) = &ingress.control[0] else {
            panic!("the original completed operation is retained as its typed control fact");
        };
        assert_eq!(returned.as_slice(), [59, 61, 67]);
        assert_eq!(returned.as_ptr() as usize, capability_allocation);
        let (descendants, occurrence_failures) = descendants;
        assert_eq!(occurrence_failures.len(), 0);
        assert_eq!(descendants.len(), 1);
        let terminal = &descendants[0];
        assert_eq!(terminal.origin.address(), origin.address());
        assert_eq!(terminal.origin, origin);
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    received_interpretation: child_retirement_received_interpretation,
                    received_source: child_retirement_received_source,
                    source_index: child_retirement_source_index,
                    acquired_ingress: child_retirement_acquired_ingress,
                    terminal_report: child_retirement_terminal_report,
                    retirement_failures: child_retirement_native_failures,
                    additional_failures: child_retirement_additional_failures,
                    behavior: child,
                    control: child_control,
                    user: child_user,
                    descendants: child_descendants,
                    child_failures: native_failures,
                    capability_failures: child_failures,
                    unread_owner_cancellation: child_unread,
                },
            interpretation: child_retirement_interpretation,
            source: child_retirement_source,
            settlements: child_settlements,
            completion: child_completion,
        } = &terminal.retirement
        else {
            panic!("the original full child terminal remains available");
        };
        assert_eq!(native_failures.as_slice(), []);
        assert!(child_retirement_interpretation.is_none());
        assert!(child_retirement_source.is_none());
        assert!(child_retirement_received_interpretation.is_none());
        assert!(child_retirement_received_source.is_none());
        assert!(child_retirement_source_index.is_none());
        assert!(child_retirement_acquired_ingress.is_none());
        assert!(child_retirement_terminal_report.is_none());
        assert!(child_retirement_native_failures.is_empty());
        assert!(child_retirement_additional_failures.is_empty());

        assert_eq!(child.entries.as_slice(), [31, 37, 41]);
        assert_eq!(child.entries.as_ptr() as usize, child_allocation);
        assert_eq!(*child_completion, Completion::Stopped);
        assert_eq!(child_control.len(), 0);
        assert_eq!(child_user.len(), 0);
        assert_eq!(child_descendants.len(), 0);
        assert_eq!(child_failures.len(), 0);
        assert!(child_unread.is_none());
        assert_eq!(child_settlements.len(), 1);
        assert_eq!(child_settlements[0].sends, NoSends);
        assert!(child_settlements[0].creations.is_empty());
        assert!(matches!(child_settlements[0].become_, Step::Stop(Stopped)));
        let parent_retained =
            parent_retained.expect("the returned outcome owns original parent state");
        let child_retained =
            child_retained.expect("the returned child terminal owns original state");
        let capability_retained =
            capability_retained.expect("the joined capability return is retained");
        assert_eq!(parent_retained.as_slice(), [43, 47, 53]);
        assert_eq!(parent_retained.as_ptr() as usize, parent_allocation);
        assert_eq!(child_retained.as_slice(), [31, 37, 41]);
        assert_eq!(child_retained.as_ptr() as usize, child_allocation);
        assert_eq!(capability_retained.as_slice(), [59, 61, 67]);
        assert_eq!(capability_retained.as_ptr() as usize, capability_allocation);
        // Current original progress was observed above independently of the
        // complete prior row. Explicit final discharge retains the original
        // opaque cause and every recovered row until all actual joins finish.
        let outcome = joined.expect("outside-owner retirement returned");
        let (
            current,
            prior,
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
            payload,
        ) = match ActorRetirement::from_local(outcome) {
            ActorRetirement::HostCommitPanicked {
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
                initialization: Some(current),
                payload,
            } => (
                current,
                Vec::new(),
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
                payload,
            ),
            ActorRetirement::HostExecutionPanicked {
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
                interpretation: Some(current),
                source: None,
                settlements: prior,
                payload,
            } => (
                current,
                prior,
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
                payload,
            ),
            other => {
                drop(other);
                panic!(
                    "the exact caught operation retains its original current progress and native cause"
                );
            }
        };
        assert_eq!(
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            payload_carrier
        );
        assert_eq!(payload_owner.strong_count(), 1);
        assert!(additional_failures.is_empty());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(retirement_failures.is_empty());
        assert!(terminal_report.is_none());
        assert!(unread_owner_cancellation.is_none());
        let InterpretationProgress::Interpreting((
            (
                Some(InterpretationProgress::Completed(Interpretation::Complete(creations))),
                Some(InterpretationProgress::Interpreting(sends)),
            ),
            Step::Continue,
        )) = current
        else {
            panic!("original current progress keeps completed creations and interrupted sends");
        };
        let CreationSettlement::Settled(created) = creations.into_settlement() else {
            panic!("current creation policy preserves its exact original Settled product");
        };
        let expected_current = usize::from(inspection.is_some());
        assert_eq!(created.len(), expected_current);
        for creation in &created {
            let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
                ChildCreationOutcome::Established(committed),
            ))) = creation
            else {
                panic!("current original head-role establishment remains exact");
            };
            assert_eq!(committed.id(), id);
            assert_eq!(committed.kind(), CreationKind::Birth);
            let endpoint = committed
                .actor()
                .into_recipient()
                .interpret(&mut ExtractLocalEndpoint);
            assert_eq!(endpoint.address(), origin.address());
        }
        let Some(InterpretationProgress::Completed(Interpretation::Complete(deliveries))) =
            &sends.inner
        else {
            panic!("earlier standard delivery results survive the conversion producer");
        };
        assert_eq!(deliveries.len(), expected_current);
        for delivery in deliveries {
            assert!(matches!(
                delivery,
                SettledItem::Attempted(ItemSettlement::Accepted(()))
            ));
        }
        let Some(InterpretationProgress::Interpreting(requests)) = &sends.owned else {
            panic!("actual interrupted observation request retains its original owning lane");
        };
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0],
            Some(InterpretationProgress::Interpreting((None, None)))
        ));
        // This application conversion explicitly consumed the report into the
        // original external receiver. No lower reply or input is reconstructed.
        assert_eq!(prior.len(), usize::from(inspection.is_none()));
        for initial in prior {
            assert!(matches!(initial.become_, Step::Continue));
            assert_eq!(initial.sends.owned.len(), 0);
            // Accepted unit delivery is explicitly discharged on source admission.
            assert_eq!(initial.sends.inner.len(), 0);
            let CreationSettlement::Settled(created) = initial.creations.into_settlement() else {
                panic!("original prior creation receipt remains retained");
            };
            let Ok(created) = created.into_one() else {
                panic!("one original prior creation receipt remains");
            };
            let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
                ChildCreationOutcome::Established(committed),
            ))) = created
            else {
                panic!("prior original head-role establishment remains exact");
            };
            let (created_id, created_kind, actor) = committed.into_parts();
            assert_eq!(created_id, id);
            assert_eq!(created_kind, CreationKind::Birth);
            let endpoint = actor.into_recipient().interpret(&mut ExtractLocalEndpoint);
            assert_eq!(endpoint.address(), origin.address());
        }
        drop((
            created,
            sends,
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
            payload,
        ));
        drop(parent_retained);
        drop(child_retained);
        drop(capability_retained);

        // Explicit final result discharge only AFTER raw actor and its known
        // children/capability tasks have completed their normal retirement.
        assert_eq!(parent_owner.strong_count(), 0);
        assert_eq!(child_owner.strong_count(), 0);
        assert_eq!(capability_owner.strong_count(), 0);
        assert_eq!(payload_owner.strong_count(), 0);
    }

    #[test]
    fn initialization_conversion_keeps_parent_child_and_capability_owners() {
        assert_parent_conversion_preserves_owned_retirement(Some(ParentCommand::Inspect));
    }

    #[test]
    fn active_conversion_keeps_parent_child_and_capability_owners() {
        assert_parent_conversion_preserves_owned_retirement(None);
    }
}

#[cfg(test)]
mod parent_report_custody {
    use crate::MailboxCapacity;
    use crate::actor;
    use crate::actors::ActorExt;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::local::children::{NoChildBindings, StructuralOrigins};
    use crate::local::effects::observation::TerminationObservations;
    use crate::local::effects::reports::{LocalParentReports, LocalTerminalReports};
    use crate::local::effects::timers::LocalTimers;
    use crate::local::effects::{
        ActionInterpreter, ApplicationCapabilities, ApplicationCapabilityInputs, CommitActions,
        NoParent,
    };
    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Births, ChildReport,
        ClassifySettlement, CreationSequence, EventIngress, Here, InterpretItem, InterpretSends,
        Interpretation, InterpretationProgress, InterpreterRequests, ItemSettlement,
        MessageProtocol, Never, NoSends, ParentReportReason, ReportToParent, SendSettlements,
        SettledItem, SettlementStatus, SourceCustody, SourceProgress, Step, User, UserEvent,
    };
    use behavior_actors::{OneShot, StopOnShutdown, TimerId};
    use communication::{Config, mailbox_channel};
    use std::future::Future;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;

    static REPORT_MAPPINGS: AtomicUsize = AtomicUsize::new(0);

    struct WorkerReport {
        revision: u64,
        bytes: Box<[u8]>,
        changed: Vec<u64>,
    }
    impl WorkerReport {
        fn new(revision: u64) -> Self {
            Self {
                revision,
                bytes: vec![31, 37, 41].into_boxed_slice(),
                changed: vec![43, 47],
            }
        }
    }
    struct ReportingChild {
        report: Option<Box<WorkerReport>>,
    }
    #[actor(message = Never, sends = InterpreterRequests<ReportToParent<Box<WorkerReport>>>)]
    impl ReportingChild {
        #[expect(
            clippy::unnecessary_wraps,
            reason = "the owning actor initialization contract returns BehaviorActed even with an uninhabited error"
        )]
        fn init(&mut self) -> BehaviorActed<Self> {
            Ok(Actions::stop().with_send(ReportToParent::new(
                self.report.take().expect("one original child report"),
            )))
        }
    }

    enum ReportEvent {
        Child(ChildReport<Box<WorkerReport>>),
    }
    impl UserEvent for ReportEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(_: MailAddr, never: Never) -> Self {
            match never {}
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Err(self)
        }
    }
    impl EventIngress<Births<ReportingChild>, ChildReport<Box<WorkerReport>>> for ReportEvent {
        fn ingress(report: ChildReport<Box<WorkerReport>>) -> Self {
            // This observes actual native event construction; no Behavior fold performs I/O.
            REPORT_MAPPINGS.fetch_add(1, Ordering::SeqCst);
            Self::Child(report)
        }
    }
    struct ReportParent {
        reports: Vec<ChildReport<Box<WorkerReport>>>,
    }
    impl BehaviorBase for ReportParent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for ReportParent {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = ReportEvent;
        type Sends = NoSends;
        type Birth = Births<ReportingChild>;
        type Ph = Never;
        type Error = Never;
        fn transition(&mut self, _: ActiveTurn, event: ReportEvent) -> BehaviorActed<Self> {
            let ReportEvent::Child(report) = event;
            self.reports.push(report);
            Ok(Actions::cont())
        }
    }
    fn parent_deadline(
        _: &mut ReportParent,
    ) -> Actions<MailAddr, Never, NoSends, Births<ReportingChild>> {
        Actions::stop()
    }
    fn shutdown_parent_deadline(
        _: &mut StopOnShutdown<ReportParent>,
    ) -> Actions<
        MailAddr,
        Never,
        <StopOnShutdown<ReportParent> as Behavior>::Sends,
        Births<ReportingChild>,
    > {
        Actions::stop()
    }

    type ParentCapabilities<C> = ApplicationCapabilities<
        C,
        (),
        NoParent,
        NoChildBindings<Never>,
        StructuralOrigins<ReportParent>,
    >;

    fn assert_report(report: &WorkerReport, allocation: *const WorkerReport, revision: u64) {
        assert_eq!(std::ptr::from_ref(report), allocation);
        assert_eq!(report.revision, revision);
        assert_eq!(report.bytes.as_ref(), [31, 37, 41]);
        assert_eq!(report.changed, [43, 47]);
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one actual parent/source controller conserves accepted and refused original reports across complete interpretation, replay and source custody"
    )]
    fn report_through_parent<C>(mut parent: C)
    where
        C: Behavior<
                Protocol = MessageProtocol<MailAddr, Never>,
                Birth = Births<ReportingChild>,
                Ph = Never,
                Error = Never,
            > + BehaviorBase<Base = ReportParent>
            + Send,
        C::Event:
            EventIngress<Births<ReportingChild>, ChildReport<Box<WorkerReport>>> + Send + 'static,
        C::Sends: InterpretSends<ParentCapabilities<C>, C::Event, Here> + SendSettlements,
    {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("one actual native report interpreter");
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one original child correlation");
        runtime.block_on(async {
            let (control, owner, mailbox, mut consumer) =
                mailbox_channel::<C::Event, User<MailAddr, Never>>(Config::new(1));
            let (terminal_sender, terminal_receiver) = oneshot::channel();
            let mut parent_capabilities = ParentCapabilities::<C>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr::APPLICATION_ROOT,
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
mailbox_capacity: MailboxCapacity::default(),
                    control: control.clone(),
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(terminal_sender),
                },
                NoChildBindings::default(),
            );
            let initialization = behavior::initialize(&mut parent).unwrap_or_else(|never| match never {});
            assert!(initialization.creates.is_empty());
            assert!(matches!(initialization.become_, Step::Continue));
            let mut timer_progress = Some(InterpretationProgress::Original(initialization.sends));
            <C::Sends as InterpretSends<ParentCapabilities<C>, C::Event, Here>>::interpret(
                &mut timer_progress,
                &mut parent_capabilities,
            )
            .await;
            C::Sends::finish_interpretation(&mut timer_progress);
            let Some(InterpretationProgress::Completed(Interpretation::Complete(timer_settlements))) =
                timer_progress
            else {
                panic!("actual wrapper timer request settles completely");
            };
            assert_eq!(
                timer_settlements.settlement_status(),
                SettlementStatus::Accepted
            );
            let deadline = parent_capabilities
                .timers
                .next_deadline()
                .expect("one actual timer initialization request was interpreted");

            let (child_control, child_owner, child_mailbox, child_consumer) = mailbox_channel::<
                <ReportingChild as Behavior>::Event,
                User<MailAddr, Never>,
            >(Config::new(1));
            let (child_terminal_sender, child_terminal_receiver) = oneshot::channel();
            let mut child_capabilities = ApplicationCapabilities::<
                ReportingChild,
                (),
                NoParent,
                NoChildBindings<Never>,
                StructuralOrigins<ReportingChild>,
            >::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr(17),
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
mailbox_capacity: MailboxCapacity::default(),
                    control: child_control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(child_terminal_sender),
                },
                NoChildBindings::default(),
            )
            .with_parent(LocalParentReports::<
                C::Event,
                ReportingChild,
                Births<ReportingChild>,
            >::new(id, control.clone()));

            let accepted = Box::new(WorkerReport::new(101));
            let accepted_allocation = std::ptr::from_ref(accepted.as_ref());
            let before = REPORT_MAPPINGS.load(Ordering::SeqCst);
            let mut input = Some(ReportToParent::new(accepted));
            let mut received = None;
            <_ as InterpretItem<
                ReportToParent<Box<WorkerReport>>,
                <ReportingChild as Behavior>::Event,
                Here,
            >>::interpret_item(&mut child_capabilities, &mut input, &mut received)
            .await;
            assert!(input.is_none());
            assert!(matches!(received, Some(ItemSettlement::Accepted(()))));
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 1);
            let event = consumer
                .recv_control()
                .await
                .expect("accepted original report queued once");
            let actions =
                behavior::delegate_transition(&mut parent, event).unwrap_or_else(|never| match never {});
            assert!(actions.creates.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
            let [report] = parent.base().reports.as_slice() else {
                panic!("the actual parent fold owns one original report");
            };
            assert_eq!(report.child, id);
            assert_report(&report.report, accepted_allocation, 101);

            let replay = Box::new(WorkerReport::new(103));
            let replay_allocation = std::ptr::from_ref(replay.as_ref());
            let mut replay_input = Some(ReportToParent::new(replay));
            <_ as InterpretItem<
                ReportToParent<Box<WorkerReport>>,
                <ReportingChild as Behavior>::Event,
                Here,
            >>::interpret_item(&mut child_capabilities, &mut replay_input, &mut received)
            .await;
            assert!(matches!(received, Some(ItemSettlement::Accepted(()))));
            assert_report(
                &replay_input
                    .as_ref()
                    .expect("already received reply leaves the next original input alone")
                    .report,
                replay_allocation,
                103,
            );
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 1);
            drop(replay_input);

            // Real pure child initialization is committed with the original all-three custody equations.
            let accepted_source = Box::new(WorkerReport::new(107));
            let accepted_source_allocation = std::ptr::from_ref(accepted_source.as_ref());
            let mut child = ReportingChild {
                report: Some(accepted_source),
            };
            let actions = behavior::initialize(&mut child).unwrap_or_else(|never| match never {});
            let mut interpreter = ActionInterpreter::new(child_capabilities);
            let mut progress = Some(InterpretationProgress::Original(actions));
            <_ as CommitActions<ReportingChild>>::commit(&mut interpreter, &mut progress).await;
            let Some(InterpretationProgress::Completed(Interpretation::Complete(settlements))) = progress
            else {
                panic!("real accepted Actions finish interpretation");
            };
            let mut source = Some(SourceProgress::Original(settlements));
            <_ as CommitActions<ReportingChild>>::offer_next(&mut interpreter, &mut source).await;
            let Some(SourceProgress::Completed(SourceCustody::Exhausted(residual))) = source else {
                panic!("accepted report receipt has no emitter-return input");
            };
            assert!(residual.sends.is_empty());
            let event = consumer
                .recv_control()
                .await
                .expect("real initialization report is delivered");
            let actions =
                behavior::delegate_transition(&mut parent, event).unwrap_or_else(|never| match never {});
            assert!(matches!(actions.become_, Step::Continue));
            let [first, second] = parent.base().reports.as_slice() else {
                panic!("two actual reports preserve correlation and independent payloads");
            };
            assert_eq!(first.child, id);
            assert_eq!(second.child, id);
            assert_report(&second.report, accepted_source_allocation, 107);
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 2);

            let elapsed = parent_capabilities
                .timers
                .pop_due(deadline)
                .expect("the interpreted timer returns its exact typed elapsed event");
            let actions =
                behavior::delegate_transition(&mut parent, elapsed).unwrap_or_else(|never| match never {});
            assert!(actions.creates.is_empty());
            assert!(
                matches!(actions.become_, Step::Stop(_)),
                "each real timer policy stops its owning wrapped parent"
            );
            drop(consumer);

            let capabilities = interpreter.capabilities_mut();
            let refused = Box::new(WorkerReport::new(109));
            let refused_allocation = std::ptr::from_ref(refused.as_ref());
            let mut refused_input = Some(ReportToParent::new(refused));
            let mut refusal = None;
            let refused_attempt = catch_unwind(AssertUnwindSafe(|| {
                let mut attempt = pin!(<_ as InterpretItem<
                    ReportToParent<Box<WorkerReport>>,
                    <ReportingChild as Behavior>::Event,
                    Here,
                >>::interpret_item(
                    capabilities, &mut refused_input, &mut refusal
                ));
                attempt
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }));
            assert!(
                refused_attempt.is_ok(),
                "closed parent must return the complete typed report rejection without interpreting its event mapping"
            );
            assert!(matches!(refused_attempt, Ok(Poll::Ready(()))));
            assert!(refused_input.is_none());
            let Some(ItemSettlement::Rejected {
                item,
                reason: ParentReportReason::ClosedParentControlLane,
            }) = refusal
            else {
                panic!("closed parent returns original ReportToParent with its existing reason");
            };
            assert_report(&item.report, refused_allocation, 109);
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 2);
            let mut refusal = Some(ItemSettlement::Rejected {
                item,
                reason: ParentReportReason::ClosedParentControlLane,
            });
            <_ as InterpretItem<
                ReportToParent<Box<WorkerReport>>,
                <ReportingChild as Behavior>::Event,
                Here,
            >>::interpret_item(capabilities, &mut refused_input, &mut refusal)
            .await;
            let Some(ItemSettlement::Rejected {
                item,
                reason: ParentReportReason::ClosedParentControlLane,
            }) = refusal
            else {
                panic!("replay preserves the exact original rejection");
            };
            assert_report(&item.report, refused_allocation, 109);
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 2);

            let refused_source = Box::new(WorkerReport::new(113));
            let refused_source_allocation = std::ptr::from_ref(refused_source.as_ref());
            let mut child = ReportingChild {
                report: Some(refused_source),
            };
            let actions = behavior::initialize(&mut child).unwrap_or_else(|never| match never {});
            let mut progress = Some(InterpretationProgress::Original(actions));
            <_ as CommitActions<ReportingChild>>::commit(&mut interpreter, &mut progress).await;
            let Some(InterpretationProgress::Completed(Interpretation::Complete(settlements))) = progress
            else {
                panic!("refused action owns its complete interpreted settlement");
            };
            let mut source = Some(SourceProgress::Original(settlements));
            <_ as CommitActions<ReportingChild>>::offer_next(&mut interpreter, &mut source).await;
            let Some(SourceProgress::Completed(SourceCustody::Retained(residual))) = source else {
                panic!("closed report remains in the existing typed source retirement lane");
            };
            let [
                SettledItem::Attempted(ItemSettlement::Rejected {
                    item,
                    reason: ParentReportReason::ClosedParentControlLane,
                }),
            ] = residual.sends.as_slice()
            else {
                panic!("source custody retains the complete original refused report");
            };
            assert_report(&item.report, refused_source_allocation, 113);
            assert_eq!(REPORT_MAPPINGS.load(Ordering::SeqCst), before + 2);
            drop((
                owner,
                mailbox,
                control,
                terminal_receiver,
                child_owner,
                child_mailbox,
                child_consumer,
                child_terminal_receiver,
                timer_settlements,
            ));
        });
    }

    #[test]
    fn parent_report_refusal_preserves_boxed_source_in_timer_shutdown_orders() {
        let parent = ReportParent {
            reports: Vec::new(),
        };
        let shutdown_outer = catch_unwind(AssertUnwindSafe(|| {
            report_through_parent(
                OneShot::new(parent, TimerId(7), Duration::ZERO, parent_deadline)
                    .stop_on_shutdown(),
            );
        }));
        let parent = ReportParent {
            reports: Vec::new(),
        }
        .stop_on_shutdown();
        let timer_outer = catch_unwind(AssertUnwindSafe(|| {
            report_through_parent(OneShot::new(
                parent,
                TimerId(11),
                Duration::ZERO,
                shutdown_parent_deadline,
            ));
        }));
        assert!(shutdown_outer.is_ok());
        assert!(timer_outer.is_ok());
        eprintln!(
            "PASS accepted/correlated/replayed/closed/source-retained report in both timer-shutdown orders"
        );
    }
}
