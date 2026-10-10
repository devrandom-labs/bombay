mod creation;
mod delivery;
pub(crate) mod observation;
mod reports;
pub(crate) mod timers;

use crate::address::{ApplicationAddresses, MailAddr};
use crate::local::children::{NoChildBindings, RetireChildTasks, StructuralOrigins};
use crate::local::effects::observation::TerminationObservations;
use crate::local::effects::timers::LocalTimers;
use crate::local::execution::ActivationTasks;
use crate::termination::{TerminalReportDisposition, Termination};
use behavior::{
    ActionSettlement, ActionSettlements, Behavior, BehaviorAddr, BehaviorMessage,
    BehaviorSettlements, CreationSettlements, InjectEvent, InterpretCreations, InterpretSends,
    InterpretationProgress, Never, Protocol, SendSettlements, SourceProgress,
    SourceSettlementCustody, Step,
};
use behavior_actors::ObservationId;
use bombay_engine::ActionsOf;
use communication::{ControlClosed, ControlSender};
use core::future::Future;
use core::marker::PhantomData;
pub(crate) use reports::LocalTerminalReports;
use reports::TerminalReportTransaction;
use std::any::Any;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::task::JoinError;

impl<C, N, Bindings, Origins> ApplicationCapabilities<C, N, NoParent, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    pub(crate) fn new_with_bindings(
        inputs: ApplicationCapabilityInputs<C, N>,
        child_bindings: Bindings,
    ) -> Self {
        Self {
            actor_spaces: inputs.actor_spaces,
            allocations: inputs.allocations,
            address: inputs.address,
            control: inputs.control,
            timers: inputs.timers,
            observations: inputs.observations,
            next_child_route: 0,
            child_bindings: Some(child_bindings),
            activation_tasks: Some(ActivationTasks::new()),
            exact_observations: Arc::new(Mutex::new(Some(HashMap::new()))),
            terminal_reports: inputs.terminal_reports,
            parent_reports: NoParent,
            origins: PhantomData,
        }
    }
}

impl<C, N, P, Bindings, Origins> ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn with_parent<Parent>(
        self,
        parent_reports: Parent,
    ) -> ApplicationCapabilities<C, N, Parent, Bindings, Origins> {
        ApplicationCapabilities {
            actor_spaces: self.actor_spaces,
            allocations: self.allocations,
            address: self.address,
            control: self.control,
            timers: self.timers,
            observations: self.observations,
            next_child_route: self.next_child_route,
            child_bindings: self.child_bindings,
            activation_tasks: self.activation_tasks,
            exact_observations: self.exact_observations,
            terminal_reports: self.terminal_reports,
            parent_reports,
            origins: PhantomData,
        }
    }

    fn inject_control_event<Control, Path>(&self, control: Control)
    where
        C::Event: InjectEvent<Control, Path>,
    {
        let event = C::Event::inject_at(control);
        match self.control.send(event) {
            Ok(()) => {}
            Err(ControlClosed(_)) => {
                unreachable!("the actor control lane remains live while its effects commit")
            }
        }
    }
}

impl<C, N, P, Bindings, Origins> RetireCapabilities
    for ApplicationCapabilities<C, N, P, Bindings, Origins>
where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    C::Event: Send + 'static,
    BehaviorMessage<C>: Send,
    Bindings: RetireChildTasks + Send,
    BindingTerminal<Bindings>: Send,
    Self: Send,
{
    type Event = C::Event;
    type Descendants = (Vec<BindingTerminal<Bindings>>, Bindings::Failures);

    async fn next_local_event(&mut self) -> Result<Self::Event, JoinError> {
        tokio::select! {
            biased;
            termination_event = self.observations.next() => Ok(termination_event),
            task = self.activation_tasks.as_mut().expect("live activation tasks remain installed").next_event() => task,
        }
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.timers.next_deadline()
    }

    fn pop_due(&mut self, now: Instant) -> Option<Self::Event> {
        self.timers.pop_due(now)
    }

    async fn receive_retirement(
        capabilities: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
    ) {
        let Some(owner) = capabilities.as_mut() else {
            return;
        };
        match (&owner.activation_tasks, received.as_ref()) {
            (Some(_), Some(_)) | (None, None) => return,
            (None, Some(_)) => {}
            (Some(_), None) => {
                let failures = Bindings::retirement_failures();
                let activation_tasks = owner
                    .activation_tasks
                    .take()
                    .expect("the original activation task owner was just observed");
                *received = Some(CapabilityRetirement {
                    activation_tasks,
                    descendants: (Vec::new(), failures),
                    terminal_report: None,
                    retirement_failures: Vec::new(),
                });
            }
        }
        let cancellations = {
            let mut observations = owner
                .exact_observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            observations
                .take()
                .into_iter()
                .flat_map(HashMap::into_values)
                .map(|(_, cancel)| cancel)
                .collect::<Vec<_>>()
        };
        for cancellation in cancellations {
            match cancellation.send(()) {
                Ok(()) | Err(()) => {}
            }
        }
        let retirement = received
            .as_mut()
            .expect("acquired capability lanes remain outside retirement");
        Bindings::receive_retirement(
            &mut owner.child_bindings,
            &mut retirement.descendants.0,
            &mut retirement.descendants.1,
        )
        .await;
        if owner.child_bindings.is_some() {
            return;
        }
        // The report transaction has already installed its original send result
        // before its existing invariant panic. Capture that native fact once;
        // retirement_complete below never resends or reconstructs the sender.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            owner
                .terminal_reports
                .receive_retirement(&mut retirement.terminal_report);
        })) {
            retirement.retirement_failures.push(payload);
        }
        if !owner.terminal_reports.retirement_complete() {
            return;
        }
        // All original output lanes are acquired before this explicit disposal.
        // A user destructor may destroy its own consumed values; retain the
        // original native cause alongside the actual completed cleanup outputs.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(capabilities.take()))) {
            retirement.retirement_failures.push(payload);
        }
    }

    #[cfg(test)]
    async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
        let mut capabilities = Some(self);
        let mut received = None;
        Self::receive_retirement(&mut capabilities, &mut received).await;
        assert!(
            capabilities.is_none(),
            "capability retirement retains incomplete original owners"
        );
        received.expect("completed retirement acquires the exact original capability lanes")
    }
}

/// Commits one complete action value for the concrete local actor.
///
/// Product traversal and concrete runtime services remain behind this private
/// seam. It is not an application extension API.
pub(crate) trait CommitActions<B: BehaviorSettlements<Ph = Never>> {
    type Retired;

    fn commit(
        &mut self,
        interpretation: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) -> impl Future<Output = ()> + Send;

    fn offer_next(
        &mut self,
        source: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) -> impl Future<Output = ()> + Send;

    fn next_local_event(&mut self) -> impl Future<Output = Result<B::Event, JoinError>> + Send;

    fn next_deadline(&mut self) -> Option<Instant> {
        None
    }

    fn pop_due(&mut self, _now: Instant) -> Option<B::Event> {
        None
    }

    /// Receive actual capability/task/child custody outside the disposable retirement work.
    fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<B::Event, Self::Retired>>,
    ) -> impl Future<Output = ()> + Send
    where
        Self: Sized + Send,
        B::Event: Send,
        Self::Retired: Send;
}

/// Affine retirement of actor-local runtime capability tasks.
pub(crate) trait RetireCapabilities {
    type Event;
    type Descendants;

    fn next_local_event(
        &mut self,
    ) -> impl core::future::Future<Output = Result<Self::Event, JoinError>> + Send;

    fn next_deadline(&mut self) -> Option<Instant>;

    fn pop_due(&mut self, now: Instant) -> Option<Self::Event>;

    /// Original capabilities and every acquired retirement lane outlive this attempt.
    fn receive_retirement(
        capabilities: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
    ) -> impl core::future::Future<Output = ()> + Send
    where
        Self: Sized + Send,
        Self::Event: Send,
        Self::Descendants: Send;

    #[cfg(test)]
    fn retire(
        self,
    ) -> impl core::future::Future<Output = CapabilityRetirement<Self::Event, Self::Descendants>> + Send;
}

impl<Capabilities> ActionInterpreter<Capabilities> {
    pub(crate) const fn new(capabilities: Capabilities) -> Self {
        Self {
            capabilities: Some(capabilities),
        }
    }
}

impl<B, Actor, Spaces, Parent, Bindings, Origins> CommitActions<B>
    for ActionInterpreter<ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>>
where
    Actor: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    B: BehaviorSettlements<
            Ph = Never,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        >,
    B::InterpretationCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::InterpretationCustody: Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    BehaviorAddr<B>: Send,
    B::Birth: InterpretCreations<
            BehaviorAddr<B>,
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            behavior::Here,
        >,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody,
        > + Send,
    <B::Sends as SendSettlements>::Settlements: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = <B::Sends as SendSettlements>::SourceCustody,
        > + Send,
    B::Sends: InterpretSends<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            behavior::Here,
        > + Send,
    ActionSettlementOf<B>: SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
            Custody = B::SourceCustody,
        > + Send,
    ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>:
        RetireCapabilities<Event = B::Event> + TerminalReportTransaction + Send,
{
    type Retired = <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants;

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        let become_ = match progress.as_ref() {
            Some(InterpretationProgress::Original(actions)) => &actions.become_,
            Some(InterpretationProgress::Interpreting((_, become_))) => become_,
            Some(InterpretationProgress::Completed(_)) | None => return,
        };
        let terminal_disposition = match become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match *never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        ActionsOf::<B>::interpret::<_, B::Event, behavior::Here>(progress, self.capabilities_mut())
            .await;
        self.capabilities_mut()
            .finish_terminal_reports(terminal_disposition);
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
        >>::prepare_source(progress);
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<
                ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
                B::Event,
            >>::offer_next_to_source(custody, self.capabilities_mut())
            .await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<
            ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins>,
            B::Event,
        >>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        self.capabilities_mut().next_local_event().await
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.capabilities_mut().next_deadline()
    }

    fn pop_due(&mut self, now: Instant) -> Option<B::Event> {
        self.capabilities_mut().pop_due(now)
    }

    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<CapabilityRetirement<B::Event, <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants>>,
    ) where
        B::Event: Send,
        <ApplicationCapabilities<Actor, Spaces, Parent, Bindings, Origins> as RetireCapabilities>::Descendants: Send,
{
        let Some(owner) = interpreter.as_mut() else {
            return;
        };
        RetireCapabilities::receive_retirement(&mut owner.capabilities, received).await;
        if owner.capabilities.is_none() && received.is_some() {
            drop(interpreter.take());
        }
    }
}

impl<Capabilities> ActionInterpreter<Capabilities> {
    /// Loan the exact existing interpreter capabilities to owning-crate law probes.
    pub(crate) fn capabilities_mut(&mut self) -> &mut Capabilities {
        self.capabilities
            .as_mut()
            .expect("live original capabilities remain installed")
    }
}

type BindingTerminal<Bindings> = <Bindings as RetireChildTasks>::Root;

pub(crate) struct ApplicationCapabilityInputs<C, N>
where
    C: Behavior,
{
    pub(crate) address: MailAddr,
    pub(crate) actor_spaces: Arc<N>,
    pub(crate) allocations: ApplicationAddresses,
    pub(crate) control: ControlSender<C::Event>,
    pub(crate) timers: LocalTimers<C::Event>,
    pub(crate) observations: TerminationObservations<MailAddr, C::Event>,
    pub(crate) terminal_reports: LocalTerminalReports,
}

pub(crate) struct ApplicationCapabilities<
    C,
    N,
    P = NoParent,
    Bindings = NoChildBindings,
    Origins = StructuralOrigins<C>,
> where
    C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    actor_spaces: Arc<N>,
    allocations: ApplicationAddresses,
    address: MailAddr,
    control: ControlSender<C::Event>,
    timers: LocalTimers<C::Event>,
    observations: TerminationObservations<MailAddr, C::Event>,
    next_child_route: u64,
    child_bindings: Option<Bindings>,
    activation_tasks: Option<ActivationTasks<C::Event>>,
    #[expect(
        clippy::type_complexity,
        reason = "Keep admission ownership, exact identity, and revocation in the same existing map."
    )]
    exact_observations:
        Arc<Mutex<Option<HashMap<ObservationId, (Arc<ObservationId>, oneshot::Sender<()>)>>>>,
    terminal_reports: LocalTerminalReports,
    parent_reports: P,
    origins: PhantomData<fn() -> Origins>,
}

pub(crate) struct NoParent;

/// Exact actor-local capability output transferred into environment retirement.
pub(crate) struct CapabilityRetirement<E, Descendants> {
    pub(crate) activation_tasks: ActivationTasks<E>,
    pub(crate) descendants: Descendants,
    pub(crate) terminal_report: Option<Result<(), Termination<MailAddr>>>,
    pub(crate) retirement_failures: Vec<Box<dyn Any + Send>>,
}

pub(crate) type ActionSettlementOf<B> = <B as BehaviorSettlements>::Settlements;

pub(crate) type InterpretedActionSettlement<B> = ActionSettlement<
    <<B as behavior::Behavior>::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements,
    <<B as behavior::Behavior>::Sends as SendSettlements>::Settlements,
    Never,
>;

/// Actor-local capability product paired with the emitting actor's address.
pub(crate) struct ActionInterpreter<Capabilities> {
    capabilities: Option<Capabilities>,
}

#[cfg(test)]
mod atomic_interpretation_contract {
    use core::convert::Infallible;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, Creations, Delivery,
        EstablishedDelivery, EstablishedRecipient, EventIngress, EventLayer, Here, InjectEvent,
        Inside, InterpretItem, ItemSettlement, LogicalDeliveryReason, MessageProtocol, Never,
        NoBirths, NoSends, Recipient, SendLayer, SourceAdmission, Step, User, UserEvent,
    };
    use behavior_actors::atomic::{
        AssignWorker, Assignment, BeginActivation, CustomerDelivery, DiagnosticAction,
        ImmediateActivation, InitializeWorker, ProxyOperation, StableProxy,
    };
    use behavior_actors::{
        CancelObservation, EstablishedObservation, EstablishedTerminationMonitor,
        InterpretEstablishedObservation, ObservationId, ObservationRejection, ObserveEstablished,
        ScheduleAt, ShutdownRequested, StopOnShutdown, TerminationMonitorError, TimerElapsed,
        TimerGeneration, TimerId,
    };
    use bombay_engine::{Completion, Driver};
    use communication::{Config, Received, mailbox_channel};
    use tokio::sync::oneshot;

    use crate::actor;
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ActorInterface;
    use crate::local::children::ChildBindings;
    use crate::local::children::NoChildBindings;
    use crate::local::children::StructuralOrigins;
    use crate::local::effects::CommitActions;
    use crate::local::effects::observation::EstablishedObservationInterpreter;
    use crate::local::effects::observation::TerminationObservations;
    use crate::local::effects::reports::LocalTerminalReports;
    use crate::local::effects::timers::LocalTimers;
    use crate::local::effects::{ActionInterpreter, RetireCapabilities};
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs};
    use crate::local::endpoint::ActorRef;
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::ActivationTasks;
    use crate::local::ingress::{Admission, AdmissionClosure, StandardIngress};
    use crate::observe::{self, pair};
    use crate::terminal::{ActorRetirement, LocalOutcome};
    use crate::topology::Hosts;
    use crate::{ActorExecutionOutcome, ActorSpace};

    struct Worker;

    #[actor(message = Never)]
    impl Worker {}

    type Proxy = StableProxy<Worker, ImmediateActivation>;

    struct ProxyParent;

    #[actor(
        message = Never,
        births = { service: Proxy },
        creation_settlements = retain_for_retirement,
    )]
    impl ProxyParent {}

    type LedgerProtocol = MessageProtocol<MailAddr, Vec<u64>>;

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete two-generation delivery and retirement trace together."
    )]
    async fn application_delivery_and_direct_hosts_preserve_exact_claim_generations() {
        let address = MailAddr(701);
        let origin = MailAddr(709);
        let hosts = ActorSpace::<LedgerProtocol>::new();
        let application_hosts = Arc::new(hosts.clone());
        let (control, source_owner, source_mailbox, source_receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let mut capabilities = ApplicationCapabilities::<SourceActor, _>::new_with_bindings(
            ApplicationCapabilityInputs {
                address: origin,
                actor_spaces: Arc::clone(&application_hosts),
                allocations: ApplicationAddresses::new(),
                control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(terminal_sender),
            },
            NoChildBindings::default(),
        );
        let absent = vec![11, 13];
        let absent_allocation = absent.as_ptr();
        let mut delivery = Some(Delivery::new(Recipient::global(address), absent));
        let mut rejected = None;
        <_ as InterpretItem<Delivery<LedgerProtocol>, SourceControlEvent, Here>>::interpret_item(
            &mut capabilities,
            &mut delivery,
            &mut rejected,
        )
        .await;
        let rejected = rejected.expect("original logical delivery receiving result");
        let ItemSettlement::Rejected { item, reason } = rejected else {
            panic!("an unclaimed logical address must return its whole delivery")
        };
        assert_eq!(reason, LogicalDeliveryReason::UnknownAddress);
        assert_eq!(item.to.address(), address);
        assert_eq!(item.message, [11, 13]);
        assert_eq!(item.message.as_ptr(), absent_allocation);
        let direct_absent = hosts.space().resolve(&address);
        assert!(direct_absent.is_none());

        let (old_control, old_owner, old_mailbox, mut old_receiver) =
            mailbox_channel::<Never, User<MailAddr, Vec<u64>>>(Config::new(1));
        let old_admission = Arc::new(Admission::new(old_owner));
        let (old_publisher, old_observation) = pair();
        let old_actor = ActorRef::<LedgerProtocol>::external(
            address,
            old_mailbox,
            Arc::downgrade(&old_admission),
            old_observation,
        );
        let old_claim = hosts.try_claim(address, old_actor).expect("old claim");
        let old_generation = old_claim.registration_id();
        let captured_application = application_hosts
            .space()
            .resolve(&address)
            .map(|actor| actor.as_ref().clone())
            .expect("application old endpoint");
        let captured_direct = hosts
            .space()
            .resolve(&address)
            .expect("direct old endpoint")
            .as_ref()
            .clone();
        let filled = captured_direct.send_from(origin, vec![17]).await;
        assert!(filled.is_ok());
        let second_fill = captured_direct.send_from(origin, vec![18]).await;
        assert!(second_fill.is_ok());
        let pending_values = vec![19, 23];
        let pending_allocation = pending_values.as_ptr();
        let (new_control, new_owner, new_mailbox, mut new_receiver) =
            mailbox_channel::<Never, User<MailAddr, Vec<u64>>>(Config::new(1));
        let new_admission = Arc::new(Admission::new(new_owner));
        let (new_publisher, new_observation) = pair();
        let new_actor = ActorRef::<LedgerProtocol>::external(
            address,
            new_mailbox,
            Arc::downgrade(&new_admission),
            new_observation,
        );
        let new_claim;
        {
            let mut delivery = Some(Delivery::new(Recipient::global(address), pending_values));
            let mut received_delivery = None;
            {
                let mut pending_delivery = pin!(<_ as InterpretItem<
                    Delivery<LedgerProtocol>,
                    SourceControlEvent,
                    Here,
                >>::interpret_item(
                    &mut capabilities,
                    &mut delivery,
                    &mut received_delivery
                ));
                let mut context = Context::from_waker(Waker::noop());
                let pending = pending_delivery.as_mut().poll(&mut context);
                assert!(matches!(pending, Poll::Pending));
                old_claim.release();
                let closed = old_admission.close();
                assert!(matches!(closed, AdmissionClosure::Closed));
                new_claim = hosts
                    .try_claim(address, new_actor)
                    .expect("fresh same-address claim");
                assert_ne!(old_generation, new_claim.registration_id());
                let first = old_receiver.recv().await;
                let Some(Received::User(first)) = first else {
                    panic!("old first delivery")
                };
                assert_eq!(first, User::new(origin, vec![17]));
                let second = old_receiver.recv().await;
                let Some(Received::User(second)) = second else {
                    panic!("old second delivery")
                };
                assert_eq!(second, User::new(origin, vec![18]));
                pending_delivery.await;
            }
            let settled = received_delivery.expect("the original admitted delivery receipt");
            assert!(matches!(settled, ItemSettlement::Accepted(())));
            let admitted = old_receiver.recv().await;
            let Some(Received::User(admitted)) = admitted else {
                panic!("preclose admitted delivery")
            };
            assert_eq!(admitted.from, origin);
            assert_eq!(admitted.message, [19, 23]);
            assert_eq!(admitted.message.as_ptr(), pending_allocation);
        }
        for captured in [captured_application, captured_direct] {
            let values = vec![29, 31];
            let allocation = values.as_ptr();
            let failed = captured.send_from(origin, values).await;
            let returned = failed
                .expect_err("captured old endpoint stays closed")
                .into_message();
            assert_eq!(returned, [29, 31]);
            assert_eq!(returned.as_ptr(), allocation);
        }
        let fresh_direct = hosts
            .space()
            .resolve(&address)
            .expect("fresh endpoint")
            .as_ref()
            .clone();
        let values = vec![37, 41];
        let allocation = values.as_ptr();
        let delivered = fresh_direct.send_from(origin, values).await;
        assert!(delivered.is_ok());
        let fresh = new_receiver.recv().await;
        let Some(Received::User(fresh)) = fresh else {
            panic!("fresh direct delivery")
        };
        assert_eq!(fresh.from, origin);
        assert_eq!(fresh.message, [37, 41]);
        assert_eq!(fresh.message.as_ptr(), allocation);
        let new_closed = new_admission.close();
        assert!(matches!(new_closed, AdmissionClosure::Closed));
        let values = vec![43, 47];
        let allocation = values.as_ptr();
        let mut delivery = Some(Delivery::new(Recipient::global(address), values));
        let mut rejected = None;
        <_ as InterpretItem<Delivery<LedgerProtocol>, SourceControlEvent, Here>>::interpret_item(
            &mut capabilities,
            &mut delivery,
            &mut rejected,
        )
        .await;
        let rejected = rejected.expect("original closed delivery receiving result");
        let ItemSettlement::Rejected { item, reason } = rejected else {
            panic!("a claimed closed endpoint must return its whole delivery")
        };
        assert_eq!(reason, LogicalDeliveryReason::ClosedRecipient);
        assert_eq!(item.to.address(), address);
        assert_eq!(item.message, [43, 47]);
        assert_eq!(item.message.as_ptr(), allocation);
        let new_terminal = new_receiver.recv().await;
        assert!(matches!(new_terminal, Some(Received::UserLaneClosed)));
        drop(new_control);
        let new_exhausted = new_receiver.recv().await;
        assert!(new_exhausted.is_none());
        let terminal = old_receiver.recv().await;
        assert!(matches!(terminal, Some(Received::UserLaneClosed)));
        drop(old_control);
        let exhausted = old_receiver.recv().await;
        assert!(exhausted.is_none());
        new_claim.release();
        drop((
            old_publisher,
            new_publisher,
            new_admission,
            source_owner,
            source_mailbox,
            source_receiver,
            terminal_receiver,
        ));
    }

    struct SourceActor;

    struct SourceOwner;

    type ObservationProtocol = MessageProtocol<MailAddr, Never>;
    type MessageObservationProtocol = MessageProtocol<MailAddr, u64>;

    enum SourceControlEvent {
        SourceInput(u64),
        Observation(EstablishedObservation<ObservationProtocol>),
        MessageObservation(EstablishedObservation<MessageObservationProtocol>),
        Timer(TimerElapsed),
    }

    impl UserEvent for SourceControlEvent {
        type Addr = MailAddr;
        type Message = Never;

        fn user(_: Self::Addr, message: Self::Message) -> Self {
            match message {}
        }

        fn into_user(self) -> Result<User<Self::Addr, Self::Message>, Self> {
            Err(self)
        }
    }

    impl EventIngress<SourceOwner, u64> for SourceControlEvent {
        fn ingress(input: u64) -> Self {
            Self::SourceInput(input)
        }
    }

    // This resource belongs to application conversion, outside every fold.
    // Only the dedicated conversion-cut controller installs it.
    static COMPLETION_CONVERSION: Mutex<Option<(oneshot::Sender<Instant>, mpsc::Receiver<()>)>> =
        Mutex::new(None);

    // Runtime test synchronization only, outside every Behavior fold.
    // The same owner gates generic EventLayer and concrete report conversion.
    pub(super) fn hold_acquired_completion(id: ObservationId, at: Instant) {
        if id != ObservationId(71) {
            return;
        }
        let permission = COMPLETION_CONVERSION.lock().unwrap().take();
        if let Some((entered, continue_conversion)) = permission {
            match entered.send(at) {
                Ok(()) | Err(_) => {}
            }
            match continue_conversion.recv() {
                Ok(()) | Err(_) => {}
            }
        }
    }

    impl InjectEvent<EstablishedObservation<ObservationProtocol>, Here> for SourceControlEvent {
        fn inject_at(report: EstablishedObservation<ObservationProtocol>) -> Self {
            Self::Observation(report)
        }
    }

    impl InjectEvent<EstablishedObservation<MessageObservationProtocol>, Inside<Here>>
        for SourceControlEvent
    {
        fn inject_at(report: EstablishedObservation<MessageObservationProtocol>) -> Self {
            Self::MessageObservation(report)
        }
    }

    impl InjectEvent<TimerElapsed, Here> for SourceControlEvent {
        fn inject_at(elapsed: TimerElapsed) -> Self {
            Self::Timer(elapsed)
        }
    }

    impl Behavior for SourceActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = SourceControlEvent;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            match event {
                SourceControlEvent::SourceInput(_)
                | SourceControlEvent::Observation(_)
                | SourceControlEvent::MessageObservation(_)
                | SourceControlEvent::Timer(_) => Ok(Actions::cont()),
            }
        }
    }

    #[tokio::test]
    async fn source_admission_places_the_exact_input_on_the_actor_control_lane() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let mut source_input = Some(47);
        let mut admitted = None;
        SourceAdmission::<SourceControlEvent, SourceOwner, u64>::admit_source(
            &mut capabilities,
            &mut source_input,
            &mut admitted,
        )
        .await;
        assert_eq!(source_input, None);
        assert_eq!(admitted, Some(Ok(())));

        let mut next_event = pin!(receiver.recv());
        let mut context = Context::from_waker(Waker::noop());
        let admitted_control = next_event.as_mut().poll(&mut context);
        assert!(matches!(
            admitted_control,
            Poll::Ready(Some(Received::Control(SourceControlEvent::SourceInput(47))))
        ));
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(terminal_receiver);
    }

    #[tokio::test]
    async fn exact_observation_start_and_cancel_publish_distinct_control_events() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let external = interface
            .external::<ObservationProtocol>()
            .expect("the observed external actor is established");
        let endpoint = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let observation = ObservationId(41);
        let request = ObserveEstablished::new(observation, external.recipient());

        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(request, endpoint);
        let authority = {
            let mut started_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let started = started_event.as_mut().poll(&mut context);
            assert!(matches!(
                &started,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Started { authority }
                )))) if authority.relationship().id() == observation
            ));
            let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::Started { authority },
            )))) = started
            else {
                panic!("the started control event transfers its registered observation authority");
            };
            authority
        };
        let original_relationship = authority.relationship().clone();
        let registered = {
            let observations = capabilities.exact_observations.lock().unwrap();
            observations
                .as_ref()
                .expect("live observation registrations remain installed")
                .get(&observation)
                .expect("the started control event follows exact registration")
                .0
                .clone()
        };

        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(authority));
        {
            let mut cancelled_event = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            let cancelled = cancelled_event.as_mut().poll(&mut context);
            assert!(matches!(
                cancelled,
                Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
                    EstablishedObservation::Cancelled { relationship }
                )))) if relationship.id() == observation && relationship == original_relationship
            ));
        }
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        assert!(Arc::ptr_eq(&registered, original_relationship.identity()));
        assert!(returned.is_empty());
        assert!(failures.is_empty());
        let (descendants, ()) = retirement.descendants;
        assert_eq!(descendants, []);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        drop(external);
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(terminal_receiver);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_observation_cancel_wins_before_completion_and_rejects_stale_reused_id() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let old = interface
            .external::<ObservationProtocol>()
            .expect("original target");
        let endpoint = old.recipient().interpret(&mut ExtractLocalEndpoint);
        let id = ObservationId(41);
        let original = ObserveEstablished::new(id, old.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(original, endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority },
        ))) = receiver.recv().await
        else {
            panic!("Started transfers the original committed grant first");
        };
        let original_relationship = authority.relationship().clone();
        let registered = {
            let current = capabilities.exact_observations.lock().unwrap();
            current
                .as_ref()
                .expect("live owner")
                .get(&id)
                .expect("committed before Started")
                .0
                .clone()
        };
        // Completion wins this first relationship while its affine grant stays
        // outside the runtime. This makes a genuinely stale in-flight cancel.
        drop(old);
        let original_stopped = receiver.recv().await;
        let replacement = interface
            .external::<ObservationProtocol>()
            .expect("replacement target");
        let replacement_endpoint = replacement.recipient().interpret(&mut ExtractLocalEndpoint);
        let new_request = ObserveEstablished::new(id, replacement.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(new_request, replacement_endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: replacement_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("numeric reuse has a fresh accepted identity");
        };
        let replacement_relationship = replacement_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(authority));
        // Collect the complete reply even when the exact-member omission
        // mutant accepts this stale grant. No custody oracle runs yet.
        let stale_response = receiver.recv().await;
        let replacement_after_stale = {
            let current = capabilities.exact_observations.lock().unwrap();
            current.as_ref().and_then(|observations| {
                observations.get(&id).map(|(identity, _)| identity.clone())
            })
        };
        // Both real cancellation interpretations synchronously return one
        // reply, including NotObserved in the stale-accepted counterfactual.
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(replacement_authority));
        let replacement_response = receiver.recv().await;
        drop(replacement);
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        let replay = {
            let mut next = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            next.as_mut().poll(&mut context)
        };
        // The target is released and every observation task has joined before
        // the exact-member omission can fail any authoritative oracle.
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert!(Arc::ptr_eq(&registered, original_relationship.identity()));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship,
                outcome,
                at,
            },
        ))) = original_stopped
        else {
            panic!("actual captured future preserves original termination");
        };
        assert_eq!(relationship, original_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_ne!(replacement_relationship, original_relationship);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::CancelRejected { request, reason },
        ))) = stale_response
        else {
            panic!("stale exact grant returns whole rejected request");
        };
        assert_eq!(reason, ObservationRejection::NotObserved);
        assert_eq!(request.relationship(), &original_relationship);
        let Some(replacement_after_stale) = replacement_after_stale else {
            panic!("stale request cannot remove replacement membership");
        };
        assert!(Arc::ptr_eq(
            &replacement_after_stale,
            replacement_relationship.identity()
        ));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = replacement_response
        else {
            panic!("only the fresh grant cancels the replacement");
        };
        assert_eq!(cancelled, replacement_relationship);
        assert!(replay.is_pending());
        drop((
            request,
            original_relationship,
            relationship,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_observation_duplicate_rejects_whole_original_and_consumers_remain_independent() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual target");
        let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        for id in [ObservationId(51), ObservationId(52)] {
            let request = ObserveEstablished::new(id, target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint.clone());
        }
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority: first },
        ))) = receiver.recv().await
        else {
            panic!("first accepted grant");
        };
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority: second },
        ))) = receiver.recv().await
        else {
            panic!("independent accepted grant");
        };
        let first_relationship = first.relationship().clone();
        let second_relationship = second.relationship().clone();
        let duplicate = ObserveEstablished::new(ObservationId(51), target.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(duplicate, endpoint.clone());
        // A duplicate-guard omission returns a whole additional Started.
        // Retain it until all affected members/tasks have completed.
        let duplicate_response = receiver.recv().await;
        let original_terminal = endpoint.termination();
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .cancel(CancelObservation::new(first));
        let cancellation_response = receiver.recv().await;
        drop(target);
        let actual = original_terminal.await;
        // Do not await one predicted report count: a guard omission admits an
        // additional member while replacing the old revocation. Join the
        // actual task owner first; every admitted completion is now available.
        let tasks = capabilities
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (returned, failures) = tasks.settle().await;
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (late, late_failures) = retirement.activation_tasks.settle().await;
        let mut completed_reports = Vec::new();
        let mut context = Context::from_waker(Waker::noop());
        let replay = loop {
            let acquired = {
                let mut next = pin!(receiver.recv());
                next.as_mut().poll(&mut context)
            };
            match acquired {
                Poll::Ready(Some(report)) => completed_reports.push(report),
                remaining => break remaining,
            }
        };
        // Every wait is joined and the exact target is released before the
        // duplicate-omission or cancellation/reaction oracle can fail.
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert_eq!(late.len(), 0);
        assert_eq!(late_failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert_eq!(first_relationship.id(), ObservationId(51));
        assert_eq!(second_relationship.id(), ObservationId(52));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::ObserveRejected { request, reason },
        ))) = duplicate_response
        else {
            panic!("duplicate preserves the whole original rejected request");
        };
        assert_eq!(reason, ObservationRejection::IdAlreadyBound);
        let (id, recipient) = request.into_inputs();
        let returned_endpoint = recipient.interpret(&mut ExtractLocalEndpoint);
        let recovered = returned_endpoint.termination().await;
        assert_eq!(id, ObservationId(51));
        assert_eq!(returned_endpoint.address(), endpoint.address());
        assert_eq!(actual, recovered);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = cancellation_response
        else {
            panic!("only first original consumer cancelled");
        };
        assert_eq!(cancelled, first_relationship);
        assert_eq!(completed_reports.len(), 1);
        let mut completed = completed_reports.into_iter();
        let Received::Control(SourceControlEvent::Observation(EstablishedObservation::Stopped {
            relationship,
            outcome,
            at,
        })) = completed
            .next()
            .expect("whole independent consumer completion")
        else {
            panic!("independent terminal report");
        };
        let extra = completed.next();
        assert!(extra.is_none());
        assert_eq!(relationship, second_relationship);
        assert_eq!(outcome, actual);
        assert!(replay.is_pending());
        drop((
            second,
            relationship,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    #[tokio::test]
    async fn two_protocols_share_one_membership_owner_and_keep_distinct_static_acknowledgements() {
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let first = interface
            .external::<ObservationProtocol>()
            .expect("first concrete protocol");
        let second = interface
            .external::<MessageObservationProtocol>()
            .expect("second concrete protocol");
        let first_endpoint = first.recipient().interpret(&mut ExtractLocalEndpoint);
        let second_endpoint = second.recipient().interpret(&mut ExtractLocalEndpoint);
        let first_request = ObserveEstablished::new(ObservationId(81), first.recipient());
        let second_request = ObserveEstablished::new(ObservationId(82), second.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(first_request, first_endpoint);
        EstablishedObservationInterpreter::<_, Inside<Here>>::new(&mut capabilities)
            .observe(second_request, second_endpoint);
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: first_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("first static acknowledgement");
        };
        let Some(Received::Control(SourceControlEvent::MessageObservation(
            EstablishedObservation::Started {
                authority: second_authority,
            },
        ))) = receiver.recv().await
        else {
            panic!("second static acknowledgement");
        };
        let first_relationship = first_authority.relationship().clone();
        let second_relationship = second_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Inside<Here>>::new(&mut capabilities)
            .cancel(CancelObservation::new(second_authority));
        let Some(Received::Control(SourceControlEvent::MessageObservation(
            EstablishedObservation::Cancelled {
                relationship: cancelled,
            },
        ))) = receiver.recv().await
        else {
            panic!("cancellation returns only through second acknowledgement lane");
        };
        drop((first, second));
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship: stopped,
                outcome,
                at,
            },
        ))) = receiver.recv().await
        else {
            panic!("first protocol remains independently observed");
        };
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (returned, failures) = retirement.activation_tasks.settle().await;
        let replay = {
            let mut next = pin!(receiver.recv());
            let mut context = Context::from_waker(Waker::noop());
            next.as_mut().poll(&mut context)
        };
        assert_eq!(cancelled, second_relationship);
        assert_eq!(stopped, first_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(returned.len(), 0);
        assert_eq!(failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert!(replay.is_pending());
        drop((
            first_authority,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));
    }

    fn stop_source_after_terminal(
        _: &mut SourceActor,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<MailAddr, Never, Vec<Never>, NoBirths> {
        // Explicit test policy: an actually delivered terminal stops the actor.
        drop(report);
        Actions::stop()
    }

    enum CompletionAdmission {
        BeforeCancellation,
        AfterCancellation,
        AfterRetirement,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn exact_completion_decision_preserves_both_control_orders_and_retired_admission() {
        type Root = StopOnShutdown<EstablishedTerminationMonitor<SourceActor, ObservationProtocol>>;
        for admission in [
            CompletionAdmission::BeforeCancellation,
            CompletionAdmission::AfterCancellation,
            CompletionAdmission::AfterRetirement,
        ] {
            let (control, mailbox_owner, mailbox_ref, mut receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (terminal_sender, terminal_receiver) = oneshot::channel();
            let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
                address: MailAddr::APPLICATION_ROOT,
                actor_spaces: Arc::new(()),
                allocations: ApplicationAddresses::new(),
                control,
                timers: LocalTimers::new(),
                observations: TerminationObservations::new(),
                terminal_reports: LocalTerminalReports::new(terminal_sender),
            };
            let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                inputs,
                NoChildBindings::default(),
            );
            let interface = ActorInterface::new((), ApplicationAddresses::new());
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual target");
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let request = ObserveEstablished::new(ObservationId(71), target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::Started { authority },
            ))) = receiver.recv().await
            else {
                panic!("committed original grant");
            };
            let relationship = authority.relationship().clone();
            let (entered, decision) = oneshot::channel();
            let (continue_conversion, permission) = mpsc::channel();
            *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
            drop(target);
            // Ready has already removed the exact member, but conversion has
            // not returned E and no control admission has yet been attempted.
            let conversion_entered = decision.await;
            let mut reports = Vec::new();
            let retirement = match admission {
                CompletionAdmission::BeforeCancellation => {
                    let released = continue_conversion.send(());
                    let stopped = receiver.recv().await;
                    reports.push(stopped);
                    EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                        .cancel(CancelObservation::new(authority));
                    reports.push(receiver.recv().await);
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    retirement
                }
                CompletionAdmission::AfterCancellation => {
                    EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                        .cancel(CancelObservation::new(authority));
                    reports.push(receiver.recv().await);
                    let released = continue_conversion.send(());
                    reports.push(receiver.recv().await);
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    retirement
                }
                CompletionAdmission::AfterRetirement => {
                    let retirement = RetireCapabilities::retire(capabilities).await;
                    let released = continue_conversion.send(());
                    match released {
                        Ok(()) | Err(_) => {}
                    }
                    drop(authority);
                    retirement
                }
            };
            let (returned, failures) = retirement.activation_tasks.settle().await;
            let replay = {
                let mut remaining = pin!(receiver.recv());
                let mut context = Context::from_waker(Waker::noop());
                remaining.as_mut().poll(&mut context)
            };
            // Both gates and every owned task are settled before any oracle.
            let original_at = conversion_entered
                .expect("actual completion timestamp acquired outside conversion");
            assert_eq!(failures.len(), 0);
            assert!(retirement.terminal_report.is_none());
            assert!(retirement.retirement_failures.is_empty());
            let (descendants, ()) = &retirement.descendants;
            assert_eq!(descendants.len(), 0);
            assert!(replay.is_pending());
            match admission {
                CompletionAdmission::BeforeCancellation
                | CompletionAdmission::AfterCancellation => {
                    assert_eq!(returned.len(), 0);
                    assert_eq!(reports.len(), 2);
                    let mut stopped_count = 0;
                    let mut rejected_count = 0;
                    for (position, report) in reports.into_iter().enumerate() {
                        match report {
                            Some(Received::Control(SourceControlEvent::Observation(
                                EstablishedObservation::Stopped {
                                    relationship: original,
                                    outcome,
                                    at,
                                },
                            ))) => {
                                let expected_position = match admission {
                                    CompletionAdmission::BeforeCancellation => 0,
                                    CompletionAdmission::AfterCancellation => 1,
                                    CompletionAdmission::AfterRetirement => unreachable!(),
                                };
                                assert_eq!(position, expected_position);
                                assert_eq!(original, relationship);
                                assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
                                stopped_count += 1;
                                assert_eq!(at, original_at);
                            }
                            Some(Received::Control(SourceControlEvent::Observation(
                                EstablishedObservation::CancelRejected { request, reason },
                            ))) => {
                                assert_eq!(request.relationship(), &relationship);
                                assert_eq!(reason, ObservationRejection::NotObserved);
                                rejected_count += 1;
                                drop(request);
                            }
                            _ => panic!(
                                "completion-winning has only complete Stopped and whole rejected cancellation"
                            ),
                        }
                    }
                    assert_eq!((stopped_count, rejected_count), (1, 1));
                }
                CompletionAdmission::AfterRetirement => {
                    assert_eq!(reports.len(), 0);
                    assert_eq!(returned.len(), 1);
                    let mut originals = returned.into_iter();
                    let SourceControlEvent::Observation(EstablishedObservation::Stopped {
                        relationship: original,
                        outcome,
                        at,
                    }) = originals.next().expect("original converted event")
                    else {
                        panic!("retired admission retains whole original E");
                    };
                    let extra = originals.next();
                    assert!(extra.is_none());
                    assert_eq!(original, relationship);
                    assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
                    assert_eq!(at, original_at);
                }
            }
            drop((mailbox_owner, mailbox_ref, terminal_receiver));
        }

        // Advanced host only: this Consumer is deliberately independent of
        // LocalEnvironment's same-actor lifetime. Preserve genuine acquired E.
        let (control, mailbox_owner, mailbox_ref, mut receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual advanced target");
        let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        let request = ObserveEstablished::new(ObservationId(71), target.recipient());
        EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
            .observe(request, endpoint);
        let started = receiver.recv().await;
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started { authority },
        ))) = started
        else {
            panic!("actual committed advanced registration");
        };
        let relationship = authority.relationship().clone();
        let (entered, decision) = oneshot::channel();
        let (continue_conversion, permission) = mpsc::channel();
        *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
        drop(target);
        let conversion_entered = decision.await;
        drop(receiver);
        let released = continue_conversion.send(());
        // Join before retiring the membership owner. Its value remains Some,
        // so the task reaches actual ControlClosed, rather than the None cut.
        let acquired_tasks = capabilities
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (returned, failures) = acquired_tasks.settle().await;
        let retirement = RetireCapabilities::retire(capabilities).await;
        let (remaining, retirement_failures) = retirement.activation_tasks.settle().await;
        // All resources and the blocked conversion are settled before oracles.
        assert!(released.is_ok());
        assert_eq!(failures.len(), 0);
        assert_eq!(remaining.len(), 0);
        assert_eq!(retirement_failures.len(), 0);
        assert!(retirement.terminal_report.is_none());
        assert!(retirement.retirement_failures.is_empty());
        let (descendants, ()) = &retirement.descendants;
        assert_eq!(descendants.len(), 0);
        assert_eq!(returned.len(), 1);
        let mut returned = returned.into_iter();
        let original = returned.next();
        let extra = returned.next();
        let Some(SourceControlEvent::Observation(EstablishedObservation::Stopped {
            relationship: retained,
            outcome,
            at,
        })) = original
        else {
            panic!("closed advanced control returns the whole original notification");
        };
        let original_at = conversion_entered.expect("actual completion decision timestamp");
        assert!(extra.is_none());
        assert_eq!(retained, relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(at, original_at);
        drop((
            authority,
            retained,
            at,
            mailbox_owner,
            mailbox_ref,
            terminal_receiver,
        ));

        // Standard lifetime: real typed Shutdown retires the actor after its
        // target completion decision, before its notification can be admitted.
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("actual standard target");
        let request = ObserveEstablished::new(ObservationId(71), target.recipient());
        let root = StopOnShutdown::new(EstablishedTerminationMonitor::established(
            SourceActor,
            request,
            stop_source_after_terminal,
        ));
        let (_publisher, termination) = observe::pair();
        let (_cancel, cancellation) = oneshot::channel();
        let (reports, report_received) = oneshot::channel();
        let (published, publication) = oneshot::channel();
        let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
            MailAddr(173),
            ActorSpace::new(),
            Config::new(2),
            termination,
            cancellation,
            move |control, timers, observations| {
                ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                    ApplicationCapabilityInputs {
                        address: MailAddr(173),
                        actor_spaces: Arc::new(()),
                        allocations: ApplicationAddresses::new(),
                        control,
                        timers,
                        observations,
                        terminal_reports: LocalTerminalReports::new(reports),
                    },
                    NoChildBindings::default(),
                ))
            },
        );
        let shutdown = environment.control();
        let environment = environment.publish_with(move |actor| match published.send(actor) {
            Ok(()) | Err(_) => {}
        });
        let joined = tokio::spawn(Driver::new(root, environment).run());
        let actor = publication.await.expect("actual standard root publication");
        // Started was synchronously queued by the initialization request before
        // publication. Same control FIFO puts that genuine grant before Shutdown.
        let (entered, decision) = oneshot::channel();
        let (continue_conversion, permission) = mpsc::channel();
        *COMPLETION_CONVERSION.lock().unwrap() = Some((entered, permission));
        drop(target);
        let conversion_entered = decision.await;
        let admitted = shutdown.send(EventLayer::Owned(ShutdownRequested));
        let driver_retirement = joined.await;
        // Local retirement transferred task ownership without joining this gate.
        let released = continue_conversion.send(());
        let driver_retirement = driver_retirement.expect("real standard Driver joined");
        let driver_retirement = match driver_retirement {
            Ok(retirement) => retirement,
            Err(driver) => {
                drop(driver);
                panic!("original actor must complete its Driver retirement");
            }
        };
        let outcome: LocalOutcome<Root, (Vec<Never>, ())> = driver_retirement.into();
        let ActorExecutionOutcome::Completed {
            behavior,
            residual,
            additional_failures,
            completion,
        } = outcome
        else {
            panic!("real standard shutdown returns its completed outcome");
        };
        let residual = residual.settle_activation_tasks().await;
        let retirement =
            ActorRetirement::<Root, Never, ()>::from_local(ActorExecutionOutcome::Completed {
                behavior,
                residual,
                additional_failures,
                completion,
            });
        // Every gate, task and source owner settled before custody assertions.
        assert!(admitted.is_ok());
        assert!(released.is_ok());
        let ActorRetirement::Completed {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            additional_failures: retirement_additional_failures,
            child_failures: (),
            behavior,
            settlements,
            control,
            user,
            descendants,
            capability_failures,
            unread_owner_cancellation,
            completion,
        } = retirement
        else {
            panic!("real completed actor retirement owns returned notification");
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());
        assert!(retirement_additional_failures.is_empty());

        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(settlements.len(), 1);
        for settlement in settlements {
            assert_eq!(settlement.sends.owned, NoSends);
            assert_eq!(settlement.sends.inner.owned.len(), 0);
            assert_eq!(settlement.sends.inner.inner.len(), 0);
            assert_eq!(settlement.creations.len(), 0);
            assert!(matches!(settlement.become_, Step::Stop(_)));
        }
        assert_eq!(user.len(), 0);
        assert_eq!(descendants.len(), 0);
        assert_eq!(capability_failures.len(), 0);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(control.len(), 1);
        let mut control = control.into_iter();
        let retained = control.next();
        let extra = control.next();
        let Some(EventLayer::Inner(EventLayer::Owned(EstablishedObservation::Stopped {
            relationship,
            outcome,
            at,
        }))) = retained
        else {
            panic!("the completed product preserves whole original Stopped");
        };
        let mut monitor = behavior.into_inner();
        let cancellation = monitor
            .take_cancellation()
            .expect("genuine Started grant reached the actor before shutdown");
        let original_at = conversion_entered.expect("actual original acquired timestamp");
        assert!(extra.is_none());
        assert_eq!(cancellation.relationship(), &relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(at, original_at);
        let (source, observation) = monitor.into_parts();
        drop((
            source,
            observation,
            cancellation,
            relationship,
            actor,
            shutdown,
            report_received,
        ));
    }

    #[test]
    fn application_timer_deadlines_and_elapsed_events_cross_both_ports() {
        let (control, mailbox_owner, mailbox_ref, receiver) =
            mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(1));
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let inputs = ApplicationCapabilityInputs::<SourceActor, ()> {
            address: MailAddr::APPLICATION_ROOT,
            actor_spaces: Arc::new(()),
            allocations: ApplicationAddresses::new(),
            control,
            timers: LocalTimers::new(),
            observations: TerminationObservations::new(),
            terminal_reports: LocalTerminalReports::new(terminal_sender),
        };
        let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
            inputs,
            NoChildBindings::default(),
        );
        let first_deadline = Instant::now() + Duration::from_secs(1);
        let first_id = TimerId(7);
        let first_generation = TimerGeneration(2);
        capabilities
            .timers
            .schedule_at::<Here>(ScheduleAt::new(first_id, first_generation, first_deadline))
            .expect("the first timer is scheduled");
        assert_eq!(
            RetireCapabilities::next_deadline(&mut capabilities),
            Some(first_deadline)
        );
        let first_elapsed = RetireCapabilities::pop_due(&mut capabilities, first_deadline);
        assert!(matches!(
            first_elapsed,
            Some(SourceControlEvent::Timer(TimerElapsed { id, generation }))
                if id == first_id && generation == first_generation
        ));

        let second_deadline = first_deadline + Duration::from_secs(1);
        let second_id = TimerId(8);
        let second_generation = TimerGeneration(3);
        capabilities
            .timers
            .schedule_at::<Here>(ScheduleAt::new(
                second_id,
                second_generation,
                second_deadline,
            ))
            .expect("the second timer is scheduled");
        let mut interpreter = ActionInterpreter::new(capabilities);
        assert_eq!(
            <ActionInterpreter<_> as CommitActions<SourceActor>>::next_deadline(&mut interpreter),
            Some(second_deadline)
        );
        let second_elapsed = <ActionInterpreter<_> as CommitActions<SourceActor>>::pop_due(
            &mut interpreter,
            second_deadline,
        );
        assert!(matches!(
            second_elapsed,
            Some(SourceControlEvent::Timer(TimerElapsed { id, generation }))
                if id == second_id && generation == second_generation
        ));
        drop(mailbox_owner);
        drop(mailbox_ref);
        drop(receiver);
        drop(terminal_receiver);
    }

    #[test]
    fn local_application_interprets_exact_worker_assignment() {
        type WorkerProtocol = MessageProtocol<crate::MailAddr, Assignment<u64>>;

        fn require<Capabilities>()
        where
            Capabilities:
                InterpretItem<AssignWorker<WorkerProtocol, u64>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_complete_customer_delivery() {
        type CustomerProtocol = MessageProtocol<crate::MailAddr, u64>;
        type Spaces = ActorSpace<CustomerProtocol>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<CustomerDelivery<CustomerProtocol>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, Spaces, super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_routed_and_terminal_diagnostics() {
        type DiagnosticProtocol = MessageProtocol<crate::MailAddr, u64>;
        type Spaces = ActorSpace<DiagnosticProtocol>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    DiagnosticAction<Recipient<DiagnosticProtocol>, u64>,
                    <Worker as Behavior>::Event,
                    Here,
                > + InterpretItem<
                    DiagnosticAction<EstablishedRecipient<DiagnosticProtocol>, u64>,
                    <Worker as Behavior>::Event,
                    Here,
                > + InterpretItem<DiagnosticAction<Infallible, u64>, <Worker as Behavior>::Event, Here>,
        {
        }

        require::<ApplicationCapabilities<Worker, Spaces, super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_worker_initialization() {
        type Proxy = StableProxy<Worker, ImmediateActivation>;

        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    InitializeWorker<Worker, ImmediateActivation>,
                    <Proxy as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<ApplicationCapabilities<Proxy, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_worker_activation() {
        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    BeginActivation<Worker, ImmediateActivation>,
                    <Proxy as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<ApplicationCapabilities<Proxy, (), super::NoParent, NoChildBindings>>();
    }

    #[test]
    fn local_application_interprets_exact_proxy_operation() {
        fn require<Capabilities>()
        where
            Capabilities: InterpretItem<
                    ProxyOperation<Here, Worker, ImmediateActivation>,
                    <ProxyParent as Behavior>::Event,
                    Here,
                >,
        {
        }

        require::<
            ApplicationCapabilities<
                ProxyParent,
                (),
                super::NoParent,
                ChildBindings<ProxyParent, Never, StructuralOrigins<ProxyParent>>,
            >,
        >();
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn foreign_observer_grant_cannot_cancel_same_id_and_shared_target_consumers_remain_independent()
     {
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservationProtocol>()
            .expect("one exact shared target");
        let mut observers = Vec::new();
        for address in [MailAddr(171), MailAddr(173)] {
            let (control, owner, mailbox, receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut capabilities = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address,
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let request = ObserveEstablished::new(ObservationId(171), target.recipient());
            EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities)
                .observe(request, endpoint);
            observers.push((capabilities, owner, mailbox, receiver, report_received));
        }
        let mut observers = observers.into_iter();
        let (mut first, first_owner, first_mailbox, mut first_receiver, first_reports) =
            observers.next().expect("first real owning interpreter");
        let (mut second, second_owner, second_mailbox, mut second_receiver, second_reports) =
            observers.next().expect("second real owning interpreter");
        let extra = observers.next();
        assert!(extra.is_none());
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: first_authority,
            },
        ))) = first_receiver.recv().await
        else {
            panic!("first committed grant");
        };
        let Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Started {
                authority: second_authority,
            },
        ))) = second_receiver.recv().await
        else {
            panic!("second committed grant");
        };
        let first_relationship = first_authority.relationship().clone();
        let second_relationship = second_authority.relationship().clone();
        EstablishedObservationInterpreter::<_, Here>::new(&mut second)
            .cancel(CancelObservation::new(first_authority));
        let foreign_response = second_receiver.recv().await;
        let (returned_rejection, unexpected_foreign) = match foreign_response {
            Some(Received::Control(SourceControlEvent::Observation(
                EstablishedObservation::CancelRejected {
                    request: returned,
                    reason,
                },
            ))) => {
                let returned_relationship = returned.relationship().clone();
                EstablishedObservationInterpreter::<_, Here>::new(&mut first).cancel(returned);
                (Some((returned_relationship, reason)), None)
            }
            report => (None, report),
        };
        drop(target);
        // Await the existing task owners while the live membership owner can
        // complete. In a numeric-only mutant either task may already have
        // been cancelled; both sets still settle without waiting for a report
        // that the mutant suppresses. No new observation task is introduced.
        let first_tasks = first
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let second_tasks = second
            .activation_tasks
            .replace(ActivationTasks::new())
            .expect("original activation tasks remain installed");
        let (first_returned, first_failures) = first_tasks.settle().await;
        let (second_returned, second_failures) = second_tasks.settle().await;
        let mut context = Context::from_waker(Waker::noop());
        let first_cancelled = {
            let mut first_event = pin!(first_receiver.recv());
            first_event.as_mut().poll(&mut context)
        };
        let second_stopped = {
            let mut second_event = pin!(second_receiver.recv());
            second_event.as_mut().poll(&mut context)
        };
        let first_retirement = RetireCapabilities::retire(first).await;
        let second_retirement = RetireCapabilities::retire(second).await;
        let (late_first, late_first_failures) = first_retirement.activation_tasks.settle().await;
        let (late_second, late_second_failures) = second_retirement.activation_tasks.settle().await;
        let mut context = Context::from_waker(Waker::noop());
        let first_replay = {
            let mut first_next = pin!(first_receiver.recv());
            first_next.as_mut().poll(&mut context)
        };
        let second_replay = {
            let mut second_next = pin!(second_receiver.recv());
            second_next.as_mut().poll(&mut context)
        };
        // Every source wait is joined before identity, rejection and replay oracles.
        assert_ne!(first_relationship, second_relationship);
        assert!(unexpected_foreign.is_none());
        let Some((returned_relationship, reason)) = returned_rejection else {
            panic!("foreign grant is returned whole");
        };
        assert_eq!(returned_relationship, first_relationship);
        assert_eq!(reason, ObservationRejection::NotObserved);
        let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Cancelled { relationship },
        )))) = first_cancelled
        else {
            panic!("returned original grant still cancels only its own member");
        };
        assert_eq!(relationship, first_relationship);
        let Poll::Ready(Some(Received::Control(SourceControlEvent::Observation(
            EstablishedObservation::Stopped {
                relationship,
                outcome,
                at,
            },
        )))) = second_stopped
        else {
            panic!("independent observer retains original shared-target completion");
        };
        assert_eq!(relationship, second_relationship);
        assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
        assert_eq!(first_returned.len(), 0);
        assert_eq!(second_returned.len(), 0);
        assert_eq!(first_failures.len(), 0);
        assert_eq!(second_failures.len(), 0);
        assert_eq!(late_first.len(), 0);
        assert_eq!(late_second.len(), 0);
        assert_eq!(late_first_failures.len(), 0);
        assert_eq!(late_second_failures.len(), 0);
        assert!(first_retirement.terminal_report.is_none());
        assert!(first_retirement.retirement_failures.is_empty());
        let (first_descendants, ()) = &first_retirement.descendants;
        assert_eq!(first_descendants.len(), 0);
        assert!(second_retirement.terminal_report.is_none());
        assert!(second_retirement.retirement_failures.is_empty());
        let (second_descendants, ()) = &second_retirement.descendants;
        assert_eq!(second_descendants.len(), 0);
        assert!(first_replay.is_pending());
        assert!(second_replay.is_pending());
        drop((
            second_authority,
            at,
            first_owner,
            first_mailbox,
            first_receiver,
            first_reports,
            second_owner,
            second_mailbox,
            second_receiver,
            second_reports,
        ));
    }

    type TerminalReply = MessageProtocol<MailAddr, EstablishedObservation<ObservationProtocol>>;

    struct TerminalPublisher {
        values: Vec<u64>,
        reply_to: EstablishedRecipient<TerminalReply>,
    }

    impl BehaviorBase for TerminalPublisher {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }

    impl Behavior for TerminalPublisher {
        type Protocol = MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<EstablishedDelivery<TerminalReply>>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    fn publish_terminal(
        actor: &mut TerminalPublisher,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<MailAddr, Never, Vec<EstablishedDelivery<TerminalReply>>, NoBirths> {
        Actions::new(
            vec![EstablishedDelivery::new(actor.reply_to.clone(), report)],
            Creations::empty(),
            Step::Continue,
        )
    }

    fn publish_terminal_inside_shutdown(
        actor: &mut StopOnShutdown<TerminalPublisher>,
        report: EstablishedObservation<ObservationProtocol>,
    ) -> Actions<
        MailAddr,
        Never,
        SendLayer<NoSends, Vec<EstablishedDelivery<TerminalReply>>>,
        NoBirths,
    > {
        Actions::new(
            SendLayer::new(
                NoSends,
                vec![EstablishedDelivery::new(
                    actor.base().reply_to.clone(),
                    report,
                )],
            ),
            Creations::empty(),
            Step::Continue,
        )
    }

    enum CompletionReportArrival {
        TerminalFirst,
        RejectionFirst,
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn outer_shutdown_driver_retirement_returns_whole_rejected_cancel_in_both_report_orders()
    {
        type Root =
            StopOnShutdown<EstablishedTerminationMonitor<TerminalPublisher, ObservationProtocol>>;
        for arrival in [
            CompletionReportArrival::TerminalFirst,
            CompletionReportArrival::RejectionFirst,
        ] {
            let interface = ActorInterface::new((), ApplicationAddresses::new());
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual observed target");
            let mut reply = interface
                .external::<TerminalReply>()
                .expect("actual terminal reply endpoint");
            let original_reply = reply.recipient();
            let original_reply_address = original_reply
                .clone()
                .interpret(&mut ExtractLocalEndpoint)
                .address();
            let values = vec![151, 157];
            let allocation = values.as_ptr() as usize;
            let publisher = TerminalPublisher {
                values,
                reply_to: original_reply.clone(),
            };
            let request = ObserveEstablished::new(ObservationId(151), target.recipient());
            let mut monitor =
                EstablishedTerminationMonitor::established(publisher, request, publish_terminal);
            let initial =
                behavior::initialize(&mut monitor).expect("pure complete initial actions");
            assert_eq!(initial.sends.owned.len(), 1);
            assert_eq!(initial.sends.inner.len(), 0);
            assert_eq!(initial.creates.len(), 0);
            assert_eq!(initial.become_, Step::Continue);
            let mut requests = initial.sends.owned.into_iter();
            let original = requests.next().expect("whole original request");
            let extra = requests.next();
            assert!(extra.is_none());
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let (control, owner, mailbox, mut receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut source = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr(151),
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            EstablishedObservationInterpreter::<_, Here>::new(&mut source)
                .observe(original, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(started))) =
                receiver.recv().await
            else {
                panic!("actual committed Started");
            };
            let actions =
                behavior::delegate_transition(&mut monitor, EventLayer::Owned(started)).unwrap();
            assert_eq!(actions.sends.owned.len(), 0);

            assert_eq!(actions.sends.inner.len(), 0);
            assert_eq!(actions.creates.len(), 0);
            assert_eq!(actions.become_, Step::Continue);
            let cancellation = monitor
                .take_cancellation()
                .expect("one affine original grant");
            let relationship = cancellation.relationship().clone();
            drop(target);
            let Some(Received::Control(SourceControlEvent::Observation(stopped))) =
                receiver.recv().await
            else {
                panic!("actual completion wins");
            };
            let EstablishedObservation::Stopped { at, .. } = &stopped else {
                panic!("complete original terminal report");
            };
            let original_at = *at;
            EstablishedObservationInterpreter::<_, Here>::new(&mut source).cancel(cancellation);
            let Some(Received::Control(SourceControlEvent::Observation(rejected))) =
                receiver.recv().await
            else {
                panic!("whole actual rejected cancellation");
            };
            let ordered = match arrival {
                CompletionReportArrival::TerminalFirst => [stopped, rejected],
                CompletionReportArrival::RejectionFirst => [rejected, stopped],
            };
            let mut rejected_reports = Vec::new();
            for report in ordered {
                let actions =
                    match behavior::delegate_transition(&mut monitor, EventLayer::Owned(report)) {
                        Ok(actions) => actions,
                        Err(error) => {
                            rejected_reports.push(error);
                            continue;
                        }
                    };
                assert_eq!(actions.sends.owned.len(), 0);

                assert_eq!(actions.creates.len(), 0);
                assert_eq!(actions.become_, Step::Continue);
                for delivery in actions.sends.inner {
                    let mut delivery = Some(delivery);
                    let mut settlement = None;
                    <ApplicationCapabilities<SourceActor, ()> as InterpretItem<
                        EstablishedDelivery<TerminalReply>,
                        SourceControlEvent,
                        Here,
                    >>::interpret_item(
                        &mut source, &mut delivery, &mut settlement
                    )
                    .await;
                    assert!(delivery.is_none());
                    assert!(matches!(
                        settlement,
                        Some(behavior::ItemSettlement::Accepted(()))
                    ));
                }
            }
            let source_retirement = RetireCapabilities::retire(source).await;
            let (returned_source, source_failures) =
                source_retirement.activation_tasks.settle().await;
            assert_eq!(returned_source.len(), 0);
            assert_eq!(source_failures.len(), 0);
            assert!(source_retirement.terminal_report.is_none());
            assert!(source_retirement.retirement_failures.is_empty());
            let (source_descendants, ()) = &source_retirement.descendants;
            assert_eq!(source_descendants.len(), 0);
            drop((owner, mailbox, receiver, report_received));
            let (root_publication, termination) = observe::pair();
            let (_cancel, cancellation) = oneshot::channel();
            let (reports, report_received) = oneshot::channel();
            let (actor_notice, publication) = oneshot::channel();
            let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
                MailAddr(161),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |control, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(161),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    ))
                },
            );
            let shutdown = environment.control();
            let environment =
                environment.publish_with(move |actor| match actor_notice.send(actor) {
                    Ok(()) | Err(_) => {}
                });
            let joined = tokio::spawn(Driver::new(StopOnShutdown::new(monitor), environment).run());
            let actor = publication.await.expect("actual root actor_notice");
            let admitted = shutdown.send(EventLayer::Owned(ShutdownRequested));
            let retirement = joined
                .await
                .expect("original wrapped behavior and environment join");
            let retirement = match retirement {
                Ok(retirement) => retirement,
                Err(driver) => {
                    drop(driver);
                    panic!("original actor must complete its Driver retirement");
                }
            };
            let residual = retirement.residual.settle_activation_tasks().await;
            // This direct Driver has no launch termination-publishing step.
            // Release the original unused publication owner only after joins.
            drop(root_publication);
            let (publisher, target) = retirement.behavior.into_inner().into_parts();
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("original callback delivery is available after every root/source join");
            };
            // Transfer the same whole typed payload through the recipient held
            // by the actual retired Behavior, outside every Behavior fold.
            let original_from = delivered.from;
            let retained_endpoint = publisher
                .reply_to
                .clone()
                .interpret(&mut ExtractLocalEndpoint);
            let retained_reply_address = retained_endpoint.address();
            let retained_delivery = retained_endpoint
                .send_from(original_from, delivered.message)
                .await;
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            drop(reply);
            // Source tasks and the root have already joined. A missing reply
            // is a finite Pending observation, never a hanging receive.
            // All operation/task/root cleanup precedes every final custody oracle.
            assert!(admitted.is_ok());
            for error in &rejected_reports {
                let TerminationMonitorError::UnexpectedReport { report, .. } = error else {
                    panic!(
                        "the original rejection suppresses the terminal callback, not an inner fold"
                    );
                };
                let EstablishedObservation::Stopped {
                    relationship: original,
                    outcome,
                    at,
                } = report
                else {
                    panic!(
                        "the original source Stopped remains whole even when its reaction is suppressed"
                    );
                };
                assert_eq!(original, &relationship);
                assert_eq!(*outcome, Ok(behavior_actors::Exit::Normal));
                assert_eq!(*at, original_at);
            }
            assert_eq!(rejected_reports.len(), 0);
            assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
            let LocalResidual::Retired {
                interpretation: retirement_interpretation,
                source: retirement_source,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                terminal_report: retirement_terminal_report,
                retirement_failures: retirement_native_failures,
                settlements,
                ingress,
                activation_tasks,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } = residual
            else {
                panic!("whole local retirement");
            };
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());

            let (remaining_controls, remaining_failures) = activation_tasks.settle().await;
            assert_eq!(settlements.len(), 1);
            for settlement in settlements {
                assert_eq!(settlement.sends.owned, NoSends);
                assert_eq!(settlement.sends.inner.owned.len(), 0);
                assert_eq!(settlement.sends.inner.inner.len(), 0);
                assert_eq!(settlement.creations.len(), 0);
                assert!(matches!(settlement.become_, Step::Stop(_)));
            }
            assert_eq!(ingress.control.len(), 0);
            assert_eq!(ingress.user.len(), 0);
            assert_eq!(remaining_controls.len(), 0);
            assert_eq!(remaining_failures.len(), 0);
            let (descendants, ()) = descendants;
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());

            assert_eq!(publisher.values.as_ptr() as usize, allocation);
            assert_eq!(publisher.values, [151, 157]);
            assert_eq!(retained_reply_address, original_reply_address);
            assert!(retained_delivery.is_ok());
            let Ok((original, reason)) = target.into_rejected_cancel() else {
                panic!("actual retired target returns the original affine grant");
            };
            assert_eq!(original.relationship(), &relationship);
            assert_eq!(original.id(), ObservationId(151));
            assert_eq!(reason, ObservationRejection::NotObserved);
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("one complete actual terminal reply remains available after joins");
            };
            assert_eq!(delivered.from, MailAddr(151));
            assert_eq!(delivered.from, original_from);
            let EstablishedObservation::Stopped {
                relationship: delivered_relationship,
                outcome,
                at,
            } = delivered.message
            else {
                panic!("whole original terminal payload");
            };
            assert_eq!(delivered_relationship, relationship);
            assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
            assert_eq!(at, original_at);
            drop((original, actor, shutdown, publisher, report_received));
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the complete deterministic controller, joined cleanup, and whole typed custody oracles together."
    )]
    async fn inner_shutdown_driver_retirement_returns_whole_rejected_cancel_in_both_report_orders()
    {
        type Root =
            EstablishedTerminationMonitor<StopOnShutdown<TerminalPublisher>, ObservationProtocol>;
        for arrival in [
            CompletionReportArrival::TerminalFirst,
            CompletionReportArrival::RejectionFirst,
        ] {
            let interface = ActorInterface::new((), ApplicationAddresses::new());
            let target = interface
                .external::<ObservationProtocol>()
                .expect("actual observed target");
            let mut reply = interface
                .external::<TerminalReply>()
                .expect("actual terminal reply endpoint");
            let original_reply = reply.recipient();
            let original_reply_address = original_reply
                .clone()
                .interpret(&mut ExtractLocalEndpoint)
                .address();
            let values = vec![151, 157];
            let allocation = values.as_ptr() as usize;
            let publisher = TerminalPublisher {
                values,
                reply_to: original_reply.clone(),
            };
            let request = ObserveEstablished::new(ObservationId(151), target.recipient());
            let mut monitor = EstablishedTerminationMonitor::established(
                StopOnShutdown::new(publisher),
                request,
                publish_terminal_inside_shutdown,
            );
            let initial =
                behavior::initialize(&mut monitor).expect("pure complete initial actions");
            assert_eq!(initial.sends.owned.len(), 1);
            assert_eq!(initial.sends.inner.owned, NoSends);
            assert_eq!(initial.sends.inner.inner.len(), 0);
            assert_eq!(initial.creates.len(), 0);
            assert_eq!(initial.become_, Step::Continue);
            let mut requests = initial.sends.owned.into_iter();
            let original = requests.next().expect("whole original request");
            let extra = requests.next();
            assert!(extra.is_none());
            let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
            let (control, owner, mailbox, mut receiver) =
                mailbox_channel::<SourceControlEvent, User<MailAddr, Never>>(Config::new(2));
            let (reports, report_received) = oneshot::channel();
            let mut source = ApplicationCapabilities::<SourceActor, ()>::new_with_bindings(
                ApplicationCapabilityInputs {
                    address: MailAddr(151),
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    control,
                    timers: LocalTimers::new(),
                    observations: TerminationObservations::new(),
                    terminal_reports: LocalTerminalReports::new(reports),
                },
                NoChildBindings::default(),
            );
            EstablishedObservationInterpreter::<_, Here>::new(&mut source)
                .observe(original, endpoint);
            let Some(Received::Control(SourceControlEvent::Observation(started))) =
                receiver.recv().await
            else {
                panic!("actual committed Started");
            };
            let actions =
                behavior::delegate_transition(&mut monitor, EventLayer::Owned(started)).unwrap();
            assert_eq!(actions.sends.owned.len(), 0);
            assert_eq!(actions.sends.inner.owned, NoSends);
            assert_eq!(actions.sends.inner.inner.len(), 0);
            assert_eq!(actions.creates.len(), 0);
            assert_eq!(actions.become_, Step::Continue);
            let cancellation = monitor
                .take_cancellation()
                .expect("one affine original grant");
            let relationship = cancellation.relationship().clone();
            drop(target);
            let Some(Received::Control(SourceControlEvent::Observation(stopped))) =
                receiver.recv().await
            else {
                panic!("actual completion wins");
            };
            let EstablishedObservation::Stopped { at, .. } = &stopped else {
                panic!("complete original terminal report");
            };
            let original_at = *at;
            EstablishedObservationInterpreter::<_, Here>::new(&mut source).cancel(cancellation);
            let Some(Received::Control(SourceControlEvent::Observation(rejected))) =
                receiver.recv().await
            else {
                panic!("whole actual rejected cancellation");
            };
            let ordered = match arrival {
                CompletionReportArrival::TerminalFirst => [stopped, rejected],
                CompletionReportArrival::RejectionFirst => [rejected, stopped],
            };
            let mut rejected_reports = Vec::new();
            for report in ordered {
                let actions =
                    match behavior::delegate_transition(&mut monitor, EventLayer::Owned(report)) {
                        Ok(actions) => actions,
                        Err(error) => {
                            rejected_reports.push(error);
                            continue;
                        }
                    };
                assert_eq!(actions.sends.owned.len(), 0);
                assert_eq!(actions.sends.inner.owned, NoSends);
                assert_eq!(actions.creates.len(), 0);
                assert_eq!(actions.become_, Step::Continue);
                for delivery in actions.sends.inner.inner {
                    let mut delivery = Some(delivery);
                    let mut settlement = None;
                    <ApplicationCapabilities<SourceActor, ()> as InterpretItem<
                        EstablishedDelivery<TerminalReply>,
                        SourceControlEvent,
                        Here,
                    >>::interpret_item(
                        &mut source, &mut delivery, &mut settlement
                    )
                    .await;
                    assert!(delivery.is_none());
                    assert!(matches!(
                        settlement,
                        Some(behavior::ItemSettlement::Accepted(()))
                    ));
                }
            }
            let source_retirement = RetireCapabilities::retire(source).await;
            let (returned_source, source_failures) =
                source_retirement.activation_tasks.settle().await;
            assert_eq!(returned_source.len(), 0);
            assert_eq!(source_failures.len(), 0);
            assert!(source_retirement.terminal_report.is_none());
            assert!(source_retirement.retirement_failures.is_empty());
            let (source_descendants, ()) = &source_retirement.descendants;
            assert_eq!(source_descendants.len(), 0);
            drop((owner, mailbox, receiver, report_received));
            let (root_publication, termination) = observe::pair();
            let (_cancel, cancellation) = oneshot::channel();
            let (reports, report_received) = oneshot::channel();
            let (actor_notice, publication) = oneshot::channel();
            let environment = LocalEnvironment::<Root, _, StandardIngress>::prepare(
                MailAddr(161),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |control, timers, observations| {
                    ActionInterpreter::new(ApplicationCapabilities::<Root, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(161),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    ))
                },
            );
            let shutdown = environment.control();
            let environment =
                environment.publish_with(move |actor| match actor_notice.send(actor) {
                    Ok(()) | Err(_) => {}
                });
            let joined = tokio::spawn(Driver::new(monitor, environment).run());
            let actor = publication.await.expect("actual root actor_notice");
            let admitted = shutdown.send(EventLayer::Inner(EventLayer::Owned(ShutdownRequested)));
            let retirement = joined
                .await
                .expect("original wrapped behavior and environment join");
            let retirement = match retirement {
                Ok(retirement) => retirement,
                Err(driver) => {
                    drop(driver);
                    panic!("original actor must complete its Driver retirement");
                }
            };
            let residual = retirement.residual.settle_activation_tasks().await;
            // This direct Driver has no launch termination-publishing step.
            // Release the original unused publication owner only after joins.
            drop(root_publication);
            let (shutdown_behavior, target) = retirement.behavior.into_parts();
            let publisher = shutdown_behavior.into_inner();
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("original callback delivery is available after every root/source join");
            };
            // Transfer the same whole typed payload through the recipient held
            // by the actual retired Behavior, outside every Behavior fold.
            let original_from = delivered.from;
            let retained_endpoint = publisher
                .reply_to
                .clone()
                .interpret(&mut ExtractLocalEndpoint);
            let retained_reply_address = retained_endpoint.address();
            let retained_delivery = retained_endpoint
                .send_from(original_from, delivered.message)
                .await;
            let delivered = {
                let mut callback_delivery = pin!(reply.receive());
                let mut context = Context::from_waker(Waker::noop());
                callback_delivery.as_mut().poll(&mut context)
            };
            drop(reply);
            // Source tasks and the root have already joined. A missing reply
            // is a finite Pending observation, never a hanging receive.
            // All operation/task/root cleanup precedes every final custody oracle.
            assert!(admitted.is_ok());
            for error in &rejected_reports {
                let TerminationMonitorError::UnexpectedReport { report, .. } = error else {
                    panic!(
                        "the original rejection suppresses the terminal callback, not an inner fold"
                    );
                };
                let EstablishedObservation::Stopped {
                    relationship: original,
                    outcome,
                    at,
                } = report
                else {
                    panic!(
                        "the original source Stopped remains whole even when its reaction is suppressed"
                    );
                };
                assert_eq!(original, &relationship);
                assert_eq!(*outcome, Ok(behavior_actors::Exit::Normal));
                assert_eq!(*at, original_at);
            }
            assert_eq!(rejected_reports.len(), 0);
            assert!(matches!(retirement.disposition, Ok(Completion::Stopped)));
            let LocalResidual::Retired {
                interpretation: retirement_interpretation,
                source: retirement_source,
                received_interpretation: retirement_received_interpretation,
                received_source: retirement_received_source,
                source_index: retirement_source_index,
                acquired_ingress: retirement_acquired_ingress,
                terminal_report: retirement_terminal_report,
                retirement_failures: retirement_native_failures,
                settlements,
                ingress,
                activation_tasks,
                descendants,
                capability_failures,
                unread_owner_cancellation,
            } = residual
            else {
                panic!("whole local retirement");
            };
            assert!(retirement_interpretation.is_none());
            assert!(retirement_source.is_none());
            assert!(retirement_received_interpretation.is_none());
            assert!(retirement_received_source.is_none());
            assert!(retirement_source_index.is_none());
            assert!(retirement_acquired_ingress.is_none());
            assert!(retirement_terminal_report.is_none());
            assert!(retirement_native_failures.is_empty());

            let (remaining_controls, remaining_failures) = activation_tasks.settle().await;
            assert_eq!(settlements.len(), 1);
            for settlement in settlements {
                assert_eq!(settlement.sends.owned.len(), 0);
                assert_eq!(settlement.sends.inner.owned, NoSends);
                assert_eq!(settlement.sends.inner.inner.len(), 0);
                assert_eq!(settlement.creations.len(), 0);
                assert!(matches!(settlement.become_, Step::Stop(_)));
            }
            assert_eq!(ingress.control.len(), 0);
            assert_eq!(ingress.user.len(), 0);
            assert_eq!(remaining_controls.len(), 0);
            assert_eq!(remaining_failures.len(), 0);
            let (descendants, ()) = descendants;
            assert_eq!(descendants.len(), 0);
            assert_eq!(capability_failures.len(), 0);
            assert!(unread_owner_cancellation.is_none());

            assert_eq!(publisher.values.as_ptr() as usize, allocation);
            assert_eq!(publisher.values, [151, 157]);
            assert_eq!(retained_reply_address, original_reply_address);
            assert!(retained_delivery.is_ok());
            let Ok((original, reason)) = target.into_rejected_cancel() else {
                panic!("actual retired target returns the original affine grant");
            };
            assert_eq!(original.relationship(), &relationship);
            assert_eq!(original.id(), ObservationId(151));
            assert_eq!(reason, ObservationRejection::NotObserved);
            let Poll::Ready(Some(delivered)) = delivered else {
                panic!("one complete actual terminal reply remains available after joins");
            };
            assert_eq!(delivered.from, MailAddr(151));
            assert_eq!(delivered.from, original_from);
            let EstablishedObservation::Stopped {
                relationship: delivered_relationship,
                outcome,
                at,
            } = delivered.message
            else {
                panic!("whole original terminal payload");
            };
            assert_eq!(delivered_relationship, relationship);
            assert_eq!(outcome, Ok(behavior_actors::Exit::Normal));
            assert_eq!(at, original_at);
            drop((original, actor, shutdown, publisher, report_received));
        }
    }
}

#[cfg(test)]
mod live_capability_retirement {
    use std::sync::Arc;

    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, Here, InjectEvent, MessageProtocol, Never,
        NoBirths, NoSends, User, UserEvent,
    };
    use behavior_actors::{
        EstablishedObservation, InterpretEstablishedObservation, ObservationId, ObserveEstablished,
    };
    use bombay_engine::{Completion, Driver};
    use communication::Config;
    use tokio::sync::oneshot;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ActorInterface;
    use crate::launch::ActorSpace;
    use crate::local::children::NoChildBindings;
    use crate::local::effects::ActionInterpreter;
    use crate::local::effects::observation::EstablishedObservationInterpreter;
    use crate::local::effects::reports::LocalTerminalReports;
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs};
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::LocalRetirementRequest;
    use crate::local::ingress::StandardIngress;
    use crate::observe;

    type ObservedProtocol = MessageProtocol<MailAddr, Never>;

    enum CapabilityEvent {
        Observation(EstablishedObservation<ObservedProtocol>),
    }

    impl UserEvent for CapabilityEvent {
        type Addr = MailAddr;
        type Message = Never;

        fn user(_: MailAddr, message: Never) -> Self {
            match message {}
        }

        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Err(self)
        }
    }

    impl InjectEvent<EstablishedObservation<ObservedProtocol>, Here> for CapabilityEvent {
        fn inject_at(observation: EstablishedObservation<ObservedProtocol>) -> Self {
            match observation {
                EstablishedObservation::Stopped { .. } => panic!("observation conversion failed"),
                observation => Self::Observation(observation),
            }
        }
    }

    struct ObservingActor {
        values: Vec<u64>,
        observations: Vec<ObservationId>,
    }

    impl Behavior for ObservingActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = CapabilityEvent;
        type Sends = NoSends;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            let CapabilityEvent::Observation(observation) = event;
            match observation {
                EstablishedObservation::Started { authority } => {
                    self.observations.push(authority.relationship().id());
                }
                _ => panic!("only the successful start can reach this actor"),
            }
            Ok(Actions::cont())
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn acquired_capability_failure_preserves_available_actor_values() {
        let values = vec![77, 177];
        let allocation = values.as_ptr() as usize;
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let target = interface
            .external::<ObservedProtocol>()
            .expect("established target");
        let recipient = target.recipient();
        let endpoint = recipient.clone().interpret(&mut ExtractLocalEndpoint);
        let (publisher, observation) = observe::pair();
        let (request, cancellation) = oneshot::channel();
        let (reports, report_received) = oneshot::channel();
        let environment = LocalEnvironment::<ObservingActor, _, StandardIngress>::prepare(
            MailAddr(81),
            ActorSpace::new(),
            Config::new(2),
            observation,
            cancellation,
            move |control, timers, observations| {
                let mut capabilities =
                    ApplicationCapabilities::<ObservingActor, ()>::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr(81),
                            actor_spaces: Arc::new(()),
                            allocations: ApplicationAddresses::new(),
                            control,
                            timers,
                            observations,
                            terminal_reports: LocalTerminalReports::new(reports),
                        },
                        NoChildBindings::default(),
                    );
                EstablishedObservationInterpreter::<_, Here>::new(&mut capabilities).observe(
                    ObserveEstablished::new(ObservationId(1), recipient),
                    endpoint,
                );
                ActionInterpreter::new(capabilities)
            },
        );
        let joined = tokio::spawn(
            Driver::new(
                ObservingActor {
                    values,
                    observations: Vec::new(),
                },
                environment,
            )
            .run(),
        );
        drop(target);
        let retirement = joined.await.expect(
            "acquired capability failure must retire with available actor state instead of unwinding",
        );
        let retirement = match retirement {
            Ok(retirement) => retirement,
            Err(driver) => {
                drop(driver);
                panic!("original actor must complete its Driver retirement");
            }
        };
        assert_eq!(retirement.behavior.values, [77, 177]);
        assert_eq!(retirement.behavior.values.as_ptr() as usize, allocation);
        assert_eq!(retirement.behavior.observations, [ObservationId(1)]);
        let Ok(Completion::RetirementRequested(LocalRetirementRequest::CapabilityFailed(failure))) =
            retirement.disposition
        else {
            panic!("live acquisition owns the original capability task failure");
        };
        let acquired_task_id = failure.id();
        let residual = retirement.residual.settle_activation_tasks().await;
        let LocalResidual::Retired {
            interpretation: retirement_interpretation,
            source: retirement_source,
            received_interpretation: retirement_received_interpretation,
            received_source: retirement_received_source,
            source_index: retirement_source_index,
            acquired_ingress: retirement_acquired_ingress,
            terminal_report: retirement_terminal_report,
            retirement_failures: retirement_native_failures,
            settlements,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = residual
        else {
            panic!("the failed live actor retains its actual retired environment");
        };
        assert!(retirement_interpretation.is_none());
        assert!(retirement_source.is_none());
        assert!(retirement_received_interpretation.is_none());
        assert!(retirement_received_source.is_none());
        assert!(retirement_source_index.is_none());
        assert!(retirement_acquired_ingress.is_none());
        assert!(retirement_terminal_report.is_none());
        assert!(retirement_native_failures.is_empty());

        assert_eq!(settlements.len(), 0);
        assert!(ingress.control.is_empty());
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        let (descendants, ()) = descendants;
        assert_eq!(descendants.len(), 0);
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(failure.is_panic());
        assert_eq!(failure.id(), acquired_task_id);
        drop((
            retirement.behavior,
            failure,
            request,
            publisher,
            report_received,
        ));
    }
}

#[cfg(test)]
mod capability_task_retirement {
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::launch::{ActorSpace, spawn_local_execution};
    use crate::local::children::ChildBindings;
    use crate::local::children::StructuralOrigins;
    use crate::local::effects::CapabilityRetirement;
    use crate::local::effects::reports::TerminalReportTransaction;
    use crate::local::effects::{ActionInterpreter, RetireCapabilities};
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs, NoParent};
    use crate::local::ingress::StandardIngress;
    use crate::terminal::{ActorRetirement, ChildOrigin, ProjectTerminal};
    use crate::termination::TerminalReportDisposition;
    use crate::worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};
    use behavior::{
        ActionItem, ActionItemResult, Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorAddr,
        BehaviorBase, ChildCons, ChildCreationOutcome, ChildHead, ChildProduct, Children,
        ComposedEvent, CreateChild, CreationId, CreationRejection, CreationSequence, Creations,
        EndpointAddress, EstablishChild, EstablishedRecipient, EventIngress, Here,
        InitializationTurn, InjectEvent, Inside, InterpretItem, InterpreterRequests,
        ItemSettlement, MessageProtocol, Never, NoBirths, NoChildren, NoSends, Protocol,
        RetirementBirths, RoutedCreation, SendEffects, SendLayer, SourceActions, SourceAdmission,
        Step, Stopped, User, UserEvent,
    };
    use behavior_actors::ShutdownRequested;
    use behavior_actors::atomic::{
        ActivationPermit, ActivationPlan, BeginActivation, ImmediateActivation, OrderedRoles,
        PendingWorkerPreparation, PrepareWorkers, StartingWorkerPreparation, WorkerActivation,
        WorkerActivationGrant, WorkerPreparation, WorkerSource,
    };
    use bombay_engine::Completion;
    use communication::{Config, ControlClosed, ControlSender};
    use core::future::{Future, poll_fn};
    use core::ops::ControlFlow;
    use core::task::Poll;
    use std::any::Any;
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::sync::{Arc, Mutex, PoisonError, Weak};
    use std::time::Instant;
    use tokio::sync::Mutex as TaskCustody;
    use tokio::sync::oneshot;
    use tokio::task::{self, Id, JoinError};
    /// Factual identity of the original Tokio producer, before or after its first poll.
    #[derive(Debug, Eq, PartialEq)]
    enum WorkerCapabilityTask {
        Unpolled,
        Started(Id),
    }

    /// Exact source boundary of the original payload moved into Tokio's `JoinError`.
    #[derive(Debug, Eq, PartialEq)]
    enum WorkerCapabilityOperation {
        Construction,
        Execution,
        Disposal,
        Receiving,
    }

    /// Original native cause at one known task-owned boundary.
    struct WorkerCapabilityFailure {
        operation: WorkerCapabilityOperation,
        payload: Box<dyn Any + Send>,
    }

    impl WorkerCapabilityFailure {
        fn into_parts(self) -> (WorkerCapabilityOperation, Box<dyn Any + Send>) {
            (self.operation, self.payload)
        }
    }

    /// Only source phases whose concrete values remain owned by the runtime.
    enum WorkerPreparationPhase<Source, Role, Worker, Plan>
    where
        Source: WorkerSource<Role, Worker, Plan>,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
    {
        Starting(StartingWorkerPreparation<Source, Role, Worker, Plan>),
        Pending(PendingWorkerPreparation<Source, Role, Worker, Plan>),
        /// The completed report was actually moved into `Event::inject_at`.
        ReportTransferred,
    }

    /// Outside-task source phase, acquired Event and additional original failures.
    struct WorkerPreparationCustody<Source, Role, Worker, Plan, Event>
    where
        Source: WorkerSource<Role, Worker, Plan>,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
    {
        task: WorkerCapabilityTask,
        phase: Option<WorkerPreparationPhase<Source, Role, Worker, Plan>>,
        event: Option<Event>,
        failures: Vec<WorkerCapabilityFailure>,
        resumed_failure_origin: Option<WorkerCapabilityOperation>,
    }

    /// Preserve actual Ready reply across disposal of its borrowing future.
    /// The output remains caller-local until the source borrow has ended.
    async fn capability_work_reply<Work>(
        constructed: Result<Work, WorkerCapabilityFailure>,
    ) -> (
        Result<Work::Output, WorkerCapabilityFailure>,
        Option<WorkerCapabilityFailure>,
    )
    where
        Work: Future + Send,
    {
        let mut work = match constructed {
            Ok(work) => Some(Box::pin(work)),
            Err(failure) => return (Err(failure), None),
        };
        poll_fn(move |cx| {
            let polled = {
                let current = work.as_mut().expect("only pending work can be polled");
                catch_unwind(AssertUnwindSafe(|| current.as_mut().poll(cx)))
            };
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(reply)) => {
                    // reply is closure-local, not captured across a Pending.
                    // take removes the owning pinned future before wrapper Ready.
                    let disposal = catch_unwind(AssertUnwindSafe(|| drop(work.take())))
                        .err()
                        .map(|payload| WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Disposal,
                            payload,
                        });
                    Poll::Ready((Ok(reply), disposal))
                }
                Err(payload) => {
                    // Drop runs after catch returned, outside primary unwind.
                    let disposal = catch_unwind(AssertUnwindSafe(|| drop(work.take())))
                        .err()
                        .map(|payload| WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Disposal,
                            payload,
                        });
                    Poll::Ready((
                        Err(WorkerCapabilityFailure {
                            operation: WorkerCapabilityOperation::Execution,
                            payload,
                        }),
                        disposal,
                    ))
                }
            }
        })
        .await
    }

    /// Keep original source metadata outside the task without output lifetime bounds.
    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "Keep the original source/role/worker/plan/Event cell and its exhaustive borrowing work, acquired reply, disposal, phase advancement and native handoff in one owning transaction."
    )]
    async fn prepare_workers<Source, Role, Worker, Plan, Event, Path>(
        custody: &Arc<TaskCustody<WorkerPreparationCustody<Source, Role, Worker, Plan, Event>>>,
        control: &ControlSender<Event>,
    ) where
        Source: WorkerPreparationSource<Role, Worker, Plan>,
        Role: Send + Sync,
        Worker: Behavior + Send,
        Plan: ActivationPlan,
        Event: InjectEvent<WorkerPreparation<Source, Role, Worker, Plan>, Path>,
    {
        loop {
            let mut original = custody.lock().await;
            original.task = WorkerCapabilityTask::Started(task::id());
            let WorkerPreparationCustody {
                phase,
                event,
                failures,
                resumed_failure_origin,
                task: _,
            } = &mut *original;
            let completed = match phase
                .as_mut()
                .expect("original source phase stays installed during work")
            {
                WorkerPreparationPhase::Starting(request) => {
                    let (source, role) = request.source_and_role();
                    let constructed = catch_unwind(AssertUnwindSafe(move || {
                        let source = source;
                        source.prepare_first(role)
                    }))
                    .map_err(|payload| WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Construction,
                        payload,
                    });
                    let (reply, disposal) = capability_work_reply(constructed).await;
                    let reply = match reply {
                        Ok(reply) => Some(reply),
                        Err(failure) => {
                            failures.push(failure);
                            None
                        }
                    };
                    if let Some(failure) = disposal {
                        failures.push(failure);
                    }
                    match reply {
                        Some(reply) => {
                            let WorkerPreparationPhase::Starting(request) =
                                phase.take().expect("same original starting phase")
                            else {
                                unreachable!("polled the starting phase");
                            };
                            let next = match reply {
                                WorkerPreparationStart::Submitted(submission) => {
                                    request.accept(submission)
                                }
                                WorkerPreparationStart::WorkerRejected(reason) => {
                                    ControlFlow::Break(request.reject(reason))
                                }
                                WorkerPreparationStart::SourceRejected(reason) => {
                                    ControlFlow::Break(request.reject_source(reason))
                                }
                            };
                            match next {
                                ControlFlow::Continue(request) => {
                                    *phase = Some(WorkerPreparationPhase::Pending(request));
                                    None
                                }
                                ControlFlow::Break(report) => Some(report),
                            }
                        }
                        None => None,
                    }
                }
                WorkerPreparationPhase::Pending(request) => {
                    let (source, role) = request.source_and_role();
                    let constructed = catch_unwind(AssertUnwindSafe(move || {
                        let source = source;
                        source.prepare_next(role)
                    }))
                    .map_err(|payload| WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Construction,
                        payload,
                    });
                    let (reply, disposal) = capability_work_reply(constructed).await;
                    let reply = match reply {
                        Ok(reply) => Some(reply),
                        Err(failure) => {
                            failures.push(failure);
                            None
                        }
                    };
                    if let Some(failure) = disposal {
                        failures.push(failure);
                    }
                    match reply {
                        Some(reply) => {
                            let WorkerPreparationPhase::Pending(request) =
                                phase.take().expect("same original pending phase")
                            else {
                                unreachable!("polled the pending phase");
                            };
                            let next = match reply {
                                Ok(submission) => request.accept(submission),
                                Err(reason) => ControlFlow::Break(request.reject(reason)),
                            };
                            match next {
                                ControlFlow::Continue(request) => {
                                    *phase = Some(WorkerPreparationPhase::Pending(request));
                                    None
                                }
                                ControlFlow::Break(report) => Some(report),
                            }
                        }
                        None => None,
                    }
                }
                WorkerPreparationPhase::ReportTransferred => {
                    unreachable!("one producer owns its report transfer")
                }
            };
            if let Some(report) = completed {
                // The original report is deliberately consumed by this actual
                // receiving conversion. No intact source is claimed afterward.
                *phase = Some(WorkerPreparationPhase::ReportTransferred);
                match catch_unwind(AssertUnwindSafe(|| Event::inject_at(report))) {
                    Ok(acquired) => *event = Some(acquired),
                    Err(payload) => failures.push(WorkerCapabilityFailure {
                        operation: WorkerCapabilityOperation::Receiving,
                        payload,
                    }),
                }
            }
            if !failures.is_empty() {
                // Every other actual failure and the surviving phase/Event stay in
                // the actor-owned cell. Tokio receives the exact earliest payload.
                let (origin, first) = failures.remove(0).into_parts();
                *resumed_failure_origin = Some(origin);
                resume_unwind(first);
            }
            match phase {
                Some(WorkerPreparationPhase::ReportTransferred) => {
                    let acquired = event
                        .take()
                        .expect("normal receiving acquired its exact Event");
                    match control.send(acquired) {
                        Ok(()) => {}
                        Err(ControlClosed(acquired)) => *event = Some(acquired),
                    }
                    return;
                }
                Some(WorkerPreparationPhase::Starting(_) | WorkerPreparationPhase::Pending(_)) => {}
                None => unreachable!("pure phase transition reinstalls original custody"),
            }
        }
    }

    /// Original grant and endpoint remain outside the Plan's owning future.
    struct WorkerActivationCustody<W, P, Event>
    where
        W: Behavior,
        BehaviorAddr<W>: EndpointAddress,
        P: ActivationPlan,
    {
        task: WorkerCapabilityTask,
        permit: ActivationPermit<W>,
        grant: WorkerActivationGrant<W, P>,
        event: Option<Event>,
        failures: Vec<WorkerCapabilityFailure>,
        resumed_failure_origin: Option<WorkerCapabilityOperation>,
    }

    async fn activate_worker<W, P, Event, Path>(
        plan: P,
        custody: &Arc<TaskCustody<WorkerActivationCustody<W, P, Event>>>,
        control: &ControlSender<Event>,
    ) where
        W: Behavior,
        BehaviorAddr<W>: EndpointAddress,
        P: ActivationPlan,
        Event: InjectEvent<WorkerActivation<W, P>, Path>,
    {
        let mut original = custody.lock().await;
        original.task = WorkerCapabilityTask::Started(task::id());
        let constructed =
            catch_unwind(AssertUnwindSafe(move || plan.activate())).map_err(|payload| {
                WorkerCapabilityFailure {
                    operation: WorkerCapabilityOperation::Construction,
                    payload,
                }
            });
        let (reply, disposal) = capability_work_reply(constructed).await;
        let reply = match reply {
            Ok(reply) => Some(reply),
            Err(failure) => {
                original.failures.push(failure);
                None
            }
        };
        if let Some(failure) = disposal {
            original.failures.push(failure);
        }
        if let Some(reply) = reply {
            // Selected ActivationPlan outputs are already Send. No output static
            // bound or additional Send bound is imposed by this disposal boundary.
            // Original grant is borrowed; conversion may consume only its input.
            let input = original.grant.resolve(reply);
            match catch_unwind(AssertUnwindSafe(|| Event::inject_at(input))) {
                Ok(event) => original.event = Some(event),
                Err(payload) => original.failures.push(WorkerCapabilityFailure {
                    operation: WorkerCapabilityOperation::Receiving,
                    payload,
                }),
            }
        }
        if !original.failures.is_empty() {
            let (origin, payload) = original.failures.remove(0).into_parts();
            original.resumed_failure_origin = Some(origin);
            resume_unwind(payload);
        }
        let event = original
            .event
            .take()
            .expect("normal reply produced the actual Event");
        match control.send(event) {
            Ok(()) => {}
            Err(ControlClosed(event)) => original.event = Some(event),
        }
    }

    // This ordinary product has no lookup, scheduling or second task owner.
    // Each vector is a passive ordered result lane; the original ActivationTasks
    // remains the only owner of every spawned task and every resulting JoinError.
    impl<Inner, Activation, Preparation> TerminalReportTransaction
        for (
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )
    where
        Inner: TerminalReportTransaction + RetireCapabilities,
    {
        fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition) {
            if let Some(inner) = self.0.as_mut() {
                inner.finish_terminal_reports(disposition);
            }
        }
    }

    impl<Inner, Activation, Preparation> RetireCapabilities
        for (
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )
    where
        Inner: RetireCapabilities + Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        type Event = Inner::Event;
        type Descendants = (Inner::Descendants, (Activation, Preparation));
        async fn next_local_event(&mut self) -> Result<Self::Event, JoinError> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .next_local_event()
                .await
        }
        fn next_deadline(&mut self) -> Option<Instant> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .next_deadline()
        }
        fn pop_due(&mut self, now: Instant) -> Option<Self::Event> {
            self.0
                .as_mut()
                .expect("live original capabilities remain")
                .pop_due(now)
        }
        async fn receive_retirement(
            capabilities: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<Self::Event, Self::Descendants>>,
        ) {
            if received.is_some() {
                return;
            }
            let Some(original) = capabilities.as_mut() else {
                return;
            };
            Inner::receive_retirement(&mut original.0, &mut original.3).await;
            if !matches!((&original.0, &original.3), (None, Some(_))) {
                return;
            }
            let Some((None, activation, preparation, Some(lower))) = capabilities.take() else {
                unreachable!("the complete lower retirement was checked without another callback");
            };
            let CapabilityRetirement {
                activation_tasks,
                descendants,
                terminal_report,
                retirement_failures,
            } = lower;
            *received = Some(CapabilityRetirement {
                activation_tasks,
                descendants: (descendants, (activation, preparation)),
                terminal_report,
                retirement_failures,
            });
        }
        async fn retire(self) -> CapabilityRetirement<Self::Event, Self::Descendants> {
            let mut capabilities = Some(self);
            let mut received = None;
            Self::receive_retirement(&mut capabilities, &mut received).await;
            received.expect("normal lower retirement installs its actual output")
        }
    }

    impl<Inner, Activation, Preparation, Event, Source, Input> SourceAdmission<Event, Source, Input>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: SourceAdmission<Event, Source, Input> + RetireCapabilities + Send,
        Event: EventIngress<Source, Input>,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
        Input: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn admit_source(
            &mut self,
            input: &mut Option<Input>,
            received: &mut Option<Result<(), Input>>,
        ) -> impl Future<Output = ()> + Send {
            async move {
                self.capabilities_mut()
                    .0
                    .as_mut()
                    .expect("live original capabilities remain")
                    .admit_source(input, received)
                    .await;
            }
        }
    }

    impl<Inner, Activation, Preparation, New, Event, Path>
        InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>
            + RetireCapabilities
            + Send,
        New: Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <Creations<CreateChild<MailAddr, New>> as ActionItem>::Input<'a>,
            received: &'a mut Option<<Creations<CreateChild<MailAddr, New>> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            Creations<CreateChild<MailAddr, New>>: 'a,
        {
            async move {
                <Inner as InterpretItem<Creations<CreateChild<MailAddr, New>>, Event, Path>>::interpret_item(
                    self.capabilities_mut().0.as_mut().expect("live original capabilities remain"), input, received,
                ).await;
            }
        }
    }

    impl<Inner, Activation, Preparation, Position, Child> EstablishChild<Position, Child>
        for ActionInterpreter<(
            Option<Inner>,
            Activation,
            Preparation,
            Option<CapabilityRetirement<Inner::Event, Inner::Descendants>>,
        )>
    where
        Inner: EstablishChild<Position, Child> + RetireCapabilities + Send,
        Child: Behavior<Protocol: Protocol<Addr = MailAddr>> + Send,
        Inner::Event: Send,
        Inner::Descendants: Send,
        Activation: Send,
        Preparation: Send,
    {
        async fn establish_child(
            &mut self,
            creation: RoutedCreation<MailAddr, Child>,
        ) -> ItemSettlement<
            RoutedCreation<MailAddr, Child>,
            ChildCreationOutcome<Child, Position>,
            CreationRejection,
            Never,
        > {
            self.capabilities_mut()
                .0
                .as_mut()
                .expect("live original capabilities remain")
                .establish_child(creation)
                .await
        }
    }

    impl<C, N, Parent, Bindings, Origins, W, P, Preparation, Path>
        InterpretItem<BeginActivation<W, P>, C::Event, Path>
        for ActionInterpreter<(
            Option<ApplicationCapabilities<C, N, Parent, Bindings, Origins>>,
            Arc<Mutex<Vec<Arc<TaskCustody<WorkerActivationCustody<W, P, C::Event>>>>>>,
            Preparation,
            Option<CapabilityRetirement<
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Event,
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Descendants,
            >>,
        )>
    where
        C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
        C::Event: InjectEvent<WorkerActivation<W, P>, Path> + Send + 'static,
        W: Behavior + 'static,
        BehaviorAddr<W>: EndpointAddress,
        EstablishedRecipient<W::Protocol>: Send,
        ActivationPermit<W>: Send,
        P: ActivationPlan + 'static,
        Preparation: Send,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>: RetireCapabilities,
        Self: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <BeginActivation<W, P> as ActionItem>::Input<'a>,
            received: &'a mut Option<<BeginActivation<W, P> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            BeginActivation<W, P>: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(request) = input.as_ref() else {
                    return;
                };
                self.capabilities_mut().0.as_mut().expect("live original capabilities remain").inject_control_event::<_, Path>(request.started());
                let Some(request) = input.take() else {
                    return;
                };
                let settlement = {
            let (permit, plan, grant) = request.into_parts();
            let custody = Arc::new(TaskCustody::new(WorkerActivationCustody {
                task: WorkerCapabilityTask::Unpolled,
                permit,
                grant,
                event: None,
                failures: Vec::new(),
                resumed_failure_origin: None,
            }));
            self.capabilities_mut()
                .1
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(custody.clone());
            let control = self.capabilities_mut().0.as_ref().expect("live original capabilities remain").control.clone();
            self.capabilities_mut()
                .0
                .as_mut().expect("live original capabilities remain")
                .activation_tasks
                .as_mut().expect("live original capability tasks remain")
                .spawn(async move {
                    activate_worker::<W, P, C::Event, Path>(plan, &custody, &control).await;
                    Ok(())
                });
            ItemSettlement::Accepted(())

                };
                *received = Some(settlement);

            }
        }
    }

    impl<C, N, Parent, Bindings, Origins, Source, Role, W, P, Activation, Path>
        InterpretItem<PrepareWorkers<Source, Role, W, P>, C::Event, Path>
        for ActionInterpreter<(
            Option<ApplicationCapabilities<C, N, Parent, Bindings, Origins>>,
            Activation,
            Arc<
                Mutex<
                    Vec<Arc<TaskCustody<WorkerPreparationCustody<Source, Role, W, P, C::Event>>>>,
                >,
            >,
            Option<CapabilityRetirement<
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Event,
                <ApplicationCapabilities<C, N, Parent, Bindings, Origins> as RetireCapabilities>::Descendants,
            >>,
        )>
    where
        C: Behavior<Protocol: Protocol<Addr = MailAddr>>,
        C::Event: InjectEvent<WorkerPreparation<Source, Role, W, P>, Path> + Send + 'static,
        Source: WorkerPreparationSource<Role, W, P> + Send + 'static,
        Role: Send + Sync + 'static,
        W: Behavior + Send + 'static,
        P: ActivationPlan + 'static,
        Activation: Send,
        ApplicationCapabilities<C, N, Parent, Bindings, Origins>: RetireCapabilities,
        Self: Send,
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future; an async function captures unused route tags and would require new lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: <PrepareWorkers<Source, Role, W, P> as ActionItem>::Input<'a>,
            received: &'a mut Option<<PrepareWorkers<Source, Role, W, P> as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            PrepareWorkers<Source, Role, W, P>: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(request) = input.take() else {
                    return;
                };
                let settlement = {
            let (receipt, starting) = request.start();
            let custody = Arc::new(TaskCustody::new(WorkerPreparationCustody {
                task: WorkerCapabilityTask::Unpolled,
                phase: Some(WorkerPreparationPhase::Starting(starting)),
                event: None,
                failures: Vec::new(),
                resumed_failure_origin: None,
            }));
            self.capabilities_mut()
                .2
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(custody.clone());
            let control = self.capabilities_mut().0.as_ref().expect("live original capabilities remain").control.clone();
            self.capabilities_mut()
                .0
                .as_mut().expect("live original capabilities remain")
                .activation_tasks
                .as_mut().expect("live original capability tasks remain")
                .spawn(async move {
                    prepare_workers::<Source, Role, W, P, C::Event, Path>(&custody, &control).await;
                    Ok(())
                });
            ItemSettlement::Accepted(receipt)

                };
                *received = Some(settlement);

            }
        }
    }
    #[cfg(test)]
    mod activation_declaration {
        use core::marker::PhantomData;
        use std::panic::panic_any;
        use std::sync::Arc;

        use crate::local::execution::ActivationTasks;
        use behavior::{
            Actions, ActiveTurn, Address, Behavior, BehaviorActed, BehaviorBase,
            ChildCreationOutcome, CommittedChild, CreateChild, CreationId, CreationKind,
            CreationSettlement, Creations, CreationsSettled, EndpointAddress, EstablishedActor,
            InterpretEstablished, ItemSettlement, MessageProtocol, Never, NoBirths, NoSends,
            Protocol, SettledItem, Step, User,
        };
        use behavior_actors::atomic::{
            ActivationPlan, InitialWorkerOutcome, ProxyControl, ProxyOutcome, StableProxy,
            WorkerActivation, WorkerInitializationOutcome, WorkerInitializationReport,
            WorkerStartResult,
        };
        use behavior_actors::{Activate as _, Active};
        use communication::{Config, ControlClosed, mailbox_channel};

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct ActivationMailAddr(u64);

        impl Address for ActivationMailAddr {
            type Nonce = u64;
        }

        #[derive(Clone)]
        pub(super) struct ActivationEndpoint(pub(super) Arc<Vec<u8>>);

        impl InterpretEstablished<MessageProtocol<ActivationMailAddr, Never>> for ActivationEndpoint {
            type Output = ();

            fn interpret_established(&mut self, endpoint: ActivationEndpoint) {
                *self = endpoint;
            }
        }

        pub(super) struct ActivationInstalled<B: Behavior>(
            ActivationEndpoint,
            PhantomData<fn() -> B>,
        );

        impl<B: Behavior> Clone for ActivationInstalled<B> {
            fn clone(&self) -> Self {
                Self(self.0.clone(), PhantomData)
            }
        }

        impl EndpointAddress for ActivationMailAddr {
            type Established<P>
                = ActivationEndpoint
            where
                P: Protocol<Addr = Self>;
            type Installed<B>
                = ActivationInstalled<B>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>;

            fn recipient<B>(installed: &Self::Installed<B>) -> Self::Established<B::Protocol>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>,
            {
                installed.0.clone()
            }
        }

        pub(super) struct ActivationWorker;

        impl BehaviorBase for ActivationWorker {
            type Base = Self;

            fn base(&self) -> &Self::Base {
                self
            }
        }

        impl Behavior for ActivationWorker {
            type Protocol = MessageProtocol<ActivationMailAddr, Never>;
            type Event = User<ActivationMailAddr, Never>;
            type Sends = NoSends;
            type Ph = Never;
            type Error = Never;
            type Birth = NoBirths;

            fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
                match event.message {}
            }
        }

        pub(super) enum ActivationWork {
            Ready,
            Panicked(Arc<Vec<u8>>),
        }

        impl ActivationPlan for ActivationWork {
            type Ready = ();
            type Rejection = Never;

            #[expect(
                clippy::unused_async_trait_impl,
                reason = "Keep the original selected plan and its native panic cold until the future is polled."
            )]
            async fn activate(self) -> Result<(), Never> {
                match self {
                    Self::Ready => Ok(()),
                    Self::Panicked(original) => panic_any(original),
                }
            }
        }

        #[expect(
            clippy::type_complexity,
            reason = "observe the existing proxy's complete concrete birth and seven named send lanes"
        )]
        pub(super) fn assert_proxy_actions(
            actions: &Actions<
                ActivationMailAddr,
                Never,
                <StableProxy<ActivationWorker, ActivationWork> as Behavior>::Sends,
                <StableProxy<ActivationWorker, ActivationWork> as Behavior>::Birth,
            >,
            births: usize,
            observation: &[CreationId],
            initialization: usize,
            activation: usize,
            reports: usize,
        ) {
            assert_eq!(actions.creates.len(), births);
            let actual: Vec<_> = actions
                .sends
                .worker_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observation);
            assert_eq!(actions.sends.worker_initializations.len(), initialization);
            assert_eq!(actions.sends.worker_activations.len(), activation);
            assert_eq!(actions.sends.worker_shutdowns.len(), 0);
            assert_eq!(actions.sends.worker_deliveries.len(), 0);
            assert_eq!(actions.sends.owner_outcomes.len(), reports);
            assert_eq!(actions.sends.diagnostics.len(), 0);
            assert!(matches!(actions.become_, Step::Continue));
        }

        pub(super) fn initialize_activation(
            original_endpoint: Arc<Vec<u8>>,
            plan: ActivationWork,
        ) -> (
            Active<StableProxy<ActivationWorker, ActivationWork>>,
            WorkerInitializationReport<ActivationWorker, ActivationWork>,
            CreationId,
        ) {
            let proxy = StableProxy::<ActivationWorker, ActivationWork>::activated();
            let initialized = proxy
                .initialize()
                .unwrap_or_else(|_| panic!("pure proxy initialization"));
            assert_proxy_actions(&initialized.actions, 0, &[], 0, 0, 0);
            let mut proxy = initialized.behavior;
            let creating = proxy
                .on(ProxyControl::start_with(ActivationWorker, plan))
                .unwrap_or_else(|_| panic!("original worker start is admitted"));
            let ids: Vec<_> = creating.creates.iter().map(CreateChild::id).collect();
            assert_proxy_actions(&creating, 1, &ids, 0, 0, 0);
            let mut births = creating.creates.into_iter();
            let birth = births.next().expect("one original worker birth");
            let extra = births.next();
            assert!(extra.is_none());
            let (id, worker, kind) = birth.into_parts();
            assert_eq!(kind, CreationKind::Birth);
            let initialized_worker = worker
                .initialize()
                .unwrap_or_else(|_| panic!("pure worker initialization"));
            assert_eq!(initialized_worker.actions.creates.len(), 0);
            assert_eq!(initialized_worker.actions.sends.owned, NoSends);
            assert_eq!(initialized_worker.actions.sends.inner, NoSends);
            assert!(matches!(initialized_worker.actions.become_, Step::Continue));
            let established = EstablishedActor::issued(ActivationInstalled(
                ActivationEndpoint(original_endpoint),
                PhantomData,
            ));
            let committed = CreationsSettled::new(CreationSettlement::Settled(Creations::one(
                SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(id, kind, established)),
                )),
            )));
            let initializing = proxy
                .on(committed)
                .unwrap_or_else(|_| panic!("original worker binding commits"));
            assert_proxy_actions(&initializing, 0, &[], 1, 0, 0);
            let mut requests = initializing.sends.worker_initializations.into_requests();
            let request = requests.pop().expect("one original initialization request");
            assert_eq!(requests.len(), 0);
            assert_eq!(request.worker().creation(), id);
            let report = request.resolve(WorkerInitializationOutcome::ReadyForActivation);
            (proxy, report, id)
        }

        #[tokio::test]
        async fn ordinary_public_activation_returns_original_ready_policy() {
            let endpoint = Arc::new(vec![107, 109]);
            let endpoint_original = Arc::downgrade(&endpoint);
            let (mut proxy, report, id) = initialize_activation(endpoint, ActivationWork::Ready);
            let activating = proxy
                .on(report)
                .unwrap_or_else(|_| panic!("original initialization correlation is accepted"));
            assert_proxy_actions(&activating, 0, &[], 0, 1, 0);
            let mut requests = activating.sends.worker_activations.into_requests();
            let request = requests.pop().expect("one original BeginActivation");
            assert_eq!(requests.len(), 0);
            let worker = request.worker();
            let started = proxy
                .on(request.started())
                .unwrap_or_else(|_| panic!("actual private activation grant is accepted"));
            assert_proxy_actions(&started, 0, &[], 0, 0, 0);
            let (control, owner, mailbox, receiver) = mailbox_channel::<
                WorkerActivation<ActivationWorker, ActivationWork>,
                User<ActivationMailAddr, Never>,
            >(Config::new(2));
            drop(receiver);
            let mut tasks = ActivationTasks::new();
            tasks.spawn(async move {
                control
                    .send(request.activate().await)
                    .map_err(|ControlClosed(event)| event)
            });
            let (mut closed, failures) = tasks.settle().await;
            assert_eq!(closed.len(), 1);
            assert_eq!(failures.len(), 0);
            let returned = closed
                .pop()
                .expect("the actual closed lane returns original Ready input");
            assert_eq!(returned.worker(), worker);
            assert_eq!(worker.creation(), id);
            let reported = proxy.on(returned).unwrap_or_else(|_| {
                panic!("the actual private grant admits its original Ready input")
            });
            assert_proxy_actions(&reported, 0, &[], 0, 0, 1);
            let mut reports = reported.sends.owner_outcomes.into_requests();
            let outcome = reports
                .pop()
                .expect("one complete original readiness report")
                .into_inner();
            assert_eq!(reports.len(), 0);
            match &outcome {
                ProxyOutcome::Initial {
                    outcome:
                        InitialWorkerOutcome::Resolved {
                            result: WorkerStartResult::Ready { attempt, readiness },
                        },
                } => {
                    assert_eq!(*readiness, ());
                    assert_eq!(attempt.to_owned(), worker);
                    assert_eq!(attempt.creation(), id);
                }
                ProxyOutcome::Initial { .. }
                | ProxyOutcome::Replacement { .. }
                | ProxyOutcome::WorkerStopped { .. }
                | ProxyOutcome::Unavailable { .. } => {
                    panic!("complete original Ready outcome required")
                }
            }
            drop((outcome, reports, proxy, closed, failures, owner, mailbox));
            let released = endpoint_original.strong_count();
            assert_eq!(released, 0);
        }
    }
    mod grouped_preparation {
        use std::convert::Infallible;
        use std::marker::PhantomData;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use behavior::{
            Actions, ActiveTurn, Address, Behavior, BehaviorActed, BehaviorBase,
            ChildCreationOutcome, ChildInputReason, ChildReport, CommittedChild, CreateChild,
            CreationId, CreationKind, CreationSettlement, Creations, CreationsSettled,
            EndpointAddress, EstablishedActor, Interpretation, InterpretationProgress,
            ItemSettlement, MessageProtocol, Never, NoBirths, NoSends, Protocol, SettledItem, Step,
            User,
        };
        use behavior_actors::atomic::{
            ActivationPolicy, ActorDrainPolicy, DiagnosticDisposition, FailureReaction,
            FixedSupervisor, ImmediateActivation, InitialWorkerOutcome, OrderedRoles,
            PrepareWorkers, ProxyControl, ProxyControlAdmission, ProxyOperation, ProxyOutcome,
            Recovery, RestartLimit, RestartRelease, StableProxy, Strategy,
            WorkerInitializationOutcome, WorkerSource, WorkerStartResult, WorkerSubmission, fixed,
        };
        use behavior_actors::{Activate as _, ChildStopped, Exit};

        use crate::worker_preparation::{WorkerPreparationSource, WorkerPreparationStart};
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct PoolMailAddr(u64);

        impl Address for PoolMailAddr {
            type Nonce = u64;
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) struct PoolEndpoint(u64);

        pub(super) struct PoolInstalled<B: Behavior>(PoolEndpoint, PhantomData<fn() -> B>);

        impl<B: Behavior> Clone for PoolInstalled<B> {
            fn clone(&self) -> Self {
                Self(self.0, PhantomData)
            }
        }

        impl EndpointAddress for PoolMailAddr {
            type Established<P>
                = PoolEndpoint
            where
                P: Protocol<Addr = Self>;

            type Installed<B>
                = PoolInstalled<B>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>;

            fn recipient<B>(installed: &Self::Installed<B>) -> Self::Established<B::Protocol>
            where
                B: Behavior<Protocol: Protocol<Addr = Self>>,
            {
                installed.0
            }
        }

        #[derive(Debug, Eq, PartialEq)]
        pub(super) struct WorkerRole(pub(super) Arc<Vec<u8>>);

        pub(super) struct ReplacementWorker(pub(super) Arc<Vec<u8>>);

        impl BehaviorBase for ReplacementWorker {
            type Base = Self;

            fn base(&self) -> &Self::Base {
                self
            }
        }

        impl Behavior for ReplacementWorker {
            type Protocol = MessageProtocol<PoolMailAddr, Never>;
            type Event = User<PoolMailAddr, Never>;
            type Sends = NoSends;
            type Ph = Never;
            type Error = Never;
            type Birth = NoBirths;

            fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
                match event.message {}
            }
        }

        pub(super) struct PreparationSource {
            pub(super) material: Arc<Vec<u8>>,
            pub(super) first_worker: Option<Arc<Vec<u8>>>,
            pub(super) requested_roles: Vec<Vec<u8>>,
        }

        impl WorkerSource<WorkerRole, ReplacementWorker, ImmediateActivation> for PreparationSource {
            type WorkerRejection = Never;
            type SourceRejection = Never;
        }

        impl WorkerPreparationSource<WorkerRole, ReplacementWorker, ImmediateActivation>
            for PreparationSource
        {
            #[expect(
                clippy::unused_async_trait_impl,
                reason = "Prepare the original worker only when this borrowed operation is polled."
            )]
            async fn prepare_first(
                &mut self,
                role: &WorkerRole,
            ) -> WorkerPreparationStart<ReplacementWorker, ImmediateActivation, Never, Never>
            {
                self.requested_roles.push(role.0.as_ref().clone());
                let original = self
                    .first_worker
                    .take()
                    .expect("one original replacement worker");
                WorkerPreparationStart::Submitted(WorkerSubmission::immediate(ReplacementWorker(
                    original,
                )))
            }

            async fn prepare_next(
                &mut self,
                role: &WorkerRole,
            ) -> Result<WorkerSubmission<ReplacementWorker, ImmediateActivation>, Never>
            {
                self.requested_roles.push(role.0.as_ref().clone());
                panic!("later preparation failed while borrowing the source");
            }
        }

        struct ProxyAdmission {
            actor: Option<EstablishedActor<StableProxy<ReplacementWorker, ImmediateActivation>>>,
            admitted: Option<(
                CreationId,
                ProxyControl<ReplacementWorker, ImmediateActivation>,
            )>,
        }

        impl ProxyControlAdmission<ReplacementWorker, ImmediateActivation> for ProxyAdmission {
            fn admit_proxy_control(
                &mut self,
                creation: CreationId,
                control: ProxyControl<ReplacementWorker, ImmediateActivation>,
            ) -> ItemSettlement<
                ProxyControl<ReplacementWorker, ImmediateActivation>,
                EstablishedActor<StableProxy<ReplacementWorker, ImmediateActivation>>,
                ChildInputReason,
                Never,
            > {
                assert!(self.admitted.is_none());
                self.admitted = Some((creation, control));
                ItemSettlement::Accepted(self.actor.take().expect("one original committed proxy"))
            }
        }

        #[expect(
            clippy::type_complexity,
            reason = "the observation spells the actual supervisor creation and send products without replacing its owning lanes"
        )]
        fn assert_supervisor_actions(
            actions: &Actions<
                PoolMailAddr,
                Never,
                <FixedSupervisor<
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                    PreparationSource,
                    Infallible,
                    Infallible,
                > as Behavior>::Sends,
                <FixedSupervisor<
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                    PreparationSource,
                    Infallible,
                    Infallible,
                > as Behavior>::Birth,
            >,
            creations: usize,
            observations: &[CreationId],
            operations: usize,
            preparations: usize,
        ) {
            assert_eq!(actions.creates.len(), creations);
            let actual: Vec<_> = actions
                .sends
                .proxy_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observations);
            assert_eq!(actions.sends.proxy_operations.len(), operations);
            assert_eq!(actions.sends.worker_preparations.len(), preparations);
            assert!(actions.sends.restart_schedules.is_empty());
            assert_eq!(actions.sends.lifecycle, NoSends);
            assert!(actions.sends.status_replies.as_slice().is_empty());
            assert!(actions.sends.capability_replies.as_slice().is_empty());
            assert!(actions.sends.diagnostics.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }

        #[expect(
            clippy::type_complexity,
            reason = "the observation preserves the actual proxy birth and send associated products"
        )]
        pub(super) fn assert_proxy_actions(
            actions: &Actions<
                PoolMailAddr,
                Never,
                <StableProxy<ReplacementWorker, ImmediateActivation> as Behavior>::Sends,
                <StableProxy<ReplacementWorker, ImmediateActivation> as Behavior>::Birth,
            >,
            creations: usize,
            observations: &[CreationId],
            initializations: usize,
            activations: usize,
            outcomes: usize,
        ) {
            assert_eq!(actions.creates.len(), creations);
            let actual: Vec<_> = actions
                .sends
                .worker_observations
                .iter()
                .map(|request| request.child)
                .collect();
            assert_eq!(actual, observations);
            assert_eq!(actions.sends.worker_initializations.len(), initializations);
            assert_eq!(actions.sends.worker_activations.len(), activations);
            assert!(actions.sends.worker_shutdowns.is_empty());
            assert_eq!(actions.sends.worker_deliveries.len(), 0);
            assert_eq!(actions.sends.owner_outcomes.len(), outcomes);
            assert!(actions.sends.diagnostics.is_empty());
            assert!(matches!(actions.become_, Step::Continue));
        }

        #[expect(
            clippy::too_many_lines,
            reason = "one complete grouped producer trace retains proxy creation, worker commitment, initialization, activation and preparation receipt ownership"
        )]
        pub(super) async fn admitted_group(
            source: PreparationSource,
            roles: OrderedRoles<WorkerRole>,
        ) -> PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>
        {
            let activation = ActivationPolicy::new(3).expect("three positive activation slots");
            let built = fixed(
                |role: &WorkerRole| -> Result<WorkerSubmission<ReplacementWorker, ImmediateActivation>, Never> {
                    Ok(WorkerSubmission::immediate(ReplacementWorker(Arc::new(role.0.as_ref().clone()))))
                },
                roles,
                activation,
                Recovery::permanent(source, Strategy::OneForAll, RestartLimit::new(3, Duration::from_secs(60)), RestartRelease::immediate()),
                FailureReaction::StopSupervisor,
                ActorDrainPolicy::WaitForActorGraph,
                DiagnosticDisposition::<Infallible>::terminate(),
            ).build::<ReplacementWorker, ImmediateActivation, Never>()
                .unwrap_or_else(|_| panic!("all original declarations prepare"));
            let initialized = built
                .initialize()
                .unwrap_or_else(|_| panic!("pure fixed initialization"));
            let mut supervisor = initialized.behavior;
            let proxy_ids: Vec<_> = initialized
                .actions
                .creates
                .iter()
                .map(CreateChild::id)
                .collect();
            assert_supervisor_actions(&initialized.actions, 3, &proxy_ids, 0, 0);
            let mut proxies = Vec::new();
            let mut committed = Vec::new();
            for (creation, endpoint) in initialized.actions.creates.into_iter().zip([801, 802, 803])
            {
                let (id, proxy, kind) = creation.into_parts();
                assert_eq!(kind, CreationKind::Birth);
                let initialized_proxy = proxy
                    .initialize()
                    .unwrap_or_else(|_| panic!("pure proxy initialization"));
                assert_proxy_actions(&initialized_proxy.actions, 0, &[], 0, 0, 0);
                proxies.push((id, initialized_proxy.behavior));
                let actor =
                    EstablishedActor::issued(PoolInstalled(PoolEndpoint(endpoint), PhantomData));
                committed.push(SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(id, kind, actor)),
                )));
            }
            let dispatched = supervisor
                .on(CreationsSettled::new(CreationSettlement::Settled(
                    committed.into_iter().collect(),
                )))
                .unwrap_or_else(|_| panic!("actual declared proxy routes commit"));
            assert_supervisor_actions(&dispatched, 0, &[], 3, 0);
            let mut operations = dispatched.sends.proxy_operations.into_items();
            let operation = operations.remove(0);
            let proxy_id = operation.creation();
            let (original_proxy_id, mut proxy) = proxies.remove(0);
            assert_eq!(proxy_id, original_proxy_id);
            let mut admission = ProxyAdmission {
                actor: Some(EstablishedActor::issued(PoolInstalled(
                    PoolEndpoint(801),
                    PhantomData,
                ))),
                admitted: None,
            };
            let mut operation = Some(InterpretationProgress::Original(operation));
            ProxyOperation::settle(&mut operation, &mut admission);
            let admitted = match operation {
                Some(InterpretationProgress::Completed(admitted)) => admitted,
                retained => {
                    drop(retained);
                    panic!("original proxy admission must return its complete settlement");
                }
            };
            let Interpretation::Complete(ItemSettlement::Accepted(receipt)) = admitted else {
                drop(admitted);
                panic!("original proxy admission must be complete and accepted");
            };
            let (admitted_id, control) = admission
                .admitted
                .take()
                .expect("whole original control retained");
            assert_eq!(admitted_id, proxy_id);
            let settled = supervisor
                .on(SettledItem::Attempted(ItemSettlement::Accepted(receipt)))
                .unwrap_or_else(|_| panic!("actual proxy receipt accepted"));
            assert_supervisor_actions(&settled, 0, &[], 0, 0);
            let creating = proxy
                .on(control)
                .unwrap_or_else(|_| panic!("original initial worker control"));
            let worker_ids: Vec<_> = creating.creates.iter().map(CreateChild::id).collect();
            assert_proxy_actions(&creating, 1, &worker_ids, 0, 0, 0);
            let mut births = creating.creates.into_iter();
            let creation = births.next().expect("one exact original worker birth");
            let extra = births.next();
            assert!(extra.is_none());
            let (worker_id, worker, kind) = creation.into_parts();
            assert_eq!(kind, CreationKind::Birth);
            let original_worker = worker
                .initialize()
                .unwrap_or_else(|_| panic!("pure worker initialization"));
            assert_eq!(original_worker.behavior.base().0.as_slice(), &[10]);
            assert!(original_worker.actions.creates.is_empty());
            assert_eq!(original_worker.actions.sends.inner, NoSends);
            assert_eq!(original_worker.actions.sends.owned, NoSends);
            assert!(matches!(original_worker.actions.become_, Step::Continue));
            let worker_commit = CreationsSettled::new(CreationSettlement::Settled(Creations::one(
                SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(CommittedChild::new(
                        worker_id,
                        kind,
                        EstablishedActor::issued(PoolInstalled(PoolEndpoint(990), PhantomData)),
                    )),
                )),
            )));
            let initializing = proxy
                .on(worker_commit)
                .unwrap_or_else(|_| panic!("actual worker route commits"));
            assert_proxy_actions(&initializing, 0, &[], 1, 0, 0);
            let mut initializations = initializing.sends.worker_initializations.into_requests();
            let initialization = initializations
                .pop()
                .expect("one exact initialization observation");
            assert!(initializations.is_empty());
            let activating = proxy
                .on(initialization.resolve(WorkerInitializationOutcome::ReadyForActivation))
                .unwrap_or_else(|_| panic!("actual initialization correlated"));
            assert_proxy_actions(&activating, 0, &[], 0, 1, 0);
            let mut activations = activating.sends.worker_activations.into_requests();
            let activation = activations.pop().expect("one activation permit");
            assert!(activations.is_empty());
            let starting = proxy
                .on(activation.started())
                .unwrap_or_else(|_| panic!("exact activation start"));
            assert_proxy_actions(&starting, 0, &[], 0, 0, 0);
            let ready = activation.activate().await;
            let reporting = proxy
                .on(ready)
                .unwrap_or_else(|_| panic!("actual immediate readiness"));
            assert_proxy_actions(&reporting, 0, &[], 0, 0, 1);
            let mut reports = reporting.sends.owner_outcomes.into_requests();
            let outcome = reports
                .pop()
                .expect("one whole original readiness report")
                .into_inner();
            assert!(reports.is_empty());
            let attempt = match &outcome {
                ProxyOutcome::Initial {
                    outcome:
                        InitialWorkerOutcome::Resolved {
                            result: WorkerStartResult::Ready { attempt, readiness },
                        },
                } => {
                    assert_eq!(*readiness, ());
                    assert_eq!(attempt.creation(), worker_id);
                    attempt.clone()
                }
                ProxyOutcome::Initial { .. }
                | ProxyOutcome::Replacement { .. }
                | ProxyOutcome::WorkerStopped { .. }
                | ProxyOutcome::Unavailable { .. } => {
                    panic!("complete initial Ready outcome required")
                }
            };
            let roster_ready = supervisor
                .on(ChildReport::new(proxy_id, outcome))
                .unwrap_or_else(|_| panic!("actual roster readiness"));
            assert_supervisor_actions(&roster_ready, 0, &[], 0, 0);
            let stopped = ChildStopped::new(attempt.creation(), Ok(Exit::Normal), Instant::now());
            let preparing = supervisor
                .on(ChildReport::new(
                    proxy_id,
                    ProxyOutcome::WorkerStopped {
                        worker: attempt,
                        stopped,
                    },
                ))
                .unwrap_or_else(|_| panic!("OneForAll selects all declared roles"));
            assert_supervisor_actions(&preparing, 0, &[], 0, 1);
            let mut requests = preparing.sends.worker_preparations.into_items();
            let request = requests.pop().expect("one actual grouped producer request");
            assert!(requests.is_empty());
            // These pure declarations and unattempted proxy requests are explicitly discharged.
            // No actor task was installed by this pure producer controller.
            drop(supervisor);
            drop(operations);
            drop(proxies);
            drop(proxy);
            drop(original_worker.behavior);
            request
        }
    }

    use std::mem;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;

    enum WorkDisposal {
        Ordinary,
        Panicked(Arc<Vec<u8>>),
    }
    struct BorrowedCapabilityWork<'a> {
        original: &'a Vec<u8>,
        disposal: WorkDisposal,
    }
    impl<'a> Future for BorrowedCapabilityWork<'a> {
        type Output = (&'a Vec<u8>, Rc<Vec<u8>>);
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready((self.original, Rc::new(vec![151, 157])))
        }
    }
    impl Drop for BorrowedCapabilityWork<'_> {
        fn drop(&mut self) {
            let disposal = mem::replace(&mut self.disposal, WorkDisposal::Ordinary);
            match disposal {
                WorkDisposal::Ordinary => {}
                WorkDisposal::Panicked(payload) => panic_any(payload),
            }
        }
    }

    #[tokio::test]
    async fn borrowed_ready_reply_survives_native_disposal_without_output_send_bound() {
        let source = vec![139, 149];
        let original_source_pointer = source.as_ptr();
        let cause = Arc::new(vec![163, 167]);
        let original_cause = Arc::downgrade(&cause);
        let (reply, disposal) = capability_work_reply(Ok(BorrowedCapabilityWork {
            original: &source,
            disposal: WorkDisposal::Panicked(cause),
        }))
        .await;
        let (copied, original_reply) = match reply {
            Ok(reply) => reply,
            Err(failure) => {
                drop(failure);
                panic!("actual Ready is acquired before disposal")
            }
        };
        let reply_original = Rc::downgrade(&original_reply);
        assert_eq!(copied, &source);
        assert_eq!(copied.as_ptr(), original_source_pointer);
        assert_eq!(original_reply.as_slice(), [151, 157]);
        let failure = disposal.expect("actual original future destructor panicked");
        assert_eq!(failure.operation, WorkerCapabilityOperation::Disposal);
        assert_eq!(original_cause.strong_count(), 1);
        assert_eq!(reply_original.strong_count(), 1);
        drop(failure);
        let cause_released = original_cause.strong_count();
        let reply_retained = reply_original.strong_count();
        drop(original_reply);
        let reply_released = reply_original.strong_count();
        assert_eq!(cause_released, 0);
        assert_eq!(reply_retained, 1);
        assert_eq!(reply_released, 0);
    }

    struct PanickingCapabilityWork {
        poll: Option<Arc<Vec<u8>>>,
        disposal: Option<Arc<Vec<u8>>>,
    }
    impl Future for PanickingCapabilityWork {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic_any(self.poll.take().expect("one original poll cause"));
        }
    }
    impl Drop for PanickingCapabilityWork {
        fn drop(&mut self) {
            if let Some(payload) = self.disposal.take() {
                panic_any(payload);
            }
        }
    }
    #[tokio::test]
    async fn capability_poll_and_disposal_keep_both_original_native_causes() {
        let poll = Arc::new(vec![173]);
        let poll_original = Arc::downgrade(&poll);
        let disposal = Arc::new(vec![179]);
        let disposal_original = Arc::downgrade(&disposal);
        let (reply, disposed) = capability_work_reply(Ok(PanickingCapabilityWork {
            poll: Some(poll),
            disposal: Some(disposal),
        }))
        .await;
        let Err(primary) = reply else {
            panic!("the genuine poll panics")
        };
        let secondary = disposed.expect("the genuine destructor independently panics");
        assert_eq!(primary.operation, WorkerCapabilityOperation::Execution);
        assert_eq!(secondary.operation, WorkerCapabilityOperation::Disposal);
        assert_eq!(poll_original.strong_count(), 1);
        assert_eq!(disposal_original.strong_count(), 1);
        drop((primary, secondary));
        let released = (
            poll_original.strong_count(),
            disposal_original.strong_count(),
        );
        assert_eq!(released, (0, 0));
    }
    use crate::local::endpoint::ExtractLocalEndpoint;
    use activation_declaration::{
        ActivationEndpoint, ActivationWork, ActivationWorker, initialize_activation,
    };
    use behavior::{ChildChoice, CreationKind, CreationSettlement, SettledItem};
    use grouped_preparation::{PreparationSource, ReplacementWorker, WorkerRole, admitted_group};

    struct CapabilityChild {
        original: Arc<Vec<u8>>,
    }
    impl BehaviorBase for CapabilityChild {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for CapabilityChild {
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

    enum CapabilityEvent {
        User(User<MailAddr, Never>),
        Activation(WorkerActivation<ActivationWorker, ActivationWork>),
        Preparation(
            WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
        ),
        ActivationSource(ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>),
        PreparationSource(
            ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        ),
        Shutdown(ShutdownRequested),
    }
    impl UserEvent for CapabilityEvent {
        type Addr = MailAddr;
        type Message = Never;
        fn user(from: MailAddr, message: Never) -> Self {
            Self::User(User::new(from, message))
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            match self {
                Self::User(user) => Ok(user),
                other => Err(other),
            }
        }
    }
    impl ComposedEvent for CapabilityEvent {
        type Inner = User<MailAddr, Never>;
        fn from_inner(event: Self::Inner) -> Self {
            Self::User(event)
        }
    }
    impl InjectEvent<WorkerActivation<ActivationWorker, ActivationWork>, Here> for CapabilityEvent {
        fn inject_at(event: WorkerActivation<ActivationWorker, ActivationWork>) -> Self {
            Self::Activation(event)
        }
    }
    impl
        InjectEvent<
            WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
            Inside<Here>,
        > for CapabilityEvent
    {
        fn inject_at(
            event: WorkerPreparation<
                PreparationSource,
                WorkerRole,
                ReplacementWorker,
                ImmediateActivation,
            >,
        ) -> Self {
            Self::Preparation(event)
        }
    }
    impl
        EventIngress<
            BeginActivation<ActivationWorker, ActivationWork>,
            ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>,
        > for CapabilityEvent
    {
        fn ingress(
            event: ActionItemResult<BeginActivation<ActivationWorker, ActivationWork>>,
        ) -> Self {
            Self::ActivationSource(event)
        }
    }
    impl
        EventIngress<
            PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>,
            ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        > for CapabilityEvent
    {
        fn ingress(
            event: ActionItemResult<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        ) -> Self {
            Self::PreparationSource(event)
        }
    }
    impl InjectEvent<ShutdownRequested, Here> for CapabilityEvent {
        fn inject_at(event: ShutdownRequested) -> Self {
            Self::Shutdown(event)
        }
    }

    struct CapabilityParent {
        original: Arc<Vec<u8>>,
        child: Option<CapabilityChild>,
        creation: CreationId,
        activation: Option<BeginActivation<ActivationWorker, ActivationWork>>,
        preparation: Option<
            PrepareWorkers<PreparationSource, WorkerRole, ReplacementWorker, ImmediateActivation>,
        >,
        received: Vec<CapabilityEvent>,
    }
    impl BehaviorBase for CapabilityParent {
        type Base = Self;
        fn base(&self) -> &Self {
            self
        }
    }
    impl Behavior for CapabilityParent {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = CapabilityEvent;
        type Sends = SendLayer<
            InterpreterRequests<BeginActivation<ActivationWorker, ActivationWork>>,
            SourceActions<
                PrepareWorkers<
                    PreparationSource,
                    WorkerRole,
                    ReplacementWorker,
                    ImmediateActivation,
                >,
            >,
        >;
        type Birth = RetirementBirths<
            <ChildCons<MailAddr, CapabilityChild, NoChildren> as ChildProduct<MailAddr>>::Choice,
        >;
        type Ph = Never;
        type Error = Never;
        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            let child = self.child.take().expect("one genuine original leaf child");
            let activation = self
                .activation
                .take()
                .expect("one actual public activation request");
            let preparation = self
                .preparation
                .take()
                .expect("one actual public grouped request");
            let creates = Children::<MailAddr>::new()
                .child(self.creation, child)
                .into_creates();
            Ok(Actions::new(
                SendLayer::new(
                    InterpreterRequests::one(activation),
                    SourceActions::sending(preparation),
                ),
                creates,
                Step::Continue,
            ))
        }
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            self.received.push(event);
            // This test-only domain policy stops on its first actual control
            // input. Environment retirement still joins both admitted tasks.
            Ok(Actions::stop())
        }
    }

    struct CapabilityChildTerminal {
        origin: ChildOrigin<CapabilityParent, ChildHead>,
        retirement: ActorRetirement<CapabilityChild, Self, ()>,
    }
    impl
        ProjectTerminal<
            ChildOrigin<CapabilityParent, ChildHead>,
            ActorRetirement<CapabilityChild, Self, ()>,
        > for CapabilityChildTerminal
    {
        fn project(
            origin: ChildOrigin<CapabilityParent, ChildHead>,
            retirement: ActorRetirement<CapabilityChild, Self, ()>,
        ) -> Self {
            // This total conversion moves the exact input; it performs no
            // assertion, callback, lookup or independent task spawning.
            Self { origin, retirement }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the single actual startup/source arbitration, original task/child joins and every complete typed custody/discharge oracle together so finite omissions fail only after cleanup."
    )]
    async fn actual_parent_retirement_keeps_both_capability_lanes_and_joined_child() {
        let endpoint = Arc::new(vec![107, 109]);
        let endpoint_original = Arc::downgrade(&endpoint);
        let cause = Arc::new(vec![113, 127]);
        let cause_original = Arc::downgrade(&cause);
        let (mut proxy, report, activation_creation) =
            initialize_activation(endpoint, ActivationWork::Panicked(cause));
        let actions = proxy
            .on(report)
            .unwrap_or_else(|_| panic!("genuine proxy activation issuance"));
        activation_declaration::assert_proxy_actions(&actions, 0, &[], 0, 1, 0);
        let mut requests = actions.sends.worker_activations.into_requests();
        let activation = requests.pop().expect("one genuine BeginActivation");
        assert_eq!(requests.len(), 0);
        let expected_worker = activation.worker();
        assert_eq!(expected_worker.creation(), activation_creation);
        let material = Arc::new(vec![40, 41]);
        let worker = Arc::new(vec![50, 51]);
        let first = Arc::new(vec![10]);
        let current = Arc::new(vec![20]);
        let tail = Arc::new(vec![30]);
        let originals = [
            Arc::downgrade(&material),
            Arc::downgrade(&worker),
            Arc::downgrade(&first),
            Arc::downgrade(&current),
            Arc::downgrade(&tail),
        ];
        let roles = OrderedRoles::new(WorkerRole(first), [WorkerRole(current), WorkerRole(tail)])
            .expect("three distinct original roles");
        let preparation = admitted_group(
            PreparationSource {
                material,
                first_worker: Some(worker),
                requested_roles: Vec::new(),
            },
            roles,
        )
        .await;
        let original_parent = Arc::new(vec![43, 47]);
        let parent_original = Arc::downgrade(&original_parent);
        let original_child = Arc::new(vec![31, 37]);
        let child_original = Arc::downgrade(&original_child);
        let mut creations = CreationSequence::new();
        let creation = creations.issue().expect("actual parent namespace");
        let roots = ActorSpace::<MessageProtocol<MailAddr, Never>>::new();
        let actor_spaces = Arc::new(roots.clone());
        let allocations = ApplicationAddresses::new();
        let activation_lane = Arc::new(Mutex::new(Vec::new()));
        let preparation_lane = Arc::new(Mutex::new(Vec::new()));
        let receiving_activation = activation_lane.clone();
        let receiving_preparation = preparation_lane.clone();
        let parent = CapabilityParent {
            original: original_parent,
            child: Some(CapabilityChild {
                original: original_child,
            }),
            creation,
            activation: Some(activation),
            preparation: Some(preparation),
            received: Vec::new(),
        };
        let (authority, startup, (), task, termination_notification) =
            spawn_local_execution::<CapabilityParent, _, StandardIngress, _, _, _>(
                roots.clone(),
                Config::new(2),
                MailAddr::APPLICATION_ROOT,
                parent,
                move |control, terminal_reports, timers, observations| {
                    let capabilities = ApplicationCapabilities::<
                        CapabilityParent,
                        ActorSpace<MessageProtocol<MailAddr, Never>>,
                        NoParent,
                        ChildBindings<
                            CapabilityParent,
                            CapabilityChildTerminal,
                            StructuralOrigins<CapabilityParent>,
                        >,
                        StructuralOrigins<CapabilityParent>,
                    >::new_with_bindings(
                        ApplicationCapabilityInputs {
                            address: MailAddr::APPLICATION_ROOT,
                            actor_spaces,
                            allocations,
                            control,
                            timers,
                            observations,
                            terminal_reports,
                        },
                        ChildBindings::<
                            CapabilityParent,
                            CapabilityChildTerminal,
                            StructuralOrigins<CapabilityParent>,
                        >::default(),
                    );
                    ActionInterpreter::new((
                        Some(capabilities),
                        receiving_activation,
                        receiving_preparation,
                        None,
                    ))
                },
                |environment| {
                    let (publication, startup) = oneshot::channel();
                    let environment = environment.publish_with(move |actor| {
                        let admitted = publication.send(actor);
                        match admitted {
                            Ok(()) => {}
                            Err(actor) => drop(actor),
                        }
                    });
                    (environment, startup, ())
                },
            );
        let original_root_task = task.id();
        // The initial source admission is transitive Driver work before publication.
        // Both background tasks remain owned by the original retirement path.
        let publication = startup.await;
        let joined = task.await;
        let notification = termination_notification
            .await
            .expect("the actual termination producer transferred its result");
        notification.expect("the ordinary termination publication succeeded");
        drop(authority);
        let outcome = match joined {
            Ok(outcome) => outcome,
            Err(error) => panic!("unexpected raw root fault {error:?}"),
        };
        let (retirement, acquired) = ActorRetirement::from_capability_local(outcome);
        let Some((returned_activation, returned_preparation)) = acquired else {
            panic!("normal joined residual owns its actual lane product")
        };
        assert!(Arc::ptr_eq(&activation_lane, &returned_activation));
        assert!(Arc::ptr_eq(&preparation_lane, &returned_preparation));
        drop((returned_activation, returned_preparation));
        let (
            mut behavior,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            additional_failures,
            settlements,
            control,
            user,
            mut descendants,
            child_failures,
            mut capability_failures,
            unread,
            disposition,
        ) = match retirement {
            ActorRetirement::Completed {
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                completion,
            } => (
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                Ok(completion),
            ),
            ActorRetirement::CapabilityFailed {
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                error,
            } => (
                behavior,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
                additional_failures,
                settlements,
                control,
                user,
                descendants,
                child_failures,
                capability_failures,
                unread_owner_cancellation,
                Err(error),
            ),
            other => {
                drop(other);
                panic!(
                    "actual source arbitration returns Stop or the original capability task failure"
                )
            }
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(additional_failures.is_empty());
        // The source port deliberately arbitrates control and task completion
        // without priority. Preserve its actual primary cause, never predict it.
        match disposition {
            Ok(completion) => {
                assert_eq!(completion, Completion::Stopped);
                assert_eq!(behavior.received.len(), 1);
                assert_eq!(control.len(), 1);
                assert_eq!(settlements.len(), 2);
                assert_eq!(capability_failures.len(), 2);
            }
            Err(failure) => {
                assert!(failure.is_panic());
                assert_eq!(behavior.received.len(), 0);
                assert_eq!(control.len(), 2);
                assert_eq!(settlements.len(), 1);
                assert_eq!(capability_failures.len(), 1);
                // This is the exact separately acquired primary JoinError;
                // append it to the outside receiving collection once.
                capability_failures.push(failure);
            }
        }
        assert_eq!(behavior.original.as_slice(), [43, 47]);
        assert!(behavior.child.is_none());
        assert!(behavior.activation.is_none());
        assert!(behavior.preparation.is_none());
        assert_eq!(behavior.creation, creation);
        assert!(publication.is_err());
        assert_eq!(user.len(), 0);
        assert!(unread.is_none());
        let mut observed_inputs = mem::take(&mut behavior.received).into_iter().chain(control);
        let activation_input = observed_inputs
            .next()
            .expect("one original synchronous Started input");
        match activation_input {
            CapabilityEvent::Activation(report) => {
                assert_eq!(report.worker(), expected_worker);
                let started = proxy.on(report).unwrap_or_else(|_| {
                    panic!("original proxy admits the exact actually received grant")
                });
                activation_declaration::assert_proxy_actions(&started, 0, &[], 0, 0, 0);
            }
            CapabilityEvent::User(user) => match user.message {},
            CapabilityEvent::Preparation(report) => {
                drop(report);
                panic!("unexpected completed preparation before the original Started")
            }
            CapabilityEvent::ActivationSource(receipt) => {
                drop(receipt);
                panic!("unexpected activation source receipt before the original Started")
            }
            CapabilityEvent::PreparationSource(_) | CapabilityEvent::Shutdown(_) => {
                panic!("the actual synchronous Started precedes source admission")
            }
        }
        let preparation_start = match observed_inputs
            .next()
            .expect("the actual preparation start returns to its source")
        {
            CapabilityEvent::PreparationSource(SettledItem::Attempted(
                ItemSettlement::Accepted(started),
            )) => started,
            CapabilityEvent::User(user) => match user.message {},
            CapabilityEvent::Preparation(report) => {
                drop(report);
                panic!("unexpected completed preparation instead of its original start receipt")
            }
            CapabilityEvent::ActivationSource(receipt) => {
                drop(receipt);
                panic!(
                    "unexpected activation source receipt instead of the original preparation start"
                )
            }
            CapabilityEvent::Activation(_)
            | CapabilityEvent::PreparationSource(_)
            | CapabilityEvent::Shutdown(_) => {
                panic!("the whole original accepted preparation start remains unread")
            }
        };
        let extra_input = observed_inputs.next();
        assert!(extra_input.is_none());
        let (failures, ()) = child_failures;
        assert_eq!(failures.len(), 0);
        assert_eq!(descendants.len(), 1);
        let child = descendants
            .pop()
            .expect("whole genuine child result after parent join");
        let ActorRetirement::Completed {
            interpretation: child_retirement_interpretation,
            source: child_retirement_source,
            received_interpretation: child_retirement_received_interpretation,
            received_source: child_retirement_received_source,
            source_index: child_retirement_source_index,
            acquired_ingress: child_retirement_acquired_ingress,
            terminal_report: child_retirement_terminal_report,
            retirement_failures: child_retirement_native_failures,
            additional_failures: child_retirement_additional_failures,
            behavior: child_behavior,
            settlements: child_settlements,
            control: child_control,
            user: child_user,
            descendants: child_descendants,
            child_failures: (),
            capability_failures: child_capabilities,
            unread_owner_cancellation: child_unread,
            completion: child_completion,
        } = child.retirement
        else {
            panic!("pure stop leaf has its original completed result")
        };
        assert!(child_retirement_interpretation.is_none());
        assert!(child_retirement_source.is_none());
        assert!(child_retirement_received_interpretation.is_none());
        assert!(child_retirement_received_source.is_none());
        assert!(child_retirement_source_index.is_none());
        assert!(child_retirement_acquired_ingress.is_none());
        assert!(child_retirement_terminal_report.is_none());
        assert!(child_retirement_native_failures.is_empty());
        assert!(child_retirement_additional_failures.is_empty());

        assert_eq!(child_behavior.original.as_slice(), [31, 37]);
        assert_eq!(child_completion, Completion::Stopped);
        assert_eq!(child_settlements.len(), 1);
        assert_eq!(child_control.len(), 0);
        assert_eq!(child_user.len(), 0);
        assert_eq!(child_descendants.len(), 0);
        assert_eq!(child_capabilities.len(), 0);
        // The standard parent drain issues cancellation to each child before
        // joining. Depending on whether this stopped child had already retired,
        // its exact unread request is absent or is the accepted original unit.
        // Both are explicitly discharged here, never reclassified as its cause.
        match child_unread {
            None | Some(()) => {}
        }
        for settlement in &child_settlements {
            assert_eq!(settlement.creations.len(), 0);
            assert_eq!(settlement.sends, NoSends);
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
        }
        // Stop adds one actual front settlement; a primary task failure does
        // not. In either case preserve the whole original initialization row.
        let mut rows = settlements.into_iter();
        let first = rows
            .next()
            .expect("the whole original initialization remains");
        let initial = match &first.become_ {
            Step::Stop(Stopped) => {
                assert_eq!(first.sends.owned.len(), 0);
                let stopped_preparation = first.sends.inner.into_inputs();
                assert_eq!(stopped_preparation.len(), 0);
                let CreationSettlement::Settled(empty_births) = first.creations.into_settlement()
                else {
                    panic!("actual empty stop creation settlement")
                };
                assert_eq!(empty_births.len(), 0);
                drop((stopped_preparation, empty_births));
                rows.next()
                    .expect("original initialization follows the actual Stop row")
            }
            Step::Continue => first,
            Step::Goto(phase) => match *phase {},
        };
        let extra = rows.next();
        assert!(extra.is_none());
        assert!(matches!(initial.become_, Step::Continue));
        assert_eq!(initial.sends.owned.len(), 1);
        let mut initial_activation = initial.sends.owned.into_iter();
        let accepted_activation = initial_activation.next();
        let Some(SettledItem::Attempted(ItemSettlement::Accepted(()))) = accepted_activation else {
            panic!("the original activation unit acceptance remains before its source discharge")
        };
        let extra_activation = initial_activation.next();
        assert!(extra_activation.is_none());
        let initial_preparation = initial.sends.inner.into_inputs();
        assert_eq!(initial_preparation.len(), 0);
        let CreationSettlement::Settled(births) = initial.creations.into_settlement() else {
            panic!("actual committed creation row")
        };
        assert_eq!(births.len(), 1);
        let birth = births
            .into_iter()
            .next()
            .expect("one actual attempted birth");
        let SettledItem::Attempted(ItemSettlement::Accepted(ChildChoice::Head(
            ChildCreationOutcome::Established(committed),
        ))) = birth
        else {
            panic!("exact original committed child")
        };
        assert_eq!(committed.id(), creation);
        assert_eq!(committed.kind(), CreationKind::Birth);
        let mut endpoint_conversion = ExtractLocalEndpoint;
        let child_endpoint = committed
            .actor()
            .recipient()
            .interpret(&mut endpoint_conversion);
        assert_eq!(child_endpoint.address(), child.origin.address());
        assert_eq!(capability_failures.len(), 2);
        assert!(capability_failures.iter().all(JoinError::is_panic));
        let ids: Vec<_> = capability_failures.iter().map(JoinError::id).collect();
        assert_ne!(ids[0], ids[1]);
        assert!(ids.iter().all(|id| *id != original_root_task));
        let activation_cells = activation_lane
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let preparation_cells = preparation_lane
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        assert_eq!(activation_cells.len(), 1);
        assert_eq!(preparation_cells.len(), 1);
        let activation = activation_cells[0].lock().await;
        let activation_task: Id = match &activation.task {
            WorkerCapabilityTask::Started(id) => id.to_owned(),
            WorkerCapabilityTask::Unpolled => {
                panic!("the original activation task actually polled")
            }
        };
        assert!(ids.contains(&activation_task));
        assert_eq!(activation.permit.worker(), expected_worker);
        assert_eq!(
            activation.resumed_failure_origin,
            Some(WorkerCapabilityOperation::Execution)
        );
        assert_eq!(activation.failures.len(), 0);
        assert!(activation.event.is_none());
        let mut observed = ActivationEndpoint(Arc::new(Vec::new()));
        activation.permit.target().interpret(&mut observed);
        assert_eq!(observed.0.as_slice(), [107, 109]);
        assert_eq!(Arc::as_ptr(&observed.0), endpoint_original.as_ptr());
        drop((observed, activation));
        let mut preparation = preparation_cells[0].lock().await;
        let preparation_task: Id = match &preparation.task {
            WorkerCapabilityTask::Started(id) => id.to_owned(),
            WorkerCapabilityTask::Unpolled => {
                panic!("the original preparation task actually polled")
            }
        };
        assert!(ids.contains(&preparation_task));
        assert_ne!(activation_task, preparation_task);
        assert_eq!(
            preparation.resumed_failure_origin,
            Some(WorkerCapabilityOperation::Execution)
        );
        assert_eq!(preparation.failures.len(), 0);
        assert!(preparation.event.is_none());
        let Some(WorkerPreparationPhase::Pending(pending)) = &mut preparation.phase else {
            panic!("whole prepared prefix and current/tail remain after later panic")
        };
        let (source, role) = pending.source_and_role();
        let original_source = originals[0].upgrade().expect("original source live");
        assert_eq!(source.material.as_slice(), [40, 41]);
        assert_eq!(Arc::as_ptr(&source.material), Arc::as_ptr(&original_source));
        drop(original_source);
        assert_eq!(source.requested_roles, vec![vec![10], vec![20]]);
        assert!(source.first_worker.is_none());
        assert_eq!(role.0.as_slice(), [20]);
        assert_eq!(Arc::as_ptr(&role.0), originals[3].as_ptr());
        drop(preparation);
        // Normal and unread control inputs are typed policy observations, not
        // extra copies of a native cause. Their complete values are surrendered
        // together only after the actual parent and child task owners join.
        drop((
            proxy,
            preparation_start,
            observed_inputs,
            initial_preparation,
            initial_activation,
            behavior,
            child_behavior,
            child_settlements,
            child_endpoint,
            committed,
            publication,
        ));
        let retained: Vec<_> = originals.iter().map(Weak::strong_count).collect();
        let endpoint_retained = endpoint_original.strong_count();
        let cause_retained = cause_original.strong_count();
        drop(capability_failures);
        let cause_released = cause_original.strong_count();
        drop((
            activation_cells,
            preparation_cells,
            activation_lane,
            preparation_lane,
        ));
        let released: Vec<_> = originals.iter().map(Weak::strong_count).collect();
        let endpoint_released = endpoint_original.strong_count();
        let parent_released = parent_original.strong_count();
        let child_released = child_original.strong_count();
        assert_eq!(retained, vec![1, 1, 1, 1, 1]);
        assert_eq!(endpoint_retained, 1);
        assert_eq!(cause_retained, 1);
        assert_eq!(cause_released, 0);
        assert_eq!(released, vec![0, 0, 0, 0, 0]);
        assert_eq!(endpoint_released, 0);
        assert_eq!(parent_released, 0);
        assert_eq!(child_released, 0);
    }
}

#[cfg(test)]
impl<E, Descendants> CapabilityRetirement<E, Descendants> {
    pub(crate) fn without_activations(descendants: Descendants) -> Self {
        Self {
            activation_tasks: ActivationTasks::new(),
            descendants,
            terminal_report: None,
            retirement_failures: Vec::new(),
        }
    }
}

#[cfg(test)]
impl<B, Inner, Activation, Preparation> CommitActions<B>
    for ActionInterpreter<(
        Option<Inner>,
        Activation,
        Preparation,
        Option<CapabilityRetirement<B::Event, Inner::Descendants>>,
    )>
where
    Inner: RetireCapabilities<Event = B::Event> + TerminalReportTransaction + Send,
    Inner::Descendants: Send,
    Activation: Send,
    Preparation: Send,
    B::Event: Send,
    B: BehaviorSettlements<
            Ph = Never,
            Settlements = InterpretedActionSettlement<B>,
            InterpretationCustody = <ActionsOf<B> as ActionSettlements>::InterpretationCustody,
            SourceCustody = <ActionsOf<B> as ActionSettlements>::SourceCustody,
        >,
    B::InterpretationCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::InterpretationCustody: Send,
    <B::Sends as SendSettlements>::InterpretationCustody: Send,
    BehaviorAddr<B>: Send,
    B::Birth: InterpretCreations<BehaviorAddr<B>, Self, B::Event, behavior::Here>,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <B::Birth as CreationSettlements<BehaviorAddr<B>>>::Settlements: SourceSettlementCustody<
            Self,
            B::Event,
            Custody = <B::Birth as CreationSettlements<BehaviorAddr<B>>>::SourceCustody,
        > + Send,
    <B::Sends as SendSettlements>::Settlements: SourceSettlementCustody<
            Self,
            B::Event,
            Custody = <B::Sends as SendSettlements>::SourceCustody,
        > + Send,
    B::Sends: InterpretSends<Self, B::Event, behavior::Here> + Send,
    ActionSettlementOf<B>:
        SourceSettlementCustody<Self, B::Event, Custody = B::SourceCustody> + Send,
{
    type Retired = (Inner::Descendants, (Activation, Preparation));

    async fn commit(
        &mut self,
        progress: &mut Option<
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, ActionSettlementOf<B>>,
        >,
    ) {
        let become_ = match progress.as_ref() {
            Some(InterpretationProgress::Original(actions)) => &actions.become_,
            Some(InterpretationProgress::Interpreting((_, become_))) => become_,
            Some(InterpretationProgress::Completed(_)) | None => return,
        };
        let terminal_disposition = match become_ {
            Step::Continue => TerminalReportDisposition::Discard,
            Step::Goto(never) => match *never {},
            Step::Stop(_) => TerminalReportDisposition::Retain,
        };
        ActionsOf::<B>::interpret::<_, B::Event, behavior::Here>(progress, self).await;
        self.capabilities_mut()
            .finish_terminal_reports(terminal_disposition);
    }

    async fn offer_next(
        &mut self,
        progress: &mut Option<SourceProgress<ActionSettlementOf<B>, B::SourceCustody>>,
    ) {
        <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::prepare_source(
            progress,
        );
        if let Some(SourceProgress::Offering(custody)) = progress {
            <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::offer_next_to_source(custody, self).await;
        }
        <ActionSettlementOf<B> as SourceSettlementCustody<Self, B::Event>>::finish_source(progress);
    }

    async fn next_local_event(&mut self) -> Result<B::Event, JoinError> {
        self.capabilities_mut().next_local_event().await
    }
    fn next_deadline(&mut self) -> Option<Instant> {
        self.capabilities_mut().next_deadline()
    }
    fn pop_due(&mut self, now: Instant) -> Option<B::Event> {
        self.capabilities_mut().pop_due(now)
    }
    async fn receive_retirement(
        interpreter: &mut Option<Self>,
        received: &mut Option<
            CapabilityRetirement<B::Event, (Inner::Descendants, (Activation, Preparation))>,
        >,
    ) {
        let Some(owner) = interpreter.as_mut() else {
            return;
        };
        RetireCapabilities::receive_retirement(&mut owner.capabilities, received).await;
        if owner.capabilities.is_none() && received.is_some() {
            drop(interpreter.take());
        }
    }
}
