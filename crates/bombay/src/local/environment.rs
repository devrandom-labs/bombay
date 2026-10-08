use crate::address::MailAddr;
use crate::local::effects::observation::TerminationObservations;
use crate::local::effects::timers::LocalTimers;
use crate::local::effects::{ActionSettlementOf, CapabilityRetirement, CommitActions};
use crate::local::endpoint::ActorRef;
#[cfg(test)]
use crate::local::endpoint::request_actor_shutdown;
use crate::local::execution::{
    ActivationTasks, LocalRetirementRequest, OwnerCancellation, close_owner_cancellation,
};
use crate::local::ingress::{
    Admission, AdmissionClosure, IngressMode, LocalInbox, StandardIngress, collect_retired_ingress,
};
#[cfg(test)]
use crate::local::ingress::{EndpointMailbox, EntityIngress, LocalIngress};
use crate::observe::Observation;
use crate::termination::Termination;
#[cfg(test)]
use behavior::InjectEvent;
use behavior::{
    Behavior, BehaviorAddr, BehaviorMessage, BehaviorSettlements, ClassifySettlement,
    Interpretation, InterpretationProgress, Never, SourceCustody, SourceProgress,
};
use bombay_address::{AddressSpace, ClaimError, Lease, Reservation};
use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
use communication::{Config, Consumer, ControlSender, Drained, Received, mailbox_channel};
use core::future::{pending, poll_fn};
use core::hash::Hash;
use core::ops::ControlFlow;
use core::pin::Pin;
use core::task::Poll;
use std::any::Any;
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::task::JoinError;

impl<B, U, Descendants> LocalResidual<B, U, Descendants>
where
    B: BehaviorSettlements,
    B::Event: Send + 'static,
{
    /// Settle the actual surviving residual without moving its lanes into an attempt.
    pub(crate) async fn receive_activation_tasks(&mut self) {
        let (ingress, activation_tasks, capability_failures) = match self {
            Self::Prepared {
                ingress,
                activation_tasks,
                capability_failures,
                ..
            }
            | Self::Uncommitted {
                ingress,
                activation_tasks,
                capability_failures,
                ..
            }
            | Self::Retired {
                ingress,
                activation_tasks,
                capability_failures,
                ..
            } => (ingress, activation_tasks, capability_failures),
        };
        activation_tasks
            .receive_settlement(&mut ingress.control, capability_failures)
            .await;
    }

    #[cfg(test)]
    pub(crate) async fn settle_activation_tasks(mut self) -> Self {
        self.receive_activation_tasks().await;
        self
    }
}

impl<B, I, M> LocalEnvironment<B, I, M, fn(ActorRef<B::Protocol>)>
where
    B: BehaviorSettlements<Ph = Never>,
    M: IngressMode<B>,
    I: CommitActions<B>,
    BehaviorAddr<B>: Hash,
    BehaviorAddr<B>: Hash,
    B::Event: Send + 'static,
{
    /// Prepare one concrete ingress mode and construct its interpreter with a
    /// clone of the control-lane capability for this exact incarnation.
    pub(crate) fn prepare(
        address: BehaviorAddr<B>,
        addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
        config: Config,
        termination: Observation<Termination<BehaviorAddr<B>>>,
        owner_cancellation: oneshot::Receiver<OwnerCancellation>,
        make_interpreter: impl FnOnce(
            ControlSender<B::Event>,
            LocalTimers<B::Event>,
            TerminationObservations<BehaviorAddr<B>, B::Event>,
        ) -> I,
    ) -> Self {
        let (control_liveness, owner, mailbox, consumer) =
            mailbox_channel::<B::Event, M::Item>(config);
        let control_liveness = Arc::new(control_liveness);
        let admission = Arc::new(Admission::new(owner));
        let endpoint = ActorRef::new(
            address,
            M::endpoint(mailbox, Arc::downgrade(&admission)),
            termination,
        );
        let timers = LocalTimers::new();
        let observations = TerminationObservations::new();
        let interpreter = make_interpreter((*control_liveness).clone(), timers, observations);
        Self {
            address,
            addresses,
            endpoint,
            consumer: Some(consumer),
            admission,
            control_liveness: Some(control_liveness),
            interpreter: Some(interpreter),
            owner_cancellation: Some(owner_cancellation),
            commitment: None,
            publication_notice: PublicationNotice::Unobserved,
            reservation: None,
            acknowledgement: None,
            acknowledged: None,
            initialization: None,
            activation_rejection: None,
            retirement: None,
            retired_ingress: None,
            unread_owner_cancellation: None,
            residual: None,
        }
    }
}

impl<B, I, M, P> LocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never>,
    M: IngressMode<B>,
    I: CommitActions<B>,
    BehaviorAddr<B>: Hash,
{
    pub(crate) fn control(&self) -> ControlSender<B::Event> {
        (**self
            .control_liveness
            .as_ref()
            .expect("the prepared incarnation owns its control lane"))
        .clone()
    }

    pub(crate) fn shutdown_control(&self) -> Weak<ControlSender<B::Event>> {
        Arc::downgrade(
            self.control_liveness
                .as_ref()
                .expect("the prepared incarnation owns its control lane"),
        )
    }

    pub(crate) fn with_private_commitment(
        mut self,
        commitment: oneshot::Sender<(ActorRef<B::Protocol>, oneshot::Sender<()>)>,
    ) -> Self {
        self.commitment = Some(commitment);
        self
    }

    pub(crate) fn publish_with<N>(self, notice: N) -> LocalEnvironment<B, I, M, N> {
        LocalEnvironment {
            address: self.address,
            addresses: self.addresses,
            endpoint: self.endpoint,
            consumer: self.consumer,
            admission: self.admission,
            control_liveness: self.control_liveness,
            interpreter: self.interpreter,
            owner_cancellation: self.owner_cancellation,
            commitment: self.commitment,
            publication_notice: PublicationNotice::Notify(notice),
            reservation: self.reservation,
            acknowledgement: self.acknowledgement,
            acknowledged: self.acknowledged,
            initialization: self.initialization,
            activation_rejection: self.activation_rejection,
            retirement: self.retirement,
            retired_ingress: self.retired_ingress,
            unread_owner_cancellation: self.unread_owner_cancellation,
            residual: self.residual,
        }
    }
}

impl<B, I, M, P> LocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never>,
    B::Event: Send,
    B::Settlements: Send,
    B::InterpretationCustody: Send,
    BehaviorAddr<B>: Hash + Send,
    BehaviorMessage<B>: Send,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    M: IngressMode<B>,
    M::Retired: Send,
    I: CommitActions<B> + Send,
    I::Retired: Send,
    P: Send,
{
    async fn receive_retirement(&mut self) {
        if self.residual.is_some() {
            return;
        }
        if let Some(receiver) = self.owner_cancellation.take() {
            self.unread_owner_cancellation = close_owner_cancellation(receiver);
        }
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        if self.retired_ingress.is_none()
            && let Some(consumer) = self.consumer.take()
        {
            self.retired_ingress = Some(collect_retired_ingress::<B, M>(consumer));
        }
        I::receive_retirement(&mut self.interpreter, &mut self.retirement).await;
        if self.interpreter.is_some() {
            return;
        }
        // Concrete standard mailbox/address/oneshot retirement has no user callback.
        // Selected report refusal is already an acquired outside result.
        drop(self.reservation.take());
        drop(self.acknowledgement.take());
        drop(self.commitment.take());
        drop(self.control_liveness.take());
        match (self.retirement.take(), self.retired_ingress.take()) {
            (
                Some(CapabilityRetirement {
                    activation_tasks,
                    descendants,
                    terminal_report,
                    retirement_failures,
                }),
                Some(ingress),
            ) => {
                self.residual = Some(match self.initialization.take() {
                    Some(initialization) => LocalResidual::Uncommitted {
                        received_interpretation: None,
                        received_source: None,
                        source_index: None,
                        acquired_ingress: None,
                        initialization,
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures: Vec::new(),
                        terminal_report,
                        retirement_failures,
                        unread_owner_cancellation: self.unread_owner_cancellation.take(),
                    },
                    None => LocalResidual::Prepared {
                        received_interpretation: None,
                        received_source: None,
                        source_index: None,
                        acquired_ingress: None,
                        ingress,
                        activation_tasks,
                        descendants,
                        capability_failures: Vec::new(),
                        terminal_report,
                        retirement_failures,
                        unread_owner_cancellation: self.unread_owner_cancellation.take(),
                    },
                });
            }
            (retirement, ingress) => {
                self.retirement = retirement;
                self.retired_ingress = ingress;
            }
        }
    }
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I, M, P> Environment<B> for LocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never> + Send,
    M: IngressMode<B>,
    M::Retired: Send,
    BehaviorAddr<B>: Hash + Clone + Send + Sync,
    B::Event: Send,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    BehaviorMessage<B>: Send,
    I: CommitActions<B> + Send,
    I::Retired: Send,
    P: FnOnce(ActorRef<B::Protocol>) + Send,
    ActionSettlementOf<B>: ClassifySettlement + Send,
    B::InterpretationCustody: Send,
    B::SourceCustody: Send,
{
    type Active = ActiveLocalEnvironment<B, I, M, P>;
    type Settlement = ActionSettlementOf<B>;
    type RetirementRequest = LocalRetirementRequest;
    type Error = LocalActivationRejection<BehaviorAddr<B>>;
    type Residual = LocalResidual<B, M::Retired, I::Retired>;

    #[expect(
        clippy::too_many_lines,
        reason = "one affine activation preserves original address rejection, private acknowledgement, pending input and received result at every owning cut"
    )]
    async fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = environment.as_mut() else {
            return;
        };
        match (&owner.initialization, actions.as_ref()) {
            (Some(_), Some(_)) | (None, None) => return,
            (None, Some(_)) => {
                owner.initialization = actions.take().map(InterpretationProgress::Original);
            }
            (Some(_), None) => {}
        }
        if owner.residual.is_some() {
            return;
        }
        if owner.activation_rejection.is_none() && owner.reservation.is_none() {
            match owner.addresses.try_reserve(owner.address) {
                Ok(reservation) => owner.reservation = Some(reservation),
                Err(error) => {
                    owner.activation_rejection = Some(LocalActivationRejection::Address(error));
                }
            }
        }
        if owner.activation_rejection.is_none() {
            if let Some(commitment) = owner.commitment.take() {
                let (acknowledge, acknowledged) = oneshot::channel();
                owner.acknowledgement = Some(acknowledged);
                match commitment.send((owner.endpoint.clone(), acknowledge)) {
                    Ok(()) => {}
                    Err(unaccepted_commitment) => {
                        // Existing BindingAbandoned policy discharges only this
                        // never-published endpoint clone and returned ACK sender.
                        drop(unaccepted_commitment);
                        owner.activation_rejection =
                            Some(LocalActivationRejection::BindingAbandoned);
                    }
                }
            }
            if owner.activation_rejection.is_none() && owner.acknowledgement.is_some() {
                poll_fn(|context| {
                    let acknowledgement = owner
                        .acknowledgement
                        .as_mut()
                        .expect("the same private ACK remains borrowed");
                    match Pin::new(acknowledgement).poll(context) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(acknowledged) => {
                            owner.acknowledged = Some(acknowledged);
                            Poll::Ready(())
                        }
                    }
                })
                .await;
                drop(owner.acknowledgement.take());
                if matches!(owner.acknowledged, Some(Err(_))) {
                    owner.activation_rejection = Some(LocalActivationRejection::BindingAbandoned);
                }
            }
        }
        if owner.activation_rejection.is_some() {
            owner.receive_retirement().await;
            let Some(owner) = environment.take() else {
                return;
            };
            match owner {
                Self {
                    activation_rejection: Some(error),
                    residual: Some(residual),
                    consumer: None,
                    interpreter: None,
                    retirement: None,
                    retired_ingress: None,
                    ..
                } => *received = Some(Err((error, residual))),
                owner => *environment = Some(owner),
            }
            return;
        }
        let Some(interpreter) = owner.interpreter.as_mut() else {
            return;
        };
        interpreter.commit(&mut owner.initialization).await;
        // An incomplete unit return retains the actual phase for retirement.
        let Some(owner) = environment.take() else {
            return;
        };
        match owner {
            Self {
                consumer: Some(consumer),
                interpreter: Some(interpreter),
                initialization: Some(InterpretationProgress::Completed(interpretation)),
                reservation: Some(reservation),
                control_liveness: Some(control_liveness),
                owner_cancellation: Some(owner_cancellation),
                acknowledgement: None,
                activation_rejection: None,
                retirement: None,
                retired_ingress: None,
                residual: None,
                endpoint,
                admission,
                publication_notice,
                ..
            } => {
                let active = ActiveLocalEnvironment {
                    inbox: LocalInbox::new(consumer),
                    admission,
                    control_liveness: Some(control_liveness),
                    interpreter: Some(interpreter),
                    owner_cancellation: Some(owner_cancellation),
                    publication: Some(Publication::Pending {
                        reservation,
                        publication_notice,
                        endpoint,
                    }),
                    lease: None,
                    interpretation: None,
                    source: None,
                    received_interpretation: None,
                    received_source: None,
                    source_index: None,
                    acquired_ingress: None,
                    retirement: None,
                    retired_ingress: None,
                    unread_owner_cancellation: None,
                };
                *received = Some(Ok((active, interpretation)));
            }
            owner => *environment = Some(owner),
        }
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = environment.as_mut() else {
            return;
        };
        if actions.is_some() {
            if owner.initialization.is_some() || owner.residual.is_some() {
                return;
            }
            owner.initialization = actions.take().map(InterpretationProgress::Original);
        }
        owner.receive_retirement().await;
        let Some(owner) = environment.take() else {
            return;
        };
        match owner {
            Self {
                residual: Some(residual),
                consumer: None,
                interpreter: None,
                retirement: None,
                retired_ingress: None,
                ..
            } => *received = Some(residual),
            owner => *environment = Some(owner),
        }
    }
}

#[allow(
    refining_impl_trait,
    reason = "Bombay's concrete Tokio environment refines executor-neutral Engine futures"
)]
impl<B, I, M, P> ActiveEnvironment<B> for ActiveLocalEnvironment<B, I, M, P>
where
    B: BehaviorSettlements<Ph = Never> + Send,
    M: IngressMode<B>,
    M::Retired: Send,
    BehaviorAddr<B>: Hash + Send + Sync,
    B::Event: Send,
    B::Sends: Send,
    <B::Birth as behavior::BirthMode>::Child: Send,
    <BehaviorAddr<B> as behavior::Address>::Nonce: Send,
    BehaviorMessage<B>: Send,
    I: CommitActions<B> + Send,
    I::Retired: Send,
    P: FnOnce(ActorRef<B::Protocol>) + Send,
    ActionSettlementOf<B>: ClassifySettlement + Send,
    B::InterpretationCustody: Send,
    B::SourceCustody: Send,
{
    type Settlement = ActionSettlementOf<B>;
    type RetirementRequest = LocalRetirementRequest;
    type Residual = LocalResidual<B, M::Retired, I::Retired>;

    // Ready owner cancellation precedes ingress and capability event acquisition.
    // It moves once into the Driver's disposition; queued ingress remains owned.
    async fn next(&mut self) -> ControlFlow<LocalRetirementRequest, Option<B::Event>> {
        enum Acquired<E, U> {
            Mailbox(Option<Received<E, U>>),
            Local(Result<E, JoinError>),
            Deadline,
            OwnerCancellation(Option<OwnerCancellation>),
        }

        loop {
            let cancellation = async {
                match self.owner_cancellation.as_mut() {
                    Some(receiver) => receiver.await.ok(),
                    None => pending().await,
                }
            };
            let acquired = if let Some(deadline) = self
                .interpreter
                .as_mut()
                .expect("the active incarnation owns its interpreter")
                .next_deadline()
            {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.interpreter.as_mut().expect("the active incarnation owns its interpreter").next_local_event() => Acquired::Local(event),
                    () = tokio::time::sleep_until(deadline.into()) => Acquired::Deadline,
                }
            } else {
                tokio::select! {
                    biased;
                    cancellation = cancellation => Acquired::OwnerCancellation(cancellation),
                    received = self.inbox.recv() => Acquired::Mailbox(received),
                    event = self.interpreter.as_mut().expect("the active incarnation owns its interpreter").next_local_event() => Acquired::Local(event),
                }
            };
            let received = match acquired {
                Acquired::OwnerCancellation(Some(cancellation)) => {
                    self.owner_cancellation = None;
                    return ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(
                        cancellation,
                    ));
                }
                Acquired::OwnerCancellation(None) => {
                    self.owner_cancellation = None;
                    continue;
                }
                Acquired::Local(Ok(event)) => return ControlFlow::Continue(Some(event)),
                Acquired::Local(Err(failure)) => {
                    return ControlFlow::Break(LocalRetirementRequest::CapabilityFailed(failure));
                }
                Acquired::Deadline => {
                    if let Some(event) = self
                        .interpreter
                        .as_mut()
                        .expect("the active incarnation owns its interpreter")
                        .pop_due(Instant::now())
                    {
                        return ControlFlow::Continue(Some(event));
                    }
                    continue;
                }
                Acquired::Mailbox(Some(received)) => received,
                Acquired::Mailbox(None) => return ControlFlow::Continue(None),
            };
            match received {
                Received::Control(event) => return ControlFlow::Continue(Some(event)),
                Received::User(item) => {
                    if let Some(event) = M::into_event(item) {
                        return ControlFlow::Continue(Some(event));
                    }
                }
                Received::UserLaneClosed => {
                    // This closes only the bounded user lane. Control events
                    // remain admissible until the complete consumer closes.
                }
            }
        }
    }

    // The same owner request precedes the next source fold. An interpretation
    // already in flight completes before this acquisition boundary is reached.
    async fn next_source(&mut self) -> ControlFlow<LocalRetirementRequest, Option<B::Event>> {
        loop {
            let cancellation = async {
                match self.owner_cancellation.as_mut() {
                    Some(receiver) => receiver.await.ok(),
                    None => pending().await,
                }
            };
            tokio::select! {
                biased;
                request = cancellation => {
                    self.owner_cancellation = None;
                    if let Some(request) = request {
                        return ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(request));
                    }
                }
                event = async {
                    // Preserve the existing unbiased source/capability arbitration.
                    tokio::select! {
                        event = self.inbox.recv_source() => Ok(event),
                        event = self.interpreter.as_mut().expect("the active incarnation owns its interpreter").next_local_event() => event.map(Some),
                    }
                } => return match event {
                    Ok(event) => ControlFlow::Continue(event),
                    Err(failure) => ControlFlow::Break(LocalRetirementRequest::CapabilityFailed(failure)),
                },
            }
        }
    }

    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<B>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        match (&self.interpretation, actions.as_ref()) {
            (Some(_), Some(_)) | (None, None) => return,
            (None, Some(_)) => {
                self.interpretation = actions.take().map(InterpretationProgress::Original);
            }
            (Some(_), None) => {}
        }
        let Some(interpreter) = self.interpreter.as_mut() else {
            return;
        };
        interpreter.commit(&mut self.interpretation).await;
        match self.interpretation.take() {
            Some(InterpretationProgress::Completed(interpretation)) => {
                *received = Some(interpretation);
            }
            interpretation => self.interpretation = interpretation,
        }
    }

    async fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        match (&self.source, settlement.as_ref()) {
            (Some(_), Some(_)) | (None, None) => return,
            (None, Some(_)) => self.source = settlement.take().map(SourceProgress::Original),
            (Some(_), None) => {}
        }
        let Some(interpreter) = self.interpreter.as_mut() else {
            return;
        };
        interpreter.offer_next(&mut self.source).await;
        match self.source.take() {
            Some(SourceProgress::Completed(source)) => *received = Some(source),
            source => self.source = source,
        }
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        if let Some(receiver) = self.owner_cancellation.as_mut() {
            match receiver.try_recv() {
                Ok(request) => {
                    self.owner_cancellation = None;
                    return ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(request));
                }
                Err(TryRecvError::Closed) => self.owner_cancellation = None,
                Err(TryRecvError::Empty) => {}
            }
        }
        let publication = self.publication.take();
        match publication {
            Some(Publication::Pending {
                reservation,
                publication_notice,
                endpoint,
            }) => {
                self.lease = Some(reservation.publish(endpoint.clone()));
                self.publication = Some(Publication::Published);
                if let PublicationNotice::Notify(notice) = publication_notice {
                    notice(endpoint);
                }
            }
            Some(Publication::Published) => {
                self.publication = Some(Publication::Published);
                unreachable!("the Driver publishes one installed incarnation exactly once")
            }
            None => unreachable!("publication requires its original pending or published fact"),
        }
        ControlFlow::Continue(())
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<B>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<ControlFlow<Self::RetirementRequest, Option<B::Event>>>,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = environment.as_mut() else {
            return;
        };
        if settlements.is_none() {
            return;
        }
        // Independent coexisting originals are incomplete. Preserve both owners,
        // rather than replacing either slot or calling them a completed receipt.
        if (owner.interpretation.is_some() && actions.is_some())
            || (owner.received_interpretation.is_some() && interpretation.is_some())
            || (owner.received_source.is_some() && source.is_some())
            || (owner.source_index.is_some() && source_index.is_some())
            || (owner.acquired_ingress.is_some() && ingress.is_some())
        {
            return;
        }
        if actions.is_some() {
            owner.interpretation = actions.take().map(InterpretationProgress::Original);
        }
        if owner.received_interpretation.is_none() {
            owner.received_interpretation = interpretation.take();
        }
        if owner.received_source.is_none() {
            owner.received_source = source.take();
        }
        if owner.source_index.is_none() {
            owner.source_index = source_index.take();
        }
        if owner.acquired_ingress.is_none() {
            owner.acquired_ingress = ingress.take();
        }
        if let Some(receiver) = owner.owner_cancellation.take() {
            owner.unread_owner_cancellation = close_owner_cancellation(receiver);
        }
        match owner.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        if owner.retired_ingress.is_none() {
            owner.retired_ingress = owner.inbox.drain();
        }
        I::receive_retirement(&mut owner.interpreter, &mut owner.retirement).await;
        if owner.interpreter.is_some() {
            return;
        }
        // These concrete standard owners retire before the semantic result.
        drop(owner.control_liveness.take());
        drop(owner.lease.take());
        drop(owner.publication.take());
        let Some(owner) = environment.take() else {
            return;
        };
        match owner {
            Self {
                interpreter: None,
                retirement:
                    Some(CapabilityRetirement {
                        activation_tasks,
                        descendants,
                        terminal_report,
                        retirement_failures,
                    }),
                retired_ingress: Some(ingress),
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                unread_owner_cancellation,
                ..
            } => {
                let original_settlements = settlements
                    .take()
                    .expect("the original settlement history remained outside all waits");
                *received = Some(LocalResidual::Retired {
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    settlements: original_settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                    capability_failures: Vec::new(),
                    terminal_report,
                    retirement_failures,
                    unread_owner_cancellation,
                });
            }
            owner => *environment = Some(owner),
        }
    }
}

pub(crate) enum LocalResidual<B: BehaviorSettlements, U, Descendants = ()> {
    Prepared {
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        ingress: Drained<B::Event, U>,
        activation_tasks: ActivationTasks<B::Event>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        unread_owner_cancellation: Option<()>,
    },
    Uncommitted {
        initialization:
            InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        ingress: Drained<B::Event, U>,
        activation_tasks: ActivationTasks<B::Event>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        unread_owner_cancellation: Option<()>,
    },
    Retired {
        interpretation:
            Option<InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>>,
        source: Option<SourceProgress<B::Settlements, B::SourceCustody>>,
        settlements: Vec<B::Settlements>,
        received_interpretation: Option<Interpretation<B::Settlements>>,
        received_source: Option<SourceCustody<B::Settlements>>,
        source_index: Option<usize>,
        acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
        ingress: Drained<B::Event, U>,
        activation_tasks: ActivationTasks<B::Event>,
        descendants: Descendants,
        capability_failures: Vec<JoinError>,
        terminal_report: Option<Result<(), Termination<MailAddr>>>,
        retirement_failures: Vec<Box<dyn Any + Send>>,
        unread_owner_cancellation: Option<()>,
    },
}

/// Exact failure selected by the concrete local activation owner.
///
/// The address rejection, abandoned private binding, and original native
/// commitment panic remain distinct, including in ordered later Driver failures.
#[derive(Debug)]
pub enum LocalActivationRejection<A> {
    Address(ClaimError<A>),
    BindingAbandoned,
    /// Commitment unwound; the actual original partial initialization remains in the residual.
    HostCommitPanicked(Box<dyn Any + Send>),
}

/// The prepared local half of one mailbox-backed behavior generation.
pub(crate) struct LocalEnvironment<
    B: BehaviorSettlements<Ph = Never>,
    I,
    M = StandardIngress,
    P = fn(ActorRef<<B as Behavior>::Protocol>),
> where
    M: IngressMode<B>,
    I: CommitActions<B>,
    BehaviorAddr<B>: Hash,
{
    address: BehaviorAddr<B>,
    addresses: AddressSpace<BehaviorAddr<B>, ActorRef<B::Protocol>>,
    endpoint: ActorRef<B::Protocol>,
    consumer: Option<Consumer<B::Event, M::Item>>,
    admission: Arc<Admission<M::Item>>,
    control_liveness: Option<Arc<ControlSender<B::Event>>>,
    interpreter: Option<I>,
    owner_cancellation: Option<oneshot::Receiver<OwnerCancellation>>,
    #[allow(
        clippy::type_complexity,
        reason = "the private binding transports one exact actor reference and its affine acknowledgement"
    )]
    commitment: Option<oneshot::Sender<(ActorRef<B::Protocol>, oneshot::Sender<()>)>>,
    publication_notice: PublicationNotice<P>,
    reservation: Option<Reservation<BehaviorAddr<B>, ActorRef<B::Protocol>>>,
    acknowledgement: Option<oneshot::Receiver<()>>,
    acknowledged: Option<Result<(), oneshot::error::RecvError>>,
    initialization:
        Option<InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>>,
    activation_rejection: Option<LocalActivationRejection<BehaviorAddr<B>>>,
    retirement: Option<CapabilityRetirement<B::Event, I::Retired>>,
    retired_ingress: Option<Drained<B::Event, M::Retired>>,
    unread_owner_cancellation: Option<()>,
    residual: Option<LocalResidual<B, M::Retired, I::Retired>>,
}

enum PublicationNotice<P> {
    Unobserved,
    Notify(P),
}

enum Publication<P, A: Eq + Hash, Endpoint> {
    Pending {
        reservation: Reservation<A, Endpoint>,
        publication_notice: PublicationNotice<P>,
        endpoint: Endpoint,
    },
    Published,
}

/// The only local value with mailbox ingress and address ownership.
pub(crate) struct ActiveLocalEnvironment<
    B: BehaviorSettlements<Ph = Never>,
    I,
    M = StandardIngress,
    P = fn(ActorRef<<B as Behavior>::Protocol>),
> where
    BehaviorAddr<B>: Hash,
    BehaviorMessage<B>: Send,
    M: IngressMode<B>,
    I: CommitActions<B>,
    BehaviorAddr<B>: Hash,
{
    inbox: LocalInbox<B, M>,
    admission: Arc<Admission<M::Item>>,
    control_liveness: Option<Arc<ControlSender<B::Event>>>,
    interpreter: Option<I>,
    owner_cancellation: Option<oneshot::Receiver<OwnerCancellation>>,
    publication: Option<Publication<P, BehaviorAddr<B>, ActorRef<B::Protocol>>>,
    lease: Option<Lease<BehaviorAddr<B>, ActorRef<B::Protocol>>>,
    interpretation:
        Option<InterpretationProgress<ActionsOf<B>, B::InterpretationCustody, B::Settlements>>,
    source: Option<SourceProgress<B::Settlements, B::SourceCustody>>,
    received_interpretation: Option<Interpretation<B::Settlements>>,
    received_source: Option<SourceCustody<B::Settlements>>,
    source_index: Option<usize>,
    acquired_ingress: Option<ControlFlow<LocalRetirementRequest, Option<B::Event>>>,
    retirement: Option<CapabilityRetirement<B::Event, I::Retired>>,
    retired_ingress: Option<Drained<B::Event, M::Retired>>,
    unread_owner_cancellation: Option<()>,
}

#[cfg(test)]
use core::pin::pin;

#[cfg(test)]
mod tests {
    use core::task::{Context, Waker};
    use std::convert::Infallible;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use behavior::{
        ActionItem, ActionSettlement, Actions, ActiveTurn, BehaviorActed, EventIngress, EventLayer,
        Here, Ingress, InitializationTurn, InterpretItem, InterpreterFault, InterpreterRequest,
        InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoBirthProtocols, NoBirths,
        NoReturnToEmitter, NoSends, Own, SettledItem, SettlementStatus, SourceAdmission, Step,
        User, UserEvent, finish_item, prepare_item,
    };
    use behavior_actors::{
        Crash, Exit, PeerStopped, ScheduleAt, ShutdownRejection, ShutdownRequested, StopOnShutdown,
        TimerElapsed, TimerGeneration, TimerId,
    };
    use communication::{Config, mailbox_channel};

    use crate::address::ApplicationAddresses;
    use crate::launch::ActorSpace;
    use crate::local::children::NoChildBindings;
    use crate::local::effects::ActionInterpreter;
    use crate::local::effects::LocalTerminalReports;
    use crate::local::effects::{ApplicationCapabilities, ApplicationCapabilityInputs};
    use crate::local::endpoint::InstalledActor;
    use crate::observe::{self, pair};
    use crate::{ActorExecutionOutcome, MailAddr};
    use bombay_engine::{Completion, Driver, DriverRetirement};
    use tokio::sync::oneshot::error::TryRecvError;
    use tokio::task;

    use super::*;

    struct FenceProbe;

    impl Behavior for FenceProbe {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[test]
    fn standard_and_entity_ingress_preserve_exact_user_and_retired_payload() {
        let address = MailAddr(7);
        let standard =
            <StandardIngress as IngressMode<FenceProbe>>::into_event(User::new(address, ()));
        assert_eq!(standard, Some(User::new(address, ())));

        let entity = <EntityIngress as IngressMode<FenceProbe>>::into_event(LocalIngress::Message(
            User::new(address, ()),
        ));
        assert_eq!(entity, Some(User::new(address, ())));

        let retired = <EntityIngress as IngressMode<FenceProbe>>::retain_at_retirement(
            LocalIngress::Message(User::new(address, ())),
        );
        assert_eq!(retired, Some(User::new(address, ())));
    }

    #[test]
    fn exact_actor_and_installed_authority_debug_identify_their_address() {
        let (control, mailbox_owner, mailbox, receiver) =
            mailbox_channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(1));
        let admission = Arc::new(Admission::new(mailbox_owner));
        let (publisher, observation) = pair();
        let actor = ActorRef::<MessageProtocol<MailAddr, ()>>::external(
            MailAddr(73),
            mailbox,
            Arc::downgrade(&admission),
            observation,
        );
        let installed = InstalledActor::<FenceProbe>::new(actor.clone(), control);

        let actor_description = format!("{actor:?}");
        assert!(actor_description.contains("ActorRef"));
        assert!(actor_description.contains("MailAddr(73)"));
        let installed_description = format!("{installed:?}");
        assert!(installed_description.contains("InstalledActor"));
        assert!(installed_description.contains("MailAddr(73)"));
        drop((publisher, admission, receiver));
    }

    type ActivationEvent = EventLayer<ShutdownRequested, User<MailAddr, ()>>;

    #[derive(Debug, PartialEq, Eq)]
    struct InitializationRequest(u8);

    #[derive(Debug, PartialEq, Eq)]
    struct InitializationReceipt(u8);

    #[derive(Debug, PartialEq, Eq)]
    enum InitializationRejection {
        Refused,
    }

    impl ActionItem for InitializationRequest {
        type Accepted = InitializationReceipt;
        type Rejection = InitializationRejection;
        type Prerequisite = Never;
        type Custody = (Option<Self>, Option<Self::Reply>);
        type Input<'a>
            = &'a mut Option<Self>
        where
            Self: 'a;
        type Reply = ItemSettlement<Self, Self::Accepted, Self::Rejection, Self::Prerequisite>;

        fn prepare_interpretation(
            progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
        ) {
            prepare_item::<Self>(progress);
        }

        fn interpretation_input<'a>(
            custody: &'a mut Self::Custody,
        ) -> Option<(Self::Input<'a>, &'a mut Option<Self::Reply>)>
        where
            Self: 'a,
        {
            let (input, received) = custody;
            match (&*input, &*received) {
                (Some(_), None) => Some((input, received)),
                _ => None,
            }
        }

        fn finish_interpretation(
            progress: &mut Option<InterpretationProgress<Self, Self::Custody, Self::Reply>>,
        ) {
            finish_item::<Self>(progress);
        }
    }

    impl InterpreterRequest for InitializationRequest {
        type ReturnToEmitter = NoReturnToEmitter;
        type LogicalProtocols = NoBirthProtocols;
    }

    struct ActivationProbe;

    impl Behavior for ActivationProbe {
        type Protocol = MessageProtocol<MailAddr, ()>;
        type Event = ActivationEvent;
        type Sends = InterpreterRequests<InitializationRequest>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont().with_send::<InitializationRequest, Own>(InitializationRequest(23)))
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[derive(Clone, Copy)]
    enum InitializationDisposition {
        Accepted,
        Rejected,
        Corrupt,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RetirementCustody(u8);

    struct GatedInitializationInterpreter {
        disposition: InitializationDisposition,
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        retirement_task: Option<RetirementTask>,
    }

    struct RetirementTask {
        entered: oneshot::Sender<task::Id>,
        release: oneshot::Receiver<()>,
        completion: RetirementTaskCompletion,
    }

    enum RetirementTaskCompletion {
        Signal(oneshot::Sender<()>),
        Panic,
    }

    impl InterpretItem<InitializationRequest, ActivationEvent, Here>
        for GatedInitializationInterpreter
    {
        #[expect(
            clippy::manual_async_fn,
            reason = "retain the original receiving-loan opaque future and exact capture contract without introducing new route or phase lifetime bounds"
        )]
        fn interpret_item<'a>(
            &'a mut self,
            input: &'a mut Option<InitializationRequest>,
            received: &'a mut Option<<InitializationRequest as ActionItem>::Reply>,
        ) -> impl Future<Output = ()> + Send + 'a
        where
            InitializationRequest: 'a,
        {
            async move {
                if received.is_some() {
                    return;
                }
                let Some(item) = input.take() else {
                    return;
                };
                *received = Some(match self.disposition {
                    InitializationDisposition::Accepted => {
                        ItemSettlement::Accepted(InitializationReceipt(item.0))
                    }
                    InitializationDisposition::Rejected => ItemSettlement::Rejected {
                        item,
                        reason: InitializationRejection::Refused,
                    },
                    InitializationDisposition::Corrupt => ItemSettlement::Corrupt {
                        item,
                        fault: InterpreterFault::CorruptTraversal,
                    },
                });
            }
        }
    }

    impl CommitActions<ActivationProbe> for GatedInitializationInterpreter {
        type Retired = RetirementCustody;

        async fn next_local_event(&mut self) -> Result<ActivationEvent, JoinError> {
            core::future::pending().await
        }

        async fn commit(
            &mut self,
            interpretation: &mut Option<
                InterpretationProgress<
                    ActionsOf<ActivationProbe>,
                    <ActivationProbe as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<ActivationProbe>,
                >,
            >,
        ) {
            let entered = self
                .entered
                .take()
                .expect("initialization enters commitment exactly once");
            let release = self
                .release
                .as_mut()
                .expect("initialization owns one commit gate");
            let entered_result = entered.send(());
            assert!(entered_result.is_ok(), "the commit observer remains live");
            let release_result = release.await;
            assert!(release_result.is_ok(), "the commit gate is released once");
            drop(self.release.take());
            ActionsOf::<ActivationProbe>::interpret::<Self, ActivationEvent, Here>(
                interpretation,
                self,
            )
            .await;
        }

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "Defer trait-port work and owned inputs until the future is polled."
        )]
        async fn offer_next(
            &mut self,
            source: &mut Option<
                SourceProgress<
                    ActionSettlementOf<ActivationProbe>,
                    <ActivationProbe as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            match source.take() {
                Some(SourceProgress::Original(settlement)) => {
                    *source = Some(SourceProgress::Completed(SourceCustody::Exhausted(
                        settlement,
                    )));
                }
                retained => *source = retained,
            }
        }

        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<ActivationEvent, Self::Retired>>,
        ) where
            Self: Sized + Send,
            Self::Retired: Send,
        {
            if received.is_some() {
                return;
            }
            let Some(owner) = interpreter.as_mut() else {
                return;
            };
            let mut activation_tasks = ActivationTasks::new();
            if let Some(task) = owner.retirement_task.take() {
                activation_tasks.spawn(async move {
                    let entered = task.entered.send(task::id());
                    assert!(entered.is_ok(), "the task-start observer remains live");
                    let release = task.release.await;
                    assert!(release.is_ok(), "the activation task receives its release");
                    match task.completion {
                        RetirementTaskCompletion::Signal(completed) => {
                            let completed = completed.send(());
                            assert!(completed.is_ok(), "the completion observer remains live");
                        }
                        RetirementTaskCompletion::Panic => {
                            panic!("the actor-owned activation task panicked")
                        }
                    }
                    Ok(())
                });
            }
            *received = Some(CapabilityRetirement {
                activation_tasks,
                descendants: RetirementCustody(23),
                terminal_report: None,
                retirement_failures: Vec::new(),
            });
            drop(interpreter.take());
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum AddressVisibility {
        Absent,
        Present,
    }

    fn address_visibility(
        addresses: &AddressSpace<MailAddr, ActorRef<MessageProtocol<MailAddr, ()>>>,
        address: MailAddr,
    ) -> AddressVisibility {
        match addresses.resolve(&address) {
            Some(_) => AddressVisibility::Present,
            None => AddressVisibility::Absent,
        }
    }

    type ActivationEnvironment =
        LocalEnvironment<ActivationProbe, GatedInitializationInterpreter, StandardIngress>;

    struct GatedActivation {
        environment: ActivationEnvironment,
        initialization: ActionsOf<ActivationProbe>,
        addresses: AddressSpace<MailAddr, ActorRef<MessageProtocol<MailAddr, ()>>>,
        commit_entered: oneshot::Receiver<()>,
        release_commit: oneshot::Sender<()>,
    }

    fn prepare_gated_activation(disposition: InitializationDisposition) -> GatedActivation {
        let address = MailAddr(23);
        let addresses = AddressSpace::new();
        let observer = addresses.clone();
        let (_termination_publisher, termination) = crate::observe::pair();
        let (_owner_cancellation, owner_cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses,
            Config::new(2),
            termination,
            owner_cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        );
        let mut behavior = ActivationProbe;
        let initialization = behavior::initialize(&mut behavior)
            .expect("the activation probe initializes successfully");

        GatedActivation {
            environment,
            initialization,
            addresses: observer,
            commit_entered,
            release_commit,
        }
    }

    #[tokio::test]
    async fn dropped_root_activation_waiter_releases_its_reservation() {
        let address = MailAddr(24);
        let addresses = AddressSpace::new();
        let observer = addresses.clone();
        let (entered, mut commitment_entered) = oneshot::channel();
        let (release_commitment, release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            addresses,
            Config::new(2),
            address,
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        ));

        tokio::select! {
            _ = &mut launch => panic!("activation settled before the test gate"),
            entry = &mut commitment_entered => {
                entry.expect("the actor entered interpretation after spawn");
            }
        }
        assert!(
            observer.resolve(&address).is_none(),
            "the reservation must remain invisible during interpretation"
        );
        drop(launch);
        let release_result = release_commitment.send(());

        let reservation_released = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match observer.try_reserve(address) {
                    Ok(reservation) => {
                        drop(reservation);
                        break;
                    }
                    Err(ClaimError::AddressInUse(_)) => tokio::task::yield_now().await,
                    Err(error) => panic!("the reservation probe failed: {error:?}"),
                }
            }
        })
        .await;
        assert!(
            reservation_released.is_ok(),
            "dropping the startup waiter left a reserved actor without a cleanup owner; gate release: {release_result:?}"
        );
    }

    #[tokio::test]
    async fn dropped_finish_waiter_retains_actor_cleanup() {
        let (entered, mut commitment_entered) = oneshot::channel();
        let (release_commitment, release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(25),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("activation settled before the test gate"),
            entry = &mut commitment_entered => {
                entry.expect("the root entered initialization interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let actor = root.actor.clone();
        let mut finishing = Box::pin(root.task.finish());
        tokio::select! {
            biased;
            _ = &mut finishing => panic!("the waiting root finished without an input"),
            () = tokio::task::yield_now() => {}
        }
        drop(finishing);

        let termination = tokio::time::timeout(Duration::from_secs(1), actor.termination()).await;
        assert!(
            termination.is_ok(),
            "dropping the finish waiter left the active actor without a cleanup owner"
        );
    }

    #[tokio::test]
    async fn dropped_finish_waiter_settles_actor_owned_activation_task() {
        let (commit_entered, mut entered_commitment) = oneshot::channel();
        let (release_commitment, commit_release) = oneshot::channel();
        let (task_entered, entered_task) = oneshot::channel();
        let (release_task, task_release) = oneshot::channel();
        let (task_completed, completed_task) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(26),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(commit_entered),
                release: Some(commit_release),
                retirement_task: Some(RetirementTask {
                    entered: task_entered,
                    release: task_release,
                    completion: RetirementTaskCompletion::Signal(task_completed),
                }),
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("the root activated before its initialization gate"),
            entered = &mut entered_commitment => {
                entered.expect("the root entered interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let actor = root.actor.clone();
        let mut finishing = Box::pin(root.task.finish());
        tokio::select! {
            biased;
            _ = &mut finishing => panic!("the root finished before owner cancellation"),
            () = tokio::task::yield_now() => {}
        }
        drop(finishing);

        let termination = tokio::time::timeout(Duration::from_secs(1), actor.termination())
            .await
            .expect("owner cancellation publishes the root termination");
        assert_eq!(termination, Err(Crash::Cancelled));
        let entered = tokio::time::timeout(Duration::from_secs(1), entered_task)
            .await
            .expect("the actor-owned activation task starts");
        assert!(entered.is_ok(), "retirement did not abort task startup");
        let released = release_task.send(());
        assert!(
            released.is_ok(),
            "the task retains its receiver after termination"
        );
        let completed = tokio::time::timeout(Duration::from_secs(1), completed_task)
            .await
            .expect("the actor-owned activation task finishes");
        assert!(
            completed.is_ok(),
            "the actor-owned task completed exactly once"
        );
    }

    #[tokio::test]
    async fn owner_retirement_preserves_later_activation_task_panic() {
        let (commit_entered, mut entered_commitment) = oneshot::channel();
        let (release_commitment, commit_release) = oneshot::channel();
        let (task_entered, entered_task) = oneshot::channel();
        let (release_task, task_release) = oneshot::channel();
        let mut launch = Box::pin(crate::launch::spawn_root_with(
            AddressSpace::new(),
            Config::new(2),
            MailAddr(27),
            ActivationProbe,
            move |_, _, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(commit_entered),
                release: Some(commit_release),
                retirement_task: Some(RetirementTask {
                    entered: task_entered,
                    release: task_release,
                    completion: RetirementTaskCompletion::Panic,
                }),
            },
        ));
        tokio::select! {
            _ = &mut launch => panic!("the root activated before its initialization gate"),
            entered = &mut entered_commitment => {
                entered.expect("the root entered interpretation");
            }
        }
        release_commitment
            .send(())
            .expect("the root still owns its initialization");
        let root = launch.await.expect("the root becomes active");
        let retirement = tokio::spawn(root.task.retire());
        let failed_task_id = entered_task
            .await
            .expect("the activation task starts during retirement");
        release_task
            .send(())
            .expect("the activation task retains its release receiver");
        let outcome = retirement
            .await
            .expect("the owner joins without erasing available actor values");
        let Ok(ActorExecutionOutcome::Completed {
            completion:
                Completion::RetirementRequested(LocalRetirementRequest::OwnerCancellation(
                    OwnerCancellation,
                )),
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    activation_tasks,
                    descendants,
                    mut capability_failures,
                    unread_owner_cancellation,
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                },
            behavior: ActivationProbe,
            additional_failures,
        }) = outcome
        else {
            panic!("the acquired owner request remains the primary cause");
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
        assert_eq!(settlements.len(), 0);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants.0, 23);
        assert!(unread_owner_cancellation.is_none());
        assert_eq!(capability_failures.len(), 1);
        let failure = capability_failures
            .pop()
            .expect("one exact later failure is retained");
        assert!(failure.is_panic());
        assert_eq!(failure.id(), failed_task_id);
        let terminal = root.actor.termination().await;
        assert_eq!(terminal, Err(Crash::Cancelled));
        let replay = root.actor.termination().await;
        assert_eq!(replay, terminal);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn ready_owner_request_prevents_actual_address_publication() {
        let address = MailAddr(43);
        let addresses = AddressSpace::new();
        let notice_addresses = addresses.clone();
        let (termination_publisher, termination) = pair();
        let (owner, cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let (notice, mut published) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses.clone(),
            Config::new(2),
            termination,
            cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        )
        .publish_with(move |actor| {
            let resolved = notice_addresses.resolve(&address);
            let sent = notice.send((actor, resolved));
            sent.expect("the complete publication observer remains owned");
        });
        let execution = tokio::spawn(Driver::new(ActivationProbe, environment).run());
        commit_entered
            .await
            .expect("actual Driver initialization enters runtime interpretation");
        let before_commit = addresses.resolve(&address);
        let admitted = owner.send(OwnerCancellation);
        let released = release_commit.send(());
        // All runtime gates are released before the joined outcome or final oracle.
        released.expect("the original commit gate remains owned");
        let DriverRetirement {
            behavior: ActivationProbe,
            disposition,
            residual,
            additional_failures,
        } = execution
            .await
            .expect("the original Driver remains independently joined")
            .unwrap_or_else(|driver| {
                drop(driver);
                panic!("the concrete local Driver completes retirement")
            });
        let LocalResidual::Retired {
            settlements,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            terminal_report,
            unread_owner_cancellation,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
        } = residual
        else {
            panic!("actual interpreted activation retires through its owning port");
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
        let publication = published.try_recv();
        let after_retirement = addresses.resolve(&address);
        let Ok(()) = admitted else {
            panic!("the earlier original request was accepted, not a closed sender");
        };
        assert!(before_commit.is_none());
        assert!(matches!(
            disposition,
            Ok(Completion::RetirementRequested(
                LocalRetirementRequest::OwnerCancellation(OwnerCancellation)
            ))
        ));
        assert_eq!(settlements.len(), 0);
        assert!(ingress.control.is_empty() && ingress.user.is_empty());
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, RetirementCustody(23));
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(after_retirement.is_none());
        match &publication {
            Ok((actor, Some(resolved))) => {
                assert_eq!(actor.address(), address);
                assert_eq!(resolved.address(), address);
            }
            Ok((_, None)) => panic!("the notice must preserve actual Address visibility"),
            Err(TryRecvError::Closed) => {}
            Err(TryRecvError::Empty) => panic!("joined retirement must discharge pending notice"),
        }
        drop(termination_publisher);
        assert!(
            matches!(publication, Err(TryRecvError::Closed)),
            "an owner request admitted before publication must keep the incarnation invisible"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn disarmed_owner_sender_still_allows_actual_publication() {
        let address = MailAddr(43);
        let addresses = AddressSpace::new();
        let notice_addresses = addresses.clone();
        let (termination_publisher, termination) = pair();
        let (owner, cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let (notice, mut published) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses.clone(),
            Config::new(2),
            termination,
            cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        )
        .publish_with(move |actor| {
            let resolved = notice_addresses.resolve(&address);
            let sent = notice.send((actor, resolved));
            sent.expect("the complete publication observer remains owned");
        });
        drop(owner);
        let mut behavior = ActivationProbe;
        let initialization =
            behavior::initialize(&mut behavior).expect("the original pure initialization succeeds");
        let activation = tokio::spawn(async move {
            let mut environment = Some(environment);
            let mut initialization = Some(initialization);
            let mut acquired = None;
            <_ as Environment<ActivationProbe>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        });
        commit_entered
            .await
            .expect("actual interpretation reached its runtime gate");
        let before_commit = addresses.resolve(&address);
        let released = release_commit.send(());
        released.expect("the actual initialization still owns its gate");
        let activated = activation
            .await
            .expect("the exact activation task is joined");
        let Ok((mut active, interpretation)) = activated else {
            panic!("the actual accepted initialization returns its local owner");
        };
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let [SettledItem::Attempted(ItemSettlement::Accepted(InitializationReceipt(23)))] =
            settlement.sends.as_slice()
        else {
            panic!("the complete original initialization send receipt is retained before offering");
        };
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.become_, Step::Continue));
        let publication_result = active.publish();
        let visible = addresses.resolve(&address);
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![settlement]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ActivationProbe>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let absent = addresses.resolve(&address);
        match &publication_result {
            ControlFlow::Continue(()) => {}
            ControlFlow::Break(_) => panic!("this contrast must publish before actual retirement"),
        }
        let publication = published.try_recv();
        let Ok((actor, Some(resolved))) = publication else {
            panic!("actual publication returns both original actor and complete resolved snapshot");
        };
        assert_eq!(actor.address(), address);
        assert_eq!(resolved.address(), address);
        assert!(before_commit.is_none());
        assert!(visible.is_some());
        assert!(absent.is_none());
        assert_eq!(status, SettlementStatus::Accepted);
        assert_retired_initialization(residual, SettlementStatus::Accepted);
        drop(termination_publisher);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn request_after_actual_publication_remains_admissible() {
        let address = MailAddr(43);
        let addresses = AddressSpace::new();
        let notice_addresses = addresses.clone();
        let (termination_publisher, termination) = pair();
        let (owner, cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let (notice, mut published) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses.clone(),
            Config::new(2),
            termination,
            cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        )
        .publish_with(move |actor| {
            let resolved = notice_addresses.resolve(&address);
            let sent = notice.send((actor, resolved));
            sent.expect("the complete publication observer remains owned");
        });
        let mut behavior = ActivationProbe;
        let initialization =
            behavior::initialize(&mut behavior).expect("the original pure initialization succeeds");
        let activation = tokio::spawn(async move {
            let mut environment = Some(environment);
            let mut initialization = Some(initialization);
            let mut acquired = None;
            <_ as Environment<ActivationProbe>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        });
        commit_entered
            .await
            .expect("actual interpretation reached its runtime gate");
        let before_commit = addresses.resolve(&address);
        let released = release_commit.send(());
        released.expect("the actual initialization still owns its gate");
        let activated = activation
            .await
            .expect("the exact activation task is joined");
        let Ok((mut active, interpretation)) = activated else {
            panic!("the actual accepted initialization returns its local owner");
        };
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let [SettledItem::Attempted(ItemSettlement::Accepted(InitializationReceipt(23)))] =
            settlement.sends.as_slice()
        else {
            panic!("the complete original initialization send receipt is retained before offering");
        };
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.become_, Step::Continue));
        let publication_result = active.publish();
        let visible = addresses.resolve(&address);
        let admitted = owner.send(OwnerCancellation);
        let acquired = active.next().await;
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![settlement]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ActivationProbe>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let absent = addresses.resolve(&address);
        match &publication_result {
            ControlFlow::Continue(()) => {}
            ControlFlow::Break(_) => panic!("this contrast must publish before actual retirement"),
        }
        let publication = published.try_recv();
        let Ok((actor, Some(resolved))) = publication else {
            panic!("actual publication returns both original actor and complete resolved snapshot");
        };
        assert_eq!(actor.address(), address);
        assert_eq!(resolved.address(), address);
        let Ok(()) = admitted else {
            panic!("publication must retain the original later cancellation port");
        };
        assert!(matches!(
            acquired,
            ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(OwnerCancellation))
        ));
        assert!(before_commit.is_none());
        assert!(visible.is_some());
        assert!(absent.is_none());
        assert_eq!(status, SettlementStatus::Accepted);
        assert_retired_initialization(residual, SettlementStatus::Accepted);
        drop(termination_publisher);
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn owner_request_during_publication_remains_available_to_local_environment() {
        let address = MailAddr(43);
        let addresses = AddressSpace::new();
        let notice_addresses = addresses.clone();
        let (termination_publisher, termination) = pair();
        let (owner, cancellation) = oneshot::channel();
        let (entered, commit_entered) = oneshot::channel();
        let (release_commit, release) = oneshot::channel();
        let (notice, mut published) = oneshot::channel();
        let environment = ActivationEnvironment::prepare(
            address,
            addresses.clone(),
            Config::new(2),
            termination,
            cancellation,
            move |_, _, _| GatedInitializationInterpreter {
                disposition: InitializationDisposition::Accepted,
                entered: Some(entered),
                release: Some(release),
                retirement_task: None,
            },
        )
        .publish_with(move |actor| {
            let resolved = notice_addresses.resolve(&address);
            let admitted = owner.send(OwnerCancellation);
            let sent = notice.send((actor, resolved, admitted));
            let Ok(()) = sent else {
                panic!("the complete publication observer remains owned");
            };
        });
        let mut behavior = ActivationProbe;
        let initialization =
            behavior::initialize(&mut behavior).expect("the original pure initialization succeeds");
        let activation = tokio::spawn(async move {
            let mut environment = Some(environment);
            let mut initialization = Some(initialization);
            let mut acquired = None;
            <_ as Environment<ActivationProbe>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        });
        commit_entered
            .await
            .expect("actual interpretation reached its runtime gate");
        let before_commit = addresses.resolve(&address);
        let released = release_commit.send(());
        released.expect("the actual initialization still owns its gate");
        let activated = activation
            .await
            .expect("the exact activation task is joined");
        let Ok((mut active, interpretation)) = activated else {
            panic!("the actual accepted initialization returns its local owner");
        };
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let [SettledItem::Attempted(ItemSettlement::Accepted(InitializationReceipt(23)))] =
            settlement.sends.as_slice()
        else {
            panic!("the complete original initialization send receipt is retained before offering");
        };
        assert!(settlement.creations.is_empty());
        assert!(matches!(settlement.become_, Step::Continue));
        let publication_result = active.publish();
        let visible = addresses.resolve(&address);
        let publication = published.try_recv();
        let Ok((actor, resolved, admitted)) = publication else {
            panic!("the actual synchronous notice returns before publication completes");
        };
        let acquired = match &admitted {
            Ok(()) => Some(active.next().await),
            Err(_) => None,
        };
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![settlement]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ActivationProbe>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let absent = addresses.resolve(&address);
        assert!(absent.is_none());
        assert_eq!(status, SettlementStatus::Accepted);
        assert_retired_initialization(residual, SettlementStatus::Accepted);
        match &publication_result {
            ControlFlow::Continue(()) => {}
            ControlFlow::Break(_) => panic!("this contrast must publish before actual retirement"),
        }
        let Some(resolved) = resolved else {
            panic!("actual publication returns both original actor and complete resolved snapshot");
        };
        assert_eq!(actor.address(), address);
        assert_eq!(resolved.address(), address);
        let Ok(()) = admitted else {
            panic!("publication must retain the original later cancellation port");
        };
        assert!(matches!(
            acquired,
            Some(ControlFlow::Break(
                LocalRetirementRequest::OwnerCancellation(OwnerCancellation)
            ))
        ));
        assert!(before_commit.is_none());
        assert!(visible.is_some());
        drop(termination_publisher);
    }

    fn assert_retired_initialization(
        residual: LocalResidual<ActivationProbe, User<MailAddr, ()>, RetirementCustody>,
        expected_status: SettlementStatus,
    ) {
        let LocalResidual::Retired {
            capability_failures,
            terminal_report,
            unread_owner_cancellation,
            settlements,
            ingress,
            activation_tasks,
            descendants,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
        } = residual
        else {
            panic!("interpreted initialization was returned as uncommitted")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        let statuses = settlements
            .iter()
            .map(ClassifySettlement::settlement_status)
            .collect::<Vec<_>>();

        assert_eq!(statuses, [expected_status]);
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].creations.len(), 0);
        assert!(matches!(settlements[0].become_, behavior::Step::Continue));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants, RetirementCustody(23));
    }

    #[tokio::test]
    async fn address_is_absent_until_accepted_initialization_commit_completes() {
        let address = MailAddr(23);
        let GatedActivation {
            environment,
            initialization,
            addresses,
            commit_entered,
            release_commit,
        } = prepare_gated_activation(InitializationDisposition::Accepted);
        let activation = tokio::spawn(async move {
            let mut environment = Some(environment);
            let mut initialization = Some(initialization);
            let mut acquired = None;
            <_ as Environment<ActivationProbe>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        });
        let entered_result = commit_entered.await;
        assert!(entered_result.is_ok(), "initialization commit must start");
        let pending_visibility = address_visibility(&addresses, address);
        let release_result = release_commit.send(());
        assert!(release_result.is_ok(), "initialization commit must resume");
        let activation_result = activation
            .await
            .expect("the activation task must retain ownership");
        let Ok((mut active, interpretation)) = activation_result else {
            panic!("accepted initialization was rejected")
        };
        let committed_visibility = address_visibility(&addresses, address);
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let publication_result = active.publish();
        let published_visibility = address_visibility(&addresses, address);
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![settlement]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ActivationProbe>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let retired_visibility = address_visibility(&addresses, address);
        match &publication_result {
            ControlFlow::Continue(()) => {}
            ControlFlow::Break(_) => {
                panic!("accepted initialization must publish before retirement")
            }
        }

        assert_eq!(pending_visibility, AddressVisibility::Absent);
        assert_eq!(committed_visibility, AddressVisibility::Absent);
        assert_eq!(published_visibility, AddressVisibility::Present);
        assert_eq!(status, SettlementStatus::Accepted);
        assert_eq!(retired_visibility, AddressVisibility::Absent);
        assert_retired_initialization(residual, SettlementStatus::Accepted);
    }

    async fn assert_failed_initialization_never_claims(
        disposition: InitializationDisposition,
        expected_status: SettlementStatus,
    ) {
        let address = MailAddr(23);
        let GatedActivation {
            environment,
            initialization,
            addresses,
            commit_entered,
            release_commit,
        } = prepare_gated_activation(disposition);
        let activation = tokio::spawn(async move {
            let mut environment = Some(environment);
            let mut initialization = Some(initialization);
            let mut acquired = None;
            <_ as Environment<ActivationProbe>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        });
        let entered_result = commit_entered.await;
        assert!(entered_result.is_ok(), "initialization commit must start");
        let pending_visibility = address_visibility(&addresses, address);
        let release_result = release_commit.send(());
        assert!(release_result.is_ok(), "initialization commit must resume");
        let activation_result = activation
            .await
            .expect("the activation task must retain ownership");
        let committed_visibility = address_visibility(&addresses, address);
        let Ok((active, interpretation)) = activation_result else {
            panic!("settlement failure lost its unpublished retirement owner")
        };
        let status = interpretation.settlement_status();
        let settlement = interpretation.into_settlement();
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![settlement]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ActivationProbe>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let retired_visibility = address_visibility(&addresses, address);

        assert_eq!(pending_visibility, AddressVisibility::Absent);
        assert_eq!(committed_visibility, AddressVisibility::Absent);
        assert_eq!(status, expected_status);
        assert_eq!(retired_visibility, AddressVisibility::Absent);
        assert_retired_initialization(residual, expected_status);
    }

    #[tokio::test]
    async fn rejected_initialization_never_becomes_addressable() {
        assert_failed_initialization_never_claims(
            InitializationDisposition::Rejected,
            SettlementStatus::Rejected,
        )
        .await;
    }

    #[tokio::test]
    async fn corrupt_initialization_never_becomes_addressable() {
        assert_failed_initialization_never_claims(
            InitializationDisposition::Corrupt,
            SettlementStatus::Corrupt,
        )
        .await;
    }

    struct PanickingFenceProbe;

    impl Behavior for PanickingFenceProbe {
        type Protocol = behavior::MessageProtocol<MailAddr, ()>;
        type Event = User<MailAddr, ()>;
        type Sends = Vec<Never>;
        type Ph = Never;
        type Error = Infallible;
        type Birth = NoBirths;

        fn init(&mut self, _: InitializationTurn) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }

        fn transition(&mut self, _: behavior::ActiveTurn, _: Self::Event) -> BehaviorActed<Self> {
            panic!("deliberate failure before the queued fence")
        }
    }

    #[test]
    fn ordinary_ingress_retains_the_exact_user_item_layout() {
        type Hosted = StopOnShutdown<FenceProbe>;
        type StandardItem = <StandardIngress as IngressMode<Hosted>>::Item;

        assert_eq!(size_of::<StandardItem>(), size_of::<User<MailAddr, ()>>());
        assert!(size_of::<LocalIngress<MailAddr, ()>>() > size_of::<StandardItem>());
    }

    #[tokio::test]
    async fn retirement_returns_every_queued_ingress_item_in_fifo_order() {
        let (control, admission_owner, user_mailbox, consumer) =
            mailbox_channel::<User<MailAddr, ()>, User<MailAddr, ()>>(Config::new(2));
        control.send(User::new(MailAddr(71), ())).unwrap();
        control.send(User::new(MailAddr(73), ())).unwrap();
        user_mailbox
            .send(User::new(MailAddr(79), ()))
            .await
            .unwrap();
        let mut inbox = LocalInbox::<FenceProbe, StandardIngress>::new(consumer);

        let retired = inbox
            .drain()
            .expect("the local inbox retains its consumer before retirement");
        let origins = retired
            .control
            .into_iter()
            .map(|event| event.from)
            .collect::<Vec<_>>();
        let user_origins = retired
            .user
            .into_iter()
            .map(|event| event.from)
            .collect::<Vec<_>>();

        assert_eq!(origins, vec![MailAddr(71), MailAddr(73)]);
        assert_eq!(user_origins, vec![MailAddr(79)]);
        drop((admission_owner, user_mailbox));
    }

    #[test]
    fn admission_closure_names_first_and_repeated_dispositions() {
        let (_control, owner, _mailbox, _consumer) = mailbox_channel::<(), ()>(Config::new(2));
        let admission = Admission::new(owner);

        let first = admission.close();
        let repeated = admission.close();

        assert!(matches!(first, AdmissionClosure::Closed));
        assert!(matches!(repeated, AdmissionClosure::AlreadyClosed));
    }

    #[tokio::test]
    async fn exact_shutdown_classifies_repeated_and_stopped_requests_without_resending() {
        type ProbeProtocol = MessageProtocol<MailAddr, ()>;
        type ShutdownEvent = <StopOnShutdown<FenceProbe> as Behavior>::Event;

        let (control, owner, mailbox, mut consumer) =
            mailbox_channel::<ShutdownEvent, User<MailAddr, ()>>(Config::new(2));
        let admission = Arc::new(Admission::new(owner));
        let (termination_publisher, termination) = crate::observe::pair();
        let actor = ActorRef::<ProbeProtocol>::new(
            MailAddr(36),
            EndpointMailbox::Standard {
                mailbox,
                admission: Arc::downgrade(&admission),
            },
            termination,
        );

        let accepted = request_actor_shutdown(&actor, &control, Ingress::new());
        let repeated = request_actor_shutdown(&actor, &control, Ingress::new());

        assert_eq!(accepted, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let accepted_event = consumer.recv_control().await;
        assert!(accepted_event.is_some());

        termination_publisher.complete(Ok(Exit::Normal));
        let stopped = request_actor_shutdown(&actor, &control, Ingress::new());

        assert_eq!(stopped, Err(ShutdownRejection::AlreadyStopped));
        drop(control);
        let closed_control = consumer.recv_control().await;
        assert!(closed_control.is_none());
        drop((actor, admission, consumer));
    }

    #[tokio::test]
    async fn ordinary_actor_does_not_admit_entity_fences() {
        let actor = crate::launch::launch_inert(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(37),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Enqueue));
        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        let control = actor.shutdown_control.upgrade().unwrap();
        let shutdown = request_actor_shutdown(&actor.actor, &control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn entity_launch_preserves_the_exact_conflicting_address() {
        let actors = crate::ActorSpace::new();
        let first = crate::launch::launch_inert_entity(
            actors.clone(),
            Config::new(2),
            MailAddr(40),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();
        let conflict = crate::launch::launch_inert_entity(
            actors,
            Config::new(2),
            MailAddr(40),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await;

        match conflict {
            Err(crate::launch::SpawnError::HostRejected {
                error: ClaimError::AddressInUse(address),
                ..
            }) => {
                assert_eq!(address, MailAddr(40));
            }
            Err(error) => panic!("address conflict was misclassified: {error:?}"),
            Ok(_) => panic!("duplicate address was accepted"),
        }
        let shutdown = request_actor_shutdown(&first.actor, &first.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = first.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn user_lane_fence_follows_the_preceding_applied_action() {
        let committed = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&committed);
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(4),
            MailAddr(41),
            StopOnShutdown::new(FenceProbe),
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .unwrap();

        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        actor.actor.fence().await.unwrap();

        assert_eq!(committed.load(Ordering::SeqCst), 2);
        let shutdown = request_actor_shutdown(&actor.actor, &actor.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));
    }

    #[tokio::test]
    async fn fence_rejected_before_enqueue_reports_the_exact_stage() {
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(43),
            StopOnShutdown::new(FenceProbe),
            |_| {},
        )
        .await
        .unwrap();
        let shutdown = request_actor_shutdown(&actor.actor, &actor.control, Ingress::new());
        assert_eq!(shutdown, Ok(()));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Ok(Exit::Normal));

        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Enqueue));
    }

    #[tokio::test]
    async fn accepted_fence_collected_during_failure_reports_acknowledgement() {
        let actor = crate::launch::launch_inert_entity(
            crate::ActorSpace::new(),
            Config::new(2),
            MailAddr(47),
            StopOnShutdown::new(PanickingFenceProbe),
            |_| {},
        )
        .await
        .unwrap();

        actor
            .actor
            .send_from(actor.actor.address(), ())
            .await
            .unwrap();
        let fence = actor.actor.fence().await;
        assert_eq!(fence, Err(crate::entity::FenceFailure::Acknowledgement));
        let termination = actor.actor.termination().await;
        assert_eq!(termination, Err(Crash::Panicked));
    }

    struct ReadyActor;
    struct ReadySource;

    #[derive(Debug, PartialEq, Eq)]
    enum ReadyEvent {
        Mailbox(User<MailAddr, u64>),
        SourceInput(u64),
        Observation(PeerStopped<MailAddr>),
        Timer(TimerElapsed),
    }

    impl UserEvent for ReadyEvent {
        type Addr = MailAddr;
        type Message = u64;
        fn user(from: MailAddr, message: u64) -> Self {
            Self::Mailbox(User::new(from, message))
        }
        fn into_user(self) -> Result<User<MailAddr, u64>, Self> {
            match self {
                Self::Mailbox(user) => Ok(user),
                other => Err(other),
            }
        }
    }
    impl EventIngress<ReadySource, u64> for ReadyEvent {
        fn ingress(input: u64) -> Self {
            Self::SourceInput(input)
        }
    }
    impl InjectEvent<PeerStopped<MailAddr>, Here> for ReadyEvent {
        fn inject_at(stopped: PeerStopped<MailAddr>) -> Self {
            Self::Observation(stopped)
        }
    }
    impl InjectEvent<TimerElapsed, Here> for ReadyEvent {
        fn inject_at(elapsed: TimerElapsed) -> Self {
            Self::Timer(elapsed)
        }
    }
    impl Behavior for ReadyActor {
        type Protocol = MessageProtocol<MailAddr, u64>;
        type Event = ReadyEvent;
        type Sends = NoSends;
        type Ph = Never;
        type Birth = NoBirths;
        type Error = Never;
        fn transition(&mut self, _: ActiveTurn, _: ReadyEvent) -> BehaviorActed<Self> {
            Ok(Actions::cont())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn jointly_ready_source_mailbox_observation_and_timer_follow_native_acquisition_order() {
        acquire_ready_campaign(Vec::new()).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn continuously_ready_mailbox_postpones_observation_and_timer_until_mailbox_quiesces() {
        acquire_ready_campaign(vec![13, 17, 19, 23, 29, 31, 37, 41]).await;
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Keep simultaneous native readiness, replenishment, exact retirement, and full event trace in one controller."
    )]
    async fn acquire_ready_campaign(mailbox_inputs: Vec<u64>) {
        let address = MailAddr(141);
        let sender = MailAddr(143);
        let peer = MailAddr(149);
        let timer = TimerId(151);
        let generation = TimerGeneration(3);
        let (publication, termination) = observe::pair();
        let (peer_publication, peer_termination) = observe::pair();
        let (owner_request, owner_cancellation) = oneshot::channel();
        let (terminal_sender, terminal_receiver) = oneshot::channel();
        let deadline = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("the deterministic already-due deadline is representable");
        peer_publication.complete(Ok(Exit::Normal));
        let environment = LocalEnvironment::<
            ReadyActor,
            ActionInterpreter<ApplicationCapabilities<ReadyActor, ()>>,
            StandardIngress,
        >::prepare(
            address,
            ActorSpace::new(),
            Config::new(2),
            termination,
            owner_cancellation,
            move |control, mut timers, mut observations| {
                observations.insert_peer::<Here>(peer, peer_termination);
                timers
                    .schedule_at::<Here>(ScheduleAt::new(timer, generation, deadline))
                    .expect("the original finite timer is scheduled once");
                let inputs = ApplicationCapabilityInputs::<ReadyActor, ()> {
                    actor_spaces: Arc::new(()),
                    allocations: ApplicationAddresses::new(),
                    address,
                    control,
                    timers,
                    observations,
                    terminal_reports: LocalTerminalReports::new(terminal_sender),
                };
                let mut capabilities = ApplicationCapabilities::<ReadyActor, ()>::new_with_bindings(
                    inputs,
                    NoChildBindings::default(),
                );
                // Genuine source admission, before any Behavior fold, stores the
                // exact SourceInput on this actor's existing control mailbox.
                {
                    let mut source_input = Some(47);
                    let mut received_admission = None;
                    let admitted = {
                        let admission =
                            SourceAdmission::<ReadyEvent, ReadySource, u64>::admit_source(
                                &mut capabilities,
                                &mut source_input,
                                &mut received_admission,
                            );
                        let mut admission = pin!(admission);
                        let mut context = Context::from_waker(Waker::noop());
                        admission.as_mut().poll(&mut context)
                    };
                    assert_eq!(admitted, Poll::Ready(()));
                    assert_eq!(source_input, None);
                    assert_eq!(received_admission, Some(Ok(())));
                }
                ActionInterpreter::new(capabilities)
            },
        );
        let endpoint = environment.endpoint.clone();
        let queued = endpoint.send_from(sender, 11).await;
        queued.expect("the original live endpoint accepts this exact mailbox input");
        let mut prepared = Some(environment);
        let mut initialization_actions = Some(Actions::cont());
        let mut received_activation = None;
        <_ as Environment<ReadyActor>>::activate(
            &mut prepared,
            &mut initialization_actions,
            &mut received_activation,
        )
        .await;
        let Some(Ok((mut active, interpretation))) = received_activation else {
            panic!("the original native environment must install this closed actor");
        };
        assert!(prepared.is_none());
        assert!(initialization_actions.is_none());
        let Interpretation::Complete(initialization) = interpretation else {
            panic!("the genuine empty initialization is complete");
        };
        let published = active.publish();
        assert!(matches!(published, ControlFlow::Continue(())));
        let mut acquired = Vec::new();
        // The real source is control ingress: Communication prioritizes it over
        // ordinary user ingress. It is not a fourth independent select branch.
        for _ in 0..2 {
            match active.next().await {
                ControlFlow::Continue(Some(event)) => acquired.push(event),
                ControlFlow::Continue(None) => {
                    panic!("jointly ready native ingress must remain live")
                }
                ControlFlow::Break(request) => {
                    drop(request);
                    panic!("jointly ready native ingress must not request retirement");
                }
            }
        }
        for input in &mailbox_inputs {
            let queued = endpoint.send_from(sender, *input).await;
            queued.expect("the original live endpoint accepts this exact mailbox input");
            match active.next().await {
                ControlFlow::Continue(Some(event)) => acquired.push(event),
                ControlFlow::Continue(None) => {
                    panic!("replenished native mailbox must remain live")
                }
                ControlFlow::Break(request) => {
                    drop(request);
                    panic!("replenished native mailbox must not request retirement");
                }
            }
        }
        for _ in 0..2 {
            match active.next().await {
                ControlFlow::Continue(Some(event)) => acquired.push(event),
                ControlFlow::Continue(None) => {
                    panic!("original observation and due timer must remain available")
                }
                ControlFlow::Break(request) => {
                    drop(request);
                    panic!("ready observation and timer must not request retirement");
                }
            }
        }
        let mut retiring = Some(active);
        let mut uncommitted_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut original_settlements = Some(vec![initialization]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<ReadyActor>>::retire(
            &mut retiring,
            &mut uncommitted_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut original_settlements,
            &mut received_retirement,
        )
        .await;
        let retired = received_retirement.expect(
            "native retirement receives the original complete residual outside its producer",
        );
        let retired = retired.settle_activation_tasks().await;
        assert!(retiring.is_none());
        assert!(uncommitted_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(original_settlements.is_none());
        let LocalResidual::Retired {
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            terminal_report,
            retirement_failures,
            settlements,
            ingress,
            activation_tasks,
            descendants,
            capability_failures,
            unread_owner_cancellation,
        } = retired
        else {
            panic!("native acquisition comparison must return the actual joined retirement");
        };
        let (descendants, child_failures) = descendants;
        drop((endpoint, owner_request, publication, terminal_receiver));
        let mut expected = vec![
            ReadyEvent::SourceInput(47),
            ReadyEvent::Mailbox(User::new(sender, 11)),
        ];
        expected.extend(
            mailbox_inputs
                .into_iter()
                .map(|input| ReadyEvent::Mailbox(User::new(sender, input))),
        );
        expected.push(ReadyEvent::Observation(PeerStopped::new(
            peer,
            Ok(Exit::Normal),
        )));
        expected.push(ReadyEvent::Timer(TimerElapsed::new(timer, generation)));
        assert_eq!(acquired, expected);
        let [initialization] = settlements.as_slice() else {
            panic!("native retirement retains the single whole initialization settlement");
        };
        let ActionSettlement {
            creations,
            sends,
            become_,
        } = initialization;
        assert!(creations.is_empty());
        assert_eq!(*sends, NoSends);
        assert!(matches!(become_, Step::Continue));
        assert_eq!(ingress.control, []);
        assert_eq!(ingress.user, []);
        assert!(activation_tasks.is_empty());
        assert_eq!(descendants.len(), 0);
        assert_eq!(child_failures, ());
        assert!(capability_failures.is_empty());
        assert_eq!(unread_owner_cancellation, None);
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
    }
}

#[cfg(test)]
mod source_acquisition_custody {
    use crate::MailAddr;
    use crate::launch::{ActorSpace, InertCapabilities};
    use crate::local::effects::ActionSettlementOf;
    use crate::local::effects::{CapabilityRetirement, CommitActions};
    use crate::local::environment::{LocalEnvironment, LocalResidual};
    use crate::local::execution::{ActivationTasks, LocalRetirementRequest, OwnerCancellation};
    use crate::local::ingress::StandardIngress;
    use crate::observe;
    use behavior::{
        Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorSettlements, ClassifySettlement,
        Here, Interpretation, InterpretationProgress, MessageProtocol, Never, NoBirths, NoSends,
        SettlementStatus, SourceProgress, SourceSettlementCustody, Step, User, UserEvent,
    };
    use bombay_engine::{ActionsOf, ActiveEnvironment, Environment};
    use communication::Config;
    use core::future::Future;
    use core::ops::ControlFlow;
    use core::task::{Context, Poll, Waker};
    use std::panic::panic_any;
    use std::sync::{Arc, Weak};
    use tokio::sync::oneshot;
    use tokio::task::{self, JoinError};

    #[derive(Debug)]
    enum ActorInput {
        Mailbox(Arc<Vec<u64>>),
        Capability(Arc<Vec<u64>>),
    }
    impl UserEvent for ActorInput {
        type Addr = MailAddr;
        type Message = Never;
        fn user(_: MailAddr, message: Never) -> Self {
            match message {}
        }
        fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
            Err(self)
        }
    }
    struct SourceCustodyActor {
        retained: Vec<ActorInput>,
    }
    impl Behavior for SourceCustodyActor {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = ActorInput;
        type Sends = NoSends;
        type Ph = Never;
        type Birth = NoBirths;
        type Error = Never;
        fn transition(&mut self, _: ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
            self.retained.push(event);
            Ok(Actions::cont())
        }
    }
    struct SourceCapabilityCustody {
        activation_tasks: ActivationTasks<ActorInput>,
    }
    impl CommitActions<SourceCustodyActor> for SourceCapabilityCustody {
        type Retired = ();
        async fn commit(
            &mut self,
            interpretation: &mut Option<
                InterpretationProgress<
                    ActionsOf<SourceCustodyActor>,
                    <SourceCustodyActor as BehaviorSettlements>::InterpretationCustody,
                    ActionSettlementOf<SourceCustodyActor>,
                >,
            >,
        ) {
            ActionsOf::<SourceCustodyActor>::interpret::<_, ActorInput, Here>(
                interpretation,
                &mut InertCapabilities,
            )
            .await;
        }
        async fn offer_next(
            &mut self,
            source: &mut Option<
                SourceProgress<
                    ActionSettlementOf<SourceCustodyActor>,
                    <SourceCustodyActor as BehaviorSettlements>::SourceCustody,
                >,
            >,
        ) {
            <ActionSettlementOf<SourceCustodyActor> as SourceSettlementCustody<
                InertCapabilities,
                ActorInput,
            >>::prepare_source(source);
            if let Some(SourceProgress::Offering(custody)) = source {
                <ActionSettlementOf<SourceCustodyActor> as SourceSettlementCustody<
                    InertCapabilities,
                    ActorInput,
                >>::offer_next_to_source(custody, &mut InertCapabilities)
                .await;
            }
            <ActionSettlementOf<SourceCustodyActor> as SourceSettlementCustody<
                InertCapabilities,
                ActorInput,
            >>::finish_source(source);
        }
        async fn next_local_event(&mut self) -> Result<ActorInput, JoinError> {
            self.activation_tasks.next_event().await
        }
        async fn receive_retirement(
            interpreter: &mut Option<Self>,
            received: &mut Option<CapabilityRetirement<ActorInput, Self::Retired>>,
        ) where
            Self: Sized + Send,
            Self::Retired: Send,
        {
            if received.is_some() {
                return;
            }
            let Some(owner) = interpreter.take() else {
                return;
            };
            *received = Some(CapabilityRetirement {
                activation_tasks: owner.activation_tasks,
                descendants: (),
                terminal_report: None,
                retirement_failures: Vec::new(),
            });
        }
    }

    enum CancellationTiming {
        OwnerReady,
        SourcePending,
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ready_owner_retirement_retains_joint_ready_move_only_source_inputs() {
        preserve_source_cancellation(CancellationTiming::OwnerReady).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn owner_cancels_pending_source_acquisition_retaining_joint_ready_move_only_inputs() {
        preserve_source_cancellation(CancellationTiming::SourcePending).await;
    }
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn preserve_source_cancellation(timing: CancellationTiming) {
        let mailbox = Arc::new(vec![11, 111]);
        let capability = Arc::new(vec![22, 122]);
        let retained_inputs = (Arc::downgrade(&mailbox), Arc::downgrade(&capability));
        let allocations = (mailbox.as_ptr(), capability.as_ptr());
        let (publisher, termination) = observe::pair();
        let (request, cancellation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (completed, completion) = oneshot::channel();
        let mut activation_tasks = ActivationTasks::new();
        activation_tasks.spawn(async move {
            released
                .await
                .expect("the local source work is explicitly released");
            completed
                .send(())
                .expect("the completed source observer stays retained");
            Err(ActorInput::Capability(capability))
        });
        let environment = LocalEnvironment::<
            SourceCustodyActor,
            SourceCapabilityCustody,
            StandardIngress,
        >::prepare(
            MailAddr(96),
            ActorSpace::new(),
            Config::new(2),
            termination,
            cancellation,
            move |_, _, _| SourceCapabilityCustody { activation_tasks },
        );
        let control = environment.control();
        let Ok((mut active, interpretation)) = ({
            let mut environment = Some(environment);
            let mut initialization = Some(Actions::cont());
            let mut acquired = None;
            <_ as Environment<SourceCustodyActor>>::activate(
                &mut environment,
                &mut initialization,
                &mut acquired,
            )
            .await;
            assert!(environment.is_none());
            assert!(initialization.is_none());
            acquired.expect("the actual local activation result is acquired outside its producer")
        }) else {
            panic!("the actual local environment installs the source custody actor");
        };
        let Interpretation::Complete(initialization) = interpretation else {
            panic!("the complete exact empty initialization product is accepted");
        };
        let mut acquiring = Box::pin(active.next_source());
        match timing {
            CancellationTiming::OwnerReady => {}
            CancellationTiming::SourcePending => {
                let mut context = Context::from_waker(Waker::noop());
                let polled = acquiring.as_mut().poll(&mut context);
                assert!(matches!(polled, Poll::Pending));
            }
        }
        let admitted = control.send(ActorInput::Mailbox(mailbox));
        assert!(admitted.is_ok());
        release
            .send(())
            .expect("the real capability task receives one release");
        completion
            .await
            .expect("the real task returns its exact rejected source event");
        let sent = request.send(OwnerCancellation);
        assert!(sent.is_ok());
        let acquired = acquiring.await;
        match acquired {
            ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(OwnerCancellation)) => {}
            ControlFlow::Break(LocalRetirementRequest::CapabilityFailed(failure)) => {
                panic!("this successful source task must not fail: {failure}")
            }
            ControlFlow::Continue(Some(event)) => panic!(
                "ready owner request must retain both exact source inputs instead of acquiring {event:?}"
            ),
            ControlFlow::Continue(None) => {
                panic!("ready owner request is not source closure")
            }
        }
        let mut retirement_environment = Some(active);
        let mut retirement_actions = None;
        let mut received_interpretation = None;
        let mut received_source = None;
        let mut source_index = None;
        let mut acquired_ingress = None;
        let mut settlements = Some(vec![initialization]);
        let mut received_retirement = None;
        <_ as ActiveEnvironment<SourceCustodyActor>>::retire(
            &mut retirement_environment,
            &mut retirement_actions,
            &mut received_interpretation,
            &mut received_source,
            &mut source_index,
            &mut acquired_ingress,
            &mut settlements,
            &mut received_retirement,
        )
        .await;
        assert!(retirement_environment.is_none());
        assert!(retirement_actions.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(settlements.is_none());
        let residual = received_retirement
            .expect("the complete original local residual is acquired outside retirement");
        let settled = residual.settle_activation_tasks().await;
        verify_source_input_retirement(settled, allocations, &retained_inputs);
        drop((control, publisher));
    }

    fn verify_source_input_retirement(
        settled: <LocalEnvironment<SourceCustodyActor, SourceCapabilityCustody, StandardIngress> as Environment<SourceCustodyActor>>::Residual,
        allocations: (*const u64, *const u64),
        retained_inputs: &(Weak<Vec<u64>>, Weak<Vec<u64>>),
    ) {
        let LocalResidual::Retired {
            capability_failures,
            terminal_report,
            unread_owner_cancellation,
            ingress,
            settlements,
            activation_tasks,
            descendants,
            interpretation,
            source,
            received_interpretation,
            received_source,
            source_index,
            acquired_ingress,
            retirement_failures,
        } = settled
        else {
            panic!("actual task retirement preserves all available source event custody");
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        let [initialization] = settlements.as_slice() else {
            panic!("the complete initialization settlement remains owned exactly once");
        };
        assert!(initialization.creations.is_empty());
        assert_eq!(
            (&initialization.sends, &initialization.become_),
            (&NoSends, &Step::Continue)
        );
        assert_eq!(
            initialization.settlement_status(),
            SettlementStatus::Accepted
        );
        assert!(activation_tasks.is_empty());
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(descendants, ());
        let [
            ActorInput::Mailbox(mailbox),
            ActorInput::Capability(capability),
        ] = ingress.control.as_slice()
        else {
            panic!(
                "the full control trace is original mailbox input followed by actual returned task input"
            );
        };
        assert_eq!(
            (mailbox.as_slice(), capability.as_slice()),
            (&[11, 111][..], &[22, 122][..])
        );
        assert_eq!((mailbox.as_ptr(), capability.as_ptr()), allocations);
        assert_eq!(
            (
                retained_inputs.0.strong_count(),
                retained_inputs.1.strong_count()
            ),
            (1, 1)
        );
        drop(ingress);
        assert_eq!(
            (
                retained_inputs.0.strong_count(),
                retained_inputs.1.strong_count()
            ),
            (0, 0)
        );
    }
    enum AcquisitionPort {
        Ordinary,
        Source,
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn owner_first_acquisition_retains_joint_ready_capability_failure_on_both_ports() {
        for port in [AcquisitionPort::Ordinary, AcquisitionPort::Source] {
            let original = Box::new(vec![44_u64, 144]);
            let (completed, completion) = oneshot::channel();
            let mut activation_tasks = ActivationTasks::new();
            activation_tasks.spawn(async move {
                completed
                    .send(task::id())
                    .expect("the owner observes task completion");
                panic_any(original)
            });
            // The task cannot suspend between the signal and panic; on this
            // current-thread runtime its failed join is ready before we resume.
            let failed_task_id = completion
                .await
                .expect("the actual capability task reached its panic");
            let (publisher, termination) = observe::pair();
            let (request, cancellation) = oneshot::channel();
            let environment = LocalEnvironment::<SourceCustodyActor, _, StandardIngress>::prepare(
                MailAddr(97),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |_, _, _| SourceCapabilityCustody { activation_tasks },
            );
            let Ok((mut active, initialization)) = ({
                let mut environment = Some(environment);
                let mut initialization = Some(Actions::cont());
                let mut acquired = None;
                <_ as Environment<SourceCustodyActor>>::activate(
                    &mut environment,
                    &mut initialization,
                    &mut acquired,
                )
                .await;
                assert!(environment.is_none());
                assert!(initialization.is_none());
                acquired
                    .expect("the actual local activation result is acquired outside its producer")
            }) else {
                panic!("the actual local actor installs before input acquisition");
            };
            let Interpretation::Complete(initialization) = initialization else {
                panic!("the complete initialization product is retained");
            };
            let admitted = request.send(OwnerCancellation);
            assert!(admitted.is_ok());
            let acquired = match port {
                AcquisitionPort::Ordinary => active.next().await,
                AcquisitionPort::Source => active.next_source().await,
            };
            let ControlFlow::Break(LocalRetirementRequest::OwnerCancellation(OwnerCancellation)) =
                acquired
            else {
                panic!("the ready owner request has priority over the ready task failure");
            };
            let mut retirement_environment = Some(active);
            let mut retirement_actions = None;
            let mut received_interpretation = None;
            let mut received_source = None;
            let mut source_index = None;
            let mut acquired_ingress = None;
            let mut settlements = Some(vec![initialization]);
            let mut received_retirement = None;
            <_ as ActiveEnvironment<SourceCustodyActor>>::retire(
                &mut retirement_environment,
                &mut retirement_actions,
                &mut received_interpretation,
                &mut received_source,
                &mut source_index,
                &mut acquired_ingress,
                &mut settlements,
                &mut received_retirement,
            )
            .await;
            assert!(retirement_environment.is_none());
            assert!(retirement_actions.is_none());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(settlements.is_none());
            let residual = received_retirement
                .expect("the complete original local residual is acquired outside retirement");
            let residual = residual.settle_activation_tasks().await;
            let LocalResidual::Retired {
                settlements,
                ingress,
                activation_tasks,
                descendants,
                mut capability_failures,
                unread_owner_cancellation,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                terminal_report,
                retirement_failures,
            } = residual
            else {
                panic!("the actual retirement retains its complete environment");
            };
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(terminal_report.is_none());
            assert!(retirement_failures.is_empty());
            assert_eq!(settlements.len(), 1);
            assert!(ingress.control.is_empty());
            assert_eq!(ingress.user.len(), 0);
            assert!(activation_tasks.is_empty());
            assert_eq!(descendants, ());
            assert!(unread_owner_cancellation.is_none());
            assert_eq!(capability_failures.len(), 1);
            let failure = capability_failures
                .pop()
                .expect("the one failure is retained as late");
            assert!(failure.is_panic());
            assert_eq!(failure.id(), failed_task_id);
            drop(publisher);
        }
    }
    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "keep the complete outside-fold controller, joined disposal and whole typed original-value oracles together; shortening it would split the single custody law or weaken observations"
    )]
    async fn primary_capability_failure_returns_original_error_once_on_both_local_ports() {
        for port in [AcquisitionPort::Ordinary, AcquisitionPort::Source] {
            let original = Box::new(vec![55_u64, 155]);
            let (completed, completion) = oneshot::channel();
            let mut activation_tasks = ActivationTasks::new();
            activation_tasks.spawn(async move {
                completed
                    .send(task::id())
                    .expect("the caller observes the actual task completing");
                panic_any(original)
            });
            let failed_task_id = completion
                .await
                .expect("the failed task is ready before acquisition");
            let (publisher, termination) = observe::pair();
            let (owner, cancellation) = oneshot::channel();
            let environment = LocalEnvironment::<SourceCustodyActor, _, StandardIngress>::prepare(
                MailAddr(98),
                ActorSpace::new(),
                Config::new(2),
                termination,
                cancellation,
                move |_, _, _| SourceCapabilityCustody { activation_tasks },
            );
            let Ok((mut active, initialization)) = ({
                let mut environment = Some(environment);
                let mut initialization = Some(Actions::cont());
                let mut acquired = None;
                <_ as Environment<SourceCustodyActor>>::activate(
                    &mut environment,
                    &mut initialization,
                    &mut acquired,
                )
                .await;
                assert!(environment.is_none());
                assert!(initialization.is_none());
                acquired
                    .expect("the actual local activation result is acquired outside its producer")
            }) else {
                panic!("the real local environment installs before acquiring the task failure");
            };
            let Interpretation::Complete(initialization) = initialization else {
                panic!("the complete initialization settlement is retained");
            };
            let acquired = match port {
                AcquisitionPort::Ordinary => active.next().await,
                AcquisitionPort::Source => active.next_source().await,
            };
            let ControlFlow::Break(LocalRetirementRequest::CapabilityFailed(failure)) = acquired
            else {
                panic!(
                    "the acquired source failure cannot become source closure, exhaustion, or owner cancellation"
                );
            };
            assert!(failure.is_panic());
            // A second poll on the same live port cannot acquire that task again.
            let mut replay = Box::pin(async {
                match port {
                    AcquisitionPort::Ordinary => active.next().await,
                    AcquisitionPort::Source => active.next_source().await,
                }
            });
            let mut context = Context::from_waker(Waker::noop());
            let polled = replay.as_mut().poll(&mut context);
            assert!(matches!(polled, Poll::Pending));
            drop(replay);
            let mut retirement_environment = Some(active);
            let mut retirement_actions = None;
            let mut received_interpretation = None;
            let mut received_source = None;
            let mut source_index = None;
            let mut acquired_ingress = None;
            let mut settlements = Some(vec![initialization]);
            let mut received_retirement = None;
            <_ as ActiveEnvironment<SourceCustodyActor>>::retire(
                &mut retirement_environment,
                &mut retirement_actions,
                &mut received_interpretation,
                &mut received_source,
                &mut source_index,
                &mut acquired_ingress,
                &mut settlements,
                &mut received_retirement,
            )
            .await;
            assert!(retirement_environment.is_none());
            assert!(retirement_actions.is_none());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(settlements.is_none());
            let residual = received_retirement
                .expect("the complete original local residual is acquired outside retirement");
            let residual = residual.settle_activation_tasks().await;
            let LocalResidual::Retired {
                settlements,
                ingress,
                activation_tasks,
                descendants,
                capability_failures,
                terminal_report,
                unread_owner_cancellation,
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
            } = residual
            else {
                panic!("actual retirement preserves the complete available residual");
            };
            assert!(interpretation.is_none());
            assert!(source.is_none());
            assert!(received_interpretation.is_none());
            assert!(received_source.is_none());
            assert!(source_index.is_none());
            assert!(acquired_ingress.is_none());
            assert!(terminal_report.is_none());
            assert!(retirement_failures.is_empty());
            let [initialization] = settlements.as_slice() else {
                panic!("the full original initialization product remains owned");
            };
            assert_eq!(initialization.sends, NoSends);
            assert!(initialization.creations.is_empty());
            assert_eq!(initialization.become_, Step::Continue);
            assert_eq!(
                initialization.settlement_status(),
                SettlementStatus::Accepted
            );
            assert!(ingress.control.is_empty());
            assert_eq!(ingress.user.len(), 0);
            assert!(activation_tasks.is_empty());
            assert_eq!(descendants, ());
            assert!(capability_failures.is_empty());
            assert!(unread_owner_cancellation.is_none());
            assert!(failure.is_panic());
            assert_eq!(failure.id(), failed_task_id);
            drop((owner, publisher));
        }
    }
}

#[cfg(test)]
mod shutdown_admission_contract {
    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;
    use crate::actor;
    use crate::address::MailAddr;
    use crate::launch::launch_inert_entity;
    use crate::local::endpoint::{ActorRef, InstalledActor};
    use crate::local::environment::LocalResidual;
    use crate::local::ingress::Admission;
    use crate::observe;
    use behavior::{
        Actions, Become, Behavior, BehaviorActed, BehaviorBase, EventLayer, Here, Ingress, Inside,
        Never, NoSends, Step, Stopped, User,
    };
    use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::{Config, Received, mailbox_channel};
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct LedgerSubmission {
        entries: Vec<u64>,
    }

    #[derive(Default)]
    struct DeliveryLedger {
        received: Vec<User<MailAddr, LedgerSubmission>>,
    }
    #[actor]
    impl DeliveryLedger {
        fn receive(&mut self, from: MailAddr, values: LedgerSubmission) -> BehaviorActed<Self> {
            self.received.push(User::new(from, values));
            Ok(Actions::cont())
        }
    }
    #[tokio::test]
    async fn installed_shutdown_preserves_preclose_permit_and_rejects_later_original() {
        type Target = StopOnShutdown<DeliveryLedger>;
        let (control, owner, mailbox, mut receiver) = mailbox_channel::<
            <Target as Behavior>::Event,
            User<MailAddr, LedgerSubmission>,
        >(Config::new(1));
        let admission = Arc::new(Admission::new(owner));
        let (publication, observation) = observe::pair();
        let endpoint = ActorRef::<DeliveryLedger>::external(
            MailAddr(571),
            mailbox,
            Arc::downgrade(&admission),
            observation,
        );
        let installed = InstalledActor::<Target>::new(endpoint.clone(), control);
        let prefix = LedgerSubmission {
            entries: vec![67, 71],
        };
        let prefix_allocation = prefix.entries.as_ptr();
        let sent = endpoint.send_from(MailAddr(73), prefix).await;
        assert!(sent.is_ok());
        let second = LedgerSubmission {
            entries: vec![79, 83],
        };
        let second_allocation = second.entries.as_ptr();
        let sent = endpoint.send_from(MailAddr(89), second).await;
        assert!(sent.is_ok());
        let values = LedgerSubmission {
            entries: vec![97, 101],
        };
        let allocation = values.entries.as_ptr();
        let mut acquired = pin!(endpoint.send_from(MailAddr(103), values));
        let mut context = Context::from_waker(Waker::noop());
        let pending = acquired.as_mut().poll(&mut context);
        assert!(matches!(pending, Poll::Pending));
        let shutdown = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(shutdown, Ok(()));
        let postclose = LedgerSubmission {
            entries: vec![107, 109],
        };
        let postclose_allocation = postclose.entries.as_ptr();
        let mut later = pin!(endpoint.send_from(MailAddr(113), postclose));
        let attempted = later.as_mut().poll(&mut context);
        let Poll::Ready(rejected) = attempted else {
            panic!("postclose admission must reject immediately")
        };
        let original = rejected
            .expect_err("first-polled postclose delivery cannot acquire admission")
            .into_message();
        assert_eq!(original.entries, [107, 109]);
        assert_eq!(original.entries.as_ptr(), postclose_allocation);
        let replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopping));
        let requested = receiver
            .recv_control()
            .await
            .expect("one original shutdown input");
        assert!(matches!(requested, EventLayer::Owned(ShutdownRequested)));
        let first = receiver.recv().await;
        let Some(Received::User(first)) = first else {
            panic!("first admitted original")
        };
        assert_eq!(first.from, MailAddr(73));
        assert_eq!(first.message.entries, [67, 71]);
        assert_eq!(first.message.entries.as_ptr(), prefix_allocation);
        let second = receiver.recv().await;
        let Some(Received::User(second)) = second else {
            panic!("second admitted original")
        };
        assert_eq!(second.from, MailAddr(89));
        assert_eq!(second.message.entries, [79, 83]);
        assert_eq!(second.message.entries.as_ptr(), second_allocation);
        let admitted = acquired.await;
        assert!(admitted.is_ok());
        let third = receiver.recv().await;
        let Some(Received::User(third)) = third else {
            panic!("preclose permit must finish before terminal marker")
        };
        assert_eq!(third.from, MailAddr(103));
        assert_eq!(third.message.entries, [97, 101]);
        assert_eq!(third.message.entries.as_ptr(), allocation);
        let terminal = receiver.recv().await;
        assert!(matches!(terminal, Some(Received::UserLaneClosed)));
        publication.complete(Ok(Exit::Normal));
        let stopped = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let repeated = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(stopped, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopped));
        drop(installed);
        let exhausted = receiver.recv().await;
        assert!(exhausted.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both owned lease incarnations, full results, and replay trace together."
    )]
    async fn installed_shutdown_cannot_retarget_actual_distinct_event_replacement() {
        type Direct = StopOnShutdown<DeliveryLedger>;
        type Nested = StopOnShutdown<Direct>;
        let actors = ActorSpace::<DeliveryLedger>::new();
        let address = MailAddr(593);
        let old_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&old_actions);
        let old = launch_inert_entity(
            actors.clone(),
            Config::new(2),
            address,
            StopOnShutdown::new(DeliveryLedger::default()),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("old installed incarnation"));
        let installed = InstalledActor::<Direct>::new(old.actor.clone(), old.control.clone());
        let requested = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(requested, Ok(()));
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopping));
        let terminal = old.actor.termination().await;
        assert_eq!(terminal, Ok(Exit::Normal));
        let retired = old.task.finish().await;
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                },
            completion,
            additional_failures,
        }) = retired
        else {
            panic!("whole old owned result")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(additional_failures.is_empty());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(behavior.base().received.len(), 0);
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(descendants, ());
        assert_eq!(settlements.len(), 1);
        assert!(settlements[0].creations.is_empty());
        assert!(matches!(settlements[0].sends.owned, NoSends));
        assert!(matches!(settlements[0].sends.inner, NoSends));
        assert!(matches!(settlements[0].become_, Step::Stop(_)));
        assert_eq!(
            *old_actions.lock().expect("old full actions"),
            [Step::Continue, Step::Stop(Stopped)]
        );
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        let fresh_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&fresh_actions);
        let fresh = launch_inert_entity(
            actors.clone(),
            Config::new(2),
            address,
            StopOnShutdown::new(StopOnShutdown::new(DeliveryLedger::default())),
            move |actions| {
                assert!(actions.creates.is_empty());
                assert!(matches!(actions.sends.owned, NoSends));
                assert!(matches!(actions.sends.inner.owned, NoSends));
                assert!(matches!(actions.sends.inner.inner, NoSends));
                recording
                    .lock()
                    .expect("complete action observation")
                    .push(actions.become_);
            },
        )
        .await
        .unwrap_or_else(|_| panic!("actual same-address nested replacement"));
        let replacement = InstalledActor::<Nested>::new(fresh.actor.clone(), fresh.control.clone());
        let stale = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let stale_replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        assert_eq!(stale, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(stale_replay, Err(ShutdownRejection::AlreadyStopped));
        let values = LedgerSubmission {
            entries: vec![157, 163],
        };
        let allocation = values.entries.as_ptr();
        let sent = fresh.actor.send_from(MailAddr(167), values).await;
        assert!(sent.is_ok());
        let committed = fresh.actor.fence().await;
        assert!(committed.is_ok());
        let mut publication = pin!(fresh.actor.termination());
        let mut context = Context::from_waker(Waker::noop());
        let pending = publication.as_mut().poll(&mut context);
        assert!(matches!(pending, Poll::Pending));
        let requested =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        let repeated =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        assert_eq!(requested, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let stopped = publication.await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let retired = fresh.task.finish().await;
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                },
            completion,
            additional_failures,
        }) = retired
        else {
            panic!("whole replacement owned result")
        };
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert!(received_interpretation.is_none());
        assert!(received_source.is_none());
        assert!(source_index.is_none());
        assert!(acquired_ingress.is_none());
        assert!(terminal_report.is_none());
        assert!(retirement_failures.is_empty());
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(additional_failures.is_empty());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(ingress.user.len(), 0);
        assert_eq!(descendants, ());
        assert_eq!(
            *fresh_actions.lock().expect("fresh full actions"),
            [Step::Continue, Step::Continue, Step::Stop(Stopped)]
        );
        assert_eq!(behavior.base().received.len(), 1);
        let original = &behavior.base().received[0];
        assert_eq!(original.from, MailAddr(167));
        assert_eq!(original.message.entries, [157, 163]);
        assert_eq!(original.message.entries.as_ptr(), allocation);
        let final_turn = settlements.len() - 1;
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner.owned, NoSends));
            assert!(matches!(settlement.sends.inner.inner, NoSends));
            match settlement.become_ {
                Step::Continue => assert!(turn < final_turn),
                Step::Stop(_) => assert_eq!(turn, final_turn),
            }
        }
        let stale_replay = installed.request_shutdown(Ingress::<ShutdownRequested, Here>::new());
        let final_replay =
            replacement.request_shutdown(Ingress::<ShutdownRequested, Inside<Here>>::new());
        assert_eq!(stale_replay, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(final_replay, Err(ShutdownRejection::AlreadyStopped));
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
    }
}

#[cfg(test)]
impl<B: BehaviorSettlements, U, Descendants, Capabilities>
    LocalResidual<B, U, (Descendants, Capabilities)>
{
    pub(crate) fn separate_capabilities(self) -> (LocalResidual<B, U, Descendants>, Capabilities) {
        match self {
            Self::Prepared {
                ingress,
                activation_tasks,
                capability_failures,
                terminal_report,
                unread_owner_cancellation,
                descendants: (descendants, capabilities),
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
            } => (
                LocalResidual::Prepared {
                    ingress,
                    activation_tasks,
                    capability_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    descendants,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                },
                capabilities,
            ),
            Self::Uncommitted {
                initialization,
                ingress,
                activation_tasks,
                capability_failures,
                terminal_report,
                unread_owner_cancellation,
                descendants: (descendants, capabilities),
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
            } => (
                LocalResidual::Uncommitted {
                    initialization,
                    ingress,
                    activation_tasks,
                    capability_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    descendants,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                },
                capabilities,
            ),
            Self::Retired {
                settlements,
                ingress,
                activation_tasks,
                capability_failures,
                terminal_report,
                unread_owner_cancellation,
                descendants: (descendants, capabilities),
                interpretation,
                source,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
            } => (
                LocalResidual::Retired {
                    settlements,
                    ingress,
                    activation_tasks,
                    capability_failures,
                    terminal_report,
                    unread_owner_cancellation,
                    descendants,
                    interpretation,
                    source,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                },
                capabilities,
            ),
        }
    }
}
