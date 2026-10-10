use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{
    AdmissionFailure, EntityDefinition, EntityFamilyAt, EntityRef, NativeEntityHost, Passivation,
};
use crate::local::endpoint::{ActorRef, ExtractLocalEndpoint, SendError, request_actor_shutdown};
use crate::local::ingress::{Admission, AdmissionClosure};
use crate::observe::{Publisher, pair};
use crate::termination::Termination;
use behavior::{
    AllocationRejection, EstablishedRecipient, Here, Ingress, InjectEvent, Never, Protocol, User,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested};
use communication::{Consumer, ControlSender, Received, mailbox_channel};
use core::fmt;
use core::future::Future;
use std::sync::{Arc, Weak};

impl<P, Event, Families> fmt::Debug for ApplicationHandle<P, Event, Families>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationHandle")
            .field("root", &self.root)
            .field("families_type", &core::any::type_name::<Families>())
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event, Families: Clone> Clone for ApplicationHandle<P, Event, Families> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            shutdown_control: self.shutdown_control.clone(),
            allocations: self.allocations.clone(),
            families: self.families.clone(),
        }
    }
}

impl<P: Protocol, Event, Families> ApplicationHandle<P, Event, Families> {
    pub(in crate::application) fn new(
        root: ActorRef<P>,
        shutdown_control: Weak<ControlSender<Event>>,
        allocations: ApplicationAddresses,
        families: Families,
    ) -> Self {
        Self {
            root,
            shutdown_control,
            allocations,
            families,
        }
    }

    /// Borrow the exact live root's delivery reference.
    #[must_use]
    pub const fn root(&self) -> &ActorRef<P> {
        &self.root
    }

    /// Form a transport-neutral interface from an explicit receptionist product.
    pub fn interface<Api>(&self, api: Api) -> ActorInterface<Api> {
        ActorInterface::new(api, self.allocations.clone())
    }

    /// Project lifecycle authority without exposing topology ownership.
    #[must_use]
    pub fn lifecycle(&self) -> ApplicationLifecycle<P, Event> {
        ApplicationLifecycle {
            root: self.root.clone(),
            control: self.shutdown_control.clone(),
        }
    }

    /// Select one statically declared native Entity family by semantic role.
    #[must_use]
    pub fn entities<Role, Position>(
        &self,
        _: Role,
    ) -> crate::entity::Entities<<Families as EntityFamilyAt<Role, Position>>::Definition>
    where
        Families: EntityFamilyAt<Role, Position>,
    {
        self.families.select_family()
    }

    /// Begin passivation of one identity in a statically declared family.
    #[must_use]
    #[allow(
        private_bounds,
        reason = "native execution remains a sealed application composition proof"
    )]
    pub fn passivate_entity<Role, Position>(
        &self,
        role: Role,
        id: &<<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Id,
    ) -> Passivation
    where
        Families: EntityFamilyAt<Role, Position>,
        <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Hosts:
            NativeEntityHost<
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Behavior,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Terminal,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::ChildFailures,
            >,
{
        self.entities(role).passivate(id)
    }
}

impl<P, Event> fmt::Debug for ApplicationLifecycle<P, Event>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationLifecycle")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<P: Protocol, Event> Clone for ApplicationLifecycle<P, Event> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            control: self.control.clone(),
        }
    }
}

impl<P: Protocol, Event> ApplicationLifecycle<P, Event> {
    /// Request shutdown through the root lifecycle lane.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection when the root is already stopping or has
    /// already stopped.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRejection>
    where
        Event: InjectEvent<ShutdownRequested, Here>,
    {
        if self.root.termination_observation().try_get().is_some() {
            return Err(ShutdownRejection::AlreadyStopped);
        }
        let Some(control) = self.control.upgrade() else {
            return Err(self.root.shutdown_rejection());
        };
        request_actor_shutdown(&self.root, &control, Ingress::new())
    }

    /// Observe termination of the exact root incarnation.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P, Event> {
        self.root.termination()
    }
}

/// One statically typed destination accepted by [`ExternalActor::send`].
///
/// Bombay implements this sealed contract for exact actor recipients and
/// stable Entity references. Applications select a capability value; they do
/// not implement routing or inspect addresses.
pub trait ExternalTarget: target_sealed::Sealed {
    /// Message admitted by this capability.
    type Message: Send + 'static;
    /// Exact rejection returned by this capability.
    type Error;

    #[doc(hidden)]
    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Target> target_sealed::Sealed for EstablishedRecipient<Target> where
    Target: Protocol<Addr = MailAddr>
{
}

impl<Target> ExternalTarget for EstablishedRecipient<Target>
where
    Target: Protocol<Addr = MailAddr>,
    Target::Msg: Send + 'static,
{
    type Message = Target::Msg;
    type Error = SendError<Target::Msg>;

    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let mut interpreter = ExtractLocalEndpoint;
        let endpoint = self.clone().interpret(&mut interpreter);
        async move { endpoint.send_from(origin, message).await }
    }
}

impl<D> target_sealed::Sealed for EntityRef<D> where D: EntityDefinition {}

impl<D> ExternalTarget for EntityRef<D>
where
    D: EntityDefinition,
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal, D::ChildFailures>,
{
    type Message = behavior::BehaviorMessage<D::Behavior>;
    type Error = AdmissionFailure<Self::Message>;

    fn send_from(
        &self,
        origin: MailAddr,
        message: Self::Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.admit_from(origin, message)
    }
}

impl<Api> fmt::Debug for ActorInterface<Api> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorInterface")
            .field("api_type", &core::any::type_name::<Api>())
            .finish_non_exhaustive()
    }
}

impl<Api> Clone for ActorInterface<Api>
where
    Api: Clone,
{
    fn clone(&self) -> Self {
        Self {
            api: self.api.clone(),
            allocations: self.allocations.clone(),
        }
    }
}

impl<Api> ActorInterface<Api> {
    pub(crate) const fn new(api: Api, allocations: ApplicationAddresses) -> Self {
        Self { api, allocations }
    }

    /// Borrow the statically declared receptionist product.
    #[must_use]
    pub const fn api(&self) -> &Api {
        &self.api
    }

    /// Establish one real external actor for a concrete reply protocol.
    ///
    /// The returned actor owns one fresh allocated address, one exact cloneable
    /// recipient, and the only receive authority for its mailbox.
    ///
    /// # Errors
    ///
    /// Returns the exact allocation failure. No raw address or unbound
    /// recipient is returned on failure.
    pub fn external<P>(&self) -> Result<ExternalActor<P>, ExternalActorError>
    where
        P: Protocol<Addr = MailAddr>,
        P::Msg: Send,
    {
        ExternalActor::establish(&self.allocations)
    }
}

impl<P> fmt::Debug for ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalActor")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<P> ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
    P::Msg: Send,
{
    fn establish(allocations: &ApplicationAddresses) -> Result<Self, ExternalActorError> {
        let address = allocations
            .allocate()
            .map_err(ExternalActorError::Allocation)?;
        let (control_liveness, owner, mailbox, receiver) =
            mailbox_channel::<Never, User<MailAddr, P::Msg>>(communication::Config::new(
                EXTERNAL_USER_CAPACITY,
            ));
        let admission = Arc::new(Admission::new(owner));
        let (termination, observation) = pair();
        let endpoint =
            ActorRef::external(address, mailbox, Arc::downgrade(&admission), observation);
        let recipient = endpoint.established_recipient();
        Ok(Self {
            address,
            recipient,
            admission,
            _control_liveness: control_liveness,
            receiver,
            termination: Some(termination),
        })
    }

    /// The fresh logical identity supplied as `User::from` by [`Self::send`].
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Clone the exact reply capability without duplicating receive authority.
    #[must_use]
    pub fn recipient(&self) -> EstablishedRecipient<P> {
        self.recipient.clone()
    }

    /// Send directly to one exact exported or extruded actor capability.
    ///
    /// The external actor's own allocated address is always used as the message
    /// origin. The reply destination remains an explicit field of the domain
    /// message when the protocol requires one.
    ///
    /// # Errors
    ///
    /// Returns the exact rejected message when the destination incarnation is
    /// no longer accepting user messages.
    pub fn send<'a, Target>(
        &'a self,
        target: &'a Target,
        message: Target::Message,
    ) -> impl Future<Output = Result<(), Target::Error>> + Send + use<'a, P, Target>
    where
        Target: ExternalTarget,
    {
        target.send_from(self.address, message)
    }

    /// Receive the next message and its truthful actor origin.
    ///
    /// Returns `None` after admission closes and every accepted message has
    /// been yielded.
    pub async fn receive(&mut self) -> Option<User<MailAddr, P::Msg>> {
        match self.receiver.recv().await? {
            Received::User(message) => Some(message),
            Received::UserLaneClosed => None,
            Received::Control(never) => match never {},
        }
    }

    /// Close new reply admission while retaining the already accepted prefix.
    pub fn close_admission(&self) {
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
    }
}

impl<P> Drop for ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    fn drop(&mut self) {
        match self.admission.close() {
            AdmissionClosure::Closed | AdmissionClosure::AlreadyClosed => {}
        }
        if let Some(termination) = self.termination.take() {
            termination.complete(Ok(Exit::Normal));
        }
    }
}

mod target_sealed {
    pub trait Sealed {}
}

/// A live application's typed root and capability projections.
pub struct ApplicationHandle<P: Protocol, Event, Families = ()> {
    root: ActorRef<P>,
    shutdown_control: Weak<ControlSender<Event>>,
    allocations: ApplicationAddresses,
    families: Families,
}

/// Explicit application lifecycle authority, separate from actor messaging.
pub struct ApplicationLifecycle<P: Protocol, Event> {
    root: ActorRef<P>,
    control: Weak<ControlSender<Event>>,
}

const EXTERNAL_USER_CAPACITY: usize = 1_024;

/// A transport-neutral product of explicitly exported actor capabilities.
///
/// `Api` is application-defined. Bombay neither infers exports from topology
/// nor grants lifecycle authority through this value.
pub struct ActorInterface<Api> {
    api: Api,
    allocations: ApplicationAddresses,
}

/// Exact failure to establish an external actor endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ExternalActorError {
    /// Bombay's one never-wrapping application address source is exhausted.
    #[error("the Bombay application has exhausted its local address space")]
    Allocation(#[source] AllocationRejection),
}

/// One allocated external actor with affine receive authority.
///
/// This value intentionally does not implement `Clone`. Cloneable producer
/// authority is available separately through [`Self::recipient`].
pub struct ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    address: MailAddr,
    recipient: EstablishedRecipient<P>,
    admission: Arc<Admission<User<MailAddr, P::Msg>>>,
    _control_liveness: ControlSender<Never>,
    receiver: Consumer<Never, User<MailAddr, P::Msg>>,
    termination: Option<Publisher<Termination<MailAddr>>>,
}

#[cfg(test)]
mod tests {
    use behavior::MessageProtocol;
    use behavior_actors::Exit;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ExternalActor;
    use crate::local::endpoint::ExtractLocalEndpoint;

    #[test]
    fn external_actor_drop_publishes_normal_termination_to_its_exact_recipient() {
        let allocations = ApplicationAddresses::new();
        let external = ExternalActor::<MessageProtocol<MailAddr, u64>>::establish(&allocations)
            .expect("the external actor is established");
        let address = external.address();
        let recipient = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let description = format!("{external:?}");
        assert!(description.contains("ExternalActor"));
        assert!(description.contains(&format!("{address:?}")));

        drop(external);
        let terminal = recipient.termination_observation().try_get();
        assert_eq!(terminal, Some(Ok(Exit::Normal)));
    }
}

#[cfg(test)]
mod installed_shutdown_contract {

    use behavior::{Become, BehaviorBase, Never, NoSends, Step, Stopped};
    use behavior_actors::{Exit, ShutdownRejection, StopOnShutdown};
    use bombay_engine::Completion;
    use communication::Config;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::{Arc, Mutex};

    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ActorInterface;
    use crate::application::ApplicationLifecycle;
    use crate::launch::launch_inert;

    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::LocalResidual;

    use crate::{LedgerProtocol, ShutdownLedger};
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep both joined incarnations and stale-view observations in one trace."
    )]
    async fn old_root_lifecycle_cannot_close_actual_same_address_replacement() {
        let actors = ActorSpace::<LedgerProtocol>::new();
        let address = MailAddr(541);
        let old_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&old_actions);
        let old = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![43],
                received: Vec::new(),
            }),
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
        .unwrap_or_else(|_| panic!("old root launch"));
        let lifecycle = ApplicationLifecycle {
            root: old.actor.clone(),
            control: old.shutdown_control.clone(),
        };
        let repeated_lifecycle = lifecycle.clone();
        let accepted = lifecycle.request_shutdown();
        let repeated = repeated_lifecycle.request_shutdown();
        assert_eq!(accepted, Ok(()));
        assert_eq!(repeated, Err(ShutdownRejection::AlreadyStopping));
        let old_termination = lifecycle.termination().await;
        assert_eq!(old_termination, Ok(Exit::Normal));
        let (old_result, termination_notification) = old.task.finish().await;
        termination_notification.expect("the actual ordinary termination notification succeeded");
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = old_result
        else {
            panic!("joined old root")
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
        assert_eq!(behavior.base().entries, [43]);
        assert_eq!(behavior.base().received.len(), 0);
        assert!(ingress.control.is_empty() && ingress.user.is_empty());
        assert_eq!(descendants, ());
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
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
        assert!(lifecycle.control.upgrade().is_none());
        let fresh_actions = Arc::new(Mutex::new(Vec::<Become<Never>>::new()));
        let recording = Arc::clone(&fresh_actions);
        let fresh = launch_inert(
            actors.clone(),
            Config::new(1),
            address,
            StopOnShutdown::new(ShutdownLedger {
                entries: vec![47],
                received: Vec::new(),
            }),
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
        .unwrap_or_else(|_| panic!("fresh root claim"));
        let fresh_lifecycle = ApplicationLifecycle {
            root: fresh.actor.clone(),
            control: fresh.shutdown_control.clone(),
        };
        let old_request = lifecycle.request_shutdown();
        let replay = repeated_lifecycle.request_shutdown();
        assert_eq!(old_request, Err(ShutdownRejection::AlreadyStopped));
        assert_eq!(replay, Err(ShutdownRejection::AlreadyStopped));
        let mut fresh_termination = pin!(fresh_lifecycle.termination());
        let mut context = Context::from_waker(Waker::noop());
        let waiting = Future::poll(fresh_termination.as_mut(), &mut context);
        assert!(matches!(waiting, Poll::Pending));
        let values = vec![53, 59];
        let allocation = values.as_ptr();
        let delivered = fresh.actor.send_from(MailAddr(61), values).await;
        assert!(delivered.is_ok());
        let new_request = fresh_lifecycle.request_shutdown();
        assert_eq!(new_request, Ok(()));
        let stopped = fresh_termination.await;
        assert_eq!(stopped, Ok(Exit::Normal));
        let (retired, termination_notification) = fresh.task.finish().await;
        termination_notification.expect("the actual ordinary termination notification succeeded");
        let Ok(ActorExecutionOutcome::Completed {
            behavior,
            residual:
                LocalResidual::Retired {
                    interpretation,
                    source,
                    settlements,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    terminal_report,
                    retirement_failures,
                    ingress,
                    descendants,
                    activation_tasks,
                    capability_failures,
                    unread_owner_cancellation,
                },
            additional_failures,
            completion,
        }) = retired
        else {
            panic!("whole joined replacement")
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
        assert_eq!(behavior.base().entries, [47]);
        assert!(activation_tasks.is_empty());
        assert!(capability_failures.is_empty());
        assert!(unread_owner_cancellation.is_none());
        assert!(matches!(completion, Completion::Stopped));
        assert_eq!(ingress.control.len(), 0);
        assert_eq!(descendants, ());
        let mut expected_actions = vec![Step::Continue; 1 + behavior.base().received.len()];
        expected_actions.push(Step::Stop(Stopped));
        assert_eq!(
            *fresh_actions.lock().expect("fresh full actions"),
            expected_actions
        );
        let mut originals = behavior.base().received.iter().chain(ingress.user.iter());
        let original = originals.next().expect("one admitted original user input");
        assert_eq!(original.from, MailAddr(61));
        assert_eq!(original.message, [53, 59]);
        assert_eq!(original.message.as_ptr(), allocation);
        assert!(originals.next().is_none());
        assert_ne!(settlements.len(), 0);
        let final_turn = settlements.len() - 1;
        for (turn, settlement) in settlements.into_iter().enumerate() {
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            match settlement.become_ {
                Step::Continue => assert!(turn < final_turn),
                Step::Stop(_) => assert_eq!(turn, final_turn),
            }
        }
        let absent = actors.resolve(&address);
        assert!(absent.is_none());
        let fresh_replay = fresh_lifecycle.request_shutdown();
        assert_eq!(fresh_replay, Err(ShutdownRejection::AlreadyStopped));
        let old_replay = lifecycle.request_shutdown();
        assert_eq!(old_replay, Err(ShutdownRejection::AlreadyStopped));
    }
    #[tokio::test]
    async fn external_customer_owns_admission_without_behavior_shutdown() {
        let interface = ActorInterface::new((), ApplicationAddresses::new());
        let mut external = interface
            .external::<LedgerProtocol>()
            .expect("real external endpoint");
        let endpoint = external.recipient().interpret(&mut ExtractLocalEndpoint);
        let values = vec![127, 131];
        let allocation = values.as_ptr();
        let accepted = endpoint.send_from(MailAddr(137), values).await;
        assert!(accepted.is_ok());
        external.close_admission();
        let values = vec![139, 149];
        let allocation_rejected = values.as_ptr();
        let rejected = endpoint.send_from(MailAddr(151), values).await;
        let original = rejected
            .expect_err("external owner closed admission")
            .into_message();
        assert_eq!(original, [139, 149]);
        assert_eq!(original.as_ptr(), allocation_rejected);
        let admitted = external
            .receive()
            .await
            .expect("exact admitted external message");
        assert_eq!(admitted.from, MailAddr(137));
        assert_eq!(admitted.message, [127, 131]);
        assert_eq!(admitted.message.as_ptr(), allocation);
        let exhausted = external.receive().await;
        assert!(exhausted.is_none());
        drop(external);
        let terminal = endpoint.termination().await;
        assert_eq!(terminal, Ok(Exit::Normal));
    }
}
