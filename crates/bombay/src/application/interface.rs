use super::MailboxCapacity;
use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{
    AdmissionFailure, EntityDefinition, EntityFamilyAt, EntityRef, EntityRetirementError,
    NativeEntityHost, Passivation, RetirementMode,
};
use crate::local::endpoint::{ActorRef, ExtractLocalEndpoint, SendError, request_actor_shutdown};
use crate::local::ingress::{Admission, AdmissionClosure};
use crate::observe::{Observation, Publisher, pair};
#[cfg(feature = "remote")]
use crate::remote::{Export, ExportError, ExportLimits, IngressError};
use crate::terminal::ActorRetirementReport;
use crate::termination::Termination;
use behavior::{
    AllocationRejection, EstablishedRecipient, Ingress, InjectEvent, Never, Protocol, User,
};
use behavior_actors::{Exit, ShutdownRejection, ShutdownRequested};
use communication::{Consumer, ControlSender, Received, TrySendError, mailbox_channel};
use core::fmt;
use core::future::Future;
use std::sync::{Arc, Weak};
#[cfg(feature = "remote")]
use tokio::sync::Semaphore;
#[cfg(feature = "remote")]
use zenoh::Session;
#[cfg(feature = "remote")]
use zenoh::query::Query;

#[cfg(feature = "remote")]
fn owns_actor<P>(target: &EstablishedRecipient<P>, allocations: &ApplicationAddresses) -> bool
where
    P: Protocol<Addr = MailAddr>,
{
    let mut interpreter = ExtractLocalEndpoint;
    let endpoint = target.clone().interpret(&mut interpreter);
    endpoint.belongs_to(allocations)
}

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
            retirement: self.retirement.clone(),
            allocations: self.allocations.clone(),
            client_capacity: self.client_capacity,
            families: self.families.clone(),
            #[cfg(feature = "remote")]
            session: self.session.clone(),
        }
    }
}

impl<P: Protocol, Event, Families> ApplicationHandle<P, Event, Families> {
    pub(in crate::application) fn new(
        root: ActorRef<P>,
        shutdown_control: Weak<ControlSender<Event>>,
        retirement: Observation<ActorRetirementReport>,
        allocations: ApplicationAddresses,
        client_capacity: MailboxCapacity,
        families: Families,
    ) -> Self {
        Self {
            root,
            shutdown_control,
            retirement,
            allocations,
            client_capacity,
            families,
            #[cfg(feature = "remote")]
            session: None,
        }
    }

    /// Borrow the exact live root's delivery reference.
    #[must_use]
    pub const fn root(&self) -> &ActorRef<P> {
        &self.root
    }

    /// Form a transport-neutral interface from an explicit receptionist product.
    pub fn interface<Api>(&self, api: Api) -> ActorInterface<Api> {
        ActorInterface::new(api, self.allocations.clone(), self.client_capacity)
    }

    /// Establish one affine caller that explicitly selects a target on each send.
    /// Use this when one typed reply mailbox serves multiple capabilities.
    ///
    /// # Errors
    /// Returns the existing allocation rejection without consuming this handle.
    pub fn external<Replies>(&self) -> Result<ExternalActor<Replies>, ExternalActorError>
    where
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        ExternalActor::establish(&self.allocations, self.client_capacity)
    }

    /// Bind one exact actor recipient or stable Entity reference to a reply client.
    /// The selected target retains its own admission and rejection contract.
    ///
    /// # Errors
    /// Returns the complete original target with its allocation rejection.
    pub fn client_to<Target, Replies>(
        &self,
        target: Target,
    ) -> Result<ApplicationClient<Target, Replies>, (Target, ExternalActorError)>
    where
        Target: ExternalTarget,
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        ApplicationClient::establish(target, &self.allocations, self.client_capacity)
    }

    /// Construct a reply client only for an actor owned by this Application.
    ///
    /// The exact root and any owned child use the same operation. Foreign actor
    /// recipients and external reply endpoints are refused before allocation.
    /// Known ownership survives target closure; a later send retains its actual
    /// native closure result and original message.
    ///
    /// This checks association only. It does not publish a network endpoint,
    /// establish readiness, authenticate a caller or grant operation permission.
    /// Sending retains the existing awaited or nonwaiting mailbox contract.
    ///
    /// # Errors
    /// Returns the complete original recipient with its association refusal or
    /// the actual error establishing the existing native reply endpoint.
    #[cfg(feature = "remote")]
    pub fn export_client<TargetProtocol, Replies>(
        &self,
        target: EstablishedRecipient<TargetProtocol>,
    ) -> Result<
        ApplicationClient<EstablishedRecipient<TargetProtocol>, Replies>,
        (EstablishedRecipient<TargetProtocol>, ExportError),
    >
    where
        TargetProtocol: Protocol<Addr = MailAddr>,
        TargetProtocol::Msg: Send + 'static,
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        if !owns_actor(&target, &self.allocations) {
            return Err((target, ExportError::UnownedTarget));
        }
        match ApplicationClient::establish(target, &self.allocations, self.client_capacity) {
            Ok(client) => Ok(client),
            Err((target, cause)) => Err((target, ExportError::Endpoint { cause })),
        }
    }

    /// Declare an actor owned by this Application through its actual Node.
    ///
    /// Only Node invocation supplies the native Session. Local-only handles and
    /// closed Sessions refuse before declaration. Exact native actor ownership
    /// is checked without allocating a reply endpoint. Each new retained request
    /// later owns one independently allocated native reply client.
    ///
    /// Limits are explicit counts, not byte or whole-service bounds. The supplied
    /// predicate compares provider-owned logical request identities; it does not
    /// authenticate them. The refusal callable receives each exact native ingress
    /// refusal synchronously and must implement the deployment's bounded policy.
    /// This constructor does not establish readiness, authorization or unique
    /// targeting across overlapping declarations.
    ///
    /// # Errors
    /// Returns the original target and actual construction cause. Limits and the
    /// callable are consumed. No native reply endpoint is allocated by construction.
    /// Cancelling the whole operation surrenders its inputs.
    #[cfg(feature = "remote")]
    pub async fn export<Target, Replies, Receipt>(
        &self,
        target: EstablishedRecipient<Target>,
        key: &str,
        limits: ExportLimits,
        same_request: fn(&Receipt, &Receipt) -> bool,
        refused: impl Fn(IngressError<Query>) + Send + Sync + 'static,
    ) -> Result<Export<Target, Replies, Receipt>, (EstablishedRecipient<Target>, ExportError)>
    where
        Target: Protocol<Addr = MailAddr>,
        Target::Msg: Send + 'static,
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        let Some(session) = self.session.as_ref() else {
            return Err((target, ExportError::NodeAbsent));
        };
        if session.is_closed() {
            return Err((target, ExportError::SessionClosed));
        }
        if limits.queued_queries.get() > Semaphore::MAX_PERMITS
            || limits.retained_queries.get() > Semaphore::MAX_PERMITS
        {
            return Err((target, ExportError::QueryCapacity));
        }
        if !owns_actor(&target, &self.allocations) {
            return Err((target, ExportError::UnownedTarget));
        }
        let interface = self.interface(target.clone());
        match Export::declare(
            session.clone(),
            interface,
            key,
            limits,
            same_request,
            refused,
        )
        .await
        {
            Ok(export) => Ok(export),
            Err(cause) => Err((target, cause)),
        }
    }

    /// Bind one exact actor recipient or stable Entity reference to a one-way sender.
    ///
    /// # Errors
    /// Returns the complete original target with its allocation rejection.
    pub fn sender_to<Target>(
        &self,
        target: Target,
    ) -> Result<ApplicationSender<Target>, (Target, ExternalActorError)>
    where
        Target: ExternalTarget,
    {
        ApplicationSender::establish(target, &self.allocations)
    }

    /// Project lifecycle authority without exposing topology ownership.
    #[must_use]
    pub fn lifecycle(&self) -> ApplicationLifecycle<P, Event> {
        ApplicationLifecycle {
            root: self.root.clone(),
            control: self.shutdown_control.clone(),
            retirement: self.retirement.clone(),
        }
    }

    /// Request shutdown through the existing root lifecycle owner.
    /// Acceptance is a request acknowledgement, not joined retirement.
    /// Missing or ambiguous shutdown authority remains a compile-time denial.
    ///
    /// # Errors
    /// Returns the exact already-stopping or already-stopped rejection.
    pub fn request_shutdown<ShutdownPath>(&self) -> Result<(), ShutdownRejection>
    where
        Event: InjectEvent<ShutdownRequested, ShutdownPath>,
    {
        self.lifecycle().request_shutdown()
    }

    /// Observe termination of the exact root through its lifecycle owner.
    /// Complete joined actor custody remains in application cleanup.
    pub fn termination(
        &self,
    ) -> impl Future<Output = Termination<P::Addr>> + use<P, Event, Families> {
        self.lifecycle().termination()
    }

    /// Observe the exact root's joined-retirement assessment through its lifecycle owner.
    /// The report can arrive while application work remains active; complete root
    /// custody and family shutdown remain in the application result.
    /// Cancelling this wait cannot cancel cleanup or another observer.
    pub fn retirement(
        &self,
    ) -> impl Future<Output = ActorRetirementReport> + use<P, Event, Families> {
        self.lifecycle().retirement()
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

    /// Retire the captured incarnation through its existing processing fence.
    /// Independent observers share completion; cancelling a wait does not cancel retirement.
    /// The mode reports graceful fencing or its original forced reason, not error-free cleanup.
    /// Complete native and callback failures remain in the application family result.
    /// Without an acquired completion the wait remains pending, matching other retirement observations.
    ///
    /// # Errors
    /// Returns the exact early refusal without affecting another incarnation.
    #[allow(
        private_bounds,
        reason = "native execution remains a sealed application composition proof"
    )]
    pub async fn retire_entity<Role, Position>(
        &self,
        role: Role,
        id: &<<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Id,
    ) -> Result<RetirementMode, EntityRetirementError>
    where
        Families: EntityFamilyAt<Role, Position>,
        <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Hosts:
            NativeEntityHost<
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Behavior,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::Terminal,
                <<Families as EntityFamilyAt<Role, Position>>::Definition as EntityDefinition>::ChildFailures,
            >,
{
        self.entities(role).retire_entity(id).await
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

impl<P, Event, Families> ApplicationHandle<P, Event, Families>
where
    P: Protocol<Addr = MailAddr>,
{
    /// Allocate one root-bound client with sole typed reply receiving.
    /// The reply capability in a domain command can infer `Replies`.
    /// Each call uses the same application allocator and a fresh external identity.
    ///
    /// # Errors
    /// Returns the existing allocation rejection, leaving this handle untouched.
    pub fn client<Replies>(
        &self,
    ) -> Result<ApplicationClient<EstablishedRecipient<P>, Replies>, ExternalActorError>
    where
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        ApplicationClient::establish(
            self.root.established_recipient(),
            &self.allocations,
            self.client_capacity,
        )
        .map_err(|(_, error)| error)
    }

    /// Allocate one root-bound sender with no reply or lifecycle authority.
    /// Its real external identity supplies message provenance; no reply protocol
    /// or raw sender address is required from the caller.
    ///
    /// # Errors
    /// Returns the existing allocation rejection, leaving this handle untouched.
    pub fn sender(&self) -> Result<ApplicationSender<EstablishedRecipient<P>>, ExternalActorError> {
        ApplicationSender::establish(self.root.established_recipient(), &self.allocations)
            .map_err(|(_, error)| error)
    }
}

/// One affine reply endpoint bound to an explicitly selected typed target.
/// Construction selects the target once. This client grants no lifecycle
/// authority and cannot be cloned to duplicate receiving.
pub struct ApplicationClient<Target, Replies>
where
    Replies: Protocol<Addr = MailAddr>,
{
    target: Target,
    caller: ExternalActor<Replies>,
}

impl<Target, Replies> ApplicationClient<Target, Replies>
where
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    fn establish(
        target: Target,
        allocations: &ApplicationAddresses,
        capacity: MailboxCapacity,
    ) -> Result<Self, (Target, ExternalActorError)> {
        match ExternalActor::establish(allocations, capacity) {
            Ok(caller) => Ok(Self { target, caller }),
            Err(error) => Err((target, error)),
        }
    }

    /// The real allocated sender identity of this client.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.caller.address()
    }

    /// Issue the exact reply capability without duplicating receive authority.
    #[must_use]
    pub fn recipient(&self) -> EstablishedRecipient<Replies> {
        self.caller.recipient()
    }

    /// Admit a command to the selected target using this client's identity.
    /// Admission is distinct from a later business reply.
    ///
    /// # Errors
    /// Returns the selected target's rejection with the complete original command.
    pub fn send(
        &self,
        command: Target::Message,
    ) -> impl Future<Output = Result<(), Target::Error>> + Send + use<'_, Target, Replies>
    where
        Target: ExternalTarget,
    {
        self.caller.send(&self.target, command)
    }

    /// Receive the next typed reply and its actual emitting actor's identity.
    /// Closed admission yields its accepted prefix before returning `None`.
    pub async fn receive(&mut self) -> Option<User<MailAddr, Replies::Msg>> {
        self.caller.receive().await
    }

    /// Close new replies while preserving the accepted prefix for receiving.
    pub fn close_admission(&self) {
        self.caller.close_admission();
    }
}

#[cfg(feature = "remote")]
impl<Target, Replies> ApplicationClient<EstablishedRecipient<Target>, Replies>
where
    Target: Protocol<Addr = MailAddr>,
    Replies: Protocol<Addr = MailAddr>,
    Replies::Msg: Send,
{
    /// Attempt the selected actor's mailbox without waiting for capacity.
    ///
    /// Uses this client's allocated sender identity and exact retained target.
    /// Permission must be checked before each attempt; this performs no
    /// authentication, retry or serialization. Acceptance is distinct from
    /// processing, a business reply and joined retirement.
    ///
    /// # Errors
    /// Returns the native [`TrySendError::Full`] or [`TrySendError::Closed`]
    /// with the complete original command. Closure establishes refused
    /// admission, not completed actor retirement.
    pub fn try_send(&self, command: Target::Msg) -> Result<(), TrySendError<Target::Msg>> {
        self.caller.try_send(&self.target, command)
    }
}

/// One external identity bound to a selected typed target for one-way commands.
/// Owns a fresh address without allocating a reply mailbox. The selected target
/// retains its own admission and lifecycle; dropping this sender does not close it.
pub struct ApplicationSender<Target> {
    target: Target,
    address: MailAddr,
}

impl<Target> ApplicationSender<Target> {
    fn establish(
        target: Target,
        allocations: &ApplicationAddresses,
    ) -> Result<Self, (Target, ExternalActorError)> {
        match allocations.allocate() {
            Ok(address) => Ok(Self { target, address }),
            Err(error) => Err((target, ExternalActorError::Allocation(error))),
        }
    }

    /// The real allocated sender identity supplied with each command.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.address
    }

    /// Admit a one-way command to the selected typed target.
    ///
    /// # Errors
    /// Returns the selected target's rejection with the complete original command.
    pub fn send(
        &self,
        command: Target::Message,
    ) -> impl Future<Output = Result<(), Target::Error>> + Send + use<'_, Target>
    where
        Target: ExternalTarget,
    {
        self.target.send_from(self.address, command)
    }
}

impl<P> ApplicationSender<EstablishedRecipient<P>>
where
    P: Protocol<Addr = MailAddr>,
{
    /// Attempt a one-way command without waiting for mailbox capacity.
    ///
    /// Uses this sender's original identity and the exact selected recipient.
    /// Stable Entity resolution uses [`Self::send`] instead.
    ///
    /// # Errors
    /// Returns Full or Closed with the complete original command, preserving
    /// the admission contract of [`ExternalActor::try_send`].
    pub fn try_send(&self, command: P::Msg) -> Result<(), TrySendError<P::Msg>> {
        let mut interpreter = ExtractLocalEndpoint;
        let endpoint = self.target.clone().interpret(&mut interpreter);
        endpoint.try_send_from(self.address, command)
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
            retirement: self.retirement.clone(),
        }
    }
}

impl<P: Protocol, Event> ApplicationLifecycle<P, Event> {
    /// Request shutdown through the root lifecycle lane.
    /// The unique typed ingress path is inferred through actor layers.
    /// An absent or ambiguous shutdown lane cannot be inferred.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection when the root is already stopping or has
    /// already stopped.
    pub fn request_shutdown<ShutdownPath>(&self) -> Result<(), ShutdownRejection>
    where
        Event: InjectEvent<ShutdownRequested, ShutdownPath>,
    {
        if self.root.termination_observation().try_get().is_some() {
            return Err(ShutdownRejection::AlreadyStopped);
        }
        let Some(control) = self.control.upgrade() else {
            return Err(self.root.shutdown_rejection());
        };
        request_actor_shutdown(
            &self.root,
            &control,
            Ingress::<ShutdownRequested, ShutdownPath>::new(),
        )
    }

    /// Observe the exact root's joined-retirement assessment.
    ///
    /// The report is available while application work is still running; it
    /// does not wait for final native-result handoff or Entity family shutdown.
    /// Independent observers share the fact, and cancelling a borrowed wait
    /// cannot cancel cleanup or another observer.
    pub fn retirement(&self) -> impl Future<Output = ActorRetirementReport> + use<P, Event> {
        let retirement = self.retirement.clone();
        async move { retirement.await }
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
            client_capacity: self.client_capacity,
        }
    }
}

impl<Api> ActorInterface<Api> {
    pub(crate) const fn new(
        api: Api,
        allocations: ApplicationAddresses,
        client_capacity: MailboxCapacity,
    ) -> Self {
        Self {
            api,
            allocations,
            client_capacity,
        }
    }

    /// Borrow the statically declared receptionist product.
    #[must_use]
    pub const fn api(&self) -> &Api {
        &self.api
    }

    /// Bind one explicitly exported target to an affine reply client.
    /// This grants no application lifecycle authority.
    ///
    /// # Errors
    /// Returns the complete original target with its allocation rejection.
    pub fn client_to<Target, Replies>(
        &self,
        target: Target,
    ) -> Result<ApplicationClient<Target, Replies>, (Target, ExternalActorError)>
    where
        Target: ExternalTarget,
        Replies: Protocol<Addr = MailAddr>,
        Replies::Msg: Send,
    {
        ApplicationClient::establish(target, &self.allocations, self.client_capacity)
    }

    /// Bind one explicitly exported target to a real one-way sender.
    ///
    /// # Errors
    /// Returns the complete original target with its allocation rejection.
    pub fn sender_to<Target>(
        &self,
        target: Target,
    ) -> Result<ApplicationSender<Target>, (Target, ExternalActorError)>
    where
        Target: ExternalTarget,
    {
        ApplicationSender::establish(target, &self.allocations)
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
        ExternalActor::establish(&self.allocations, self.client_capacity)
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
    pub(super) fn establish(
        allocations: &ApplicationAddresses,
        capacity: MailboxCapacity,
    ) -> Result<Self, ExternalActorError> {
        let address = allocations
            .allocate()
            .map_err(ExternalActorError::Allocation)?;
        let (control_liveness, owner, mailbox, receiver) =
            mailbox_channel::<Never, User<MailAddr, P::Msg>>(communication::Config::new(
                capacity.get(),
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

    /// Attempt delivery to an exact actor without waiting for mailbox capacity.
    ///
    /// Uses this external actor's allocated address as `User::from`. The
    /// recipient never resolves or retargets to a replacement. This operation
    /// performs no remote authentication; a caller enforcing permission must
    /// recheck it before each attempt. Existing mailbox synchronization remains.
    /// Stable Entity resolution and hydration use [`Self::send`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] or [`TrySendError::Closed`] with the exact
    /// original message. Closure establishes refused admission, not actor
    /// termination or joined retirement. Error Debug can reveal the payload;
    /// non-Debug messages remain supported through exhaustive matching.
    pub fn try_send<Target>(
        &self,
        target: &EstablishedRecipient<Target>,
        message: Target::Msg,
    ) -> Result<(), TrySendError<Target::Msg>>
    where
        Target: Protocol<Addr = MailAddr>,
    {
        let mut interpreter = ExtractLocalEndpoint;
        let endpoint = target.clone().interpret(&mut interpreter);
        endpoint.try_send_from(self.address, message)
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
    retirement: Observation<ActorRetirementReport>,
    allocations: ApplicationAddresses,
    client_capacity: MailboxCapacity,
    families: Families,
    #[cfg(feature = "remote")]
    pub(super) session: Option<Session>,
}

/// Explicit application lifecycle authority, separate from actor messaging.
pub struct ApplicationLifecycle<P: Protocol, Event> {
    root: ActorRef<P>,
    control: Weak<ControlSender<Event>>,
    retirement: Observation<ActorRetirementReport>,
}

/// A transport-neutral product of explicitly exported actor capabilities.
///
/// `Api` is application-defined. Bombay neither infers exports from topology
/// nor grants lifecycle authority through this value.
pub struct ActorInterface<Api> {
    api: Api,
    allocations: ApplicationAddresses,
    client_capacity: MailboxCapacity,
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
    mod invocation;
    use crate::actors::ActorExt;
    #[cfg(feature = "remote")]
    use crate::application::{ApplicationScope, MailboxCapacity};
    #[cfg(feature = "remote")]
    use crate::remote::ExportError;
    use behavior::{AllocationRejection, BehaviorBase, MessageProtocol, Never, User};
    use behavior_actors::{Exit, StopOnShutdown};
    use core::num::NonZeroUsize;
    #[cfg(feature = "remote")]
    use std::error::Error as StdError;
    #[cfg(feature = "remote")]
    use std::ptr;
    use std::sync::{Arc, Mutex};
    use tokio::task::JoinError;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::{Application, ExternalActor, ExternalActorError};
    use crate::entity::{
        ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
        EntityCapacity, EntityDefinition, EntityId, EntityShutdown,
    };
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::{
        ActorFailureAssessment, ActorRetirement, ActorRetirementReport, ActorSpace, Completion,
        RetirementAssessment, ShutdownLedger,
    };

    type AccountRetirement =
        Result<ActorRetirement<StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>, JoinError>;

    enum AccountReport {
        ActivationFailed(
            EntityId<Box<[u64]>>,
            ActivationId,
            EntityActivationError<Never, StopOnShutdown<ShutdownLedger>, Never, Vec<Never>>,
        ),
        AdmissionRefused(EntityId<Box<[u64]>>, AdmissionFailure<Vec<u64>>),
        ForcedRetirement(EntityId<Box<[u64]>>, ActivationId, DrainFailure),
        Retired(
            EntityId<Box<[u64]>>,
            ActivationId,
            AccountRetirement,
            ActorRetirementReport,
        ),
    }

    struct Accounts {
        reports: Arc<Mutex<Vec<AccountReport>>>,
    }

    #[derive(Clone, Copy)]
    struct AccountsRole;

    impl EntityDefinition for Accounts {
        type Id = Box<[u64]>;
        type Behavior = StopOnShutdown<ShutdownLedger>;
        type Hosts = ActorSpace<ShutdownLedger>;
        type HydrationError = Never;
        type Terminal = Never;
        type ChildFailures = Vec<Never>;

        #[expect(
            clippy::unused_async_trait_impl,
            reason = "hydration transfers domain identity only when its native future is polled"
        )]
        async fn hydrate(&self, id: EntityId<Self::Id>) -> Result<Self::Behavior, Never> {
            Ok(ShutdownLedger {
                entries: id.into_inner().into_vec(),
                received: Vec::new(),
            }
            .stop_on_shutdown())
        }

        fn activation_failed(
            &self,
            id: EntityId<Self::Id>,
            activation: ActivationId,
            failure: EntityActivationError<Never, Self::Behavior, Never, Vec<Never>>,
        ) {
            self.reports
                .lock()
                .expect("account report custody")
                .push(AccountReport::ActivationFailed(id, activation, failure));
        }

        fn admission_refused(&self, id: EntityId<Self::Id>, failure: AdmissionFailure<Vec<u64>>) {
            self.reports
                .lock()
                .expect("account report custody")
                .push(AccountReport::AdmissionRefused(id, failure));
        }

        fn forced_retirement(
            &self,
            id: &EntityId<Self::Id>,
            activation: ActivationId,
            failure: DrainFailure,
        ) {
            self.reports.lock().expect("account report custody").push(
                AccountReport::ForcedRetirement(id.clone(), activation, failure),
            );
        }

        fn retired(
            &self,
            id: &EntityId<Self::Id>,
            activation: ActivationId,
            retirement: AccountRetirement,
            retirement_report: ActorRetirementReport,
        ) {
            self.reports
                .lock()
                .expect("account report custody")
                .push(AccountReport::Retired(
                    id.clone(),
                    activation,
                    retirement,
                    retirement_report,
                ));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "retain the original allocation refusal, identity, retry and full retirement in one independent journey"
    )]
    async fn rejected_target_construction_returns_the_original_entity_identity_for_retry() {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let definition = Accounts {
            reports: Arc::clone(&reports),
        };
        let capacity = EntityCapacity::new(
            NonZeroUsize::new(1).expect("one resident"),
            NonZeroUsize::new(4).expect("four pending commands"),
        );
        let original = Box::<[u64]>::from([79, 83]);
        let allocation = original.as_ptr();
        let (outcome, root, notifications, families) = Application::new(
            ShutdownLedger {
                entries: vec![73],
                received: Vec::new(),
            }
            .stop_on_shutdown(),
        )
        .entity_family(
            AccountsRole,
            definition,
            DirectoryConfig::default(),
            capacity,
        )
        .run_with_entities(|mut application| async move {
            let target = application.entities(AccountsRole).entity(original);
            let allocations = application.allocations.clone();
            application.allocations = ApplicationAddresses::from_next(u64::MAX);
            let (target, error) = application
                .sender_to(target)
                .err()
                .expect("exhaustion returns the complete original target");
            assert_eq!(target.id().as_ptr(), allocation);
            assert_eq!(&**target.id(), &[79, 83]);
            assert!(matches!(
                error,
                ExternalActorError::Allocation(AllocationRejection::Exhausted)
            ));
            let (target, error) = application
                .client_to::<_, MessageProtocol<MailAddr, u64>>(target)
                .err()
                .expect("reply allocation also returns the complete original target");
            assert_eq!(target.id().as_ptr(), allocation);
            assert!(matches!(
                error,
                ExternalActorError::Allocation(AllocationRejection::Exhausted)
            ));
            application.allocations = allocations;
            let sender = application
                .sender_to(target)
                .unwrap_or_else(|_| panic!("the returned original target can be retried"));
            sender
                .send(vec![89, 97])
                .await
                .expect("native Entity admission activates the original logical identity");
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            sender.address()
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller host is entered"))
        .into_parts();
        let (sender, cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original caller completes"));
        assert!(matches!(&cleanup, Ok(())));
        let notifications =
            notifications.expect("both root notification results remain independent");
        notifications
            .termination
            .expect("actual root termination notification");
        notifications
            .retirement
            .expect("actual joined root report notification");
        let (_, root) = root.expect("independent original root retirement");
        assert_eq!(root.completion(), Some(&Completion::Stopped));
        assert_eq!(root.behavior().expect("root retained").base().entries, [73]);
        let (family, ()) = families;
        let (AccountsRole, family_retirement) = family.expect("family received");
        let (shutdown, metrics, disposal) = family_retirement.into_parts();
        assert!(disposal.is_none());
        assert_eq!(metrics.activations, 1);
        assert_eq!(metrics.residents, 0);
        let EntityShutdown::Settled {
            represented,
            entities,
        } = shutdown
        else {
            panic!("the native account family settles");
        };
        assert_eq!(represented, 1);
        assert_eq!(entities, [EntityId::new(Box::<[u64]>::from([79, 83]))]);
        let reports = reports.lock().expect("complete original account reports");
        assert_eq!(reports.len(), 1);
        for report in &*reports {
            match report {
                AccountReport::Retired(id, _activation, retirement, report) => {
                    assert_eq!(&**id.get(), &[79, 83]);
                    let retirement = retirement.as_ref().expect("exact account task custody");
                    let custody = match retirement {
                        ActorRetirement::Completed {
                            custody,
                            completion,
                            ..
                        } => {
                            assert_eq!(*completion, Completion::Stopped);
                            custody
                        }
                        ActorRetirement::OwnerCancelled { custody, .. } => custody,
                        _ => panic!("fenced retirement returns completion or owner cancellation"),
                    };
                    let actor = custody.behavior.base();
                    assert_eq!(actor.entries, [79, 83]);
                    assert_eq!(actor.received, [User::new(sender, vec![89, 97])]);
                    assert_eq!(custody.user, []);
                    assert_eq!(custody.descendants, []);
                    assert_eq!(custody.child_failures, []);
                    assert_eq!(report.retirement(), RetirementAssessment::Established);
                    assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
                }
                AccountReport::ActivationFailed(id, activation, _failure) => {
                    panic!("unexpected account activation failure: {id:?} {activation:?}");
                }
                AccountReport::AdmissionRefused(id, failure) => {
                    panic!("unexpected account admission refusal: {id:?} {failure:?}");
                }
                AccountReport::ForcedRetirement(id, activation, failure) => {
                    panic!(
                        "unexpected forced account retirement: {id:?} {activation:?} {failure:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_root_client_allocation_preserves_live_application_authority() {
        let (outcome, root, notifications, ()) = Application::new(
            ShutdownLedger {
                entries: vec![13, 17],
                received: Vec::new(),
            }
            .stop_on_shutdown(),
        )
        .run_with(|mut application| async move {
            let root = application.root().address();
            application.allocations = ApplicationAddresses::from_next(u64::MAX);
            let client = application.client::<MessageProtocol<MailAddr, u64>>();
            assert!(matches!(
                client,
                Err(ExternalActorError::Allocation(
                    AllocationRejection::Exhausted
                ))
            ));
            let sender = application.sender();
            assert!(matches!(
                sender,
                Err(ExternalActorError::Allocation(
                    AllocationRejection::Exhausted
                ))
            ));
            assert_eq!(application.root().address(), root);
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
        })
        .await
        .unwrap_or_else(|_| panic!("the actual caller host is entered"))
        .into_parts();
        let ((), cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("work completed"));
        cleanup.expect("the actual cleanup producer completed independently");
        let notifications = notifications.expect("both original root notification results survive");
        notifications
            .termination
            .expect("actual ordinary termination notification");
        notifications
            .retirement
            .expect("actual joined retirement notification");
        let (_, retirement) = root.expect("the original root retirement remains available");
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        let custody = retirement
            .custody()
            .expect("the root retains its original data");
        assert_eq!(custody.behavior.base().entries, [13, 17]);
        assert_eq!(custody.behavior.base().received, []);
    }

    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn export_client_refusals_keep_originals_and_native_allocation_cause() {
        let scope = ApplicationScope {
            allocations: ApplicationAddresses::from_next(u64::MAX - 2),
            client_capacity: MailboxCapacity::default(),
            mailbox_capacity: MailboxCapacity::default(),
        };
        let external = scope.external::<MessageProtocol<MailAddr, u64>>().unwrap();
        let external_recipient = external.recipient();
        let expected_external = external_recipient.clone();
        let (outcome, native, notifications, ()) = scope
            .with_root(
                ShutdownLedger {
                    entries: vec![13, 17],
                    received: Vec::new(),
                }
                .stop_on_shutdown(),
            )
            .run_with(move |application| async move {
                let root = application.root().established_recipient();
                let expected_root = root.clone();
                let unowned = application
                    .export_client::<_, MessageProtocol<MailAddr, u64>>(external_recipient);
                let available =
                    application.export_client::<_, MessageProtocol<MailAddr, u64>>(root.clone());
                let construction =
                    application.export_client::<_, MessageProtocol<MailAddr, u64>>(root);
                let shutdown = application.request_shutdown();
                let report = application.lifecycle().retirement().await;
                (
                    unowned,
                    available,
                    construction,
                    expected_root,
                    shutdown,
                    report,
                    external,
                )
            })
            .await
            .unwrap_or_else(|_| panic!("the actual Application starts"))
            .into_parts();
        let (originals, cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original Work results are retained"));
        let (_, retirement) = native.expect("the original root joins");
        let notifications = notifications.expect("both notification results are acquired");
        let (unowned, available, construction, expected_root, shutdown, report, external) =
            originals;
        assert!(cleanup.is_ok());
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        let custody = retirement
            .custody()
            .expect("the native root state survives");
        assert_eq!(custody.behavior.base().entries, [13, 17]);
        assert_eq!(custody.behavior.base().received, []);
        assert_eq!(custody.descendants, []);
        assert_eq!(custody.child_failures, []);
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        assert!(shutdown.is_ok());
        assert!(matches!(
            &unowned,
            Err((target, ExportError::UnownedTarget)) if target == &expected_external
        ));
        assert!(
            available.is_ok(),
            "the refusal preserves the last available reply endpoint"
        );
        let (returned_root, error) = construction
            .as_ref()
            .err()
            .expect("the owned root retains its native reply-allocation refusal");
        let ExportError::Endpoint { cause } = error else {
            panic!("native reply allocation retains its exact endpoint cause");
        };
        let ExternalActorError::Allocation(rejection) = cause;
        let source = StdError::source(error).expect("the actual native cause is chained");
        let retained_address = ptr::from_ref(source).addr();
        let original_address = ptr::from_ref(cause).addr();
        let allocation = StdError::source(cause).expect("the actual allocation cause is retained");
        assert_eq!(returned_root, &expected_root);
        assert_eq!(*rejection, AllocationRejection::Exhausted);
        assert_eq!(retained_address, original_address);
        assert_eq!(allocation.to_string(), rejection.to_string());
        drop((
            unowned,
            available,
            construction,
            external,
            expected_external,
            expected_root,
        ));
    }

    #[test]
    fn external_actor_drop_publishes_normal_termination_to_its_exact_recipient() {
        let allocations = ApplicationAddresses::new();
        let external = ExternalActor::<MessageProtocol<MailAddr, u64>>::establish(
            &allocations,
            crate::MailboxCapacity::default(),
        )
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
    use core::future::{Future, poll_fn};
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};
    use std::mem;
    use std::panic::{panic_any, resume_unwind};
    use std::ptr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    use crate::ActorExecutionOutcome;
    use crate::ActorSpace;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ApplicationLifecycle;
    use crate::application::{ActorInterface, Application, WorkOutcome};
    use crate::launch::launch_inert;

    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::local::environment::LocalResidual;
    use crate::observe;
    use crate::terminal::{
        ActorCustody, ActorFailureAssessment, ActorRetirement, RetirementAssessment,
        RetirementNotificationError,
    };
    use crate::topology::{ApplicationTopology, Hosts, RootRegistration};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};

    use crate::{LedgerProtocol, NativeHost, Runtime, ShutdownLedger};
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
        let (_old_report_publication, old_retirement) = observe::pair();
        let lifecycle = ApplicationLifecycle {
            root: old.actor.clone(),
            control: old.shutdown_control.clone(),
            retirement: old_retirement,
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
                    operation_failures,
                    descendant_report,
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
        assert_eq!(operation_failures, ActorFailureAssessment::Incomplete);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::NotEstablished
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::Incomplete
        );
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
        let (_fresh_report_publication, fresh_retirement) = observe::pair();
        let fresh_lifecycle = ApplicationLifecycle {
            root: fresh.actor.clone(),
            control: fresh.shutdown_control.clone(),
            retirement: fresh_retirement,
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
                    operation_failures,
                    descendant_report,
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
        assert_eq!(operation_failures, ActorFailureAssessment::Incomplete);
        assert_eq!(
            descendant_report.retirement(),
            RetirementAssessment::NotEstablished
        );
        assert_eq!(
            descendant_report.failures(),
            ActorFailureAssessment::Incomplete
        );
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
    enum LedgerSpaceDisposal {
        Return,
        #[expect(
            clippy::redundant_allocation,
            reason = "Box is the original panic carrier; Arc separately proves cause lifetime without opening or reboxing the native payload"
        )]
        Unwind(Box<Arc<Vec<u64>>>),
    }

    struct RetiringLedgerSpace {
        actors: ActorSpace<LedgerProtocol>,
        disposal: LedgerSpaceDisposal,
        retired_spaces: Arc<AtomicUsize>,
    }

    impl NativeHost for RetiringLedgerSpace {
        type Runtime = Runtime;
        fn runtime(&self) -> &Self::Runtime {
            const RUNTIME: Runtime = Runtime::new();
            &RUNTIME
        }
    }

    impl Hosts<LedgerProtocol> for RetiringLedgerSpace {
        fn space(&self) -> &ActorSpace<LedgerProtocol> {
            &self.actors
        }
    }

    impl RootRegistration<LedgerProtocol> for RetiringLedgerSpace {
        fn root_space(&self) -> &ActorSpace<LedgerProtocol> {
            &self.actors
        }
    }

    impl ApplicationTopology<StopOnShutdown<ShutdownLedger>, ()> for RetiringLedgerSpace {
        type Descendants = Never;
    }

    impl Drop for RetiringLedgerSpace {
        fn drop(&mut self) {
            self.retired_spaces.fetch_add(1, Ordering::SeqCst);
            match mem::replace(&mut self.disposal, LedgerSpaceDisposal::Return) {
                LedgerSpaceDisposal::Return => {}
                LedgerSpaceDisposal::Unwind(payload) => resume_unwind(payload),
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one public Application controller proves independent report axes, actual space disposal, work barriers and complete native custody"
    )]
    async fn root_retirement_report_is_available_before_application_work_finishes() {
        for disposal in [
            LedgerSpaceDisposal::Return,
            LedgerSpaceDisposal::Unwind(Box::new(Arc::new(vec![167, 173]))),
        ] {
            let expected_cause = match &disposal {
                LedgerSpaceDisposal::Return => None,
                LedgerSpaceDisposal::Unwind(payload) => Some((
                    Arc::downgrade(payload.as_ref()),
                    ptr::from_ref(payload.as_ref()).cast::<()>(),
                )),
            };
            let expected_failures = match &expected_cause {
                None => ActorFailureAssessment::NoFailuresFound,
                Some(_) => ActorFailureAssessment::FailuresFound,
            };
            let actors = ActorSpace::<LedgerProtocol>::new();
            let retired_spaces = Arc::new(AtomicUsize::new(0));
            let payload = vec![157, 163];
            let allocation = payload.as_ptr();
            let (report_publication, report_received) = oneshot::channel();
            let (release_work, work_released) = oneshot::channel();
            let (execution, receiving) = Application::with_topology(
                StopOnShutdown::new(ShutdownLedger {
                    entries: payload,
                    received: Vec::new(),
                }),
                RetiringLedgerSpace {
                    actors: actors.clone(),
                    disposal,
                    retired_spaces: Arc::clone(&retired_spaces),
                },
            )
            .execute_with(move |application| async move {
                let lifecycle = application.lifecycle();
                {
                    let mut cancelled = pin!(application.retirement());
                    let mut context = Context::from_waker(Waker::noop());
                    let pending = Future::poll(cancelled.as_mut(), &mut context);
                    assert!(matches!(pending, Poll::Pending));
                }
                let independent = lifecycle.clone();
                let requested = lifecycle.request_shutdown();
                requested.expect("the exact root accepts its first stop request");
                let report = application.retirement().await;
                let independently_observed = independent.retirement().await;
                assert_eq!(independently_observed, report);
                let published = report_publication.send(report);
                published.expect("the independent work observer remains present");
                work_released
                    .await
                    .expect("work has its explicit outside release");
                report
            })
            .unwrap_or_else(|_| panic!("the public application uses the entered executor"));
            let mut execution = pin!(execution);
            let report = timeout(Duration::from_secs(10), async {
                tokio::select! {
                    report = report_received => report.expect("work acquired the actual joined report"),
                    () = &mut execution => panic!("work cannot finish before its outside release"),
                }
            })
            .await
            .expect("joined report must be available without the work-completion barrier");
            assert_eq!(retired_spaces.load(Ordering::SeqCst), 1);
            assert_eq!(report.retirement(), RetirementAssessment::Established);
            assert_eq!(report.failures(), expected_failures);
            let missing = actors.resolve(&MailAddr::APPLICATION_ROOT);
            assert!(missing.is_none());
            let mut receiving = pin!(receiving);
            let mut context = Context::from_waker(Waker::noop());
            let pending_receiving = Future::poll(receiving.as_mut(), &mut context);
            assert!(matches!(pending_receiving, Poll::Pending));
            let released = release_work.send(());
            released.expect("the report did not finish or cancel application work");
            execution.await;
            let (work, native, notifications, ()) = receiving.await.into_parts();
            let WorkOutcome::Completed {
                output,
                cleanup: Ok(()),
            } = work
            else {
                panic!("the actual caller-local work and cleanup finish independently");
            };
            assert_eq!(output, report);
            let notifications =
                notifications.expect("both original notification results are retained");
            notifications
                .termination
                .expect("ordinary termination succeeded");
            notifications
                .retirement
                .expect("joined-report publication succeeded");
            let (origin, native) =
                native.expect("the native root result remains independently retained");
            assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
            let ActorRetirement::Completed {
                completion: Completion::Stopped,
                interpretation: None,
                source: None,
                settlements,
                custody:
                    ActorCustody {
                        behavior,
                        control,
                        user,
                        descendants,
                        child_failures,
                        capability_failures,
                        additional_failures,
                        received_interpretation: None,
                        received_source: None,
                        source_index: None,
                        acquired_ingress: None,
                        retirement_failures,
                        terminal_report: None,
                        unread_owner_cancellation: None,
                    },
            } = native
            else {
                panic!("the exact stopped native root and all of its lanes survive");
            };
            assert_eq!(behavior.base().entries, [157, 163]);
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(behavior.base().received.len(), 0);
            assert!(control.is_empty() && user.is_empty());
            assert_eq!(descendants.len(), 0);
            assert_eq!(child_failures, []);
            assert!(capability_failures.is_empty());
            assert!(additional_failures.is_empty());
            match expected_cause {
                None => assert!(retirement_failures.is_empty()),
                Some((original_cause, allocation)) => {
                    assert_eq!(original_cause.strong_count(), 1);
                    assert_eq!(retirement_failures.len(), 1);
                    let payload = retirement_failures
                        .into_iter()
                        .next()
                        .expect("the exact capability disposal cause remains native");
                    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), allocation);
                    drop(payload);
                    assert_eq!(original_cause.strong_count(), 0);
                }
            }
            assert_eq!(settlements.len(), 1);
            let settlement = settlements
                .into_iter()
                .next()
                .expect("one actual stop settlement");
            assert!(settlement.creations.is_empty());
            assert!(matches!(settlement.sends.owned, NoSends));
            assert!(matches!(settlement.sends.inner, NoSends));
            assert!(matches!(settlement.become_, Step::Stop(Stopped)));
        }
    }

    struct RetirementObserver {
        failure: Mutex<Option<Arc<Vec<u64>>>>,
        application_work: Waker,
    }

    impl Wake for RetirementObserver {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // Ensure the real work can inspect the already committed fact even
            // when this independently registered observer panics during wake.
            self.application_work.wake_by_ref();
            let failure = self
                .failure
                .lock()
                .expect("the observer owns its original panic")
                .take();
            panic_any(failure.expect("the observer panics once with its original allocation"));
        }
    }

    #[tokio::test]
    async fn root_report_notification_fault_retains_native_result_and_committed_report() {
        let actors = ActorSpace::<LedgerProtocol>::new();
        let failure = Arc::new(vec![167_u64, 173]);
        let retained_failure = Arc::downgrade(&failure);
        let values = vec![179, 181];
        let allocation = values.as_ptr();
        let (execution, receiving) = Application::with_topology(
            StopOnShutdown::new(ShutdownLedger {
                entries: values,
                received: Vec::new(),
            }),
            actors.clone(),
        )
        .execute_with(move |application| async move {
            let lifecycle = application.lifecycle();
            let mut observer = pin!(application.retirement());
            poll_fn(|context| {
                let observer_waker = Waker::from(Arc::new(RetirementObserver {
                    failure: Mutex::new(Some(Arc::clone(&failure))),
                    application_work: context.waker().clone(),
                }));
                let mut observer_context = Context::from_waker(&observer_waker);
                let pending = Future::poll(observer.as_mut(), &mut observer_context);
                assert!(matches!(pending, Poll::Pending));
                Poll::Ready(())
            })
            .await;
            drop(failure);
            let stopped = lifecycle.request_shutdown();
            stopped.expect("the actual root accepts shutdown");
            let report = application.retirement().await;
            let independently_observed = observer.await;
            assert_eq!(report, independently_observed);
            report
        })
        .unwrap_or_else(|_| panic!("the public application uses the entered executor"));
        let ((), (work, native, notifications, ())) = timeout(Duration::from_secs(10), async {
            tokio::join!(execution, async { receiving.await.into_parts() })
        })
        .await
        .expect("a report observer panic cannot lose the actor result or stall cleanup");
        let WorkOutcome::Completed {
            output: report,
            cleanup: Ok(()),
        } = work
        else {
            panic!("work observes the committed report and actual cleanup finishes");
        };
        assert_eq!(report.retirement(), RetirementAssessment::Established);
        // The notification fault occurs after this immutable actor snapshot.
        assert_eq!(report.failures(), ActorFailureAssessment::NoFailuresFound);
        let notifications = notifications.expect("the complete notification product is retained");
        notifications
            .termination
            .expect("ordinary termination succeeded independently");
        let Err(RetirementNotificationError::Panicked { payload }) = notifications.retirement
        else {
            panic!("the original report observer panic has its own receipt");
        };
        assert_eq!(retained_failure.strong_count(), 1);
        let (_, native) = native.expect("the complete native root survives report notification");
        let ActorRetirement::Completed {
            custody: ActorCustody { behavior, .. },
            completion: Completion::Stopped,
            ..
        } = native
        else {
            panic!("the native stopped actor retains its original state");
        };
        assert_eq!(behavior.base().entries, [179, 181]);
        assert_eq!(behavior.base().entries.as_ptr(), allocation);
        let missing = actors.resolve(&MailAddr::APPLICATION_ROOT);
        assert!(missing.is_none());
        drop(payload);
        assert_eq!(retained_failure.strong_count(), 0);
    }

    #[tokio::test]
    async fn external_customer_owns_admission_without_behavior_shutdown() {
        let interface = ActorInterface::new(
            (),
            ApplicationAddresses::new(),
            crate::MailboxCapacity::default(),
        );
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
