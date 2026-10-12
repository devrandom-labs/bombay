use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{
    AdmissionFailure, EntityDefinition, EntityFamilyAt, EntityRef, NativeEntityHost, Passivation,
};
use crate::local::endpoint::{ActorRef, ExtractLocalEndpoint, SendError, request_actor_shutdown};
use crate::local::ingress::{Admission, AdmissionClosure};
use crate::observe::{Publisher, pair};
use crate::termination::Termination;
use behavior::{
    AllocationRejection, EstablishedRecipient, Ingress, InjectEvent, MessageProtocol, Never,
    Protocol, User,
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
        ExternalActor::establish(&self.allocations)
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
        ApplicationClient::establish(target, &self.allocations)
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
        ApplicationClient::establish(self.root.established_recipient(), &self.allocations)
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
    ) -> Result<Self, (Target, ExternalActorError)> {
        match ExternalActor::establish(allocations) {
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

/// One external identity bound to a selected typed target for one-way commands.
/// Its uninhabited reply protocol exposes no receiving or lifecycle authority.
pub struct ApplicationSender<Target> {
    target: Target,
    caller: ExternalActor<MessageProtocol<MailAddr, Never>>,
}

impl<Target> ApplicationSender<Target> {
    fn establish(
        target: Target,
        allocations: &ApplicationAddresses,
    ) -> Result<Self, (Target, ExternalActorError)> {
        match ExternalActor::establish(allocations) {
            Ok(caller) => Ok(Self { target, caller }),
            Err(error) => Err((target, error)),
        }
    }

    /// The real allocated sender identity supplied with each command.
    #[must_use]
    pub const fn address(&self) -> MailAddr {
        self.caller.address()
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
        self.caller.send(&self.target, command)
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
        ApplicationClient::establish(target, &self.allocations)
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
    use crate::actors::ActorExt;
    use behavior::{AllocationRejection, BehaviorBase, MessageProtocol, Never, User};
    use behavior_actors::{Exit, StopOnShutdown};
    use core::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use tokio::task::JoinError;

    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::{Application, ExternalActor, ExternalActorError};
    use crate::entity::{
        ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
        EntityCapacity, EntityDefinition, EntityId, EntityShutdown,
    };
    use crate::local::endpoint::ExtractLocalEndpoint;
    use crate::{ActorRetirement, ActorSpace, Completion, ShutdownLedger};

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
        Retired(EntityId<Box<[u64]>>, ActivationId, AccountRetirement),
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
        ) {
            self.reports
                .lock()
                .expect("account report custody")
                .push(AccountReport::Retired(id.clone(), activation, retirement));
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
        let (outcome, root, families) = Application::new(
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
        .unwrap_or_else(|_| panic!("the actual caller host is entered"));
        let (sender, cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original caller completes"));
        assert!(matches!(&cleanup, Ok(())));
        let (_, root) = root.expect("independent original root retirement");
        assert_eq!(root.completion(), Some(&Completion::Stopped));
        assert_eq!(root.behavior().expect("root retained").base().entries, [73]);
        let (family, ()) = families;
        let (AccountsRole, (shutdown, metrics, disposal)) = family.expect("family received");
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
                AccountReport::Retired(id, _activation, retirement) => {
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
        let outcome = Application::new(
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
        .unwrap_or_else(|_| panic!("the actual caller host is entered"));
        let ((), cleanup) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("work completed"));
        let (_, retirement) = cleanup.expect("the original root retirement remains available");
        assert_eq!(retirement.completion(), Some(&Completion::Stopped));
        let custody = retirement
            .custody()
            .expect("the root retains its original data");
        assert_eq!(custody.behavior.base().entries, [13, 17]);
        assert_eq!(custody.behavior.base().received, []);
    }

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
        let old_result = old.task.finish().await;
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
        let retired = fresh.task.finish().await;
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
