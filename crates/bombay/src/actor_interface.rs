//! Transport-neutral access to explicitly exported actor capabilities.

use std::future::Future;
use std::sync::Arc;

use behavior::{
    AllocationRejection, EstablishedRecipient, InterpretEstablished, Never, Protocol, User,
};
use behavior_actors::Exit;
use bombay_address::{AddressSpace, ClaimError, Lease};
use communication::{Consumer, ControlSender, Received, mailbox_channel};

use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{AdmissionFailure, EntityDefinition, EntityRef, NativeEntityHost};
use crate::local::{ActorRef, Admission, SendError, Termination};
use crate::observe::{Publisher, pair};

const EXTERNAL_USER_CAPACITY: usize = 1_024;

mod target_sealed {
    pub trait Sealed {}
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
    D::Hosting: NativeEntityHost<D::Behavior, D::Terminal>,
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

/// A transport-neutral product of explicitly exported actor capabilities.
///
/// `Api` is application-defined. Bombay neither infers exports from hosting
/// topology nor grants lifecycle authority through this value.
pub struct ActorInterface<Api> {
    api: Api,
    allocations: ApplicationAddresses,
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
    /// The returned actor owns one fresh claimed address, one exact cloneable
    /// recipient, and the only receive authority for its mailbox.
    ///
    /// # Errors
    ///
    /// Returns the exact allocation or Address claim failure. No raw address
    /// or unclaimed recipient is returned on failure.
    pub fn external<P>(&self) -> Result<ExternalActor<P>, ExternalActorError>
    where
        P: Protocol<Addr = MailAddr>,
        P::Msg: Send,
    {
        ExternalActor::establish(&self.allocations)
    }
}

/// Exact failure to establish an external actor endpoint.
#[derive(Debug, thiserror::Error)]
pub enum ExternalActorError {
    /// Bombay's one never-wrapping application address source is exhausted.
    #[error("the Bombay application has exhausted its local address space")]
    Allocation(#[source] AllocationRejection),
    /// Address could not commit the exact external endpoint claim.
    #[error("the external actor address could not be claimed")]
    Address(ClaimError<MailAddr>),
}

/// One claimed external actor with affine receive authority.
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
    lease: Option<Lease<MailAddr, ActorRef<P>>>,
    termination: Option<Publisher<Termination<MailAddr>>>,
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
        let addresses = AddressSpace::new();
        let lease = addresses
            .try_claim(address, endpoint.clone())
            .map_err(ExternalActorError::Address)?;
        let recipient = endpoint.established_recipient();
        Ok(Self {
            address,
            recipient,
            admission,
            _control_liveness: control_liveness,
            receiver,
            lease: Some(lease),
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
    /// The external actor's own claimed address is always used as the message
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
            Some(()) | None => {}
        }
    }
}

impl<P> Drop for ExternalActor<P>
where
    P: Protocol<Addr = MailAddr>,
{
    fn drop(&mut self) {
        match self.admission.close() {
            Some(()) | None => {}
        }
        if let Some(lease) = self.lease.take() {
            lease.release();
        }
        if let Some(termination) = self.termination.take() {
            termination.complete(Ok(Exit::Normal));
        }
    }
}

pub(crate) struct ExtractLocalEndpoint;

impl<P> InterpretEstablished<P> for ExtractLocalEndpoint
where
    P: Protocol<Addr = MailAddr>,
{
    type Output = ActorRef<P>;

    fn interpret_established(&mut self, endpoint: ActorRef<P>) -> Self::Output {
        endpoint
    }
}
