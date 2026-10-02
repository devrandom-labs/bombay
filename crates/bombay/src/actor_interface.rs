//! Transport-neutral access to explicitly exported actor capabilities.

use core::fmt;
use std::future::Future;
use std::sync::Arc;

use behavior::{
    AllocationRejection, EstablishedRecipient, InterpretEstablished, Never, Protocol, User,
};
use behavior_actors::Exit;
use communication::{Consumer, ControlSender, Received, mailbox_channel};

use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::{AdmissionFailure, EntityDefinition, EntityRef, NativeEntityHost};
use crate::local::{ActorRef, Admission, AdmissionClosure, SendError, Termination};
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
    D::Hosts: NativeEntityHost<D::Behavior, D::Terminal>,
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
/// `Api` is application-defined. Bombay neither infers exports from topology
/// nor grants lifecycle authority through this value.
pub struct ActorInterface<Api> {
    api: Api,
    allocations: ApplicationAddresses,
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

#[cfg(test)]
mod tests {
    use behavior::MessageProtocol;
    use behavior_actors::Exit;

    use super::{ApplicationAddresses, ExternalActor, ExtractLocalEndpoint, MailAddr};

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
