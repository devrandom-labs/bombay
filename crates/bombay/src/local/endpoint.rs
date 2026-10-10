use crate::address::MailAddr;
use crate::local::ingress::{Admission, AdmissionClosure, EndpointMailbox, LocalIngress};
use crate::observe::{Observation, affine_pair};
use crate::terminal::ActorRetirementReport;
use crate::termination::Termination;
use behavior::{
    Behavior, EstablishedRecipient, Ingress, InjectEvent, InterpretEstablished, Protocol, User,
};
use behavior_actors::{ShutdownRejection, ShutdownRequested};
use communication::{ControlSender, MailboxRef, TrySendError, UserClosed};
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use std::sync::Weak;

impl<P> fmt::Debug for ActorRef<P>
where
    P: Protocol,
    P::Addr: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRef")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<B> fmt::Debug for InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledActor")
            .field("recipient", &self.recipient)
            .finish_non_exhaustive()
    }
}

impl<B> Clone for InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    fn clone(&self) -> Self {
        Self {
            recipient: self.recipient.clone(),
            control: self.control.clone(),
            retirement: self.retirement.clone(),
            behavior: PhantomData,
        }
    }
}

impl<B> InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    pub(crate) fn new(
        recipient: ActorRef<B::Protocol>,
        control: ControlSender<B::Event>,
        retirement: Observation<ActorRetirementReport>,
    ) -> Self {
        Self {
            recipient,
            control,
            retirement,
            behavior: PhantomData,
        }
    }

    /// Observe the runtime's joined-retirement assessment for this exact child.
    ///
    /// Independent waits share the same published report. Cancelling one wait
    /// does not consume it or cancel another observer. This proves a different
    /// fact from ordinary termination and grants no additional stop authority.
    pub fn retirement(&self) -> impl Future<Output = ActorRetirementReport> + use<B> {
        let retirement = self.retirement.clone();
        async move { retirement.await }
    }

    pub(crate) fn recipient(&self) -> ActorRef<B::Protocol> {
        self.recipient.clone()
    }

    pub(crate) fn request_shutdown<TargetPath>(
        &self,
        ingress: Ingress<ShutdownRequested, TargetPath>,
    ) -> Result<(), ShutdownRejection>
    where
        B::Event: InjectEvent<ShutdownRequested, TargetPath>,
    {
        request_actor_shutdown(&self.recipient, &self.control, ingress)
    }
}

impl<P: Protocol> Clone for ActorRef<P> {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            endpoint: self.endpoint.clone(),
            termination: self.termination.clone(),
            protocol: PhantomData,
        }
    }
}

impl<P: Protocol> ActorRef<P> {
    pub(in crate::local) fn new(
        address: P::Addr,
        endpoint: EndpointMailbox<P::Addr, P::Msg>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            endpoint,
            termination,
            protocol: PhantomData,
        }
    }

    pub(crate) fn external(
        address: P::Addr,
        mailbox: MailboxRef<User<P::Addr, P::Msg>>,
        admission: Weak<Admission<User<P::Addr, P::Msg>>>,
        termination: Observation<Termination<P::Addr>>,
    ) -> Self {
        Self {
            address,
            endpoint: EndpointMailbox::Standard { mailbox, admission },
            termination,
            protocol: PhantomData,
        }
    }

    /// Observe the retained terminal result of this exact incarnation.
    ///
    /// Every clone refers to the same one-publication fact. Resolving a later
    /// actor at the same address produces a different observation.
    ///
    /// # Errors
    ///
    /// Resolves to the exact crash when this incarnation terminates
    /// abnormally; normal exits retain their typed exit reason.
    pub fn termination(&self) -> impl Future<Output = Termination<P::Addr>> + use<P> {
        let observation = self.termination.clone();
        async move { observation.await }
    }

    pub(crate) fn termination_observation(&self) -> Observation<Termination<P::Addr>> {
        self.termination.clone()
    }

    pub(crate) fn shutdown_rejection(&self) -> ShutdownRejection {
        if self.termination.try_get().is_some() {
            ShutdownRejection::AlreadyStopped
        } else {
            ShutdownRejection::AlreadyStopping
        }
    }

    /// The typed address of this exact actor reference.
    #[must_use]
    pub const fn address(&self) -> P::Addr {
        self.address
    }

    /// Admit one user message from an explicitly supplied typed origin through
    /// the behavior's complete event sum.
    ///
    /// This reference resolves only the target. It does not claim, host, or
    /// validate `from`; the external boundary that owns that identity must
    /// supply it truthfully.
    ///
    /// # Errors
    ///
    /// Returns [`SendError`] with the original message when this incarnation's
    /// user lane is no longer live.
    pub async fn send_from(&self, from: P::Addr, message: P::Msg) -> Result<(), SendError<P::Msg>> {
        match &self.endpoint {
            EndpointMailbox::Standard { mailbox, .. } => mailbox
                .send(User::new(from, message))
                .await
                .map_err(SendError::from_standard_rejection),
            EndpointMailbox::Entity { mailbox, .. } => mailbox
                .send(LocalIngress::Message(User::new(from, message)))
                .await
                .map_err(SendError::from_entity_rejection),
        }
    }

    pub(crate) fn try_send_from(
        &self,
        from: P::Addr,
        message: P::Msg,
    ) -> Result<(), TrySendError<P::Msg>> {
        match &self.endpoint {
            EndpointMailbox::Standard { mailbox, .. } => mailbox
                .try_send(User::new(from, message))
                .map_err(|rejected| match rejected {
                    TrySendError::Full(user) => TrySendError::Full(user.message),
                    TrySendError::Closed(user) => TrySendError::Closed(user.message),
                }),
            EndpointMailbox::Entity { mailbox, .. } => mailbox
                .try_send(LocalIngress::Message(User::new(from, message)))
                .map_err(|rejected| match rejected {
                    TrySendError::Full(LocalIngress::Message(user)) => {
                        TrySendError::Full(user.message)
                    }
                    TrySendError::Closed(LocalIngress::Message(user)) => {
                        TrySendError::Closed(user.message)
                    }
                    TrySendError::Full(LocalIngress::Fence(_))
                    | TrySendError::Closed(LocalIngress::Fence(_)) => {
                        unreachable!("ordinary ActorRef delivery creates only message ingress")
                    }
                }),
        }
    }

    pub(crate) async fn fence(&self) -> Result<(), crate::entity::FenceFailure> {
        let (publisher, observation) = affine_pair();
        let EndpointMailbox::Entity { mailbox, .. } = &self.endpoint else {
            return Err(crate::entity::FenceFailure::Enqueue);
        };
        if mailbox.send(LocalIngress::Fence(publisher)).await.is_err() {
            return Err(crate::entity::FenceFailure::Enqueue);
        }
        observation.await
    }
}

impl<P> ActorRef<P>
where
    P: Protocol<Addr = MailAddr>,
{
    /// Issue the exact recipient for this installed incarnation.
    ///
    /// The capability retains this endpoint directly. It performs no address
    /// lookup and never retargets to a replacement at the same logical
    /// address.
    #[must_use]
    pub fn established_recipient(&self) -> EstablishedRecipient<P> {
        EstablishedRecipient::issued(self.clone())
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<M> SendError<M> {
    /// Recover the exact message rejected by the closed actor reference.
    #[must_use]
    pub fn into_message(self) -> M {
        self.0
    }

    fn from_standard_rejection<A>(rejected: UserClosed<User<A, M>>) -> Self {
        let UserClosed(user) = rejected;
        Self(user.message)
    }

    fn from_entity_rejection<A>(rejected: UserClosed<LocalIngress<A, M>>) -> Self {
        let UserClosed(LocalIngress::Message(user)) = rejected else {
            unreachable!("ordinary ActorRef delivery creates only message ingress");
        };
        Self(user.message)
    }
}

impl<P> InterpretEstablished<P> for ExtractLocalEndpoint
where
    P: Protocol<Addr = MailAddr>,
{
    type Output = ActorRef<P>;

    fn interpret_established(&mut self, endpoint: ActorRef<P>) -> Self::Output {
        endpoint
    }
}

/// A non-owning, protocol-indexed user-lane capability.
///
/// The Communication anchor cannot keep or resurrect an actor. Indexing by
/// `P` preserves destination protocol identity independently of the concrete
/// behavior or transparent wrappers currently implementing it.
pub struct ActorRef<P: Protocol> {
    address: P::Addr,
    endpoint: EndpointMailbox<P::Addr, P::Msg>,
    termination: Observation<Termination<P::Addr>>,
    protocol: PhantomData<fn() -> P>,
}

/// One installed incarnation's message endpoint and exact behavior control.
///
/// The runtime issues this value only after committing the child's binding.
pub struct InstalledActor<B>
where
    B: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    recipient: ActorRef<B::Protocol>,
    control: ControlSender<B::Event>,
    retirement: Observation<ActorRetirementReport>,
    behavior: PhantomData<fn() -> B>,
}

pub(crate) fn request_actor_shutdown<P: Protocol, E, TargetPath>(
    recipient: &ActorRef<P>,
    control: &ControlSender<E>,
    ingress: Ingress<ShutdownRequested, TargetPath>,
) -> Result<(), ShutdownRejection>
where
    E: InjectEvent<ShutdownRequested, TargetPath>,
{
    if recipient.termination.try_get().is_some() {
        return Err(ShutdownRejection::AlreadyStopped);
    }
    match recipient.endpoint.close_admission() {
        AdmissionClosure::Closed => {}
        AdmissionClosure::AlreadyClosed => return Err(recipient.shutdown_rejection()),
    }
    control
        .send(ingress.event(ShutdownRequested))
        .map_err(|_| recipient.shutdown_rejection())
}

/// Exact user payload rejected because the incarnation is no longer live.
#[derive(thiserror::Error)]
#[error("actor reference is closed")]
pub struct SendError<M>(M);

pub(crate) struct ExtractLocalEndpoint;

#[cfg(test)]
mod delivery_rejection {
    use crate::address::MailAddr;
    use crate::local::ingress::{Admission, AdmissionClosure, EndpointMailbox, LocalIngress};
    use crate::observe;
    use behavior::{MessageProtocol, Never, User};
    use communication::{Config, Received, TrySendError, UserClosed, mailbox_channel};
    use std::sync::Arc;

    use super::{ActorRef, SendError};

    #[tokio::test]
    async fn exact_entity_endpoint_keeps_full_and_closed_originals_and_ordered_prefix() {
        let (control, owner, mailbox, mut receiver) =
            mailbox_channel::<Never, LocalIngress<MailAddr, Box<[u8]>>>(Config::new(2));
        let admission = Arc::new(Admission::new(owner));
        let (termination_publication, termination) = observe::pair();
        let endpoint = ActorRef::<MessageProtocol<MailAddr, Box<[u8]>>>::new(
            MailAddr(211),
            EndpointMailbox::Entity {
                mailbox,
                admission: Arc::downgrade(&admission),
            },
            termination,
        );
        let origin = MailAddr(223);
        let first = vec![11, 13].into_boxed_slice();
        let first_allocation = first.as_ptr();
        let second = vec![17, 19].into_boxed_slice();
        let second_allocation = second.as_ptr();
        let first_admitted = endpoint.try_send_from(origin, first);
        let second_admitted = endpoint.try_send_from(origin, second);
        assert!(first_admitted.is_ok());
        assert!(second_admitted.is_ok());
        let original = vec![23, 29].into_boxed_slice();
        let original_allocation = original.as_ptr();
        let full = endpoint.try_send_from(origin, original);
        let original = match full {
            Err(TrySendError::Full(original)) => original,
            Err(TrySendError::Closed(_)) => panic!("live Entity mailbox pressure is not closure"),
            Ok(()) => panic!("the full Entity mailbox cannot accept another message"),
        };
        assert_eq!(original.as_ref(), [23, 29]);
        assert_eq!(original.as_ptr(), original_allocation);
        let closed = admission.close();
        assert!(matches!(closed, AdmissionClosure::Closed));
        let refused = endpoint.try_send_from(origin, original);
        let original = match refused {
            Err(TrySendError::Closed(original)) => original,
            Err(TrySendError::Full(_)) => panic!("Entity closure wins over remaining pressure"),
            Ok(()) => panic!("closed Entity admission cannot accept a message"),
        };
        assert_eq!(original.as_ref(), [23, 29]);
        assert_eq!(original.as_ptr(), original_allocation);
        let replayed = endpoint.try_send_from(origin, original);
        let original = match replayed {
            Err(TrySendError::Closed(original)) => original,
            Err(TrySendError::Full(_)) => panic!("Entity closure remains final on replay"),
            Ok(()) => panic!("replay cannot reopen the exact Entity mailbox"),
        };
        assert_eq!(original.as_ref(), [23, 29]);
        assert_eq!(original.as_ptr(), original_allocation);
        drop(original);
        for (allocation, bytes) in [(first_allocation, [11, 13]), (second_allocation, [17, 19])] {
            let accepted_ingress = receiver.recv().await;
            let Some(Received::User(LocalIngress::Message(user))) = accepted_ingress else {
                panic!("each original Entity prefix message survives before closure");
            };
            assert_eq!(user.from, origin);
            assert_eq!(user.message.as_ref(), bytes);
            assert_eq!(user.message.as_ptr(), allocation);
        }
        let closed = receiver.recv().await;
        assert!(matches!(closed, Some(Received::UserLaneClosed)));
        drop(control);
        let exhausted = receiver.recv().await;
        assert!(exhausted.is_none());
        // This primitive projection test supplies no actor termination fact.
        drop((endpoint, admission, receiver, termination_publication));
    }

    #[test]
    fn rejected_user_delivery_recovers_the_exact_owned_payload() {
        let payload = String::from("application-owned-payload");
        let rejected = UserClosed(LocalIngress::Message(User::new(MailAddr(7), payload)));

        let error = SendError::from_entity_rejection(rejected);
        assert_eq!(format!("{error:?}"), "SendError { .. }");

        let rejected_message = error.into_message();
        assert_eq!(rejected_message, "application-owned-payload");
    }
}
