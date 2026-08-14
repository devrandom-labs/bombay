//! Typed handles to resolved actor mailboxes.

use behavior::{Address, EventInput, RouteInput, ShutdownRequested, UserEvent};

use crate::runtime::lifecycle::IncarnationReporter;
use crate::{
    DeliveryEndpoint, EventSender, LifecycleTransition, MailboxAnchor, MailboxSender, NoLifecycle,
    RejectedDelivery,
};

/// A resolved actor endpoint rejected a message because its mailbox retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("actor mailbox closed")]
pub struct MailboxDeliveryClosed;

/// Failure to publish a typed graceful-shutdown request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ShutdownRequestError {
    /// The actor mailbox consumer has already retired.
    #[error("the actor mailbox consumer has already retired")]
    Closed,
}

/// A typed, directly resolved handle to an actor's user protocol.
///
/// References are issued by [`crate::System::spawn`] and may be cloned, but
/// their concrete mailbox sender cannot be supplied or extracted externally.
/// The message and event types are associated facts of the sender's event
/// protocol, not independent choices.
///
/// ```compile_fail
/// use bombay::{ActorRef, behavior::MailAddr};
///
/// let _: ActorRef<MailAddr, ()> = ActorRef::new(MailAddr(1), ());
/// ```
pub struct ActorRef<A, S, L = NoLifecycle> {
    address: A,
    sender: S,
    lifecycle: L,
}

impl<A, S> ActorRef<A, S, NoLifecycle> {
    /// Construct a typed reference from an address and event sender.
    pub(crate) const fn new(address: A, sender: S) -> Self {
        Self::with_lifecycle(address, sender, NoLifecycle)
    }
}

impl<A, S, L> ActorRef<A, S, L> {
    pub(crate) const fn with_lifecycle(address: A, sender: S, lifecycle: L) -> Self {
        Self {
            address,
            sender,
            lifecycle,
        }
    }

    /// The destination address.
    pub const fn address(&self) -> &A {
        &self.address
    }
}

impl<A, E, L> ActorRef<A, MailboxSender<E>, L> {
    pub(crate) fn sender_anchor(&self) -> MailboxAnchor<E> {
        self.sender.anchor()
    }
}

impl<A, S, L> ActorRef<A, S, L>
where
    A: Address + Send + Sync,
    S: EventSender + Sync,
    S::Event: UserEvent<Addr = A>,
    <S::Event as UserEvent>::Message: Send,
    L: Sync,
{
    /// Deliver a message directly, stamped with `from`.
    ///
    /// Delivery awaits bounded mailbox admission and retains ownership of the
    /// message until admission succeeds or the exact event is returned on
    /// closure. Sequential sends by one producer retain their order. Cloned
    /// references used by concurrent producers have no pre-admission ordering guarantee;
    /// their accepted messages may interleave in either order.
    ///
    /// # Errors
    ///
    /// Returns the event sender's delivery failure.
    pub async fn send(
        &self,
        from: A,
        message: <S::Event as UserEvent>::Message,
    ) -> Result<(), S::Error> {
        self.sender
            .send(<S::Event as UserEvent>::user(from, message))
            .await
    }
}

impl<A, E, L> ActorRef<A, MailboxSender<E>, L>
where
    E: EventInput<ShutdownRequested>,
    L: IncarnationReporter,
{
    /// Publish one priority graceful-shutdown request.
    ///
    /// The request does not wait behind bounded user-mailbox backpressure.
    /// A successful publication does not mean the actor has retired; await
    /// its [`crate::Handle`] outcome for terminal completion.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownRequestError::Closed`] after mailbox retirement.
    pub fn request_shutdown(&self) -> Result<(), ShutdownRequestError> {
        let event = E::inject(ShutdownRequested);
        self.sender
            .send_control(event)
            .map_err(|_| ShutdownRequestError::Closed)?;
        self.lifecycle.emit(LifecycleTransition::ShutdownRequested);
        Ok(())
    }
}

impl<A, E, L> ActorRef<A, MailboxSender<E>, L>
where
    E: RouteInput<ShutdownRequested>,
{
    pub(crate) fn request_shutdown_if_supported(&self) {
        if let Ok(event) = E::route(ShutdownRequested) {
            let _ = self.sender.send_control(event);
        }
    }
}

impl<A, E, L> DeliveryEndpoint<A, <E as UserEvent>::Message> for ActorRef<A, MailboxAnchor<E>, L>
where
    A: Address + Send + Sync,
    E: UserEvent<Addr = A> + Send,
    <E as UserEvent>::Message: Send,
    L: Sync,
{
    type Error = MailboxDeliveryClosed;

    async fn deliver(
        &self,
        from: A,
        message: <E as UserEvent>::Message,
    ) -> Result<(), RejectedDelivery<<E as UserEvent>::Message, Self::Error>> {
        let event = E::user(from, message);
        match self.sender.send(event).await {
            Ok(()) => Ok(()),
            Err(closed) => {
                let Ok(user) = closed.0.into_user() else {
                    unreachable!("UserEvent::user must round-trip through UserEvent::into_user")
                };
                Err(RejectedDelivery::new(user.message, MailboxDeliveryClosed))
            }
        }
    }
}

impl<A: Clone, S: Clone, L: Clone> Clone for ActorRef<A, S, L> {
    fn clone(&self) -> Self {
        Self::with_lifecycle(
            self.address.clone(),
            self.sender.clone(),
            self.lifecycle.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::{Arc, Mutex};

    use behavior::{MailAddr, ShutdownProtocol, ShutdownRequested, User};

    use super::{ActorRef, MailboxDeliveryClosed};
    use crate::{
        DeliveryEndpoint, EventSender, EventSource, MailboxConfig, RejectedDelivery,
        ShutdownRequestError,
    };

    struct Message(String);

    #[derive(Clone)]
    struct Sender<E>(Arc<Mutex<Vec<E>>>);

    impl<E: Send> EventSender for Sender<E> {
        type Event = E;
        type Error = Infallible;

        async fn send(&self, event: E) -> Result<(), Self::Error> {
            self.0.lock().expect("event lock").push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn moves_a_non_clone_message_directly_into_the_actor_event() {
        let events: Arc<Mutex<Vec<User<MailAddr, Message>>>> = Arc::new(Mutex::new(Vec::new()));
        let actor_ref = ActorRef::new(MailAddr(9), Sender(events.clone()));

        actor_ref
            .send(MailAddr(7), Message(String::from("hello")))
            .await
            .unwrap();

        assert_eq!(*actor_ref.address(), MailAddr(9));
        let events = events.lock().expect("event lock");
        assert_eq!(events[0].from, MailAddr(7));
        assert_eq!(events[0].message.0, "hello");
    }

    #[tokio::test]
    async fn resolved_closed_mailbox_returns_the_exact_non_clone_message() {
        let (sender, source) = MailboxConfig::bounded(1).create::<User<MailAddr, Message>>();
        let endpoint = ActorRef::new(MailAddr(9), sender.anchor());
        drop(source);

        let RejectedDelivery { message, error } =
            DeliveryEndpoint::deliver(&endpoint, MailAddr(7), Message(String::from("owned")))
                .await
                .expect_err("retired mailbox must reject delivery");

        assert_eq!(message.0, "owned");
        assert_eq!(error, MailboxDeliveryClosed);
    }

    #[tokio::test]
    async fn publishes_shutdown_through_the_priority_lane() {
        let (sender, mut source) =
            MailboxConfig::bounded(1).create::<ShutdownProtocol<User<MailAddr, Message>>>();
        let actor_ref = ActorRef::new(MailAddr(9), sender);

        actor_ref.request_shutdown().unwrap();

        assert!(matches!(
            source.next().await,
            Some(ShutdownProtocol::ShutdownRequested(ShutdownRequested))
        ));
    }

    #[test]
    fn reports_closed_shutdown_delivery() {
        let (closed, source) =
            MailboxConfig::bounded(1).create::<ShutdownProtocol<User<MailAddr, Message>>>();
        drop(source);
        let closed = ActorRef::new(MailAddr(2), closed);
        assert_eq!(closed.request_shutdown(), Err(ShutdownRequestError::Closed));
    }
}
