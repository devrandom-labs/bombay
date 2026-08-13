//! Bombay Communication-backed two-lane actor mailboxes.

use communication::{
    Config, Consumer, ControlClosed, ControlSender, Received, UserAnchor, UserClosed, UserSender,
    channel,
};

use crate::{EventSender, EventSource};

/// Configuration for bounded actor user mailboxes.
///
/// Bombay deliberately selects Bombay Communication's zero-aging mode:
/// every waiting control event precedes every waiting user event. Sustained
/// control traffic may therefore starve user traffic. A second priority
/// protocol or fairness consumer is required before bombay can justify an
/// aging policy.
///
/// User events are FIFO in Communication's admission-ticket order. Clones of
/// one sender have no additional cross-producer ordering contract: concurrent
/// producers may acquire admission tickets in either order, while each
/// producer's sequential sends retain their order.
///
/// The user lane is bounded. Async sends apply backpressure while retaining
/// their event and return that exact event if retirement closes the lane;
/// bombay never silently drops an accepted event.
#[derive(Debug, Clone, Copy)]
pub struct MailboxConfig {
    config: Config,
}

impl MailboxConfig {
    /// Construct a bounded user input.
    #[must_use]
    pub const fn bounded(user_capacity: usize) -> Self {
        Self {
            config: Config::new(user_capacity),
        }
    }

    /// Create one concrete sender/source pair with this configuration.
    #[must_use]
    pub(crate) fn create<E: Send>(&self) -> (MailboxSender<E>, MailboxReceiver<E>) {
        let (control, user, consumer) = channel::<E, E>(self.config);
        (MailboxSender { control, user }, MailboxReceiver(consumer))
    }
}

/// Counting edge handle for bounded user delivery.
#[doc(hidden)]
pub struct MailboxSender<E> {
    control: ControlSender<E>,
    user: UserSender<E>,
}

impl<E> Clone for MailboxSender<E> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            user: self.user.clone(),
        }
    }
}

impl<E> MailboxSender<E> {
    /// Derive a non-owning user endpoint suitable for address registration.
    #[must_use]
    pub(crate) fn anchor(&self) -> MailboxAnchor<E> {
        MailboxAnchor(self.user.anchor())
    }

    /// Submit one already-formed priority event without user backpressure.
    pub(crate) fn send_control(&self, event: E) -> Result<(), ControlClosed<E>> {
        self.control.send(event)
    }
}

impl<E: Send> EventSender for MailboxSender<E> {
    type Event = E;
    type Error = UserClosed<E>;

    async fn send(&self, event: E) -> Result<(), Self::Error> {
        self.user.send(event).await
    }
}

/// Non-owning address-table endpoint; it cannot keep the user lane alive.
#[doc(hidden)]
pub struct MailboxAnchor<E>(UserAnchor<E>);

impl<E> Clone for MailboxAnchor<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E: Send> EventSender for MailboxAnchor<E> {
    type Event = E;
    type Error = UserClosed<E>;

    async fn send(&self, event: E) -> Result<(), Self::Error> {
        self.0.send(event).await
    }
}

/// Actor-owned bounded user event source.
#[doc(hidden)]
pub struct MailboxReceiver<E>(Consumer<E, E>);

impl<E: Send> EventSource for MailboxReceiver<E> {
    type Event = E;

    async fn next(&mut self) -> Option<Self::Event> {
        match self.0.recv().await {
            Some(Received::Control(event) | Received::User(event)) => Some(event),
            Some(Received::UserLaneClosed) | None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{EventSender, EventSource};
    use tokio::sync::Barrier;
    use tokio::task::yield_now;

    use std::sync::Arc;

    use super::MailboxConfig;

    #[tokio::test]
    async fn registry_anchor_does_not_keep_mailbox_alive() {
        let (sender, mut source) = MailboxConfig::bounded(2).create::<u64>();
        let anchor = sender.anchor();
        drop(sender);
        assert_eq!(source.next().await, None);
        assert!(anchor.send(7).await.is_err());
    }

    #[tokio::test]
    async fn queued_user_events_drain_before_lane_closure() {
        let (sender, mut source) = MailboxConfig::bounded(2).create::<u64>();
        sender.send(1).await.unwrap();
        drop(sender);
        assert_eq!(source.next().await, Some(1));
        assert_eq!(source.next().await, None);
    }

    #[tokio::test]
    async fn blocked_producer_recovers_payload_when_receiver_retires() {
        let (sender, receiver) = MailboxConfig::bounded(1).create::<u64>();
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let blocked = tokio::spawn({
            let sender = sender.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                sender.send(3).await
            }
        });
        barrier.wait().await;
        yield_now().await;
        assert!(!blocked.is_finished());

        drop(receiver);
        let error = blocked
            .await
            .unwrap()
            .expect_err("receiver retirement rejects send");
        assert_eq!(error.0, 3);
    }

    #[tokio::test]
    async fn control_event_precedes_queued_user_events() {
        let (sender, mut source) = MailboxConfig::bounded(2).create::<u64>();
        sender.send(1).await.unwrap();
        sender.send(2).await.unwrap();
        sender.send_control(9).unwrap();

        assert_eq!(source.next().await, Some(9));
        assert_eq!(source.next().await, Some(1));
        assert_eq!(source.next().await, Some(2));
    }

    #[tokio::test]
    async fn zero_aging_drains_complete_control_backlog_before_waiting_user() {
        let (sender, mut source) = MailboxConfig::bounded(1).create::<u64>();
        sender.send(1).await.unwrap();
        for control in 10..18 {
            sender.send_control(control).unwrap();
        }

        for control in 10..18 {
            assert_eq!(source.next().await, Some(control));
        }
        assert_eq!(source.next().await, Some(1));
    }
}
