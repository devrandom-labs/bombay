//! Internal mailbox driving contracts.

use core::future::Future;

/// Supplies already-formed events to an actor.
///
/// A mailbox, stream, simulator, or test queue can implement this contract.
/// Framing, authentication, sender stamping, and closure policy belong to the
/// source. Effect interpretation remains the responsibility of
/// [`bombay_engine::Environment`].
#[doc(hidden)]
pub trait EventSource {
    /// Event accepted by the actor's process protocol.
    type Event;

    /// Produce the next event, or `None` when this source is complete.
    fn next(&mut self) -> impl Future<Output = Option<Self::Event>> + Send;
}

/// Submits complete events to an actor mailbox.
///
/// A sender accepts exactly one event type, so the event is an associated
/// fact of the sender rather than an independent choice. This is the narrow
/// adapter used by [`crate::ActorRef`] after it stamps a typed user message
/// into an event. Bombay Communication remains the sole production
/// implementation and owns queueing, closure, and backpressure.
#[doc(hidden)]
pub trait EventSender {
    /// Event accepted by this sender.
    type Event;

    /// Submission failure.
    type Error;

    /// Submit one event.
    fn send(&self, event: Self::Event) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
