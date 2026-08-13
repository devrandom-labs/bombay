//! Runtime port: events in, transition effects out, then explicit retirement.

use core::future::Future;

/// Gives a process protocol its live or simulated meaning.
///
/// Unlike narrower ingress ports, an environment also owns effect
/// interpretation and the ordering between input and effects.
pub trait Environment {
    /// Events supplied to the behavior.
    type Event;
    /// Effects emitted by the behavior in one transition.
    type Effect;
    /// Failure while interpreting an effect.
    type Error;

    /// Produce the next event, or `None` when the source is closed.
    fn next(&mut self) -> impl Future<Output = Option<Self::Event>> + Send;

    /// Interpret one successful transition's complete effect.
    fn interpret(
        &mut self,
        effect: Self::Effect,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Retire resources that must be gone before terminal publication.
    fn retire(&mut self) -> impl Future<Output = ()> + Send;
}
