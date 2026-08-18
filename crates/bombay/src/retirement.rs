//! Terminal handoff after one Driver execution has been destroyed.

use super::IncarnationOutcome;

/// Consumes the terminal capability for one incarnation exactly once.
///
/// Implementations may release an identity lease and publish the supplied
/// outcome. They cannot affect Driver execution because invocation occurs only
/// after the Driver future and all values it owns have been dropped.
pub trait Retirement<B, A, E = A> {
    /// Retire the incarnation with its exact terminal classification.
    fn retire(self, outcome: IncarnationOutcome<B, A, E>);
}

impl<B, A, E, F> Retirement<B, A, E> for F
where
    F: FnOnce(IncarnationOutcome<B, A, E>),
{
    fn retire(self, outcome: IncarnationOutcome<B, A, E>) {
        self(outcome);
    }
}
