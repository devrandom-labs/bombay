//! Terminal handoff after one Driver execution has been destroyed.

use super::IncarnationOutcome;

/// Consumes the terminal capability for one incarnation exactly once.
///
/// Implementations may release an identity lease and publish the supplied
/// outcome. They cannot affect Driver execution because invocation occurs only
/// after the Driver future and all values it owns have been dropped.
pub trait Retirement<B, R, BehaviorError, ActivationError, EnvironmentError = ActivationError> {
    type Output;

    /// Retire the incarnation with its exact terminal classification.
    fn retire(
        self,
        outcome: IncarnationOutcome<B, R, BehaviorError, ActivationError, EnvironmentError>,
    ) -> Self::Output;
}

impl<B, R, BehaviorError, ActivationError, EnvironmentError, F>
    Retirement<B, R, BehaviorError, ActivationError, EnvironmentError> for F
where
    F: FnOnce(IncarnationOutcome<B, R, BehaviorError, ActivationError, EnvironmentError>),
{
    type Output = ();

    fn retire(
        self,
        outcome: IncarnationOutcome<B, R, BehaviorError, ActivationError, EnvironmentError>,
    ) {
        self(outcome);
    }
}
