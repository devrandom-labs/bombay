//! Terminal handoff after one Driver execution finishes or drops.

use super::ActorExecutionOutcome;
use behavior::Never;

/// Consumes the terminal capability for one incarnation exactly once.
///
/// Implementations may release an identity lease and publish the supplied
/// outcome. Invocation follows completion or disposal of the Driver future.
/// Ordinary completion transfers its surviving values into the outcome.
pub trait Retirement<B, R, BehaviorError, ActivationError, Request = Never> {
    type Output;

    /// Retire the incarnation with its exact terminal classification.
    fn retire(
        self,
        outcome: ActorExecutionOutcome<B, R, BehaviorError, ActivationError, Request>,
    ) -> Self::Output;
}

impl<B, R, BehaviorError, ActivationError, Request, F>
    Retirement<B, R, BehaviorError, ActivationError, Request> for F
where
    F: FnOnce(ActorExecutionOutcome<B, R, BehaviorError, ActivationError, Request>),
{
    type Output = ();

    fn retire(self, outcome: ActorExecutionOutcome<B, R, BehaviorError, ActivationError, Request>) {
        self(outcome);
    }
}
