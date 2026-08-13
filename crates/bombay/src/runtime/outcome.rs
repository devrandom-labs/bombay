//! Generation-safe observation of Tokio actor-task completion.

use behavior::{Address, Crash, Exit};
use bombay_engine::{RunError, RunExit};

/// Every terminal state of an actor task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome<T> {
    /// The actor future returned its domain result.
    Returned(T),
    /// The actor future unwound with a panic.
    Panicked,
    /// The actor future was aborted before returning.
    Cancelled,
}

pub(crate) trait IntoPeerOutcome<A: Address> {
    fn peer_outcome(&self) -> Result<Exit<A>, Crash>;
}

impl<A: Address, B, E> IntoPeerOutcome<A> for Result<RunExit<Exit<A>>, RunError<B, E>> {
    fn peer_outcome(&self) -> Result<Exit<A>, Crash> {
        match self {
            Ok(RunExit::Stopped(exit)) => Ok(*exit),
            Ok(RunExit::EnvironmentClosed) => Ok(Exit::Collected),
            Err(RunError::Behavior(_)) => Err(Crash::Failed),
            Err(RunError::Environment(_)) => Err(Crash::EnvironmentFailed),
            Err(RunError::Poisoned) => Err(Crash::Panicked),
        }
    }
}

pub(crate) fn classify_task<A: Address, T: IntoPeerOutcome<A>>(
    outcome: &TaskOutcome<T>,
) -> Result<Exit<A>, Crash> {
    match outcome {
        TaskOutcome::Returned(outcome) => outcome.peer_outcome(),
        TaskOutcome::Panicked => Err(Crash::Panicked),
        TaskOutcome::Cancelled => Err(Crash::Cancelled),
    }
}
