//! Exact terminal classification for one Driver execution.

use bombay_engine::{Completion, DriverError};

/// Every factual way one incarnation can terminate.
///
/// Behavior and environment failures retain their concrete owned payloads.
/// Panic and cancellation are classified by the incarnation because they are
/// properties of executing the Driver future, not Behavior decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum IncarnationOutcome<B, A, E = A> {
    /// The Driver returned successfully for the stated reason.
    Completed(Completion),
    /// Behavior initialization or one Behavior fold failed.
    BehaviorFailed(B),
    /// The prepared environment rejected activation.
    ActivationFailed(A),
    /// The environment rejected one complete action commitment.
    EnvironmentFailed(E),
    /// Driver execution unwound through a panic.
    Panicked,
    /// Driver execution was dropped before returning.
    Cancelled,
}

impl<B, A, E> From<Result<Completion, DriverError<B, A, E>>> for IncarnationOutcome<B, A, E> {
    fn from(result: Result<Completion, DriverError<B, A, E>>) -> Self {
        match result {
            Ok(completion) => Self::Completed(completion),
            Err(DriverError::Behavior(error)) => Self::BehaviorFailed(error),
            Err(DriverError::Activation(error)) => Self::ActivationFailed(error),
            Err(DriverError::Environment(error)) => Self::EnvironmentFailed(error),
        }
    }
}
