//! Exact terminal classification for one Driver execution.

use bombay_engine::{Completion, DriverError, DriverRetirement, SettlementFailure};

/// Every factual way one incarnation can terminate.
///
/// Behavior and environment failures retain their concrete owned payloads.
/// Panic and cancellation are classified by the incarnation because they are
/// properties of executing the Driver future, not Behavior decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum IncarnationOutcome<B, R, BehaviorError, ActivationError> {
    /// The Driver returned successfully for the stated reason.
    Completed {
        behavior: B,
        residual: R,
        completion: Completion,
    },
    /// Behavior initialization or one Behavior fold failed.
    BehaviorFailed {
        behavior: B,
        residual: R,
        error: BehaviorError,
    },
    /// The prepared environment rejected activation.
    ActivationFailed {
        behavior: B,
        residual: R,
        error: ActivationError,
    },
    /// Complete action-settlement custody could not lawfully continue.
    SettlementFailed {
        behavior: B,
        residual: R,
        error: SettlementFailure,
    },
    /// Driver execution unwound through a panic.
    Panicked,
    /// Driver execution was dropped before returning.
    Cancelled,
}

impl<B, R, BehaviorError, ActivationError>
    From<DriverRetirement<B, R, DriverError<BehaviorError, ActivationError>>>
    for IncarnationOutcome<B, R, BehaviorError, ActivationError>
{
    fn from(
        retirement: DriverRetirement<B, R, DriverError<BehaviorError, ActivationError>>,
    ) -> Self {
        let DriverRetirement {
            behavior,
            residual,
            disposition,
        } = retirement;
        match disposition {
            Ok(completion) => Self::Completed {
                behavior,
                residual,
                completion,
            },
            Err(DriverError::Behavior(error)) => Self::BehaviorFailed {
                behavior,
                residual,
                error,
            },
            Err(DriverError::Activation(error)) => Self::ActivationFailed {
                behavior,
                residual,
                error,
            },
            Err(DriverError::Settlement(error)) => Self::SettlementFailed {
                behavior,
                residual,
                error,
            },
        }
    }
}
