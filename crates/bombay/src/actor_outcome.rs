//! Exact terminal classification for one Driver execution.

use bombay_engine::{Completion, DriverError, DriverRetirement, SettlementFailure};

/// Every factual way one incarnation can terminate.
///
/// Behavior and environment failures retain their concrete owned payloads.
/// Panic and cancellation are classified by the incarnation because they are
/// properties of executing the Driver future, not Behavior decisions.
#[derive(Debug, PartialEq, Eq)]
pub enum ActorExecutionOutcome<B, R, BehaviorError, ActivationError> {
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
    /// The pure initialization fold panicked while the current behavior survived.
    InitializationPanicked { behavior: B, residual: R },
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
    for ActorExecutionOutcome<B, R, BehaviorError, ActivationError>
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
            Err(DriverError::InitializationPanicked) => {
                Self::InitializationPanicked { behavior, residual }
            }
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

#[cfg(test)]
mod tests {
    use bombay_engine::{DriverError, DriverRetirement, SettlementFailure};

    use super::ActorExecutionOutcome;

    #[test]
    fn settlement_retirement_preserves_each_exact_failure_and_owned_payload() {
        for failure in [
            SettlementFailure::Rejected,
            SettlementFailure::Corrupt,
            SettlementFailure::SourceClosed,
        ] {
            let retirement = DriverRetirement {
                behavior: 7_u8,
                residual: String::from("retained settlement"),
                disposition: Err::<_, DriverError<&'static str, &'static str>>(
                    DriverError::Settlement(failure),
                ),
            };
            let outcome: ActorExecutionOutcome<_, _, _, _> = retirement.into();
            assert_eq!(
                outcome,
                ActorExecutionOutcome::SettlementFailed {
                    behavior: 7,
                    residual: String::from("retained settlement"),
                    error: failure,
                }
            );
        }
    }
}
