//! Exact terminal classification for one Driver execution.

use behavior::Never;
use std::any::Any;

use bombay_engine::{Completion, DriverError, DriverRetirement, SettlementFailure};

/// Every factual way one incarnation can terminate.
///
/// Behavior and environment failures retain their concrete owned payloads.
/// Caught pure transitions and borrowing host panics retain their distinct
/// native payloads and outside owners. Uncaught execution panic and cancellation
/// remain guard classifications; they do not claim that cleanup joined.
/// Caught initialization retains the original native payload through retirement.
#[derive(Debug)]
pub enum ActorExecutionOutcome<B, R, BehaviorError, ActivationError, Request = Never> {
    /// The Driver returned successfully for the stated reason.
    Completed {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        completion: Completion<Request>,
    },
    /// Behavior initialization or one Behavior fold failed.
    BehaviorFailed {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        error: BehaviorError,
    },
    /// The pure initialization fold panicked while the current behavior survived.
    InitializationPanicked {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        payload: Box<dyn Any + Send>,
    },
    /// A pure event fold panicked while its partially mutated behavior survived.
    TransitionPanicked {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        payload: Box<dyn Any + Send>,
    },
    /// A borrowing host operation panicked while the active owners survived.
    HostExecutionPanicked {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        payload: Box<dyn Any + Send>,
    },
    /// Borrowing activation panicked while its exact prepared or partially initialized owner survived.
    ActivationPanicked {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        payload: Box<dyn Any + Send>,
    },
    /// Retirement itself panicked; this variant does not imply an active incarnation.
    RetirementPanicked {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        payload: Box<dyn Any + Send>,
    },
    /// A host returned incomplete original-input/reply custody.
    InterpreterContractFailed {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
    },
    /// The prepared environment rejected activation.
    ActivationFailed {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        error: ActivationError,
    },
    /// Complete action-settlement custody could not lawfully continue.
    SettlementFailed {
        behavior: B,
        residual: R,
        additional_failures: Vec<DriverError<BehaviorError, ActivationError>>,
        error: SettlementFailure,
    },
    /// Driver execution unwound through a panic.
    Panicked,
    /// Driver execution was dropped before returning.
    Cancelled,
}

impl<B, R, BehaviorError, ActivationError, Request>
    From<DriverRetirement<B, R, DriverError<BehaviorError, ActivationError>, Request>>
    for ActorExecutionOutcome<B, R, BehaviorError, ActivationError, Request>
{
    fn from(
        retirement: DriverRetirement<B, R, DriverError<BehaviorError, ActivationError>, Request>,
    ) -> Self {
        let DriverRetirement {
            behavior,
            residual,
            disposition,
            additional_failures,
        } = retirement;
        match disposition {
            Ok(completion) => Self::Completed {
                behavior,
                residual,
                additional_failures,
                completion,
            },
            Err(DriverError::Behavior(error)) => Self::BehaviorFailed {
                behavior,
                residual,
                additional_failures,
                error,
            },
            Err(DriverError::InitializationPanicked(payload)) => Self::InitializationPanicked {
                behavior,
                residual,
                additional_failures,
                payload,
            },
            Err(DriverError::TransitionPanicked(payload)) => Self::TransitionPanicked {
                behavior,
                residual,
                additional_failures,
                payload,
            },
            Err(DriverError::HostExecutionPanicked(payload)) => Self::HostExecutionPanicked {
                behavior,
                residual,
                additional_failures,
                payload,
            },
            Err(DriverError::ActivationPanicked(payload)) => Self::ActivationPanicked {
                behavior,
                residual,
                additional_failures,
                payload,
            },
            Err(DriverError::RetirementPanicked(payload)) => Self::RetirementPanicked {
                behavior,
                residual,
                additional_failures,
                payload,
            },
            Err(DriverError::InterpreterContractFailed) => Self::InterpreterContractFailed {
                behavior,
                residual,
                additional_failures,
            },
            Err(DriverError::Activation(error)) => Self::ActivationFailed {
                behavior,
                residual,
                additional_failures,
                error,
            },
            Err(DriverError::Settlement(error)) => Self::SettlementFailed {
                behavior,
                residual,
                additional_failures,
                error,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use bombay_engine::{DriverError, DriverRetirement, SettlementFailure};

    use crate::actor_outcome::ActorExecutionOutcome;

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
                additional_failures: vec![DriverError::Behavior("later behavior failure")],
                disposition: Err::<_, DriverError<&'static str, &'static str>>(
                    DriverError::Settlement(failure),
                ),
            };
            let outcome: ActorExecutionOutcome<_, _, _, _> = retirement.into();
            let ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual,
                additional_failures,
                error,
            } = outcome
            else {
                panic!("the exact settlement failure must survive");
            };
            assert_eq!(behavior, 7);
            assert_eq!(residual, "retained settlement");
            assert_eq!(error, failure);
            let [DriverError::Behavior(later)] = additional_failures.as_slice() else {
                panic!("the independent later failure must survive projection");
            };
            assert_eq!(*later, "later behavior failure");
        }
    }
}

#[cfg(test)]
mod failure_projection;
