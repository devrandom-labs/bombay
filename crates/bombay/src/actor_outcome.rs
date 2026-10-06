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
    use std::any::Any;
    use std::ptr;

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

    #[derive(Clone, Copy)]
    enum ExecutionFailureOrigin {
        HostExecution,
        Activation,
        Retirement,
        ReceivingContract,
    }

    fn failure_projection_preserves_owned_originals(origin: ExecutionFailureOrigin) {
        let behavior: Box<[u64]> = vec![7, 11].into_boxed_slice();
        let residual: Box<[u64]> = vec![17, 19].into_boxed_slice();
        let behavior_allocation = behavior.as_ptr();
        let residual_allocation = residual.as_ptr();
        let later_behavior: Box<[u64]> = vec![23, 29].into_boxed_slice();
        let later_activation: Box<[u64]> = vec![31, 37].into_boxed_slice();
        let later_native: Box<[u64]> = vec![41, 43].into_boxed_slice();
        let later_behavior_allocation = later_behavior.as_ptr();
        let later_activation_allocation = later_activation.as_ptr();
        let later_native_allocation = later_native.as_ptr();
        let later_payload: Box<dyn Any + Send> = Box::new(later_native);
        let later_carrier = ptr::from_ref(later_payload.as_ref()).cast::<()>();
        let additional_failures = vec![
            DriverError::Behavior(later_behavior),
            DriverError::Activation(later_activation),
            DriverError::RetirementPanicked(later_payload),
        ];
        let mut primary_original = None;
        let error = match origin {
            ExecutionFailureOrigin::ReceivingContract => DriverError::InterpreterContractFailed,
            ExecutionFailureOrigin::HostExecution
            | ExecutionFailureOrigin::Activation
            | ExecutionFailureOrigin::Retirement => {
                let native: Box<[u64]> = vec![71, 73].into_boxed_slice();
                let native_allocation = native.as_ptr();
                let payload: Box<dyn Any + Send> = Box::new(native);
                let native_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
                primary_original = Some((native_carrier, native_allocation));
                match origin {
                    ExecutionFailureOrigin::HostExecution => {
                        DriverError::HostExecutionPanicked(payload)
                    }
                    ExecutionFailureOrigin::Activation => DriverError::ActivationPanicked(payload),
                    ExecutionFailureOrigin::Retirement => DriverError::RetirementPanicked(payload),
                    ExecutionFailureOrigin::ReceivingContract => {
                        unreachable!("the contract failure owns no native cause")
                    }
                }
            }
        };
        let retirement = DriverRetirement {
            behavior,
            residual,
            additional_failures,
            disposition: Err(error),
        };
        let outcome: ActorExecutionOutcome<Box<[u64]>, Box<[u64]>, Box<[u64]>, Box<[u64]>> =
            retirement.into();
        let (behavior, residual, additional_failures, payload) = match (origin, outcome) {
            (
                ExecutionFailureOrigin::HostExecution,
                ActorExecutionOutcome::HostExecutionPanicked {
                    behavior,
                    residual,
                    additional_failures,
                    payload,
                },
            )
            | (
                ExecutionFailureOrigin::Activation,
                ActorExecutionOutcome::ActivationPanicked {
                    behavior,
                    residual,
                    additional_failures,
                    payload,
                },
            )
            | (
                ExecutionFailureOrigin::Retirement,
                ActorExecutionOutcome::RetirementPanicked {
                    behavior,
                    residual,
                    additional_failures,
                    payload,
                },
            ) => (behavior, residual, additional_failures, Some(payload)),
            (
                ExecutionFailureOrigin::ReceivingContract,
                ActorExecutionOutcome::InterpreterContractFailed {
                    behavior,
                    residual,
                    additional_failures,
                },
            ) => (behavior, residual, additional_failures, None),
            _ => panic!("the exact execution failure origin must survive retirement projection"),
        };
        assert_eq!(
            (behavior.as_ptr(), behavior.as_ref()),
            (behavior_allocation, &[7, 11][..])
        );
        assert_eq!(
            (residual.as_ptr(), residual.as_ref()),
            (residual_allocation, &[17, 19][..])
        );
        let [
            DriverError::Behavior(later_behavior),
            DriverError::Activation(later_activation),
            DriverError::RetirementPanicked(later_payload),
        ] = additional_failures.as_slice()
        else {
            panic!("the complete ordered heterogeneous additional failure lane must survive");
        };
        assert_eq!(
            (later_behavior.as_ptr(), later_behavior.as_ref()),
            (later_behavior_allocation, &[23, 29][..])
        );
        assert_eq!(
            (later_activation.as_ptr(), later_activation.as_ref()),
            (later_activation_allocation, &[31, 37][..])
        );
        let later_native = later_payload
            .downcast_ref::<Box<[u64]>>()
            .expect("the later native cause retains its concrete owned payload");
        assert_eq!(
            (
                ptr::from_ref(later_payload.as_ref()).cast::<()>(),
                later_native.as_ptr(),
                later_native.as_ref()
            ),
            (later_carrier, later_native_allocation, &[41, 43][..])
        );
        match (payload.as_ref(), primary_original) {
            (Some(payload), Some((native_carrier, native_allocation))) => {
                let native = payload
                    .downcast_ref::<Box<[u64]>>()
                    .expect("the primary native cause retains its concrete owned payload");
                assert_eq!(
                    (
                        ptr::from_ref(payload.as_ref()).cast::<()>(),
                        native.as_ptr(),
                        native.as_ref()
                    ),
                    (native_carrier, native_allocation, &[71, 73][..])
                );
            }
            (None, None) => {}
            _ => panic!("a contract failure cannot fabricate or erase an original native cause"),
        }
        drop(payload);
        drop(additional_failures);
        drop(residual);
        drop(behavior);
    }

    #[test]
    fn host_execution_projection_preserves_every_owned_original() {
        failure_projection_preserves_owned_originals(ExecutionFailureOrigin::HostExecution);
    }

    #[test]
    fn activation_panic_projection_preserves_every_owned_original() {
        failure_projection_preserves_owned_originals(ExecutionFailureOrigin::Activation);
    }

    #[test]
    fn retirement_panic_projection_preserves_every_owned_original() {
        failure_projection_preserves_owned_originals(ExecutionFailureOrigin::Retirement);
    }

    #[test]
    fn receiving_contract_projection_preserves_every_owned_original() {
        failure_projection_preserves_owned_originals(ExecutionFailureOrigin::ReceivingContract);
    }
}
