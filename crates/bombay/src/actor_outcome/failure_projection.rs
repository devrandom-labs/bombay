use std::any::Any;
use std::ptr;

use bombay_engine::{DriverError, DriverRetirement};

use super::ActorExecutionOutcome;

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
