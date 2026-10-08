use std::any::Any;
use std::ptr;
use std::sync::Arc;
use std::vec::Drain;

use bombay_engine::{Completion, DriverError, DriverRetirement, SettlementFailure};

use super::ActorExecutionOutcome;

#[derive(Clone, Copy)]
enum ExecutionFailureOrigin {
    HostExecution,
    Activation,
    Retirement,
    ReceivingContract,
    Settlement,
}

#[expect(
    clippy::too_many_lines,
    reason = "one closed native-origin controller observes every owned lane and original opaque payload before explicit discharge"
)]
fn failure_projection_preserves_owned_originals(origin: ExecutionFailureOrigin) {
    let mut behavior_original = vec![7_u64, 11];
    let mut residual_original = vec![17_u64, 19];
    let behavior_allocation = behavior_original.as_ptr();
    let residual_allocation = residual_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let mut later_activation_original = vec![31_u64, 37];
    let later_native = Arc::new(vec![41_u64, 43]);
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let later_activation_allocation = later_activation_original.as_ptr();
    let later_native_lifetime = Arc::downgrade(&later_native);
    let later_payload: Box<dyn Any + Send> = Box::new(later_native);
    let later_carrier = ptr::from_ref(later_payload.as_ref()).cast::<()>();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
        DriverError::RetirementPanicked(later_payload),
    ];
    let mut primary_original = None;
    let error = match origin {
        ExecutionFailureOrigin::ReceivingContract => DriverError::InterpreterContractFailed,
        ExecutionFailureOrigin::Settlement => DriverError::Settlement(SettlementFailure::Rejected),
        ExecutionFailureOrigin::HostExecution
        | ExecutionFailureOrigin::Activation
        | ExecutionFailureOrigin::Retirement => {
            let native = Arc::new(vec![71_u64, 73]);
            let native_lifetime = Arc::downgrade(&native);
            let payload: Box<dyn Any + Send> = Box::new(native);
            let native_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
            primary_original = Some((native_carrier, native_lifetime));
            match origin {
                ExecutionFailureOrigin::HostExecution => {
                    DriverError::HostExecutionPanicked(payload)
                }
                ExecutionFailureOrigin::Activation => DriverError::ActivationPanicked(payload),
                ExecutionFailureOrigin::Retirement => DriverError::RetirementPanicked(payload),
                ExecutionFailureOrigin::ReceivingContract | ExecutionFailureOrigin::Settlement => {
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
    let outcome: ActorExecutionOutcome<_, _, _, _, Drain<'_, u64>> = retirement.into();
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
        )
        | (
            ExecutionFailureOrigin::Settlement,
            ActorExecutionOutcome::SettlementFailed {
                behavior,
                residual,
                additional_failures,
                error: SettlementFailure::Rejected,
            },
        ) => (behavior, residual, additional_failures, None),
        _ => panic!("the exact execution failure origin must survive retirement projection"),
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
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
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(
        ptr::from_ref(later_payload.as_ref()).cast::<()>(),
        later_carrier
    );
    let later_native_owners = later_native_lifetime.strong_count();
    assert_eq!(later_native_owners, 1);
    match (payload.as_ref(), primary_original.as_ref()) {
        (Some(payload), Some((native_carrier, native_lifetime))) => {
            assert_eq!(
                ptr::from_ref(payload.as_ref()).cast::<()>(),
                *native_carrier
            );
            let native_owners = native_lifetime.strong_count();
            assert_eq!(native_owners, 1);
        }
        (None, None) => {}
        _ => panic!("a contract failure cannot fabricate or erase an original native cause"),
    }
    drop(payload);
    drop(additional_failures);
    let later_native_owners = later_native_lifetime.strong_count();
    assert_eq!(later_native_owners, 0);
    if let Some((_, native_lifetime)) = primary_original.as_ref() {
        let native_owners = native_lifetime.strong_count();
        assert_eq!(native_owners, 0);
    }
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

#[test]
fn settlement_failure_projection_preserves_every_owned_original() {
    failure_projection_preserves_owned_originals(ExecutionFailureOrigin::Settlement);
}

#[test]
fn completed_projection_preserves_original_request_and_every_owned_lane() {
    let mut behavior_original = vec![7_u64, 11];
    let behavior_allocation = behavior_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let mut residual_original = vec![17_u64, 19];
    let residual_allocation = residual_original.as_ptr();
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let mut later_activation_original = vec![31_u64, 37];
    let later_activation_allocation = later_activation_original.as_ptr();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
    ];
    let mut request_original = vec![59_u64, 61];
    let request_allocation = request_original.as_ptr();
    let retirement = DriverRetirement {
        behavior,
        residual,
        additional_failures,
        disposition: Ok(Completion::RetirementRequested(request_original.drain(..))),
    };
    let outcome: ActorExecutionOutcome<_, _, _, _, _> = retirement.into();
    let ActorExecutionOutcome::Completed {
        behavior,
        residual,
        additional_failures,
        completion: Completion::RetirementRequested(request),
    } = outcome
    else {
        panic!("the original requested completion must survive retirement projection");
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
        (residual_allocation, &[17, 19][..])
    );
    let [
        DriverError::Behavior(later_behavior),
        DriverError::Activation(later_activation),
    ] = additional_failures.as_slice()
    else {
        panic!("the complete ordered additional failure lane must survive projection");
    };
    assert_eq!(
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(
        (request.as_slice().as_ptr(), request.as_slice()),
        (request_allocation, &[59, 61][..])
    );
    drop(request);
    drop(additional_failures);
    drop(residual);
    drop(behavior);
}

#[test]
fn behavior_failure_projection_preserves_every_owned_original() {
    let mut behavior_original = vec![7_u64, 11];
    let behavior_allocation = behavior_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let mut residual_original = vec![17_u64, 19];
    let residual_allocation = residual_original.as_ptr();
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let mut later_activation_original = vec![31_u64, 37];
    let later_activation_allocation = later_activation_original.as_ptr();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
    ];
    let mut failure_original = vec![71_u64, 73];
    let failure_allocation = failure_original.as_ptr();
    let retirement = DriverRetirement {
        behavior,
        residual,
        additional_failures,
        disposition: Err(DriverError::Behavior(failure_original.drain(..))),
    };
    let outcome: ActorExecutionOutcome<_, _, _, _, Drain<'_, u64>> = retirement.into();
    let ActorExecutionOutcome::BehaviorFailed {
        behavior,
        residual,
        additional_failures,
        error,
    } = outcome
    else {
        panic!("the exact behavior failure must survive retirement projection");
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
        (residual_allocation, &[17, 19][..])
    );
    let [
        DriverError::Behavior(later_behavior),
        DriverError::Activation(later_activation),
    ] = additional_failures.as_slice()
    else {
        panic!("the complete ordered additional failure lane must survive projection");
    };
    assert_eq!(
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(
        (error.as_slice().as_ptr(), error.as_slice()),
        (failure_allocation, &[71, 73][..])
    );
    drop(error);
    drop(additional_failures);
    drop(residual);
    drop(behavior);
}

#[test]
fn activation_failure_projection_preserves_every_owned_original() {
    let mut behavior_original = vec![7_u64, 11];
    let behavior_allocation = behavior_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let mut residual_original = vec![17_u64, 19];
    let residual_allocation = residual_original.as_ptr();
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let mut later_activation_original = vec![31_u64, 37];
    let later_activation_allocation = later_activation_original.as_ptr();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
    ];
    let mut failure_original = vec![71_u64, 73];
    let failure_allocation = failure_original.as_ptr();
    let retirement = DriverRetirement {
        behavior,
        residual,
        additional_failures,
        disposition: Err(DriverError::Activation(failure_original.drain(..))),
    };
    let outcome: ActorExecutionOutcome<_, _, _, _, Drain<'_, u64>> = retirement.into();
    let ActorExecutionOutcome::ActivationFailed {
        behavior,
        residual,
        additional_failures,
        error,
    } = outcome
    else {
        panic!("the exact activation failure must survive retirement projection");
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
        (residual_allocation, &[17, 19][..])
    );
    let [
        DriverError::Behavior(later_behavior),
        DriverError::Activation(later_activation),
    ] = additional_failures.as_slice()
    else {
        panic!("the complete ordered additional failure lane must survive projection");
    };
    assert_eq!(
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(
        (error.as_slice().as_ptr(), error.as_slice()),
        (failure_allocation, &[71, 73][..])
    );
    drop(error);
    drop(additional_failures);
    drop(residual);
    drop(behavior);
}

#[test]
fn initialization_panic_projection_preserves_every_owned_original() {
    let mut behavior_original = vec![7_u64, 11];
    let behavior_allocation = behavior_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let mut residual_original = vec![17_u64, 19];
    let residual_allocation = residual_original.as_ptr();
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let mut later_activation_original = vec![31_u64, 37];
    let later_activation_allocation = later_activation_original.as_ptr();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
    ];
    let native = Arc::new(vec![71_u64, 73]);
    let native_lifetime = Arc::downgrade(&native);
    let payload: Box<dyn Any + Send> = Box::new(native);
    let native_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
    let retirement = DriverRetirement {
        behavior,
        residual,
        additional_failures,
        disposition: Err(DriverError::InitializationPanicked(payload)),
    };
    let outcome: ActorExecutionOutcome<_, _, _, _, Drain<'_, u64>> = retirement.into();
    let ActorExecutionOutcome::InitializationPanicked {
        behavior,
        residual,
        additional_failures,
        payload,
    } = outcome
    else {
        panic!("the exact initialization panic origin must survive retirement projection");
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
        (residual_allocation, &[17, 19][..])
    );
    let [
        DriverError::Behavior(later_behavior),
        DriverError::Activation(later_activation),
    ] = additional_failures.as_slice()
    else {
        panic!("the complete ordered additional failure lane must survive projection");
    };
    assert_eq!(
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), native_carrier);
    let native_owners = native_lifetime.strong_count();
    assert_eq!(native_owners, 1);
    drop(payload);
    let native_owners = native_lifetime.strong_count();
    assert_eq!(native_owners, 0);
    drop(additional_failures);
    drop(residual);
    drop(behavior);
}

#[test]
fn transition_panic_projection_preserves_every_owned_original() {
    let mut behavior_original = vec![7_u64, 11];
    let behavior_allocation = behavior_original.as_ptr();
    let behavior = behavior_original.drain(..);
    let mut residual_original = vec![17_u64, 19];
    let residual_allocation = residual_original.as_ptr();
    let residual = residual_original.drain(..);
    let mut later_behavior_original = vec![23_u64, 29];
    let later_behavior_allocation = later_behavior_original.as_ptr();
    let mut later_activation_original = vec![31_u64, 37];
    let later_activation_allocation = later_activation_original.as_ptr();
    let additional_failures = vec![
        DriverError::Behavior(later_behavior_original.drain(..)),
        DriverError::Activation(later_activation_original.drain(..)),
    ];
    let native = Arc::new(vec![71_u64, 73]);
    let native_lifetime = Arc::downgrade(&native);
    let payload: Box<dyn Any + Send> = Box::new(native);
    let native_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
    let retirement = DriverRetirement {
        behavior,
        residual,
        additional_failures,
        disposition: Err(DriverError::TransitionPanicked(payload)),
    };
    let outcome: ActorExecutionOutcome<_, _, _, _, Drain<'_, u64>> = retirement.into();
    let ActorExecutionOutcome::TransitionPanicked {
        behavior,
        residual,
        additional_failures,
        payload,
    } = outcome
    else {
        panic!("the exact transition panic origin must survive retirement projection");
    };
    assert_eq!(
        (behavior.as_slice().as_ptr(), behavior.as_slice()),
        (behavior_allocation, &[7, 11][..])
    );
    assert_eq!(
        (residual.as_slice().as_ptr(), residual.as_slice()),
        (residual_allocation, &[17, 19][..])
    );
    let [
        DriverError::Behavior(later_behavior),
        DriverError::Activation(later_activation),
    ] = additional_failures.as_slice()
    else {
        panic!("the complete ordered additional failure lane must survive projection");
    };
    assert_eq!(
        (
            later_behavior.as_slice().as_ptr(),
            later_behavior.as_slice()
        ),
        (later_behavior_allocation, &[23, 29][..])
    );
    assert_eq!(
        (
            later_activation.as_slice().as_ptr(),
            later_activation.as_slice()
        ),
        (later_activation_allocation, &[31, 37][..])
    );
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), native_carrier);
    let native_owners = native_lifetime.strong_count();
    assert_eq!(native_owners, 1);
    drop(payload);
    let native_owners = native_lifetime.strong_count();
    assert_eq!(native_owners, 0);
    drop(additional_failures);
    drop(residual);
    drop(behavior);
}
