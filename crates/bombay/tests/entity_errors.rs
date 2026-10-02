//! Error types expose stable diagnostics and std error integration.

use bombay::entity::{AdmissionFailure, DirectoryError, FenceFailure, Refusal};

#[test]
fn directory_error_reports_each_failure_domain() {
    assert_eq!(
        DirectoryError::<()>::InvalidShardCount.to_string(),
        "shard count was not a power of two"
    );
    assert_eq!(
        DirectoryError::ActivationIdsExhausted(()).to_string(),
        "activation identity namespace is exhausted"
    );
    assert_eq!(
        DirectoryError::DispatchIdsExhausted(()).to_string(),
        "dispatch identity namespace is exhausted"
    );
}

#[test]
fn admission_failure_reports_each_failure_domain() {
    assert_eq!(
        AdmissionFailure::Refused {
            command: (),
            reason: Refusal::Draining,
        }
        .to_string(),
        "lifecycle admission or delivery refused the command"
    );
    assert_eq!(
        AdmissionFailure::ActivationIdsExhausted(()).to_string(),
        "activation identity namespace is exhausted"
    );
    assert_eq!(
        AdmissionFailure::DispatchIdsExhausted(()).to_string(),
        "dispatch identity namespace is exhausted"
    );
}

#[test]
fn fence_failure_reports_the_failed_stage() {
    assert_eq!(FenceFailure::Enqueue.to_string(), "fence was not enqueued");
    assert_eq!(
        FenceFailure::Acknowledgement.to_string(),
        "fence was enqueued but not acknowledged"
    );
}
