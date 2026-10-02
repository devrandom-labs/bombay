use bombay::behavior::AllocationRejection;
use bombay::prelude::ExternalActorError;

fn original_allocation(error: ExternalActorError) -> AllocationRejection {
    match error {
        ExternalActorError::Allocation(reason) => reason,
    }
}

fn main() {}
