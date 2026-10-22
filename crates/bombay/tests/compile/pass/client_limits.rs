use bombay::remote::ClientLimits;
use std::num::NonZeroUsize;

fn main() {
    let _limits = ClientLimits {
        concurrent_requests: NonZeroUsize::new(1).unwrap(),
        retained_requests: NonZeroUsize::new(2).unwrap(),
        retained_responses: NonZeroUsize::new(3).unwrap(),
    };
}
