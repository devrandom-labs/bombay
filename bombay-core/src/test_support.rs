//! Test-only helpers shared by the unit and integration suites (card #150).
//!
//! Behind the `test-support` feature: `tests/*.rs` link the lib externally and
//! cannot reach `pub(crate)`, and `#[doc(hidden)]` is not access control.

use core::time::Duration;

/// The fail-fast bound for a "this must terminate" await (card #148): a
/// regression that hangs the loop FAILS here instead of stalling the suite.
///
/// Scaled under MIRI. MIRI's virtual clock advances **5 µs per basic block**
/// (`miri/src/clock.rs`: `NANOSECONDS_PER_BASIC_BLOCK = 5000`) — roughly 5000×
/// faster than the work it times — so a natively-calibrated bound fires
/// spuriously under the interpreter, on a test that is making fine progress.
/// Measured (#150): the 8×50-sender race needs ~20 s real under MIRI and passes
/// comfortably inside this bound, while the native 5 s fail-fast is unchanged.
#[must_use]
pub const fn terminate_bound() -> Duration {
    if cfg!(miri) {
        Duration::from_mins(10)
    } else {
        Duration::from_secs(5)
    }
}
