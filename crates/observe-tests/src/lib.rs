//! Isolated model-checking boundary for Bombay's private observation code.

#[cfg(any(not(test), loom))]
#[path = "../../bombay/src/observe/mod.rs"]
pub mod observe;

#[cfg(any(not(test), loom))]
pub use observe::*;

#[cfg(not(test))]
#[path = "../../bombay/src/observe/test_support/mod.rs"]
pub mod probe;
