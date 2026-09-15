//! Isolated model-checking boundary for Bombay's private observation code.

#![allow(
    clippy::duplicate_mod,
    reason = "the shared probe is public for fuzz targets and is also compiled privately by Observe's own unit corpus"
)]

#[path = "../../bombay/src/observe/mod.rs"]
pub mod observe;

pub use observe::*;

#[path = "../../bombay/src/observe/test_support/mod.rs"]
pub mod probe;
