//! CI tool: turns a cargo-mutants run into a pass/fail verdict that cannot
//! be vacuously green. See docs/adr/0006-mutation-viable-ratchet.md.

#![allow(
    clippy::redundant_pub_crate,
    reason = "multi-module binary crate: pub(crate) documents the crate-internal API surface across sibling modules; unreachable_pub is deferred workspace-wide (root Cargo.toml), so pub(crate) is the intent-revealing choice over bare pub"
)]
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "mutants-gate is a CLI tool: stdout carries the ratio report, stderr carries failure reasons"
)]

mod model;
mod gate;

fn main() {
    eprintln!("mutants-gate: not yet implemented");
}
