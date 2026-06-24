//! Cucumber harness for the `kameo_console` poller (`console/src/poller.rs`).
//!
//! Like `tui_bdd`, this must be a STANDARD libtest test (no `harness = false`):
//! cucumber 0.23's libtest-writer path does not implement nextest's `--list`
//! enumeration protocol, so `nix flake check`'s `cargoNextest` can only see it
//! when it is one ordinary test function.
//!
//! Task 12 (card #76): wires the request/reply + framing + size-gate scenarios
//! that exercise the REAL `Poller` (via `poll_once_over`) and the extracted
//! `check_frame_len` / `decode_frame` helpers. Truncation/EOF (Task 13) and the
//! connect/disconnect lifecycle (Task 14) are not yet wired, so a name-prefix
//! filter restricts the run to the scenarios with step definitions. The filter
//! is removed in Task 14 once the feature is fully covered.

mod steps;

use cucumber::World;
use steps::poller::PollerWorld;

/// Exact names of the scenarios wired in this task. `starts_with` over the full
/// name keeps the filter precise (no accidental matches) while staying robust to
/// nothing else in the file.
const WIRED: &[&str] = &[
    "A poll writes exactly one request byte then reads a length-prefixed frame",
    "A Snapshot encodes and decodes back to the same value (round-trip)",
    "A successful poll publishes the decoded Snapshot into the shared slot",
    "Sequential polls observe the server's advancing seq in order",
    "A frame whose length equals MAX_FRAME_BYTES is accepted",
    "A frame one byte larger than MAX_FRAME_BYTES is rejected as InvalidData",
    "A garbage maximal length 0xFFFFFFFF is rejected before allocation",
    "A zero-length frame decodes as an empty payload and fails MessagePack decode",
    "A well-sized frame carrying invalid MessagePack triggers reconnect",
    // Task 13: truncation / EOF boundary scenarios
    "A truncated payload (fewer bytes than the prefix promised) errors the poll",
    "A truncated length prefix (fewer than 4 bytes) errors the poll",
    "A length prefix exactly at MAX with a payload that under-delivers",
];

#[tokio::test(flavor = "multi_thread")]
async fn poller_features() {
    // `with_default_cli` stops cucumber from parsing the libtest flags nextest
    // injects (`--list`, `--exact`, `--format terse`), which would abort its
    // clap `Cli` with exit code 2.
    PollerWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit("../tests/features/console/poller.feature", |_, _, s| {
            WIRED.iter().any(|p| s.name.starts_with(p))
        })
        .await;
}
