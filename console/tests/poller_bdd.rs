//! Cucumber harness for the `kameo_console` poller (`console/src/poller.rs`).
//!
//! Like `tui_bdd`, this must be a STANDARD libtest test (no `harness = false`):
//! cucumber 0.23's libtest-writer path does not implement nextest's `--list`
//! enumeration protocol, so `nix flake check`'s `cargoNextest` can only see it
//! when it is one ordinary test function.
//!
//! Task 14 (card #76): the poller feature is now FULLY wired — the request/reply
//! + framing + size-gate scenarios (Task 12), the truncation/EOF boundary
//! scenarios (Task 13), and the connect/disconnect/retry/mid-poll-death
//! lifecycle scenarios (Task 14, driving the real `connect_attempt` and the
//! bounded `poll_loop_until_error` twin of `poll_loop`). The name-prefix filter
//! is therefore GONE: the whole file runs with `fail_on_skipped`, so any
//! unwired or skipped scenario now FAILS the run (false-green elimination).

mod steps;

use cucumber::World;
use steps::poller::PollerWorld;

#[tokio::test(flavor = "multi_thread")]
async fn poller_features() {
    // `with_default_cli` stops cucumber from parsing the libtest flags nextest
    // injects (`--list`, `--exact`, `--format terse`), which would abort its
    // clap `Cli` with exit code 2.
    PollerWorld::cucumber()
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            // Anchor to CARGO_MANIFEST_DIR (= the `console` crate dir): nextest
            // does not guarantee the test cwd, so a bare relative path makes
            // cucumber fail with "Could not read path" under the nix-sandbox
            // `cargoNextest`. `/..` climbs from `console/` to the workspace root.
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/features/console/poller.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
