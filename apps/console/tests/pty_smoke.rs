//! Tier-2 ("Selenium-for-terminals") end-to-end smoke test (card #83).
//!
//! The in-process `TestBackend` suite (#76/#82) drives `App::render_once` /
//! `App::press` directly and can never reach the compiled binary. Two surfaces are
//! therefore structurally unreachable in-process and sit at 0%:
//!
//! - `console/src/main.rs` — the entrypoint: `clap` parsing, `spawn_poller` wiring,
//!   the `--demo` runtime, `ratatui::run`, clean teardown.
//! - the literal `event::read()` in `App::handle_events` — the one line `App::press`
//!   cannot substitute for.
//!
//! This test drives the *real* `bombay-console --demo` binary through a pseudo-terminal,
//! re-emulates the visible screen from the raw PTY bytes with `vt100`, and asserts on
//! the emulated grid — exercising startup -> the input poll -> clean shutdown.
//!
//! Non-flaky by construction: every wait is bounded and asserts a *specific* rendered
//! string; there are no fixed sleeps between an action and its assertion, so the test
//! can never pass "both ways". A hang turns into a loud, grid-dumping failure.

use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

/// Fixed terminal size so the layout — and the centered help popup
/// (`centered_rect(area, 54, ..)`) — always fit and render deterministically.
const ROWS: u16 = 40;
const COLS: u16 = 120;

/// Per-step ceiling for "wait until the grid shows X". Generous relative to the
/// 250ms input `TICK` and the demo's snapshot cadence, tight enough to fail fast.
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// A live `bombay-console --demo` process attached to a PTY, with a background thread
/// draining the master into a `vt100` parser so the test can assert on the rendered grid.
struct TerminalSession {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Box<dyn Write + Send>,
    parser: Arc<Mutex<vt100::Parser>>,
    reader_thread: Option<thread::JoinHandle<()>>,
}

impl TerminalSession {
    /// Open a fixed-size PTY, spawn the compiled console binary in `--demo` mode on its
    /// slave, and start streaming its output into a `vt100` parser of the same size.
    fn spawn_demo() -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: ROWS,
                cols: COLS,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("openpty failed");

        // `CARGO_BIN_EXE_<name>` is injected by cargo for integration tests of a crate
        // that has a `[[bin]]`, so we drive the real, freshly-built binary.
        let mut cmd = CommandBuilder::new(env!("CARGO_BIN_EXE_bombay-console"));
        cmd.arg("--demo");
        // A sane terminal type so crossterm/ratatui emit escapes vt100 can replay.
        cmd.env("TERM", "xterm-256color");

        let child = pair.slave.spawn_command(cmd).expect("spawn failed");
        // Drop the slave so that when the child exits, the master read returns EOF and
        // the reader thread can terminate.
        drop(pair.slave);

        let mut reader = pair.master.try_clone_reader().expect("clone reader failed");
        let writer = pair.master.take_writer().expect("take writer failed");

        let parser = Arc::new(Mutex::new(vt100::Parser::new(ROWS, COLS, 0)));
        let sink = Arc::clone(&parser);
        let reader_thread = thread::spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => sink.lock().unwrap().process(&buf[..n]),
                }
            }
        });

        Self {
            child,
            writer,
            parser,
            reader_thread: Some(reader_thread),
        }
    }

    /// Snapshot of the currently-visible grid as plain text.
    fn grid(&self) -> String {
        self.parser.lock().unwrap().screen().contents()
    }

    /// Send raw bytes as if typed at the terminal (raw mode: keys are their bytes).
    fn send(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).expect("write to pty failed");
        self.writer.flush().expect("flush pty failed");
    }

    /// Block until `needle` appears in the rendered grid, or fail with the grid dumped.
    fn wait_for(&self, needle: &str) {
        self.wait_until(needle, true);
    }

    /// Block until `needle` is *absent* from the rendered grid, or fail likewise.
    fn wait_for_absent(&self, needle: &str) {
        self.wait_until(needle, false);
    }

    fn wait_until(&self, needle: &str, want_present: bool) {
        let start = Instant::now();
        loop {
            if self.grid().contains(needle) == want_present {
                return;
            }
            assert!(
                start.elapsed() < STEP_TIMEOUT,
                "timed out after {STEP_TIMEOUT:?} waiting for {needle:?} to be \
                 {}; current grid:\n{}",
                if want_present { "present" } else { "absent" },
                self.grid(),
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    /// Wait for the child to exit and return whether it exited successfully, or fail
    /// (after killing it) if it does not exit within the step timeout.
    fn wait_for_exit(&mut self) -> bool {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait failed") {
                return status.success();
            }
            if start.elapsed() >= STEP_TIMEOUT {
                let _ = self.child.kill();
                panic!(
                    "console did not exit within {STEP_TIMEOUT:?} of 'q'; last grid:\n{}",
                    self.grid()
                );
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        // Ensure the child is gone even if an assertion unwound mid-test, so the reader
        // thread sees EOF and we never leak a process.
        let _ = self.child.kill();
        if let Some(handle) = self.reader_thread.take() {
            let _ = handle.join();
        }
    }
}

/// Startup -> input poll (help open/close, filter) -> clean quit, all through a real PTY.
#[test]
fn smoke_startup_help_filter_and_quit() {
    let mut session = TerminalSession::spawn_demo();

    // 1. main.rs booted, spawn_poller wired, ratatui::run drew the first frame.
    //    Anchored on the rename-stable "Console" substring (the dashboard title is
    //    mid-rename from "Kameo Console" -> "Bombay Console"), so a branding change
    //    won't break this smoke test.
    session.wait_for("Console");

    // 2. A real `event::read()` poll delivered '?' and the modal help popup rendered.
    session.send(b"?");
    session.wait_for("Keybindings");

    // 3. Esc dismisses the modal (through the same poll).
    session.send(&[0x1b]);
    session.wait_for_absent("Keybindings");

    // 4. '/' enters filter mode and the typed query echoes into the search line.
    session.send(b"/zzz");
    session.wait_for("/zzz");

    // 5. Esc leaves filter mode and clears the query.
    session.send(&[0x1b]);
    session.wait_for_absent("/zzz");

    // 6. 'q' quits; the process tears down the terminal and exits cleanly.
    session.send(b"q");
    assert!(
        session.wait_for_exit(),
        "console exited with a failure status"
    );
}
