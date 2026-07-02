# PTY-driven E2E smoke test (card #83) — design

> Follow-up to #76/#82 (epic #74). The in-process `TestBackend` tier drives
> `App::render_once` / `App::press` directly and can never reach the compiled
> binary. This card adds a **Tier-2** ("Selenium-for-terminals") test that drives
> the real `bombay-console` binary through a pseudo-terminal.

## The gap this closes

Two surfaces are **structurally unreachable in-process** and sit at 0% after #76/#82:

- `console/src/main.rs` — the binary entrypoint: `clap` arg parsing, `spawn_poller`
  wiring, the `--demo` runtime bootstrap, `ratatui::run`, clean teardown.
- The literal `event::read()` in `App::handle_events` (`console/src/tui.rs:723`).
  In-process, `App::press` substitutes for the *dispatch* (`on_key`) but never for
  the actual blocking terminal read — that line only runs when a real terminal
  delivers a key.

A PTY test is the only tier that exercises **startup → real `event::read()` poll →
clean shutdown** end to end.

## Approach (decided)

- **PTY driver:** `portable-pty` (wezterm's crate). Lower-level than `expectrl`, with
  explicit fixed-size control and a plain reader/writer. We assert on a re-emulated
  grid, so `expectrl`'s byte-stream `expect` layer would be redundant.
- **Screen re-emulation:** `vt100`. The PTY yields raw bytes *with* ANSI escapes;
  `vt100::Parser` replays them into a grid exactly like a real terminal, so
  assertions read rendered text (`screen().contents()`) rather than brittle escape
  sequences.
- **Gating:** always-on under `nix flake check` first. Every wait is bounded and
  asserts a specific rendered string; no fixed sleeps between action and assertion.
  Fallback **only if** it proves flaky/unsupported in the sandbox: `#[ignore]`-by-default
  + an explicit CI opt-in step — never a silently-skipped green.

## Components

New file `console/tests/pty_smoke.rs` (a plain integration test, **not** a cucumber
runner — this tier is one imperative E2E, not a `.feature` spec).

New console `[dev-dependencies]`: `portable-pty = "0.9"`, `vt100 = "0.16"`.

### `TerminalSession` helper (in the test file)

- `openpty(PtySize { rows: 40, cols: 120, .. })` — fixed size so the layout and the
  centered help popup (`centered_rect(area, 54, …)`) always fit.
- Spawn `env!("CARGO_BIN_EXE_bombay-console")` with `--demo` on the PTY slave.
  `--demo` self-hosts the example actor system on an ephemeral loopback port, so the
  test needs no external server. Cargo sets `CARGO_BIN_EXE_bombay-console` for
  integration tests of a package that has a `[[bin]]`.
- One background thread drains `master.try_clone_reader()` into an
  `Arc<Mutex<vt100::Parser>>` (parser sized 40×120 to match the PTY). Read returns
  EOF when the child exits — the thread ends cleanly.
- `master.take_writer()` sends keystrokes.
- `wait_for(substring, timeout)` / `wait_for_absent(substring, timeout)`: bounded
  poll loop — lock the parser, check `screen().contents()`, else short sleep, until a
  hard per-step timeout (~5s). On miss, fail with the current grid dumped into the
  message so CI failures are diagnosable.

## Test body — `smoke_startup_help_and_quit`

1. `wait_for("Kameo Console", 5s)` — dashboard top title (`tui.rs:369`), always drawn.
   Proves `main.rs` started, `spawn_poller` wired, `ratatui::run` drew frame 1.
2. send `?` → `wait_for("Keybindings", 5s)` — help popup title (`tui.rs:341`). Proves
   the real `event::read()` poll delivered the key and re-rendered (**the in-process gap**).
3. send `Esc` → `wait_for_absent("Keybindings", 5s)` — modal dismissed via the poll.
4. (from the card's script) send `/` then a query → `wait_for` the typed query to
   appear in the filter line; then `Esc` to leave filter mode.
5. send `q` → poll `child.try_wait()` in a bounded loop (~5s); assert exit success.
   On timeout, `child.kill()` + fail.

Steps 1–3 + 5 are load-bearing; step 4 rounds out the card's keystroke script.

## Error handling / non-flakiness

- Every wait is bounded and asserts a **specific** rendered string (CLAUDE.md rule 8).
- No fixed `sleep` between an action and its assertion — only the poll-loop's short
  inter-check sleep, which cannot cause a false pass (the assertion still gates).
- A bug-probe (missing key delivery, no clean exit) makes the test **fail**, not hang
  forever — the per-step timeout converts a hang into a loud failure.

## Risk: the nix build sandbox

The Linux sandbox must provide `/dev/ptmx` + `/dev/pts` (nix mounts a private
devpts) and a loopback interface (nix brings `lo` up) for `--demo`'s server. Rust
test sources are kept by `craneLib.fileset.commonCargoSources`, so — unlike the
`.feature` catalog — **no `flake.nix` fileset change is needed**. If the sandbox
cannot open a PTY or the test is flaky, apply the `#[ignore]`-by-default + CI opt-in
fallback above.

## Docs / README impact

No public API change — this is a test tier. Per the per-card rule, the change is
recorded in `docs/testing/coverage-baseline.md` and noted in
`docs/testing/README.md` (Tier-2 PTY test now exists); the README carries no change.

## Out of scope

- Non-`--demo` startup paths (they need a live external server — a different tier).
- Exhaustive key coverage (that is the in-process tier's job, #76/#82).
- Any change to production `main.rs`/`tui.rs` behavior.
