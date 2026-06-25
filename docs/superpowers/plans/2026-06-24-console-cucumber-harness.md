# Console cucumber harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the spec-only console `.feature` files (`tests/features/console/`) to the real SUT so every non-`@bug` console scenario is green under `nix flake check`, bootstrapping the reusable cucumber harness.

**Architecture:** Per-crate cucumber runners reach private SUT through a `#[cfg(feature="testing")]` surface auto-enabled by a self dev-dependency. `tui`/`poller` runners live in `console/tests/` (test `kameo_console`); `server_wire` in root `tests/` (tests the root `kameo` in-tree server). Feature files stay unified at root; runners reference them by relative path. Scenario Outlines bind via cucumber-native expansion (one step fn handles all Examples rows).

**Tech Stack:** Rust edition 2024, `cucumber = "0.23"`, `proptest = "1.11"`, `rstest = "0.26"`, tokio, `rmp-serde`, crane/nextest gate.

**Design doc:** `docs/superpowers/specs/2026-06-24-console-cucumber-harness-design.md`

---

## File structure

```
Cargo.toml                       MODIFY  workspace deps: cucumber, rstest, proptest
console/Cargo.toml               MODIFY  [features] testing; [dev-dependencies]; [[test]] targets
console/src/lib.rs               MODIFY  pub mod testing (gated re-exports)
console/src/tui.rs               MODIFY  18 helpers private fn -> pub(crate)
console/src/poller.rs            MODIFY  extract pub(crate) check_frame_len + decode_frame
console/tests/tui_bdd.rs         CREATE  runner main for tui.feature (+ properties)
console/tests/poller_bdd.rs      CREATE  runner main for poller.feature (+ properties)
console/tests/steps/mod.rs       CREATE  shared step-module declarations
console/tests/steps/tui.rs       CREATE  TuiWorld + tui step defs + proptest laws
console/tests/steps/poller.rs    CREATE  PollerWorld + poller step defs + proptest laws
Cargo.toml (root)                MODIFY  [features] testing; expose snapshot + reset hook
src/console/mod.rs               MODIFY  testing re-exports (snapshot, reset)
src/console/registry.rs          MODIFY  #[cfg(feature="testing")] reset_for_test()
tests/console_wire_bdd.rs        CREATE  WireWorld + server_wire step defs + proptest laws
docs/testing/README.md           MODIFY  document the World + step pattern
README.md                        MODIFY  every commit
```

## Execution notes (discovered during bootstrap — supersede the plan body where they conflict)

1. **Runner pattern (NOT `harness = false`).** cucumber 0.23's `harness = false` + libtest
   writer does not implement nextest's `--list`/`--exact` enumeration (CLI rejects them, exit 2),
   and `nix flake check` runs `cargoNextest`. So every runner is a **standard libtest test**:
   ```rust
   #[tokio::test(flavor = "multi_thread")]
   async fn <feature>_features() {
       World::cucumber()
           .fail_on_skipped()
           .with_default_cli()                 // stop cucumber parsing nextest's argv
           .filter_run_and_exit(<feature_path>, |_, _, s| <predicate>)
           .await;
   }
   ```
   `[[test]]` targets keep their name but **omit `harness = false`**. Consequence: a whole
   feature file is ONE nextest test — there is **no per-scenario process isolation**, so the
   server_wire global-static reset (`reset_for_test`) is essential, called per scenario.
2. **`pub`, not `pub(crate)`, for re-exported items.** `pub use` cannot re-export `pub(crate)`
   items at `pub` visibility (E0364/E0365). Make exposed helpers `pub` but keep their **module
   private** (`mod tui;`, `mod poller;` — no `pub`) so the gated `testing` module stays the only
   external door (still CLAUDE.md rule 4 compliant). Same fix applies to poller items (Task 11).
3. **Incremental filter discipline.** Each task BROADENS the `filter_run_and_exit` predicate to
   include its newly-wired scenarios, and VERIFIES the scenario count went up (guard against a
   typo'd predicate that vacuously matches zero scenarios = false green). The LAST task for each
   feature file drops the filter entirely and runs the whole file with `fail_on_skipped()` so any
   unwired scenario fails: Task 8 for `tui.feature`, Task 10 for `tui.properties.feature`,
   Task 14 for `poller.feature`, Task 15 for `poller.properties.feature`, Task 18+19 for
   `server_wire.feature`, Task 20 for `server_wire.properties.feature`.

## Conventions for every task

- **Toolchain:** no local cargo. Run all cargo via `nix develop -c <cmd>` (the flake dev shell), e.g. `nix develop -c cargo test -p kameo_console --test tui_bdd`.
- **README:** the pre-commit hook blocks any commit without a staged `README.md` change. Each commit step stages a one-line, truthful README update.
- **Facts only (CLAUDE.md rule 0):** every asserted value below is grounded in the feature file + source line cited.

---

# PHASE 1 — Bootstrap + `tui` (smallest blast radius)

### Task 1: Add workspace dev-dependencies

**Files:**
- Modify: `Cargo.toml` (root `[workspace.dependencies]`)

- [ ] **Step 1: Add the three deps to `[workspace.dependencies]`**

In root `Cargo.toml`, under `[workspace.dependencies]` (after the existing `tokio` line):

```toml
cucumber = { version = "0.23", features = ["libtest"] }
proptest = "1.11"
rstest = "0.26"
```

- [ ] **Step 2: Verify it resolves**

Run: `nix develop -c cargo metadata --format-version 1 >/dev/null`
Expected: exits 0 (lockfile updates, no resolution error).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock README.md
git commit -m "test(console): card #76 add cucumber/proptest/rstest workspace dev-deps"
```
(README: append to the docs/testing bullet that Phase 3 wiring of console started.)

---

### Task 2: Console `testing` feature + expose tui helpers

**Files:**
- Modify: `console/src/tui.rs` (18 helper signatures)
- Modify: `console/src/lib.rs`
- Modify: `console/Cargo.toml`

- [ ] **Step 1: Flip the 18 tui helpers from `fn` to `pub(crate) fn`**

In `console/src/tui.rs`, change each of these declarations from `fn name` to `pub(crate) fn name` (lines per current source): `detect_deadlocks` (:1124), `rate_context` (:1174), `sort_actors` (:1191), `compare` (:1204), `severity` (:1227), `actor_rate` (:1245), `backpressure_style` (:1489), `fmt_short` (:1504), `fmt_ago` (:1514), `sparkline_line` (:1547), `spark_height` (:1570), `braille` (:1579), `fade_toward_bg` (:1588), `color_rgb` (:1597), `centered_rect` (:1611), `short_type_name` (:1859), `mailbox_bar` (:1923), `fmt_uptime` (:1938). Also make `SortCol` and `STUCK_THRESHOLD` `pub(crate)` (used by compare/severity steps).

- [ ] **Step 2: Add the gated testing surface to `console/src/lib.rs`**

Append to `console/src/lib.rs`:

```rust
/// Test-only access to the crate's private helpers, for the cucumber harness.
/// Gated so normal builds never expose it (CLAUDE.md rule 4).
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::tui::{
        STUCK_THRESHOLD, SortCol, actor_rate, backpressure_style, braille, centered_rect,
        color_rgb, compare, detect_deadlocks, fade_toward_bg, fmt_ago, fmt_short, fmt_uptime,
        mailbox_bar, rate_context, severity, short_type_name, sort_actors, spark_height,
        sparkline_line,
    };
    // wire types the steps construct as fixtures:
    pub use kameo::console::wire::{
        ActorCounters, ActorId, ActorSnapshot, ActorStatus, HandlerActivity, Links, MailboxKind,
        MailboxStats, MessageCount, RefCounts, Snapshot, Totals, WaitEdge, WaitKind,
    };
}
```

Note: `mod tui;` is currently private in `lib.rs` — keep it private; the `testing` module re-exports from it. If the crate root has `mod tui;` without `pub`, `pub(crate)` items are reachable from `crate::tui::*` inside `testing`. Confirm `lib.rs` declares `mod tui;` (it does).

- [ ] **Step 3: Add the feature + self dev-dependency to `console/Cargo.toml`**

```toml
[features]
testing = []

[dev-dependencies]
kameo_console = { path = ".", features = ["testing"] }
cucumber = { workspace = true }
proptest = { workspace = true }
rstest = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync", "net", "io-util"] }
```

- [ ] **Step 4: Verify the feature compiles**

Run: `nix develop -c cargo build -p kameo_console --features testing`
Expected: builds clean. Then `nix develop -c cargo build -p kameo_console` (no feature) also builds, proving `testing` is not leaked into normal builds.

- [ ] **Step 5: Commit**

```bash
git add console/src/tui.rs console/src/lib.rs console/Cargo.toml Cargo.lock README.md
git commit -m "test(console): card #76 expose tui helpers via testing feature"
```

---

### Task 3: Bootstrap the runner + TuiWorld + first scenario (`fmt_short`)

This task proves the whole harness pattern with the single simplest helper. Once green, the rest of tui is more of the same.

**Files:**
- Create: `console/tests/steps/mod.rs`
- Create: `console/tests/steps/tui.rs`
- Create: `console/tests/tui_bdd.rs`
- Modify: `console/Cargo.toml` (`[[test]]` target)

- [ ] **Step 1: Declare the runner as a no-harness test target**

Append to `console/Cargo.toml`:

```toml
[[test]]
name = "tui_bdd"
harness = false
```

- [ ] **Step 2: Write the runner main**

Create `console/tests/tui_bdd.rs`:

```rust
mod steps;

use cucumber::{World, writer};
use steps::tui::TuiWorld;

#[tokio::main]
async fn main() {
    TuiWorld::cucumber()
        .with_writer(writer::Libtest::or_basic())
        .run("../tests/features/console/tui.feature")
        .await;
}
```

- [ ] **Step 3: Write the steps module wrapper**

Create `console/tests/steps/mod.rs`:

```rust
pub mod tui;
```

- [ ] **Step 4: Write TuiWorld + the `fmt_short` steps**

Create `console/tests/steps/tui.rs`:

```rust
use std::time::Duration;

use cucumber::{World, then, when};

#[derive(Debug, Default, World)]
pub struct TuiWorld {
    // Pure-helper scratch: the last string the SUT produced.
    last_string: String,
}

#[when(regex = r"^fmt_short is called with (\d+) milliseconds$")]
async fn when_fmt_short(world: &mut TuiWorld, millis: u64) {
    world.last_string = kameo_console::testing::fmt_short(Duration::from_millis(millis));
}

#[then(regex = r#"^it returns "(.*)"$"#)]
async fn then_returns_string(world: &mut TuiWorld, expected: String) {
    assert_eq!(world.last_string, expected);
}
```

- [ ] **Step 5: Run — expect the `fmt_short` scenario to drive real code and pass**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`
Expected: the 6 `fmt_short` rows pass; OTHER tui scenarios show as failed/undefined steps (expected — they're wired in later tasks). Confirm `fmt_short` rows specifically are green in the output.

If the `libtest` writer + plain `cargo test` misbehaves, fall back (per design): replace the runner main body with a `#[tokio::test]` wrapper that asserts no failures — see Appendix A. Re-run and confirm before continuing.

- [ ] **Step 6: Commit**

```bash
git add console/tests/ console/Cargo.toml README.md
git commit -m "test(console): card #76 bootstrap cucumber harness, wire fmt_short"
```

---

### Task 4: Remaining string/number formatter outlines

**Files:**
- Modify: `console/tests/steps/tui.rs`

These all reuse the `then_returns_string` / a new `then_returns_u8` Then. Each `When` parses the outline placeholders and calls the helper.

- [ ] **Step 1: Add the formatter `When` steps**

Append to `console/tests/steps/tui.rs`:

```rust
#[when(regex = r"^fmt_ago is called with (\d+) seconds$")]
async fn when_fmt_ago(world: &mut TuiWorld, secs: u64) {
    world.last_string = kameo_console::testing::fmt_ago(Duration::from_secs(secs));
}

#[when(regex = r"^fmt_uptime is called with (\d+) seconds$")]
async fn when_fmt_uptime(world: &mut TuiWorld, secs: u64) {
    world.last_string = kameo_console::testing::fmt_uptime(Duration::from_secs(secs));
}

#[when(regex = r#"^short_type_name is called with "(.*)"$"#)]
async fn when_short_type_name(world: &mut TuiWorld, input: String) {
    world.last_string = kameo_console::testing::short_type_name(&input).to_string();
}
```

Note on the empty-string row (`tui.feature:339`): the Examples row has an empty `input` and empty `output`; the `(.*)` captures `""` and `then_returns_string` asserts `""`. The `::Leading` row (`:340`) asserts `Leading`.

- [ ] **Step 2: Add `spark_height` When + a `u8` Then**

```rust
#[derive(Debug, Default, World)]  // (already defined above — do NOT redefine; add field instead)
```

Add a `last_u8: u8` field to `TuiWorld`, then:

```rust
#[when(regex = r"^spark_height is called with value (\d+) and max (\d+)$")]
async fn when_spark_height(world: &mut TuiWorld, value: u64, max: u64) {
    world.last_u8 = kameo_console::testing::spark_height(value, max);
}

#[then(regex = r"^it returns (\d+)$")]
async fn then_returns_u8(world: &mut TuiWorld, expected: u8) {
    assert_eq!(world.last_u8, expected);
}
```

- [ ] **Step 3: Run the four outlines**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`
Expected: `fmt_short`, `fmt_ago`, `fmt_uptime`, `spark_height`, `short_type_name` scenarios all green.

- [ ] **Step 4: Commit**

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire fmt_ago/fmt_uptime/spark_height/short_type_name"
```

---

### Task 5: Color, glyph and layout helpers

`braille`, `fade_toward_bg`, `color_rgb`, `centered_rect`, `backpressure_style`, `mailbox_bar`. These need `ratatui` types (`Color`, `Rect`, `Style`). Add `ratatui` to console dev-deps (already a normal dep) — reachable in tests via the crate.

**Files:**
- Modify: `console/tests/steps/tui.rs`

- [ ] **Step 1: braille — assert via the documented bit tables, not a guessed char**

The feature (`tui.feature:103-115`) asserts `braille(left,right)` equals "the braille glyph for clamped heights (cl,cr)". Compute the expected glyph from the same `LEFT`/`RIGHT` bit tables (`tui.rs:1581-1582`) inside the step so the assertion is independent of the SUT:

```rust
// Reference copy of the bit tables (tui.rs:1581-1582) — the oracle for the glyph.
const LEFT: [u8; 5] = [0x00, 0x40, 0x44, 0x46, 0x47];
const RIGHT: [u8; 5] = [0x00, 0x80, 0xA0, 0xB0, 0xB8];

fn braille_oracle(cl: u8, cr: u8) -> char {
    char::from_u32(0x2800 + u32::from(LEFT[cl as usize] | RIGHT[cr as usize])).unwrap()
}
```

Add a `last_char: char` field to `TuiWorld`. Steps:

```rust
#[when(regex = r"^braille is called with left (\d+) and right (\d+)$")]
async fn when_braille(world: &mut TuiWorld, left: u8, right: u8) {
    world.last_char = kameo_console::testing::braille(left, right);
}

#[then(regex = r"^it returns the braille glyph for clamped heights \((\d+), (\d+)\)$")]
async fn then_braille(world: &mut TuiWorld, cl: u8, cr: u8) {
    assert_eq!(world.last_char, braille_oracle(cl, cr));
}
```

- [ ] **Step 2: fade_toward_bg + color_rgb (Rgb triple assertions)**

Add `last_rgb: (u8, u8, u8)` to `TuiWorld`. `fade_toward_bg` returns a `ratatui::style::Color`; map it to a triple via `color_rgb` is wrong (it would re-map) — instead match `Color::Rgb(r,g,b)` directly:

```rust
use ratatui::style::Color;

#[given(regex = r"^a starting color Rgb\((\d+),(\d+),(\d+)\)$")]
async fn given_start_color(world: &mut TuiWorld, r: u8, g: u8, b: u8) {
    world.color = Color::Rgb(r, g, b);
}

#[when(regex = r"^fade_toward_bg is called with factor ([0-9.]+)$")]
async fn when_fade(world: &mut TuiWorld, factor: f32) {
    let Color::Rgb(r, g, b) = kameo_console::testing::fade_toward_bg(world.color, factor) else {
        panic!("fade_toward_bg must return Color::Rgb");
    };
    world.last_rgb = (r, g, b);
}

#[then(regex = r"^it returns Rgb\((\d+),(\d+),(\d+)\)$")]
async fn then_rgb(world: &mut TuiWorld, r: u8, g: u8, b: u8) {
    assert_eq!(world.last_rgb, (r, g, b));
}
```

Add a `color: Color` field to `TuiWorld` (default `Color::Reset`). For `color_rgb` (`tui.feature:145-160`) parse the color name/Rgb literal:

```rust
fn parse_color(s: &str) -> Color {
    match s {
        "Red" => Color::Red, "LightRed" => Color::LightRed, "Yellow" => Color::Yellow,
        "Green" => Color::Green, "Cyan" => Color::Cyan, "Black" => Color::Black,
        "DarkGray" => Color::DarkGray, "White" => Color::White, "Reset" => Color::Reset,
        rgb if rgb.starts_with("Rgb(") => {
            let nums: Vec<u8> = rgb.trim_start_matches("Rgb(").trim_end_matches(')')
                .split(',').map(|n| n.trim().parse().unwrap()).collect();
            Color::Rgb(nums[0], nums[1], nums[2])
        }
        other => panic!("unhandled color in feature: {other}"),
    }
}

#[when(regex = r"^color_rgb is called with (.+)$")]
async fn when_color_rgb(world: &mut TuiWorld, color: String) {
    world.last_rgb = kameo_console::testing::color_rgb(parse_color(&color));
}
```

(`then_rgb` from above is reused.)

- [ ] **Step 3: centered_rect (Rect in/out)**

Add `area: Rect` and `last_rect: Rect` fields.

```rust
use ratatui::layout::Rect;

#[given(regex = r"^an area at \((\d+),(\d+)\) sized (\d+)x(\d+)$")]
async fn given_area(world: &mut TuiWorld, x: u16, y: u16, w: u16, h: u16) {
    world.area = Rect { x, y, width: w, height: h };
}

#[when(regex = r"^centered_rect is requested at (\d+)x(\d+)$")]
async fn when_centered(world: &mut TuiWorld, w: u16, h: u16) {
    world.last_rect = kameo_console::testing::centered_rect(world.area, w, h);
}

#[then(regex = r"^the result is at \((\d+),(\d+)\) sized (\d+)x(\d+)$")]
async fn then_rect(world: &mut TuiWorld, x: u16, y: u16, w: u16, h: u16) {
    assert_eq!(world.last_rect, Rect { x, y, width: w, height: h });
}
```

- [ ] **Step 4: backpressure_style + mailbox_bar**

`backpressure_style` returns a `Style`; the feature names bands `normal`/`yellow`/`red`. Map via the documented colors (`tui.rs:1489-1502`: red `>=0.8`, yellow `>=0.5`, else FG). Build a reference and compare the produced `Style.fg`:

```rust
use ratatui::style::Style;

const FG: Color = Color::Rgb(205, 205, 212);

fn style_band(style: Style) -> &'static str {
    match style.fg {
        Some(Color::Red) => "red",
        Some(Color::Yellow) => "yellow",
        _ => "normal",
    }
}
```

Confirm against source which exact colors `backpressure_style` sets (read `tui.rs:1489-1502`); adjust the match arms to the real `Color`s it uses before asserting. Steps:

```rust
#[when(regex = r"^backpressure_style is called with len (\d+) and capacity (\d+)$")]
async fn when_backpressure(world: &mut TuiWorld, len: usize, cap: usize) {
    world.last_style = kameo_console::testing::backpressure_style(len, cap);
}

#[then(regex = r"^the style is (normal|yellow|red)$")]
async fn then_style(world: &mut TuiWorld, band: String) {
    assert_eq!(style_band(world.last_style), band);
}

#[when(regex = r"^mailbox_bar is called with len (\d+) and capacity (\d+)$")]
async fn when_mailbox_bar(world: &mut TuiWorld, len: usize, cap: usize) {
    let (text, style) = kameo_console::testing::mailbox_bar(len, cap);
    world.last_string = text;
    world.last_style = style;
    world.mb_len = len;
    world.mb_cap = cap;
}

#[then(regex = r"^the style matches backpressure_style for the same len and capacity$")]
async fn then_style_matches(world: &mut TuiWorld) {
    let expected = kameo_console::testing::backpressure_style(world.mb_len, world.mb_cap);
    assert_eq!(world.last_style, expected);
}
```

(`mailbox_bar` text uses the existing `then_returns_string` Then; add `last_style: Style`, `mb_len: usize`, `mb_cap: usize` fields.)

- [ ] **Step 5: Run and verify**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`
Expected: braille, fade_toward_bg, color_rgb, centered_rect, backpressure_style, mailbox_bar scenarios green.

- [ ] **Step 6: Commit**

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire color/glyph/layout tui helpers"
```

---

### Task 6: Snapshot-driven helpers — rate_context, actor_rate, severity, compare/sort_actors

These need `ActorSnapshot`/`Snapshot` fixtures. Add a fixture builder to the steps module.

**Files:**
- Modify: `console/tests/steps/tui.rs`

- [ ] **Step 1: Add fixture constructors**

```rust
use std::time::{Duration, SystemTime};
use kameo_console::testing::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, HandlerActivity, Links, MailboxKind,
    MailboxStats, RefCounts, Snapshot, Totals, WaitEdge, WaitKind,
};

fn actor(id: u64) -> ActorSnapshot {
    ActorSnapshot {
        id: ActorId(id),
        name: format!("Actor{id}"),
        status: ActorStatus::Running,
        handling: None,
        waiting_on: None,
        strategy: None,
        spawned_at: SystemTime::UNIX_EPOCH,
        mailbox: MailboxStats { kind: MailboxKind::Unbounded, len: 0, capacity: None },
        counters: ActorCounters::default(),
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

fn snapshot(actors: Vec<ActorSnapshot>) -> Snapshot {
    Snapshot {
        seq: 0,
        captured_at: SystemTime::UNIX_EPOCH,
        uptime: Duration::ZERO,
        actors,
        totals: Totals::default(),
    }
}
```

- [ ] **Step 2: rate_context steps** (`tui.feature:230-247`)

Add `prev_received: std::collections::HashMap<ActorId, u64>` and `dt: Option<Duration>` fields.

```rust
#[given("a snapshot and no previous snapshot")]
async fn given_no_prev(world: &mut TuiWorld) {
    let (pr, dt) = kameo_console::testing::rate_context(&snapshot(vec![actor(1)]), None);
    world.prev_received = pr;
    world.dt = dt;
}

#[then("the previous-received map is empty")]
async fn then_prev_empty(world: &mut TuiWorld) {
    assert!(world.prev_received.is_empty());
}

#[then("the returned dt is None")]
async fn then_dt_none(world: &mut TuiWorld) {
    assert_eq!(world.dt, None);
}
```

For the reversed-clock scenario (`:239`): build prev at `t=10s` and current at `t=4s` using `SystemTime::UNIX_EPOCH + Duration` and assert `dt == None` (duration_since errs → `.ok()` None, `tui.rs:1186`):

```rust
#[given("a previous snapshot captured at t = 10s")]
async fn given_prev_10s(world: &mut TuiWorld) {
    world.prev_snapshot = Some({
        let mut s = snapshot(vec![actor(1)]);
        s.captured_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        s
    });
}

#[given("a current snapshot captured at t = 4s (earlier than the previous)")]
async fn given_cur_4s(world: &mut TuiWorld) {
    let mut cur = snapshot(vec![actor(1)]);
    cur.captured_at = SystemTime::UNIX_EPOCH + Duration::from_secs(4);
    let (_pr, dt) = kameo_console::testing::rate_context(&cur, world.prev_snapshot.as_ref());
    world.dt = dt;
}

#[when("rate_context is called")]
async fn when_rate_context(_world: &mut TuiWorld) { /* computed in the Given for these scenarios */ }
```

(Add `prev_snapshot: Option<Snapshot>` field. The first scenario's `When rate_context is called` is a no-op because the Given already computed it; this is acceptable since the scenario asserts the result, but to keep the `When` meaningful, you may move the computation into `when_rate_context` using stored snapshots — implementer's choice. Either way assert the specific values.)

- [ ] **Step 3: actor_rate steps** (`tui.feature:254-276`)

```rust
fn parse_dt(s: &str) -> Option<Duration> {
    match s {
        "None" => None,
        "0s" => Some(Duration::ZERO),
        sec if sec.ends_with('s') => Some(Duration::from_secs(sec.trim_end_matches('s').parse().unwrap())),
        other => panic!("unhandled dt: {other}"),
    }
}

#[given(regex = r"^an actor whose messages_received is (\d+)$")]
async fn given_actor_received(world: &mut TuiWorld, now: u64) {
    let mut a = actor(1);
    a.counters.messages_received = now;
    world.actor = Some(a);
}

#[given(regex = r"^a previous received count of (\d+) for that actor$")]
async fn given_prev_count(world: &mut TuiWorld, prev: u64) {
    world.prev_received.insert(ActorId(1), prev);
}

#[when(regex = r"^actor_rate is called with dt (None|\d+s)$")]
async fn when_actor_rate(world: &mut TuiWorld, dt: String) {
    let a = world.actor.as_ref().unwrap();
    world.last_u64 = kameo_console::testing::actor_rate(a, &world.prev_received, parse_dt(&dt));
}

#[then(regex = r"^it returns (\d+)$")]    // NOTE: collides with then_returns_u8 — use a distinct phrasing
async fn then_returns_u64(world: &mut TuiWorld, expected: u64) {
    assert_eq!(world.last_u64, expected);
}
```

IMPORTANT regex-collision note: `spark_height`'s Then is also `^it returns (\d+)$`. Cucumber matches step text regardless of which scenario; a single `^it returns (\d+)$` step can serve BOTH if the World stores the last numeric result in one field. Resolve by using ONE numeric field `last_u64` for both `spark_height` (cast u8→u64) and `actor_rate`, and ONE `then_returns_u64`. Update Task 4 Step 2 accordingly (store `spark_height` result as `u64`). This keeps a single unambiguous Then.

The "absent from previous" scenario (`:273`): Given builds an actor with id present in snapshot but `prev_received` empty for that id, `When actor_rate is called with a 1s dt`, `Then it returns 0`.

- [ ] **Step 4: severity steps** (`tui.feature:283-300`)

```rust
fn parse_status(s: &str) -> (ActorStatus, Option<HandlerActivity>) {
    match s {
        "Stopped" => (ActorStatus::Stopped { at: SystemTime::UNIX_EPOCH, reason: "x".into() }, None),
        "Restarting" => (ActorStatus::Restarting, None),
        "Running (handling >= 5s)" =>
            (ActorStatus::Running, Some(HandlerActivity { message: "M".into(), elapsed: Duration::from_secs(5) })),
        "Stopping" => (ActorStatus::Stopping, None),
        "Starting" => (ActorStatus::Starting, None),
        "Running (handling < 5s)" =>
            (ActorStatus::Running, Some(HandlerActivity { message: "M".into(), elapsed: Duration::from_secs(1) })),
        "Running (not handling)" => (ActorStatus::Running, None),
        other => panic!("unhandled status: {other}"),
    }
}

#[given(regex = r"^an actor whose status is (.+)$")]
async fn given_status(world: &mut TuiWorld, status: String) {
    let (st, handling) = parse_status(&status);
    let mut a = actor(1);
    a.status = st;
    a.handling = handling;
    world.actor = Some(a);
}

#[when("severity is computed")]
async fn when_severity(world: &mut TuiWorld) {
    world.last_u64 = u64::from(kameo_console::testing::severity(world.actor.as_ref().unwrap()));
}
```

(`Then it returns <rank>` reuses `then_returns_u64`. Verify `STUCK_THRESHOLD == 5s` against `tui.rs:926`; the "handling >= 5s" fixture uses exactly 5s to hit the `>=` boundary.)

- [ ] **Step 5: compare / sort_actors steps** (`tui.feature:307-322`)

```rust
use kameo_console::testing::{SortCol, compare, sort_actors};

#[given(regex = r"^two actors with equal mailbox length but ids (\d+) and (\d+)$")]
async fn given_two_actors(world: &mut TuiWorld, id_a: u64, id_b: u64) {
    world.two = (actor(id_a), actor(id_b));   // both mailbox.len == 0 (equal)
}

#[when("compare is called for SortCol::Mailbox")]
async fn when_compare(world: &mut TuiWorld) {
    let (a, b) = &world.two;
    world.ordering = Some(compare(a, b, SortCol::Mailbox, &Default::default(), None));
}

#[then(regex = r"^the actor with id (\d+) orders before the actor with id (\d+)$")]
async fn then_orders_before(world: &mut TuiWorld, first: u64, _second: u64) {
    // compare(a,b): Less means a (id_a) before b (id_b).
    let (a, _b) = &world.two;
    let before_id = if world.ordering.unwrap().is_le() { a.id.0 } else { world.two.1.id.0 };
    assert_eq!(before_id, first);
}

#[when("sort_actors is called for SortCol::Mailbox with desc = true")]
async fn when_sort_desc(world: &mut TuiWorld) {
    let (a, b) = &world.two;
    let mut v = vec![a, b];
    sort_actors(&mut v, SortCol::Mailbox, true, &Default::default(), None);
    world.sorted_first_id = Some(v[0].id.0);
}
```

For the desc scenario, the `Then the actor with id 7 orders before the actor with id 3` asserts `world.sorted_first_id == Some(7)`. Add a dedicated Then or reuse with a flag — implementer picks one unambiguous phrasing per the feature text (`:309` uses "compare is called", `:318` uses "sort_actors is called"). Add fields `two: (ActorSnapshot, ActorSnapshot)`, `ordering: Option<std::cmp::Ordering>`, `sorted_first_id: Option<u64>`.

- [ ] **Step 6: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`
Expected: rate_context, actor_rate, severity, compare, sort_actors scenarios green.

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire snapshot-driven tui helpers (rate/severity/compare)"
```

---

### Task 7: sparkline_line scenarios

**Files:**
- Modify: `console/tests/steps/tui.rs`

`sparkline_line(samples, max, width) -> Line<'static>`. A `Line` is a vec of `Span`s; each span's content is one braille cell, styled active (cyan) or idle (grey). Assertions: cell count, baseline glyph, active vs idle color, right-alignment.

- [ ] **Step 1: Steps for the three sparkline scenarios** (`tui.feature:350-370`)

```rust
use ratatui::text::Line;

#[when(regex = r"^sparkline_line is called with no samples, max (\d+) and width (\d+)$")]
async fn when_sparkline_empty(world: &mut TuiWorld, max: u64, width: usize) {
    world.last_line = Some(kameo_console::testing::sparkline_line(&[], max, width));
}

#[then(regex = r"^the line has exactly (\d+) braille cells$")]
async fn then_line_cells(world: &mut TuiWorld, n: usize) {
    assert_eq!(world.last_line.as_ref().unwrap().spans.len(), n);
}

#[then("every cell shows the idle baseline (bottom dot only)")]
async fn then_all_baseline(world: &mut TuiWorld) {
    // baseline glyph = braille(1,1) per spark_height max==0 ⇒ 1 (tui.feature NOTE :354)
    let baseline = braille_oracle(1, 1);
    for span in &world.last_line.as_ref().unwrap().spans {
        assert!(span.content.chars().all(|c| c == baseline), "non-baseline cell: {:?}", span.content);
    }
}
```

For the right-align/scroll scenario (`:357`) and the active/idle color scenario (`:366`): build `samples` per the Given, call `sparkline_line`, and assert the rightmost span carries the newest data (color = active/cyan) and the rest are idle/grey. Read `tui.rs:1547-1566` for the exact active/idle `Color`s and assert the produced span styles against them (CLAUDE.md rule 8 — specific values, not "non-empty").

Add `last_line: Option<Line<'static>>` field.

- [ ] **Step 2: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire sparkline_line scenarios"
```

---

### Task 8: detect_deadlocks example scenarios

**Files:**
- Modify: `console/tests/steps/tui.rs`

8 example scenarios (`tui.feature:377-431`). Build snapshots whose actors carry `waiting_on` edges, run `detect_deadlocks`, assert the returned `Vec<Vec<ActorId>>`.

- [ ] **Step 1: Edge builder + steps**

```rust
fn waiting(mut a: ActorSnapshot, target: u64) -> ActorSnapshot {
    a.waiting_on = Some(WaitEdge { target: ActorId(target), kind: WaitKind::Ask, elapsed: Duration::ZERO });
    a
}
```

Implement one `Given` per scenario shape (no-wait, chain A→B→C, self-cycle, 2-cycle, 3-cycle, min-id-normalize 5→2→8, two disjoint cycles, dangling target). Each Given builds the snapshot and stores `world.cycles = detect_deadlocks(&snap)`. Add `cycles: Vec<Vec<ActorId>>` field. Example for the 2-cycle (`:395`):

```rust
#[given("actors A→B and B→A")]
async fn given_two_cycle(world: &mut TuiWorld) {
    let snap = snapshot(vec![waiting(actor(1), 2), waiting(actor(2), 1)]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}
```

Map A,B,C,D to ids 1,2,3,4; the min-id scenario uses literal ids 5,2,8.

- [ ] **Step 2: Then steps**

```rust
#[then("it returns zero cycles")]
async fn then_zero_cycles(world: &mut TuiWorld) { assert!(world.cycles.is_empty()); }

#[then(regex = r"^it returns exactly (one|two) cycles?$")]
async fn then_n_cycles(world: &mut TuiWorld, n: String) {
    let expected = if n == "one" { 1 } else { 2 };
    assert_eq!(world.cycles.len(), expected);
}

#[then(regex = r"^the returned cycle begins with id (\d+)$")]
async fn then_cycle_begins(world: &mut TuiWorld, id: u64) {
    assert_eq!(world.cycles[0][0], ActorId(id));
}
```

Add Then steps for "contains exactly A and B" (assert the sorted member set), "self-cycle containing only A" (`vec![vec![ActorId(1)]]`), "neither cycle shares a member", "does not panic" (reaching the assert is the proof). Assert specific id sets, never just counts where the feature names members.

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test tui_bdd`

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire detect_deadlocks example scenarios"
```

---

### Task 9: tui property laws (`tui.properties.feature`, `@property`)

**Files:**
- Create: `console/tests/tui_props_bdd.rs` (second runner, same World/steps module)
- Modify: `console/tests/steps/tui.rs`
- Modify: `console/Cargo.toml` (`[[test]]` for the props runner)

Each `@property` scenario binds to a step that runs an inline `proptest!`. Because cucumber steps are async and `proptest!` is sync, run the proptest in a blocking section.

- [ ] **Step 1: Add the props runner target + main**

`console/Cargo.toml`:
```toml
[[test]]
name = "tui_props_bdd"
harness = false
```
`console/tests/tui_props_bdd.rs`:
```rust
mod steps;
use cucumber::{World, writer};
use steps::tui::TuiWorld;

#[tokio::main]
async fn main() {
    TuiWorld::cucumber()
        .with_writer(writer::Libtest::or_basic())
        .run("../tests/features/console/tui.properties.feature")
        .await;
}
```

- [ ] **Step 2: spark_height law step** (`tui.properties.feature:47`)

```rust
use proptest::prelude::*;

#[then("the result is an integer in the closed range [1, 4]")]
async fn law_spark_height(_world: &mut TuiWorld) {
    proptest!(|(v in any::<u64>(), m in any::<u64>())| {
        let h = kameo_console::testing::spark_height(v, m);
        prop_assert!((1..=4).contains(&h));
        if m == 0 { prop_assert_eq!(h, 1); }
    });
    // boundary-biased cases the GEN comment requires:
    for (v, m, exp) in [(0u64,0u64,1u8),(100,0,1),(0,10,1),(10,10,4),(u64::MAX,10,4)] {
        assert_eq!(kameo_console::testing::spark_height(v, m), exp);
    }
}
```

The `Given/When` lines for property scenarios are generic ("Given any value v and any max m" / "When spark_height(v,m) is computed"); bind them to no-op steps, and put the proptest in the first `Then`. The follow-up `And` lines ("when m == 0 the result is exactly 1") bind to no-op or a focused assert.

- [ ] **Step 3: One law step per property scenario**

Implement the same pattern for: `actor_rate` (no panic + guard outcomes, `:58`), `mailbox_bar` (10 cells, pct∈[0,100], cap0⇒0%, len>cap⇒100%, `:72`), `backpressure_style` (band = true ratio, `:85`), `centered_rect` (containment, `:97`), `short_type_name` (idempotent + substring, `:111`), `fmt_short`/`fmt_ago`/`fmt_uptime` (non-empty/no-panic/field ranges, `:124/:134/:144`), `braille` (U+2800..=U+28FF + clamp idempotence, `:155`), `color_rgb` (total + FG default, `:168`), `sparkline_line` (exactly w cells, `:180`). Each uses the generator from its `# GEN:` line including the named boundary values, and asserts the `# ORACLE:` predicate. Code for each follows Step 2's shape with the helper's signature.

- [ ] **Step 4: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test tui_props_bdd`
Expected: all `@property` tui scenarios green.

```bash
git add console/tests/ console/Cargo.toml README.md
git commit -m "test(console): card #76 wire tui @property laws (proptest)"
```

---

### Task 10: detect_deadlocks `@model` law

**Files:**
- Modify: `console/tests/steps/tui.rs`

- [ ] **Step 1: Reference oracle (functional-graph successor-chase)**

```rust
use std::collections::HashMap;

/// Reference cycle finder for a functional graph (≤1 successor per node).
/// Mirrors the SUT's domain (tui.rs:1124-1159): follow each node's single successor;
/// a revisited node on the walk forms a cycle. Normalize to min id, sort by first id.
fn cycles_oracle(edges: &HashMap<u64, u64>) -> Vec<Vec<u64>> {
    let mut in_cycle = std::collections::HashSet::new();
    let mut cycles: Vec<Vec<u64>> = Vec::new();
    for &start in edges.keys() {
        if in_cycle.contains(&start) { continue; }
        let mut path = Vec::new();
        let mut pos = HashMap::new();
        let mut cur = start;
        loop {
            if let Some(&i) = pos.get(&cur) {
                let cyc = path[i..].to_vec();
                in_cycle.extend(cyc.iter().copied());
                cycles.push(cyc);
                break;
            }
            if in_cycle.contains(&cur) { break; }
            match edges.get(&cur) {
                Some(&n) => { pos.insert(cur, path.len()); path.push(cur); cur = n; }
                None => break,
            }
        }
    }
    for c in &mut cycles {
        if let Some(p) = (0..c.len()).min_by_key(|&i| c[i]) { c.rotate_left(p); }
    }
    cycles.sort_by_key(|c| c.first().copied());
    cycles
}
```

- [ ] **Step 2: The model law step** (`tui.properties.feature:200`)

```rust
#[then("the returned cycles are exactly the cycles of the graph, each reported once")]
async fn law_detect_deadlocks(_world: &mut TuiWorld) {
    proptest!(|(edges in proptest::collection::hash_map(0u64..8, prop_oneof![Just(None), (0u64..8).prop_map(Some), (8u64..12).prop_map(Some)], 0..8))| {
        // Build a snapshot: every key is an actor; Some(target) becomes a waiting_on edge.
        // Targets in 8..12 are "dangling" (no such actor) and must end a chain.
        let actors: Vec<_> = edges.keys().map(|&id| {
            match edges[&id] { Some(t) => waiting(actor(id), t), None => actor(id) }
        }).collect();
        let sut = kameo_console::testing::detect_deadlocks(&snapshot(actors))
            .into_iter().map(|c| c.into_iter().map(|x| x.0).collect::<Vec<_>>()).collect::<Vec<_>>();
        // Oracle uses only edges whose target is a real node (dangling ends a chain).
        let real: HashMap<u64,u64> = edges.iter()
            .filter_map(|(&k,&v)| v.filter(|t| edges.contains_key(t)).map(|t| (k,t))).collect();
        prop_assert_eq!(sut, cycles_oracle(&real));
    });
}
```

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test tui_props_bdd`
Expected: the `@model` detect_deadlocks scenario green. Phase 1 (tui) complete.

```bash
git add console/tests/steps/tui.rs README.md
git commit -m "test(console): card #76 wire detect_deadlocks @model law"
```

---

# PHASE 2 — `poller`

### Task 11: Extract testable decode + size-gate from `poll()`

**Files:**
- Modify: `console/src/poller.rs`
- Modify: `console/src/lib.rs` (testing re-exports)

The property laws need the size gate and the msgpack decode independent of a live socket. Extract two `pub(crate)` free functions and have `poll()` call them (behaviour-preserving refactor).

- [ ] **Step 1: Add the two helpers**

In `console/src/poller.rs`:

```rust
/// The frame-size gate: accept iff `len <= MAX_FRAME_BYTES`, else InvalidData (poller.rs:113).
pub(crate) fn check_frame_len(len: u32) -> io::Result<()> {
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot frame too large ({len} bytes)"),
        ));
    }
    Ok(())
}

/// Decode a payload into a Snapshot, mapping any rmp error to InvalidData (poller.rs:123-124).
pub(crate) fn decode_frame(buf: &[u8]) -> io::Result<Snapshot> {
    let Message::Snapshot(snapshot) =
        rmp_serde::from_slice(buf).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(snapshot)
}
```

Make `MAX_FRAME_BYTES` `pub(crate)`. Rewrite `poll()` to use them:

```rust
fn poll(&mut self) -> io::Result<()> {
    self.stream.write_all(&[0])?;
    let mut len = [0u8; 4];
    self.stream.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len);
    check_frame_len(len)?;
    let mut buf = vec![0u8; len as usize];
    self.stream.read_exact(&mut buf)?;
    *self.snapshot.lock().unwrap() = Some(decode_frame(&buf)?);
    Ok(())
}
```

- [ ] **Step 2: Re-export through testing**

In `console/src/lib.rs` `testing` module add:
```rust
pub use crate::poller::{MAX_FRAME_BYTES, check_frame_len, decode_frame};
```

- [ ] **Step 3: Verify existing build still works**

Run: `nix develop -c cargo build -p kameo_console && nix develop -c cargo build -p kameo_console --features testing`
Expected: both clean.

- [ ] **Step 4: Commit**

```bash
git add console/src/poller.rs console/src/lib.rs README.md
git commit -m "test(console): card #76 extract poller check_frame_len/decode_frame"
```

---

### Task 12: Poller runner + PollerWorld + framing sequence scenarios

**Files:**
- Create: `console/tests/poller_bdd.rs`
- Modify: `console/tests/steps/mod.rs` (add `pub mod poller;`)
- Create: `console/tests/steps/poller.rs`
- Modify: `console/Cargo.toml` (`[[test]]`)

`poller.feature` `@sequence` + `@boundary` scenarios can be exercised directly against `check_frame_len`/`decode_frame` plus a loopback `TcpStream` pair for the wire-level ones. NOTE: the private `Poller::poll` is not exposed; the size-gate/decode laws use the extracted free fns, and the round-trip uses `decode_frame` over bytes produced by `rmp_serde::to_vec_named(&Message::Snapshot(..))`.

- [ ] **Step 1: Runner target + main**

```toml
[[test]]
name = "poller_bdd"
harness = false
```
```rust
// console/tests/poller_bdd.rs
mod steps;
use cucumber::{World, writer};
use steps::poller::PollerWorld;

#[tokio::main]
async fn main() {
    PollerWorld::cucumber()
        .with_writer(writer::Libtest::or_basic())
        .run("../tests/features/console/poller.feature")
        .await;
}
```

- [ ] **Step 2: PollerWorld + round-trip + size-gate steps**

Create `console/tests/steps/poller.rs`. Define `PollerWorld` holding: an optional encoded frame `Vec<u8>`, the decode/gate `io::Result`, and a constructed `Snapshot`. Wire:
- "A Snapshot encodes and decodes back to the same value" (`poller.feature:43`): build a `Snapshot` with seq 7 + known actors, encode via `rmp_serde::to_vec_named(&Message::Snapshot(s))`, `decode_frame`, assert field-for-field (Snapshot has no `PartialEq`, `wire.rs` — compare `seq`, `actors.len()`, each actor `id`/`name`, etc., OR re-encode both and compare bytes).
- size-gate boundary scenarios (`:108/:116/:123`): call `check_frame_len(L)` for `L ∈ {MAX, MAX+1, 0xFFFFFFFF}`, assert `Ok`/`Err(InvalidData)` and that the error message names the size.
- zero-length + invalid-msgpack (`:130/:139`): `decode_frame(&[])` and `decode_frame(&[0xFF; 4])` → assert `Err`, `ErrorKind::InvalidData`.

The single-poll write/read-prefix scenario (`:36`) and lifecycle scenarios go through a real loopback socket — see Task 14. For Task 12 cover the round-trip, size-gate, zero-length and invalid-msgpack scenarios.

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test poller_bdd`
Expected: the round-trip + size-gate + decode-error scenarios green; socket-level scenarios pending (Task 14).

```bash
git add console/tests/ console/Cargo.toml README.md
git commit -m "test(console): card #76 wire poller round-trip + size-gate scenarios"
```

---

### Task 13: Poller boundary scenarios over a loopback socket

**Files:**
- Modify: `console/tests/steps/poller.rs`

The truncated-prefix, truncated-payload, and under-delivery scenarios (`poller.feature:147/:155/:162`) need a real `TcpStream`. Because `Poller` is private and not exposed, drive these at the protocol level: open a `std::net::TcpListener` on `127.0.0.1:0`, connect a client, have the server thread write the crafted bytes, and on the client side perform the exact read sequence `poll()` does (`write_all(&[0])`, `read_exact(4)`, `check_frame_len`, `read_exact(len)`, `decode_frame`) — asserting the specific `io::ErrorKind` (`UnexpectedEof` for short reads).

- [ ] **Step 1: Loopback fixture**

```rust
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn loopback() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server)
}
```

- [ ] **Step 2: Steps for the three truncation scenarios**

For each: server writes the crafted prefix/payload then drops; client does the read sequence; assert `err.kind() == ErrorKind::UnexpectedEof`. Assert the SPECIFIC kind, not just "an error" (CLAUDE.md rule 8).

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test poller_bdd`

```bash
git add console/tests/steps/poller.rs README.md
git commit -m "test(console): card #76 wire poller truncation boundary scenarios"
```

---

### Task 14: Poller lifecycle + the single-poll sequence scenario

**Files:**
- Modify: `console/tests/steps/poller.rs`

Lifecycle scenarios (`poller.feature:69-101`) assert `ConsoleState`/`ConnectionState` transitions and the 5s backoff. `connect_loop`/`poll_loop`/`spawn_poller` are private and infinite-looping → not directly unit-testable. Two of these are `@timing` (`:69` connect-timeout records Disconnected; `:76` 5s backoff). Drive what is observable without the private loop:
- connect-timeout (`:69`): `TcpStream::connect_timeout` to an unbound `127.0.0.1:0`-style dead addr with a 50ms timeout returns `Err`; assert the error is non-empty when formatted (mirrors `connect_loop` building `Disconnected { error, since }`, poller.rs:50-53). Construct a `ConnectionState::Disconnected` from it and assert it carries the error string + a `since` instant.
- single-poll write/read-prefix (`:36`): over a loopback socket, the client writes `[0]`, the server reads exactly 1 byte and replies with a valid length-prefixed frame; assert the client read the 4-byte prefix then `len` payload bytes.

For the backoff (`:76`), `Connecting→Connected` (`:83`), mid-poll death (`:90`), and reconnect (`:98`) scenarios that require the private forever-loop: these need either (a) exposing `connect_loop`/`poll_loop` via the `testing` feature with a bounded variant, or (b) asserting the observable `ConnectionState` transitions a test can construct. Prefer (a): add `#[cfg(feature="testing")]` thin wrappers that run ONE connect attempt / ONE poll and return the resulting `ConnectionState`, re-exported via `testing`. Implement these wrappers, then wire the scenarios to them. Do NOT sleep a real 5s in the test — assert the backoff Duration value the code uses (`Duration::from_secs(5)`, poller.rs:54) by exposing it as a `pub(crate) const BACKOFF` and asserting equality.

- [ ] **Step 1: Add `BACKOFF` const + bounded testing wrappers in `poller.rs`**

Extract the literal `Duration::from_secs(5)` into `pub(crate) const BACKOFF: Duration = Duration::from_secs(5);` and use it at poller.rs:54. Add `#[cfg(feature="testing")]` `pub(crate) fn try_connect_once(...) -> ConnectionState` and `pub(crate) fn poll_once(...) -> io::Result<()>` wrappers. Re-export `BACKOFF` + wrappers + `ConnectionState` via `testing`.

- [ ] **Step 2: Wire the lifecycle scenarios to the wrappers**

Assert the specific `ConnectionState` variant and payload per scenario; assert `BACKOFF == Duration::from_secs(5)` for the backoff scenario.

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test poller_bdd`
Expected: all non-`@bug` poller.feature scenarios green.

```bash
git add console/src/poller.rs console/src/lib.rs console/tests/steps/poller.rs README.md
git commit -m "test(console): card #76 wire poller lifecycle scenarios via bounded test hooks"
```

---

### Task 15: Poller property laws

**Files:**
- Create: `console/tests/poller_props_bdd.rs`
- Modify: `console/Cargo.toml`, `console/tests/steps/poller.rs`

Laws (`poller.properties.feature`): round-trip identity (`:29`), size-gate predicate (`:42`), oversized-rejected-before-alloc (`:55`), invalid-msgpack-total (`:66`).

- [ ] **Step 1: Props runner target + main** (mirror Task 9 Step 1, feature path `poller.properties.feature`, World `PollerWorld`).

- [ ] **Step 2: Law steps**

- round-trip: `proptest!` over arbitrary `Snapshot` (build a `prop_compose!` strategy producing `Snapshot` with seq∈{boundary}, actors vec of varied ids/names incl empty/unicode, all `ActorStatus`/`MailboxKind` variants). Assert `re-encode(decode_frame(encode(s))) == encode(s)` (bytes equal, since no PartialEq).
- size-gate: `proptest!(|(l in any::<u32>())| { prop_assert_eq!(check_frame_len(l).is_ok(), l <= MAX_FRAME_BYTES); })` plus the named boundaries.
- oversized: for `l > MAX` assert `Err(InvalidData)` and that no `vec![0u8; l as usize]` is built (the gate returns before alloc — assert by calling `check_frame_len` alone).
- invalid-msgpack-total: `proptest!(|(b in proptest::collection::vec(any::<u8>(), 0..256))| { let r = decode_frame(&b); prop_assert!(r.is_ok() || r.as_ref().unwrap_err().kind() == ErrorKind::InvalidData); });` (never panics).

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test -p kameo_console --test poller_props_bdd`

```bash
git add console/tests/ console/Cargo.toml README.md
git commit -m "test(console): card #76 wire poller @property laws"
```

---

# PHASE 3 — `server_wire` (root `kameo` crate)

### Task 16: Root crate `testing` feature: expose snapshot + reset hook

**Files:**
- Modify: `Cargo.toml` (root `[features]`, `[dev-dependencies]`)
- Modify: `src/console/mod.rs`
- Modify: `src/console/registry.rs`

- [ ] **Step 1: Add a registry reset hook**

In `src/console/registry.rs`, add:

```rust
/// Test-only: clear the global registry + reset the process-global counters so a
/// cucumber scenario starts from a known state (the statics SEQ/REAPED_STOPPED/
/// TOTAL_SPAWNED are process-global, registry.rs:32-34).
#[cfg(feature = "testing")]
pub(crate) fn reset_for_test() {
    REGISTRY.lock().unwrap().clear();
    SEQ.store(0, Ordering::Relaxed);
    REAPED_STOPPED.store(0, Ordering::Relaxed);
    TOTAL_SPAWNED.store(0, Ordering::Relaxed);
}
```

(Confirm the registry static's name — read registry.rs for the `HashMap` static; use its real identifier.)

- [ ] **Step 2: Expose via a root testing module**

In `src/console/mod.rs` add:
```rust
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use super::registry::{reset_for_test, snapshot};
    pub use super::wire;
}
```

- [ ] **Step 3: Root crate feature + self dev-dep**

In root `Cargo.toml` `[features]` add `testing = ["console"]` (the reset hook lives behind both). Add to root `[dev-dependencies]`:
```toml
kameo = { path = ".", features = ["testing"] }
cucumber = { workspace = true }
proptest = { workspace = true }
```
(`kameo` self dev-dep enables `testing` for the root crate's own tests.)

- [ ] **Step 4: Verify build**

Run: `nix develop -c cargo build --features testing && nix develop -c cargo build`
Expected: both clean; `testing` not active in the plain build.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml src/console/mod.rs src/console/registry.rs Cargo.lock README.md
git commit -m "test(console): card #76 root testing feature (snapshot + registry reset)"
```

---

### Task 17: WireWorld + server harness + first seq scenario

**Files:**
- Create: `tests/console_wire_bdd.rs`
- Modify: root `Cargo.toml` (`[[test]]`)

`tests/console.rs` already demonstrates spawning actors + `serve`; mirror its setup. `snapshot(grave_window)` is callable directly via `kameo::console::testing::snapshot`, so most seq/totals scenarios do not need a TCP client at all — call `snapshot()` repeatedly and assert seq deltas. Scenarios that explicitly exercise the TCP framing use a `TcpStream` to the served address.

- [ ] **Step 1: Runner target + main**

```toml
[[test]]
name = "console_wire_bdd"
harness = false
required-features = ["testing"]
```
```rust
// tests/console_wire_bdd.rs
use cucumber::{World, writer};

#[derive(Debug, Default, World)]
pub struct WireWorld {
    seqs: Vec<u64>,
    // + handles/actor refs as needed
}

#[tokio::main]
async fn main() {
    WireWorld::cucumber()
        .with_writer(writer::Libtest::or_basic())
        .run("../tests/features/console/server_wire.feature")
        .await;
}
```

NOTE the feature path is `../tests/...` only if cucumber's CWD is the crate root; for the ROOT crate the features are at `tests/features/console/server_wire.feature` (no `../`). Use the root-relative path `tests/features/console/server_wire.feature`.

- [ ] **Step 2: Reset + seq-delta steps**

Every scenario starts by calling `kameo::console::testing::reset_for_test()` in a `Background` or `Given` so the global statics are deterministic. Wire "seq strictly increases across rapid sequential polls" (`server_wire.feature:44`) and "seq advances by exactly one" (`:53`):

```rust
#[given("a console server with at least one live actor")]
async fn given_server_actor(world: &mut WireWorld) {
    kameo::console::testing::reset_for_test();
    // spawn one actor as tests/console.rs does so the registry is non-empty
    // (use the same demo/test actor pattern).
}

#[when(regex = r"^the client requests (\d+) snapshots back to back$")]
async fn when_n_snapshots(world: &mut WireWorld, n: usize) {
    for _ in 0..n {
        let s = kameo::console::testing::snapshot(std::time::Duration::from_secs(300)).await;
        world.seqs.push(s.seq);
    }
}

#[then("each snapshot's seq is strictly greater than the previous one")]
async fn then_seq_increasing(world: &mut WireWorld) {
    assert!(world.seqs.windows(2).all(|w| w[1] > w[0]));
}

#[then("the second snapshot's seq equals the first snapshot's seq plus one")]
async fn then_seq_plus_one(world: &mut WireWorld) {
    assert_eq!(world.seqs[1], world.seqs[0] + 1);
}
```

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test --features testing --test console_wire_bdd`
Expected: the two seq scenarios green.

```bash
git add tests/console_wire_bdd.rs Cargo.toml README.md
git commit -m "test(console): card #76 WireWorld + seq monotonicity scenarios"
```

---

### Task 18: Remaining `@sequence` + `@lifecycle` + `@boundary` server_wire scenarios

**Files:**
- Modify: `tests/console_wire_bdd.rs`

- [ ] **Step 1: captured_at/uptime monotonic** (`:62`) — call `snapshot()` twice, assert `s2.captured_at >= s1.captured_at` and `s2.uptime >= s1.uptime`.

- [ ] **Step 2: grave-window boundary + totals** (`:99/:181/:190`) — stop an actor, then call `snapshot(ttl)` with chosen `ttl`. Because the reap predicate is `since.elapsed() > ttl` (registry.rs:470, strict), call once with a large `ttl` (actor present, `Stopped` with reason) and once with `ttl = Duration::ZERO` after a tiny real wait so `elapsed() > 0` (actor reaped). For `total_stopped == 3` (`:194`): after `reset_for_test`, stop 3 actors, reap 2 (poll with `ttl=0` after they age), then poll and assert `totals.total_stopped == 3` and `== REAPED_STOPPED(2) + 1`. Assert exact values.

- [ ] **Step 3: server boundary scenarios** (`:144/:152/:161/:172`) — the request-byte-ignored, pipelined-requests, no-frame-cap, and encode-failure scenarios go through a real TCP client to the served address. Start the server via `kameo::console::serve`, connect a `TcpStream`, send `0xFF` / 3 bytes / surplus bytes, and assert one length-prefixed frame per byte with strictly increasing seq. For encode-failure (`:172`): documented as a wiring detail — if an unencodable Snapshot can't be constructed without intrusive hooks, assert the observable contract (client sees EOF on the length read, no partial frame) using a server-side injected encode error behind `#[cfg(feature="testing")]`, OR mark this single scenario's mechanism explicitly in the plan note and surface it (never a silent skip).

- [ ] **Step 4: lifecycle scenarios** (`:112/:119/:126/:133`) — client-disconnect-mid-stream, one-client-disconnect-doesnt-disturb-another, idle-client-no-snapshot, shutdown-refuses-connections. Use real `TcpStream`s + the `ConsoleHandle` from `serve`; assert specific outcomes (second client still gets a frame; post-shutdown `TcpStream::connect` errors).

- [ ] **Step 5: Run and commit**

Run: `nix develop -c cargo test --features testing --test console_wire_bdd`

```bash
git add tests/console_wire_bdd.rs README.md
git commit -m "test(console): card #76 wire server_wire sequence/lifecycle/boundary scenarios"
```

---

### Task 19: `@linearizability` server_wire scenarios

**Files:**
- Modify: `tests/console_wire_bdd.rs`

Concurrent scenarios (`:73/:82/:91`) — real overlap with `tokio::spawn` + `tokio::sync::Barrier` (CLAUDE.md rule 8).

- [ ] **Step 1: unique-seq-under-concurrent-clients** (`:73`) — spawn 8 tasks each calling `snapshot()` several times behind a `Barrier`; collect all seqs; assert the set has no duplicates (`HashSet::len() == total`).

- [ ] **Step 2: membership atomic under concurrent spawn** (`:82`) and each-actor-once-under-concurrent-stops (`:91`) — spawn/stop actors on tasks behind a barrier while polling; assert every returned actor has a set `id`+`status` and that `ids` are unique within a snapshot (`actors.iter().map(|a| a.id)` has no dup).

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test --features testing --test console_wire_bdd`

```bash
git add tests/console_wire_bdd.rs README.md
git commit -m "test(console): card #76 wire server_wire @linearizability scenarios"
```

---

### Task 20: server_wire property laws

**Files:**
- Create: `tests/console_wire_props_bdd.rs`
- Modify: root `Cargo.toml`

Laws (`server_wire.properties.feature`): seq strictly increasing + `+1`-stepped (`:30`), captured_at/uptime non-decreasing (`:43`), total_stopped conserved (`:55`), membership linearizable (`:76`).

- [ ] **Step 1: Props runner target + main** (feature `server_wire.properties.feature`, `required-features = ["testing"]`).

- [ ] **Step 2: Law steps** — for seq/totals laws, `reset_for_test()` then a `proptest!`-driven op count `n` calling `snapshot()` n times asserting `seq_i == seq_0 + i`. For total_stopped, model `ever_stopped` over a generated spawn/stop/reap op sequence and assert `totals.total_stopped == ever_stopped` at the final poll. proptest with side-effecting global state must run sequentially — use `proptest!` with a single-threaded config and `reset_for_test()` at the top of each case.

- [ ] **Step 3: Run and commit**

Run: `nix develop -c cargo test --features testing --test console_wire_props_bdd`

```bash
git add tests/ Cargo.toml README.md
git commit -m "test(console): card #76 wire server_wire @property/@model laws"
```

---

### Task 21: Full gate + hakari + docs + close

**Files:**
- Modify: `docs/testing/README.md`, `README.md`
- Run: `cargo hakari generate`

- [ ] **Step 1: Document the harness pattern**

Add a "Wiring (Phase 3) — the World + step pattern" section to `docs/testing/README.md`: the `testing`-feature surface, runner-per-feature with the libtest writer, self dev-dependency feature activation, cucumber-native Outline expansion, proptest-in-a-step for laws. Reference the design doc.

- [ ] **Step 2: hakari**

Run: `nix develop -c cargo hakari generate` then `nix develop -c cargo hakari verify`
Expected: workspace-hack updated, verify clean. Stage any changes.

- [ ] **Step 3: The single gate**

Run: `nix flake check`
Expected: green — build + clippy + fmt + nextest + doctest + deny + audit + actionlint all pass; every non-`@bug` console scenario green.
If a scenario flakes (timing/concurrency), fix determinism (no real sleeps; barrier-based overlap) — never `#[ignore]` a non-`@bug` scenario.

- [ ] **Step 4: Final commit**

```bash
git add docs/testing/README.md README.md .config/hakari.toml workspace-hack/ Cargo.toml Cargo.lock
git commit -m "test(console): card #76 document harness pattern; hakari; close console wiring"
```

- [ ] **Step 5: Open the PR** (only if the user asks) referencing card #76; do not add Claude attribution.

---

## Appendix A — tokio-test fallback runner (if libtest writer fights nextest)

Replace a runner's `main` with:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn run_feature() {
    let summary = TuiWorld::cucumber()
        .run("../tests/features/console/tui.feature")
        .await;
    // cucumber returns the run; assert nothing failed.
    assert!(!summary.execution_has_failed(), "cucumber scenarios failed");
}
```

(Confirm the cucumber 0.23 API for accessing the failure summary; if `run` returns `()`, use `.run_and_exit()` in a `harness=false` `main` instead — exits non-zero on failure, which nextest reports as a failed test.) Keep `[[test]] harness = false` removed for the tokio-test form (it needs the standard harness). Use this form per-runner only if the primary libtest path is unreliable; the rest of the plan is identical.

---

## Self-review notes

- **Spec coverage:** tui (Tasks 3-8) + tui.properties (9-10); poller (12-14) + poller.properties (15); server_wire (17-19) + server_wire.properties (20). All six feature files covered.
- **Global-static hazard:** addressed by `reset_for_test()` (Task 16) + delta assertions; the one absolute assertion (`total_stopped==3`) uses the reset.
- **Determinism:** grave-window driven via `snapshot(ttl)` not sleeps; concurrency via Barrier; no real 5s backoff sleep (assert `BACKOFF` const).
- **Type consistency:** `last_u64` is the single numeric Then field shared by spark_height/actor_rate/severity; `then_returns_string` shared by all string helpers; fixture `actor(id)`/`snapshot(actors)` reused throughout.
- **Open mechanism flags (resolve at execution, never silent-skip):** encode-failure scenario (Task 18 Step 3) and cucumber-0.23 failure-summary API (Appendix A) are the two spots needing a confirm-against-source decision.
