# Scope: console crate TUI pure rendering helpers (console/src/tui.rs). These are the
#        side-effect-free formatters and graph/layout math the renderer depends on. Each is
#        exercised at its edges (zero, max, overflow, div-by-zero, empty) so a regression in
#        the math surfaces here rather than as a garbled frame or a panic in the live UI.
#
# All values below were derived from the source (console/src/tui.rs); where the exact glyph
# or rounding is implementation-detail-heavy it is stated as a NOTE rather than guessed.
#
# Authoring rules: one cross-cutting tag per Scenario(/Outline); invariant-first Then; facts
# only. No step definitions (wiring phase). Most helpers are pure functions → Scenario
# Outline + Examples.

@console @tui
Feature: TUI rendering helpers — formatting, graph and layout math
  As the console renderer
  I want the pure helper functions to be correct at their boundaries
  So that no input (zero, overflow, empty, div-by-zero) garbles a frame or panics the UI

  # ---------------------------------------------------------------------------
  # fmt_short(Duration) → human "Ns" under a minute, else "Mm SSs"
  #   secs < 60 → "{secs:.1}s" ; else "{m}m{ss:02}s"   (tui.rs:1504-1512)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: fmt_short formats a duration compactly
    When fmt_short is called with <millis> milliseconds
    Then it returns "<text>"

    Examples:
      | millis  | text   |
      | 0       | 0.0s   |
      | 500     | 0.5s   |
      | 59900   | 59.9s  |
      | 60000   | 1m00s  |
      | 90000   | 1m30s  |
      | 3661000 | 61m01s |
    # NOTE: at/over 60s it switches to whole-second m/s (the sub-second precision is dropped);
    # there is no hour rollover — 3661s renders as 61m01s, not 1h01m01s.

  # ---------------------------------------------------------------------------
  # fmt_ago(Duration) → "Ns ago" / "Mm SSs ago" / "Hh MMm ago"   (tui.rs:1514-1523)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: fmt_ago formats an elapsed-since string
    When fmt_ago is called with <secs> seconds
    Then it returns "<text>"

    Examples:
      | secs | text         |
      | 0    | 0s ago       |
      | 59   | 59s ago      |
      | 60   | 1m 00s ago   |
      | 3599 | 59m 59s ago  |
      | 3600 | 1h 00m ago   |
      | 7320 | 2h 02m ago   |

  # ---------------------------------------------------------------------------
  # fmt_uptime(Duration) → always "HH:MM:SS" zero-padded   (tui.rs:1938-1946)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: fmt_uptime is a zero-padded HH:MM:SS clock
    When fmt_uptime is called with <secs> seconds
    Then it returns "<text>"

    Examples:
      | secs   | text     |
      | 0      | 00:00:00 |
      | 59     | 00:00:59 |
      | 3661   | 01:01:01 |
      | 86399  | 23:59:59 |
      | 90061  | 25:01:01 |
    # NOTE: hours are NOT capped at 24 — uptime past a day renders as 25:01:01, etc.

  # ---------------------------------------------------------------------------
  # spark_height(value, max) → 1..=4 dot column, with a max==0 guard   (tui.rs:1570-1575)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: spark_height scales a value into a 1..4 dot column
    When spark_height is called with value <value> and max <max>
    Then it returns <height>

    Examples:
      | value | max | height |
      | 0     | 0   | 1      |
      | 100   | 0   | 1      |
      | 0     | 10  | 1      |
      | 1     | 10  | 1      |
      | 5     | 10  | 2      |
      | 10    | 10  | 4      |
      | 999   | 10  | 4      |
    # INVARIANT: max==0 short-circuits to 1 (the baseline) — never a divide-by-zero. The
    # result is always clamped to 1..=4, so an idle column still shows the bottom dot and an
    # over-peak value never exceeds 4.

  # ---------------------------------------------------------------------------
  # braille(left, right) → one U+2800-based glyph, columns clamped to 0..=4  (tui.rs:1579-1585)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: braille builds a cell glyph from two clamped column heights
    When braille is called with left <left> and right <right>
    Then it returns the braille glyph for clamped heights (<cl>, <cr>)

    Examples:
      | left | right | cl | cr |
      | 0    | 0     | 0  | 0  |
      | 4    | 4     | 4  | 4  |
      | 9    | 9     | 4  | 4  |
      | 2    | 0     | 2  | 0  |
    # NOTE: left/right are clamped with `.min(4)` then index fixed tables; height 0 lights no
    # dots in that column. The exact code point (0x2800 + bits) is an implementation detail —
    # assert via the documented bit tables (LEFT/RIGHT at :1581-1582), not a guessed char.

  # ---------------------------------------------------------------------------
  # fade_toward_bg(color, factor) → lerp toward BG; factor 1.0 keeps, 0.0 = BG  (tui.rs:1588-1592)
  #   BG = Rgb(18,18,22)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: fade_toward_bg blends a color toward the background by factor
    Given a starting color Rgb(<r>,<g>,<b>)
    When fade_toward_bg is called with factor <factor>
    Then it returns Rgb(<or>,<og>,<ob>)

    Examples:
      | r   | g   | b   | factor | or  | og  | ob  | note            |
      | 200 | 200 | 200 | 1.0    | 200 | 200 | 200 | unchanged       |
      | 200 | 200 | 200 | 0.0    | 18  | 18  | 22  | fully BG        |
    # NOTE: factor 0.5 lands at the midpoint between the color and BG per channel, e.g.
    # R = 18 + (200-18)*0.5 = 109 (f32 truncation to u8). Pin the exact 0.5 row at wiring
    # time against the lerp `(target + (c-target)*factor) as u8` rounding (truncation, :1591).

  # ---------------------------------------------------------------------------
  # color_rgb(Color) → approximate RGB for named/Rgb colors; default ≈ FG  (tui.rs:1597-1608)
  #   FG = Rgb(205,205,212)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: color_rgb maps a color to its approximate RGB triple
    When color_rgb is called with <color>
    Then it returns Rgb(<r>,<g>,<b>)

    Examples:
      | color          | r   | g   | b   |
      | Rgb(10,20,30)  | 10  | 20  | 30  |
      | Red            | 235 | 80  | 80  |
      | LightRed       | 235 | 80  | 80  |
      | Yellow         | 220 | 180 | 90  |
      | Green          | 143 | 196 | 110 |
      | Cyan           | 110 | 180 | 200 |
      | Black          | 0   | 0   | 0   |
      | DarkGray       | 120 | 120 | 128 |
      | White          | 205 | 205 | 212 |
      | Reset          | 205 | 205 | 212 |
    # INVARIANT: any unlisted/Reset/default color falls through to ≈ FG (205,205,212), so the
    # fade math always has a concrete triple to blend (no panic on a named ANSI color).

  # ---------------------------------------------------------------------------
  # centered_rect(area, width, height) → centered, clamped to area  (tui.rs:1611-1620)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: centered_rect centers a clamped rect inside the area
    Given an area at (<ax>,<ay>) sized <aw>x<ah>
    When centered_rect is requested at <w>x<h>
    Then the result is at (<rx>,<ry>) sized <rw>x<rh>

    Examples:
      | ax | ay | aw  | ah | w  | h  | rx | ry | rw | rh | case              |
      | 0  | 0  | 100 | 40 | 50 | 10 | 25 | 15 | 50 | 10 | smaller, centered |
      | 0  | 0  | 50  | 10 | 50 | 10 | 0  | 0  | 50 | 10 | equal to area     |
      | 0  | 0  | 30  | 8  | 80 | 20 | 0  | 0  | 30 | 8  | larger → clamped  |
      | 5  | 3  | 20  | 10 | 10 | 4  | 10 | 6  | 10 | 4  | offset area       |
    # INVARIANT: width/height are clamped to the area first (`.min`), so a request larger than
    # the area can never produce a rect that overflows it — the (area-size)/2 offset stays >= 0.

  # ---------------------------------------------------------------------------
  # backpressure_style(len, capacity) → style by fill ratio; capacity==0 guarded  (tui.rs:1489-1502)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: backpressure_style colors a mailbox by fill ratio with a zero-cap guard
    When backpressure_style is called with len <len> and capacity <cap>
    Then the style is <style>

    Examples:
      | len | cap | style  | ratio                 |
      | 0   | 0   | normal | 0.0 (zero-cap guard)  |
      | 7   | 0   | normal | 0.0 (zero-cap guard)  |
      | 0   | 10  | normal | 0.0                   |
      | 4   | 10  | normal | 0.4                   |
      | 5   | 10  | yellow | 0.5 (>=0.5)           |
      | 8   | 10  | red    | 0.8 (>=0.8)           |
      | 100 | 10  | red    | 10.0 (over capacity)  |
    # INVARIANT: capacity==0 forces ratio 0.0 (normal/FG), never a divide-by-zero. "normal" =
    # Style::new().fg(FG); thresholds are >=0.8 red, >=0.5 yellow, else FG.

  # ---------------------------------------------------------------------------
  # mailbox_bar(len, capacity) → "len / cap  BAR  PCT%" + backpressure style  (tui.rs:1923-1936)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: mailbox_bar renders a 10-cell bar with a zero-cap guard
    When mailbox_bar is called with len <len> and capacity <cap>
    Then the text is "<text>"
    And the style matches backpressure_style for the same len and capacity

    Examples:
      | len | cap | text                         | note                |
      | 0   | 0   | 0 / 0  ░░░░░░░░░░  0%         | zero-cap guard      |
      | 0   | 10  | 0 / 10  ░░░░░░░░░░  0%        | empty               |
      | 5   | 10  | 5 / 10  █████░░░░░  50%       | half                |
      | 10  | 10  | 10 / 10  ██████████  100%    | full                |
      | 100 | 10  | 100 / 10  ██████████  100%   | clamped over-full   |
    # INVARIANT: capacity==0 → ratio 0.0 (empty bar, 0%), no divide-by-zero; the ratio is
    # clamped to 0.0..=1.0 so an over-full mailbox still renders a full 10-cell bar at 100%.

  # ---------------------------------------------------------------------------
  # actor_rate(actor, prev_received, dt) → msg/s, 0 when dt is None or zero  (tui.rs:1245-1257)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: actor_rate computes msg/s with a None/zero dt guard
    Given an actor whose messages_received is <now>
    And a previous received count of <prev> for that actor
    When actor_rate is called with dt <dt>
    Then it returns <rate>

    Examples:
      | now | prev | dt     | rate | note                          |
      | 100 | 100  | 1s     | 0    | no change                     |
      | 200 | 100  | 1s     | 100  | 100 msgs in 1s                |
      | 250 | 100  | 2s     | 75   | 150 msgs / 2s = 75            |
      | 200 | 100  | None   | 0    | no dt → 0 (first snapshot)    |
      | 200 | 100  | 0s     | 0    | dt==0 guard → 0 (no div)      |
      | 50  | 100  | 1s     | 0    | counter reset → saturating 0  |
    # INVARIANT: dt None or dt.as_secs_f64()==0.0 short-circuits to 0 (no divide-by-zero); a
    # decreased counter (process restart) saturates the delta to 0 rather than underflowing.
    # A missing prev entry also yields 0.

  @boundary
  Scenario: actor_rate is 0 when the actor has no previous-received entry
    Given an actor present in this snapshot but absent from the previous one
    When actor_rate is called with a 1s dt
    Then it returns 0

  # ---------------------------------------------------------------------------
  # short_type_name(&str) → strip module path and generics: a::b::Foo<X> → Foo  (tui.rs:1859-1862)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario Outline: short_type_name strips the module path and generic args
    When short_type_name is called with "<input>"
    Then it returns "<output>"

    Examples:
      | input                  | output |
      | a::b::Foo<X>           | Foo    |
      | Foo                    | Foo    |
      | crate::module::Bar     | Bar    |
      | Vec<a::b::Thing>       | Vec    |
      |                        |        |
      | ::Leading              | Leading|
    # INVARIANT: split at the first '<' (generics dropped), then take the last "::" segment.
    # The empty string maps to the empty string (no panic). A name with no '<' or "::" is
    # returned whole.

  # ---------------------------------------------------------------------------
  # sparkline_line(samples, max, width) → braille line, right-aligned & left-padded  (tui.rs:1547-1566)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: sparkline_line with no samples renders a full idle baseline
    When sparkline_line is called with no samples, max 0 and width 9
    Then the line has exactly 9 braille cells
    And every cell shows the idle baseline (bottom dot only)
    # NOTE: width*2 columns, zero-left-padded; max 0 → every spark_height is 1 (baseline).

  @sequence
  Scenario: sparkline_line right-aligns samples and scrolls older ones off the left
    Given more than 18 samples (oldest first) and width 9
    When sparkline_line is called with the busiest sample as max
    Then only the most recent 18 samples are shown (2 per cell)
    And the newest sample occupies the rightmost cell
    # INVARIANT: only the last width*2 samples survive; the series is right-aligned so new
    # samples enter on the right and the display scrolls left.

  @boundary
  Scenario: sparkline_line marks cells with traffic active and idle cells dim
    Given a width-9 sparkline where only the most recent sample is non-zero
    When the line is built
    Then the rightmost cell is drawn in the active (cyan) color
    And the remaining cells are drawn in the idle (grey) color

  # ---------------------------------------------------------------------------
  # detect_deadlocks(snapshot) → cycles in the functional wait-for graph  (tui.rs:1124-1171)
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: detect_deadlocks finds no cycle when no actor waits
    Given a snapshot where no actor has a waiting_on edge
    When detect_deadlocks runs
    Then it returns zero cycles

  @sequence
  Scenario: detect_deadlocks finds no cycle on a non-cyclic wait chain
    Given actors A→B→C where C waits on nothing
    When detect_deadlocks runs
    Then it returns zero cycles

  @sequence
  Scenario: detect_deadlocks reports a self-cycle (an actor waiting on itself)
    Given actor A whose waiting_on target is A
    When detect_deadlocks runs
    Then it returns exactly one cycle containing only A

  @sequence
  Scenario: detect_deadlocks reports a single two-actor cycle once
    Given actors A→B and B→A
    When detect_deadlocks runs
    Then it returns exactly one cycle
    And the cycle contains exactly A and B
    # INVARIANT: each member of a cycle is recorded in `in_cycle`, so a 2-cycle is emitted once,
    # not once per entry node.

  @sequence
  Scenario: detect_deadlocks reports a single multi-actor cycle once
    Given actors A→B, B→C and C→A
    When detect_deadlocks runs
    Then it returns exactly one cycle
    And the cycle contains exactly A, B and C

  @sequence
  Scenario: detect_deadlocks normalizes each cycle to start at its lowest id
    Given a 3-actor cycle among ids 5, 2 and 8 in wait order 5→2→8→5
    When detect_deadlocks runs
    Then the returned cycle begins with id 2
    And cycles are ordered by their first (lowest) id
    # INVARIANT: HashMap iteration is unordered, so each cycle is rotate_left'd to its minimum
    # id and the cycle list is sorted by first id — the banner is stable across snapshots.

  @sequence
  Scenario: detect_deadlocks separates two independent cycles
    Given a cycle A↔B and a separate cycle C↔D in the same snapshot
    When detect_deadlocks runs
    Then it returns exactly two cycles
    And neither cycle shares a member with the other

  @boundary
  Scenario: detect_deadlocks tolerates a wait edge to a non-existent target
    Given actor A waiting on a target id that no actor in the snapshot has
    When detect_deadlocks runs
    Then it returns zero cycles and does not panic
    # NOTE: a dangling target simply ends the chain (next.get returns None at :1151-1157).
