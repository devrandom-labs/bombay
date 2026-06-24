# Phase 2 (card #74): laws over the console TUI's pure rendering helpers, layered on
# tui.feature's example outlines. See docs/testing/properties.md.
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model (here: a reference cycle/SCC finder).
#   * Each scenario ALSO carries one Phase-1 category tag.
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only, grounded in console/src/tui.rs (signatures verified against source).
#   * No step definitions (wiring is Phase 3).
#
# Verified helper signatures/behaviour (console/src/tui.rs):
#   * spark_height(value: u64, max: u64) -> u8 : max==0 ⇒ 1; else ((v/max)*4).round().clamp(1,4) (:1570).
#   * actor_rate(actor, prev_received: &HashMap<ActorId,u64>, dt: Option<Duration>) -> u64 :
#       0 unless dt is Some and dt.as_secs_f64()>0 and prev has the id; delta is
#       saturating_sub (counter reset ⇒ 0) (:1245-1257).
#   * backpressure_style(len: usize, capacity: usize) -> Style : ratio 0.0 if cap==0 else
#       len/cap (UNCLAMPED here); thresholds ≥0.8 red, ≥0.5 yellow, else FG (:1489-1502).
#   * mailbox_bar(len, capacity) -> (String, Style) : ratio (len/cap).clamp(0.0,1.0), cap==0⇒0.0;
#       10-cell bar; style == backpressure_style(len,capacity) (:1923-1936).
#   * centered_rect(area: Rect, width: u16, height: u16) -> Rect : width/height .min(area.*),
#       offset (area-size)/2 (:1611-1620).
#   * short_type_name(name: &str) -> &str : split('<').next, then rsplit("::").next (:1859-1862).
#   * fmt_short / fmt_ago / fmt_uptime take a Duration (:1504,:1514,:1938).
#   * detect_deadlocks(snapshot: &Snapshot) -> Vec<Vec<ActorId>> : cycles of the FUNCTIONAL
#       wait-for graph (≤1 out-edge/node), each normalized to start at its min id, list sorted
#       by first id (:1124-1171).

@console @tui @phase2
Feature: TUI helpers — laws over graph, layout and formatting math
  As the console renderer
  I want every pure helper to obey its range/containment/totality law for ALL inputs
  So that no value (zero, overflow, empty, div-by-zero, cycle) garbles a frame or panics

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary
  Scenario: spark_height is always in 1..=4 for any value and max, including max==0
    Given any value v and any max m
    When spark_height(v, m) is computed
    Then the result is an integer in the closed range [1, 4]
    And when m == 0 the result is exactly 1 (the baseline, no divide-by-zero)
    # GEN: v, m ∈ boundary-biased u64 {0, 1, m-1, m, m+1, u64::MAX}; pairs include m==0 with
    #      v ∈ {0, 1, MAX}, and v > m (over-peak).
    # ORACLE: the range predicate 1 <= h <= 4, plus the m==0 ⇒ 1 short-circuit (:1571-1574).
    # Generalizes: tui.feature "spark_height scales a value into a 1..4 dot column".

  @property @boundary
  Scenario: actor_rate is >= 0 and never panics for any counters and dt, including dt zero/None
    Given any actor with messages_received now, any previous count prev, and any dt
    When actor_rate(actor, prev_received, dt) is computed
    Then it returns a u64 (>= 0) and never panics
    And it returns 0 when dt is None, when dt is zero, or when prev has no entry for the actor
    And a decreased counter (now < prev) saturates the delta to 0 rather than underflowing
    # GEN: now, prev ∈ boundary-biased u64 {0, 1, u64::MAX}; dt ∈ {None, Some(0s), Some(1ns),
    #      Some(1s), Some(Duration::MAX)}; prev_received ∈ {absent id, present id}.
    # ORACLE: the guard `dt.as_secs_f64() > 0.0 && prev present` else 0; delta via saturating_sub
    #         (:1250-1255). Total over all inputs — assert no panic + the guard outcomes.
    # Generalizes: tui.feature "actor_rate computes msg/s with a None/zero dt guard",
    #              "actor_rate is 0 when the actor has no previous-received entry".

  @property @boundary
  Scenario: mailbox_bar's fill ratio is confined to [0,1] for any len and capacity
    Given any mailbox length len and any capacity cap
    When mailbox_bar(len, cap) is computed
    Then the rendered bar has exactly 10 cells and the percentage is in [0, 100]
    And capacity 0 yields a 0% empty bar (no divide-by-zero)
    And len > cap still yields a full bar capped at 100% (ratio clamped to 1.0)
    # GEN: len, cap ∈ boundary-biased usize {0, 1, cap-1, cap, cap+1, usize::MAX}; include
    #      cap==0 with len ∈ {0, 1, MAX}, and len >> cap (over-full).
    # ORACLE: ratio = (len/cap).clamp(0.0,1.0), cap==0 ⇒ 0.0 (:1924-1928); filled = round(ratio*10)
    #         ∈ [0,10], pct = round(ratio*100) ∈ [0,100]; bar = filled '█' + (10-filled) '░'.
    # Generalizes: tui.feature "mailbox_bar renders a 10-cell bar with a zero-cap guard".

  @property @boundary
  Scenario: backpressure_style picks the threshold band of the true fill ratio for any len/cap
    Given any mailbox length len and any capacity cap
    When backpressure_style(len, cap) is computed
    Then it is red iff ratio >= 0.8, yellow iff 0.5 <= ratio < 0.8, else FG
    And capacity 0 forces ratio 0.0 (FG, never a divide-by-zero) for any len
    # GEN: len, cap ∈ boundary-biased usize {0, 1, cap-1, cap, cap+1, MAX}; pairs straddling the
    #      0.5 and 0.8 thresholds (e.g. len/cap == 0.49, 0.5, 0.79, 0.8); cap==0 with len>0.
    # ORACLE: ratio = if cap==0 {0.0} else {len/cap} (UNCLAMPED, :1490-1494); the three-band
    #         step function (:1495-1501). Note ratio here is NOT clamped, unlike mailbox_bar.
    # Generalizes: tui.feature "backpressure_style colors a mailbox by fill ratio".

  @property @boundary
  Scenario: centered_rect is always fully contained within the input area for any area and size
    Given any area Rect and any requested width w and height h
    When centered_rect(area, w, h) is computed
    Then the result lies entirely inside area (rx >= area.x, ry >= area.y,
         rx + rw <= area.x + area.width, ry + rh <= area.y + area.height)
    And the result size never exceeds the area (rw <= area.width, rh <= area.height)
    # GEN: area.{x,y,width,height} and w,h ∈ boundary-biased u16 {0, 1, u16::MAX}; include
    #      w,h larger than the area (clamp path), equal to the area, and a zero-sized area.
    # ORACLE: containment predicate — width/height are .min(area.*) first, so (area-size)/2 >= 0
    #         and size+offset <= area for every input (:1612-1619). No reference model; the
    #         invariant is the rectangle-containment relation itself.
    # Generalizes: tui.feature "centered_rect centers a clamped rect inside the area".

  @property @boundary
  Scenario: short_type_name never panics and is idempotent for any string
    Given any string s
    When short_type_name(s) is computed
    Then it never panics and returns a substring of s
    And applying short_type_name again to the result returns the same value (idempotent)
    And the empty string maps to the empty string
    # GEN: s ∈ {"", "::", "::::", "a::b::Foo<X>", "Vec<a::b::T<U>>", deep generics, unicode,
    #      no-delimiter "Foo", trailing "::"}; include strings whose '<'/"::"sit at boundaries.
    # ORACLE: idempotence — f(f(s)) == f(s); split('<').next ∘ rsplit("::").next has no '<' or
    #         "::" left to strip on a second pass (:1859-1862). Result is always a slice of s.
    # Generalizes: tui.feature "short_type_name strips the module path and generic args".

  @property @boundary
  Scenario: fmt_short never panics and yields a non-empty string for any Duration
    Given any Duration d
    When fmt_short(d) is computed
    Then it returns a non-empty string and never panics
    # GEN: d ∈ {Duration::ZERO, 1ns, 59.999s, 60s, large, Duration::MAX}.
    # ORACLE: totality — as_secs_f64()/as_secs() are total over Duration; the two format arms
    #         cover all d (:1504-1512). No panic, non-empty output.
    # Generalizes: tui.feature "fmt_short formats a duration compactly".

  @property @boundary
  Scenario: fmt_ago never panics and yields a non-empty string for any Duration
    Given any Duration d
    When fmt_ago(d) is computed
    Then it returns a non-empty string ending in " ago" and never panics
    # GEN: d ∈ {Duration::ZERO, 59s, 60s, 3599s, 3600s, Duration::MAX}.
    # ORACLE: totality — the three arms (<60s, <3600s, else) cover all d; arithmetic is on
    #         as_secs() with /, % only (no overflow), :1514-1523.
    # Generalizes: tui.feature "fmt_ago formats an elapsed-since string".

  @property @boundary
  Scenario: fmt_uptime is always an HH:MM:SS string for any Duration, with MM,SS in 0..=59
    Given any Duration d
    When fmt_uptime(d) is computed
    Then it never panics and the minute and second fields are each in [0, 59]
    And hours are not capped (a duration past a day renders hours > 23)
    # GEN: d ∈ {Duration::ZERO, 59s, 3661s, 86399s, 90061s, Duration::MAX}.
    # ORACLE: secs/3600, (secs%3600)/60, secs%60 — minute/second fields are mod 60 by
    #         construction (:1938-1946); hours uncapped. Total over Duration.
    # Generalizes: tui.feature "fmt_uptime is a zero-padded HH:MM:SS clock".

  # ---------------------------------------------------------------------------
  # @model — refinement against a reference cycle/SCC finder
  # ---------------------------------------------------------------------------

  @model @sequence
  Scenario: detect_deadlocks returns exactly the real cycles of any wait-for graph, each once, normalized
    Given any wait-for graph where each actor has at most one waiting_on edge (functional graph)
    When detect_deadlocks runs on a snapshot encoding that graph
    Then the returned cycles are exactly the cycles of the graph, each reported once
    And each cycle is rotated to begin at its lowest actor id
    And the cycle list is ordered by each cycle's first (lowest) id
    And a wait edge to a non-existent target ends a chain and contributes no cycle
    # GEN: random functional graphs over n actors (n ∈ {0, 1, 2, small, large}); each actor's
    #      waiting_on ∈ {None, Some(self), Some(other id), Some(dangling id)}; include zero cycles,
    #      a self-cycle, a 2-cycle, a k-cycle, multiple disjoint cycles, and chains feeding a cycle.
    # ORACLE: a reference cycle finder on a functional graph (follow each node's single successor;
    #         a cycle = the set of nodes on a closed walk; ignore nodes whose successor is absent).
    #         Compare the SUT's set-of-cycles to the reference's, after normalizing both to start
    #         at the min id and sorting by first id. Equivalent to Tarjan SCCs of size >= 1 that
    #         are reachable into themselves, but the functional structure makes the simple
    #         successor-chase reference sufficient (matches the SUT's algorithm domain, :1124-1159).
    # Generalizes: tui.feature "detect_deadlocks finds no cycle when no actor waits",
    #              "…non-cyclic wait chain", "…self-cycle", "…single two-actor cycle once",
    #              "…single multi-actor cycle once", "…normalizes each cycle to its lowest id",
    #              "…separates two independent cycles", "…tolerates a wait edge to a non-existent target".
