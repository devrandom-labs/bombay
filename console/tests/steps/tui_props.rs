//! Step definitions for `tui.properties.feature`'s `@property` laws.
//!
//! Each `@property` scenario states ∀-quantified invariants over one pure
//! `console::src::tui` helper. The generic `Given`/`When` lines are bound to
//! NO-OP steps (cucumber 0.23 + `fail_on_skipped` fails any scenario with an
//! unmatched line); the real proptest-backed assertion lives in each scenario's
//! discriminating `Then`/`And` lines. `proptest!` is a synchronous macro run
//! inline in the async step body — fine, it completes before the fn returns.
//!
//! Generators follow each scenario's `# GEN:` comment (boundary-biased via
//! `prop_oneof![Just(0), Just(1), Just(MAX), any::<_>()]` so the named edges are
//! hit); assertions encode each `# ORACLE:` predicate, plus the explicit
//! boundary values as direct (non-proptest) checks where practical.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use cucumber::{World, given, then, when};
use kameo_console::testing::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, Links, MailboxKind, MailboxStats,
    RefCounts, Snapshot, Totals, WaitEdge, WaitKind, actor_rate, backpressure_style, braille,
    centered_rect, color_rgb, detect_deadlocks, fmt_ago, fmt_short, fmt_uptime, mailbox_bar,
    short_type_name, spark_height, sparkline_line,
};
use proptest::prelude::*;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use std::time::SystemTime;

/// A unit world: proptest carries its own per-case state, so no per-scenario
/// fields are needed (each scenario gets a fresh `World::default()`).
#[derive(Debug, Default, World)]
pub struct TuiPropsWorld;

// ---------------------------------------------------------------------------
// Fixtures + strategies
// ---------------------------------------------------------------------------

/// FG ≈ the console foreground; the `color_rgb`/`backpressure_style` default.
const FG: (u8, u8, u8) = (205, 205, 212);

fn make_actor(id: u64, messages_received: u64) -> ActorSnapshot {
    ActorSnapshot {
        id: ActorId(id),
        name: format!("Actor{id}"),
        status: ActorStatus::Running,
        handling: None,
        waiting_on: None,
        strategy: None,
        spawned_at: SystemTime::UNIX_EPOCH,
        mailbox: MailboxStats {
            kind: MailboxKind::Unbounded,
            len: 0,
            capacity: None,
        },
        counters: ActorCounters {
            messages_received,
            ..ActorCounters::default()
        },
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

/// Boundary-biased u64: guarantees {0, 1, MAX} are hit alongside random values.
fn u64_edges() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0u64), Just(1u64), Just(u64::MAX), any::<u64>()]
}

/// Boundary-biased usize: {0, 1, MAX} plus random (capped to keep `repeat`
/// allocations in `mailbox_bar`/`sparkline_line` callers bounded — values here
/// only feed ratio math, not allocation).
fn usize_edges() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(0usize),
        Just(1usize),
        Just(usize::MAX),
        any::<usize>()
    ]
}

/// Boundary-biased u16: {0, 1, MAX} plus random (for Rect fields + sizes).
fn u16_edges() -> impl Strategy<Value = u16> {
    prop_oneof![Just(0u16), Just(1u16), Just(u16::MAX), any::<u16>()]
}

/// Boundary-biased u8 incl. the braille clamp edge (4 vs 5) and 9.
fn u8_edges() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(0u8),
        Just(1u8),
        Just(4u8),
        Just(5u8),
        Just(9u8),
        Just(u8::MAX),
        any::<u8>()
    ]
}

/// Durations covering the named edges plus randoms; `Duration::MAX`-class values
/// included so `as_secs_f64`/`as_secs` totality is exercised.
fn duration_edges() -> impl Strategy<Value = Duration> {
    prop_oneof![
        Just(Duration::ZERO),
        Just(Duration::from_nanos(1)),
        Just(Duration::from_secs(59)),
        Just(Duration::from_secs(60)),
        Just(Duration::from_secs(3599)),
        Just(Duration::from_secs(3600)),
        Just(Duration::from_secs(86_399)),
        Just(Duration::from_secs(90_061)),
        Just(Duration::MAX),
        (0u64..u64::MAX, 0u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
    ]
}

/// Type-name strings: the explicit GEN corpus plus random strings (incl. unicode
/// and embedded `<`/`::`).
fn type_name_edges() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        Just("::".to_string()),
        Just("::::".to_string()),
        Just("a::b::Foo<X>".to_string()),
        Just("Vec<a::b::T<U>>".to_string()),
        Just("a::b::c::d::Deep<E<F<G>>>".to_string()),
        Just("Foo".to_string()),
        Just("trailing::".to_string()),
        Just("名前::型<引数>".to_string()),
        ".*".prop_map(String::from),
        proptest::collection::vec(prop_oneof![Just("::"), Just("<"), Just(">"), Just("a"), Just("名")], 0..8)
            .prop_map(|parts| parts.concat()),
    ]
}

/// The full set of `Color` variants `color_rgb` must total over: every named
/// ANSI variant, `Rgb`, `Indexed`, and `Reset`.
fn color_edges() -> impl Strategy<Value = Color> {
    prop_oneof![
        Just(Color::Reset),
        Just(Color::Black),
        Just(Color::Red),
        Just(Color::Green),
        Just(Color::Yellow),
        Just(Color::Blue),
        Just(Color::Magenta),
        Just(Color::Cyan),
        Just(Color::Gray),
        Just(Color::DarkGray),
        Just(Color::LightRed),
        Just(Color::LightGreen),
        Just(Color::LightYellow),
        Just(Color::LightBlue),
        Just(Color::LightMagenta),
        Just(Color::LightCyan),
        Just(Color::White),
        Just(Color::Rgb(0, 0, 0)),
        Just(Color::Rgb(255, 255, 255)),
        (any::<u8>(), any::<u8>(), any::<u8>()).prop_map(|(r, g, b)| Color::Rgb(r, g, b)),
        any::<u8>().prop_map(Color::Indexed),
    ]
}

/// Maps a `backpressure_style`/state `Style` back to its threshold band by the
/// real colors it sets (Red ≥0.8, Yellow ≥0.5, else FG = Rgb(205,205,212)).
fn style_band(style: Style) -> &'static str {
    let red = Style::new().fg(Color::Red);
    let yellow = Style::new().fg(Color::Yellow);
    if style == red {
        "red"
    } else if style == yellow {
        "yellow"
    } else {
        // backpressure_style's else arm is `Style::new().fg(Rgb(205,205,212))`.
        "fg"
    }
}

// ---------------------------------------------------------------------------
// Generic NO-OP Given/When lines (every line needs a matching step).
// ---------------------------------------------------------------------------

#[given(regex = r"^any value v and any max m$")]
fn g_spark(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^spark_height\(v, m\) is computed$")]
fn w_spark(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any actor with messages_received now, any previous count prev, and any dt$")]
fn g_rate(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^actor_rate\(actor, prev_received, dt\) is computed$")]
fn w_rate(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any mailbox length len and any capacity cap$")]
fn g_mailbox(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^mailbox_bar\(len, cap\) is computed$")]
fn w_mailbox(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^backpressure_style\(len, cap\) is computed$")]
fn w_backpressure(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any area Rect and any requested width w and height h$")]
fn g_centered(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^centered_rect\(area, w, h\) is computed$")]
fn w_centered(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any string s$")]
fn g_short(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^short_type_name\(s\) is computed$")]
fn w_short(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any Duration d$")]
fn g_duration(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^fmt_short\(d\) is computed$")]
fn w_fmt_short(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^fmt_ago\(d\) is computed$")]
fn w_fmt_ago(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^fmt_uptime\(d\) is computed$")]
fn w_fmt_uptime(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any left height l and any right height r$")]
fn g_braille(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^braille\(l, r\) is computed$")]
fn w_braille(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any Color c$")]
fn g_color(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^color_rgb\(c\) is computed$")]
fn w_color(_w: &mut TuiPropsWorld) {}

#[given(regex = r"^any sample slice, any max, and any width w$")]
fn g_sparkline(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^sparkline_line\(samples, max, w\) is computed$")]
fn w_sparkline(_w: &mut TuiPropsWorld) {}

// ---------------------------------------------------------------------------
// Law 1 — spark_height ∈ [1,4], m==0 ⇒ 1 (:1571-1574)
// ---------------------------------------------------------------------------

#[then(regex = r"^the result is an integer in the closed range \[1, 4\]$")]
fn spark_height_in_range(_w: &mut TuiPropsWorld) {
    // ORACLE: 1 <= spark_height(v, m) <= 4 for all v, m.
    proptest!(|(v in u64_edges(), m in u64_edges())| {
        let h = spark_height(v, m);
        prop_assert!((1..=4).contains(&h), "spark_height({v},{m})={h} out of [1,4]");
    });
    // Explicit boundary corner: over-peak v > m must still clamp to 4.
    assert_eq!(spark_height(u64::MAX, 1), 4);
}

#[then(regex = r"^when m == 0 the result is exactly 1 \(the baseline, no divide-by-zero\)$")]
fn spark_height_zero_max(_w: &mut TuiPropsWorld) {
    // ORACLE: the m==0 short-circuit returns 1 for any v (:1571-1573).
    proptest!(|(v in u64_edges())| {
        prop_assert_eq!(spark_height(v, 0), 1);
    });
    // Explicit GEN edges: m==0 with v ∈ {0,1,MAX}.
    assert_eq!(spark_height(0, 0), 1);
    assert_eq!(spark_height(1, 0), 1);
    assert_eq!(spark_height(u64::MAX, 0), 1);
}

// ---------------------------------------------------------------------------
// Law 2 — actor_rate >= 0, total, guard returns 0 (:1250-1255)
// ---------------------------------------------------------------------------

#[then(regex = r"^it returns a u64 \(>= 0\) and never panics$")]
fn actor_rate_total(_w: &mut TuiPropsWorld) {
    // ORACLE: total over (now, prev, dt, prev-present?). u64 is >= 0 by type; the
    // assertion is that no input panics and the guard outcome holds.
    proptest!(|(now in u64_edges(), prev in u64_edges(), dt_secs in 0u64..u64::MAX,
                dt_nanos in 0u32..1_000_000_000, has_dt in any::<bool>(),
                dt_none in any::<bool>(), present in any::<bool>())| {
        let actor = make_actor(1, now);
        let mut map = HashMap::new();
        if present {
            map.insert(ActorId(1), prev);
        }
        let dt = if dt_none {
            None
        } else if has_dt {
            Some(Duration::new(dt_secs, dt_nanos))
        } else {
            Some(Duration::ZERO)
        };
        let rate = actor_rate(&actor, &map, dt);
        // When the guard fails (no dt / zero dt / absent prev), rate is exactly 0.
        let guard_open = matches!(dt, Some(d) if d.as_secs_f64() > 0.0) && present;
        if !guard_open {
            prop_assert_eq!(rate, 0, "guard should force 0 for dt={:?} present={}", dt, present);
        }
        let _ = rate; // u64 >= 0 trivially; assertion is no-panic + guard.
    });
}

#[then(regex = r"^it returns 0 when dt is None, when dt is zero, or when prev has no entry for the actor$")]
fn actor_rate_zero_cases(_w: &mut TuiPropsWorld) {
    // ORACLE: each of the three guard-failure cases yields 0 (:1250-1255).
    let actor = make_actor(1, 100);
    let mut present = HashMap::new();
    present.insert(ActorId(1), 10u64);
    let absent: HashMap<ActorId, u64> = HashMap::new();
    // dt None
    assert_eq!(actor_rate(&actor, &present, None), 0);
    // dt zero
    assert_eq!(actor_rate(&actor, &present, Some(Duration::ZERO)), 0);
    // prev absent
    assert_eq!(actor_rate(&actor, &absent, Some(Duration::from_secs(1))), 0);
    // Property: any of these conditions ⇒ 0.
    proptest!(|(now in u64_edges(), prev in u64_edges())| {
        let actor = make_actor(1, now);
        let mut map = HashMap::new();
        map.insert(ActorId(1), prev);
        prop_assert_eq!(actor_rate(&actor, &map, None), 0);
        prop_assert_eq!(actor_rate(&actor, &map, Some(Duration::ZERO)), 0);
        let empty: HashMap<ActorId, u64> = HashMap::new();
        prop_assert_eq!(actor_rate(&actor, &empty, Some(Duration::from_secs(1))), 0);
    });
}

#[then(regex = r"^a decreased counter \(now < prev\) saturates the delta to 0 rather than underflowing$")]
fn actor_rate_saturates(_w: &mut TuiPropsWorld) {
    // ORACLE: delta via saturating_sub (counter reset ⇒ 0), :1252.
    proptest!(|(now in u64_edges(), prev in u64_edges())| {
        let actor = make_actor(1, now);
        let mut map = HashMap::new();
        map.insert(ActorId(1), prev);
        let rate = actor_rate(&actor, &map, Some(Duration::from_secs(1)));
        if now < prev {
            prop_assert_eq!(rate, 0, "now={} < prev={} must saturate to 0", now, prev);
        }
    });
    // Explicit reset edge: 0 received, prev MAX, 1s dt ⇒ 0 (no underflow panic).
    let actor = make_actor(1, 0);
    let mut map = HashMap::new();
    map.insert(ActorId(1), u64::MAX);
    assert_eq!(actor_rate(&actor, &map, Some(Duration::from_secs(1))), 0);
}

// ---------------------------------------------------------------------------
// Law 3 — mailbox_bar: 10 cells, pct ∈ [0,100], cap0⇒0%, len>cap⇒100% (:1924-1933)
// ---------------------------------------------------------------------------

/// Parses the `pct%` from a `mailbox_bar` string and counts the bar cells.
fn parse_bar(s: &str) -> (usize, u64) {
    // Format: "{len} / {capacity}  {bar}  {pct}%"; bar is run of █/░.
    let cells = s.chars().filter(|&c| c == '█' || c == '░').count();
    let pct: u64 = s
        .rsplit("  ")
        .next()
        .and_then(|t| t.strip_suffix('%'))
        .and_then(|t| t.trim().parse().ok())
        .expect("mailbox_bar must end in a `N%` token");
    (cells, pct)
}

#[then(regex = r"^the rendered bar has exactly 10 cells and the percentage is in \[0, 100\]$")]
fn mailbox_bar_cells_pct(_w: &mut TuiPropsWorld) {
    // ORACLE: 10-cell bar; pct = round(ratio*100), ratio clamped ∈ [0,1] ⇒ pct ∈ [0,100].
    proptest!(|(len in usize_edges(), cap in usize_edges())| {
        let (bar, _style) = mailbox_bar(len, cap);
        let (cells, pct) = parse_bar(&bar);
        prop_assert_eq!(cells, 10, "bar `{}` had {} cells", bar, cells);
        prop_assert!(pct <= 100, "pct {} > 100 for len={} cap={}", pct, len, cap);
    });
}

#[then(regex = r"^capacity 0 yields a 0% empty bar \(no divide-by-zero\)$")]
fn mailbox_bar_zero_cap(_w: &mut TuiPropsWorld) {
    // ORACLE: cap==0 ⇒ ratio 0.0 ⇒ pct 0, all-idle bar (:1924-1925).
    proptest!(|(len in usize_edges())| {
        let (bar, _) = mailbox_bar(len, 0);
        let (cells, pct) = parse_bar(&bar);
        prop_assert_eq!(cells, 10);
        prop_assert_eq!(pct, 0, "cap 0 must be 0%, got {} (len={})", pct, len);
        prop_assert_eq!(bar.matches('█').count(), 0, "cap 0 must have no filled cells");
    });
    // Explicit GEN edges: cap 0 with len ∈ {0,1,MAX}.
    for len in [0usize, 1, usize::MAX] {
        let (bar, _) = mailbox_bar(len, 0);
        assert_eq!(parse_bar(&bar).1, 0);
    }
}

#[then(regex = r"^len > cap still yields a full bar capped at 100% \(ratio clamped to 1\.0\)$")]
fn mailbox_bar_overfull(_w: &mut TuiPropsWorld) {
    // ORACLE: ratio clamped to 1.0 ⇒ len>cap (cap>0) ⇒ 100%, 10 filled cells.
    proptest!(|(cap in 1usize..=usize::MAX)| {
        // len strictly greater than cap (saturating to MAX at the top).
        let len = cap.saturating_add(1);
        let (bar, _) = mailbox_bar(len, cap);
        let (cells, pct) = parse_bar(&bar);
        prop_assert_eq!(cells, 10);
        prop_assert_eq!(pct, 100, "len {} > cap {} must clamp to 100%", len, cap);
        prop_assert_eq!(bar.matches('█').count(), 10, "over-full bar must be all filled");
    });
    // Explicit: len >> cap.
    assert_eq!(parse_bar(&mailbox_bar(usize::MAX, 1).0).1, 100);
}

// ---------------------------------------------------------------------------
// Law 4 — backpressure_style threshold bands; UNCLAMPED ratio (:1490-1501)
// ---------------------------------------------------------------------------

#[then(regex = r"^it is red iff ratio >= 0\.8, yellow iff 0\.5 <= ratio < 0\.8, else FG$")]
fn backpressure_bands(_w: &mut TuiPropsWorld) {
    // ORACLE: ratio = if cap==0 {0.0} else {len/cap} (UNCLAMPED); three-band step.
    proptest!(|(len in usize_edges(), cap in usize_edges())| {
        let ratio = if cap == 0 { 0.0 } else { len as f64 / cap as f64 };
        let expected = if ratio >= 0.8 { "red" } else if ratio >= 0.5 { "yellow" } else { "fg" };
        let got = style_band(backpressure_style(len, cap));
        prop_assert_eq!(got, expected, "len={} cap={} ratio={} band mismatch", len, cap, ratio);
    });
    // Explicit threshold straddles (cap=100): 0.49→fg, 0.5→yellow, 0.79→yellow, 0.8→red.
    assert_eq!(style_band(backpressure_style(49, 100)), "fg");
    assert_eq!(style_band(backpressure_style(50, 100)), "yellow");
    assert_eq!(style_band(backpressure_style(79, 100)), "yellow");
    assert_eq!(style_band(backpressure_style(80, 100)), "red");
}

#[then(regex = r"^capacity 0 forces ratio 0\.0 \(FG, never a divide-by-zero\) for any len$")]
fn backpressure_zero_cap(_w: &mut TuiPropsWorld) {
    // ORACLE: cap==0 ⇒ ratio 0.0 ⇒ FG (:1490-1492).
    proptest!(|(len in usize_edges())| {
        prop_assert_eq!(style_band(backpressure_style(len, 0)), "fg",
            "cap 0 must be FG for len={}", len);
    });
    assert_eq!(style_band(backpressure_style(usize::MAX, 0)), "fg");
}

// ---------------------------------------------------------------------------
// Law 5 — centered_rect containment (:1612-1619)
// ---------------------------------------------------------------------------

#[then(regex = r"^the result lies entirely inside area .*$")]
fn centered_contained(_w: &mut TuiPropsWorld) {
    // ORACLE: width/height .min(area.*) first ⇒ result fully inside area.
    proptest!(|(x in u16_edges(), y in u16_edges(), aw in u16_edges(), ah in u16_edges(),
                w in u16_edges(), h in u16_edges())| {
        // `Rect::new` clamps width/height so `x+width`/`y+height` never overflow u16
        // (ratatui-core rect.rs:179). Build through it so the input is a well-formed
        // Rect — `centered_rect`'s contract is over valid areas, not raw struct
        // literals that violate the upstream invariant (CLAUDE.md rule 3).
        let area = Rect::new(x, y, aw, ah);
        let r = centered_rect(area, w, h);
        prop_assert!(r.x >= area.x);
        prop_assert!(r.y >= area.y);
        prop_assert!(r.x + r.width <= area.x + area.width,
            "x+w {} > area x+w {}", r.x + r.width, area.x + area.width);
        prop_assert!(r.y + r.height <= area.y + area.height);
    });
}

#[then(regex = r"^the result size never exceeds the area \(rw <= area\.width, rh <= area\.height\)$")]
fn centered_size(_w: &mut TuiPropsWorld) {
    // ORACLE: size is .min(area.*), so rw<=aw and rh<=ah.
    proptest!(|(x in u16_edges(), y in u16_edges(), aw in u16_edges(), ah in u16_edges(),
                w in u16_edges(), h in u16_edges())| {
        let area = Rect::new(x, y, aw, ah);
        let r = centered_rect(area, w, h);
        prop_assert!(r.width <= area.width);
        prop_assert!(r.height <= area.height);
    });
    // Explicit edges: oversized request clamps to the area; equal stays equal; zero area ⇒ zero.
    let area = Rect { x: 5, y: 6, width: 20, height: 10 };
    assert_eq!(centered_rect(area, u16::MAX, u16::MAX), Rect { x: 5, y: 6, width: 20, height: 10 });
    assert_eq!(centered_rect(area, 20, 10), Rect { x: 5, y: 6, width: 20, height: 10 });
    let zero = Rect { x: 1, y: 1, width: 0, height: 0 };
    assert_eq!(centered_rect(zero, 8, 8), Rect { x: 1, y: 1, width: 0, height: 0 });
}

// ---------------------------------------------------------------------------
// Law 6 — short_type_name: substring, idempotent, ""→"" (:1859-1862)
// ---------------------------------------------------------------------------

#[then(regex = r"^it never panics and returns a substring of s$")]
fn short_substring(_w: &mut TuiPropsWorld) {
    // ORACLE: result is always a slice of s.
    proptest!(|(s in type_name_edges())| {
        let out = short_type_name(&s);
        prop_assert!(s.contains(out), "`{}` is not a substring of `{}`", out, s);
    });
}

#[then(regex = r"^applying short_type_name again to the result returns the same value \(idempotent\)$")]
fn short_idempotent(_w: &mut TuiPropsWorld) {
    // ORACLE: f(f(s)) == f(s) — no '<' or "::" left after one pass.
    proptest!(|(s in type_name_edges())| {
        let once = short_type_name(&s);
        let twice = short_type_name(once);
        prop_assert_eq!(once, twice, "not idempotent on `{}`", s);
    });
    // Explicit corpus checks.
    assert_eq!(short_type_name("a::b::Foo<X>"), "Foo");
    assert_eq!(short_type_name("Vec<a::b::T<U>>"), "Vec");
    assert_eq!(short_type_name("Foo"), "Foo");
}

#[then(regex = r"^the empty string maps to the empty string$")]
fn short_empty(_w: &mut TuiPropsWorld) {
    // ORACLE: "" → "".
    assert_eq!(short_type_name(""), "");
}

// ---------------------------------------------------------------------------
// Law 7 — fmt_short non-empty, total (:1504-1512)
// ---------------------------------------------------------------------------

#[then(regex = r"^it returns a non-empty string and never panics$")]
fn fmt_short_nonempty(_w: &mut TuiPropsWorld) {
    // ORACLE: totality; both arms produce non-empty output.
    proptest!(|(d in duration_edges())| {
        prop_assert!(!fmt_short(d).is_empty());
    });
    for d in [Duration::ZERO, Duration::from_nanos(1), Duration::from_secs(60), Duration::MAX] {
        assert!(!fmt_short(d).is_empty());
    }
}

// ---------------------------------------------------------------------------
// Law 8 — fmt_ago non-empty, ends " ago", total (:1514-1523)
// ---------------------------------------------------------------------------

#[then(regex = "^it returns a non-empty string ending in \" ago\" and never panics$")]
fn fmt_ago_suffix(_w: &mut TuiPropsWorld) {
    // ORACLE: all three arms append " ago".
    proptest!(|(d in duration_edges())| {
        let s = fmt_ago(d);
        prop_assert!(!s.is_empty());
        prop_assert!(s.ends_with(" ago"), "`{}` does not end in ` ago`", s);
    });
    for d in [Duration::ZERO, Duration::from_secs(60), Duration::from_secs(3600), Duration::MAX] {
        assert!(fmt_ago(d).ends_with(" ago"));
    }
}

// ---------------------------------------------------------------------------
// Law 9 — fmt_uptime HH:MM:SS, MM/SS ∈ [0,59], hours uncapped (:1938-1946)
// ---------------------------------------------------------------------------

#[then(regex = r"^it never panics and the minute and second fields are each in \[0, 59\]$")]
fn uptime_fields(_w: &mut TuiPropsWorld) {
    // ORACLE: (secs%3600)/60 and secs%60 are mod-60 by construction.
    proptest!(|(d in duration_edges())| {
        let s = fmt_uptime(d);
        let parts: Vec<&str> = s.split(':').collect();
        prop_assert_eq!(parts.len(), 3, "`{}` is not HH:MM:SS", s);
        let mm: u64 = parts[1].parse().expect("MM numeric");
        let ss: u64 = parts[2].parse().expect("SS numeric");
        prop_assert!(mm <= 59, "MM {} > 59", mm);
        prop_assert!(ss <= 59, "SS {} > 59", ss);
    });
}

#[then(regex = r"^hours are not capped \(a duration past a day renders hours > 23\)$")]
fn uptime_hours_uncapped(_w: &mut TuiPropsWorld) {
    // ORACLE: hours = secs/3600, no cap. 90061s = 25h 01m 01s.
    assert_eq!(fmt_uptime(Duration::from_secs(90_061)), "25:01:01");
    assert_eq!(fmt_uptime(Duration::from_secs(86_399)), "23:59:59");
    // Property: hours field == secs/3600 exactly (uncapped).
    proptest!(|(d in duration_edges())| {
        let s = fmt_uptime(d);
        let hours: u64 = s.split(':').next().unwrap().parse().expect("HH numeric");
        prop_assert_eq!(hours, d.as_secs() / 3600);
    });
}

// ---------------------------------------------------------------------------
// Law 10 — braille: single cell ∈ U+2800..=U+28FF, clamp to 4 (:1579-1585)
// ---------------------------------------------------------------------------

#[then(regex = r"^it returns a single char in the closed range U\+2800\.\.=U\+28FF and never panics$")]
fn braille_in_range(_w: &mut TuiPropsWorld) {
    // ORACLE: bits = LEFT|RIGHT <= 0xFF ⇒ 0x2800+bits ∈ braille block.
    proptest!(|(l in u8_edges(), r in u8_edges())| {
        let c = braille(l, r);
        let cp = c as u32;
        prop_assert!((0x2800..=0x28FF).contains(&cp), "braille({l},{r})=U+{cp:04X} out of block");
    });
    // Explicit edges.
    assert!((0x2800..=0x28FF).contains(&(braille(0, 0) as u32)));
    assert!((0x2800..=0x28FF).contains(&(braille(u8::MAX, u8::MAX) as u32)));
}

#[then(regex = r"^heights above 4 produce the same glyph as 4 \(columns clamped with \.min\(4\)\)$")]
fn braille_clamp(_w: &mut TuiPropsWorld) {
    // ORACLE: braille(l,r) == braille(l.min(4), r.min(4)).
    proptest!(|(l in u8_edges(), r in u8_edges())| {
        prop_assert_eq!(braille(l, r), braille(l.min(4), r.min(4)));
    });
    // Explicit clamp-edge straddles.
    assert_eq!(braille(5, 5), braille(4, 4));
    assert_eq!(braille(9, 0), braille(4, 0));
    assert_eq!(braille(u8::MAX, u8::MAX), braille(4, 4));
}

// ---------------------------------------------------------------------------
// Law 11 — color_rgb total; unmapped ⇒ FG (:1597-1607)
// ---------------------------------------------------------------------------

#[then(regex = r"^it returns an \(u8,u8,u8\) triple and never panics$")]
fn color_rgb_total(_w: &mut TuiPropsWorld) {
    // ORACLE: exhaustive match — never panics for any Color.
    proptest!(|(c in color_edges())| {
        let _ = color_rgb(c); // exhaustive; the assertion is no-panic over all variants.
    });
}

#[then(regex = r"^any color not explicitly listed \(Reset/White/Gray/Blue/…\) maps to FG \(205,205,212\)$")]
fn color_rgb_default(_w: &mut TuiPropsWorld) {
    // ORACLE: the `_ => (205,205,212)` arm catches Reset/White/Gray/Blue/Magenta/Indexed.
    for c in [Color::Reset, Color::White, Color::Gray, Color::Blue, Color::Magenta,
              Color::LightBlue, Color::LightMagenta, Color::Indexed(7), Color::Indexed(0)] {
        assert_eq!(color_rgb(c), FG, "{c:?} should map to FG");
    }
    // Rgb is returned verbatim.
    proptest!(|(r in any::<u8>(), g in any::<u8>(), b in any::<u8>())| {
        prop_assert_eq!(color_rgb(Color::Rgb(r, g, b)), (r, g, b));
    });
    // Any Indexed(n) is unmapped ⇒ FG.
    proptest!(|(n in any::<u8>())| {
        prop_assert_eq!(color_rgb(Color::Indexed(n)), FG);
    });
}

// ---------------------------------------------------------------------------
// Law 12 — sparkline_line: exactly width cells; last w*2 samples; left pad (:1547-1565)
// ---------------------------------------------------------------------------

/// Counts braille spans (cells) in a `sparkline_line` `Line`.
fn line_cells(line: &ratatui::text::Line<'_>) -> usize {
    line.spans.len()
}

#[then(regex = r"^the returned Line has exactly w braille cells and never panics$")]
fn sparkline_cell_count(_w: &mut TuiPropsWorld) {
    // ORACLE: data has length cols=w*2 for every input ⇒ chunks(2) ⇒ exactly w cells.
    proptest!(|(width in 0usize..=64, max in u64_edges(),
                samples in proptest::collection::vec(u64_edges(), 0..200))| {
        let line = sparkline_line(&samples, max, width);
        prop_assert_eq!(line_cells(&line), width,
            "width={} samples={} cells={}", width, samples.len(), line_cells(&line));
    });
    // Explicit GEN edges: width {0,1,9}, samples len {0,1,w*2-1,w*2,w*2+1}.
    assert_eq!(line_cells(&sparkline_line(&[], 0, 0)), 0);
    assert_eq!(line_cells(&sparkline_line(&[], 1, 1)), 1);
    let w = 9usize;
    for n in [0usize, 1, w * 2 - 1, w * 2, w * 2 + 1, 200] {
        let samples: Vec<u64> = (0..n).map(|i| i as u64).collect();
        assert_eq!(line_cells(&sparkline_line(&samples, u64::MAX, w)), w, "n={n}");
    }
}

#[then(regex = r"^only the most recent w\*2 samples influence the cells \(older samples scroll off\)$")]
fn sparkline_recent_window(_w: &mut TuiPropsWorld) {
    // ORACLE: data = samples[len.saturating_sub(cols)..]; prepending older samples
    // beyond the last cols must not change the rendered Line.
    proptest!(|(width in 1usize..=32, max in prop_oneof![Just(1u64), Just(u64::MAX)],
                old in proptest::collection::vec(u64_edges(), 0..40))| {
        // The window is exactly `cols = width*2`. To isolate "only the most recent
        // cols influence", `recent` must FILL the window — otherwise a shorter
        // series leaves room for the `old` prefix to enter the window (then it
        // legitimately influences the cells). Generate a full-window `recent`, then
        // prepend arbitrary older samples; both share the same last `cols`.
        let cols = width * 2;
        let recent: Vec<u64> = (0..cols).map(|i| i as u64).collect();
        let mut longer = old.clone();
        longer.extend_from_slice(&recent);
        let a = sparkline_line(&recent, max, width);
        let b = sparkline_line(&longer, max, width);
        // The newest `cols` samples are identical, so the cells must match.
        prop_assert_eq!(line_cells(&a), line_cells(&b));
        let aa: Vec<String> = a.spans.iter().map(|s| s.content.to_string()).collect();
        let bb: Vec<String> = b.spans.iter().map(|s| s.content.to_string()).collect();
        prop_assert_eq!(aa, bb, "older samples beyond w*2 leaked into the glyphs");
    });
}

#[then(regex = r"^when samples are fewer than w\*2 the line is left-padded with idle baseline cells$")]
fn sparkline_left_pad(_w: &mut TuiPropsWorld) {
    // ORACLE: cols.saturating_sub(len) zero-cells are prepended ⇒ leading idle cells.
    // An idle cell renders the braille baseline (spark_height(0,max)=1 → both
    // columns one dot), styled SPARK_IDLE; its glyph == braille(1,1) when max>0.
    let baseline = braille(1, 1).to_string();
    proptest!(|(width in 2usize..=32, max in prop_oneof![Just(1u64), Just(u64::MAX)])| {
        // One non-zero sample, far fewer than width*2 ⇒ many leading pad cells.
        let samples = vec![max.max(1)];
        let line = sparkline_line(&samples, max, width);
        prop_assert_eq!(line_cells(&line), width);
        // The first cell is from two padded zeros ⇒ idle baseline glyph.
        let first = line.spans.first().unwrap().content.to_string();
        prop_assert_eq!(first, baseline.clone(),
            "left pad cell `{}` != idle baseline `{}`", line.spans.first().unwrap().content, baseline);
    });
    // Explicit: empty samples ⇒ all-baseline line of width 9.
    let line = sparkline_line(&[], 1, 9);
    assert_eq!(line_cells(&line), 9);
    for span in &line.spans {
        assert_eq!(span.content.to_string(), braille(1, 1).to_string());
    }
}

// ---------------------------------------------------------------------------
// @model — detect_deadlocks ≡ reference cycle finder (tui.rs:1124-1171)
// ---------------------------------------------------------------------------

/// Reference cycle finder for a functional graph (≤1 successor/node). Mirrors the SUT's
/// domain (tui.rs:1124-1159): follow each node's single successor; a revisited node on the
/// walk forms a cycle. Normalize each cycle to start at its min id; sort cycles by first id.
/// INDEPENDENT reimplementation — does NOT call `detect_deadlocks`.
fn cycles_oracle(edges: &HashMap<u64, u64>) -> Vec<Vec<u64>> {
    let mut in_cycle: HashSet<u64> = HashSet::new();
    let mut cycles: Vec<Vec<u64>> = Vec::new();
    for &start in edges.keys() {
        if in_cycle.contains(&start) {
            continue;
        }
        let mut path = Vec::new();
        let mut pos: HashMap<u64, usize> = HashMap::new();
        let mut cur = start;
        loop {
            if let Some(&i) = pos.get(&cur) {
                let cyc = path[i..].to_vec();
                in_cycle.extend(cyc.iter().copied());
                cycles.push(cyc);
                break;
            }
            if in_cycle.contains(&cur) {
                break;
            }
            match edges.get(&cur) {
                Some(&n) => {
                    pos.insert(cur, path.len());
                    path.push(cur);
                    cur = n;
                }
                None => break,
            }
        }
    }
    for c in &mut cycles {
        if let Some(p) = (0..c.len()).min_by_key(|&i| c[i]) {
            c.rotate_left(p);
        }
    }
    cycles.sort_by_key(|c| c.first().copied());
    cycles
}

/// Minimal deadlock fixtures (the file's other `make_actor` is 2-arg for rate laws; the
/// deadlock graph only needs id + an optional `waiting_on` edge). Mirrors `tui.rs` steps.
fn make_actor_dl(id: u64) -> ActorSnapshot {
    ActorSnapshot {
        id: ActorId(id),
        name: format!("Actor{id}"),
        status: ActorStatus::Running,
        handling: None,
        waiting_on: None,
        strategy: None,
        spawned_at: std::time::SystemTime::UNIX_EPOCH,
        mailbox: MailboxStats {
            kind: MailboxKind::Unbounded,
            len: 0,
            capacity: None,
        },
        counters: ActorCounters::default(),
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

fn waiting(mut a: ActorSnapshot, target: u64) -> ActorSnapshot {
    a.waiting_on = Some(WaitEdge {
        target: ActorId(target),
        kind: WaitKind::Ask,
        elapsed: Duration::ZERO,
    });
    a
}

fn make_snapshot_dl(actors: Vec<ActorSnapshot>) -> Snapshot {
    Snapshot {
        seq: 0,
        captured_at: std::time::SystemTime::UNIX_EPOCH,
        uptime: Duration::ZERO,
        actors,
        totals: Totals::default(),
    }
}

#[given(
    regex = r"^any wait-for graph where each actor has at most one waiting_on edge \(functional graph\)$"
)]
fn g_model_graph(_w: &mut TuiPropsWorld) {}

#[when(regex = r"^detect_deadlocks runs on a snapshot encoding that graph$")]
fn w_model_run(_w: &mut TuiPropsWorld) {}

#[then(regex = r"^the returned cycles are exactly the cycles of the graph, each reported once$")]
fn model_cycles_exact(_w: &mut TuiPropsWorld) {
    // ORACLE: an INDEPENDENT successor-chase cycle finder (`cycles_oracle`); the SUT
    // (`detect_deadlocks`) must agree for every valid functional graph. The And lines
    // (rotation to min id, ordering by first id, dangling-target ends a chain) are
    // SUBSUMED by this set-equality — the oracle already encodes all three.
    proptest!(|(edges in proptest::collection::hash_map(
            0u64..8,
            prop_oneof![Just(None), (0u64..8).prop_map(Some), (8u64..12).prop_map(Some)],
            0..8))| {
        let actors: Vec<_> = edges.keys().map(|&id| match edges[&id] {
            Some(t) => waiting(make_actor_dl(id), t),
            None => make_actor_dl(id),
        }).collect();
        let sut: Vec<Vec<u64>> = detect_deadlocks(&make_snapshot_dl(actors))
            .into_iter().map(|c| c.into_iter().map(|x| x.0).collect()).collect();
        // The oracle uses only edges whose target is a REAL node (a dangling target
        // ends a chain → contributes no cycle, matching the SUT's `next.get` miss).
        let real: HashMap<u64, u64> = edges.iter()
            .filter_map(|(&k, &v)| v.filter(|t| edges.contains_key(t)).map(|t| (k, t)))
            .collect();
        prop_assert_eq!(sut, cycles_oracle(&real));
    });
}

#[then(regex = r"^each cycle is rotated to begin at its lowest actor id$")]
fn model_rotated(_w: &mut TuiPropsWorld) {
    // SUBSUMED by `model_cycles_exact` (the oracle normalizes rotation).
}

#[then(regex = r"^the cycle list is ordered by each cycle's first \(lowest\) id$")]
fn model_ordered(_w: &mut TuiPropsWorld) {
    // SUBSUMED by `model_cycles_exact` (the oracle sorts by first id).
}

#[then(regex = r"^a wait edge to a non-existent target ends a chain and contributes no cycle$")]
fn model_dangling(_w: &mut TuiPropsWorld) {
    // SUBSUMED by `model_cycles_exact` (the oracle's `real` map drops dangling edges).
}
