use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use cucumber::{World, given, then, when};
use kameo_console::testing::{
    ActorCounters, ActorId, ActorSnapshot, ActorStatus, HandlerActivity, Links, MailboxKind,
    MailboxStats, RefCounts, Snapshot, SortCol, Totals, WaitEdge, WaitKind,
};
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::Line;

#[derive(Debug, Default, World)]
pub struct TuiWorld {
    last_string: String,
    last_u64: u64,
    last_char: char,
    last_rgb: (u8, u8, u8),
    color: Option<Color>,
    area: Rect,
    last_rect: Rect,
    last_style: Style,
    mb_len: usize,
    mb_cap: usize,
    // rate_context / actor_rate / severity / compare / sort_actors fields
    prev_received: HashMap<ActorId, u64>,
    dt: Option<Duration>,
    prev_snapshot: Option<Snapshot>,
    current_snapshot: Option<Snapshot>,
    actor: Option<ActorSnapshot>,
    two: Option<(ActorSnapshot, ActorSnapshot)>,
    ordered_first_id: u64,
    // sparkline_line scenarios
    last_line: Option<Line<'static>>,
    sparkline_samples: Vec<u64>,
    // detect_deadlocks scenarios
    cycles: Vec<Vec<ActorId>>,
}

// ---------------------------------------------------------------------------
// Fixture constructors
// ---------------------------------------------------------------------------

fn make_actor(id: u64) -> ActorSnapshot {
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
        counters: ActorCounters::default(),
        message_types: Vec::new(),
        refs: RefCounts { strong: 1, weak: 0 },
        links: Links::default(),
        supervision: None,
    }
}

fn make_snapshot(actors: Vec<ActorSnapshot>) -> Snapshot {
    Snapshot {
        seq: 0,
        captured_at: SystemTime::UNIX_EPOCH,
        uptime: Duration::ZERO,
        actors,
        totals: Totals::default(),
    }
}

fn make_snapshot_at(actors: Vec<ActorSnapshot>, offset_secs: u64) -> Snapshot {
    Snapshot {
        seq: 0,
        captured_at: SystemTime::UNIX_EPOCH + Duration::from_secs(offset_secs),
        uptime: Duration::ZERO,
        actors,
        totals: Totals::default(),
    }
}

fn parse_dt(s: &str) -> Option<Duration> {
    if s == "None" {
        return None;
    }
    let secs: u64 = s
        .strip_suffix('s')
        .expect("dt token must end in 's'")
        .parse()
        .expect("dt secs");
    Some(Duration::from_secs(secs))
}

fn parse_status(s: &str) -> (ActorStatus, Option<HandlerActivity>) {
    match s {
        "Stopped" => (
            ActorStatus::Stopped {
                at: SystemTime::UNIX_EPOCH,
                reason: String::new(),
            },
            None,
        ),
        "Restarting" => (ActorStatus::Restarting, None),
        "Running (handling >= 5s)" => (
            ActorStatus::Running,
            Some(HandlerActivity {
                message: "Msg".to_owned(),
                elapsed: Duration::from_secs(5),
            }),
        ),
        "Stopping" => (ActorStatus::Stopping, None),
        "Starting" => (ActorStatus::Starting, None),
        "Running (handling < 5s)" => (
            ActorStatus::Running,
            Some(HandlerActivity {
                message: "Msg".to_owned(),
                elapsed: Duration::from_secs(1),
            }),
        ),
        "Running (not handling)" => (ActorStatus::Running, None),
        other => panic!("unknown status string: {other}"),
    }
}

// ---------------------------------------------------------------------------
// fmt_short
// ---------------------------------------------------------------------------

#[when(regex = r"^fmt_short is called with (\d+) milliseconds$")]
async fn when_fmt_short(world: &mut TuiWorld, millis: u64) {
    world.last_string = kameo_console::testing::fmt_short(Duration::from_millis(millis));
}

// ---------------------------------------------------------------------------
// fmt_ago
// ---------------------------------------------------------------------------

#[when(regex = r"^fmt_ago is called with (\d+) seconds$")]
async fn when_fmt_ago(world: &mut TuiWorld, secs: u64) {
    world.last_string = kameo_console::testing::fmt_ago(Duration::from_secs(secs));
}

// ---------------------------------------------------------------------------
// fmt_uptime
// ---------------------------------------------------------------------------

#[when(regex = r"^fmt_uptime is called with (\d+) seconds$")]
async fn when_fmt_uptime(world: &mut TuiWorld, secs: u64) {
    world.last_string = kameo_console::testing::fmt_uptime(Duration::from_secs(secs));
}

// ---------------------------------------------------------------------------
// short_type_name
// ---------------------------------------------------------------------------

#[when(regex = r#"^short_type_name is called with "(.*)"$"#)]
async fn when_short_type_name(world: &mut TuiWorld, input: String) {
    world.last_string = kameo_console::testing::short_type_name(&input).to_string();
}

// ---------------------------------------------------------------------------
// spark_height
// ---------------------------------------------------------------------------

#[when(regex = r"^spark_height is called with value (\d+) and max (\d+)$")]
async fn when_spark_height(world: &mut TuiWorld, value: u64, max: u64) {
    world.last_u64 = u64::from(kameo_console::testing::spark_height(value, max));
}

// ---------------------------------------------------------------------------
// braille
// ---------------------------------------------------------------------------

#[when(regex = r"^braille is called with left (\d+) and right (\d+)$")]
async fn when_braille(world: &mut TuiWorld, left: u8, right: u8) {
    world.last_char = kameo_console::testing::braille(left, right);
}

// ---------------------------------------------------------------------------
// fade_toward_bg
// ---------------------------------------------------------------------------

#[given(regex = r"^a starting color Rgb\((\d+),(\d+),(\d+)\)$")]
async fn given_color_rgb(world: &mut TuiWorld, r: u8, g: u8, b: u8) {
    world.color = Some(Color::Rgb(r, g, b));
}

#[when(regex = r"^fade_toward_bg is called with factor (\d+\.\d+)$")]
async fn when_fade_toward_bg(world: &mut TuiWorld, factor: f32) {
    let color = world.color.expect("color set in Given step");
    let result = kameo_console::testing::fade_toward_bg(color, factor);
    if let Color::Rgb(r, g, b) = result {
        world.last_rgb = (r, g, b);
    } else {
        panic!("fade_toward_bg returned non-Rgb color: {result:?}");
    }
}

// ---------------------------------------------------------------------------
// color_rgb
// ---------------------------------------------------------------------------

#[when(regex = r"^color_rgb is called with (.+)$")]
async fn when_color_rgb(world: &mut TuiWorld, color: String) {
    world.last_rgb = kameo_console::testing::color_rgb(parse_color(&color));
}

// ---------------------------------------------------------------------------
// centered_rect
// ---------------------------------------------------------------------------

#[given(regex = r"^an area at \((\d+),(\d+)\) sized (\d+)x(\d+)$")]
async fn given_area(world: &mut TuiWorld, ax: u16, ay: u16, aw: u16, ah: u16) {
    world.area = Rect {
        x: ax,
        y: ay,
        width: aw,
        height: ah,
    };
}

#[when(regex = r"^centered_rect is requested at (\d+)x(\d+)$")]
async fn when_centered_rect(world: &mut TuiWorld, w: u16, h: u16) {
    world.last_rect = kameo_console::testing::centered_rect(world.area, w, h);
}

// ---------------------------------------------------------------------------
// backpressure_style
// ---------------------------------------------------------------------------

#[when(regex = r"^backpressure_style is called with len (\d+) and capacity (\d+)$")]
async fn when_backpressure_style(world: &mut TuiWorld, len: usize, cap: usize) {
    world.last_style = kameo_console::testing::backpressure_style(len, cap);
}

// ---------------------------------------------------------------------------
// mailbox_bar
// ---------------------------------------------------------------------------

#[when(regex = r"^mailbox_bar is called with len (\d+) and capacity (\d+)$")]
async fn when_mailbox_bar(world: &mut TuiWorld, len: usize, cap: usize) {
    let (text, style) = kameo_console::testing::mailbox_bar(len, cap);
    world.last_string = text;
    world.last_style = style;
    world.mb_len = len;
    world.mb_cap = cap;
}

// ---------------------------------------------------------------------------
// rate_context
// ---------------------------------------------------------------------------

#[given(regex = r"^a snapshot and no previous snapshot$")]
async fn given_snapshot_no_prev(world: &mut TuiWorld) {
    world.current_snapshot = Some(make_snapshot(vec![make_actor(1)]));
    world.prev_snapshot = None;
}

#[given(regex = r"^a previous snapshot captured at t = (\d+)s$")]
async fn given_prev_snapshot_at(world: &mut TuiWorld, secs: u64) {
    world.prev_snapshot = Some(make_snapshot_at(vec![make_actor(1)], secs));
}

#[given(regex = r"^a current snapshot captured at t = (\d+)s \(earlier than the previous\)$")]
async fn given_current_snapshot_at(world: &mut TuiWorld, secs: u64) {
    world.current_snapshot = Some(make_snapshot_at(vec![make_actor(1)], secs));
}

#[when(regex = r"^rate_context is called$")]
async fn when_rate_context(world: &mut TuiWorld) {
    let current = world
        .current_snapshot
        .as_ref()
        .expect("current snapshot set");
    let prev = world.prev_snapshot.as_ref();
    let (prev_received, dt) = kameo_console::testing::rate_context(current, prev);
    world.prev_received = prev_received;
    world.dt = dt;
}

#[then(regex = r"^the previous-received map is empty$")]
async fn then_prev_received_empty(world: &mut TuiWorld) {
    assert!(
        world.prev_received.is_empty(),
        "expected empty prev_received map"
    );
}

#[then(regex = r"^the returned dt is None$")]
async fn then_dt_is_none(world: &mut TuiWorld) {
    assert!(
        world.dt.is_none(),
        "expected dt to be None, got {:?}",
        world.dt
    );
}

// ---------------------------------------------------------------------------
// actor_rate
// ---------------------------------------------------------------------------

#[given(regex = r"^an actor whose messages_received is (\d+)$")]
async fn given_actor_messages_received(world: &mut TuiWorld, now: u64) {
    let mut a = make_actor(1);
    a.counters.messages_received = now;
    world.actor = Some(a);
}

#[given(regex = r"^a previous received count of (\d+) for that actor$")]
async fn given_prev_received_count(world: &mut TuiWorld, prev: u64) {
    let actor = world.actor.as_ref().expect("actor set");
    world.prev_received.insert(actor.id, prev);
}

#[when(regex = r"^actor_rate is called with dt (.+)$")]
async fn when_actor_rate(world: &mut TuiWorld, dt_str: String) {
    let dt = parse_dt(dt_str.trim());
    let actor = world.actor.as_ref().expect("actor set");
    world.last_u64 = kameo_console::testing::actor_rate(actor, &world.prev_received, dt);
}

#[given(regex = r"^an actor present in this snapshot but absent from the previous one$")]
async fn given_actor_absent_from_prev(world: &mut TuiWorld) {
    world.actor = Some(make_actor(42));
    world.prev_received = HashMap::new();
}

#[when(regex = r"^actor_rate is called with a 1s dt$")]
async fn when_actor_rate_1s_dt(world: &mut TuiWorld) {
    let actor = world.actor.as_ref().expect("actor set");
    world.last_u64 = kameo_console::testing::actor_rate(
        actor,
        &world.prev_received,
        Some(Duration::from_secs(1)),
    );
}

// ---------------------------------------------------------------------------
// severity
// ---------------------------------------------------------------------------

#[given(regex = r"^an actor whose status is (.+)$")]
async fn given_actor_status(world: &mut TuiWorld, status_str: String) {
    let (status, handling) = parse_status(status_str.trim());
    let mut a = make_actor(1);
    a.status = status;
    a.handling = handling;
    world.actor = Some(a);
}

#[when(regex = r"^severity is computed$")]
async fn when_severity(world: &mut TuiWorld) {
    let actor = world.actor.as_ref().expect("actor set");
    world.last_u64 = u64::from(kameo_console::testing::severity(actor));
}

// ---------------------------------------------------------------------------
// compare / sort_actors
// ---------------------------------------------------------------------------

#[given(regex = r"^two actors with equal mailbox length but ids (\d+) and (\d+)$")]
async fn given_two_actors_equal_mailbox(world: &mut TuiWorld, id_a: u64, id_b: u64) {
    let mut a = make_actor(id_a);
    let mut b = make_actor(id_b);
    a.mailbox.len = 0;
    b.mailbox.len = 0;
    world.two = Some((a, b));
}

#[when(regex = r"^compare is called for SortCol::Mailbox$")]
async fn when_compare_mailbox(world: &mut TuiWorld) {
    let (a, b) = world.two.as_ref().expect("two actors set");
    let ord = kameo_console::testing::compare(a, b, SortCol::Mailbox, &HashMap::new(), None);
    world.ordered_first_id = if ord.is_le() { a.id.0 } else { b.id.0 };
}

#[when(regex = r"^sort_actors is called for SortCol::Mailbox with desc = true$")]
async fn when_sort_actors_mailbox_desc(world: &mut TuiWorld) {
    let (a, b) = world.two.as_ref().expect("two actors set");
    let mut v = vec![a, b];
    kameo_console::testing::sort_actors(&mut v, SortCol::Mailbox, true, &HashMap::new(), None);
    world.ordered_first_id = v[0].id.0;
}

#[then(regex = r"^the actor with id (\d+) orders before the actor with id (\d+)$")]
async fn then_actor_orders_before(
    world: &mut TuiWorld,
    expected_first: u64,
    _expected_second: u64,
) {
    assert_eq!(
        world.ordered_first_id, expected_first,
        "expected actor {expected_first} to be first, but {_expected_second} would be first"
    );
}

// ---------------------------------------------------------------------------
// Shared Then steps
// ---------------------------------------------------------------------------

#[then(regex = r#"^it returns "(.*)"$"#)]
async fn then_returns_string(world: &mut TuiWorld, expected: String) {
    assert_eq!(world.last_string, expected);
}

#[then(regex = r"^it returns (\d+)$")]
async fn then_returns_u64(world: &mut TuiWorld, expected: u64) {
    assert_eq!(world.last_u64, expected);
}

#[then(regex = r"^it returns the braille glyph for clamped heights \((\d+), (\d+)\)$")]
async fn then_braille_glyph(world: &mut TuiWorld, cl: u8, cr: u8) {
    // Oracle: same bit tables as braille() in tui.rs:1581-1582.
    const LEFT: [u8; 5] = [0x00, 0x40, 0x44, 0x46, 0x47];
    const RIGHT: [u8; 5] = [0x00, 0x80, 0xA0, 0xB0, 0xB8];
    let expected =
        char::from_u32(0x2800 + u32::from(LEFT[cl as usize] | RIGHT[cr as usize])).unwrap();
    assert_eq!(world.last_char, expected);
}

#[then(regex = r"^it returns Rgb\((\d+),\s*(\d+),\s*(\d+)\)$")]
async fn then_returns_rgb(world: &mut TuiWorld, r: u8, g: u8, b: u8) {
    assert_eq!(world.last_rgb, (r, g, b));
}

#[then(regex = r"^the result is at \((\d+),(\d+)\) sized (\d+)x(\d+)$")]
async fn then_rect_result(world: &mut TuiWorld, rx: u16, ry: u16, rw: u16, rh: u16) {
    assert_eq!(
        world.last_rect,
        Rect {
            x: rx,
            y: ry,
            width: rw,
            height: rh
        }
    );
}

#[then(regex = r"^the style is (normal|yellow|red)$")]
async fn then_style_is(world: &mut TuiWorld, style_name: String) {
    // FG = Color::Rgb(205, 205, 212) (tui.rs:43).
    let expected = match style_name.as_str() {
        "normal" => Style::new().fg(Color::Rgb(205, 205, 212)),
        "yellow" => Style::new().yellow(),
        "red" => Style::new().red(),
        _ => panic!("unknown style name: {style_name}"),
    };
    assert_eq!(world.last_style, expected);
}

#[then(regex = r#"^the text is "(.*)"$"#)]
async fn then_text_is(world: &mut TuiWorld, expected: String) {
    assert_eq!(world.last_string, expected);
}

#[then(regex = r"^the style matches backpressure_style for the same len and capacity$")]
async fn then_style_matches_backpressure(world: &mut TuiWorld) {
    let expected = kameo_console::testing::backpressure_style(world.mb_len, world.mb_cap);
    assert_eq!(world.last_style, expected);
}

// ---------------------------------------------------------------------------
// sparkline_line scenarios
// ---------------------------------------------------------------------------

/// Oracle: same bit tables as braille() in tui.rs:1581-1582.
fn braille_oracle(cl: u8, cr: u8) -> char {
    const LEFT: [u8; 5] = [0x00, 0x40, 0x44, 0x46, 0x47];
    const RIGHT: [u8; 5] = [0x00, 0x80, 0xA0, 0xB0, 0xB8];
    char::from_u32(0x2800 + u32::from(LEFT[cl as usize] | RIGHT[cr as usize])).unwrap()
}

/// Oracle: same scaling as spark_height() in tui.rs:1570-1575.
fn spark_height_oracle(value: u64, max: u64) -> u8 {
    if max == 0 {
        return 1;
    }
    ((value as f64 / max as f64 * 4.0).round() as u8).clamp(1, 4)
}

// Scenario 1: no samples → full idle baseline

#[when(regex = r"^sparkline_line is called with no samples, max (\d+) and width (\d+)$")]
async fn when_sparkline_no_samples(world: &mut TuiWorld, max: u64, width: usize) {
    world.last_line = Some(kameo_console::testing::sparkline_line(&[], max, width));
}

#[then(regex = r"^the line has exactly (\d+) braille cells$")]
async fn then_line_has_n_cells(world: &mut TuiWorld, n: usize) {
    let line = world.last_line.as_ref().expect("last_line must be set");
    assert_eq!(
        line.spans.len(),
        n,
        "expected {n} spans (cells), got {}",
        line.spans.len()
    );
}

#[then(regex = r"^every cell shows the idle baseline \(bottom dot only\)$")]
async fn then_every_cell_idle_baseline(world: &mut TuiWorld) {
    let line = world.last_line.as_ref().expect("last_line must be set");
    // max 0 → spark_height returns 1 for every column → braille(1,1)
    let expected_glyph = braille_oracle(1, 1).to_string();
    for (i, span) in line.spans.iter().enumerate() {
        assert_eq!(
            span.content.as_ref(),
            expected_glyph.as_str(),
            "cell {i}: expected idle baseline glyph {expected_glyph:?}, got {:?}",
            span.content
        );
    }
}

// Scenario 2: right-aligns and scrolls

#[given(regex = r"^more than 18 samples \(oldest first\) and width 9$")]
async fn given_more_than_18_samples(world: &mut TuiWorld) {
    // 20 samples so the oldest 2 are scrolled off; values increase so the
    // newest (index 19, value 20) is the busiest.
    world.sparkline_samples = (1u64..=20).collect();
}

#[when(regex = r"^sparkline_line is called with the busiest sample as max$")]
async fn when_sparkline_called_with_busiest_max(world: &mut TuiWorld) {
    let samples = world.sparkline_samples.clone();
    let max = *samples.iter().max().expect("non-empty samples");
    world.last_line = Some(kameo_console::testing::sparkline_line(&samples, max, 9));
}

#[then(regex = r"^only the most recent 18 samples are shown \(2 per cell\)$")]
async fn then_most_recent_18_shown(world: &mut TuiWorld) {
    let line = world.last_line.as_ref().expect("last_line must be set");
    // width 9 → 9 cells (spans), each covering 2 samples
    assert_eq!(line.spans.len(), 9, "expected 9 cells for width 9");
}

#[then(regex = r"^the newest sample occupies the rightmost cell$")]
async fn then_newest_in_rightmost_cell(world: &mut TuiWorld) {
    let samples = &world.sparkline_samples;
    let line = world.last_line.as_ref().expect("last_line must be set");
    let max = *samples.iter().max().expect("non-empty samples");
    // The last 18 samples are used (indices 2..20 for a 20-element vec).
    // Rightmost cell = last pair of the last 18 = samples[18] and samples[19].
    let n = samples.len();
    let cols = 18usize; // width * 2
    let window_start = n.saturating_sub(cols);
    let right_left = samples[window_start + cols - 2];
    let right_right = samples[window_start + cols - 1];
    let expected_glyph = braille_oracle(
        spark_height_oracle(right_left, max),
        spark_height_oracle(right_right, max),
    )
    .to_string();
    let rightmost = line.spans.last().expect("at least one span");
    assert_eq!(
        rightmost.content.as_ref(),
        expected_glyph.as_str(),
        "rightmost cell glyph mismatch: expected {expected_glyph:?} (samples {right_left},{right_right} max {max}), got {:?}",
        rightmost.content
    );
}

// Scenario 3: active / idle colors

#[given(regex = r"^a width-9 sparkline where only the most recent sample is non-zero$")]
async fn given_width9_only_recent_nonzero(world: &mut TuiWorld) {
    // 17 zeros + 1 non-zero at the end; stored for use in When.
    let mut samples = vec![0u64; 17];
    samples.push(10);
    world.sparkline_samples = samples;
}

#[when(regex = r"^the line is built$")]
async fn when_line_is_built(world: &mut TuiWorld) {
    let samples = world.sparkline_samples.clone();
    let max = *samples.iter().max().expect("non-empty samples");
    world.last_line = Some(kameo_console::testing::sparkline_line(&samples, max, 9));
}

#[then(regex = r"^the rightmost cell is drawn in the active \(cyan\) color$")]
async fn then_rightmost_active_color(world: &mut TuiWorld) {
    // SPARK_ACTIVE = Color::Rgb(110, 180, 200) (tui.rs:54)
    let active = Color::Rgb(110, 180, 200);
    let line = world.last_line.as_ref().expect("last_line must be set");
    let rightmost = line.spans.last().expect("at least one span");
    assert_eq!(
        rightmost.style.fg,
        Some(active),
        "rightmost span fg expected active color Rgb(110,180,200), got {:?}",
        rightmost.style.fg
    );
}

#[then(regex = r"^the remaining cells are drawn in the idle \(grey\) color$")]
async fn then_remaining_cells_idle_color(world: &mut TuiWorld) {
    // SPARK_IDLE = Color::Rgb(70, 70, 80) (tui.rs:53)
    let idle = Color::Rgb(70, 70, 80);
    let line = world.last_line.as_ref().expect("last_line must be set");
    let all_but_last = line.spans.len() - 1;
    for (i, span) in line.spans.iter().take(all_but_last).enumerate() {
        assert_eq!(
            span.style.fg,
            Some(idle),
            "span {i} fg expected idle color Rgb(70,70,80), got {:?}",
            span.style.fg
        );
    }
}

// ---------------------------------------------------------------------------
// detect_deadlocks
// ---------------------------------------------------------------------------

fn waiting(mut a: ActorSnapshot, target: u64) -> ActorSnapshot {
    a.waiting_on = Some(WaitEdge {
        target: ActorId(target),
        kind: WaitKind::Ask,
        elapsed: Duration::ZERO,
    });
    a
}

#[given(regex = r"^a snapshot where no actor has a waiting_on edge$")]
async fn given_no_waiting_on(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![make_actor(1), make_actor(2), make_actor(3)]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^actors A→B→C where C waits on nothing$")]
async fn given_chain_a_b_c(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![
        waiting(make_actor(1), 2),
        waiting(make_actor(2), 3),
        make_actor(3),
    ]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^actor A whose waiting_on target is A$")]
async fn given_self_cycle(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![waiting(make_actor(1), 1)]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^actors A→B and B→A$")]
async fn given_two_cycle(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![waiting(make_actor(1), 2), waiting(make_actor(2), 1)]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^actors A→B, B→C and C→A$")]
async fn given_three_cycle(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![
        waiting(make_actor(1), 2),
        waiting(make_actor(2), 3),
        waiting(make_actor(3), 1),
    ]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^a 3-actor cycle among ids 5, 2 and 8 in wait order 5→2→8→5$")]
async fn given_normalize_cycle(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![
        waiting(make_actor(5), 2),
        waiting(make_actor(2), 8),
        waiting(make_actor(8), 5),
    ]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^a cycle A↔B and a separate cycle C↔D in the same snapshot$")]
async fn given_two_independent_cycles(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![
        waiting(make_actor(1), 2),
        waiting(make_actor(2), 1),
        waiting(make_actor(3), 4),
        waiting(make_actor(4), 3),
    ]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[given(regex = r"^actor A waiting on a target id that no actor in the snapshot has$")]
async fn given_dangling_target(world: &mut TuiWorld) {
    let snap = make_snapshot(vec![waiting(make_actor(1), 99)]);
    world.cycles = kameo_console::testing::detect_deadlocks(&snap);
}

#[when(regex = r"^detect_deadlocks runs$")]
async fn when_detect_deadlocks_runs(_world: &mut TuiWorld) {
    // Given steps already ran detect_deadlocks and stored results in world.cycles.
}

#[then(regex = r"^it returns zero cycles$")]
async fn then_zero_cycles(world: &mut TuiWorld) {
    assert!(
        world.cycles.is_empty(),
        "expected zero cycles, got {:?}",
        world.cycles
    );
}

#[then(regex = r"^it returns zero cycles and does not panic$")]
async fn then_zero_cycles_no_panic(world: &mut TuiWorld) {
    assert!(
        world.cycles.is_empty(),
        "expected zero cycles, got {:?}",
        world.cycles
    );
}

#[then(regex = r"^it returns exactly one cycle containing only A$")]
async fn then_one_cycle_only_a(world: &mut TuiWorld) {
    assert_eq!(
        world.cycles,
        vec![vec![ActorId(1)]],
        "expected exactly one cycle containing only A (id 1), got {:?}",
        world.cycles
    );
}

#[then(regex = r"^it returns exactly one cycle$")]
async fn then_exactly_one_cycle(world: &mut TuiWorld) {
    assert_eq!(
        world.cycles.len(),
        1,
        "expected exactly one cycle, got {}: {:?}",
        world.cycles.len(),
        world.cycles
    );
}

#[then(regex = r"^it returns exactly two cycles$")]
async fn then_exactly_two_cycles(world: &mut TuiWorld) {
    assert_eq!(
        world.cycles.len(),
        2,
        "expected exactly two cycles, got {}: {:?}",
        world.cycles.len(),
        world.cycles
    );
}

#[then(regex = r"^the cycle contains exactly A and B$")]
async fn then_cycle_contains_a_and_b(world: &mut TuiWorld) {
    let mut members: Vec<ActorId> = world.cycles[0].clone();
    members.sort_by_key(|id| id.0);
    assert_eq!(
        members,
        vec![ActorId(1), ActorId(2)],
        "expected cycle members {{A=1, B=2}}, got {:?}",
        members
    );
}

#[then(regex = r"^the cycle contains exactly A, B and C$")]
async fn then_cycle_contains_a_b_c(world: &mut TuiWorld) {
    let mut members: Vec<ActorId> = world.cycles[0].clone();
    members.sort_by_key(|id| id.0);
    assert_eq!(
        members,
        vec![ActorId(1), ActorId(2), ActorId(3)],
        "expected cycle members {{A=1, B=2, C=3}}, got {:?}",
        members
    );
}

#[then(regex = r"^the returned cycle begins with id (\d+)$")]
async fn then_cycle_begins_with_id(world: &mut TuiWorld, id: u64) {
    assert_eq!(
        world.cycles[0][0],
        ActorId(id),
        "expected cycle to begin with id {id}, got {:?}",
        world.cycles[0][0]
    );
}

#[then(regex = r"^cycles are ordered by their first \(lowest\) id$")]
async fn then_cycles_ordered_by_first_id(world: &mut TuiWorld) {
    let first_ids: Vec<u64> = world.cycles.iter().map(|c| c[0].0).collect();
    let mut sorted = first_ids.clone();
    sorted.sort_unstable();
    assert_eq!(
        first_ids, sorted,
        "cycles are not ordered by their first id: {:?}",
        first_ids
    );
}

#[then(regex = r"^neither cycle shares a member with the other$")]
async fn then_cycles_disjoint(world: &mut TuiWorld) {
    let set_a: std::collections::HashSet<u64> = world.cycles[0].iter().map(|id| id.0).collect();
    let set_b: std::collections::HashSet<u64> = world.cycles[1].iter().map(|id| id.0).collect();
    let intersection: std::collections::HashSet<u64> =
        set_a.intersection(&set_b).copied().collect();
    assert!(
        intersection.is_empty(),
        "cycles share members: {:?}; cycle0={:?} cycle1={:?}",
        intersection,
        world.cycles[0],
        world.cycles[1]
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_color(s: &str) -> Color {
    if let Some(inner) = s.strip_prefix("Rgb(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<u8> = inner
            .split(',')
            .filter_map(|p| p.trim().parse().ok())
            .collect();
        if parts.len() == 3 {
            return Color::Rgb(parts[0], parts[1], parts[2]);
        }
    }
    match s {
        "Red" => Color::Red,
        "LightRed" => Color::LightRed,
        "Yellow" => Color::Yellow,
        "LightYellow" => Color::LightYellow,
        "Green" => Color::Green,
        "LightGreen" => Color::LightGreen,
        "Cyan" => Color::Cyan,
        "LightCyan" => Color::LightCyan,
        "Black" => Color::Black,
        "DarkGray" => Color::DarkGray,
        "White" => Color::White,
        _ => Color::Reset,
    }
}
