use std::time::Duration;

use cucumber::{World, given, then, when};
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};

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
    world.area = Rect { x: ax, y: ay, width: aw, height: ah };
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
    assert_eq!(world.last_rect, Rect { x: rx, y: ry, width: rw, height: rh });
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
// Helpers
// ---------------------------------------------------------------------------

fn parse_color(s: &str) -> Color {
    if let Some(inner) = s.strip_prefix("Rgb(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<u8> = inner.split(',').filter_map(|p| p.trim().parse().ok()).collect();
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
