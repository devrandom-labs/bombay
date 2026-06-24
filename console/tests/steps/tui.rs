use std::time::Duration;

use cucumber::{World, then, when};

#[derive(Debug, Default, World)]
pub struct TuiWorld {
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
