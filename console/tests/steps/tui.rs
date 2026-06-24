use std::time::Duration;

use cucumber::{World, then, when};

#[derive(Debug, Default, World)]
pub struct TuiWorld {
    last_string: String,
    last_u64: u64,
}

#[when(regex = r"^fmt_short is called with (\d+) milliseconds$")]
async fn when_fmt_short(world: &mut TuiWorld, millis: u64) {
    world.last_string = kameo_console::testing::fmt_short(Duration::from_millis(millis));
}

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

#[when(regex = r"^spark_height is called with value (\d+) and max (\d+)$")]
async fn when_spark_height(world: &mut TuiWorld, value: u64, max: u64) {
    world.last_u64 = u64::from(kameo_console::testing::spark_height(value, max));
}

#[then(regex = r#"^it returns "(.*)"$"#)]
async fn then_returns_string(world: &mut TuiWorld, expected: String) {
    assert_eq!(world.last_string, expected);
}

#[then(regex = r"^it returns (\d+)$")]
async fn then_returns_u64(world: &mut TuiWorld, expected: u64) {
    assert_eq!(world.last_u64, expected);
}
