use core::convert::Infallible;

use bombay::behavior::Never;
use bombay::testing::InfallibleResultExt;

fn main() {
    let behavior_value = Result::<_, Never>::Ok(String::from("behavior")).infallible();
    let core_value = Result::<_, Infallible>::Ok(String::from("core")).infallible();

    assert_eq!(behavior_value, "behavior");
    assert_eq!(core_value, "core");
}
