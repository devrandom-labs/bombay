#[test]
fn application_children_are_checked_from_caller_syntax() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/application_children.rs");
    #[cfg(not(feature = "axum"))]
    cases.compile_fail("tests/compile/fail/application_child_must_be_behavior.rs");
    #[cfg(feature = "axum")]
    cases.compile_fail("tests/compile/fail/application_child_must_be_behavior_feature_unified.rs");
}
