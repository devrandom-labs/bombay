#[test]
fn application_children_are_checked_from_caller_syntax() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/application_children.rs");
    cases.pass("tests/compile/pass/application_inferred_custody.rs");
    cases.pass("tests/compile/pass/application_recursive_custody.rs");
    cases.pass("tests/compile/pass/application_timer_custody.rs");
    cases.pass("tests/compile/pass/application_component_custody.rs");
    cases.compile_fail("tests/compile/fail/application_child_must_be_behavior.rs");
}
