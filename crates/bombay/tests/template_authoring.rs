#[test]
fn behavior_layer_authoring_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/fluent_template_stack.rs");
    cases.compile_fail("tests/compile/fail/fluent_template_order_is_static.rs");
    cases.compile_fail("tests/compile/fail/stash_requires_closed_phase.rs");
}
