#[test]
fn behavior_layer_authoring_contract() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/fluent_template_stack.rs");
    cases.pass("tests/compile/pass/stash_machine.rs");
    cases.compile_fail("tests/compile/fail/discarded_actor_policy_and_terminal.rs");
    cases.compile_fail("tests/compile/fail/fluent_template_order_is_static.rs");
    cases.compile_fail("tests/compile/fail/stash_requires_closed_phase.rs");
    cases.compile_fail("tests/compile/fail/stash_requires_infallible_error.rs");
}
