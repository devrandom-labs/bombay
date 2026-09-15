#[test]
fn ordinary_prelude_keeps_foundational_mechanisms_deliberate() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/advanced_behavior_imports.rs");
    cases.pass("tests/compile/pass/advanced_runtime_imports.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_send_input.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_delivery_interpreter.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_unsettled_observation.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_logical_host_requirements.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_behavior_trait.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_actor_space.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_hosts.rs");
    cases.compile_fail("tests/compile/fail/prelude_hides_app.rs");
}
