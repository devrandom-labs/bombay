#[test]
fn output_consumer_surface_is_exact() {
    let fixtures = trybuild::TestCases::new();
    fixtures.pass("tests/compile/pass/output_consumer_is_a_closure.rs");
    fixtures.compile_fail("tests/compile/fail/output_consumer_trait_is_not_a_port.rs");
}
