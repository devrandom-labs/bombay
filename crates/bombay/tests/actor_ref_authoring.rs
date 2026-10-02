#[test]
fn actor_reference_and_installed_authority_remain_distinct() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/actor_ref_ambiguous_send.rs");
    cases.compile_fail("tests/compile/fail/actor_ref_wrong_established_protocol.rs");
    cases.compile_fail("tests/compile/fail/installed_actor_wrong_behavior.rs");
}
