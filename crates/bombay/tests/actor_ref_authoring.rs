#[test]
fn actor_reference_admission_names_the_supplied_origin() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/actor_ref_ambiguous_send.rs");
    cases.compile_fail("tests/compile/fail/actor_ref_wrong_established_protocol.rs");
}
