#[test]
fn application_scope_and_early_receiving_authority_are_affine() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/application_scope_cannot_clone.rs");
    cases.compile_fail("tests/compile/fail/application_scope_cannot_declare_second_root.rs");
    cases.compile_fail("tests/compile/fail/application_scope_receiver_cannot_clone.rs");
}
