#[test]
fn entity_identity_is_nominally_typed_by_domain_family() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/entity_id_family_mismatch.rs");
    cases.compile_fail("tests/compile/fail/entity_application_missing_host.rs");
    cases.compile_fail("tests/compile/fail/entity_application_role_exchange.rs");
}
