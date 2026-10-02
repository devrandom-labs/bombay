#[test]
fn ordinary_and_advanced_entity_exports_remain_addressable() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/entity_public_surface.rs");
}

#[test]
fn lifecycle_representation_is_private_to_the_entity_owner() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/entity_lifecycle_representation_is_private.rs");
}
