#[test]
fn entity_has_no_second_machine_dependency() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/entity_cannot_import_machine.rs");
}
