#[test]
fn driver_surface_conformance() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/*.rs");
    cases.compile_fail("tests/compile/fail/*.rs");
}
