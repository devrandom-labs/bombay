#[test]
fn infallible_test_extraction_is_statically_bounded() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/infallible_result_errors.rs");
    cases.compile_fail("tests/compile/fail/inhabited_result_is_not_infallible.rs");
}
