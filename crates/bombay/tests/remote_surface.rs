#![cfg(feature = "remote")]

#[test]
fn transport_is_private() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/remote_configuration.rs");
    cases.compile_fail("tests/compile/fail/remote_query.rs");
}

#[test]
fn client_limits_require_response_capacity() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/client_limits.rs");
    cases.compile_fail("tests/compile/fail/client_limits_require_responses.rs");
}
