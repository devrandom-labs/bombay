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

#[test]
fn remote_operations_require_verification_and_typed_completion() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/remote_client_policies.rs");
    cases.pass("tests/compile/pass/remote_export_policies.rs");
    cases.compile_fail("tests/compile/fail/remote_client_requires_verification.rs");
    cases.compile_fail("tests/compile/fail/remote_export_requires_verification.rs");
    cases.compile_fail("tests/compile/fail/remote_client_requires_completion.rs");
}
