#![cfg(feature = "remote")]

#[test]
fn transport_is_private() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/remote_configuration.rs");
    cases.compile_fail("tests/compile/fail/remote_session.rs");
    cases.compile_fail("tests/compile/fail/remote_query.rs");
}
