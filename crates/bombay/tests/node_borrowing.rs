#![cfg(feature = "remote")]

#[test]
fn native_session_loans_preserve_their_owner() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/node_raw_can_return_session.rs");
    cases.compile_fail("tests/compile/fail/node_join_cannot_return_session.rs");
    cases.compile_fail("tests/compile/fail/node_close_cannot_overlap_borrowed_session.rs");
}
