#![cfg(feature = "remote")]

#[test]
fn node_work_and_connection_custody_remain_distinct() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/node_raw_can_return_session.rs");
    cases.compile_fail("tests/compile/fail/node_work_cannot_receive_session.rs");
    cases.compile_fail("tests/compile/fail/node_close_cannot_overlap_borrowed_session.rs");
}
