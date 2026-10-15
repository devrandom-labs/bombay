#![cfg(feature = "remote")]

#[test]
fn node_work_retains_caller_values_and_keeps_transport_private() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/compile/pass/node_work_retains_borrowed_values.rs");
    cases.compile_fail("tests/compile/fail/node_work_cannot_receive_session.rs");
    cases.compile_fail("tests/compile/fail/node_paired_work_cannot_receive_session.rs");
    cases.compile_fail("tests/compile/fail/node_close_requires_received_execution.rs");
}
