#![cfg(feature = "remote")]

use bombay::remote::ExportError;
use std::error::Error;
use std::io;
use std::ptr;
use std::sync::Arc;

#[test]
fn withdrawal_failure_keeps_its_source_out_of_display() {
    let cause = Box::new(io::Error::other("private native withdrawal detail"));
    let original = ptr::from_ref(cause.as_ref());
    let failure = Arc::new(ExportError::Withdrawal { cause });
    assert_eq!(failure.to_string(), "network withdrawal was refused");
    assert_eq!(
        format!("{failure:?}"),
        "ExportError(network withdrawal was refused)"
    );
    let source = failure
        .source()
        .expect("the actual native cause remains available");
    assert!(ptr::eq(
        source.downcast_ref::<io::Error>().unwrap(),
        original
    ));
    assert_eq!(source.to_string(), "private native withdrawal detail");
}

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
    cases.compile_fail("tests/compile/fail/remote_client_requires_interpretation.rs");
    cases.compile_fail("tests/compile/fail/remote_export_requires_declaration.rs");
    cases.compile_fail("tests/compile/fail/remote_export_withdrawal_reuse.rs");
}
