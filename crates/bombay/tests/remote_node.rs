#![cfg(feature = "remote")]

use bombay::remote::NodeStartupRefusal;
use std::error::Error;
use std::io;
use std::ptr;

#[test]
fn startup_diagnostics_keep_native_cause_out_of_ordinary_output() {
    let secret = "private-node-configuration";
    let native = Box::new(io::Error::other(secret));
    let original_allocation = ptr::from_ref(native.as_ref());
    let cause = NodeStartupRefusal::Open { cause: native };
    let displayed = cause.to_string();
    let debugged = format!("{cause:?}");
    let source = cause.source().expect("actual original native cause");
    let native = source
        .downcast_ref::<io::Error>()
        .expect("the original concrete native error");
    let retained_allocation = ptr::from_ref(native);
    assert_eq!(retained_allocation, original_allocation);
    assert_eq!(displayed, "native Node startup failed");
    assert!(!displayed.contains(secret));
    assert!(!debugged.contains(secret));
    assert_eq!(source.to_string(), secret);
}
