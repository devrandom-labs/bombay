#![cfg(feature = "axum")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::Router;
use bombay::prelude::*;

mod application_support;

use application_support::{RootTerminal, assert_completed};

struct Root;

#[bombay::behavior::behavior(addr = MailAddr, message = Never)]
impl Root {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the Behavior initialization contract returns its exact error type"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::stop())
    }

    #[allow(
        clippy::unused_self,
        reason = "the Behavior receive signature owns self"
    )]
    fn receive(&mut self, _: MailAddr, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

#[test]
fn axum_router_receives_the_live_root_reference_exactly_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();

    let terminal: RootTerminal<_> = Application::new(Root.stop_on_shutdown())
        .run_axum(
            "127.0.0.1:0".parse().expect("socket address parses"),
            move |application| {
                assert_eq!(application.root().address(), MailAddr(0));
                observed.fetch_add(1, Ordering::SeqCst);
                Router::new()
            },
        )
        .expect("a normally stopping root gracefully stops Axum");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_completed(terminal);
}

#[test]
fn bind_failure_does_not_activate_or_build_the_router() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").expect("test port binds");
    let address = occupied.local_addr().expect("bound address is available");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();

    let error: AxumRunError<RootTerminal<_>> = Application::new(Root.stop_on_shutdown())
        .run_axum(address, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Router::new()
        })
        .expect_err("the occupied address must reject a second listener");

    assert!(matches!(error, AxumRunError::Bind { address: rejected, .. } if rejected == address));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn axum_protocol_inversion_is_compile_checked() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/compile/fail/axum_wrong_root_protocol.rs");
}
