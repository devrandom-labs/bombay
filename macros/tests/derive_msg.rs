//! Runtime behaviour of `#[derive(Msg)]`: the generated impl and the budget
//! override, exercised natively (the derive expands at this crate's compile
//! time, assertions run under nextest). Compile-fail behaviour (the tripwire,
//! generics, unions) lives in the paired `compile_fail` doctests on the derive.

use bombay_core::message::Msg;

#[derive(bombay_macros::Msg)]
enum Small {
    Ping,
    Pong(u64),
}

#[derive(bombay_macros::Msg)]
struct Unit;

/// The derive emits `impl Msg`, and an un-annotated type gets the default budget.
#[test]
fn derive_emits_impl_with_default_budget() {
    assert_eq!(<Small as Msg>::SLOT_BUDGET, 256);
    assert_eq!(<Unit as Msg>::SLOT_BUDGET, 256);
}
