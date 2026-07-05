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

#[derive(bombay_macros::Msg)]
#[msg(budget = 8192)]
enum Roomy {
    Bulk([u8; 4096]),
}

/// `#[msg(budget = N)]` overrides the default, and a message within the raised
/// budget still compiles (the assert reads the overridden const).
#[test]
fn budget_attribute_overrides_the_default() {
    assert_eq!(<Roomy as Msg>::SLOT_BUDGET, 8192);
}
