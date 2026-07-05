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

// Exactly at the default budget: size_of == 256 == SLOT_BUDGET must still compile
// (guards the inclusive `<=` in the derive's static-assert against a `<` regression).
#[derive(bombay_macros::Msg)]
struct ExactDefault([u8; 256]);

// Exactly at an overridden budget: size_of == 8192 == the raised SLOT_BUDGET.
#[derive(bombay_macros::Msg)]
#[msg(budget = 8192)]
struct ExactOverride([u8; 8192]);

/// A message whose `size_of` is exactly its budget compiles — the tripwire is
/// inclusive (`size_of <= SLOT_BUDGET`), not strict. If the derive's comparison
/// regressed to `<`, `ExactDefault`/`ExactOverride` would fail to compile and
/// break this test's build.
#[test]
fn size_exactly_at_budget_compiles() {
    assert_eq!(core::mem::size_of::<ExactDefault>(), 256);
    assert_eq!(<ExactDefault as Msg>::SLOT_BUDGET, 256);
    assert_eq!(core::mem::size_of::<ExactOverride>(), 8192);
    assert_eq!(<ExactOverride as Msg>::SLOT_BUDGET, 8192);
}
