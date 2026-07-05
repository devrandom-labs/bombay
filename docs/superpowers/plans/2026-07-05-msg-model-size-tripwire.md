# `Msg` Model + Size-Tripwire Derive (card #114) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give bombay-core a `Msg` marker trait carrying a per-slot byte budget, plus a `#[derive(Msg)]` that emits a compile-time slot-size tripwire so a fat inline message variant fails the build instead of silently taxing every mailbox queue slot.

**Architecture:** Two pieces. (1) `bombay-core/src/message.rs` — a `pub trait Msg: Send + 'static { const SLOT_BUDGET: usize = 256; }` marker, no coupling to `Mailboxed` (arbitrary `type Msg` still compiles). (2) `macros/src/derive_msg.rs` — `#[proc_macro_derive(Msg, attributes(msg))]` that emits `impl ::bombay_core::message::Msg` plus a generated `const _: () = assert!(size_of::<T>() <= SLOT_BUDGET, …)` static-assert; `#[msg(budget = N)]` overrides the budget; generics and unions are rejected with a clear `compile_error!`.

**Tech Stack:** Rust edition 2024, `syn` 2.0 / `quote` / `proc-macro2` (already deps of `macros`), Nix flake gate (`cargoNextest` + `cargoDocTest`). No new runtime deps; one dev-dep (`bombay-core`) added to `macros`.

**Design spec:** [`docs/superpowers/specs/2026-07-05-msg-model-size-tripwire-design.md`](../specs/2026-07-05-msg-model-size-tripwire-design.md)

**Conventions (non-negotiable — from CLAUDE.md + repo):**
- **TDD:** write the failing test, watch it fail, then implement. Use `superpowers:test-driven-development`.
- **God-level clippy bar applies to ALL new code.** `derive_msg.rs` and `message.rs` are NEW → they carry **no** `#[allow]` quarantine header (that block is only on vendored kameo files). No `unwrap`/`expect`/`panic` in production; functions ≤80 lines, ≤5 args, cognitive-complexity ≤9; all `use` at file top; doc every `pub` item.
- **Gate:** `nix develop --command cargo …` for iterating; `nix flake check` is the authoritative gate. Never invoke a raw `/nix/store` path.
- **Commits:** conventional, scoped `core(message)` / `macros(msg)`; **no** Claude/Anthropic attribution.
- **Branch:** already on `core/114-msg-model-tripwire` (off `main`).

---

## File Structure

- **Create** `bombay-core/src/message.rs` — the `Msg` trait + its trait-level tests. One responsibility: the message-model marker.
- **Modify** `bombay-core/src/lib.rs` — add `pub mod message;`.
- **Create** `macros/src/derive_msg.rs` — `DeriveMsg` (`Parse` + `ToTokens`) and the `parse_budget` helper + unit tests. One responsibility: the derive.
- **Modify** `macros/src/lib.rs` — `mod derive_msg;`, `use`, and the `#[proc_macro_derive(Msg, attributes(msg))]` entry point (with the paired `compile_fail` doctests).
- **Modify** `macros/Cargo.toml` — add `[dev-dependencies] bombay-core` (path) so the runtime tests + doctests can resolve `::bombay_core`.
- **Create** `macros/tests/derive_msg.rs` — native runtime tests of the generated impl + budget override.
- **Modify** `docs/testing/coverage-baseline.md` — record the new `message` module + `macros` derive tests. (README stays untouched — bombay-core is not yet behind the umbrella; matches #113/#133.)

---

## Task 1: `Msg` marker trait in bombay-core

**Files:**
- Create: `bombay-core/src/message.rs`
- Modify: `bombay-core/src/lib.rs`
- Test: `bombay-core/src/message.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Create `bombay-core/src/message.rs` with ONLY the module doc + the test module (no trait yet), so the test fails to compile first:

```rust
//! The `Msg` marker trait: an actor's single closed message type (card #114).
//!
//! A mailbox queues `Signal<A>` **by value**, so every slot costs `size_of` of
//! the largest `A::Msg` variant. `Msg` carries the per-slot byte budget that
//! bounds it; `#[derive(Msg)]` (the `bombay_macros` crate) implements this trait
//! and emits a compile-time static-assert that trips when the budget is exceeded.
//!
//! This module deliberately does **not** tighten `mailbox::Mailboxed::Msg`
//! (still `Send + 'static`): arbitrary `type Msg` stays legal, and `#116` decides
//! whether `Actor::Msg` bounds `: Msg`.

#[cfg(test)]
mod tests {
    use super::Msg;

    struct Ping;
    impl Msg for Ping {}

    struct Roomy;
    impl Msg for Roomy {
        const SLOT_BUDGET: usize = 4096;
    }

    /// The default slot budget is exactly 256 B (4 cache lines) — pins the
    /// constant so a mutation to it is caught (like the mailbox's `Capacity::MAX`).
    #[test]
    fn slot_budget_defaults_to_256() {
        assert_eq!(<Ping as Msg>::SLOT_BUDGET, 256);
    }

    /// The budget is overridable by hand — the escape hatch `#[derive(Msg)]`
    /// automates via `#[msg(budget = N)]`.
    #[test]
    fn slot_budget_is_overridable() {
        assert_eq!(<Roomy as Msg>::SLOT_BUDGET, 4096);
    }

    /// `Msg` is a usable generic bound (what `#116`'s `Actor::Msg` would rest on).
    #[test]
    fn msg_is_usable_as_a_generic_bound() {
        fn budget_of<M: Msg>() -> usize {
            M::SLOT_BUDGET
        }
        assert_eq!(budget_of::<Ping>(), 256);
    }
}
```

Add to `bombay-core/src/lib.rs` after `pub mod mailbox;`:

```rust
pub mod message;
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `nix develop --command cargo test -p bombay-core --lib message`
Expected: FAIL — compile error, `cannot find trait Msg in this scope` (trait not defined yet).

- [ ] **Step 3: Write the minimal implementation**

Insert the trait into `bombay-core/src/message.rs` between the module doc and the `#[cfg(test)]` block:

```rust
/// An actor's single closed message type, stored in a mailbox slot **by value**.
///
/// `Send + 'static` for now; `#9` relaxes `Send` to the cfg-gated `MaybeSend`
/// for single-threaded client builds. Implement with `#[derive(Msg)]` — it also
/// emits the slot-size tripwire — or by hand when you have a measured reason to
/// set a non-default [`SLOT_BUDGET`](Msg::SLOT_BUDGET).
pub trait Msg: Send + 'static {
    /// The per-slot byte budget for this message type. A mailbox queues by
    /// value, so this bounds `size_of` of the largest variant; the derive trips
    /// the build if `size_of::<Self>()` exceeds it. Default 256 B (4 cache
    /// lines) — enough for identity-bearing commands (several AIDs/hashes), tight
    /// enough to catch the KB-scale inline blob.
    const SLOT_BUDGET: usize = 256;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay-core --lib message`
Expected: PASS — 3 tests pass.

- [ ] **Step 5: Run the gate**

Run: `nix flake check`
Expected: green (fmt, clippy, nextest, doctest all pass). If clippy flags a missing doc, add it; if fmt complains, run `nix develop --command cargo fmt`.

- [ ] **Step 6: Commit**

```bash
git add bombay-core/src/message.rs bombay-core/src/lib.rs
git commit -m "core(message): Msg marker trait with default slot budget (#114)"
```

---

## Task 2: `#[derive(Msg)]` — generated impl + default budget (native runtime test)

Builds the derive minimally: emit `impl Msg` (no budget override, no tripwire, no validation yet). Proven by a native runtime test that the derive expands and the default budget flows through.

**Files:**
- Modify: `macros/Cargo.toml`
- Create: `macros/src/derive_msg.rs`
- Modify: `macros/src/lib.rs`
- Test: `macros/tests/derive_msg.rs`

- [ ] **Step 1: Add the dev-dependency**

In `macros/Cargo.toml`, add a `[dev-dependencies]` section (after `[dependencies]`, before `[lints]`):

```toml
[dev-dependencies]
bombay-core = { path = "../bombay-core" }
```

- [ ] **Step 2: Write the failing test**

Create `macros/tests/derive_msg.rs`:

```rust
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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `nix develop --command cargo test -p bombay_macros --test derive_msg`
Expected: FAIL — `cannot find derive macro Msg` (the derive doesn't exist yet).

- [ ] **Step 4: Write the minimal derive**

Create `macros/src/derive_msg.rs` (NEW code — NO `#[allow]` quarantine header):

```rust
//! `#[derive(Msg)]` — implements the `Msg` marker trait and (from Task 3) emits
//! a compile-time slot-size tripwire. See card #114 and the design spec.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    DeriveInput, Ident,
    parse::{Parse, ParseStream},
};

/// A parsed `#[derive(Msg)]` input: the message type's identifier.
pub struct DeriveMsg {
    ident: Ident,
}

impl Parse for DeriveMsg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let input: DeriveInput = input.parse()?;
        Ok(Self { ident: input.ident })
    }
}

impl ToTokens for DeriveMsg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {}
        });
    }
}
```

Wire it into `macros/src/lib.rs`. Add near the other `mod` lines (top of file):

```rust
mod derive_msg;
```

Add to the `use` group:

```rust
use derive_msg::DeriveMsg;
```

Add the entry point next to the other `#[proc_macro_derive]` fns (e.g. after `derive_reply`):

```rust
/// Derive the [`Msg`](https://docs.rs/bombay-core/latest/bombay_core/message/trait.Msg.html)
/// marker trait and emit a compile-time slot-size tripwire.
///
/// A mailbox queues messages by value, so a fat inline variant taxes every
/// queue slot. This derive trips the build when `size_of` of the message exceeds
/// its `Msg::SLOT_BUDGET` (default 256 B). Box the largest variant to fix it, or
/// raise the budget with `#[msg(budget = N)]`.
///
/// Within budget — compiles:
/// ```
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Ok { Small(u64) }
/// ```
#[proc_macro_derive(Msg, attributes(msg))]
pub fn derive_msg(input: TokenStream) -> TokenStream {
    let derive_msg = parse_macro_input!(input as DeriveMsg);
    TokenStream::from(derive_msg.into_token_stream())
}
```

(`TokenStream`, `parse_macro_input`, and `ToTokens` are already imported at the top of `lib.rs`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `nix develop --command cargo test -p bombay_macros --test derive_msg`
Expected: PASS — `derive_emits_impl_with_default_budget` passes.

- [ ] **Step 6: Run the gate**

Run: `nix flake check`
Expected: green. The new `Ok` doctest on `derive_msg` compiles and runs (proc-macro doctests execute — verified). If clippy flags `derive_msg.rs`, fix it up to the bar (no quarantine header).

- [ ] **Step 7: Commit**

```bash
git add macros/Cargo.toml macros/src/derive_msg.rs macros/src/lib.rs macros/tests/derive_msg.rs
git commit -m "macros(msg): #[derive(Msg)] emits the Msg impl (#114)"
```

---

## Task 3: The slot-size tripwire (paired `compile_fail` doctests)

Adds the generated `const` static-assert and its paired pass/fail/fixed doctests. This is the heart of the card.

**Files:**
- Modify: `macros/src/derive_msg.rs` (add the assert to `to_tokens`)
- Modify: `macros/src/lib.rs` (add the paired doctests to the derive's doc comment)

- [ ] **Step 1: Write the failing test (the compile_fail doctest)**

In `macros/src/lib.rs`, extend the `derive_msg` doc comment (added in Task 2) with the fail + fixed doctests, directly under the "Within budget — compiles" block:

```rust
/// A fat inline variant trips the budget:
/// ```compile_fail
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Bad { Bulk([u8; 4096]) }
/// ```
///
/// Boxing the fat variant fixes it (as `Signal` boxes `LinkDied`):
/// ```
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Fixed { Bulk(Box<[u8; 4096]>) }
/// ```
```

- [ ] **Step 2: Run the doctests to verify the tripwire is missing**

Run: `nix develop --command cargo test -p bombay_macros --doc`
Expected: FAIL — the `Bad` block is marked `compile_fail` but currently **compiles** (no assert yet), so the doctest harness reports it as an unexpected success:
`Test compiled successfully, but it's marked \`compile_fail\`.`

- [ ] **Step 3: Add the static-assert to the derive**

Replace the `to_tokens` impl in `macros/src/derive_msg.rs` with the version that emits the tripwire:

```rust
impl ToTokens for DeriveMsg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        let over_budget = format!(
            "`{ident}` exceeds its Msg::SLOT_BUDGET — box the largest variant \
             (as Signal boxes LinkDied), or raise it with #[msg(budget = N)]"
        );
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {}

            const _: () = ::core::assert!(
                ::core::mem::size_of::<#ident>()
                    <= <#ident as ::bombay_core::message::Msg>::SLOT_BUDGET,
                #over_budget
            );
        });
    }
}
```

- [ ] **Step 4: Run the doctests to verify they pass**

Run: `nix develop --command cargo test -p bombay_macros --doc`
Expected: PASS — `Ok` and `Fixed` compile; `Bad` now fails to compile, satisfying `compile_fail`.

- [ ] **Step 5: Verify the runtime tests still pass**

Run: `nix develop --command cargo test -p bombay_macros --test derive_msg`
Expected: PASS — `Small`/`Unit` are within budget, so the added assert is satisfied.

- [ ] **Step 6: Run the gate**

Run: `nix flake check`
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add macros/src/derive_msg.rs macros/src/lib.rs
git commit -m "macros(msg): compile-time slot-size tripwire + paired doctests (#114)"
```

---

## Task 4: `#[msg(budget = N)]` override + `parse_budget` unit tests

Adds the budget attribute (raises/lowers the per-type budget) and exhaustive unit tests on the parse logic — the derive's only real branching, the part `cargo-mutants` would probe.

**Files:**
- Modify: `macros/src/derive_msg.rs` (add `budget` field, `parse_budget`, override const, unit tests)
- Modify: `macros/tests/derive_msg.rs` (runtime override test)
- Modify: `macros/src/lib.rs` (doctest: `#[msg(budget = N)]` lets a fat enum compile)

- [ ] **Step 1: Write the failing unit tests (parse logic)**

Append a `#[cfg(test)]` module to `macros/src/derive_msg.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::parse_budget;
    use syn::{Attribute, parse_quote};

    fn attrs(attr: Attribute) -> Vec<Attribute> {
        vec![attr]
    }

    #[test]
    fn budget_attribute_yields_its_value() {
        let parsed = parse_budget(&attrs(parse_quote!(#[msg(budget = 8192)]))).unwrap();
        assert_eq!(parsed, Some(8192));
    }

    #[test]
    fn absent_attribute_yields_none() {
        let parsed = parse_budget(&attrs(parse_quote!(#[derive(Clone)]))).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn non_integer_budget_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(budget = "x")]))).is_err());
    }

    #[test]
    fn bare_budget_without_value_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(budget)]))).is_err());
    }

    #[test]
    fn unknown_msg_key_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(limit = 8)]))).is_err());
    }
}
```

> Note: `unwrap()` is allowed here because this is a `#[cfg(test)]` module (the god-level bar bans `unwrap` in production, not tests). Keep it out of the non-test code.

- [ ] **Step 2: Run to verify it fails**

Run: `nix develop --command cargo test -p bombay_macros --lib`
Expected: FAIL — `cannot find function parse_budget` (not defined yet).

- [ ] **Step 3: Implement `parse_budget` + wire the override**

In `macros/src/derive_msg.rs`, extend the imports:

```rust
use syn::{
    Attribute, DeriveInput, Ident, LitInt,
    parse::{Parse, ParseStream},
};
```

Add the `budget` field to the struct:

```rust
/// A parsed `#[derive(Msg)]` input: the type's identifier and an optional
/// per-type slot budget from `#[msg(budget = N)]`.
pub struct DeriveMsg {
    ident: Ident,
    budget: Option<usize>,
}
```

Update `Parse` to read the budget:

```rust
impl Parse for DeriveMsg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Note: do NOT shadow `input` (god-level clippy bans `shadow-reuse`);
        // bind the parsed AST to a distinct name.
        let derive: DeriveInput = input.parse()?;
        let budget = parse_budget(&derive.attrs)?;
        Ok(Self { ident: derive.ident, budget })
    }
}
```

Update `to_tokens` to emit the override const when present:

```rust
impl ToTokens for DeriveMsg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        let budget_const = self
            .budget
            .map(|n| quote! { const SLOT_BUDGET: usize = #n; });
        let over_budget = format!(
            "`{ident}` exceeds its Msg::SLOT_BUDGET — box the largest variant \
             (as Signal boxes LinkDied), or raise it with #[msg(budget = N)]"
        );
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {
                #budget_const
            }

            const _: () = ::core::assert!(
                ::core::mem::size_of::<#ident>()
                    <= <#ident as ::bombay_core::message::Msg>::SLOT_BUDGET,
                #over_budget
            );
        });
    }
}
```

Add the `parse_budget` free function (below the impls, above `#[cfg(test)]`):

```rust
/// Extracts `budget = N` from `#[msg(...)]` attributes, if present. Errors on a
/// non-integer value, a bare `budget`, or any key other than `budget`.
fn parse_budget(attrs: &[Attribute]) -> syn::Result<Option<usize>> {
    let mut budget = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("msg")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("budget") {
                budget = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
                Ok(())
            } else {
                Err(meta.error("unknown `msg` key; the only key is `budget`"))
            }
        })?;
    }
    Ok(budget)
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `nix develop --command cargo test -p bombay_macros --lib`
Expected: PASS — all 5 `parse_budget` tests pass.

- [ ] **Step 5: Add + run the runtime override test**

Append to `macros/tests/derive_msg.rs`:

```rust
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
```

Run: `nix develop --command cargo test -p bombay_macros --test derive_msg`
Expected: PASS — `Roomy` compiles (4096 ≤ 8192) and reports budget 8192.

- [ ] **Step 6: Add the override doctest**

In `macros/src/lib.rs`, append to the `derive_msg` doc comment:

```rust
/// Or raise the budget for a deliberately large message:
/// ```
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// #[msg(budget = 8192)]
/// enum Big { Bulk([u8; 4096]) }
/// ```
```

Run: `nix develop --command cargo test -p bombay_macros --doc`
Expected: PASS.

- [ ] **Step 7: Run the gate**

Run: `nix flake check`
Expected: green.

- [ ] **Step 8: Commit**

```bash
git add macros/src/derive_msg.rs macros/src/lib.rs macros/tests/derive_msg.rs
git commit -m "macros(msg): #[msg(budget = N)] override + parse_budget tests (#114)"
```

---

## Task 5: Defensive boundary — reject generics and unions

The derive rejects what it cannot correctly check: a generic type (a marker trait has no monomorphized site for a generic const-assert) and a union. Both surface as the derive's own `compile_error!` via `parse_macro_input!`.

**Files:**
- Modify: `macros/src/derive_msg.rs` (validation in `Parse`)
- Modify: `macros/src/lib.rs` (compile_fail doctests for generic + union)

- [ ] **Step 1: Write the failing tests (compile_fail doctests)**

In `macros/src/lib.rs`, append to the `derive_msg` doc comment:

```rust
/// The derive needs a concrete type — a generic is rejected:
/// ```compile_fail
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// enum Generic<T> { A(T) }
/// ```
///
/// Unions are rejected (structs and enums only):
/// ```compile_fail
/// use bombay_core::message::Msg;
/// #[derive(bombay_macros::Msg)]
/// union U { a: u32, b: u64 }
/// ```
```

- [ ] **Step 2: Run to verify they fail**

Run: `nix develop --command cargo test -p bombay_macros --doc`
Expected: FAIL — the **union** block is the clean red signal: the un-validated derive emits `impl Msg for U {}` and `size_of` works on unions, so `U` compiles and the `compile_fail` block reports an unexpected success.

> The **generic** block may *already* fail to compile — but for the **wrong reason**: the un-validated derive emits `impl ::bombay_core::message::Msg for Generic {}`, dropping `<T>`, so rustc errors with "missing generics for enum `Generic`", not our rejection. `compile_fail` can't tell the two apart. Step 3 makes the rejection **explicit and correct** (our `compile_error!` fires first, before any malformed impl), which is the point of this task — so proceed regardless of whether the generic block is currently red or green.

- [ ] **Step 3: Add validation to `Parse`**

In `macros/src/derive_msg.rs`, extend imports with `Data`:

```rust
use syn::{
    Attribute, Data, DeriveInput, Ident, LitInt,
    parse::{Parse, ParseStream},
};
```

Replace the `Parse` impl with the validating version:

```rust
impl Parse for DeriveMsg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Do NOT shadow `input` (god-level clippy bans `shadow-reuse`).
        let derive: DeriveInput = input.parse()?;

        if let Some(param) = derive.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "`#[derive(Msg)]` needs a concrete type: the slot-size tripwire \
                 cannot size an unmonomorphized generic",
            ));
        }
        if let Data::Union(data) = &derive.data {
            return Err(syn::Error::new_spanned(
                data.union_token,
                "`#[derive(Msg)]` supports structs and enums, not unions",
            ));
        }

        let budget = parse_budget(&derive.attrs)?;
        Ok(Self { ident: derive.ident, budget })
    }
}
```

- [ ] **Step 4: Run the doctests to verify they pass**

Run: `nix develop --command cargo test -p bombay_macros --doc`
Expected: PASS — `Generic` and `U` now fail to compile with the derive's own error, satisfying `compile_fail`; the positive doctests still compile.

- [ ] **Step 5: Verify runtime + unit tests still pass**

Run: `nix develop --command cargo test -p bombay_macros`
Expected: PASS — the derive still accepts the concrete structs/enums in the runtime tests.

- [ ] **Step 6: Run the gate**

Run: `nix flake check`
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add macros/src/derive_msg.rs macros/src/lib.rs
git commit -m "macros(msg): reject generic and union message types (#114)"
```

---

## Task 6: Coverage baseline doc + mutation verification

Record the new surface in the coverage baseline (README stays untouched — bombay-core is not yet public), and confirm the `Msg` trait survives the mutation gate with zero survivors.

**Files:**
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 1: Update the coverage baseline**

Open `docs/testing/coverage-baseline.md`, find where the mailbox (#133) / error (#113) modules are recorded, and add a sibling entry for the message model. Match the surrounding format; the content to convey:

> `bombay-core/src/message.rs` (card #114) — the `Msg` marker trait (`SLOT_BUDGET`, default 256 B). Trait covered by `bombay-core` unit tests (default, hand-override, generic-bound). The `#[derive(Msg)]` proc-macro (`macros/src/derive_msg.rs`) is covered by: native runtime tests (`macros/tests/derive_msg.rs`) for the generated impl + `#[msg(budget = N)]` override; `parse_budget` unit tests for the attribute logic; and paired `compile_fail` doctests on the derive for the slot-size tripwire, boxed-remedy, budget-escape, and generic/union rejection. No README change — the rebuilt spine is not behind the umbrella yet (same as #113/#133).

- [ ] **Step 2: Run the mutation gate on bombay-core**

Run: `nix build .#mutants -L`
Expected: build succeeds → **zero survivors**. If the `Msg` trait produces a surviving mutant (e.g. `SLOT_BUDGET` changed and no test caught it), that means a test gap — add/tighten a `bombay-core` test to kill it, then re-run. (Do not touch the `macros` crate here; it is outside the `--package bombay-core` mutation gate by design — its logic is covered by the Task-4 unit tests.)

- [ ] **Step 3: Final full gate**

Run: `nix flake check`
Expected: green across the board.

- [ ] **Step 4: Commit**

```bash
git add docs/testing/coverage-baseline.md
git commit -m "docs(testing): record #114 message model + derive coverage"
```

---

## Task 7: Open the PR

- [ ] **Step 1: Push the branch**

```bash
git push -u origin core/114-msg-model-tripwire
```
(If SSH times out, push over HTTPS via the `gh` credential helper.)

- [ ] **Step 2: Open the PR**

```bash
gh pr create --repo devrandom-labs/bombay --base main \
  --title "core(message): Msg model + size-tripwire derive (#114)" \
  --body "$(cat <<'EOF'
Card #114. Adds the `Msg` marker trait (`bombay-core`) carrying a per-slot byte
budget (default 256 B), and `#[derive(Msg)]` (`bombay_macros`) which emits the
`impl Msg` plus a compile-time static-assert that trips when `size_of` of the
message exceeds its budget — making the mailbox's by-value slot-size discipline a
real compile error instead of a silence-able clippy lint. `#[msg(budget = N)]`
overrides the budget; generics and unions are rejected.

Scope per the card's finalized closed-enum model: `handle` stays with #116, the
reply port with #115. Does not touch `Mailboxed` (arbitrary `type Msg` still
compiles). Path is `::bombay_core` (spine not yet behind the umbrella).

Tested: native runtime tests (generated impl + override), `parse_budget` unit
tests, and paired `compile_fail` doctests (tripwire fires, boxing fixes it,
budget escape, generic/union rejection). `Msg` trait under the `bombay-core`
cargo-mutants gate.

Design: docs/superpowers/specs/2026-07-05-msg-model-size-tripwire-design.md
EOF
)"
```

- [ ] **Step 3: Confirm the card is on the board and CI is green**

Run: `gh pr checks --repo devrandom-labs/bombay <PR#>` (wait for `Nix Flake Check`).
Confirm #114 is on project board #4; move its Status to In Progress / the PR links it.

---

## Self-Review (completed during authoring)

- **Spec coverage:** `Msg` trait (Task 1) ✓; derive impl (Task 2) ✓; tripwire (Task 3) ✓; budget override + parse tests (Task 4) ✓; generic/union rejection (Task 5) ✓; coverage-baseline + mutants (Task 6) ✓; path `::bombay_core`, no README change, no `Mailboxed` change — all honored. `#116`/`#115` boundary respected (nothing here builds `handle`/reply).
- **Type consistency:** `DeriveMsg { ident, budget }`, `parse_budget(&[Attribute]) -> syn::Result<Option<usize>>`, `Msg::SLOT_BUDGET` used identically across Tasks 1–5. Trait path `::bombay_core::message::Msg` consistent in every generated snippet.
- **No placeholders:** every step has concrete code + exact `nix develop --command` invocations + expected pass/fail.
- **Known checkpoint:** Task 3 Step 2 and Task 5 Step 2 rely on `compile_fail` doctests *failing as unexpected successes* before the guard exists — that is the TDD red state, intended.
