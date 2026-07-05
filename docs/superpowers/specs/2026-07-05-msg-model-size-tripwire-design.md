# `Msg` model + size-tripwire derive (card #114) — design

> Part of the core-rebuild epic (#122). Card #114's body describes the kameo
> reference (`Message<T>`, `Context`, `DynMessage`/`BoxMessage`), but the card's
> **finalized comments** override that with the closed per-actor `Msg`-enum model:
> no open `Handler<M>`, one closed `Msg` enum per actor, `handle` on the `Actor`
> trait (#116), the typed reply port on #115. Under that model, this card's real
> deliverable is the **`Msg` marker trait** plus a **compile-time slot-size
> tripwire derive** — the "box-oversized-variants discipline, first-class, not
> left to the `large_enum_variant` clippy lint" the #122 design-risk note calls
> for.

## The gap this closes

The mailbox (#133) queues `Signal<A>` **by value** — no per-message heap box.
That is the whole point of the model (zero-alloc `tell`), but it has one sharp
edge, measured in the #122 note: a `tokio`/`flume` slot costs
`size_of::<Signal<A>>()` = the size of the actor's **largest** `Msg` variant, so
a single fat inline variant taxes *every* queued slot, even tiny messages
(measured `4104 B` inline vs `16 B` boxed → **256×** for 1 000 queued messages),
and a by-value `send` `memcpy`s the whole message, so zero-box inverts to
pessimal above ~a cache line.

`mailbox.rs` already guards its **own** `Signal` layout (`LinkDied` is boxed; the
`link_died_variant_is_boxed_so_message_slots_stay_small` test pins it). Nothing
yet extends that discipline to the **user's** `Msg` type — and `clippy`'s
`large_enum_variant` only fires above 200 B and can be `#[allow]`d away. This
card makes the budget a **real compile error** the user opts into, with an
explicit escape.

## Decisions (locked with the card owner)

1. **Scope** — `Msg` marker trait (`bombay-core`) + `#[derive(Msg)]` size
   tripwire (`macros`). `handle` stays with #116, the reply port with #115.
2. **Mechanism A — opt-in derive.** Not a mandatory `Mailboxed::Msg: Msg` bound
   (that would break the mailbox's deliberate "`Msg` is any concrete type"
   design — its `u64`/`(u32, u32)` test actors — and over-constrain
   single-message actors). Not a #116 run-loop guard (wrong card, and its error
   would point at `Signal<A>` not the user's enum). The derive is the blessed
   path; "first-class" = a real compile error, not a silence-able lint.
3. **Default budget `SLOT_BUDGET = 256`** (4 cache lines) — fits identity-bearing
   commands carrying several KERI AIDs/hashes (~32 B each) yet trips the KB-scale
   inline blob. Overridable per type via `#[msg(budget = N)]`.

## Architecture — two pieces, one boundary

### 1. `bombay-core/src/message.rs` (new module)

A marker trait, nothing more:

```rust
/// The seam a mailbox and run-loop dispatch on: an actor's single closed
/// message type, stored in a queue slot **by value**.
///
/// `Send + 'static` for now; #9 relaxes `Send` → the cfg-gated `MaybeSend`
/// for single-threaded client builds. #116 may bound its `Actor::Msg: Msg`;
/// this card does not touch `Mailboxed`, so arbitrary `type Msg` still compiles.
pub trait Msg: Send + 'static {
    /// Per-slot byte budget. A mailbox queues `Signal<A>` by value, so every
    /// slot costs `size_of` of the largest `Msg` variant; this bounds it.
    /// Default 256 B; `#[derive(Msg)]` overrides it from `#[msg(budget = N)]`.
    const SLOT_BUDGET: usize = 256;
}
```

Wired into `bombay-core/src/lib.rs` as `pub mod message;`. It deliberately does
**not** tighten `Mailboxed::Msg` (still `Send + 'static`) — Mechanism A.

### 2. `macros/src/derive_msg.rs` (new), wired in `macros/src/lib.rs`

`#[proc_macro_derive(Msg, attributes(msg))]`:

- **Accepts** a *concrete* `struct` or `enum` (a single-command actor's message
  can be a struct).
- **Rejects** — with a clear `compile_error!`, not a downstream const-eval error:
  - a **generic** type — a pure marker trait has no monomorphized site to hang a
    generic const-assert on, so silently skipping the check would be a lie.
    (YAGNI: a follow-up card handles generic `Msg` if a real need appears.)
  - a **union**.
- **Generates**:
  ```rust
  impl ::bombay_core::message::Msg for T {
      // emitted ONLY when #[msg(budget = N)] is present:
      const SLOT_BUDGET: usize = N;
  }

  const _: () = ::core::assert!(
      ::core::mem::size_of::<T>()
          <= <T as ::bombay_core::message::Msg>::SLOT_BUDGET,
      "`T` exceeds its Msg::SLOT_BUDGET — box the largest variant (as Signal \
       boxes LinkDied), or raise it with #[msg(budget = N)]",
  );
  ```
  The macro cannot read `size_of` itself (it sees only tokens), so the budget
  check is a **generated `const` static-assert** evaluated at const-eval /
  monomorphization. The derive knows the type's identifier at expansion, so it
  bakes the enum name into the message with `format!` (producing a plain
  `&'static str` literal — const-`assert!` messages must be literals, so no
  runtime formatting). The assert references `<T as Msg>::SLOT_BUDGET`, so the
  override flows through uniformly whether defaulted or set.

**Path convention — `::bombay_core`, not `::bombay`.** The vendored derives emit
`::bombay::…` because they target the umbrella crate, but the umbrella **does not
yet depend on or re-export `bombay-core`** — the M1 spine is deliberately
standalone until the whole core lands (#112–#121; see the `bombay-core/src/lib.rs`
doc: "the surface is settled once the whole core lands"). So the `Msg` derive
emits `::bombay_core::message::Msg`, the crate where the trait actually lives.
The derive crate (`bombay_macros`) does **not** need a `bombay-core` dependency —
it only emits a path token; the *deriving* crate provides the trait. When the
spine lands and the umbrella re-exports it, a follow-up switches the path to
`::bombay` (or adopts `proc-macro-crate` to resolve renames robustly).

**Remedy vs escape.** The primary remedy for a tripped budget is **boxing the fat
variant** (as `Signal` boxes `LinkDied`). The `#[msg(budget = N)]` attribute is
the deliberate, greppable escape for a measured, genuinely-large message — visible
in review, unlike an `#[allow]`.

### Re-export (deferred, serde-style)

The trait (`bombay_core::message::Msg`) and the derive (`bombay_macros::Msg`)
share the name `Msg` but live in different namespaces (type vs macro), so they
*can* be co-re-exported and `use …::Msg` brings in both — exactly as
`serde::Serialize` does. During M1 the derive tests import `bombay_core::message::Msg`
and `bombay_macros::Msg` directly (the spine isn't behind the umbrella yet);
folding both into a single `bombay::Msg` re-export is part of the umbrella
re-wiring when the whole core lands.

## Data flow

1. A user writes their closed command enum and `#[derive(Msg)]`s it.
2. At compile time the derive emits `impl Msg` + the `const _` static-assert.
3. If the enum's largest variant pushes `size_of` over the (possibly overridden)
   budget, const-eval fails the build with a message naming the enum and the
   remedy. Otherwise it compiles and the type is usable wherever `M: Msg` is
   required (e.g. #116's `Actor::Msg`, if it opts into the bound).
4. Runtime is unchanged: `Msg` is a marker; the mailbox stores `Signal::Message(msg)`
   by value exactly as before.

## Testing (rule 7 categories + the card's "hard as fuck" bar)

Three tiers, all inside the **existing** gate (`cargoNextest` + `cargoDocTest`) —
no new test runner, no `trybuild` (it shells out to `cargo` at test-time, which
crane's offline sandbox breaks). **Verified empirically:** doctests *do* run for a
`proc-macro` crate under this toolchain (a probe `//!` doctest failed as expected;
the vendored derives' examples are `​```ignore`d, which is why they don't).

**Tier 1 — compile-fail via paired `compile_fail` doctests** (on the derive's doc
comment, run by `cargoDocTest`). Each guard is a **pass / fail / fixed triple** so
"fails to compile" is attributable to the budget, not an unrelated error (rule 8:
fails for the *right* reason). The paired *pass* doctest also forces
`::bombay_core::message::Msg` to resolve, so the *fail* doctest can only be failing
on the budget:
- within budget → compiles;
- fat inline variant (`Bulk([u8; 4096])`) → `compile_fail` (tripwire fires);
- boxed variant (`Bulk(Box<[u8; 4096]>)`) → compiles (the remedy, mirrors
  `Signal`/`LinkDied`);
- `#[msg(budget = 8192)]` on the fat enum → compiles (escape works);
- **defensive boundary:** `#[derive(Msg)]` on a **generic** type and on a **union**
  → `compile_fail` (the derive's own `compile_error!`).

**Tier 2 — generated-impl behaviour via native runtime tests** (`macros/tests/`,
run by nextest; the derive expands at test-crate compile time, asserts at
runtime):
- a small enum and a struct derive → `<T as Msg>::SLOT_BUDGET == 256` (impl
  emitted, default flows through);
- `#[msg(budget = 8192)]` enum → `<T as Msg>::SLOT_BUDGET == 8192` (override
  emitted). These crates depend on `bombay_core` + `bombay_macros` as normal
  deps; bombay-core's *own* tests never derive (a crate can't name itself
  `::bombay_core` without an `extern crate self` alias), so the derive is only
  ever exercised from `macros`.

**Tier 3 — `parse_budget` unit tests** (`#[cfg(test)]` in `derive_msg.rs`, native;
`proc-macro2`/`syn` types work in unit tests): `#[msg(budget = N)]` → `Some(N)`;
absent → `None`; malformed (`budget = "x"`, bare `budget`, unknown key) → `Err`.
This is the derive's only real branching logic — exhaustively covered so a
mutation (default selection, the parse, the key match) is killed.

**`bombay-core` — the trait** (native tests, and under the `--package bombay-core`
`cargo-mutants` gate → zero survivors):
- `Msg::SLOT_BUDGET` default is exactly `256` (pins the constant, like the
  mailbox's `Capacity::MAX` test — kills a budget-constant mutation);
- a hand-written `impl Msg` overriding `SLOT_BUDGET` compiles (the escape hatch
  the derive automates) and the trait is usable as a `fn f<M: Msg>()` bound.

**Mutants scope note:** the gate runs `cargo mutants --package bombay-core`, so the
`Msg` trait is under the zero-survivors bar; the `macros` derive is not (extending
the gate to a proc-macro crate risks nesting cargo invocations in the sandbox for
little gain). The derive's sole branching logic — `parse_budget` — is instead
pinned by exhaustive Tier-3 unit tests; the rest is token assembly (mutation there
is noise).

## Documentation impact — coverage baseline, NOT the README public-API section

The README's *"public API at a glance"* documents the **umbrella `bombay::prelude`**,
which still ships the *old kameo* surface (`Message<M>`, the old `SendError`). The
rebuilt `bombay-core` spine is deliberately **not public yet** ("the surface is
settled once the whole core lands"), so #133 (mailbox) and #113 (error) did **not**
advertise their new types in the README — they recorded the module + tests in
[`docs/testing/coverage-baseline.md`](../../testing/coverage-baseline.md). #114
follows that exact precedent: adding `Msg` to the README now would misdescribe the
shipped API. → **Update `docs/testing/coverage-baseline.md`** with the new
`message` module and the `macros` derive tests; leave the README untouched. The
`Msg`/`#[derive(Msg)]` surface joins the README when the umbrella re-export lands
with the rest of the spine.

## Sequencing / boundaries

- **Branch `core/114-msg-model-tripwire` off `main`.** The `Msg` trait has no
  dependency on the #113 error types, so #114 is independently mergeable and does
  not inherit PR #135's open state. (Confirmed: `main` has `mailbox.rs` but not
  `error.rs`.)
- **Does not touch `Mailboxed`** (substrate) or the run-loop (#116).
- **Leaves `handle` to #116** and the **reply port to #115**. #116 may bound
  `Actor::Msg: Msg`; that is #116's decision.

## Out of scope (YAGNI)

- Auto-boxing oversized variants (the model owner chose a **tripwire, not
  auto-box** — the user boxes deliberately, as `Signal` does).
- Generic `Msg` types (rejected with a clear error; revisit only on real need).
- Any `Context`, `handle`, reply-port, or `DynMessage`/`BoxMessage` surface from
  the card's kameo-reference body — superseded by the closed-enum model.
