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
