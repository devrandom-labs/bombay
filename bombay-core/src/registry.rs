//! Local name→actor registry (card #119): register a name, look it up, dedup
//! on collision.
//!
//! This is the **in-process** resolver only. Cross-node addressing is the
//! Zenoh key-expr (#121/#2) — a different mechanism (routing/discovery over
//! the dataspace), not this map.
//!
//! # Design (finalized on card #119, revised by the #122 adversarial review)
//!
//! * **Map primitive: [`papaya`]** — lock-free reads (no guard ever held
//!   across an `.await`, because every op copies/clones out and drops the
//!   guard before returning), purpose-built for the read-heavy lookup /
//!   moderate register-churn workload. `dashmap` was rejected (its `Ref`
//!   holds a sync shard lock — one refactor away from a deadlock across
//!   `.await`); see the research record on the card.
//! * **Values are erased WEAK handles** ([`WeakActorRef`] behind a private
//!   trait object), like kameo's `HashMap<ActorId, Link>` — a registration
//!   never pins the actor's channel: dropping the last strong
//!   [`ActorRef`] stops the actor even while its name stays registered.
//! * **Register-once, atomically.** The claim decision (free / dead incumbent
//!   → take it; live incumbent → [`NameTaken`]) runs inside a single
//!   [`papaya::HashMap::compute`] — there is no check-then-act window, so
//!   racing registrants on one name always produce exactly one winner.
//! * **Dead reads as absent, everywhere.** An entry whose channel has closed
//!   behaves as if unregistered on *every* path: `lookup` yields `Ok(None)`
//!   (never a stale ref, never [`WrongActorType`]), and `register` reclaims
//!   the name. Read path and write path share one liveness rule:
//!   *the mailbox channel is open*.
//! * **A concrete type, not a trait seam.** The card note sketched a
//!   `Registry` trait so a deterministic impl could serve loom/DST — that
//!   premise dissolved: the DST lane is MIRI (ADR-0005), and papaya 0.2.4
//!   runs green under the sweep's exact `-Zmiri-strict-provenance` flags
//!   (verified empirically on the pinned nightly), so the production impl
//!   itself is fully visible to the lane. Evidence: ADR-0009.
//!
//! Entries are removed by [`Registry::unregister`] (or overwritten by a
//! reclaim); a dead entry that is never reclaimed lingers as a tombstone
//! until then — it is unobservable through `lookup`, and lifecycle-driven
//! cleanup is the supervision card's job (#120).

use core::{any::Any, fmt};
use std::borrow::Cow;

use papaya::{Compute, HashMap, Operation};

use crate::{
    actor::{Actor, ActorRef, WeakActorRef},
    error::{NameTaken, WrongActorType},
};

/// The one liveness rule, shared by every registry path: an entry is alive
/// while its actor's mailbox channel is open — strong senders still exist
/// *and* the run-loop's receiver has not been dropped. Erases the `Actor`
/// type parameter so one map holds registrations of every actor type;
/// `as_any` recovers the concrete [`WeakActorRef`] for typed lookup.
trait ErasedEntry: Send + Sync {
    /// `true` while the registered actor's channel is open.
    fn is_alive(&self) -> bool;
    /// The entry as `Any`, for the typed downcast in lookup.
    fn as_any(&self) -> &dyn Any;
}

impl<A: Actor> ErasedEntry for WeakActorRef<A> {
    fn is_alive(&self) -> bool {
        // Both legs are load-bearing: `upgrade` fails once every strong sender
        // is gone, but succeeds while one lingers even after the receiver
        // dropped — `is_alive` (channel-closed) catches that reaped-but-
        // referenced state.
        self.upgrade().is_some_and(|strong| strong.is_alive())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The local name→actor registry: register a live [`ActorRef`] under a
/// string name, look it up later (typed), unregister to free the name.
///
/// Shareable across tasks and threads (`&self` everywhere, `Send + Sync`);
/// all operations are synchronous and lock-free — nothing here is ever held
/// across an `.await`.
pub struct Registry {
    map: HashMap<Cow<'static, str>, Box<dyn ErasedEntry>>,
}

impl Registry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Registers `actor_ref`'s actor under `name`. Register-once: fails with
    /// [`NameTaken`] while a **live** actor holds the name; a dead incumbent
    /// (channel closed) is reclaimed atomically by the same call.
    ///
    /// Stores a downgraded [`WeakActorRef`] — registration does not keep the
    /// actor alive.
    ///
    /// # Errors
    ///
    /// [`NameTaken`] — the name is currently held by a live actor (possibly
    /// the same one: registration is not idempotent).
    pub fn register<A: Actor>(
        &self,
        name: impl Into<Cow<'static, str>>,
        actor_ref: &ActorRef<A>,
    ) -> Result<(), NameTaken> {
        let weak = actor_ref.downgrade();
        let guard = self.map.guard();
        // The whole claim decision lives inside one atomic `compute` — the
        // liveness check and the insert cannot interleave with a racing
        // registrant (no check-then-act). The closure may run more than once
        // under contention, so it clones the weak handle per attempt.
        let outcome = self.map.compute(
            name.into(),
            |entry| match entry {
                Some((_, current)) if current.is_alive() => Operation::Abort(()),
                _ => {
                    let claim: Box<dyn ErasedEntry> = Box::new(weak.clone());
                    Operation::Insert(claim)
                }
            },
            &guard,
        );
        match outcome {
            Compute::Inserted(..) | Compute::Updated { .. } => Ok(()),
            Compute::Aborted(()) => Err(NameTaken),
            // The closure never returns `Operation::Remove`, so papaya cannot
            // report a removal — reaching this is a programmer bug here or a
            // breaking behavior change in papaya, not a caller-visible state.
            Compute::Removed(..) => unreachable!("register never removes"),
        }
    }

    /// Looks up the actor registered under `name`, typed as `A`.
    ///
    /// `Ok(None)` covers both true absence and a dead incumbent (channel
    /// closed) — a dead entry reads as absent on every path, whatever its
    /// type, exactly as `register`'s reclaim rule treats it.
    ///
    /// The returned [`ActorRef`] is a fresh strong handle: it participates in
    /// ref-count liveness like any other clone.
    ///
    /// # Errors
    ///
    /// [`WrongActorType`] — the name is held by a **live** actor of a
    /// different `Actor` type.
    pub fn lookup<A: Actor>(&self, name: &str) -> Result<Option<ActorRef<A>>, WrongActorType> {
        let guard = self.map.guard();
        let Some(entry) = self.map.get(name, &guard) else {
            return Ok(None);
        };
        match entry.as_any().downcast_ref::<WeakActorRef<A>>() {
            // Upgrade alone is not liveness: a lingering strong ref elsewhere
            // keeps `upgrade` succeeding after the receiver dropped, so the
            // channel-open filter applies the same rule `register` uses.
            Some(weak) => Ok(weak.upgrade().filter(ActorRef::is_alive)),
            None if entry.is_alive() => Err(WrongActorType),
            None => Ok(None),
        }
    }

    /// Removes the entry under `name`, live or dead. Returns `true` if an
    /// entry was removed, `false` if the name was not registered.
    pub fn unregister(&self, name: &str) -> bool {
        let guard = self.map.guard();
        self.map.remove(name, &guard).is_some()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `entries` counts dead-but-unreclaimed tombstones too — it reflects
        // map occupancy, not live actors.
        f.debug_struct("Registry")
            .field("entries", &self.map.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use futures::stream::AbortHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::Registry;
    use crate::{
        actor::{Actor, ActorRef},
        error::{NameTaken, WrongActorType},
        mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, Mailboxed, Signal},
        message::Msg,
    };
    use proptest::prelude::*;

    // Minimal actors purely to key mailboxes/refs (the registry never runs a
    // loop). Two distinct types so the wrong-type lookup boundary is testable.
    // `ProbeMsg` carries a `u64` so the round-trip test can prove the looked-up
    // ref reaches the *same* channel, not just a channel.
    struct Probe;
    #[derive(Debug)]
    struct ProbeMsg(u64);
    impl Msg for ProbeMsg {}
    impl Mailboxed for Probe {
        type Msg = ProbeMsg;
    }
    impl Actor for Probe {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Probe)
        }
        async fn handle(
            &mut self,
            _: ProbeMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct Other;
    #[derive(Debug)]
    struct OtherMsg;
    impl Msg for OtherMsg {}
    impl Mailboxed for Other {
        type Msg = OtherMsg;
    }
    impl Actor for Other {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Other)
        }
        async fn handle(
            &mut self,
            _: OtherMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    /// Builds a ref + receiver pair for an actor type. Keeping the receiver in
    /// the test's hands lets each test reap the actor on its own terms (drop of
    /// the receiver is exactly what the run-loop does on stop).
    fn build<A: Actor>(id: u64) -> (ActorRef<A>, MailboxReceiver<A>) {
        let cap = Capacity::try_from(4usize).expect("valid capacity");
        let (tx, rx) = Mailbox::<A>::bounded(cap);
        let (abort, _reg) = AbortHandle::new_pair();
        let actor_ref = ActorRef::new(ActorId::new(id), tx, CancellationToken::new(), abort, None);
        (actor_ref, rx)
    }

    /// Sequence: register then lookup resolves a ref wired to the SAME mailbox
    /// channel — proven by a message round-trip (exact payload received on the
    /// original receiver), not just by id equality.
    #[tokio::test]
    async fn register_then_lookup_resolves_the_same_actor() {
        let registry = Registry::new();
        let (actor_ref, mut rx) = build::<Probe>(1);

        registry
            .register("counter", &actor_ref)
            .expect("fresh name registers");

        let resolved = registry
            .lookup::<Probe>("counter")
            .expect("same type")
            .expect("live actor resolves");
        assert_eq!(resolved.id(), ActorId::new(1), "resolves the registrant");

        // Both awaits are bounded (#179 discipline): under a hostile mutant
        // (e.g. `Capacity::get -> 0`, a rendezvous channel) an unbounded
        // sequential tell-then-recv deadlocks instead of failing fast.
        timeout(Duration::from_secs(5), resolved.tell(ProbeMsg(42)))
            .await
            .expect("tell must not hang")
            .expect("looked-up ref delivers");
        let received = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("recv must not hang");
        let Some(Signal::Message {
            msg: ProbeMsg(n), ..
        }) = received
        else {
            panic!("expected the message on the ORIGINAL receiver");
        };
        assert_eq!(n, 42, "the exact payload crossed the same channel");
    }

    /// A name never registered reads as absent — `Ok(None)`, not an error.
    #[test]
    fn lookup_of_unknown_name_is_absent() {
        let registry = Registry::new();
        assert!(
            registry
                .lookup::<Probe>("nobody")
                .expect("no entry, no type conflict")
                .is_none(),
        );
    }

    /// Register-once: a second register on a name held by a LIVE actor is
    /// rejected with `NameTaken`, and the incumbent entry is untouched.
    #[test]
    fn register_on_live_name_is_rejected_and_keeps_incumbent() {
        let registry = Registry::new();
        let (first, _rx1) = build::<Probe>(1);
        let (second, _rx2) = build::<Probe>(2);

        registry.register("hot", &first).expect("fresh name");
        assert_eq!(
            registry.register("hot", &second),
            Err(NameTaken),
            "a live incumbent blocks re-registration",
        );

        let resolved = registry
            .lookup::<Probe>("hot")
            .expect("same type")
            .expect("incumbent still live");
        assert_eq!(
            resolved.id(),
            ActorId::new(1),
            "the losing register must not have replaced the incumbent",
        );
    }

    /// Defensive boundary: a name registered under one actor type looked up as
    /// another is a type error while the incumbent is alive.
    #[test]
    fn lookup_with_wrong_actor_type_errors() {
        let registry = Registry::new();
        let (actor_ref, _rx) = build::<Probe>(1);

        registry.register("typed", &actor_ref).expect("fresh name");
        assert_eq!(
            registry
                .lookup::<Other>("typed")
                .expect_err("a live entry of a different type is a type conflict"),
            WrongActorType,
        );
    }

    /// Sequence: unregister frees the name — the entry is gone (`Ok(None)`), a
    /// second unregister is a no-op (`false`), and the freed name is
    /// re-registrable by a different live actor.
    #[test]
    fn unregister_frees_the_name_for_reregistration() {
        let registry = Registry::new();
        let (first, _rx1) = build::<Probe>(1);
        let (second, _rx2) = build::<Probe>(2);

        registry.register("cycle", &first).expect("fresh name");
        assert!(registry.unregister("cycle"), "removes the live entry");
        assert!(
            registry
                .lookup::<Probe>("cycle")
                .expect("no entry")
                .is_none(),
            "unregister-then-lookup reads absent",
        );
        assert!(
            !registry.unregister("cycle"),
            "double-unregister is a no-op",
        );

        registry
            .register("cycle", &second)
            .expect("a freed name is registrable again");
        let resolved = registry
            .lookup::<Probe>("cycle")
            .expect("same type")
            .expect("new registrant live");
        assert_eq!(resolved.id(), ActorId::new(2));
    }

    /// Lifecycle: a registered actor that is fully reaped (all strong refs AND
    /// the receiver gone) reads as absent — the stale entry never yields a ref.
    #[test]
    fn lookup_of_reaped_actor_is_absent() {
        let registry = Registry::new();
        let (actor_ref, rx) = build::<Probe>(1);
        registry.register("ghost", &actor_ref).expect("fresh name");

        drop(actor_ref);
        drop(rx);

        assert!(
            registry
                .lookup::<Probe>("ghost")
                .expect("dead reads absent")
                .is_none(),
            "a reaped actor's stale entry must not resolve",
        );
    }

    /// Lifecycle: a dead incumbent does not squat its name — a new live actor
    /// can claim it, and lookup then resolves the replacement.
    #[test]
    fn register_reclaims_a_dead_incumbents_name() {
        let registry = Registry::new();
        let (first, rx1) = build::<Probe>(1);
        registry.register("seat", &first).expect("fresh name");
        drop(first);
        drop(rx1);

        let (second, _rx2) = build::<Probe>(2);
        registry
            .register("seat", &second)
            .expect("a dead incumbent's name is reclaimable");

        let resolved = registry
            .lookup::<Probe>("seat")
            .expect("same type")
            .expect("replacement live");
        assert_eq!(
            resolved.id(),
            ActorId::new(2),
            "lookup resolves the claimant"
        );
    }

    /// Defensive boundary: a DEAD incumbent of another type reads as absent,
    /// not as a type conflict — dead entries behave as absent on every path
    /// (same invariant the register reclaim path enforces).
    #[test]
    fn dead_incumbent_of_another_type_reads_absent_not_type_error() {
        let registry = Registry::new();
        let (actor_ref, rx) = build::<Probe>(1);
        registry.register("seat", &actor_ref).expect("fresh name");
        drop(actor_ref);
        drop(rx);

        assert!(
            registry
                .lookup::<Other>("seat")
                .expect("a dead entry cannot claim a type")
                .is_none(),
        );
    }

    /// THE design invariant of the weak-handle registry: registration must not
    /// pin the actor. Dropping the last strong ref closes the channel even
    /// though the registry still holds the name.
    #[tokio::test]
    async fn registration_does_not_pin_the_actor() {
        let registry = Registry::new();
        let (actor_ref, mut rx) = build::<Probe>(1);
        registry
            .register("pinned?", &actor_ref)
            .expect("fresh name");

        drop(actor_ref);

        let received = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("recv must not hang");
        assert!(
            received.is_none(),
            "the registry entry must not keep the mailbox channel open",
        );
        assert!(
            registry
                .lookup::<Probe>("pinned?")
                .expect("dead reads absent")
                .is_none(),
            "and the entry must not resurrect the actor",
        );
    }

    /// Liveness for the registry means the CHANNEL is open, not merely that
    /// strong refs linger: with the receiver gone (actor reaped) but a strong
    /// ref still held somewhere, the name is reclaimable…
    #[test]
    fn name_reclaimable_when_only_receiver_is_gone() {
        let registry = Registry::new();
        let (first, rx1) = build::<Probe>(1);
        registry.register("seat", &first).expect("fresh name");
        drop(rx1); // reaped: receiver gone, strong ref `first` still alive

        let (second, _rx2) = build::<Probe>(2);
        registry
            .register("seat", &second)
            .expect("a receiver-gone incumbent is dead, despite lingering refs");
    }

    /// …and lookup applies the SAME liveness rule (read path and write path
    /// enforce the invariant identically): receiver gone ⇒ absent, even while
    /// a strong ref to the dead actor still exists.
    #[test]
    fn lookup_sees_receiver_reaped_actor_as_absent() {
        let registry = Registry::new();
        let (actor_ref, rx) = build::<Probe>(1);
        registry.register("seat", &actor_ref).expect("fresh name");
        drop(rx); // strong ref still held

        assert!(
            registry
                .lookup::<Probe>("seat")
                .expect("dead reads absent")
                .is_none(),
        );
        drop(actor_ref);
    }

    /// #186 / ADR-0010 (read path): an actor in the DRAIN WINDOW — every
    /// external strong ref dropped, a queued message still pinning the channel
    /// via its `self_sender` — reads as ABSENT: no external handle can reach
    /// it, so the registry must not hand out a resurrecting ref. Fails under
    /// the ADR-0003 shape, where the queued self_sender keeps flume's
    /// `sender_count` non-zero and the weak upgrade still succeeds.
    #[test]
    fn lookup_in_drain_window_reads_absent() {
        let registry = Registry::new();
        let (actor_ref, _rx) = build::<Probe>(1);
        registry.register("drain", &actor_ref).expect("fresh name");

        actor_ref
            .tell(ProbeMsg(9))
            .try_send()
            .expect("open mailbox accepts the message");
        drop(actor_ref); // drain window: only the queued self_sender remains

        assert!(
            registry
                .lookup::<Probe>("drain")
                .expect("dying reads absent, never a type conflict")
                .is_none(),
            "a draining actor must not be resolvable",
        );
    }

    /// #186 / ADR-0010 (write path, same rule as the read path): a draining
    /// incumbent's name is reclaimable from the moment its last external
    /// strong ref drops — a new live actor claims it without waiting for the
    /// backlog to drain.
    #[test]
    fn register_reclaims_name_in_drain_window() {
        let registry = Registry::new();
        let (first, _rx1) = build::<Probe>(1);
        registry.register("seat", &first).expect("fresh name");

        first
            .tell(ProbeMsg(9))
            .try_send()
            .expect("open mailbox accepts the message");
        drop(first); // drain window for the incumbent

        let (second, _rx2) = build::<Probe>(2);
        registry
            .register("seat", &second)
            .expect("a draining incumbent's name is reclaimable");
    }

    /// `Default` is a working empty registry, equivalent to `new` (guards the
    /// `Default → new` delegation — a self-recursive mutant there only crashes
    /// if something actually calls `Registry::default()`).
    #[test]
    fn default_is_an_empty_working_registry() {
        let registry = Registry::default();
        assert!(
            registry
                .lookup::<Probe>("anything")
                .expect("empty")
                .is_none(),
            "a default registry starts empty",
        );
        let (actor_ref, _rx) = build::<Probe>(1);
        registry
            .register("first", &actor_ref)
            .expect("a default registry accepts registrations");
    }

    /// Debug guard: names the struct and surfaces the entry count (stubbed
    /// formatters are a known cargo-mutants target).
    #[test]
    fn registry_debug_names_struct_and_entry_count() {
        let registry = Registry::new();
        let (a, _rx1) = build::<Probe>(1);
        let (b, _rx2) = build::<Probe>(2);
        registry.register("a", &a).expect("fresh name");
        registry.register("b", &b).expect("fresh name");

        let shown = format!("{registry:?}");
        assert!(shown.contains("Registry"), "names the struct: {shown}");
        assert!(shown.contains('2'), "surfaces the entry count: {shown}");
    }

    /// The registry is shared across tasks/threads by design — compile-time
    /// property, breaks the build if an un-Sync field ever sneaks in.
    #[test]
    fn registry_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Registry>();
    }

    /// Linearizability: N threads race `register` on ONE name with real
    /// overlap (OS threads + barrier). Exactly one wins; every loser observes
    /// `NameTaken`; lookup resolves the winner's actor — never a torn or lost
    /// registration.
    #[test]
    fn concurrent_register_single_winner_on_one_name() {
        const RACERS: u64 = 4;
        let registry = Registry::new();
        let pairs: Vec<_> = (0..RACERS).map(build::<Probe>).collect();
        let barrier = Barrier::new(pairs.len());

        let results: Vec<Result<(), NameTaken>> = thread::scope(|s| {
            let handles: Vec<_> = pairs
                .iter()
                .map(|(actor_ref, _rx)| {
                    s.spawn(|| {
                        barrier.wait();
                        registry.register("hot", actor_ref)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("no racer panics"))
                .collect()
        });

        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(winners, 1, "exactly one racer registers: {results:?}");
        assert!(
            results.iter().all(|r| matches!(r, Ok(()) | Err(NameTaken))),
            "every loser observes NameTaken: {results:?}",
        );

        let winner_idx = results
            .iter()
            .position(Result::is_ok)
            .expect("one winner exists");
        let resolved = registry
            .lookup::<Probe>("hot")
            .expect("same type")
            .expect("winner live");
        assert_eq!(
            resolved.id(),
            pairs[winner_idx].0.id(),
            "lookup resolves the winner, not a lost/torn registration",
        );
    }

    /// Linearizability over the reclaim path: a DEAD incumbent's name is raced
    /// by N claimants. The stale-replace decision is atomic — exactly one
    /// claim succeeds, and no interleaving double-replaces.
    #[test]
    fn concurrent_reclaim_of_dead_name_single_winner() {
        const RACERS: u64 = 4;
        let registry = Registry::new();
        let (dead, rx) = build::<Probe>(99);
        registry.register("seat", &dead).expect("fresh name");
        drop(dead);
        drop(rx);

        let pairs: Vec<_> = (0..RACERS).map(build::<Probe>).collect();
        let barrier = Barrier::new(pairs.len());

        let results: Vec<Result<(), NameTaken>> = thread::scope(|s| {
            let handles: Vec<_> = pairs
                .iter()
                .map(|(actor_ref, _rx)| {
                    s.spawn(|| {
                        barrier.wait();
                        registry.register("seat", actor_ref)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("no racer panics"))
                .collect()
        });

        let winners = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            winners, 1,
            "exactly one claimant reclaims the dead name: {results:?}",
        );

        let winner_idx = results
            .iter()
            .position(Result::is_ok)
            .expect("one winner exists");
        let resolved = registry
            .lookup::<Probe>("seat")
            .expect("same type")
            .expect("claimant live");
        assert_eq!(resolved.id(), pairs[winner_idx].0.id());
    }

    /// Lookup-during-register consistency: readers racing a register/unregister
    /// churn loop only ever observe absent or the ONE registrant — never a
    /// type error, never a foreign id, never a torn entry.
    #[test]
    fn concurrent_lookup_during_register_churn_is_consistent() {
        const ROUNDS: usize = 50;
        let registry = Registry::new();
        let (actor_ref, _rx) = build::<Probe>(7);
        let expected = actor_ref.id();

        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..ROUNDS {
                    registry
                        .register("churn", &actor_ref)
                        .expect("sole writer never conflicts with itself");
                    assert!(registry.unregister("churn"), "own entry removable");
                }
            });
            for _ in 0..2 {
                s.spawn(|| {
                    for _ in 0..ROUNDS {
                        match registry.lookup::<Probe>("churn") {
                            Ok(None) => {}
                            Ok(Some(seen)) => assert_eq!(
                                seen.id(),
                                expected,
                                "a reader may only see THE registrant",
                            ),
                            Err(err) => {
                                panic!("single-type churn can never type-conflict: {err}")
                            }
                        }
                    }
                });
            }
        });
    }
    /// Adversarial-name strategy: empty, Unicode, and up-to-512-char names —
    /// the rubric's "strings empty / max / max+1" proptest mandate.
    fn arb_name() -> impl proptest::strategy::Strategy<Value = String> {
        proptest::string::string_regex(".{0,512}").expect("valid regex")
    }

    proptest! {
        /// Defensive boundary (rubric category 3): arbitrary names must
        /// round-trip through register/lookup/unregister without panicking and
        /// without cross-contaminating — the unit suite only uses short ASCII
        /// names ("hot", "seat").
        #[test]
        fn prop_register_lookup_unregister_roundtrips_arbitrary_name(name in arb_name()) {
            let registry = Registry::new();
            let (actor_ref, _rx) = build::<Probe>(1);
            prop_assert!(registry.register(name.clone(), &actor_ref).is_ok());
            let resolved = registry
                .lookup::<Probe>(&name)
                .expect("same type")
                .expect("live actor resolves");
            prop_assert_eq!(resolved.id(), ActorId::new(1));
            prop_assert!(registry.unregister(&name));
            prop_assert!(
                registry.lookup::<Probe>(&name).expect("no type conflict").is_none(),
                "unregister frees the name for re-registration"
            );
        }

        /// Two distinct arbitrary names keep their separate incumbents — no
        /// key-collision or cross-contamination under adversarial inputs.
        #[test]
        fn prop_distinct_arbitrary_names_do_not_cross_contaminate(
            name_a in arb_name(),
            name_b in arb_name(),
        ) {
            prop_assume!(name_a != name_b);
            let registry = Registry::new();
            let (a, _rx_a) = build::<Probe>(1);
            let (b, _rx_b) = build::<Probe>(2);
            prop_assert!(registry.register(name_a.clone(), &a).is_ok());
            prop_assert!(registry.register(name_b.clone(), &b).is_ok());
            let ra = registry
                .lookup::<Probe>(&name_a)
                .expect("same type")
                .expect("a live");
            let rb = registry
                .lookup::<Probe>(&name_b)
                .expect("same type")
                .expect("b live");
            prop_assert_eq!(ra.id(), ActorId::new(1));
            prop_assert_eq!(rb.id(), ActorId::new(2));
        }
    }
}
