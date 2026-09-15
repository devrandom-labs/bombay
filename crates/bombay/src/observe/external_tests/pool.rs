//! Pooled-slot and drop-count attacks: generation leakage through slot
//! recycling, stale waiter/state bits surviving `reset`, and exactly-once
//! outcome destruction across churn well beyond the pool capacity (128).

use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use crate::observe::ObservationSpace;
use crate::observe::test_support::{CountWake, DropProbe};

/// 300 generations (pool capacity is 128) each complete with a uniquely
/// counted probe and retire unobserved. Every probe must be destroyed
/// exactly once — no leak, no double drop — across reuse and pool overflow.
#[test]
fn pool_churn_drops_each_outcome_exactly_once() {
    const GENERATIONS: u64 = 300;
    let space = ObservationSpace::<u64, DropProbe>::new();
    let mut counters = Vec::new();
    for generation in 0..GENERATIONS {
        let (probe, counter) = DropProbe::new(generation);
        counters.push(counter);
        let mut subject = space.subject(generation).expect("fresh key");
        subject.complete(probe);
    } // each subject retires at the end of its iteration

    drop(space); // drain the pool: pooled slots drop their retained outcomes
    for (generation, counter) in counters.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "generation {generation} outcome dropped != 1 times"
        );
    }
}

/// Deterministic same-slot reuse (the pool is LIFO and single-threaded
/// here): a recycled slot must not leak the previous generation's COMPLETED
/// bit or outcome into the replacement.
#[test]
fn recycled_slot_never_leaks_previous_generation() {
    let space = ObservationSpace::<u32, u64>::new();
    let mut first = space.subject(7).expect("first registration succeeds");
    first.complete(111);
    drop(first); // unobserved: the completed slot is pooled

    let mut second = space.subject(7).expect("retired key is registrable");
    let obs = space.observe(&7).expect("replacement retained");
    assert_eq!(
        obs.try_get(),
        None,
        "replacement generation must start pending (stale COMPLETED leaked)"
    );
    second.complete(222);
    assert_eq!(obs.try_get(), Some(222));
}

/// An observation captured before retirement pins the OLD slot: the
/// replacement gets a different slot and the two generations stay isolated.
#[test]
fn pinned_observation_isolates_generations() {
    let space = ObservationSpace::<u32, u64>::new();
    let mut first = space.subject(9).expect("first registration succeeds");
    first.complete(111);
    let old = space.observe(&9).expect("completed generation observable");

    drop(first); // pinned by `old`: NOT pooled
    let mut second = space.subject(9).expect("retired key is registrable");
    let new = space.observe(&9).expect("replacement retained");
    assert_eq!(new.try_get(), None, "replacement starts pending");
    second.complete(222);

    assert_eq!(old.try_get(), Some(111), "old generation keeps its outcome");
    assert_eq!(new.try_get(), Some(222), "new generation sees only its own");
}

/// A slot whose previous generation collected a stale waiter registration
/// (a timed-out wait leaves HAS_WAITER set until deregistration; a dropped
/// direct registration leaves the waker) must be fully reset before reuse:
/// no dead waiter fires across generations and the replacement completes
/// normally.
#[test]
fn stale_waiter_slot_reuse_is_clean() {
    let space = ObservationSpace::<u32, u64>::new();
    let subject = space.subject(5).expect("first registration succeeds");
    let obs = space.observe(&5).expect("subject retained");
    // Leave HAS_WAITER set with an empty registry (zero-timeout wait).
    assert_eq!(obs.wait_timeout(Duration::ZERO), None);
    // Leave a direct waker registration with no owner behind.
    let (stale_waker, stale_probe) = CountWake::waker();
    assert!(!obs.register_waker(&stale_waker));
    drop(obs);
    drop(subject); // unobserved: pooled with a stale registration

    let mut second = space.subject(5).expect("retired key is registrable");
    let (waker, probe) = CountWake::waker();
    let obs2 = space.observe(&5).expect("replacement retained");
    assert!(!obs2.register_waker(&waker));
    second.complete(77);

    assert_eq!(
        stale_probe.count(),
        0,
        "a stale waiter must not fire across generations"
    );
    assert_eq!(probe.count(), 1, "the live waiter fires exactly once");
    assert_eq!(obs2.try_get(), Some(77));
}

/// Pool reuse across keys: a slot retired under key A and recycled for key
/// B carries nothing over, and key A is immediately registrable again.
#[test]
fn cross_key_pool_reuse_is_clean() {
    let space = ObservationSpace::<u32, u64>::new();
    let mut a = space.subject(1).expect("key A registrable");
    a.complete(10);
    drop(a); // pooled

    let mut b = space.subject(2).expect("key B registrable");
    let obs_b = space.observe(&2).expect("key B observable");
    assert_eq!(obs_b.try_get(), None, "no outcome leaks across keys");
    b.complete(20);
    assert_eq!(obs_b.try_get(), Some(20));

    let mut a2 = space.subject(1).expect("key A registrable again");
    a2.complete(30);
    assert_eq!(
        space.observe(&1).expect("key A observable").try_get(),
        Some(30)
    );
}

/// An observation that outlives its subject's retirement keeps the
/// published outcome, and `into_outcome` on the last handle moves it out
/// with exactly one total drop (the slot's destructor must skip it).
#[test]
fn orphaned_observation_keeps_and_moves_outcome_exactly_once() {
    let space = ObservationSpace::<u32, DropProbe>::new();
    let (probe, counter) = DropProbe::new(1);
    let mut subject = space.subject(3).expect("first registration succeeds");
    subject.complete(probe);
    let obs = space.observe(&3).expect("completed generation observable");
    drop(subject); // pinned by obs: not pooled

    drop(space);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "orphan keeps the outcome alive inside the pinned slot"
    );
    let outcome = obs.into_outcome().expect("last handle moves the outcome");
    assert_eq!(outcome.tag, 1);
    drop(outcome);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "outcome dropped exactly once (slot destructor must skip the moved value)"
    );
}

/// A generation retired without completing recycles cleanly: the
/// replacement starts pending and completes normally; an orphaned
/// observation of the uncompleted generation stays pending forever (it can
/// never fabricate an outcome).
#[test]
fn uncompleted_generation_recycles_without_fabricating() {
    let space = ObservationSpace::<u32, u64>::new();
    let subject = space.subject(8).expect("first registration succeeds");
    let orphan = space.observe(&8).expect("subject retained");
    drop(subject); // never completed; pinned by `orphan`

    let mut second = space.subject(8).expect("retired key is registrable");
    let obs = space.observe(&8).expect("replacement retained");
    assert_eq!(obs.try_get(), None);
    second.complete(55);
    assert_eq!(obs.try_get(), Some(55));
    assert_eq!(
        orphan.try_get(),
        None,
        "orphan of an uncompleted generation must stay pending"
    );
}

/// `into_outcome` transfers drop ownership: retire without pooling (live
/// observation), move the outcome out, recycle nothing — the probe drops
/// once when the moved value dies, and the slot's own drop adds nothing.
#[test]
fn into_outcome_then_full_teardown_drops_exactly_once() {
    let (probe, counter) = DropProbe::new(2);
    let outcome = {
        let space = ObservationSpace::<u32, DropProbe>::new();
        let mut subject = space.subject(4).expect("first registration succeeds");
        subject.complete(probe);
        let obs = space.observe(&4).expect("completed generation observable");
        drop(subject);
        // `space` drops here; obs pins the slot, so nothing pooled.
        obs.into_outcome().expect("last handle moves the outcome")
    };
    assert_eq!(outcome.tag, 2);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "moved outcome still live"
    );
    drop(outcome);
    assert_eq!(counter.load(Ordering::SeqCst), 1, "dropped exactly once");
}

/// The pool cap (128) bounds retention but must not break exactly-once
/// destruction for generations that overflow the cap while STILL observed:
/// overflowed slots with observers are freed on last handle, not pooled.
#[test]
fn pool_overflow_with_live_observers_drops_exactly_once() {
    const GENERATIONS: u32 = 200;
    let space = ObservationSpace::<u32, DropProbe>::new();
    let mut counters = Vec::new();
    let mut observations = Vec::new();
    for key in 0..GENERATIONS {
        let (probe, counter) = DropProbe::new(u64::from(key));
        counters.push(counter);
        let mut subject = space.subject(key).expect("fresh key");
        subject.complete(probe);
        observations.push(space.observe(&key).expect("observable"));
    } // subjects retire pinned by their observations: nothing pools

    drop(space);
    drop(observations);
    for (key, counter) in counters.iter().enumerate() {
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "key {key} outcome dropped != 1 times"
        );
    }
}

/// `into_outcome` racing a concurrent handle drop: two handles on a
/// completed generation, one thread drops its handle while the other
/// attempts the move. The move must succeed AT MOST once (never move out
/// of a shared slot), and the outcome is destroyed exactly once either
/// way — by the mover or by the slot's final drop.
#[test]
fn into_outcome_racing_handle_drop_moves_at_most_once() {
    for round in 0..2_000_u64 {
        let space = ObservationSpace::<u8, DropProbe>::new();
        let (probe, counter) = DropProbe::new(round);
        let mut subject = space.subject(1).expect("first registration succeeds");
        subject.complete(probe);
        let obs_a = space.observe(&1).expect("subject retained");
        let obs_b = space.observe(&1).expect("subject retained");
        drop(subject); // pinned by the two observations

        let barrier = std::sync::Arc::new(Barrier::new(2));
        let dropper = {
            let barrier = std::sync::Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                drop(obs_a);
            })
        };
        let mover = {
            let barrier = std::sync::Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                obs_b.into_outcome()
            })
        };
        dropper.join().expect("dropper panicked");
        let moved = mover.join().expect("mover panicked");
        if let Some(outcome) = &moved {
            assert_eq!(outcome.tag, round, "moved outcome carries its value");
        }
        drop(space);
        drop(moved);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "round {round}: outcome dropped != once across the race"
        );
    }
}

/// A moved-out outcome whose slot is later recycled (the moving handle was
/// the last reference, so the slot is pooled on retire... the move happens
/// BEFORE retire) must not be re-dropped by `reset`.
#[test]
fn recycled_after_into_outcome_never_redrops() {
    let space = ObservationSpace::<u32, DropProbe>::new();
    let (probe, counter) = DropProbe::new(3);
    let mut subject = space.subject(6).expect("first registration succeeds");
    subject.complete(probe);
    // Take the outcome while the subject still exists: clone the Arc path
    // is impossible via the public API, so observe-then-move needs the
    // subject gone. Retire first, keeping the observation.
    let obs = space.observe(&6).expect("completed generation observable");
    drop(subject); // pinned by obs: not pooled
    let outcome = obs.into_outcome().expect("last handle moves the outcome");
    drop(outcome);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    drop(space);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "OUTCOME_VALID was cleared by the move: no re-drop at teardown"
    );
}

/// A pending generation's waker registry dies with the last observation:
/// `into_outcome` returning `None` (the generation never completed)
/// consumes the slot, and a waker registered on it must never fire
/// afterward — the entry is destroyed with the slot, not leaked.
#[test]
fn pending_into_outcome_consumes_registered_waker_silently() {
    for round in 0..50_u64 {
        let space = ObservationSpace::<u8, u64>::new();
        let subject = space.subject(1).expect("first registration succeeds");
        let obs = space.observe(&1).expect("subject retained");
        let (waker, probe) = CountWake::waker();
        assert!(!obs.register_waker(&waker), "pending: registration stored");
        drop(subject); // retire without completing

        assert_eq!(
            obs.into_outcome(),
            None,
            "a pending generation has no outcome (round {round})"
        );
        // The observation, slot, and its waker entry are gone; nothing can
        // fire the waker now (the subject is retired, the slot consumed).
        assert_eq!(
            probe.count(),
            0,
            "a consumed pending slot must never fire its wakers (round {round})"
        );
    }
}

/// The completed twin: `register_waker`, then `complete` — the drain fires
/// the waker exactly once and empties the registry — then `into_outcome`
/// moves the outcome out. The fire and the take are independent and both
/// exact: the take must not re-fire the waker, and the slot's final drop
/// must not re-drop the moved outcome.
#[test]
fn completed_into_outcome_fires_waker_then_moves_outcome() {
    for round in 0..50_u64 {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(1).expect("first registration succeeds");
        let obs = space.observe(&1).expect("subject retained");
        let (waker, probe) = CountWake::waker();
        assert!(!obs.register_waker(&waker), "pending: registration stored");

        subject.complete(round);
        assert_eq!(
            probe.count(),
            1,
            "the drain must fire the registered waker exactly once (round {round})"
        );
        drop(subject); // retired; the generation outlives via obs

        assert_eq!(
            obs.into_outcome(),
            Some(round),
            "into_outcome must move the completed outcome (round {round})"
        );
        assert_eq!(
            probe.count(),
            1,
            "the take must not fire the waker again (round {round})"
        );
    }
}

/// Hash-map scale churn: thousands of STRING keys (a keyspace neither the
/// inline vector nor the small u8-key tests exercise) register/complete/
/// retire with counted probes, twice over the same keys. The promoted
/// hash map, the shared 128-slot pool, and the recycling path must keep
/// exactly-once destruction at scale, and every retired key must
/// re-register cleanly.
#[test]
fn hash_map_scale_churn_drops_exactly_once() {
    const KEYS: u64 = 3000;
    const ROUNDS: u64 = 2;

    let space = ObservationSpace::<String, DropProbe>::new();
    let counter = std::sync::Arc::new(AtomicUsize::new(0));
    let mut created = 0usize;

    for round in 0..ROUNDS {
        for key in 0..KEYS {
            let k = format!("scale-key-{key}");
            let mut subject = space.subject(k.clone()).expect("retired key re-registers");
            subject.complete(DropProbe::with_counter(round * KEYS + key, &counter));
            created += 1;
            // drop(subject): retire; the slot is pooled or recycled.
        }
    }
    drop(space);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "created {created} outcomes but observed {} drops at scale",
        counter.load(Ordering::SeqCst)
    );
}

/// The will_wake dedup under load: registering the SAME waker 1000 times
/// on one pending generation stays ONE entry — completion fires it exactly
/// once (never 1000 times), and the outcome resolves.
#[test]
fn same_waker_registered_1000_times_fires_once() {
    let space = ObservationSpace::<u32, u64>::new();
    let mut subject = space.subject(7).expect("first registration succeeds");
    let observation = space.observe(&7).expect("subject retained");
    let (waker, probe) = CountWake::waker();
    for _ in 0..1000 {
        assert!(
            !observation.register_waker(&waker),
            "repeated registration must stay deduped"
        );
    }
    subject.complete(99);
    assert_eq!(
        probe.count(),
        1,
        "a will_wake-deduped registration must fire exactly once"
    );
    assert_eq!(observation.try_get(), Some(99));
}

/// Every waiter of a pooled-and-recycled slot's NEW generation is woken
/// exactly once even when the slot saw heavy waiter traffic before.
#[test]
fn recycled_slot_waiter_traffic_stays_exactly_once() {
    let space = ObservationSpace::<u32, u64>::new();
    for round in 0..8_u32 {
        let mut subject = space.subject(round).expect("fresh key each round");
        let probes: Vec<_> = (0..4).map(|_| CountWake::waker()).collect();
        for (waker, _) in &probes {
            let obs = space.observe(&round).expect("subject retained");
            assert!(!obs.register_waker(waker));
        }
        subject.complete(u64::from(round));
        for (i, (_, probe)) in probes.iter().enumerate() {
            assert_eq!(
                probe.count(),
                1,
                "round {round} waiter {i} woken != 1 times"
            );
        }
    }
}
