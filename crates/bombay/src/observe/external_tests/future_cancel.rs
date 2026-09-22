//! Adversarial cancellation: futures observing the same slot with
//! shared or migrated wakers. The frozen semantics require cancellation to
//! be safe — dropping one future must never disarm a distinct live future's
//! wakeup, and a cancelled future's waker must never fire after the drop.

use std::pin::Pin;
use std::task::Poll;

use crate::observe::pair;
use crate::observe::test_support::{CountWake, poll_once};

#[test]
fn pending_shared_observation_future_can_move_between_polls() {
    let (publisher, observation) = pair::<u64>();
    let mut future = observation.into_future();
    let (waker, probe) = CountWake::waker();
    assert!(poll_once(Pin::new(&mut future), &waker).is_pending());

    let mut moved = future;
    publisher.complete(8);

    assert_eq!(probe.count(), 1);
    assert_eq!(poll_once(Pin::new(&mut moved), &waker), Poll::Ready(8));
}

/// shared-waker cancellation regression — minimal reproducer. Regression coverage.
///
/// Two futures on two observations of the SAME slot are polled with
/// the same task waker (the `select!`/`join!` pattern: one task driving two
/// observation futures). Cancelling one must leave the other wakeable:
/// completion has to wake the shared waker so the surviving future is
/// re-polled.
#[test]
fn cancelled_sibling_future_keeps_survivor_wakeable() {
    let (publisher, observation) = pair::<u64>();
    let obs1 = observation.clone();
    let obs2 = observation.clone();
    let (waker, probe) = CountWake::waker();

    let mut f1 = Box::pin(obs1.into_future());
    let mut f2 = Box::pin(obs2.into_future());
    assert!(poll_once(f1.as_mut(), &waker).is_pending());
    assert!(poll_once(f2.as_mut(), &waker).is_pending());

    drop(f1); // cancel the sibling before completion
    publisher.complete(42);

    assert!(
        probe.count() >= 1,
        "completion must wake the surviving future's waker; got {} wakes",
        probe.count()
    );
    assert_eq!(poll_once(f2.as_mut(), &waker), Poll::Ready(42));
}

/// shared-waker cancellation regression adjacent: same topology with three futures — cancelling any
/// one still disarms the survivors (the registration is not reference
/// counted).
#[test]
fn cancelled_siblings_leave_last_survivor_wakeable() {
    let (publisher, observation) = pair::<u64>();
    let (waker, probe) = CountWake::waker();

    let mut futures: Vec<_> = (0..3)
        .map(|_| {
            let obs = observation.clone();
            Box::pin(obs.into_future())
        })
        .collect();
    for f in &mut futures {
        assert!(poll_once(f.as_mut(), &waker).is_pending());
    }
    drop(futures.remove(0));
    drop(futures.remove(0));

    publisher.complete(9);
    assert!(
        probe.count() >= 1,
        "completion must wake the surviving future's waker; got {} wakes",
        probe.count()
    );
    assert_eq!(poll_once(futures[0].as_mut(), &waker), Poll::Ready(9));
}

/// A cancelled future's waker must not fire after the drop: single future,
/// distinct waker, dropped before completion. The drain at completion must
/// find no registration for it.
#[test]
fn cancelled_future_waker_never_fires() {
    let (publisher, observation) = pair::<u64>();
    let (waker, probe) = CountWake::waker();

    {
        let obs = observation.clone();
        let mut f = Box::pin(obs.into_future());
        assert!(poll_once(f.as_mut(), &waker).is_pending());
    } // cancel

    publisher.complete(1);
    assert_eq!(
        probe.count(),
        0,
        "a cancelled future's waker must never fire"
    );
}

/// A future that migrates between executors (polled with waker A, then with
/// a different waker B) and is then cancelled must never fire either waker.
#[test]
fn cancelled_migrated_future_fires_neither_waker() {
    let (publisher, observation) = pair::<u64>();
    let (waker_a, probe_a) = CountWake::waker();
    let (waker_b, probe_b) = CountWake::waker();

    {
        let obs = observation.clone();
        let mut f = Box::pin(obs.into_future());
        assert!(poll_once(f.as_mut(), &waker_a).is_pending());
        assert!(poll_once(f.as_mut(), &waker_b).is_pending());
    } // cancel after migration

    publisher.complete(5);
    assert_eq!(probe_a.count(), 0, "pre-migration waker must not fire");
    assert_eq!(probe_b.count(), 0, "post-migration waker must not fire");
}

/// Two INDEPENDENT futures (different wakers) on the same slot:
/// cancelling one must leave the other's registration intact; completion
/// wakes exactly the survivor's waker.
#[test]
fn cancel_one_of_two_independent_futures_wakes_only_survivor() {
    let (publisher, observation) = pair::<u64>();
    let (waker_a, probe_a) = CountWake::waker();
    let (waker_b, probe_b) = CountWake::waker();

    let obs_a = observation.clone();
    let obs_b = observation.clone();
    let mut fa = Box::pin(obs_a.into_future());
    let mut fb = Box::pin(obs_b.into_future());
    assert!(poll_once(fa.as_mut(), &waker_a).is_pending());
    assert!(poll_once(fb.as_mut(), &waker_b).is_pending());

    drop(fa);
    publisher.complete(77);

    assert_eq!(probe_a.count(), 0, "cancelled future's waker must not fire");
    assert!(
        probe_b.count() >= 1,
        "surviving future's waker must fire at completion"
    );
    assert_eq!(poll_once(fb.as_mut(), &waker_b), Poll::Ready(77));
}

/// Cancellation after the outcome is published is unobservable: the future
/// resolves normally and its waker count is irrelevant.
#[test]
fn cancel_after_completion_is_unobservable() {
    let (publisher, observation) = pair::<u64>();
    let (waker, _probe) = CountWake::waker();

    let mut f = Box::pin(observation.into_future());
    assert!(poll_once(f.as_mut(), &waker).is_pending());
    publisher.complete(23);
    assert_eq!(poll_once(f.as_mut(), &waker), Poll::Ready(23));
    drop(f);
}

/// The same waker registered TWICE — via two observations of one
/// slot (which share the registry) — must be
/// deduplicated by `will_wake` identity: one entry, exactly one fire at
/// completion, and both observations resolve.
#[test]
fn same_waker_registered_twice_fires_once() {
    for round in 0..50_u64 {
        let (publisher, observation) = pair::<u64>();
        let obs_a = observation.clone();
        let obs_b = observation.clone();
        let (waker, probe) = CountWake::waker();

        assert!(
            !obs_a.register_waker(&waker),
            "pending: registration stored"
        );
        assert!(
            !obs_b.register_waker(&waker),
            "deduped re-registration still reports pending (round {round})"
        );
        publisher.complete(round);

        assert_eq!(
            probe.count(),
            1,
            "a will_wake-deduped registration must fire exactly once (round {round})"
        );
        assert_eq!(obs_a.try_get(), Some(round));
        assert_eq!(obs_b.try_get(), Some(round));
    }
}

/// A future polled with MANY distinct wakers (a pathological executor
/// rotating wakers every poll) retains only the current registration.
/// Completion fires the latest waker exactly once and no migrated-away waker.
#[test]
fn many_distinct_wakers_leave_only_latest_registered() {
    const POLLS: usize = 50;
    let (publisher, observation) = pair::<u64>();

    let mut f = Box::pin(observation.into_future());
    let mut probes = Vec::new();
    for _ in 0..POLLS {
        let (waker, probe) = CountWake::waker();
        assert!(poll_once(f.as_mut(), &waker).is_pending());
        probes.push(probe);
    }
    publisher.complete(99);
    for (i, probe) in probes[..POLLS - 1].iter().enumerate() {
        assert_eq!(probe.count(), 0, "migrated-away waker {i} fired");
    }
    assert_eq!(
        probes[POLLS - 1].count(),
        1,
        "latest waker did not fire once"
    );
    assert_eq!(
        poll_once(f.as_mut(), &CountWake::waker().0),
        Poll::Ready(99)
    );
    drop(f);
}

/// shared-waker cancellation regression adjacent through the public `register_waker` API: a direct
/// waker registration has no owning future at all, yet a future sharing the
/// waker removes it on drop. The direct registrant is never woken.
#[test]
fn cancelled_future_steals_direct_waker_registration() {
    let (publisher, observation) = pair::<u64>();
    let obs1 = observation.clone();
    let obs2 = observation.clone();
    let (waker, probe) = CountWake::waker();

    assert!(!obs1.register_waker(&waker), "pending: registration stored");
    let mut f2 = Box::pin(obs2.into_future());
    assert!(poll_once(f2.as_mut(), &waker).is_pending());
    drop(f2);

    publisher.complete(43);
    assert!(
        probe.count() >= 1,
        "the direct registration must fire at completion; got {} wakes",
        probe.count()
    );
}

/// A re-polled survivor heals itself: after the sibling's cancellation, if
/// the executor re-polls the survivor for any reason before completion, the
/// registration must be restored and completion must wake it.
#[test]
fn repolled_survivor_heals_registration() {
    let (publisher, observation) = pair::<u64>();
    let (waker, probe) = CountWake::waker();

    let obs1 = observation.clone();
    let obs2 = observation.clone();
    let mut f1 = Box::pin(obs1.into_future());
    let mut f2 = Box::pin(obs2.into_future());
    assert!(poll_once(f1.as_mut(), &waker).is_pending());
    assert!(poll_once(f2.as_mut(), &waker).is_pending());
    drop(f1);

    // Executor re-polls the survivor for an unrelated reason.
    assert!(poll_once(f2.as_mut(), &waker).is_pending());
    publisher.complete(31);
    assert!(
        probe.count() >= 1,
        "re-polled survivor must be woken at completion"
    );
}

/// The same task waker on two futures over TWO DIFFERENT pairs (a
/// `join!` over two publishers). Each pair owns its own slot and its
/// own waiter registry, so shared-waker cancellation regression's shared-entry mechanism (one
/// observation, N futures, one deduped registration) cannot apply here:
/// cancelling either future must leave the other's registry entry intact,
/// and completing the survivor's slot fires the shared waker exactly
/// once. Note two observations of the SAME slot share one registry, so
/// that variant is shared-waker cancellation regression (covered by its own reproducers).
#[test]
fn same_waker_across_two_pairs_survivor_resolves() {
    for round in 0..50_u64 {
        let (publisher_a, observation_a) = pair::<u64>();
        let (publisher_b, observation_b) = pair::<u64>();
        let (waker, probe) = CountWake::waker();

        let mut fa = Box::pin(observation_a.into_future());
        let mut fb = Box::pin(observation_b.into_future());
        assert!(poll_once(fa.as_mut(), &waker).is_pending());
        assert!(poll_once(fb.as_mut(), &waker).is_pending());

        drop(fa); // cancel one arm of the join
        publisher_b.complete(round);

        assert_eq!(
            probe.count(),
            1,
            "shared waker fired != once (round {round}): per-slot entries must drain exactly once"
        );
        assert_eq!(poll_once(fb.as_mut(), &waker), Poll::Ready(round));

        // Retire both pairs cleanly. Completing A fires nothing:
        // fa's registration was deregistered on drop.
        publisher_a.complete(round);
    }
}
