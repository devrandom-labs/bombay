//! Loom models over the real implementation (completion publication, unique
//! take races, and the blocking wait path).
//!
//! Runs with `RUSTFLAGS="--cfg loom" cargo test -p observe --lib
//! --release`; the frozen gate runs these with `LOOM_MAX_PREEMPTIONS=3`.

use loom::thread;
use loom::thread::yield_now;

use super::pair;

/// Direct-pair publication racing a read never exposes a torn or missing
/// completed outcome.
#[test]
fn pair_completion_racing_observation_never_loses_outcome() {
    loom::model(|| {
        let (publisher, observation) = pair::<u64>();
        let observer = thread::spawn(move || {
            loop {
                if let Some(outcome) = observation.try_get() {
                    return outcome;
                }
                yield_now();
            }
        });
        publisher.complete(42);
        assert_eq!(observer.join().unwrap(), 42);
    });
}

/// Every cloned blocking observation of a direct pair is notified.
#[test]
fn pair_completion_wakes_all_blocking_observers() {
    loom::model(|| {
        let (publisher, first) = pair::<u64>();
        let second = first.clone();
        let first = thread::spawn(move || first.wait());
        let second = thread::spawn(move || second.wait());
        yield_now();
        publisher.complete(9);
        assert_eq!(first.join().unwrap(), 9);
        assert_eq!(second.join().unwrap(), 9);
    });
}

/// A clone made after publication remains attached to the same retained fact.
#[test]
fn late_pair_clone_reads_retained_outcome() {
    loom::model(|| {
        let (publisher, observation) = pair::<u64>();
        publisher.complete(5);
        let late = observation.clone();
        assert_eq!(observation.try_get(), Some(5));
        assert_eq!(late.try_get(), Some(5));
    });
}

/// Fresh pairs are allocation-identical only within themselves: completing
/// one slot cannot change another slot's state.
#[test]
fn concurrent_pairs_are_isolated() {
    loom::model(|| {
        let (first_publisher, first) = pair::<u64>();
        let (second_publisher, second) = pair::<u64>();
        let completion = thread::spawn(move || first_publisher.complete(1));
        second_publisher.complete(2);
        completion.join().unwrap();
        assert_eq!(first.try_get(), Some(1));
        assert_eq!(second.try_get(), Some(2));
    });
}

/// Dropping the affine authority does not fabricate completion or affect a
/// separately created pair.
#[test]
fn incomplete_pair_drop_is_isolated_and_pending() {
    loom::model(|| {
        let (abandoned, pending) = pair::<u64>();
        let (publisher, completed) = pair::<u64>();
        let dropper = thread::spawn(move || drop(abandoned));
        publisher.complete(7);
        dropper.join().unwrap();
        assert_eq!(pending.try_get(), None);
        assert_eq!(completed.try_get(), Some(7));
    });
}

/// A waiter that registers after completion returns without parking.
#[test]
fn waiter_after_completion_returns_immediately() {
    loom::model(|| {
        let (publisher, observation) = pair::<u64>();
        publisher.complete(4);
        assert_eq!(observation.wait(), 4);
    });
}

/// `into_outcome` racing completion is never torn: it either sees the
/// pending state or the fully published outcome.
#[test]
fn into_outcome_racing_complete() {
    loom::model(|| {
        #[derive(Debug, PartialEq, Eq)]
        struct Handle(u64);
        let (publisher, observation) = pair::<Handle>();
        let last = observation.clone();
        let observer = thread::spawn(move || last.into_outcome());
        publisher.complete(Handle(9));
        drop(observation);
        // `complete` consumed the publisher; dropping every observation
        // handle reclaims the slot and the outcome.
        let result = observer.join().unwrap();
        assert!(result.is_none() || result == Some(Handle(9)));
    });
}

/// The wait_timeout registration protocol matches wait() under the model
/// (the clock is not modeled; the completion path is what matters).
#[test]
fn wait_timeout_wakes_with_outcome() {
    loom::model(|| {
        let (publisher, observation) = pair::<u64>();
        let observer =
            thread::spawn(move || observation.wait_timeout(std::time::Duration::from_secs(1)));
        yield_now();
        publisher.complete(9);
        assert_eq!(observer.join().unwrap(), Some(9));
    });
}
