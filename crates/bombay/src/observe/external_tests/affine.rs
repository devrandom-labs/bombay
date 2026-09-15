//! Affine-pair contract: move-only outcomes, unique observation ownership,
//! exact cancellation, waker migration, abandonment, and destruction.

use std::cell::Cell;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Wake};

use crate::observe::affine_pair;
use crate::observe::test_support::{CountWake, poll_once};

#[derive(Debug, PartialEq, Eq)]
struct MoveOnly(String);

#[derive(Debug)]
struct NonCloneDrop {
    drops: Arc<AtomicUsize>,
    tag: u64,
}

impl NonCloneDrop {
    fn new(tag: u64) -> (Self, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        (
            Self {
                drops: Arc::clone(&drops),
                tag,
            },
            drops,
        )
    }
}

impl Drop for NonCloneDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn completed_affine_observation_moves_non_clone_outcome() {
    let (publisher, observation) = affine_pair();
    publisher.complete(MoveOnly("owned".to_owned()));
    let mut observation = Box::pin(observation);
    let (waker, _) = CountWake::waker();
    assert_eq!(
        poll_once(observation.as_mut(), &waker),
        Poll::Ready(MoveOnly("owned".to_owned()))
    );
}

#[test]
fn pending_affine_observation_wakes_and_moves_exact_outcome() {
    let (publisher, observation) = affine_pair();
    let mut observation = Box::pin(observation);
    let (waker, probe) = CountWake::waker();
    assert!(poll_once(observation.as_mut(), &waker).is_pending());

    publisher.complete(MoveOnly("terminal".to_owned()));
    assert_eq!(probe.count(), 1);
    assert_eq!(
        poll_once(observation.as_mut(), &waker),
        Poll::Ready(MoveOnly("terminal".to_owned()))
    );
}

#[test]
fn pending_affine_observation_can_move_between_polls() {
    let (publisher, mut observation) = affine_pair();
    let (waker, probe) = CountWake::waker();
    assert!(poll_once(Pin::new(&mut observation), &waker).is_pending());

    let mut moved = observation;
    publisher.complete(MoveOnly("moved".to_owned()));

    assert_eq!(probe.count(), 1);
    assert_eq!(
        poll_once(Pin::new(&mut moved), &waker),
        Poll::Ready(MoveOnly("moved".to_owned()))
    );
}

#[test]
fn affine_task_migration_replaces_stale_waker() {
    let (publisher, observation) = affine_pair::<MoveOnly>();
    let mut observation = Box::pin(observation);
    let (waker_a, probe_a) = CountWake::waker();
    let (waker_b, probe_b) = CountWake::waker();

    assert!(poll_once(observation.as_mut(), &waker_a).is_pending());
    assert!(poll_once(observation.as_mut(), &waker_b).is_pending());
    assert_eq!(
        Arc::strong_count(&probe_a),
        2,
        "migration retained the old waker in the future or slot"
    );
    publisher.complete(MoveOnly("migrated".to_owned()));

    assert_eq!(probe_a.count(), 0, "migrated-away waker fired");
    assert_eq!(probe_b.count(), 1, "current waker was not fired once");
    assert_eq!(
        poll_once(observation.as_mut(), &waker_b),
        Poll::Ready(MoveOnly("migrated".to_owned()))
    );
}

#[test]
fn cancelling_migrated_affine_observation_deregisters_current_waker() {
    let (publisher, observation) = affine_pair::<MoveOnly>();
    let mut observation = Box::pin(observation);
    let (waker_a, probe_a) = CountWake::waker();
    let (waker_b, probe_b) = CountWake::waker();

    assert!(poll_once(observation.as_mut(), &waker_a).is_pending());
    assert!(poll_once(observation.as_mut(), &waker_b).is_pending());
    assert_eq!(Arc::strong_count(&probe_a), 2);
    drop(observation);
    assert_eq!(
        Arc::strong_count(&probe_b),
        2,
        "cancellation retained the installed waker"
    );
    publisher.complete(MoveOnly("cancelled".to_owned()));

    assert_eq!(probe_a.count(), 0);
    assert_eq!(probe_b.count(), 0);
}

#[test]
fn publisher_abandonment_leaves_affine_observation_pending() {
    let (publisher, observation) = affine_pair::<MoveOnly>();
    let mut observation = Box::pin(observation);
    let (waker, probe) = CountWake::waker();
    assert!(poll_once(observation.as_mut(), &waker).is_pending());
    drop(publisher);
    assert!(poll_once(observation.as_mut(), &waker).is_pending());
    drop(observation);
    assert_eq!(
        Arc::strong_count(&probe),
        2,
        "abandoned observation retained its cancelled waker"
    );
    assert_eq!(probe.count(), 0);
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("deliberate affine waker panic");
    }
}

#[test]
fn panicking_waker_does_not_lose_affine_outcome() {
    let (outcome, drops) = NonCloneDrop::new(4);
    let (publisher, observation) = affine_pair();
    let mut observation = Box::pin(observation);
    let waker = Arc::new(PanicWake).into();
    assert!(poll_once(observation.as_mut(), &waker).is_pending());

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        publisher.complete(outcome);
    }));
    assert!(panic.is_err());

    let (waker, _) = CountWake::waker();
    let outcome = match poll_once(observation.as_mut(), &waker) {
        Poll::Ready(outcome) => outcome,
        Poll::Pending => panic!("published outcome was lost after waker panic"),
    };
    drop(observation);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(outcome);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn consumed_non_clone_outcome_drops_exactly_once() {
    let (outcome, drops) = NonCloneDrop::new(1);
    let (publisher, observation) = affine_pair();
    publisher.complete(outcome);
    let mut observation = Box::pin(observation);
    let (waker, _) = CountWake::waker();
    let outcome = match poll_once(observation.as_mut(), &waker) {
        Poll::Ready(outcome) => outcome,
        Poll::Pending => panic!("completed affine observation stayed pending"),
    };
    drop(observation);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(outcome.tag, 1);
    drop(outcome);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn completed_but_cancelled_outcome_drops_exactly_once() {
    let (outcome, drops) = NonCloneDrop::new(2);
    let (publisher, observation) = affine_pair();
    publisher.complete(outcome);
    drop(observation);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn completion_racing_cancellation_drops_outcome_exactly_once() {
    for round in 0..200_u64 {
        let (outcome, drops) = NonCloneDrop::new(round);
        let (publisher, observation) = affine_pair();
        let mut observation = Box::pin(observation);
        let (waker, _) = CountWake::waker();
        assert!(poll_once(observation.as_mut(), &waker).is_pending());

        let cancel = std::thread::spawn(move || drop(observation));
        let complete = std::thread::spawn(move || publisher.complete(outcome));
        cancel.join().expect("cancellation thread panicked");
        complete.join().expect("completion thread panicked");
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "round {round}: outcome was leaked or dropped more than once"
        );
    }
}

#[test]
fn affine_pair_crosses_threads_with_send_non_sync_outcome() {
    struct SendNotSync(Cell<u8>);

    let (publisher, observation) = affine_pair();
    std::thread::spawn(move || publisher.complete(SendNotSync(Cell::new(7))))
        .join()
        .expect("publisher thread panicked");
    let outcome = std::thread::spawn(move || {
        let mut observation = Box::pin(observation);
        let (waker, _) = CountWake::waker();
        match poll_once(observation.as_mut(), &waker) {
            Poll::Ready(outcome) => outcome,
            Poll::Pending => panic!("published outcome stayed pending"),
        }
    })
    .join()
    .expect("observation thread panicked");
    assert_eq!(outcome.0.get(), 7);
}

#[test]
fn affine_handles_retain_no_outcome_after_move() {
    let (outcome, drops) = NonCloneDrop::new(3);
    let (publisher, observation) = affine_pair();
    let slot_lifetime = Arc::clone(&drops);
    publisher.complete(outcome);
    let mut observation = Box::pin(observation);
    let (waker, _) = CountWake::waker();
    let moved = match poll_once(observation.as_mut(), &waker) {
        Poll::Ready(outcome) => outcome,
        Poll::Pending => panic!("published outcome stayed pending"),
    };
    drop(observation);
    assert_eq!(slot_lifetime.load(Ordering::SeqCst), 0);
    drop(moved);
    assert_eq!(slot_lifetime.load(Ordering::SeqCst), 1);
}
