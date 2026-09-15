//! Direct-pair contract: affine publication, retained fan-out, cancellation,
//! panic safety, destruction, and isolation without a keyed namespace.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::task::{Poll, Wake};
use std::time::Duration;

use crate::observe::pair;
use crate::observe::test_support::{CountWake, DropProbe, poll_once};

#[test]
fn pair_starts_pending_and_completion_is_retained() {
    let (publisher, observation) = pair::<u64>();
    assert_eq!(observation.try_get(), None);
    publisher.complete(41);
    assert_eq!(observation.try_get(), Some(41));
    assert_eq!(observation.try_get(), Some(41));
}

#[test]
fn observations_cloned_before_and_after_completion_share_one_fact() {
    let (publisher, first) = pair::<String>();
    let second = first.clone();
    publisher.complete("terminal".to_owned());
    let third = first.clone();
    for observation in [&first, &second, &third] {
        assert_eq!(observation.try_get().as_deref(), Some("terminal"));
    }
}

#[test]
fn many_blocking_observers_all_receive_the_outcome() {
    const OBSERVERS: usize = 16;
    let (publisher, observation) = pair::<u64>();
    let barrier = Arc::new(Barrier::new(OBSERVERS + 1));
    let waiters: Vec<_> = (0..OBSERVERS)
        .map(|_| {
            let observation = observation.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                observation.wait()
            })
        })
        .collect();
    barrier.wait();
    publisher.complete(77);
    for waiter in waiters {
        assert_eq!(waiter.join().unwrap(), 77);
    }
}

#[test]
fn completion_racing_wait_never_loses_the_outcome() {
    for _ in 0..200 {
        let (publisher, observation) = pair::<u64>();
        let waiter = std::thread::spawn(move || observation.wait());
        publisher.complete(9);
        assert_eq!(waiter.join().unwrap(), 9);
    }
}

#[test]
fn timeout_cancellation_does_not_change_other_observers() {
    let (publisher, timed) = pair::<u64>();
    let survivor = timed.clone();
    assert_eq!(timed.wait_timeout(Duration::from_millis(1)), None);
    publisher.complete(12);
    assert_eq!(survivor.wait(), 12);
}

#[test]
fn dropping_incomplete_publisher_leaves_observations_pending() {
    let (publisher, observation) = pair::<u64>();
    drop(publisher);
    assert_eq!(observation.try_get(), None);
    assert_eq!(observation.wait_timeout(Duration::from_millis(1)), None);
}

#[test]
fn dropping_one_observation_cannot_cancel_siblings() {
    let (publisher, cancelled) = pair::<u64>();
    let survivor = cancelled.clone();
    drop(cancelled);
    publisher.complete(88);
    assert_eq!(survivor.try_get(), Some(88));
}

#[test]
fn cancelled_future_waker_never_fires() {
    let (publisher, observation) = pair::<u64>();
    let (waker, probe) = CountWake::waker();
    let mut future = Box::pin(observation.into_future());
    assert_eq!(poll_once(future.as_mut(), &waker), Poll::Pending);
    drop(future);
    publisher.complete(1);
    assert_eq!(probe.count(), 0);
}

#[test]
fn cancelling_shared_waker_sibling_keeps_survivor_registered() {
    let (publisher, first) = pair::<u64>();
    let second = first.clone();
    let (waker, probe) = CountWake::waker();
    let mut first = Box::pin(first.into_future());
    let mut second = Box::pin(second.into_future());
    assert_eq!(poll_once(first.as_mut(), &waker), Poll::Pending);
    assert_eq!(poll_once(second.as_mut(), &waker), Poll::Pending);
    drop(first);
    publisher.complete(55);
    assert!(probe.count() >= 1);
    assert_eq!(poll_once(second.as_mut(), &waker), Poll::Ready(55));
}

#[test]
fn direct_and_future_waker_ownership_survive_sibling_cancellation() {
    let (publisher, direct) = pair::<u64>();
    let future_observation = direct.clone();
    let (waker, probe) = CountWake::waker();
    assert!(!direct.register_waker(&waker));
    let mut future = Box::pin(future_observation.into_future());
    assert_eq!(poll_once(future.as_mut(), &waker), Poll::Pending);
    drop(future);
    publisher.complete(21);
    assert_eq!(probe.count(), 1);
    assert_eq!(direct.try_get(), Some(21));
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("deliberate pair waker panic");
    }
}

#[test]
fn panicking_waker_does_not_strand_later_pair_waiters() {
    let (publisher, panicking) = pair::<u64>();
    let good = panicking.clone();
    assert!(!panicking.register_waker(&Arc::new(PanicWake).into()));
    let (good_waker, good_probe) = CountWake::waker();
    assert!(!good.register_waker(&good_waker));

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        publisher.complete(34);
    }));
    assert!(panic.is_err());
    assert_eq!(good_probe.count(), 1);
    assert_eq!(good.try_get(), Some(34));
}

#[test]
fn independent_pairs_cannot_leak_outcomes() {
    let (first_publisher, first) = pair::<u64>();
    let (second_publisher, second) = pair::<u64>();
    first_publisher.complete(1);
    assert_eq!(first.try_get(), Some(1));
    assert_eq!(second.try_get(), None);
    second_publisher.complete(2);
    assert_eq!(first.try_get(), Some(1));
    assert_eq!(second.try_get(), Some(2));
}

#[test]
fn completed_pair_outcome_is_destroyed_exactly_once() {
    let (probe, drops) = DropProbe::new(7);
    let (publisher, observation) = pair();
    let clone = observation.clone();
    publisher.complete(probe);
    drop(observation);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn pending_pair_has_no_outcome_to_destroy() {
    let (publisher, observation) = pair::<DropProbe>();
    drop(publisher);
    drop(observation);
}

#[test]
fn move_only_outcome_can_be_taken_by_last_observation() {
    #[derive(Debug, PartialEq, Eq)]
    struct MoveOnly(String);

    let (publisher, observation) = pair();
    publisher.complete(MoveOnly("owned".to_owned()));
    assert_eq!(
        observation.into_outcome(),
        Some(MoveOnly("owned".to_owned()))
    );
}

#[test]
fn shared_move_only_outcome_cannot_be_taken() {
    struct MoveOnly;

    let (publisher, observation) = pair();
    let survivor = observation.clone();
    publisher.complete(MoveOnly);
    assert!(observation.into_outcome().is_none());
    assert!(survivor.into_outcome().is_some());
}
