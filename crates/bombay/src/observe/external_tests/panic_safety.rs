//! Panic-safety of the completion drain: a user waker may panic (the
//! `Wake` contract does not forbid it). One panicking waker must not
//! strand the other waiters of the same generation — a parked thread
//! waiter must still be unparked, and later wakers must still fire.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::{Poll, Wake};
use std::time::Duration;

use super::wait_for_thread_registrations;
use crate::observe::ObservationSpace;
use crate::observe::test_support::{CountWake, DropProbe};

/// A waker that panics when woken.
struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("user waker panicked");
    }
}

/// panicking-waker regression — minimal reproducer. Regression coverage.
///
/// Registration order: panicking waker FIRST, then a well-behaved waker,
/// then a parked thread waiter. Completion drains in registration order;
/// the correct behavior is that every waiter is woken despite the panic.
/// Today the panic aborts the drain: the later waker never fires and the
/// parked thread is never unparked.
#[test]
fn panicking_waker_must_not_strand_other_waiters() {
    let space = ObservationSpace::<u8, u64>::new();
    let mut subject = space.subject(1).expect("first registration succeeds");

    let obs1 = space.observe(&1).expect("subject retained");
    let readiness = obs1.register_waker(&std::task::Waker::from(Arc::new(PanicWake)));
    assert_eq!(readiness, Poll::Pending);

    let obs2 = space.observe(&1).expect("subject retained");
    let (good_waker, good_probe) = CountWake::waker();
    let readiness = obs2.register_waker(&good_waker);
    assert_eq!(readiness, Poll::Pending);

    let obs3 = space.observe(&1).expect("subject retained");
    let registration = obs3.clone();
    let (outcome_sender, outcome_receiver) = mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || {
        let outcome = obs3.wait();
        outcome_sender
            .send(outcome)
            .expect("outcome receiver remains live");
    });
    let handle = waiter.thread().clone();
    wait_for_thread_registrations(&registration, &[handle.id()]);

    // The completing thread itself must tolerate the panic path; catch it
    // here so the test can assert on the other waiters.
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        subject.complete(42);
    }));
    assert!(
        panic.is_err(),
        "the panicking waker must propagate its panic"
    );

    let outcome = match outcome_receiver.recv_timeout(Duration::from_secs(5)) {
        Ok(outcome) => outcome,
        Err(timeout) => {
            handle.unpark();
            waiter
                .join()
                .expect("waiter thread panicked during cleanup");
            panic!("a parked waiter was stranded by an earlier waker's panic: {timeout}");
        }
    };
    waiter.join().expect("waiter thread panicked");
    assert_eq!(outcome, 42);
    assert_eq!(
        good_probe.count(),
        1,
        "a later waker was skipped by an earlier waker's panic (count {})",
        good_probe.count()
    );
}

/// The inverse order: well-behaved waiters registered BEFORE the panicking
/// one must be unaffected (they are drained first).
#[test]
fn waiters_before_the_panicking_one_are_woken() {
    let space = ObservationSpace::<u8, u64>::new();
    let mut subject = space.subject(2).expect("first registration succeeds");

    let obs1 = space.observe(&2).expect("subject retained");
    let (good_waker, good_probe) = CountWake::waker();
    let readiness = obs1.register_waker(&good_waker);
    assert_eq!(readiness, Poll::Pending);

    let obs2 = space.observe(&2).expect("subject retained");
    let readiness = obs2.register_waker(&std::task::Waker::from(Arc::new(PanicWake)));
    assert_eq!(readiness, Poll::Pending);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        subject.complete(7);
    }));
    drop(panic);

    assert_eq!(
        good_probe.count(),
        1,
        "waiter registered before the panicking one must still fire"
    );
}

/// A timed waiter skipped by a panicking drain reads the published outcome
/// after an independent unpark. The wake is causal; deadline timing does not
/// determine the registration order.
#[test]
fn wait_timeout_waiter_recovers_after_panicking_drain() {
    for round in 0..20_u64 {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(1).expect("first registration succeeds");

        let obs_panic = space.observe(&1).expect("subject retained");
        let readiness = obs_panic.register_waker(&std::task::Waker::from(Arc::new(PanicWake)));
        assert_eq!(readiness, Poll::Pending);

        let observation = space.observe(&1).expect("subject retained");
        let registration = observation.clone();
        let waiter = std::thread::spawn(move || observation.wait_timeout(Duration::from_secs(5)));
        wait_for_thread_registrations(&registration, &[waiter.thread().id()]);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            subject.complete(42);
        }));
        assert!(
            panic.is_err(),
            "the waker's panic propagates (round {round})"
        );

        waiter.thread().unpark();
        let outcome = waiter.join().expect("waiter panicked");
        assert_eq!(
            outcome,
            Some(42),
            "a skipped timed waiter must recover the outcome (round {round})"
        );
    }
}

/// After a panicking drain, the outcome is still published and readable,
/// and the slot can be retired and recycled without further fallout
/// (drop counts stay exactly-once).
#[test]
fn state_after_panicking_drain_stays_consistent() {
    let space = ObservationSpace::<u8, DropProbe>::new();
    let (probe, counter) = DropProbe::new(1);
    let mut subject = space.subject(3).expect("first registration succeeds");

    let obs = space.observe(&3).expect("subject retained");
    let readiness = obs.register_waker(&std::task::Waker::from(Arc::new(PanicWake)));
    assert_eq!(readiness, Poll::Pending);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        subject.complete(probe);
    }));
    drop(panic);

    let (completion_waker, _) = crate::observe::test_support::CountWake::waker();
    let readiness = obs.register_waker(&completion_waker);
    assert_eq!(
        readiness,
        Poll::Ready(()),
        "outcome must be published even if the drain panicked"
    );
    drop(obs);
    drop(subject);
    drop(space);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "outcome dropped exactly once after a panicking drain"
    );
}
