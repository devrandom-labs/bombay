//! Documented-contract defenses: panics, error payloads, and edge
//! behaviors the public API promises. These guard the contract the
//! adversarial tests rely on.

use std::time::Duration;

use crate::observe::{ObservationSpace, SubjectExists, UnknownSubject};

/// Double completion panics — the exactly-once publication contract is
/// enforced, not assumed.
#[test]
fn double_complete_panics() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(1).expect("first registration succeeds");
    subject.complete(1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        subject.complete(2);
    }));
    assert!(panic.is_err(), "second completion must panic");
    // The first outcome survives the attempted second publication.
    assert_eq!(space.observe(&1).expect("retained").try_get(), Some(1));
}

/// Registration conflict returns the conflicting key in the error.
#[test]
fn subject_exists_error_carries_key() {
    let space = ObservationSpace::<u8, u8>::new();
    let _subject = space.subject(7).expect("first registration succeeds");
    let error = match space.subject(7) {
        Ok(_) => panic!("second registration must conflict"),
        Err(error) => error,
    };
    assert_eq!(error, SubjectExists(7));
}

/// Observing a vacant key returns the key in the error.
#[test]
fn unknown_subject_error_carries_key() {
    let space = ObservationSpace::<u8, u8>::new();
    let error = match space.observe(&9) {
        Ok(_) => panic!("vacant key must be unobservable"),
        Err(error) => error,
    };
    assert_eq!(error, UnknownSubject(9));
}

/// `wait_timeout` panics when the deadline overflows `Instant`
/// (documented), and never returns a fabricated outcome before doing so.
#[test]
fn wait_timeout_overflowing_deadline_panics() {
    let space = ObservationSpace::<u8, u8>::new();
    let _subject = space.subject(3).expect("first registration succeeds");
    let observation = space.observe(&3).expect("subject retained");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        observation.wait_timeout(Duration::MAX)
    }));
    assert!(panic.is_err(), "overflowing deadline must panic");
}

/// A completed observation with a huge-but-valid timeout returns the
/// outcome immediately.
#[test]
fn wait_timeout_huge_on_completed_returns_outcome() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(4).expect("first registration succeeds");
    subject.complete(44);
    let observation = space.observe(&4).expect("subject retained");
    assert_eq!(
        observation.wait_timeout(Duration::from_secs(3600)),
        Some(44)
    );
}

/// A truly move-only outcome (no `Clone`): only `into_outcome` can read
/// it, exactly once, by the last handle.
#[test]
fn move_only_outcome_flows_through_into_outcome() {
    struct MoveOnly(String);

    let space = ObservationSpace::<u8, MoveOnly>::new();
    let mut subject = space.subject(5).expect("first registration succeeds");
    subject.complete(MoveOnly("payload".to_string()));
    let observation = space.observe(&5).expect("subject retained");
    drop(subject);
    let outcome = observation.into_outcome().expect("last handle moves it");
    assert_eq!(outcome.0, "payload");
}

/// `into_outcome` on a pending generation consumes the handle and yields
/// nothing — no outcome is fabricated and the slot stays pending for its
/// other handles.
#[test]
fn into_outcome_pending_yields_nothing() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(6).expect("first registration succeeds");
    let first = space.observe(&6).expect("subject retained");
    let second = space.observe(&6).expect("subject retained");
    // Two handles: not exclusive — must refuse even after completion.
    subject.complete(66);
    assert_eq!(
        first.into_outcome(),
        None,
        "shared slot must refuse the move"
    );
    assert_eq!(second.try_get(), Some(66));
}

/// A zero timeout on a completed observation returns the outcome
/// immediately — the deadline is never consulted once the outcome is
/// published.
#[test]
fn wait_timeout_zero_on_completed_returns_outcome() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(4).expect("first registration succeeds");
    subject.complete(41);
    let observation = space.observe(&4).expect("subject retained");
    assert_eq!(observation.wait_timeout(Duration::ZERO), Some(41));
}

/// `register_waker` on a completed-and-RETIRED observation returns `true`
/// (already published) without registering: the slot keeps its COMPLETED
/// bit across retirement while any observation pins it.
#[test]
fn register_waker_true_on_retired_completed() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(4).expect("first registration succeeds");
    subject.complete(42);
    let observation = space.observe(&4).expect("completed generation observable");
    drop(subject); // retire; the observation pins the completed slot
    let (waker, probe) = crate::observe::test_support::CountWake::waker();
    assert!(
        observation.register_waker(&waker),
        "completed: nothing registered"
    );
    assert_eq!(observation.try_get(), Some(42));
    assert_eq!(probe.count(), 0, "no registration, no wake");
}

/// `register_waker` returning `true` (already published — nothing
/// registered) must not interfere with a later `into_outcome` take: the
/// value still moves out exactly once by the last handle.
#[test]
fn register_waker_true_then_into_outcome_takes() {
    for round in 0..50_u64 {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(4).expect("first registration succeeds");
        subject.complete(round);
        let observation = space.observe(&4).expect("completed generation observable");
        let (waker, probe) = crate::observe::test_support::CountWake::waker();
        assert!(
            observation.register_waker(&waker),
            "completed: nothing registered"
        );
        drop(subject); // retire; the observation is the last handle
        assert_eq!(
            observation.into_outcome(),
            Some(round),
            "a true-return registration must not block the take (round {round})"
        );
        assert_eq!(probe.count(), 0, "nothing was registered, nothing fires");
    }
}

/// A blocking `wait` on an already-completed observation returns
/// immediately, and can be repeated: waiting is idempotent on completion.
#[test]
fn wait_twice_on_completed_returns_immediately() {
    let space = ObservationSpace::<u8, u8>::new();
    let mut subject = space.subject(4).expect("first registration succeeds");
    subject.complete(43);
    let observation = space.observe(&4).expect("subject retained");
    assert_eq!(observation.wait(), 43);
    assert_eq!(observation.wait(), 43);
}

/// Cloning the space shares the namespace: conflicts, observations, and
/// completions are visible across clones, and dropping one clone changes
/// nothing.
#[test]
fn cloned_space_shares_namespace() {
    let space = ObservationSpace::<u8, u8>::new();
    let clone = space.clone();
    let mut subject = clone.subject(8).expect("first registration succeeds");
    assert!(space.subject(8).is_err(), "conflict visible across clones");
    drop(space);
    subject.complete(88);
    assert_eq!(
        clone.observe(&8).expect("retained").try_get(),
        Some(88),
        "completion visible after the other clone dropped"
    );
}
