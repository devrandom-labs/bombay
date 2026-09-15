//! Bounded Loom models over the real observe protocol, driven from the
//! external (public-API) side. The whole file compiles only under
//! `--cfg loom`; the normal gate sees an empty target.
//!
//! Run:
//!   RUSTFLAGS="--cfg loom" cargo test \
//!     --package observe-tests \
//!     --release --lib
//!
//! NOTE: under `--cfg loom` other targets could execute the loom-instrumented
//! module outside a model and panic; always select the isolated library target.

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc as StdArc;
use std::task::{Context, Poll, Wake, Waker};

use crate::observe::{ObservationSpace, affine_pair};

const DEFAULT_PREEMPTIONS: usize = 8;

fn builder() -> loom::model::Builder {
    let mut builder = loom::model::Builder::new();
    let preemptions = std::env::var("LOOM_MAX_PREEMPTIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PREEMPTIONS);
    builder.preemption_bound = Some(preemptions);
    builder
}

/// An observation captured anywhere across a retire/re-register boundary
/// must still resolve to exactly the outcome of the generation it
/// captured — never lost, never the other generation's value.
#[test]
fn loom_observe_across_retire_reregister_never_loses() {
    builder().check(|| {
        let space = ObservationSpace::<u8, u64>::new();
        let mut first = space.subject(1).expect("first registration succeeds");

        let observer = {
            let space = space.clone();
            thread::spawn(move || {
                // A vacancy window exists between retirement and
                // re-registration: spin through it.
                let observation = loop {
                    if let Ok(observation) = space.observe(&1) {
                        break observation;
                    }
                    thread::yield_now();
                };
                // Blocking wait: resolves to the captured generation's
                // outcome whichever one it is.
                observation.wait()
            })
        };

        first.complete(100);
        drop(first); // retire generation A

        let replacement = {
            let space = space.clone();
            thread::spawn(move || {
                // Spin until the key is vacant (retirement may not have
                // happened yet in this interleaving).
                let mut subject = loop {
                    if let Ok(subject) = space.subject(1) {
                        break subject;
                    }
                    thread::yield_now();
                };
                subject.complete(200);
                subject
            })
        };

        let outcome = observer.join().expect("observer panicked");
        assert!(
            outcome == 100 || outcome == 200,
            "observation resolved to an alien outcome: {outcome}"
        );
        drop(replacement.join().expect("replacement panicked"));
    });
}

/// A pooled slot (retired with no observers after its waiter drained) is
/// recycled for the next generation: the new waiter's registration races
/// the new completion on the SAME slot memory; it must resolve to the new
/// outcome, and nothing from the old generation may fire again.
#[test]
fn loom_pooled_slot_reuse_with_waiters() {
    builder().check(|| {
        let space = ObservationSpace::<u8, u64>::new();

        // Generation A: waiter + completion + retirement (slot pooled).
        let mut first = space.subject(1).expect("first registration succeeds");
        let waiter_a = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || observation.wait())
        };
        first.complete(100);
        assert_eq!(waiter_a.join().expect("waiter A panicked"), 100);
        drop(first);

        // Generation B reuses the pooled slot (single-slot pool, LIFO).
        let mut second = space.subject(1).expect("retired key registrable");
        let waiter_b = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || observation.wait())
        };
        second.complete(200);
        assert_eq!(waiter_b.join().expect("waiter B panicked"), 200);
    });
}

/// The future path under the model: `block_on` parks on the loom-aware
/// waker; completion racing the poll must wake it with the outcome.
#[test]
fn loom_future_resolves_racing_completion() {
    builder().check(|| {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(1).expect("first registration succeeds");

        let poller = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || loom::future::block_on(observation.into_future()))
        };
        let completer = thread::spawn(move || {
            subject.complete(42);
        });

        assert_eq!(poller.join().expect("poller panicked"), 42);
        completer.join().expect("completer panicked");
    });
}

#[derive(Debug, PartialEq, Eq)]
struct MoveOnly(u64);

/// Affine publication racing first poll wakes the task and transfers the
/// exact non-Clone outcome without relying on publisher `Arc` teardown.
#[test]
fn loom_affine_publication_racing_await_moves_outcome() {
    builder().check(|| {
        let (publisher, observation) = affine_pair();
        let poller = thread::spawn(move || loom::future::block_on(observation));
        let completer = thread::spawn(move || publisher.complete(MoveOnly(42)));

        assert_eq!(poller.join().expect("poller panicked"), MoveOnly(42));
        completer.join().expect("completer panicked");
    });
}

struct ModelWake(AtomicUsize);

impl Wake for ModelWake {
    fn wake(self: StdArc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Once migration finishes, completion can see only the replacement waker.
#[test]
fn loom_affine_migration_leaves_no_stale_waker() {
    builder().check(|| {
        let (publisher, observation) = affine_pair::<MoveOnly>();
        let mut observation = Box::pin(observation);
        let probe_a = StdArc::new(ModelWake(AtomicUsize::new(0)));
        let probe_b = StdArc::new(ModelWake(AtomicUsize::new(0)));
        let waker_a = Waker::from(StdArc::clone(&probe_a));
        let waker_b = Waker::from(StdArc::clone(&probe_b));

        assert!(poll_affine(observation.as_mut(), &waker_a).is_pending());
        assert!(poll_affine(observation.as_mut(), &waker_b).is_pending());
        publisher.complete(MoveOnly(7));

        assert_eq!(probe_a.0.load(Ordering::SeqCst), 0);
        assert_eq!(probe_b.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            poll_affine(observation.as_mut(), &waker_b),
            Poll::Ready(MoveOnly(7))
        );
    });
}

/// Cancellation removes the installed affine waker before later completion.
#[test]
fn loom_affine_cancellation_deregisters_waker() {
    builder().check(|| {
        let (publisher, observation) = affine_pair::<MoveOnly>();
        let mut observation = Box::pin(observation);
        let probe = StdArc::new(ModelWake(AtomicUsize::new(0)));
        let waker = Waker::from(StdArc::clone(&probe));
        assert!(poll_affine(observation.as_mut(), &waker).is_pending());
        drop(observation);
        publisher.complete(MoveOnly(9));
        assert_eq!(probe.0.load(Ordering::SeqCst), 0);
    });
}

struct ModelDrop(Arc<AtomicUsize>);

impl Drop for ModelDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Completion and cancellation may linearize in either order, but ownership
/// of the non-Clone outcome is destroyed exactly once.
#[test]
fn loom_affine_completion_racing_cancellation_drops_once() {
    builder().check(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let (publisher, observation) = affine_pair();
        let cancel = thread::spawn(move || drop(observation));
        let complete = {
            let drops = Arc::clone(&drops);
            thread::spawn(move || publisher.complete(ModelDrop(drops)))
        };
        cancel.join().expect("canceller panicked");
        complete.join().expect("completer panicked");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

fn poll_affine<O>(
    observation: Pin<&mut crate::observe::AffineObservation<O>>,
    waker: &Waker,
) -> Poll<O> {
    observation.poll(&mut Context::from_waker(waker))
}

/// Concurrent registration at one key: exactly one winner; the winner's
/// completion is observable by a waiter captured after the race.
#[test]
fn loom_concurrent_subject_one_winner_observable() {
    builder().check(|| {
        let space = Arc::new(ObservationSpace::<u8, u64>::new());

        let contender_a = {
            let space = Arc::clone(&space);
            thread::spawn(move || {
                space.subject(1).map(|mut s| {
                    s.complete(1);
                    s
                })
            })
        };
        let contender_b = {
            let space = Arc::clone(&space);
            thread::spawn(move || {
                space.subject(1).map(|mut s| {
                    s.complete(2);
                    s
                })
            })
        };

        let a = contender_a.join().expect("contender A panicked");
        let b = contender_b.join().expect("contender B panicked");
        let winners = u8::from(a.is_ok()) + u8::from(b.is_ok());
        assert_eq!(winners, 1, "exactly one subject must win the key");

        let outcome = space.observe(&1).expect("winner retained").wait();
        assert!(outcome == 1 || outcome == 2, "alien outcome {outcome}");
    });
}

/// Two waiters on one generation, completion racing both registrations:
/// the drain must wake both, each with the exact outcome.
#[test]
fn loom_two_waiters_both_woken() {
    builder().check(|| {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(1).expect("first registration succeeds");

        let waiter_a = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || observation.wait())
        };
        let waiter_b = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || observation.wait())
        };
        subject.complete(7);

        assert_eq!(waiter_a.join().expect("waiter A panicked"), 7);
        assert_eq!(waiter_b.join().expect("waiter B panicked"), 7);
    });
}

/// wait_timeout's registration protocol under the model (no clock: the
/// timeout never fires, so this models the completed path only).
#[test]
fn loom_wait_timeout_completed_path() {
    builder().check(|| {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subject = space.subject(1).expect("first registration succeeds");

        let waiter = {
            let observation = space.observe(&1).expect("generation live");
            thread::spawn(move || observation.wait_timeout(core::time::Duration::from_secs(1)))
        };
        subject.complete(11);
        assert_eq!(waiter.join().expect("waiter panicked"), Some(11));
    });
}

/// `into_outcome`'s exclusive take racing the subject's retirement and a
/// concurrent observer: the take either succeeds (the subject retired
/// first and no other observation holds the slot — then the generation is
/// no longer observable) or is refused (still shared — then the observer
/// resolves to the exact outcome). The value is never moved twice and
/// never from a wrong generation.
#[test]
fn loom_into_outcome_racing_retire() {
    builder().check(|| {
        let space = Arc::new(ObservationSpace::<u8, u64>::new());
        let mut subject = space.subject(1).expect("first registration succeeds");
        let taker_obs = space.observe(&1).expect("generation live");

        let taker = thread::spawn(move || taker_obs.into_outcome());
        let observer = {
            let space = Arc::clone(&space);
            thread::spawn(move || space.observe(&1).ok().map(|obs| obs.wait()))
        };
        subject.complete(9);
        drop(subject); // retire: the generation outlives via its observers

        let taken = taker.join().expect("taker panicked");
        let observed = observer.join().expect("observer panicked");
        // Every combination is legal: the take may succeed (subject retired
        // and no other handle was live at that instant), be refused (still
        // shared), and the observer may capture (resolving exactly 9) or
        // miss the retirement window. Both may even resolve sequentially —
        // the observer's wait returns and drops its handle, then the take
        // finds the single remaining reference. The invariants: no wrong
        // value ever moves, and a successful take is exclusive.
        match (taken, observed) {
            (Some(value), Some(seen)) => {
                assert_eq!(value, 9, "the take must move the exact outcome");
                assert_eq!(seen, 9, "an observer must resolve to the exact outcome");
            }
            (Some(value), None) => {
                assert_eq!(value, 9, "the take must move the exact outcome");
            }
            (None, Some(seen)) => {
                assert_eq!(seen, 9, "an observer must resolve to the exact outcome");
            }
            (None, None) => {
                // Refused take plus a missed capture window: the published
                // value was never read and is dropped with the slot
                // (exactly-once destruction asserted natively and under
                // Miri). Legal: into_outcome consumes its handle even on
                // refusal, and a refused take does not pin the generation.
            }
        }
    });
}

/// Promotion-boundary churn: five keys — INLINE_CAP is 4, so the fifth
/// live registration promotes the key table to its hash form, and key 4
/// registers after keys 0..=3 retired, so it pops any pooled slot
/// (cross-key slot reuse). One thread registers/completes/retires each
/// generation while another observes and waits. Values encode the key, so
/// a recycled slot delivering a stale generation's value, or any cross-key
/// mix-up under the promoted map, fails the tag check.
#[test]
fn loom_promotion_boundary_generation_isolation() {
    builder().check(|| {
        let space = Arc::new(ObservationSpace::<u8, u64>::new());

        let publisher = {
            let space = Arc::clone(&space);
            thread::spawn(move || {
                for key in 0..5_u8 {
                    let mut subject = space.subject(key).expect("vacant key registers");
                    subject.complete(u64::from(key) << 8);
                    // subject drops at iteration end: retirement.
                }
            })
        };
        let observer = {
            let space = Arc::clone(&space);
            thread::spawn(move || {
                for key in 0..5_u8 {
                    // The publisher may not have registered `key` yet
                    // (UnknownSubject is legal), or may have retired it
                    // already; whatever generation is captured must be
                    // exactly this key's.
                    if let Ok(observation) = space.observe(&key) {
                        let value = observation.wait();
                        assert_eq!(
                            value >> 8,
                            u64::from(key),
                            "observation of key {key} resolved to a foreign generation's value {value}"
                        );
                    }
                }
            })
        };

        publisher.join().expect("publisher panicked");
        observer.join().expect("observer panicked");
    });
}
