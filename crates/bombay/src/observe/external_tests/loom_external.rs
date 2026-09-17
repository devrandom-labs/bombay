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
use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc as StdArc;
use std::task::{Context, Poll, Wake, Waker};

use crate::observe::{affine_pair, pair};

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

/// The future path under the model: `block_on` parks on the loom-aware
/// waker; completion racing the poll must wake it with the outcome.
#[test]
fn loom_future_resolves_racing_completion() {
    builder().check(|| {
        let (publisher, observation) = pair::<u64>();

        let poller = thread::spawn(move || loom::future::block_on(observation.into_future()));
        let completer = thread::spawn(move || publisher.complete(42));

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

/// Two waiters on one slot, completion racing both registrations:
/// the drain must wake both, each with the exact outcome.
#[test]
fn loom_two_waiters_both_woken() {
    builder().check(|| {
        let (publisher, observation) = pair::<u64>();

        let waiter_a = {
            let observation = observation.clone();
            thread::spawn(move || observation.wait())
        };
        let waiter_b = thread::spawn(move || observation.wait());
        publisher.complete(7);

        assert_eq!(waiter_a.join().expect("waiter A panicked"), 7);
        assert_eq!(waiter_b.join().expect("waiter B panicked"), 7);
    });
}

/// wait_timeout's registration protocol under the model (no clock: the
/// timeout never fires, so this models the completed path only).
#[test]
fn loom_wait_timeout_completed_path() {
    builder().check(|| {
        let (publisher, observation) = pair::<u64>();

        let waiter =
            thread::spawn(move || observation.wait_timeout(core::time::Duration::from_secs(1)));
        publisher.complete(11);
        assert_eq!(waiter.join().expect("waiter panicked"), Some(11));
    });
}
