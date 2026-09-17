//! Exhaustive small models of the directory's synchronization protocols.

use std::num::NonZeroUsize;

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

struct ActivationClaim;

#[test]
fn concurrent_claims_start_exactly_one_activation() {
    loom::model(|| {
        let claim = Arc::new(Mutex::new(None));
        let starts = Arc::new(AtomicUsize::new(0));
        let callers: Vec<_> = (0..2)
            .map(|_| {
                let claim = Arc::clone(&claim);
                let starts = Arc::clone(&starts);
                thread::spawn(move || {
                    let mut claim = claim.lock().unwrap();
                    if claim.is_none() {
                        *claim = Some(ActivationClaim);
                        starts.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.join().unwrap();
        }
        assert!(claim.lock().unwrap().is_some());
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    });
}

#[derive(Clone, Copy)]
enum Admission {
    Open(usize),
    Draining(NonZeroUsize),
    Fenced,
}

struct Reservation;

impl Admission {
    fn reserve(&mut self) -> Option<Reservation> {
        match *self {
            Self::Open(reservations) => {
                *self = Self::Open(
                    reservations
                        .checked_add(1)
                        .expect("live delivery reservations fit in usize"),
                );
                Some(Reservation)
            }
            Self::Draining(_) | Self::Fenced => None,
        }
    }

    fn close(&mut self) {
        *self = match *self {
            Self::Open(reservations) => match NonZeroUsize::new(reservations) {
                Some(reservations) => Self::Draining(reservations),
                None => Self::Fenced,
            },
            Self::Draining(reservations) => Self::Draining(reservations),
            Self::Fenced => Self::Fenced,
        };
    }

    fn resolve(&mut self, _: Reservation) {
        *self = match *self {
            Self::Open(reservations) => Self::Open(
                reservations
                    .checked_sub(1)
                    .expect("only reserved deliveries may resolve"),
            ),
            Self::Draining(reservations) => match NonZeroUsize::new(reservations.get() - 1) {
                Some(reservations) => Self::Draining(reservations),
                None => Self::Fenced,
            },
            Self::Fenced => panic!("fenced admission has no live reservation"),
        };
    }
}

#[test]
fn fence_follows_every_admitted_delivery_resolution() {
    loom::model(|| {
        let admission = Arc::new(Mutex::new(Admission::Open(0)));
        let delivery = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || try_deliver(&admission))
        };
        let drain = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || admission.lock().unwrap().close())
        };
        delivery.join().unwrap();
        drain.join().unwrap();
        let state = admission.lock().unwrap();
        assert!(matches!(*state, Admission::Fenced));
    });
}

#[test]
fn delayed_removal_cannot_remove_a_replacement_binding() {
    loom::model(|| {
        let binding = Arc::new(Mutex::new(Some(1_usize)));
        let removal = {
            let binding = Arc::clone(&binding);
            thread::spawn(move || {
                thread::yield_now();
                let mut current = binding.lock().unwrap();
                if *current == Some(1) {
                    *current = None;
                }
            })
        };
        let replacement = {
            let binding = Arc::clone(&binding);
            thread::spawn(move || {
                *binding.lock().unwrap() = Some(2);
            })
        };
        removal.join().unwrap();
        replacement.join().unwrap();
        assert_eq!(*binding.lock().unwrap(), Some(2));
    });
}

#[test]
fn canceled_waiter_and_activation_completion_drop_command_once() {
    loom::model(|| {
        let waiter = Arc::new(Mutex::new(Some(1_usize)));
        let drops = Arc::new(AtomicUsize::new(0));
        let cancel = {
            let waiter = Arc::clone(&waiter);
            let drops = Arc::clone(&drops);
            thread::spawn(move || {
                if waiter.lock().unwrap().take().is_some() {
                    drops.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let activation = {
            let waiter = Arc::clone(&waiter);
            let drops = Arc::clone(&drops);
            thread::spawn(move || {
                if waiter.lock().unwrap().take().is_some() {
                    drops.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        cancel.join().unwrap();
        activation.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    });
}

fn try_deliver(admission: &Arc<Mutex<Admission>>) {
    let reservation = admission.lock().unwrap().reserve();
    if let Some(reservation) = reservation {
        thread::yield_now();
        admission.lock().unwrap().resolve(reservation);
    }
}

#[test]
fn reservations_racing_drain_close_resolve_before_the_fence() {
    loom::model(|| {
        let admission = Arc::new(Mutex::new(Admission::Open(0)));
        let first = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || try_deliver(&admission))
        };
        let second = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || try_deliver(&admission))
        };
        let drain = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || admission.lock().unwrap().close())
        };
        first.join().unwrap();
        second.join().unwrap();
        drain.join().unwrap();
        let state = admission.lock().unwrap();
        assert!(matches!(*state, Admission::Fenced));
    });
}
