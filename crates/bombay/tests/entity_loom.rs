//! Exhaustive small models of the directory's synchronization protocols.

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

#[test]
fn concurrent_claims_start_exactly_one_activation() {
    loom::model(|| {
        let phase = Arc::new(Mutex::new(false));
        let starts = Arc::new(AtomicUsize::new(0));
        let callers: Vec<_> = (0..2)
            .map(|_| {
                let phase = Arc::clone(&phase);
                let starts = Arc::clone(&starts);
                thread::spawn(move || {
                    let mut activating = phase.lock().unwrap();
                    if !*activating {
                        *activating = true;
                        starts.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for caller in callers {
            caller.join().unwrap();
        }
        assert!(*phase.lock().unwrap());
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    });
}

#[derive(Default)]
struct Admission {
    closed: bool,
    reservations: usize,
    fence_enqueued: bool,
}

#[test]
fn fence_follows_every_admitted_delivery_resolution() {
    loom::model(|| {
        let admission = Arc::new(Mutex::new(Admission::default()));
        let delivery = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || {
                let admitted = {
                    let mut state = admission.lock().unwrap();
                    if state.closed {
                        false
                    } else {
                        state.reservations += 1;
                        true
                    }
                };
                if admitted {
                    thread::yield_now();
                    let mut state = admission.lock().unwrap();
                    state.reservations -= 1;
                    if state.closed && state.reservations == 0 {
                        state.fence_enqueued = true;
                    }
                }
            })
        };
        let drain = {
            let admission = Arc::clone(&admission);
            thread::spawn(move || {
                let mut state = admission.lock().unwrap();
                state.closed = true;
                if state.reservations == 0 {
                    state.fence_enqueued = true;
                }
            })
        };
        delivery.join().unwrap();
        drain.join().unwrap();
        let state = admission.lock().unwrap();
        assert!(state.closed);
        assert_eq!(state.reservations, 0);
        assert!(state.fence_enqueued);
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

fn try_deliver(admission: &Arc<Mutex<Admission>>) -> bool {
    let admitted = {
        let mut state = admission.lock().unwrap();
        if state.closed {
            false
        } else {
            state.reservations += 1;
            true
        }
    };
    if admitted {
        thread::yield_now();
        let mut state = admission.lock().unwrap();
        state.reservations -= 1;
        if state.closed && state.reservations == 0 {
            state.fence_enqueued = true;
        }
    }
    admitted
}

#[test]
fn reservations_racing_drain_close_resolve_before_the_fence() {
    loom::model(|| {
        let admission = Arc::new(Mutex::new(Admission::default()));
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
            thread::spawn(move || {
                let mut state = admission.lock().unwrap();
                state.closed = true;
                if state.reservations == 0 {
                    state.fence_enqueued = true;
                }
            })
        };
        first.join().unwrap();
        second.join().unwrap();
        drain.join().unwrap();
        let state = admission.lock().unwrap();
        assert!(state.closed);
        assert_eq!(state.reservations, 0);
        assert!(state.fence_enqueued);
    });
}
