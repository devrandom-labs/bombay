//! Deterministic adversarial stress: real threads, barrier-synchronized,
//! fixed-seed SplitMix64 op selection. Publishers complete independent
//! pairs; observers hammer try_get/wait/wait_timeout/waker cancellation.
//! Every value read is tag-checked against the pair that published
//! it (no cross-pair leakage), and every blocking wait must return
//! (publishers complete every pair before the run ends).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::Wake;
use std::thread;
use std::time::Duration;

use crate::observe::test_support::{CountWake, ThreadWake};
use crate::observe::{Observation, pair};

/// SplitMix64: deterministic, seedable, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Outcome encoding: publisher tag in the high byte, pair index in the
/// next word, round in the low word. Cross-pair leakage breaks the tag
/// check (a read on pair `p` must carry `p`'s index and owner).
fn encode(publisher: u64, pair_index: u64, round: u64) -> u64 {
    (publisher << 56) | ((pair_index & 0xFFFF) << 32) | (round & 0xFFFF_FFFF)
}

fn pair_index_of(outcome: u64) -> u64 {
    (outcome >> 32) & 0xFFFF
}

fn publisher_of(outcome: u64) -> u64 {
    outcome >> 56
}

/// 64 pairs, 2 publishers (one per even/odd pair index) and 4 observer
/// threads all released by one barrier; observers pick pairs at random and
/// run mixed read/wait/cancel ops until every pair is complete. Value
/// integrity only; the run is deterministic in op selection (not in
/// interleaving).
#[test]
fn stress_publishers_observers_value_integrity() {
    const PAIRS: u64 = 64;
    const PUBLISHERS: u64 = 2;
    const OBSERVERS: u64 = 4;

    // One publisher handle and one observation handle per pair. Pair 0 is
    // the seed: completed here, before any thread exists, so every
    // observer has a GUARANTEED first read even under extreme
    // descheduling (the keyed corpus relied on the same retained-seed
    // trick).
    let pairs: Vec<_> = (0..PAIRS).map(|_| pair::<u64>()).collect();
    let observations: Arc<Vec<Observation<u64>>> = Arc::new(
        pairs.iter().map(|(_, observation)| observation.clone()).collect(),
    );
    let mut publisher_slots: Vec<Option<_>> =
        pairs.into_iter().map(|(publisher, _)| Some(publisher)).collect();
    publisher_slots[0]
        .take()
        .expect("seed publisher")
        .complete(u64::MAX); // distinctive: publishers never emit 0xFF_..
    // Each publisher thread owns the pairs whose index it parity-matches;
    // the Vec<Option<_>> split hands each one its own handles.
    let mut by_publisher: Vec<Vec<(u64, _)>> =
        (0..PUBLISHERS).map(|_| Vec::new()).collect();
    for (index, publisher) in publisher_slots.into_iter().enumerate() {
        if let Some(publisher) = publisher {
            by_publisher[index % PUBLISHERS as usize].push((index as u64, publisher));
        }
    }
    // Set once every publisher finishes: bounds the observers' loop.
    let done = Arc::new(AtomicBool::new(false));

    let barrier = Arc::new(Barrier::new((PUBLISHERS + OBSERVERS) as usize));
    let publishers: Vec<_> = (0..PUBLISHERS)
        .map(|id| {
            let mut owned = std::mem::take(&mut by_publisher[id as usize]);
            let barrier = Arc::clone(&barrier);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut rng = Rng(0xA11C_E000 + id);
                barrier.wait();
                // Fisher-Yates over the owned pairs: the completion order
                // is rng-chosen, the set is fixed.
                for i in (1..owned.len()).rev() {
                    let j = rng.below(i as u64 + 1) as usize;
                    owned.swap(i, j);
                }
                for (round, (pair_index, publisher)) in owned.into_iter().enumerate() {
                    publisher.complete(encode(id, pair_index, round as u64));
                }
                done.store(true, Ordering::SeqCst);
            })
        })
        .collect();

    let observers: Vec<_> = (0..OBSERVERS)
        .map(|id| {
            let observations = Arc::clone(&observations);
            let barrier = Arc::clone(&barrier);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                let mut rng = Rng(0x0B5E_7000 + id);
                barrier.wait();
                let mut reads = 0_u64;
                // Guaranteed first read: the completed seed pair is
                // retained for the whole run.
                assert_eq!(
                    observations[0].wait(),
                    u64::MAX,
                    "seed pair must resolve to its value"
                );
                reads += 1;
                // Loop until every pair is complete: every captured pair
                // completes before the publishers retire, so the blocking
                // `wait` arm below is guaranteed to return.
                while !done.load(Ordering::Relaxed) {
                    let pair_index = rng.below(PAIRS) as usize;
                    let observation = observations[pair_index].clone();
                    match rng.below(4) {
                        0 => {
                            if let Some(outcome) = observation.try_get() {
                                assert_eq!(
                                    pair_index_of(outcome),
                                    pair_index as u64,
                                    "try_get leaked across pairs"
                                );
                                reads += 1;
                            }
                        }
                        1 => {
                            if let Some(outcome) =
                                observation.wait_timeout(Duration::from_millis(50))
                            {
                                assert_eq!(
                                    pair_index_of(outcome),
                                    pair_index as u64,
                                    "wait_timeout leaked across pairs"
                                );
                                reads += 1;
                            }
                        }
                        2 => {
                            // Cancellation storm: register and immediately drop.
                            let (waker, _probe) = CountWake::waker();
                            let _ = observation.register_waker(&waker);
                        }
                        _ => {
                            // Blocking wait with a safety net: every captured
                            // pair completes before its publisher retires, so
                            // this must return.
                            let outcome = observation.wait();
                            assert_eq!(
                                pair_index_of(outcome),
                                pair_index as u64,
                                "wait leaked across pairs"
                            );
                            assert_eq!(publisher_of(outcome) % 2, pair_index as u64 % 2,
                                "outcome publisher does not own the pair");
                            reads += 1;
                        }
                    }
                }
                reads
            })
        })
        .collect();

    barrier.wait();
    for publisher in publishers {
        publisher.join().expect("publisher panicked");
    }
    let total_reads: u64 = observers
        .into_iter()
        .map(|observer| observer.join().expect("observer panicked"))
        .sum();
    assert!(total_reads > 0, "stress run observed no completions at all");
}

/// Fanout storm: 8 waiters observe the same pair; one publisher
/// completes it. Every waiter must wake with the exact outcome, 200 rounds
/// with a fresh pair each round. Barriers make the registration race
/// real: waiters start waiting while the publisher may already have
/// completed.
#[test]
fn stress_waiter_fanout_exact_outcome() {
    const WAITERS: usize = 8;
    const ROUNDS: u64 = 200;

    let failures = Arc::new(AtomicUsize::new(0));

    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        let start = Arc::new(Barrier::new(WAITERS + 1));
        let waiters: Vec<_> = (0..WAITERS)
            .map(|_| {
                let observation = observation.clone();
                let start = Arc::clone(&start);
                let failures = Arc::clone(&failures);
                thread::spawn(move || {
                    start.wait();
                    let outcome = observation.wait();
                    if outcome != round {
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();
        start.wait();
        publisher.complete(round);
        for waiter in waiters {
            waiter.join().expect("waiter panicked");
        }
    }
    assert_eq!(
        failures.load(Ordering::SeqCst),
        0,
        "waiter received wrong outcome"
    );
}

/// Timeout-boundary storm: a waiter loops `wait_timeout(0)` and
/// `wait_timeout(1ns)` while the publisher completes mid-storm. Every
/// `Some` must carry the exact published outcome; after completion the
/// loop must eventually observe `Some`.
#[test]
fn stress_zero_timeout_boundary() {
    const ROUNDS: u64 = 500;

    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let waiter = {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                // Completion is guaranteed (the publisher completes before
                // retiring), so loop until observed: a fixed iteration cap
                // would make the test itself racy under scheduler load.
                loop {
                    if let Some(outcome) = observation.wait_timeout(Duration::ZERO) {
                        assert_eq!(outcome, round, "zero-timeout returned a wrong outcome");
                        return true;
                    }
                    if let Some(outcome) = observation.wait_timeout(Duration::from_nanos(1)) {
                        assert_eq!(outcome, round, "nano-timeout returned a wrong outcome");
                        return true;
                    }
                }
            })
        };
        barrier.wait();
        thread::yield_now();
        publisher.complete(round);
        assert!(
            waiter.join().expect("waiter panicked"),
            "waiter never observed the completion"
        );
    }
}

/// Reentrant wake/drop: a waker whose `wake` drops ANOTHER observation of
/// the same slot (re-entering the slot's waiter machinery during the
/// drain) must not deadlock or corrupt the drain.
#[test]
fn stress_reentrant_wake_drops_observation() {
    struct Reentrant {
        victim: std::sync::Mutex<Option<Observation<u64>>>,
        fires: AtomicUsize,
    }

    impl Wake for Reentrant {
        fn wake(self: Arc<Self>) {
            self.fires.fetch_add(1, Ordering::SeqCst);
            // Re-enter: dropping the victim observation during the drain.
            let _ = self.victim.lock().expect("victim lock").take();
        }
    }

    for round in 0..100_u64 {
        let (publisher, observation) = pair::<u64>();
        let observer = observation.clone();
        let victim = observation;
        let reentrant = Arc::new(Reentrant {
            victim: std::sync::Mutex::new(Some(victim)),
            fires: AtomicUsize::new(0),
        });
        assert!(!observer.register_waker(&std::task::Waker::from(reentrant.clone())));
        publisher.complete(round);
        assert_eq!(
            reentrant.fires.load(Ordering::SeqCst),
            1,
            "reentrant waker fired != once (round {round})"
        );
        assert_eq!(observer.try_get(), Some(round));
    }
}

/// Spurious-unpark injection: an external thread fires extra unparks at a
/// blocked waiter while a second waiter registers concurrently, stressing
/// the duplicate-registration dedup (`waiters.last()` check). Whatever the
/// internal duplication, every waiter must still resolve to the exact
/// outcome and no waiter may be left parked.
#[test]
fn stress_spurious_unpark_injection() {
    const ROUNDS: u64 = 100;

    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        let start = Arc::new(Barrier::new(4));
        let mk_waiter = || {
            let observation = observation.clone();
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                observation.wait()
            })
        };
        let waiter_a = mk_waiter();
        let waiter_b = mk_waiter();
        let handle_a = waiter_a.thread().clone();

        let injector = {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                for _ in 0..64 {
                    handle_a.unpark(); // injected spurious wakeups
                    thread::yield_now();
                }
            })
        };
        start.wait();
        // Let the injection interleave with registration, then complete.
        for _ in 0..32 {
            thread::yield_now();
        }
        publisher.complete(round);
        injector.join().expect("injector panicked");
        assert_eq!(waiter_a.join().expect("waiter A panicked"), round);
        assert_eq!(waiter_b.join().expect("waiter B panicked"), round);
    }
}

/// A waker whose `wake` re-registers ITSELF on the same slot (reentrant
/// registration during the drain) must not deadlock or loop: the
/// re-registration sees COMPLETED and returns immediately.
#[test]
fn stress_reentrant_wake_reregistration_no_loop() {
    struct ReRegister {
        observed: std::sync::Mutex<Option<Observation<u64>>>,
        fires: AtomicUsize,
    }

    impl Wake for ReRegister {
        fn wake(self: Arc<Self>) {
            self.fires.fetch_add(1, Ordering::SeqCst);
            let guard = self.observed.lock().expect("observation lock");
            if let Some(observed) = guard.as_ref() {
                // Reentrant registration mid-drain: must return `true`
                // (already completed) without registering again.
                let (waker, _) = CountWake::waker();
                assert!(
                    observed.register_waker(&waker),
                    "re-registration must see COMPLETED"
                );
            }
        }
    }

    for round in 0..50_u64 {
        let (publisher, observation) = pair::<u64>();
        let observer = observation.clone();
        let re = Arc::new(ReRegister {
            observed: std::sync::Mutex::new(Some(observation)),
            fires: AtomicUsize::new(0),
        });
        assert!(!observer.register_waker(&std::task::Waker::from(re.clone())));
        publisher.complete(round);
        assert_eq!(
            re.fires.load(Ordering::SeqCst),
            1,
            "reentrant waker fired != once (round {round})"
        );
    }
}

/// Publisher lifecycle migration: register on one thread, migrate the
/// publication authority through a second, complete on a third; a waiter
/// on a fourth must observe the exact outcome. The completion protocol
/// must not depend on thread affinity, and `Publisher: Send` must hold.
/// (Under the unkeyed law the authority is consumed by `complete`, so no
/// post-completion retire step exists.)
#[test]
fn stress_publisher_thread_migration() {
    for round in 0..100_u64 {
        let (publisher, observation) = pair::<u64>();
        let waiter = thread::spawn(move || observation.wait());
        let completer = thread::spawn(move || {
            let publisher = thread::spawn(move || publisher)
                .join()
                .expect("migration thread 1 panicked");
            thread::spawn(move || publisher.complete(round))
                .join()
                .expect("migration thread 2 panicked");
        });
        assert_eq!(waiter.join().expect("waiter panicked"), round);
        completer.join().expect("completer panicked");
    }
}

/// Registration flood: hundreds of distinct wakers on one pending
/// slot, each must fire exactly once at completion.
#[test]
fn stress_registration_flood_wakes_each_once() {
    const WAKERS: usize = 256;
    let (publisher, observation) = pair::<u64>();
    let probes: Vec<_> = (0..WAKERS).map(|_| CountWake::waker()).collect();
    for (waker, _) in &probes {
        let observer = observation.clone();
        assert!(!observer.register_waker(waker));
    }
    publisher.complete(99);
    for (i, (_, probe)) in probes.iter().enumerate() {
        assert_eq!(probe.count(), 1, "waker {i} fired != once");
    }
}

/// Raw-API waker registration racing completion: the waiter thread calls
/// `register_waker` directly (no future), then blocks on `park`.
/// `register_waker` returning `false` is a promise: "registered, you WILL
/// be woken". The completion drain must fire the waker whether the
/// registration won or lost the race. A lost wake strands the parked
/// waiter; the `recv_timeout` watchdog turns that into a hard failure
/// (and `Ok(None)` would catch a wake fired before the publication was
/// observable, an ordering violation).
#[test]
fn stress_register_waker_racing_completion_no_lost_wake() {
    const ROUNDS: u64 = 400;

    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let (waker, _) = ThreadWake::waker();
                if observation.register_waker(&waker) {
                    // Already published: nothing registered, read directly.
                    tx.send(observation.try_get()).expect("send failed");
                    return;
                }
                // Registered: block until the completion drain unparks us.
                // A lost wakeup strands this park forever -> watchdog.
                std::thread::park();
                tx.send(observation.try_get()).expect("send failed");
            })
        };
        barrier.wait();
        // Bias one round in four toward the registered-then-completed
        // ordering (registration wins the race); the rest race freely.
        if round % 4 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
        publisher.complete(round);
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Some(outcome)) => assert_eq!(outcome, round, "wrong outcome (round {round})"),
            Ok(None) => panic!(
                "register_waker returned false but the outcome was not readable after the wake (round {round})"
            ),
            Err(_) => panic!(
                "register_waker returned false but no wake arrived: waiter stranded (round {round})"
            ),
        }
        waiter.join().expect("waiter panicked");
    }
}

/// Mixed drain on ONE slot: two blocking thread waiters, two
/// `wait_timeout` waiters, and two raw waker registrations, all racing one
/// completion. The drain must resolve every waiter exactly once with the
/// exact outcome — the native counterpart of the loom mixed-drain model
/// that proved infeasible under the scheduler (recorded in Batch 16).
#[test]
fn stress_mixed_waiter_drain_all_resolved() {
    const ROUNDS: u64 = 200;

    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        let barrier = Arc::new(Barrier::new(7)); // 6 waiters + publisher

        let thread_waiters: Vec<_> = (0..2)
            .map(|_| {
                let observation = observation.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    observation.wait()
                })
            })
            .collect();

        let timeout_waiters: Vec<_> = (0..2)
            .map(|_| {
                let observation = observation.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    observation.wait_timeout(Duration::from_secs(5))
                })
            })
            .collect();

        let waker_waiters: Vec<_> = (0..2)
            .map(|_| {
                let observation = observation.clone();
                let barrier = Arc::clone(&barrier);
                let (waker, probe) = CountWake::waker();
                thread::spawn(move || {
                    barrier.wait();
                    // Either the registration won (false: will be woken) or
                    // the completion won (true: already published) — both
                    // legal; the outcome must be readable either way.
                    if !observation.register_waker(&waker) {
                        // Spin until the waker fires, then read. A lost
                        // wake strands this loop; the deadline fails it.
                        let deadline = std::time::Instant::now() + Duration::from_secs(10);
                        while probe.count() == 0 {
                            if std::time::Instant::now() > deadline {
                                panic!("registered waker never fired");
                            }
                            thread::yield_now();
                        }
                    }
                    observation
                        .try_get()
                        .expect("outcome must be readable after the wake")
                })
            })
            .collect();

        barrier.wait();
        publisher.complete(round);

        for waiter in thread_waiters {
            assert_eq!(waiter.join().expect("thread waiter panicked"), round);
        }
        for waiter in timeout_waiters {
            assert_eq!(
                waiter.join().expect("timeout waiter panicked"),
                Some(round),
                "timeout waiter must resolve to the exact outcome"
            );
        }
        for waiter in waker_waiters {
            assert_eq!(
                waiter.join().expect("waker waiter panicked"),
                round,
                "waker waiter must resolve to the exact outcome"
            );
        }
    }
}

/// The `wait()` re-registration dedup path, pinned deterministically:
/// the SAME waiter thread registers on one pair, is spuriously woken and
/// re-registers (so the `waiters.last()` dedup check misses and a second
/// entry accumulates). The drain then fires the waker twice (two unpark
/// tokens). The waiter consumes one token and returns; the second token
/// is queued for that thread's NEXT park — a stale token that must not
/// corrupt a later wait: a subsequent wait on a DIFFERENT pair (same
/// thread) still resolves exactly (the stale token only causes one
/// spurious park, and the loop's recheck heals it).
#[test]
fn stress_duplicate_waiter_entry_stale_token_self_heals() {
    const ROUNDS: u64 = 50;

    for round in 0..ROUNDS {
        let (publisher_one, observation_one) = pair::<u64>();
        let (publisher_two, observation_two) = pair::<u64>();
        let handle = {
            let first = observation_one.clone();
            let second = observation_two.clone();
            thread::spawn(move || {
                let first_outcome = first.wait_timeout(Duration::from_secs(5));
                // The stale token (if any) is queued for the NEXT park on
                // this thread: the second wait must still resolve exactly.
                let second_outcome = second.wait_timeout(Duration::from_secs(5));
                (first_outcome, second_outcome)
            })
        };
        // Let the waiter register, then inject a spurious wake so it
        // re-registers (duplicate entry).
        thread::sleep(Duration::from_millis(10));
        handle.thread().unpark();
        thread::sleep(Duration::from_millis(10)); // re-registration window
        publisher_one.complete(round);

        assert_eq!(
            handle.join().expect("waiter panicked"),
            (Some(round), None),
            "the first wait must resolve despite its duplicate entry; \
             the second pair is still pending (round {round})"
        );

        // Phase 2: complete the second pair; the same waiter thread has
        // already exited, but a fresh wait on another thread must still
        // resolve exactly (a stale token on a retired thread cannot leak).
        publisher_two.complete(round + 100);
        assert_eq!(
            observation_two.wait(),
            round + 100,
            "a stale unpark token must not corrupt the next wait (round {round})"
        );
        drop(observation_one);
        drop(observation_two);
    }
}

/// A waker whose `wake` calls the BLOCKING `wait` on the same slot
/// during the drain: `complete` sets COMPLETED before the drain, so the
/// reentrant wait returns immediately (never parks the completing thread),
/// the drain finishes, and every waiter resolves exactly once.
#[test]
fn stress_wait_inside_wake_during_drain() {
    struct WaitInWake {
        observed: std::sync::Mutex<Option<Observation<u64>>>,
        fires: AtomicUsize,
        value: std::sync::Mutex<Option<u64>>,
    }

    impl Wake for WaitInWake {
        fn wake(self: Arc<Self>) {
            self.fires.fetch_add(1, Ordering::SeqCst);
            let guard = self.observed.lock().expect("observation lock");
            if let Some(observed) = guard.as_ref() {
                // Reentrant blocking wait during the drain: COMPLETED is
                // already set, so this returns immediately.
                *self.value.lock().expect("value lock") = Some(observed.wait());
            }
        }
    }

    for round in 0..100_u64 {
        let (publisher, observation) = pair::<u64>();
        let waker_observer = observation.clone();
        let other_observer = observation.clone();
        let reentrant = Arc::new(WaitInWake {
            observed: std::sync::Mutex::new(Some(waker_observer)),
            fires: AtomicUsize::new(0),
            value: std::sync::Mutex::new(None),
        });
        assert!(!observation.register_waker(&std::task::Waker::from(reentrant.clone())));
        // A second waiter (the drain must still resolve it after the
        // reentrant wait).
        let barrier = Arc::new(Barrier::new(2));
        let other = {
            let other_observer = other_observer.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                other_observer.wait()
            })
        };
        barrier.wait();
        publisher.complete(round);
        assert_eq!(
            reentrant.fires.load(Ordering::SeqCst),
            1,
            "reentrant waker fired != once (round {round})"
        );
        assert_eq!(
            *reentrant.value.lock().expect("value lock"),
            Some(round),
            "the in-drain wait must resolve to the exact outcome (round {round})"
        );
        assert_eq!(
            other.join().expect("other waiter panicked"),
            round,
            "the drain must still resolve the other waiter (round {round})"
        );
    }
}

/// A waker whose `wake` calls `wait_timeout` on the same slot during
/// the drain: COMPLETED is set before the drain, so it returns Some
/// immediately (the completing thread never parks), and the drain still
/// resolves every other waiter exactly once.
#[test]
fn stress_wait_timeout_inside_wake_during_drain() {
    struct TimeoutInWake {
        observed: std::sync::Mutex<Option<Observation<u64>>>,
        fires: AtomicUsize,
        value: std::sync::Mutex<Option<Option<u64>>>,
    }

    impl Wake for TimeoutInWake {
        fn wake(self: Arc<Self>) {
            self.fires.fetch_add(1, Ordering::SeqCst);
            let guard = self.observed.lock().expect("observation lock");
            if let Some(observed) = guard.as_ref() {
                *self.value.lock().expect("value lock") =
                    Some(observed.wait_timeout(Duration::from_secs(5)));
            }
        }
    }

    for round in 0..50_u64 {
        let (publisher, observation) = pair::<u64>();
        let waker_observer = observation.clone();
        let reentrant = Arc::new(TimeoutInWake {
            observed: std::sync::Mutex::new(Some(waker_observer)),
            fires: AtomicUsize::new(0),
            value: std::sync::Mutex::new(None),
        });
        assert!(!observation.register_waker(&std::task::Waker::from(reentrant.clone())));
        publisher.complete(round);
        assert_eq!(
            reentrant.fires.load(Ordering::SeqCst),
            1,
            "reentrant waker fired != once (round {round})"
        );
        assert_eq!(
            *reentrant.value.lock().expect("value lock"),
            Some(Some(round)),
            "the in-drain wait_timeout must resolve to the exact outcome (round {round})"
        );
        assert_eq!(observation.try_get(), Some(round));
    }
}

/// `into_outcome` from within a waker during the drain is refused: the
/// waker's own observation keeps the slot shared, so the exclusive take
/// must return None and leave the outcome readable.
#[test]
fn stress_into_outcome_inside_wake_during_drain_refused() {
    struct TakeInWake {
        observed: std::sync::Mutex<Option<Observation<u64>>>,
        fires: AtomicUsize,
        taken: std::sync::Mutex<Option<Option<u64>>>,
    }

    impl Wake for TakeInWake {
        fn wake(self: Arc<Self>) {
            self.fires.fetch_add(1, Ordering::SeqCst);
            // Consume the waker's own observation (into_outcome takes self).
            if let Some(observed) = self.observed.lock().expect("observation lock").take() {
                // Another observation of the slot still lives, so the slot
                // is shared: the take must be refused.
                *self.taken.lock().expect("taken lock") = Some(observed.into_outcome());
            }
        }
    }

    for round in 0..50_u64 {
        let (publisher, observation) = pair::<u64>();
        let taker_observer = observation.clone();
        let reentrant = Arc::new(TakeInWake {
            observed: std::sync::Mutex::new(Some(observation)),
            fires: AtomicUsize::new(0),
            taken: std::sync::Mutex::new(None),
        });
        assert!(!taker_observer.register_waker(&std::task::Waker::from(reentrant.clone())));
        publisher.complete(round);
        assert_eq!(
            reentrant.fires.load(Ordering::SeqCst),
            1,
            "reentrant waker fired != once (round {round})"
        );
        assert_eq!(
            *reentrant.taken.lock().expect("taken lock"),
            Some(None),
            "the in-drain take must be refused while the slot is shared (round {round})"
        );
        assert_eq!(taker_observer.try_get(), Some(round), "outcome stays readable");
    }
}

/// Pinned pending slots never fabricate: observations captured on pairs
/// whose publisher is retired WITHOUT completing must time out forever
/// (never resolve to a value), even while other pairs complete
/// concurrently. The threaded twin of the affine uniqueness law.
#[test]
fn stress_pinned_pending_timeout_never_fabricates() {
    const ROUNDS: u64 = 200;

    for round in 0..ROUNDS {
        // A pair abandoned without completing: two handles to the same
        // slot, one stays here, one goes to the waiter.
        let (abandoned, pinned) = pair::<u64>();
        let pinned_waiter = pinned.clone();
        drop(abandoned); // retire pending; both handles hold the slot

        // Complete an unrelated pair while the pinned observation waits:
        // it must never see that value.
        let (churn_publisher, churn_observation) = pair::<u64>();
        let barrier = Arc::new(Barrier::new(2));
        let waiter = {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                // The abandoned slot never completes; this must always time out.
                pinned_waiter.wait_timeout(Duration::from_millis(20))
            })
        };
        barrier.wait();
        churn_publisher.complete(round);
        assert_eq!(
            churn_observation.try_get(),
            Some(round),
            "the churn pair must complete (round {round})"
        );
        assert_eq!(
            waiter.join().expect("waiter panicked"),
            None,
            "a retired-pending slot must never fabricate an outcome (round {round})"
        );
        assert_eq!(
            pinned.try_get(),
            None,
            "the pinned observation must stay pending (round {round})"
        );
    }
}
