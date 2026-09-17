//! Coverage-guided fuzz target: future lifecycle sequences over unkeyed
//! pairs — creation, polled waker migration, completion, ready reads, and
//! cancellation. Exact wake-count assertions: only a live future's
//! currently installed waker fires at completion; migrated and cancelled
//! wakers never fire. A shared `DropProbe` counter checks exactly-once
//! destruction across the whole campaign.
//!
//! One pair is live at a time; futures created from its observations
//! outlive it, tagged with their generation.
//!
//! Ops (byte % 6): 0 open a pair (retiring the live one), 1 create a
//! future from the live observation, 2 poll the newest future with a
//! fresh waker, 3 complete the live pair, 4 read a completed future,
//! 5 cancel (drop) the newest future.

#![no_main]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use observe::probe::{CountWake, DropProbe, poll_once};
use observe::{ObservationFuture, pair};

struct Fut {
    future: Pin<Box<ObservationFuture<DropProbe>>>,
    generation: u64,
    wakers: Vec<Arc<CountWake>>,
    done: bool,
}

fuzz_target!(|data: &[u8]| {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut created = 0_usize;
    let mut live_publisher: Option<observe::Publisher<DropProbe>> = None;
    let mut live_generation: u64 = 0;
    let mut live_observation: Option<observe::Observation<DropProbe>> = None;
    // generation -> published tag, or None while pending.
    let mut outcomes: HashMap<u64, Option<u64>> = HashMap::new();
    let mut futures: Vec<Fut> = Vec::new();
    // Wakers of cancelled futures with their fire count at cancellation:
    // nothing may fire them afterwards (checked at teardown).
    let mut cancelled: Vec<(Arc<CountWake>, usize)> = Vec::new();

    for byte in data.iter().take(128) {
        match byte % 6 {
            // Open a fresh pair; the live one retires as-is (pending
            // outcomes never synthesize).
            0 => {
                if let Some(publisher) = live_publisher.take() {
                    outcomes.entry(live_generation).or_insert(None);
                    drop(publisher);
                }
                live_generation += 1;
                let (publisher, observation) = pair::<DropProbe>();
                live_publisher = Some(publisher);
                live_observation = Some(observation);
            }
            // Create a future from the live pair's observation.
            1 => {
                if let Some(observation) = &live_observation {
                    futures.push(Fut {
                        future: Box::pin(observation.clone().into_future()),
                        generation: live_generation,
                        wakers: Vec::new(),
                        done: false,
                    });
                }
            }
            // Poll the newest future with a fresh waker: pending futures
            // install it (replacing any earlier one).
            2 => {
                if let Some(fut) = futures.last_mut()
                    && !fut.done
                {
                    let (waker, probe) = CountWake::waker();
                    let pending = poll_once(fut.future.as_mut(), &waker).is_pending();
                    let generation_pending = outcomes
                        .get(&fut.generation)
                        .copied()
                        .flatten()
                        .is_none();
                    assert_eq!(
                        pending, generation_pending,
                        "poll disagreed with the recorded outcome at generation {}",
                        fut.generation
                    );
                    fut.wakers.push(probe);
                }
            }
            // Complete the live pair: every pending future of this
            // generation has exactly its last-installed waker fired.
            3 => {
                if let Some(publisher) = live_publisher.take() {
                    publisher.complete(DropProbe::with_counter(live_generation, &counter));
                    created += 1;
                    outcomes.insert(live_generation, Some(live_generation));
                    for fut in &futures {
                        if fut.generation == live_generation && !fut.done {
                            for (i, probe) in fut.wakers.iter().enumerate() {
                                let expected = usize::from(i + 1 == fut.wakers.len());
                                assert_eq!(
                                    probe.count(),
                                    expected,
                                    "waker {i} of {} fired {} times at completion (generation {})",
                                    fut.wakers.len(),
                                    probe.count(),
                                    fut.generation
                                );
                            }
                        }
                    }
                }
            }
            // Read a completed future: consume it, asserting Ready with
            // the exact generation tag (the poll moves the probe out —
            // accounted as created).
            4 => {
                if let Some(mut fut) = futures.pop()
                    && fut.done
                {
                    match poll_once(fut.future.as_mut(), &std::task::Waker::noop()) {
                        std::task::Poll::Ready(probe) => {
                            created += 1;
                            assert_eq!(
                                probe.tag, fut.generation,
                                "completed future resolved to a foreign outcome"
                            );
                        }
                        std::task::Poll::Pending => {
                            panic!("a completed future polled Pending");
                        }
                    }
                }
            }
            // Cancel: drop the newest future; its installed waker must
            // never fire afterwards.
            5 => {
                if let Some(fut) = futures.pop() {
                    for probe in fut.wakers {
                        let fires = probe.count();
                        cancelled.push((probe, fires));
                    }
                }
            }
            _ => unreachable!("byte % 6 is exhausted"),
        }
        // Futures of a generation whose completion just landed become
        // readable (done) — recompute from the recorded outcomes.
        for fut in futures.iter_mut() {
            if outcomes
                .get(&fut.generation)
                .copied()
                .flatten()
                .is_some()
            {
                fut.done = true;
            }
        }
    }

    // Teardown: cancelled wakers still show their cancellation-time fire
    // counts, and every created probe was destroyed exactly once.
    drop(live_publisher);
    drop(live_observation);
    for (probe, fires) in cancelled {
        assert_eq!(
            probe.count(),
            fires,
            "a cancelled waker fired after cancellation"
        );
    }
    drop(futures);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "drop accounting diverged: {created} probes created"
    );
});
