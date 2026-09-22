//! Coverage-guided fuzz target: interprets the input as an op sequence
//! against the sequential semantics of unkeyed observation pairs.
//! Outcome tags encode the generation number, so a recycled slot
//! delivering a foreign generation's outcome panics and becomes a crash
//! artifact.
//!
//! One pair is live at a time; completed or retired generations survive
//! through their cloned observations. A shared `DropProbe` counter checks
//! exactly-once destruction across the whole campaign: the total number
//! of probes created (one per completion, one per successful `try_get`
//! clone) must equal the total number of drops after teardown.
//!
//! Ops (byte % 8): 0 open a pair (retiring the live one), 1 complete,
//! 2 clone the live observation, 3 try_get on the live observation,
//! 4 try_get on the newest retired observation, 5 into_outcome on the
//! newest retired observation, 6 register_waker on the live observation,
//! 7 drop the live observation.

#![no_main]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use observe::probe::{CountWake, DropProbe};
use observe::{Observation, pair};

fuzz_target!(|data: &[u8]| {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut created = 0_usize;
    // The live pair: publisher (present until completion consumes it or a
    // retire drops it), its generation, and every observation clone.
    let mut live_publisher: Option<observe::Publisher<DropProbe>> = None;
    let mut live_generation: u64 = 0;
    let mut live_observations: Vec<Observation<DropProbe>> = Vec::new();
    // generation -> the tag its completion published, or None while pending.
    let mut outcomes: HashMap<u64, Option<u64>> = HashMap::new();
    // Observations outliving their pair: (generation, observation).
    let mut graveyard: Vec<(u64, Observation<DropProbe>)> = Vec::new();

    for byte in data.iter().take(256) {
        match byte % 8 {
            // Open a fresh pair; the live one retires as-is.
            0 => {
                if let Some(publisher) = live_publisher.take() {
                    outcomes.entry(live_generation).or_insert(None);
                    drop(publisher);
                }
                graveyard.extend(
                    live_observations
                        .drain(..)
                        .map(|observation| (live_generation, observation)),
                );
                live_generation += 1;
                let (publisher, observation) = pair::<DropProbe>();
                live_publisher = Some(publisher);
                live_observations.push(observation);
            }
            // Complete the live pair. The publication authority is
            // consumed, so a second complete op for the same generation
            // is a no-op.
            1 => {
                if let Some(publisher) = live_publisher.take() {
                    publisher.complete(DropProbe::with_counter(live_generation, &counter));
                    created += 1;
                    outcomes.insert(live_generation, Some(live_generation));
                }
            }
            // Clone the live observation: a completed generation's clone
            // must expose the exact outcome forever.
            2 => {
                if let Some(observation) = live_observations.last() {
                    live_observations.push(observation.clone());
                }
            }
            // try_get on the live observation must match the recorded
            // outcome exactly (a successful read clones the probe).
            3 => {
                if let Some(observation) = live_observations.last() {
                    let expected = outcomes.get(&live_generation).copied().flatten();
                    let seen = observation.try_get().map(|probe| {
                        created += 1;
                        probe.tag
                    });
                    assert_eq!(
                        seen, expected,
                        "live try_get diverged at generation {live_generation}"
                    );
                }
            }
            // try_get on the newest retired observation must match its
            // own generation's recorded outcome.
            4 => {
                if let Some((generation, observation)) = graveyard.last() {
                    let expected = outcomes.get(generation).copied().flatten();
                    let seen = observation.try_get().map(|probe| {
                        created += 1;
                        probe.tag
                    });
                    assert_eq!(
                        seen, expected,
                        "retired try_get diverged at generation {generation}"
                    );
                }
            }
            // into_outcome on the newest retired observation moves the
            // outcome out only when it is the last reference; a refusal
            // (the slot is still shared) is legal, but a success must
            // carry the exact outcome.
            5 => {
                if let Some((generation, observation)) = graveyard.pop() {
                    let expected = outcomes.get(&generation).copied().flatten();
                    if let Some(probe) = observation.into_outcome() {
                        assert_eq!(
                            Some(probe.tag),
                            expected,
                            "into_outcome moved a foreign or synthesized outcome at generation \
                             {generation}"
                        );
                    }
                }
            }
            // register_waker succeeds exactly when the outcome is
            // published; the waker is dropped immediately either way.
            6 => {
                if let Some(observation) = live_observations.last() {
                    let (waker, _probe) = CountWake::waker();
                    let completed = outcomes
                        .get(&live_generation)
                        .copied()
                        .flatten()
                        .is_some();
                    assert_eq!(
                        observation.register_waker(&waker),
                        completed,
                        "register_waker disagreed with the recorded outcome at generation \
                         {live_generation}"
                    );
                }
            }
            // Drop the live observation; the last one retires the slot.
            7 => {
                live_observations.pop();
            }
            _ => unreachable!("byte % 8 is exhausted"),
        }
    }

    // Teardown: every probe created by the campaign must have been
    // destroyed exactly once.
    drop(live_publisher);
    drop(live_observations);
    drop(graveyard);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "drop accounting diverged: {created} probes created"
    );
});
