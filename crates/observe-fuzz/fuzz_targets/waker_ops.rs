//! Coverage-guided fuzz target: raw `register_waker` accounting at fuzz
//! scale over independent unkeyed pairs. Exact fire-count assertions: a
//! waker registered while its generation is pending fires EXACTLY ONCE
//! iff that generation eventually completes (the drain fires it), never
//! otherwise — including when the publisher retires pending. Drop
//! accounting (one shared `DropProbe` counter: created == dropped after
//! teardown) and generation-tagged integrity run alongside, so a leak, a
//! double-drop, or a foreign outcome is a crash artifact.
//!
//! One pair is live at a time; observations outliving it carry their
//! generation tag.
//!
//! Ops (byte % 7): 0 open a pair (retiring the live one), 1
//! register_waker on a live observation, 2 complete the live pair,
//! 3 try_get on the live observation, 4 retire the pending publisher,
//! 5 into_outcome on the newest retired observation, 6 drop the newest
//! live observation.

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
    let mut live_publisher: Option<observe::Publisher<DropProbe>> = None;
    let mut live_generation: u64 = 0;
    let mut live_observations: Vec<Observation<DropProbe>> = Vec::new();
    // generation -> published tag, or None while pending/retired.
    let mut outcomes: HashMap<u64, Option<u64>> = HashMap::new();
    // Registered wakers by generation: fire once iff the generation
    // completed, never otherwise.
    let mut registered: HashMap<u64, Vec<Arc<CountWake>>> = HashMap::new();
    // Observations outliving their pair: (generation, observation).
    let mut graveyard: Vec<(u64, Observation<DropProbe>)> = Vec::new();

    for byte in data.iter().take(256) {
        match byte % 7 {
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
            // register_waker succeeds exactly when the outcome is
            // published; a pending registration joins the drain list.
            1 => {
                if let Some(observation) = live_observations.last() {
                    let (waker, probe) = CountWake::waker();
                    let completed =
                        outcomes.get(&live_generation).copied().flatten().is_some();
                    assert_eq!(
                        observation.register_waker(&waker),
                        completed,
                        "register_waker disagreed with the recorded outcome at generation \
                         {live_generation}"
                    );
                    if !completed {
                        registered
                            .entry(live_generation)
                            .or_default()
                            .push(probe);
                    }
                }
            }
            // Complete the live pair: every registered waker of the
            // generation fires exactly once.
            2 => {
                if let Some(publisher) = live_publisher.take() {
                    publisher.complete(DropProbe::with_counter(live_generation, &counter));
                    created += 1;
                    outcomes.insert(live_generation, Some(live_generation));
                    for probe in registered.get(&live_generation).into_iter().flatten() {
                        assert_eq!(
                            probe.count(),
                            1,
                            "a registered waker fired {} times at completion (generation {})",
                            probe.count(),
                            live_generation
                        );
                    }
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
            // Retire the pending publisher: its registered wakers must
            // never fire.
            4 => {
                if let Some(publisher) = live_publisher.take() {
                    outcomes.entry(live_generation).or_insert(None);
                    drop(publisher);
                }
            }
            // into_outcome on the newest retired observation: a success
            // must carry the exact outcome (a refusal while shared is
            // legal).
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
            // Drop the newest live observation.
            6 => {
                live_observations.pop();
            }
            _ => unreachable!("byte % 7 is exhausted"),
        }
    }

    // Teardown: registered wakers fired exactly once iff their generation
    // completed, never otherwise; every created probe was destroyed
    // exactly once.
    drop(live_publisher);
    drop(live_observations);
    drop(graveyard);
    for (generation, probes) in &registered {
        let completed = outcomes.get(generation).copied().flatten().is_some();
        for probe in probes {
            let expected = usize::from(completed);
            assert_eq!(
                probe.count(),
                expected,
                "waker fire count diverged for generation {generation}"
            );
        }
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "drop accounting diverged: {created} probes created"
    );
});
