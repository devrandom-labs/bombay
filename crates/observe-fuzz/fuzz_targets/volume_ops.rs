//! Coverage-guided fuzz target: high-volume churn over many live
//! unkeyed pairs at once — the scale complement to `ops` (which keeps
//! one pair live). Pairs accumulate across the run; completions, reads,
//! cancellations, and retires interleave in input-driven order. A shared
//! `DropProbe` counter checks exactly-once destruction across the whole
//! campaign (created == dropped after teardown, counting every
//! successful `try_get` clone), and generation tags catch any foreign
//! or synthesized outcome.
//!
//! Each input byte splits into an op (`byte % 6`) and a pair selector
//! (`byte / 6`), so input drives churn across arbitrarily many live
//! pairs.
//!
//! Ops (byte % 6): 0 open a new pair, 1 complete the selected pair,
//! 2 clone the selected pair's newest observation, 3 try_get on the
//! selected pair's newest observation, 4 retire the selected pending
//! publisher, 5 drop the selected pair's newest observation (a fully
//! drained pair leaves the live set).

#![no_main]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use observe::probe::DropProbe;
use observe::{Observation, pair};

struct Pair {
    publisher: Option<observe::Publisher<DropProbe>>,
    generation: u64,
    observations: Vec<Observation<DropProbe>>,
}

fuzz_target!(|data: &[u8]| {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut created = 0_usize;
    let mut pairs: Vec<Pair> = Vec::new();
    let mut next_generation: u64 = 0;
    // generation -> published tag, or None while pending/retired.
    let mut outcomes: HashMap<u64, Option<u64>> = HashMap::new();

    for byte in data.iter().take(256) {
        let op = byte % 6;
        if op != 0 && pairs.is_empty() {
            continue; // nothing live to churn yet
        }
        let index = usize::from(byte / 6) % pairs.len().max(1);
        match op {
            // Open a new pair.
            0 => {
                let (publisher, observation) = pair::<DropProbe>();
                pairs.push(Pair {
                    publisher: Some(publisher),
                    generation: next_generation,
                    observations: vec![observation],
                });
                next_generation += 1;
            }
            // Complete the selected pair's publisher.
            1 => {
                let target = &mut pairs[index];
                if let Some(publisher) = target.publisher.take() {
                    publisher.complete(DropProbe::with_counter(target.generation, &counter));
                    created += 1;
                    outcomes.insert(target.generation, Some(target.generation));
                }
            }
            // Clone the selected pair's newest observation.
            2 => {
                let target = &mut pairs[index];
                if let Some(observation) = target.observations.last() {
                    target.observations.push(observation.clone());
                }
            }
            // try_get on the selected pair's newest observation must
            // match its generation's recorded outcome exactly.
            3 => {
                let target = &mut pairs[index];
                if let Some(observation) = target.observations.last() {
                    let expected = outcomes.get(&target.generation).copied().flatten();
                    let seen = observation.try_get().map(|probe| {
                        created += 1;
                        probe.tag
                    });
                    assert_eq!(
                        seen, expected,
                        "try_get diverged at generation {}",
                        target.generation
                    );
                }
            }
            // Retire the selected pending publisher.
            4 => {
                let target = &mut pairs[index];
                if let Some(publisher) = target.publisher.take() {
                    outcomes.entry(target.generation).or_insert(None);
                    drop(publisher);
                }
            }
            // Drop the selected pair's newest observation; a fully
            // drained pair leaves the live set.
            5 => {
                let target = &mut pairs[index];
                target.observations.pop();
                if target.observations.is_empty() && target.publisher.is_none() {
                    outcomes.entry(target.generation).or_insert(None);
                    pairs.remove(index);
                }
            }
            _ => unreachable!("byte % 6 is exhausted"),
        }
    }

    // Teardown: every probe created by the campaign must have been
    // destroyed exactly once.
    for Pair {
        publisher,
        generation,
        observations,
    } in pairs
    {
        if let Some(publisher) = publisher {
            outcomes.entry(generation).or_insert(None);
            drop(publisher);
        }
        drop(observations);
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "drop accounting diverged: {created} probes created"
    );
});
