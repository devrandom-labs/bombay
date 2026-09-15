//! Coverage-guided fuzz target: op sequences that churn subject
//! generations across the SmallMap promotion boundary. `ops` uses 4 keys
//! (= INLINE_CAP), so the inline key table never reaches the length that
//! promotes it to the hash map; this target uses 6 keys, so the fifth
//! live generation forces the promoted hash path (insert_vacant's
//! promotion, hash `remove_if`, hash `get`) to be exercised at fuzz
//! scale.
//!
//! Outcomes are `DropProbe`s sharing one drop counter: the total number
//! of values created (one per completion, one per successful `try_get`
//! clone) must equal the total number of drops after teardown — a leak or
//! a double-drop is a crash artifact. Tags encode (epoch, key), so any
//! cross-generation or cross-key leak also panics.
//!
//! Ops (byte % 7): 0 register, 1 complete, 2 observe, 3 try_get, 4 retire
//! (drop subject), 5 into_outcome, 6 drop an observation.
//! Keys: byte % 6 (INLINE_CAP boundary at 4).

#![no_main]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use observe::{Observation, ObservationSpace, Subject};
use observe::probe::DropProbe;

const KEYS: u8 = 6;

/// Value encoding: epoch in high bits, key in low 8 bits.
fn encode(epoch: u64, key: u8) -> u64 {
    (epoch << 8) | u64::from(key)
}

fuzz_target!(|data: &[u8]| {
    let space = ObservationSpace::<u8, DropProbe>::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut created = 0usize;
    let mut subjects: HashMap<u8, Subject<u8, DropProbe>> = HashMap::new();
    let mut epochs: HashMap<u8, u64> = HashMap::new();
    // (key, epoch) -> completed?  The slot keeps its outcome after retire,
    // so an observation of a retired generation still resolves iff that
    // generation completed.
    let mut completed: HashMap<(u8, u64), bool> = HashMap::new();
    let mut observations: Vec<(u8, u64, Observation<DropProbe>)> = Vec::new();

    for pair in data.chunks_exact(2).take(256) {
        let (op, key) = (pair[0] % 7, pair[1] % KEYS);
        match op {
            0 => {
                if let Ok(subject) = space.subject(key) {
                    let epoch = epochs.get(&key).map_or(0, |e| e + 1);
                    epochs.insert(key, epoch);
                    subjects.insert(key, subject);
                }
            }
            1 => {
                if let Some(subject) = subjects.get_mut(&key) {
                    let epoch = epochs[&key];
                    if completed.insert((key, epoch), true).is_none() {
                        subject.complete(DropProbe::with_counter(encode(epoch, key), &counter));
                        created += 1;
                    }
                }
            }
            2 => {
                if let Ok(obs) = space.observe(&key) {
                    observations.push((key, epochs[&key], obs));
                }
            }
            3 => {
                if let Some((k, e, obs)) = observations.last() {
                    let expected = completed
                        .get(&(*k, *e))
                        .copied()
                        .unwrap_or(false)
                        .then(|| encode(*e, *k));
                    match obs.try_get() {
                        Some(value) => {
                            assert_eq!(
                                Some(value.tag),
                                expected,
                                "try_get diverged for key {k} epoch {e}"
                            );
                            created += 1;
                        }
                        None => assert_eq!(None, expected, "try_get lost a completed outcome"),
                    }
                }
            }
            4 => {
                if let Some(subject) = subjects.remove(&key) {
                    drop(subject); // retire: the generation's slot outlives it via observations
                }
            }
            5 => {
                if let Some((k, e, obs)) = observations.pop() {
                    let subject_gone = !subjects.contains_key(&k) || epochs[&k] != e;
                    let done = completed
                        .get(&(k, e))
                        .copied()
                        .unwrap_or(false)
                        .then(|| encode(e, k));
                    let refs = observations
                        .iter()
                        .filter(|(k2, e2, _)| k2 == &k && e2 == &e)
                        .count();
                    let result = obs.into_outcome();
                    if subject_gone && done.is_some() && refs == 0 {
                        let value = result.expect("into_outcome must move the last outcome");
                        assert_eq!(
                            value.tag, done.unwrap(),
                            "into_outcome moved a wrong-generation value"
                        );
                        drop(value);
                    } else {
                        assert!(
                            result.is_none(),
                            "into_outcome must refuse while shared/pending"
                        );
                    }
                }
            }
            _ => {
                observations.pop();
            }
        }
    }

    // Teardown: every remaining subject, observation, and the space drop
    // all outstanding values (slot finals, pooled resets, clones). Every
    // value created must be dropped exactly once.
    drop(observations);
    drop(subjects);
    drop(space);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        created,
        "created {created} values but observed {} drops",
        counter.load(Ordering::SeqCst)
    );
});
