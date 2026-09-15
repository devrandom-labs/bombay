//! Coverage-guided fuzz target: interprets the input as an op sequence
//! against the sequential semantics. Value tags encode (epoch, key) so any
//! cross-generation or cross-key leakage panics and becomes a crash
//! artifact.
//!
//! Ops (byte % 8): 0 register, 1 complete, 2 observe, 3 try_get, 4 retire,
//! 5 into_outcome, 6 register_waker(drop immediately), 7 drop an
//! observation. Keys: byte % 4 (INLINE_CAP boundary at 4 keys).

#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;
use observe::{Observation, ObservationSpace, Subject};

const KEYS: u8 = 4;

/// Value encoding: epoch in high bits, key in low 8 bits.
fn encode(epoch: u64, key: u8) -> u64 {
    (epoch << 8) | u64::from(key)
}

fuzz_target!(|data: &[u8]| {
    let space = ObservationSpace::<u8, u64>::new();
    let mut subjects: HashMap<u8, Subject<u8, u64>> = HashMap::new();
    let mut completed: HashMap<u8, u64> = HashMap::new(); // key -> value of live generation
    let mut epochs: HashMap<u8, u64> = HashMap::new();
    let mut observations: Vec<(u8, u64, Observation<u64>)> = Vec::new(); // (key, epoch, obs)
    let mut retired: HashMap<(u8, u64), Option<u64>> = HashMap::new();

    for pair in data.chunks_exact(2).take(256) {
        let (op, key) = (pair[0] % 8, pair[1] % KEYS);
        match op {
            0 => {
                if let Ok(subject) = space.subject(key) {
                    let epoch = epochs.get(&key).map_or(0, |e| e + 1);
                    epochs.insert(key, epoch);
                    subjects.insert(key, subject);
                    completed.remove(&key);
                }
            }
            1 => {
                if let Some(subject) = subjects.get_mut(&key) {
                    if !completed.contains_key(&key) {
                        let epoch = epochs[&key];
                        let value = encode(epoch, key);
                        subject.complete(value);
                        completed.insert(key, value);
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
                    let expected = if subjects.contains_key(k) && epochs[k] == *e {
                        completed.get(k).copied()
                    } else {
                        retired.get(&(*k, *e)).copied().flatten()
                    };
                    assert_eq!(
                        obs.try_get(),
                        expected,
                        "try_get diverged for key {k} epoch {e}"
                    );
                }
            }
            4 => {
                if let Some(subject) = subjects.remove(&key) {
                    let epoch = epochs[&key];
                    retired.insert((key, epoch), completed.get(&key).copied());
                    drop(subject);
                }
            }
            5 => {
                if let Some((k, e, obs)) = observations.pop() {
                    let subject_gone = !subjects.contains_key(&k) || epochs[&k] != e;
                    let done = if subject_gone {
                        retired.get(&(k, e)).copied().flatten()
                    } else {
                        completed.get(&k).copied()
                    };
                    let refs = observations.iter().filter(|(k2, e2, _)| k2 == &k && e2 == &e).count();
                    let result = obs.into_outcome();
                    if subject_gone && done.is_some() && refs == 0 {
                        assert_eq!(result, done, "into_outcome must move the last outcome");
                    } else {
                        assert_eq!(result, None, "into_outcome must refuse while shared/pending");
                    }
                }
            }
            6 => {
                if let Some((_, _, obs)) = observations.last() {
                    let (waker, _) = observe::probe::CountWake::waker();
                    let _ = obs.register_waker(&waker);
                }
            }
            _ => {
                observations.pop();
            }
        }
    }
});
