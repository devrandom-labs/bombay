//! Coverage-guided fuzz target: raw `register_waker` accounting at fuzz
//! scale, over churned pooled slots. `future_ops` covers the future-poll
//! path; this one drives the direct API — a waker is REGISTERED exactly
//! when its generation is pending, and once registered it must fire
//! EXACTLY ONCE iff that generation eventually completes (the drain fires
//! it), never otherwise — including when the slot dies pooled-and-reset
//! or consumed before completing. Drop accounting (one `DropProbe`
//! counter: created == dropped after teardown) and (epoch,key)-tagged
//! integrity run alongside, so a leak, double-drop, or cross-generation
//! leak is a crash artifact.
//!
//! Ops (byte % 8): 0 register, 1 complete, 2 observe, 3 try_get, 4 retire,
//! 5 register_waker, 6 into_outcome, 7 drop an observation.
//! Keys: byte % 3.

#![no_main]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::fuzz_target;
use observe::{Observation, ObservationSpace, Subject};
use observe::probe::{CountWake, DropProbe};

const KEYS: u8 = 3;

fn encode(epoch: u64, key: u8) -> u64 {
    (epoch << 8) | u64::from(key)
}

fuzz_target!(|data: &[u8]| {
    let space = ObservationSpace::<u8, DropProbe>::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut created = 0usize;
    let mut subjects: HashMap<u8, Subject<u8, DropProbe>> = HashMap::new();
    let mut epochs: HashMap<u8, u64> = HashMap::new();
    let mut completed: HashMap<(u8, u64), bool> = HashMap::new();
    let mut observations: Vec<(u8, u64, Observation<DropProbe>)> = Vec::new();
    // (key, epoch, probe) of every successfully registered waker.
    let mut wakers: Vec<(u8, u64, Arc<CountWake>)> = Vec::new();

    for pair in data.chunks_exact(2).take(256) {
        let (op, key) = (pair[0] % 8, pair[1] % KEYS);
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
                    let expected = completed.get(&(*k, *e)).copied().unwrap_or(false);
                    match obs.try_get() {
                        Some(value) => {
                            assert!(expected, "try_get resolved a never-completed generation");
                            assert_eq!(value.tag, encode(*e, *k), "try_get cross-generation leak");
                            created += 1;
                        }
                        None => assert!(!expected, "try_get lost a completed outcome"),
                    }
                }
            }
            4 => {
                if let Some(subject) = subjects.remove(&key) {
                    drop(subject);
                }
            }
            5 => {
                if let Some((k, e, obs)) = observations.last() {
                    let is_pending = !completed.get(&(*k, *e)).copied().unwrap_or(false);
                    let (waker, probe) = CountWake::waker();
                    assert_eq!(
                        !obs.register_waker(&waker),
                        is_pending,
                        "register_waker completion flag diverged for key {k} epoch {e}"
                    );
                    if is_pending {
                        wakers.push((*k, *e, probe));
                    }
                }
            }
            6 => {
                if let Some((k, e, obs)) = observations.pop() {
                    let subject_gone = !subjects.contains_key(&k) || epochs[&k] != e;
                    let done = completed.get(&(k, e)).copied().unwrap_or(false);
                    let refs = observations
                        .iter()
                        .filter(|(k2, e2, _)| k2 == &k && e2 == &e)
                        .count();
                    let result = obs.into_outcome();
                    if subject_gone && done && refs == 0 {
                        let value = result.expect("into_outcome must move the last outcome");
                        assert_eq!(value.tag, encode(e, k), "into_outcome wrong generation");
                        drop(value);
                    } else {
                        assert!(result.is_none(), "into_outcome must refuse while shared/pending");
                    }
                }
            }
            _ => {
                observations.pop();
            }
        }
    }

    // Every registered waker fired exactly once iff its generation
    // completed. (Wakers of never-completed generations were destroyed
    // with their slots: pooled reset or consumed take.)
    for (k, e, probe) in &wakers {
        let expected = usize::from(completed.get(&(*k, *e)).copied().unwrap_or(false));
        assert_eq!(
            probe.count(),
            expected,
            "waker of key {k} epoch {e} fired {} times, expected {expected}",
            probe.count()
        );
    }

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
