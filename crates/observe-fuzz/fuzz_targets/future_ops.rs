//! Coverage-guided fuzz target: futures, waker registration, polling, and
//! cancellation sequences. Exact wake-count assertions: only a live future's
//! current waker fires at completion; migrated and cancelled wakers never
//! fire. Futures here always poll with per-future distinct
//! wakers (the shared-waker topology deterministically hits shared-waker cancellation regression,
//! preserved separately and intentionally out of this target).

#![no_main]

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use libfuzzer_sys::fuzz_target;
use observe::probe::{CountWake, poll_once};
use observe::{ObservationFuture, ObservationSpace, Subject};

const KEYS: u8 = 2;

struct Fut {
    future: ObservationFuture<u64>,
    key: u8,
    epoch: u64,
    wakers: Vec<Arc<CountWake>>,
}

fuzz_target!(|data: &[u8]| {
    let space = ObservationSpace::<u8, u64>::new();
    let mut subjects: Vec<Option<Subject<u8, u64>>> = (0..KEYS).map(|_| None).collect();
    let mut completed = HashSet::new();
    let mut epochs = [0_u64; KEYS as usize];
    let mut futures: Vec<Fut> = Vec::new();
    // Wakers of cancelled futures with their fire count at cancellation:
    // nothing may fire them afterwards (checked at teardown).
    let mut cancelled_probes: Vec<(Arc<CountWake>, usize)> = Vec::new();

    for pair in data.chunks_exact(2).take(128) {
        let (op, key) = (pair[0] % 6, pair[1] % KEYS);
        let k = usize::from(key);
        match op {
            0 => {
                if let Ok(subject) = space.subject(key) {
                    epochs[k] += 1;
                    subjects[k] = Some(subject);
                }
            }
            1 => {
                if let Some(subject) = subjects[k].as_mut()
                    && completed.insert((key, epochs[k]))
                {
                    let value = (epochs[k] << 8) | u64::from(key);
                    subject.complete(value);
                    // Every live future of this generation: only its
                    // latest registered waker fires exactly once.
                    for f in &futures {
                        if f.key == key && f.epoch == epochs[k] {
                            for (i, probe) in f.wakers.iter().enumerate() {
                                let expected = usize::from(i + 1 == f.wakers.len());
                                assert_eq!(
                                    probe.count(),
                                    expected,
                                    "migrated/current waker count mismatch"
                                );
                            }
                        }
                    }
                }
            }
            2 => {
                if let Ok(obs) = space.observe(&key) {
                    futures.push(Fut {
                        future: obs.into_future(),
                        key,
                        epoch: epochs[k],
                        wakers: Vec::new(),
                    });
                }
            }
            3 => {
                if let Some(f) = futures.last_mut() {
                    let (waker, probe) = CountWake::waker();
                    let expected = completed.contains(&(f.key, f.epoch));
                    match poll_once(Pin::new(&mut f.future), &waker) {
                        Poll::Ready(value) => assert!(expected, "ready while pending: {value}"),
                        Poll::Pending => {
                            assert!(!expected, "pending while completed");
                            f.wakers.push(probe);
                        }
                    }
                }
            }
            4 => {
                if let Some(subject) = subjects[k].take() {
                    drop(subject); // retire
                }
            }
            _ => {
                if let Some(f) = futures.pop() {
                    for probe in &f.wakers {
                        cancelled_probes.push((Arc::clone(probe), probe.count()));
                    }
                    drop(f.future);
                }
            }
        }
    }
    for (probe, at_cancel) in &cancelled_probes {
        assert_eq!(
            probe.count(),
            *at_cancel,
            "a cancelled future's waker fired after cancellation"
        );
    }
});
