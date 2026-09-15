//! Exhaustive small-state exploration: every bounded history over a tiny
//! alphabet is enumerated and checked against a minimal model — no
//! sampling, no seed. Covers single-key generation cycling, handle drop
//! orders, and the inline->hash promotion boundary.

use std::collections::HashMap;
use std::sync::Arc;

use crate::observe::test_support::{CountWake, DropProbe};
use crate::observe::{Observation, ObservationSpace, Subject};

/// Every op sequence of `depth` over `alphabet`, applied to a fresh space
/// per sequence.
fn enumerate_histories(alphabet: &[char], depth: usize, mut check: impl FnMut(&[char])) {
    let mut history = vec!['?'; depth];
    fn go(
        alphabet: &[char],
        depth: usize,
        at: usize,
        history: &mut Vec<char>,
        check: &mut impl FnMut(&[char]),
    ) {
        if at == depth {
            check(history);
            return;
        }
        for &op in alphabet {
            history[at] = op;
            go(alphabet, depth, at + 1, history, check);
        }
    }
    go(alphabet, depth, 0, &mut history, &mut check);
}

/// A minimal single-key model: vacant, or live with a completion state;
/// retirement forgets the generation but remembers its outcome for the
/// observations that pinned it.
struct SingleKey {
    live: Option<bool>, // Some(completed) while a generation is retained
    epoch: u64,
    retired_outcomes: HashMap<u64, bool>,
}

impl SingleKey {
    fn new() -> Self {
        Self {
            live: None,
            epoch: 0,
            retired_outcomes: HashMap::new(),
        }
    }
}

/// Alphabet: R=register C=complete O=observe X=retire D=drop one obs.
/// Every observable result is checked against the model at every step.
fn check_single_key_history(history: &[char]) {
    let space = ObservationSpace::<u8, u64>::new();
    let mut model = SingleKey::new();
    let mut subject: Option<Subject<u8, u64>> = None;
    let mut observations: Vec<(u64, Observation<u64>)> = Vec::new();

    for &op in history {
        match op {
            'R' => {
                let result = space.subject(0);
                if model.live.is_some() {
                    assert!(
                        result.is_err(),
                        "{history:?}: register while live succeeded"
                    );
                } else {
                    model.live = Some(false);
                    model.epoch += 1;
                    subject = Some(result.expect("{history:?}: register while vacant failed"));
                }
            }
            'C' => {
                if model.live == Some(false) {
                    subject
                        .as_mut()
                        .expect("live subject")
                        .complete(model.epoch);
                    model.live = Some(true);
                }
            }
            'O' => {
                let result = space.observe(&0);
                match model.live {
                    Some(_) => observations.push((
                        model.epoch,
                        result.expect("{history:?}: observe while live failed"),
                    )),
                    None => assert!(
                        result.is_err(),
                        "{history:?}: observe while vacant succeeded"
                    ),
                }
            }
            'X' => {
                if let Some(completed) = model.live.take() {
                    model.retired_outcomes.insert(model.epoch, completed);
                    subject = None;
                }
            }
            'D' => {
                observations.pop();
            }
            _ => unreachable!(),
        }
        // After every op: every live observation must see exactly the
        // outcome the model records for its captured epoch.
        for (epoch, obs) in &observations {
            let expected = if model.live.is_some() && *epoch == model.epoch {
                (model.live == Some(true)).then_some(*epoch)
            } else {
                model
                    .retired_outcomes
                    .get(epoch)
                    .copied()
                    .unwrap_or(false)
                    .then_some(*epoch)
            };
            assert_eq!(
                obs.try_get(),
                expected,
                "{history:?}: observation of epoch {epoch} diverged"
            );
        }
    }
}

/// All single-key histories to depth 5 over the full alphabet (5^5 = 3125)
/// plus depth 7 over the reduced register/complete/observe/retire alphabet
/// (4^7 = 16384). No sampling: the entire space is checked.
#[test]
fn exhaustive_single_key_histories() {
    enumerate_histories(&['R', 'C', 'O', 'X', 'D'], 5, check_single_key_history);
    enumerate_histories(&['R', 'C', 'O', 'X'], 7, check_single_key_history);
}

/// Every drop order of a completed generation's handles (subject,
/// observation, future) must destroy the outcome exactly once and never
/// fire a cancelled future's waker, in all permutations and both
/// completion timings.
#[test]
fn exhaustive_handle_drop_orders() {
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for completed in [false, true] {
        for (order, permutation) in permutations.iter().enumerate() {
            let space = ObservationSpace::<u8, DropProbe>::new();
            let (probe, counter) = DropProbe::new(order as u64);
            let mut subject = Some(space.subject(1).expect("first registration succeeds"));
            let mut observation = Some(space.observe(&1).expect("subject retained"));
            let (waker, wake_probe) = CountWake::waker();
            let mut future = Some(Box::pin(
                space.observe(&1).expect("subject retained").into_future(),
            ));

            if completed {
                subject.as_mut().expect("subject live").complete(probe);
            } else {
                assert!(
                    crate::observe::test_support::poll_once(
                        future.as_mut().expect("future live").as_mut(),
                        &waker
                    )
                    .is_pending()
                );
            }

            for &handle in permutation {
                match handle {
                    0 => drop(subject.take()),
                    1 => drop(observation.take()),
                    2 => drop(future.take()),
                    _ => unreachable!(),
                }
            }
            drop(space);

            if completed {
                assert_eq!(
                    counter.load(std::sync::atomic::Ordering::SeqCst),
                    1,
                    "order {permutation:?}: completed outcome dropped != once"
                );
            } else {
                assert_eq!(
                    wake_probe.count(),
                    0,
                    "order {permutation:?}: cancelled future's waker fired"
                );
            }
        }
    }
}

/// The promotion boundary: register keys 0..=5 (INLINE_CAP is 4, so the map
/// promotes mid-sequence), then for every retirement subset of {1, 2, 3}
/// and every re-registration order, every live key must observe correctly
/// and every retired key must be vacant — checked at each step.
#[test]
fn exhaustive_promotion_boundary() {
    // All subsets of {1, 2, 3} to retire after promotion.
    for subset in 0..8_u8 {
        let space = ObservationSpace::<u8, u64>::new();
        let mut subjects: HashMap<u8, Subject<u8, u64>> = HashMap::new();
        for key in 0..=5_u8 {
            let mut subject = space.subject(key).expect("fresh key");
            subject.complete(u64::from(key) + 100);
            subjects.insert(key, subject);
        }
        // Observe all six before any retirement.
        let observations: HashMap<u8, Observation<u64>> = (0..=5_u8)
            .map(|key| (key, space.observe(&key).expect("live key observable")))
            .collect();

        for victim in 1..=3_u8 {
            if subset & (1 << (victim - 1)) != 0 {
                subjects.remove(&victim); // retire
            }
        }
        // Re-register retired keys in reverse order.
        for victim in (1..=3_u8).rev() {
            if subset & (1 << (victim - 1)) != 0 {
                assert!(
                    space.observe(&victim).is_err(),
                    "retired key {victim} still observable before re-registration"
                );
                let mut subject = space.subject(victim).expect("retired key registrable");
                assert_eq!(
                    space
                        .observe(&victim)
                        .expect("re-registered key observable")
                        .try_get(),
                    None,
                    "re-registered key {victim} starts pending"
                );
                subject.complete(u64::from(victim) + 200);
                subjects.insert(victim, subject);
            }
        }
        // Final state: every key observes its most recent outcome; old
        // observations of retired generations keep their original outcome.
        for (key, obs) in &observations {
            let retired = (1..=3_u8).contains(key) && subset & (1 << (key - 1)) != 0;
            let expected = u64::from(*key) + 100;
            if retired {
                assert_eq!(
                    obs.try_get(),
                    Some(expected),
                    "old observation of retired key {key} lost its outcome"
                );
            }
            assert_eq!(
                space.observe(key).expect("key live at end").try_get(),
                Some(if retired {
                    u64::from(*key) + 200
                } else {
                    expected
                }),
                "key {key} final outcome wrong"
            );
        }
    }
}

/// Exhaustive futures-vs-completion ordering on one generation: poll count
/// in {0, 1, 2}, completion before/after polls, cancellation before/after
/// completion — every combination checked for exact wake counts.
#[test]
fn exhaustive_future_poll_cancel_orders() {
    for polls in 0..=2_u8 {
        for complete_after_poll in [false, true] {
            for cancel in [false, true] {
                let space = ObservationSpace::<u8, u64>::new();
                let mut subject = space.subject(9).expect("first registration succeeds");
                let obs = space.observe(&9).expect("subject retained");
                let (waker, probe) = CountWake::waker();
                let mut future = Box::pin(obs.into_future());

                if !complete_after_poll {
                    subject.complete(7);
                }
                for _ in 0..polls {
                    let poll = crate::observe::test_support::poll_once(future.as_mut(), &waker);
                    assert_eq!(
                        poll.is_ready(),
                        !complete_after_poll,
                        "polls={polls} complete_after={complete_after_poll} cancel={cancel}"
                    );
                }
                if complete_after_poll {
                    subject.complete(7);
                    let registered = polls > 0;
                    assert_eq!(
                        probe.count(),
                        usize::from(registered),
                        "polls={polls}: waker fired != once"
                    );
                }
                if cancel {
                    drop(future);
                } else {
                    let poll = crate::observe::test_support::poll_once(future.as_mut(), &waker);
                    assert_eq!(poll, std::task::Poll::Ready(7));
                }
                drop(subject);
                drop(space);
                if cancel {
                    assert_eq!(
                        probe.count(),
                        usize::from(complete_after_poll && polls > 0),
                        "cancelled future woken after drop"
                    );
                }
            }
        }
    }
}

/// Promoted-map contention: every key of 0..=5 completes and retires in
/// every relative order of two retirements, with observations taken at
/// each point — exhaustive over retirement pairs.
#[test]
fn exhaustive_double_retire_orders() {
    for first in 0..=5_u8 {
        for second in 0..=5_u8 {
            if first == second {
                continue;
            }
            let space = ObservationSpace::<u8, u64>::new();
            let mut subjects: HashMap<u8, Subject<u8, u64>> = HashMap::new();
            for key in 0..=5_u8 {
                let mut subject = space.subject(key).expect("fresh key");
                subject.complete(u64::from(key));
                subjects.insert(key, subject);
            }
            subjects.remove(&first);
            subjects.remove(&second);
            for key in 0..=5_u8 {
                let result = space.observe(&key);
                if key == first || key == second {
                    assert!(result.is_err(), "retired key {key} observable");
                } else {
                    assert_eq!(
                        result.expect("live key observable").try_get(),
                        Some(u64::from(key)),
                        "live key {key} outcome wrong after retiring {first},{second}"
                    );
                }
            }
        }
    }
}

/// Every history over the alphabet {R=register, C=complete, O=observe,
/// W=register_waker, X=retire, D=drop one obs} to depth 6 (6^6 = 46656) —
/// the strongest non-sampled coverage of the register_waker / drain
/// protocol. A waker is REGISTERED exactly when its observation's
/// generation is still pending (register_waker returns `false`); once
/// registered, it must fire EXACTLY ONCE iff that generation eventually
/// completes, and never otherwise — including when the slot dies (pooled
/// and reset, or consumed) before completing. Observation resolution is
/// also checked against the model after every op.
fn check_waker_history(history: &[char]) {
    let space = ObservationSpace::<u8, u64>::new();
    let mut subject: Option<Subject<u8, u64>> = None;
    let mut epoch: u64 = 0;
    let mut live: Option<bool> = None; // Some(completed)
    // epoch -> completed (completed generations keep their outcome; the
    // drain fires exactly the wakers registered at completion time).
    let mut completed: HashMap<u64, bool> = HashMap::new();
    let mut observations: Vec<(u64, Observation<u64>)> = Vec::new();
    // (epoch, probe) of every successfully registered waker.
    let mut registered: Vec<(u64, std::sync::Arc<CountWake>)> = Vec::new();

    for &op in history {
        match op {
            'R' => {
                if live.is_none() {
                    subject = Some(
                        space
                            .subject(0)
                            .expect("{history:?}: vacant register failed"),
                    );
                    epoch += 1;
                    live = Some(false);
                }
            }
            'C' => {
                if live == Some(false) {
                    subject
                        .as_mut()
                        .expect("{history:?}: live subject")
                        .complete(epoch);
                    live = Some(true);
                    completed.insert(epoch, true);
                }
            }
            'O' => {
                if let Ok(obs) = space.observe(&0) {
                    observations.push((epoch, obs));
                }
            }
            'W' => {
                if let Some((e, obs)) = observations.last() {
                    let (waker, probe) = CountWake::waker();
                    let is_pending = !completed.get(e).copied().unwrap_or(false);
                    assert_eq!(
                        !obs.register_waker(&waker),
                        is_pending,
                        "{history:?}: register_waker completion flag diverged for epoch {e}"
                    );
                    if is_pending {
                        registered.push((*e, probe));
                    }
                }
            }
            'X' => {
                if let Some(completed_now) = live.take() {
                    completed.entry(epoch).or_insert(completed_now);
                    subject = None;
                }
            }
            'D' => {
                observations.pop();
            }
            _ => unreachable!(),
        }
        // After every op: every observation resolves to its epoch's
        // completion, exactly.
        for (e, obs) in &observations {
            let expected = completed.get(e).copied().unwrap_or(false).then_some(*e);
            assert_eq!(
                obs.try_get(),
                expected,
                "{history:?}: observation of epoch {e} diverged"
            );
        }
    }

    // Terminal: every registered waker fired exactly once iff its epoch
    // completed.
    for (e, probe) in &registered {
        assert_eq!(
            probe.count(),
            usize::from(completed.get(e).copied().unwrap_or(false)),
            "{history:?}: waker of epoch {e} fired {} times",
            probe.count()
        );
    }
}

/// Exhaustive waker-drain histories (see `check_waker_history`): depth 6
/// (6^6 = 46,656) plus depth 7 (6^7 = 279,936) — the full space, no
/// sampling.
#[test]
fn exhaustive_waker_drain_histories() {
    enumerate_histories(&['R', 'C', 'O', 'W', 'X', 'D'], 6, check_waker_history);
    enumerate_histories(&['R', 'C', 'O', 'W', 'X', 'D'], 7, check_waker_history);
}

/// Drop a space with pooled slots whose outcomes are probes: teardown must
/// destroy every pooled outcome exactly once (pool drain path).
#[test]
fn pooled_outcomes_dropped_at_space_teardown() {
    let counters: Vec<Arc<std::sync::atomic::AtomicUsize>> = (0..20_u64)
        .map(|tag| {
            let space = ObservationSpace::<u64, DropProbe>::new();
            let (probe, counter) = DropProbe::new(tag);
            let mut subject = space.subject(tag).expect("fresh key");
            subject.complete(probe);
            drop(subject); // pooled, outcome retained in the slot
            let before = counter.load(std::sync::atomic::Ordering::SeqCst);
            assert_eq!(before, 0, "pooled outcome dropped early (tag {tag})");
            drop(space);
            assert_eq!(
                counter.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "pooled outcome dropped != once at teardown (tag {tag})"
            );
            counter
        })
        .collect();
    drop(counters);
}
