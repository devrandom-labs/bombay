//! Independent sequential reference model driven by proptest.
//!
//! The model is a plain `HashMap`-of-generations reimplementation of the
//! documented semantics (per-key live generation, completion, observation,
//! retirement, futures with per-future wakers, direct waker registration,
//! `into_outcome` move-out). Every operation's observable result is
//! compared against the model, including exact wake counts at completion.
//!
//! Keyspace is 8 keys, twice `INLINE_CAP` (4), so runs exercise both the
//! inline vector and the promoted hash map. Retirement biases pool reuse.
//!
//! NOTE (honest scoping): futures here always poll with wakers distinct
//! per future. The shared-waker-across-futures topology is excluded
//! because it deterministically hits shared-waker cancellation regression, which is preserved
//! separately in `future_cancel.rs`.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use crate::observe::test_support::{CountWake, DropProbe, poll_once};
use crate::observe::{Observation, ObservationFuture, ObservationSpace, Subject};
use proptest::prelude::*;

const KEYS: u8 = 8; // 2x INLINE_CAP: exercises inline and promoted maps

#[derive(Debug, Clone, Copy)]
enum Op {
    Register(u8),
    Complete(u8),
    Observe(u8),
    TryGet(usize),
    RegisterWaker(usize),
    IntoOutcome(usize),
    Retire(u8),
    DropObs(usize),
    NewFuture(usize),
    PollFuture(usize),
    MigrateFuture(usize),
    CancelFuture(usize),
}

#[derive(Debug, Clone, Copy)]
enum FuturePoll {
    Repoll,
    Migrate,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..KEYS).prop_map(Op::Register),
        4 => (0..KEYS).prop_map(Op::Complete),
        4 => (0..KEYS).prop_map(Op::Observe),
        3 => (0..16_usize).prop_map(Op::TryGet),
        2 => (0..16_usize).prop_map(Op::RegisterWaker),
        1 => (0..16_usize).prop_map(Op::IntoOutcome),
        3 => (0..KEYS).prop_map(Op::Retire),
        1 => (0..16_usize).prop_map(Op::DropObs),
        3 => (0..16_usize).prop_map(Op::NewFuture),
        3 => (0..16_usize).prop_map(Op::PollFuture),
        1 => (0..16_usize).prop_map(Op::MigrateFuture),
        1 => (0..16_usize).prop_map(Op::CancelFuture),
    ]
}

/// Ops for the churn drop-accounting property: no futures, focused on
/// generation lifecycle, retention, and destruction accounting.
#[derive(Debug, Clone, Copy)]
enum ChurnOp {
    Register(u8),
    Complete(u8),
    Observe(u8),
    TryGet(usize),
    Retire(u8),
    DropObs(usize),
    IntoOutcome(usize),
}

fn churn_op_strategy() -> impl Strategy<Value = ChurnOp> {
    prop_oneof![
        4 => (0..CHURN_KEYS).prop_map(ChurnOp::Register),
        4 => (0..CHURN_KEYS).prop_map(ChurnOp::Complete),
        3 => (0..CHURN_KEYS).prop_map(ChurnOp::Observe),
        2 => (0..16_usize).prop_map(ChurnOp::TryGet),
        3 => (0..CHURN_KEYS).prop_map(ChurnOp::Retire),
        1 => (0..16_usize).prop_map(ChurnOp::DropObs),
        1 => (0..16_usize).prop_map(ChurnOp::IntoOutcome),
    ]
}

const CHURN_KEYS: u8 = 4;

/// What the model knows about one key: absent from the map means vacant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyState {
    Live { epoch: u64, outcome: Option<u64> },
}

struct ObsHandle {
    observation: Observation<u64>,
    key: u8,
    epoch: u64,
}

struct FutureHandle {
    future: ObservationFuture<u64>,
    key: u8,
    epoch: u64,
    /// Waker migration history. Only the final entry remains registered;
    /// every earlier entry must stay unwoken at completion.
    wakers: Vec<(std::task::Waker, Arc<CountWake>)>,
}

struct Harness {
    space: ObservationSpace<u8, u64>,
    subjects: HashMap<u8, Subject<u8, u64>>,
    observations: HashMap<usize, ObsHandle>,
    futures: HashMap<usize, FutureHandle>,
    next_handle: usize,
}

struct Campaign {
    model: ModelState,
    harness: Harness,
    /// Pending direct `register_waker` registrations, tagged by (key,
    /// epoch): they outlive the observation handle they were made through
    /// (no owner), so they are tracked at epoch level. Registrations on a
    /// retired-then-recycled slot are drained by `reset` without firing —
    /// asserted by the pool tests, not here.
    registrations: Vec<(u8, u64, Arc<CountWake>)>,
}

/// The independent model: per-key live epoch, per-key epoch counter, and
/// the outcomes reached by retired epochs.
#[derive(Default)]
struct ModelState {
    keys: HashMap<u8, KeyState>,
    epochs: HashMap<u8, u64>,
    retired: HashMap<(u8, u64), Option<u64>>,
}

impl ModelState {
    fn live(&self, key: u8) -> Option<(u64, Option<u64>)> {
        match self.keys.get(&key) {
            Some(KeyState::Live { epoch, outcome }) => Some((*epoch, *outcome)),
            _ => None,
        }
    }

    /// The outcome visible to an observation of `(key, epoch)`, whether the
    /// epoch is still live or retired.
    fn outcome_of(&self, key: u8, epoch: u64) -> Option<u64> {
        match self.live(key) {
            Some((live_epoch, outcome)) if live_epoch == epoch => outcome,
            _ => self.retired.get(&(key, epoch)).copied().flatten(),
        }
    }

    fn register(&mut self, key: u8) -> bool {
        if self.live(key).is_some() {
            return false;
        }
        let epoch = self.epochs.get(&key).copied().map_or(0, |e| e + 1);
        self.epochs.insert(key, epoch);
        self.keys.insert(
            key,
            KeyState::Live {
                epoch,
                outcome: None,
            },
        );
        true
    }

    fn retire(&mut self, key: u8) {
        if let Some(KeyState::Live { epoch, outcome }) = self.keys.remove(&key) {
            self.retired.insert((key, epoch), outcome);
        }
    }

    fn outcome_value(key: u8, epoch: u64) -> u64 {
        epoch * 1000 + u64::from(key)
    }
}

fn run_case(ops: Vec<Op>) {
    let mut campaign = Campaign {
        model: ModelState::default(),
        registrations: Vec::new(),
        harness: Harness {
            space: ObservationSpace::new(),
            subjects: HashMap::new(),
            observations: HashMap::new(),
            futures: HashMap::new(),
            next_handle: 0,
        },
    };
    for op in ops {
        apply(&mut campaign, op);
    }
    // Teardown: every registered-but-never-completed waker must stay silent
    // and every completed epoch's registrations must have fired — verified
    // eagerly at Complete; nothing further to check here.
}

/// Number of strong handles the model believes pin the slot of `(key, epoch)`.
///
/// While the generation is retained, the slot is referenced twice on the
/// subject side: once by the key-table entry and once by the `Subject`
/// handle itself. Both go away at retirement (a retired slot is pooled
/// only when no observation pins it, in which case no handle exists to
/// call `into_outcome` through).
fn slot_refs(campaign: &Campaign, key: u8, epoch: u64) -> usize {
    let subject_side = if campaign
        .model
        .live(key)
        .is_some_and(|(live_epoch, _)| live_epoch == epoch)
    {
        2 // key-table entry + Subject handle
    } else {
        0
    };
    let obs_refs = campaign
        .harness
        .observations
        .values()
        .filter(|o| o.key == key && o.epoch == epoch)
        .count();
    let future_refs = campaign
        .harness
        .futures
        .values()
        .filter(|f| f.key == key && f.epoch == epoch)
        .count();
    subject_side + obs_refs + future_refs
}

fn apply(campaign: &mut Campaign, op: Op) {
    match op {
        Op::Register(key) => {
            let result = campaign.harness.space.subject(key);
            if campaign.model.register(key) {
                let subject =
                    result.unwrap_or_else(|e| panic!("model vacant but register failed: {e}"));
                campaign.harness.subjects.insert(key, subject);
            } else {
                assert!(
                    result.is_err(),
                    "model live but register succeeded for key {key}"
                );
            }
        }
        Op::Complete(key) => {
            let Some((epoch, outcome)) = campaign.model.live(key) else {
                return; // no live subject: op not applicable
            };
            if outcome.is_some() {
                return; // already completed: completing again would panic by contract
            }
            let value = ModelState::outcome_value(key, epoch);
            campaign
                .harness
                .subjects
                .get_mut(&key)
                .expect("model live => subject handle")
                .complete(value);
            campaign.model.keys.insert(
                key,
                KeyState::Live {
                    epoch,
                    outcome: Some(value),
                },
            );
            assert_completion_notifications(campaign, key, epoch, value);
        }
        Op::Observe(key) => {
            let result = campaign.harness.space.observe(&key);
            match campaign.model.live(key) {
                Some((epoch, _)) => {
                    let observation =
                        result.unwrap_or_else(|e| panic!("model live but observe failed: {e}"));
                    let id = campaign.harness.next_handle;
                    campaign.harness.next_handle += 1;
                    campaign.harness.observations.insert(
                        id,
                        ObsHandle {
                            observation,
                            key,
                            epoch,
                        },
                    );
                }
                None => assert!(
                    result.is_err(),
                    "model vacant but observe succeeded for key {key}"
                ),
            }
        }
        Op::TryGet(id) => {
            let Some(handle) = campaign.harness.observations.get(&id) else {
                return;
            };
            let expected = campaign.model.outcome_of(handle.key, handle.epoch);
            assert_eq!(
                handle.observation.try_get(),
                expected,
                "try_get diverged for key {} epoch {}",
                handle.key,
                handle.epoch
            );
        }
        Op::RegisterWaker(id) => {
            let Some(handle) = campaign.harness.observations.get(&id) else {
                return;
            };
            let (waker, probe) = CountWake::waker();
            let completed = campaign
                .model
                .outcome_of(handle.key, handle.epoch)
                .is_some();
            assert_eq!(
                handle.observation.register_waker(&waker),
                completed,
                "register_waker completion flag diverged"
            );
            if !completed {
                campaign
                    .registrations
                    .push((handle.key, handle.epoch, probe));
            }
        }
        Op::IntoOutcome(id) => {
            let Some(handle) = campaign.harness.observations.remove(&id) else {
                return;
            };
            let completed = campaign
                .model
                .outcome_of(handle.key, handle.epoch)
                .is_some();
            // The handle being moved is already removed from the harness:
            // exclusivity means NO other strong reference to the slot.
            let exclusive = slot_refs(campaign, handle.key, handle.epoch) == 0;
            let result = handle.observation.into_outcome();
            if completed && exclusive {
                assert_eq!(
                    result,
                    Some(ModelState::outcome_value(handle.key, handle.epoch)),
                    "into_outcome must move the completed outcome out of the last handle"
                );
            } else {
                assert_eq!(
                    result, None,
                    "into_outcome must refuse while pending or shared (key {} epoch {})",
                    handle.key, handle.epoch
                );
            }
        }
        Op::Retire(key) => {
            if campaign.model.live(key).is_none() {
                return;
            }
            campaign
                .harness
                .subjects
                .remove(&key)
                .expect("model live => subject handle");
            campaign.model.retire(key);
        }
        Op::DropObs(id) => {
            campaign.harness.observations.remove(&id);
        }
        Op::NewFuture(id) => {
            let Some(handle) = campaign.harness.observations.remove(&id) else {
                return;
            };
            let future = handle.observation.into_future();
            campaign.harness.futures.insert(
                id,
                FutureHandle {
                    future,
                    key: handle.key,
                    epoch: handle.epoch,
                    wakers: Vec::new(),
                },
            );
        }
        Op::PollFuture(id) => poll_future(campaign, id, FuturePoll::Repoll),
        Op::MigrateFuture(id) => poll_future(campaign, id, FuturePoll::Migrate),
        Op::CancelFuture(id) => {
            campaign.harness.futures.remove(&id);
        }
    }
}

fn poll_future(campaign: &mut Campaign, id: usize, poll: FuturePoll) {
    let Some(handle) = campaign.harness.futures.get_mut(&id) else {
        return;
    };
    let expected = campaign.model.outcome_of(handle.key, handle.epoch);
    let installs_waker = match poll {
        FuturePoll::Repoll => handle.wakers.is_empty(),
        FuturePoll::Migrate => true,
    };
    let (waker, probe) = if installs_waker {
        CountWake::waker()
    } else {
        // Re-poll with the same waker: idempotent re-registration.
        let (waker, probe) = handle.wakers.last().expect("non-empty waker history");
        (waker.clone(), Arc::clone(probe))
    };
    match poll_once(Pin::new(&mut handle.future), &waker) {
        Poll::Ready(value) => assert_eq!(
            Some(value),
            expected,
            "future resolved to the wrong outcome (key {} epoch {})",
            handle.key,
            handle.epoch
        ),
        Poll::Pending => {
            assert!(
                expected.is_none(),
                "future pending although model says completed (key {} epoch {})",
                handle.key,
                handle.epoch
            );
            if installs_waker {
                handle.wakers.push((waker, probe));
            }
        }
    }
}

/// After `complete`, every live future of the epoch must have only its latest
/// waker fired exactly once; migrated-away wakers must remain unwoken. Every
/// pending direct registration still fires exactly once.
fn assert_completion_notifications(campaign: &Campaign, key: u8, epoch: u64, value: u64) {
    for handle in campaign.harness.futures.values() {
        if handle.key == key && handle.epoch == epoch {
            for (i, (_, probe)) in handle.wakers.iter().enumerate() {
                let expected = usize::from(i + 1 == handle.wakers.len());
                assert_eq!(
                    probe.count(),
                    expected,
                    "future waker {i} of key {key} epoch {epoch} fired unexpectedly"
                );
            }
        }
    }
    for (i, (reg_key, reg_epoch, probe)) in campaign.registrations.iter().enumerate() {
        if *reg_key == key && *reg_epoch == epoch {
            assert_eq!(
                probe.count(),
                1,
                "direct registration {i} of key {key} epoch {epoch} fired != 1 time"
            );
        }
    }
    for handle in campaign.harness.observations.values() {
        if handle.key == key && handle.epoch == epoch {
            // Completed epoch: try_get must now agree with the model.
            assert_eq!(handle.observation.try_get(), Some(value));
        }
    }
}

proptest! {
    /// Random operation sequences conform to the independent model:
    /// registration conflicts, observation capture, completion visibility,
    /// exact wake counts, retirement isolation, and `into_outcome` rules.
    /// Default 256 cases (proptest default); long campaign runs override
    /// with `PROPTEST_CASES` when a deeper randomized run is needed.
    #[test]
    fn sequential_model_conformance(ops in prop::collection::vec(op_strategy(), 1..48)) {
        run_case(ops);
    }
}

proptest! {
    /// Retention/reclamation accounting at generation-churn scale: every
    /// completed outcome plus every `try_get` clone must be destroyed
    /// exactly once by full teardown, whatever the retire/re-register
    /// interleaving (pool recycling, `reset` drops, `into_outcome` takes,
    /// slot finals) — no leak, no double drop — while (epoch,key)-tagged
    /// integrity holds throughout.
    #[test]
    fn churn_drop_accounting_exactly_once(
        ops in prop::collection::vec(churn_op_strategy(), 1..64),
    ) {
        let space = ObservationSpace::<u8, DropProbe>::new();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut created = 0usize;
        let mut subjects: HashMap<u8, Subject<u8, DropProbe>> = HashMap::new();
        let mut epochs: HashMap<u8, u64> = HashMap::new();
        let mut completed: HashSet<(u8, u64)> = HashSet::new();
        let mut observations: Vec<(u8, u64, Observation<DropProbe>)> = Vec::new();

        let tag = |key: u8, epoch: u64| (epoch << 8) | u64::from(key);

        for op in ops {
            match op {
                ChurnOp::Register(key) => {
                    if let Ok(subject) = space.subject(key) {
                        let epoch = epochs.get(&key).map_or(0, |e| e + 1);
                        epochs.insert(key, epoch);
                        subjects.insert(key, subject);
                    }
                }
                ChurnOp::Complete(key) => {
                    if let Some(subject) = subjects.get_mut(&key) {
                        let epoch = epochs[&key];
                        if completed.insert((key, epoch)) {
                            subject.complete(DropProbe::with_counter(tag(key, epoch), &counter));
                            created += 1;
                        }
                    }
                }
                ChurnOp::Observe(key) => {
                    if let Ok(obs) = space.observe(&key) {
                        observations.push((key, epochs[&key], obs));
                    }
                }
                ChurnOp::TryGet(idx) => {
                    if let Some((k, e, obs)) = observations.get(idx % observations.len().max(1)) {
                        let expected = completed.contains(&(*k, *e));
                        match obs.try_get() {
                            Some(value) => {
                                assert_eq!(value.tag, tag(*k, *e), "try_get cross-generation leak");
                                assert!(expected, "try_get resolved a never-completed generation");
                                created += 1;
                            }
                            None => assert!(!expected, "try_get lost a completed outcome"),
                        }
                    }
                }
                ChurnOp::Retire(key) => {
                    if let Some(subject) = subjects.remove(&key) {
                        drop(subject);
                    }
                }
                ChurnOp::DropObs(idx) => {
                    if !observations.is_empty() {
                        observations.swap_remove(idx % observations.len());
                    }
                }
                ChurnOp::IntoOutcome(idx) => {
                    if observations.is_empty() {
                        continue;
                    }
                    let (k, e, obs) = observations.swap_remove(idx % observations.len());
                    let generation_retired = !subjects.contains_key(&k) || epochs[&k] != e;
                    let generation_completed = completed.contains(&(k, e));
                    let refs = observations
                        .iter()
                        .filter(|(k2, e2, _)| k2 == &k && e2 == &e)
                        .count();
                    let result = obs.into_outcome();
                    if generation_retired && generation_completed && refs == 0 {
                        let value = result.expect("into_outcome must move the last outcome");
                        assert_eq!(value.tag, tag(k, e), "into_outcome wrong generation");
                        drop(value);
                    } else {
                        assert!(result.is_none(), "into_outcome must refuse while shared/pending");
                    }
                }
            }
        }

        drop(observations);
        drop(subjects);
        drop(space);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::SeqCst),
            created,
            "created {created} values but observed {} drops",
            counter.load(std::sync::atomic::Ordering::SeqCst)
        );
    }
}

proptest! {
    /// Shared-waker cancellation (the shared-waker cancellation regression topology) must never
    /// lose the OUTCOME: futures on the same generation polled with one
    /// shared waker, an arbitrary proper subset cancelled, then the
    /// generation completes — every survivor must still resolve to the
    /// exact published value when the executor re-polls it (the healing
    /// path). Wake counts are intentionally NOT asserted here: the lost
    /// wake itself is shared-waker cancellation regression, preserved in `future_cancel.rs`; this
    /// property fences off any deeper corruption (wrong value, stuck
    /// pending, panic) in the same topology.
    #[test]
    fn shared_waker_cancellation_never_loses_outcome(
        key in 0..3u8,
        siblings in 2..5usize,
        cancellations in 0..4usize,
        rounds in 1..4u64,
    ) {
        let space = ObservationSpace::<u8, u64>::new();
        let (shared_waker, _probe) = CountWake::waker();
        let mut subject: Option<Subject<u8, u64>> = None;
        for round in 0..rounds {
            drop(subject.take()); // retire any previous generation
            let mut s = space.subject(key).expect("retired above: must be registrable");
            let value = round * 100 + u64::from(key) + 1;

            let mut futures: Vec<Option<ObservationFuture<u64>>> = (0..siblings)
                .map(|_| {
                    let obs = space.observe(&key).expect("live generation");
                    Some(obs.into_future())
                })
                .collect();
            for f in futures.iter_mut().flatten() {
                assert!(
                    poll_once(Pin::new(f), &shared_waker).is_pending(),
                    "pending before completion"
                );
            }
            // Cancel a proper subset (never all of them).
            for f in futures.iter_mut().take(cancellations.min(siblings - 1)) {
                *f = None; // cancel
            }
            s.complete(value);
            subject = Some(s);

            for f in futures.iter_mut().flatten() {
                assert_eq!(
                    poll_once(Pin::new(f), &shared_waker),
                    std::task::Poll::Ready(value),
                    "survivor failed to resolve after shared-waker cancellation"
                );
            }
        }
        drop(subject);
    }
}
