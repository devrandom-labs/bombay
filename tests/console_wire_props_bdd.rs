//! Cucumber harness for the ROOT `kameo` crate's in-tree console server/registry
//! **laws** — the `@property` / `@model` scenarios in
//! `tests/features/console/server_wire.properties.feature`, layered on top of the
//! example scenarios already wired in `tests/console_wire_bdd.rs`.
//!
//! Like the other console runners, this MUST be a STANDARD libtest test (no
//! `harness = false`): cucumber 0.23's libtest-writer does not implement
//! nextest's `--list` enumeration, so `nix flake check`'s `cargoNextest` sees it
//! as one ordinary test function. It builds only with the `testing` feature (see
//! `required-features` in Cargo.toml).
//!
//! ## Mechanism: deterministic boundary-loop, NOT the proptest async bridge
//!
//! The laws are universally-quantified over op sequences / poll counts and must
//! `reset_for_test()` + spawn REAL kameo actors + `await` `snapshot()` inside each
//! case. `proptest!` is a SYNC macro, while the cucumber step runs in an async
//! `#[tokio::test(flavor = "multi_thread")]` context. The documented async bridge
//! (`block_in_place` + `Handle::current().block_on`) was evaluated but is fragile
//! here: the body spawns real actors and runs nested `tokio::spawn` + `Barrier`
//! concurrency (the @model law), which deadlocks/misbehaves under a re-entrant
//! `block_on` on the same worker. Per the task's explicit FALLBACK, each law is
//! therefore implemented as a deterministic property LOOP over the GEN-named
//! boundary values PLUS a handful of seeded pseudo-random op sequences, asserting
//! the SAME ORACLE each iteration with specific `assert_eq!`/`assert!`. This still
//! checks the law at its boundaries; it trades proptest shrinking for async +
//! global-state robustness. `reset_for_test()` is called at the START of EACH case
//! (not just each scenario), since `SEQ`/`TOTAL_SPAWNED`/`REAPED_STOPPED` and the
//! registry are process-global and shared across the whole feature file.
//!
//! The suite keeps `.max_concurrent_scenarios(1)` AND every case resets the global
//! statics first, so cases never observe each other's counters.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use cucumber::{World, given, then, when};
use kameo::{error::Infallible, prelude::*};
use tokio::sync::Barrier;

/// A grave window far larger than any test latency, so a freshly-spawned actor is
/// never reaped out of a snapshot mid-case (the reap predicate is
/// `since.elapsed() > ttl`, registry.rs:481).
const KEEP: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct Echo;

impl Actor for Echo {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A tiny deterministic LCG so the "seeded pseudo-random" op sequences are
/// reproducible run-to-run (no flake from a wall-clock RNG). Numerical Recipes
/// constants.
struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

#[derive(Debug, Default, World)]
pub struct WiredPropsWorld;

/// Spawns a live Echo actor and waits for startup so its monitor is `Running` in
/// the registry. The returned ref must be kept alive by the caller for the
/// monitor to keep reporting a live probe.
async fn spawn_live() -> ActorRef<Echo> {
    let actor = Echo::spawn(Echo);
    actor.wait_for_startup().await;
    actor
}

// ===========================================================================
// @property @sequence — seq is strictly increasing and +1-stepped per poll
// ===========================================================================
//
// GEN: n in boundary-biased usize {1, 2, 5, 64, 256}; single producer, no
//      concurrent polls.
// ORACLE: monotonic counter model — seq_i == seq_0 + i (SEQ.fetch_add(1) once per
//         snapshot, registry.rs:447).
#[when(regex = r"^the client requests n snapshots back to back on that connection$")]
async fn law_seq_strictly_increasing(_world: &mut WiredPropsWorld) {
    // n = 0 is meaningless for "strictly increasing"; the feature's "any poll
    // count n" is checked over its boundary set, which starts at 1.
    for &n in &[1usize, 2, 5, 64, 256] {
        kameo::console::testing::reset_for_test();
        let _actor = spawn_live().await; // "at least one live actor"

        let mut seqs = Vec::with_capacity(n);
        for _ in 0..n {
            seqs.push(kameo::console::testing::snapshot(KEEP).await.seq);
        }

        assert_eq!(seqs.len(), n, "must have collected n={n} seqs");
        // Strictly increasing AND +1-stepped from seq_0 (single producer).
        let seq0 = seqs[0];
        for (i, &s) in seqs.iter().enumerate() {
            let expected = seq0
                .checked_add(i as u64)
                .expect("seq model must not overflow within the tested range");
            assert_eq!(
                s, expected,
                "with one producer, seq_{i} must equal seq_0 + {i}; got {seqs:?} for n={n}",
            );
        }
        assert!(
            seqs.windows(2).all(|w| w[1] > w[0]),
            "seqs must be strictly increasing, got {seqs:?} for n={n}",
        );
    }
}

#[then(regex = r"^the seq values observed form a strictly increasing sequence$")]
async fn then_seq_increasing_noop(_world: &mut WiredPropsWorld) {
    // The universally-quantified check ran in the When step over the whole
    // boundary set; both Then clauses are part of that one assertion.
}

#[then(
    regex = r"^each seq equals the previous one plus exactly one \(single producer, no concurrent polls\)$"
)]
async fn then_seq_plus_one_noop(_world: &mut WiredPropsWorld) {}

// ===========================================================================
// @property @sequence — uptime is monotonic; captured_at is a fresh wall-clock stamp
// ===========================================================================
//
// GEN: n in {1, 2, 8, 64}; polls in program order on one connection.
// ORACLE: uptime = START.elapsed() (an Instant, registry.rs:448-449) is monotonic →
//         non-decreasing in program order. captured_at = SystemTime::now() is a best-effort
//         WALL clock — NOT monotonic (a clock step can regress it; the client handles this,
//         see invariants.md:201) — so it is asserted to be a fresh/plausible current stamp,
//         never ordered. (Asserting captured_at ordering was a non-invariant; it caused a
//         CI clock-step flake.)
#[when(regex = r"^the client requests n snapshots in order$")]
async fn law_clocks(_world: &mut WiredPropsWorld) {
    for &n in &[1usize, 2, 8, 64] {
        kameo::console::testing::reset_for_test();
        let _actor = spawn_live().await;

        let mut captured: Vec<SystemTime> = Vec::with_capacity(n);
        let mut uptimes: Vec<Duration> = Vec::with_capacity(n);
        for _ in 0..n {
            let snap = kameo::console::testing::snapshot(KEEP).await;
            captured.push(snap.captured_at);
            uptimes.push(snap.uptime);
        }

        assert_eq!(captured.len(), n, "must have n={n} captured_at samples");
        // uptime (Instant) IS monotonic — assert it strictly.
        assert!(
            uptimes.windows(2).all(|w| w[1] >= w[0]),
            "uptime must be non-decreasing in program order, got {uptimes:?} for n={n}",
        );
        // captured_at (wall clock) — assert each is a fresh, plausible stamp, not ordered.
        let now = SystemTime::now();
        for c in &captured {
            let skew = now.duration_since(*c).unwrap_or_else(|e| e.duration());
            assert!(
                skew < Duration::from_secs(3600),
                "captured_at {c:?} must be a fresh wall-clock stamp near now ({now:?}); \
                 skew {skew:?} for n={n}",
            );
        }
    }
}

#[then(regex = r"^each snapshot's captured_at is a fresh wall-clock timestamp$")]
async fn then_captured_fresh_noop(_world: &mut WiredPropsWorld) {}

#[then(regex = r"^each snapshot's uptime is at or after the previous one's$")]
async fn then_uptime_non_decreasing_noop(_world: &mut WiredPropsWorld) {}

// ===========================================================================
// @property @sequence — total_stopped counts every stopped actor exactly once
// ===========================================================================
//
// GEN: op sequences over {spawn, stop, advance-clock-past-ttl-then-poll} of
//      length [0, 64]; include boundaries {0 stops, 1 reaped, 1 not-yet-reaped,
//      all reaped, none reaped}.
// ORACLE: integer model `ever_stopped` incremented once per stop. SUT computes
//         total_stopped = REAPED_STOPPED + stopped_now (registry.rs:454); a stop
//         migrates stopped_now -> REAPED on reap (registry.rs:472), so the sum ==
//         ever_stopped at every poll. Also pins that the unchecked `+` at
//         registry.rs:454 does not wrap within the tested range.

/// One op the schedule can take. `Reap` advances the clock past a zero ttl then
/// polls with ttl ZERO so every actor stopped for strictly > 0s is reaped; a
/// plain `Poll` polls with the huge KEEP ttl so nothing is reaped.
#[derive(Clone, Copy, Debug)]
enum Op {
    Spawn,
    Stop,
    Reap,
    Poll,
}

/// Runs one op schedule against the real registry and checks the ORACLE after a
/// final poll. `ever_stopped` is the integer model: incremented once per Stop.
/// Returns nothing; panics with a specific message on a violation.
async fn run_total_stopped_schedule(ops: &[Op], label: &str) {
    kameo::console::testing::reset_for_test();

    // Keep live actors alive so their monitors stay Running until we stop them.
    let mut live: Vec<ActorRef<Echo>> = Vec::new();
    let mut ever_stopped: u64 = 0;

    for op in ops {
        match op {
            Op::Spawn => live.push(spawn_live().await),
            Op::Stop => {
                if let Some(actor) = live.pop() {
                    actor.stop_gracefully().await.unwrap();
                    actor.wait_for_shutdown().await;
                    ever_stopped = ever_stopped
                        .checked_add(1)
                        .expect("ever_stopped model overflow");
                }
            }
            Op::Reap => {
                // A real ms-elapse so a just-stopped monitor's since.elapsed() is
                // strictly > 0, then a ttl-ZERO poll reaps every Stopped monitor.
                tokio::time::sleep(Duration::from_millis(2)).await;
                let _ = kameo::console::testing::snapshot(Duration::ZERO).await;
            }
            Op::Poll => {
                let _ = kameo::console::testing::snapshot(KEEP).await;
            }
        }
    }

    // Final poll with the huge ttl: anything still Stopped stays present and is
    // counted in stopped_now; previously reaped ones are in REAPED_STOPPED.
    let snap = kameo::console::testing::snapshot(KEEP).await;
    assert_eq!(
        snap.totals.total_stopped, ever_stopped,
        "[{label}] total_stopped must equal ever_stopped model = {ever_stopped}; \
         schedule ended with total_stopped = {}",
        snap.totals.total_stopped,
    );
    // No-wrap pin (rule 2): a realistic count never overflows u64; assert the
    // observed total round-trips through the model without saturation.
    assert!(
        snap.totals.total_stopped <= ops.len() as u64,
        "[{label}] total_stopped {} cannot exceed the number of ops {} (no wrap/double-count)",
        snap.totals.total_stopped,
        ops.len(),
    );
}

#[when(regex = r"^the client polls after the sequence$")]
async fn law_total_stopped_conserved(_world: &mut WiredPropsWorld) {
    // --- Named boundary schedules from GEN ---------------------------------
    // 0 stops (empty + spawn-only).
    run_total_stopped_schedule(&[], "empty").await;
    run_total_stopped_schedule(&[Op::Spawn, Op::Spawn, Op::Poll], "0-stops").await;
    // 1 stop, reaped before the final poll.
    run_total_stopped_schedule(&[Op::Spawn, Op::Stop, Op::Reap], "1-reaped").await;
    // 1 stop, NOT yet reaped (kept by the huge-ttl final poll).
    run_total_stopped_schedule(&[Op::Spawn, Op::Stop], "1-not-yet-reaped").await;
    // All reaped: several stops, all reaped before the final poll.
    run_total_stopped_schedule(
        &[
            Op::Spawn,
            Op::Spawn,
            Op::Spawn,
            Op::Stop,
            Op::Stop,
            Op::Stop,
            Op::Reap,
        ],
        "all-reaped",
    )
    .await;
    // None reaped: several stops, only KEEP polls in between.
    run_total_stopped_schedule(
        &[
            Op::Spawn,
            Op::Spawn,
            Op::Stop,
            Op::Poll,
            Op::Spawn,
            Op::Stop,
            Op::Poll,
        ],
        "none-reaped",
    )
    .await;
    // Mixed: reap some, leave others present, interleaved polls.
    run_total_stopped_schedule(
        &[
            Op::Spawn,
            Op::Stop,
            Op::Reap, // first stop migrates to REAPED
            Op::Spawn,
            Op::Spawn,
            Op::Stop, // second stop stays present
            Op::Poll,
        ],
        "mixed",
    )
    .await;

    // --- Seeded pseudo-random schedules over lengths up to the [0,64] bound -
    for seed in 0u64..6 {
        let mut rng = Lcg(0x5151_5151 ^ seed.wrapping_mul(0x9E37_79B9));
        // Lengths spanning the bound, including the max (64).
        let len = match seed {
            0 => 0,
            5 => 64,
            other => (rng.below(40) as usize) + (other as usize),
        };
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            ops.push(match rng.below(4) {
                0 => Op::Spawn,
                1 => Op::Stop,
                2 => Op::Reap,
                _ => Op::Poll,
            });
        }
        run_total_stopped_schedule(&ops, &format!("rand-seed-{seed}-len-{len}")).await;
    }
}

#[then(regex = r"^totals.total_stopped equals the number of actors that have ever stopped$")]
async fn then_total_stopped_eq_model_noop(_world: &mut WiredPropsWorld) {}

#[then(regex = r"^it never double-counts a reaped actor nor loses one mid-reap, for any schedule$")]
async fn then_total_stopped_no_dup_noop(_world: &mut WiredPropsWorld) {}

// ===========================================================================
// @model @linearizability — membership under one lock is a consistent snapshot
// ===========================================================================
//
// GEN: op sequence over {spawn(id), stop(id)} of length [1, 64] on tokio tasks
//      with a Barrier for real overlap; include empty-registry + single-actor
//      boundaries; >=8 concurrent pollers.
// ORACLE: a set model of live ids stepped by the ops — every produced snapshot's
//         id set must be consistent with SOME valid linearization of the partial
//         order (monitor set cloned under one lock, registry.rs:423-427):
//           * SUBSET of ever-spawned — no id materializes from thin air;
//           * SUPERSET of the actors already-live-and-registered BEFORE the
//             concurrent window that are never stopped — a registered live actor
//             is never dropped from a snapshot. (An actor whose spawn races the
//             poll may legitimately be absent: a linearization point before its
//             spawn is valid, so concurrently-spawned ids are upper-bounded only.)
//         All produced seqs are distinct (global SEQ).

/// Number of concurrent pollers (the GEN floor is >= 8).
const POLLERS: usize = 8;

/// Runs one concurrent spawn/stop interleaving with `POLLERS` overlapping pollers
/// gated on one Barrier, and checks the ORACLE clauses.
///
/// * `n_pre_live` actors are spawned and registered BEFORE the barrier and never
///   stopped — they are the guaranteed-present lower bound.
/// * `n_conc_spawn` actors are spawned CONCURRENTLY (inside barrier-gated tasks);
///   they may or may not appear in any given snapshot (upper bound only).
/// * `n_stop` actors are pre-spawned then stopped CONCURRENTLY; present-as-Stopped
///   or reaped are both valid, so they are not required to be present.
///
/// Panics with a specific message on a violation.
async fn run_membership_case(n_pre_live: usize, n_conc_spawn: usize, n_stop: usize, label: &str) {
    kameo::console::testing::reset_for_test();

    // Pre-existing live actors, registered before any concurrency starts: kept
    // alive for the whole case, so they MUST appear in every snapshot.
    let mut pre_live: Vec<ActorRef<Echo>> = Vec::with_capacity(n_pre_live);
    for _ in 0..n_pre_live {
        pre_live.push(spawn_live().await);
    }
    let pre_live_ids: HashSet<u64> = pre_live.iter().map(|a| a.id().sequence_id()).collect();

    // Pre-spawn the actors that will be CONCURRENTLY stopped, so their ids are
    // known up front; keep refs until their stopper task drops them.
    let mut to_stop: Vec<ActorRef<Echo>> = Vec::with_capacity(n_stop);
    for _ in 0..n_stop {
        to_stop.push(spawn_live().await);
    }
    let stop_ids: HashSet<u64> = to_stop.iter().map(|a| a.id().sequence_id()).collect();

    // Tasks: n_conc_spawn spawners + n_stop stoppers + POLLERS pollers, all
    // released together for genuine overlap.
    let extra_spawns = n_conc_spawn;
    let parties = extra_spawns + n_stop + POLLERS;
    let barrier = Arc::new(Barrier::new(parties));

    let spawners: Vec<_> = (0..extra_spawns)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let actor = Echo::spawn(Echo);
                actor.wait_for_startup().await;
                actor
            })
        })
        .collect();

    let stoppers: Vec<_> = to_stop
        .into_iter()
        .map(|actor| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                actor.stop_gracefully().await.unwrap();
                actor.wait_for_shutdown().await;
            })
        })
        .collect();

    let pollers: Vec<_> = (0..POLLERS)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                // A few polls each so snapshots land at varied interleavings.
                let mut snaps = Vec::with_capacity(3);
                for _ in 0..3 {
                    snaps.push(kameo::console::testing::snapshot(KEEP).await);
                }
                snaps
            })
        })
        .collect();

    // Keep the extra-spawn refs alive past the assertions so their monitors stay.
    let mut kept: Vec<ActorRef<Echo>> = Vec::with_capacity(extra_spawns);
    for s in spawners {
        kept.push(s.await.expect("spawner task must not panic"));
    }
    for s in stoppers {
        s.await.expect("stopper task must not panic");
    }
    let extra_ids: HashSet<u64> = kept.iter().map(|a| a.id().sequence_id()).collect();

    let mut all_snaps = Vec::new();
    for p in pollers {
        all_snaps.extend(p.await.expect("poller task must not panic"));
    }

    // ORACLE model sets.
    // ever_spawned = pre-live + concurrent spawns + the pre-spawned stoppers.
    //                Every class was spawned at SOME point — the upper bound.
    let ever_spawned: HashSet<u64> = pre_live_ids
        .iter()
        .chain(extra_ids.iter())
        .chain(stop_ids.iter())
        .copied()
        .collect();
    // guaranteed_present = actors registered before the concurrent window and
    // never stopped — the lower bound that must appear in every snapshot.
    let guaranteed_present: &HashSet<u64> = &pre_live_ids;

    // (a) all produced seqs distinct.
    let mut seqs: Vec<u64> = all_snaps.iter().map(|s| s.seq).collect();
    let unique_seqs: HashSet<u64> = seqs.iter().copied().collect();
    assert_eq!(
        unique_seqs.len(),
        seqs.len(),
        "[{label}] no two produced snapshots may share a seq; got {:?}",
        {
            seqs.sort_unstable();
            &seqs
        },
    );

    for snap in &all_snaps {
        let ids: Vec<u64> = snap.actors.iter().map(|a| a.id.0).collect();
        // (b) every actor id at most once per snapshot.
        let unique_ids: HashSet<u64> = ids.iter().copied().collect();
        assert_eq!(
            unique_ids.len(),
            ids.len(),
            "[{label}] each actor id must appear at most once in a snapshot, got {ids:?}",
        );
        // (c) consistent with some linearization: subset of ever-spawned AND
        // superset of the guaranteed-present (already-registered, never-stopped)
        // set. Concurrently-spawned ids and stopped ids are not required either
        // way — both presence and absence are valid linearizations.
        assert!(
            unique_ids.is_subset(&ever_spawned),
            "[{label}] snapshot membership {unique_ids:?} must be a subset of ever-spawned \
             {ever_spawned:?} (no actor from thin air)",
        );
        assert!(
            guaranteed_present.is_subset(&unique_ids),
            "[{label}] every already-registered, never-stopped actor {guaranteed_present:?} must \
             be present in membership {unique_ids:?} (no live actor dropped)",
        );
    }
}

#[when(regex = r"^a client polls while those operations run with real overlap$")]
async fn law_membership_consistent(_world: &mut WiredPropsWorld) {
    // Boundaries from GEN: empty registry, single actor, and mixed
    // spawn/stop interleavings up to the [1,64] bound. Args are
    // (pre_live guaranteed-present, concurrent spawns, concurrent stops).
    run_membership_case(0, 0, 0, "empty-registry").await;
    run_membership_case(1, 0, 0, "single-actor-live").await;
    run_membership_case(1, 1, 0, "single-live-plus-conc-spawn").await;
    run_membership_case(0, 1, 1, "single-spawn-and-stop").await;
    run_membership_case(4, 8, 0, "live-plus-conc-spawn").await;
    run_membership_case(0, 0, 8, "all-concurrently-stopped").await;
    run_membership_case(4, 16, 6, "mixed-4-16-6").await;
    run_membership_case(8, 32, 16, "mixed-8-32-16").await;
    run_membership_case(8, 40, 16, "max-8-40-16").await;
}

#[then(regex = r"^no two snapshots produced by the process ever share the same seq$")]
async fn then_membership_seqs_unique_noop(_world: &mut WiredPropsWorld) {}

#[then(regex = r"^every actor id appears at most once in the returned snapshot$")]
async fn then_membership_no_dup_noop(_world: &mut WiredPropsWorld) {}

#[then(
    regex = r"^the returned membership equals the registry's contents at one linearization point between the concurrent spawns/stops \(no half-applied batch, no torn entry\)$"
)]
async fn then_membership_linearizable_noop(_world: &mut WiredPropsWorld) {}

// ===========================================================================
// Given steps — each law spawns/resets per-case inside its When step, so these
// only need to exist (fail_on_skipped). They assert no global state up front.
// ===========================================================================

#[given(regex = r"^a console server with at least one live actor and one open client connection$")]
async fn given_server_live_actor_conn(_world: &mut WiredPropsWorld) {}

#[given(regex = r"^a console server and one open client connection$")]
async fn given_server_one_conn(_world: &mut WiredPropsWorld) {}

#[given(regex = r"^any poll count n$")]
async fn given_any_poll_count(_world: &mut WiredPropsWorld) {}

#[given(regex = r"^any sequence of spawns and stops with reaps interleaved at arbitrary points$")]
async fn given_any_spawn_stop_reap_sequence(_world: &mut WiredPropsWorld) {}

#[given(regex = r"^a console server and any concurrent interleaving of spawn and stop operations$")]
async fn given_server_concurrent_interleaving(_world: &mut WiredPropsWorld) {}

// ===========================================================================
// Runner — the whole properties file (this is the last task for it).
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn server_wire_property_laws() {
    // The whole properties file runs: every @property/@model scenario has step
    // defs, so `.fail_on_skipped()` turns any unwired scenario into a failure.
    //
    // Each scenario's universally-quantified law is checked in its When step by a
    // deterministic LOOP over the GEN boundary values plus seeded pseudo-random
    // op sequences (see the module doc — the async/proptest bridge is unsuitable
    // for the nested-concurrency @model law). Every case calls `reset_for_test()`
    // first; the process-global SEQ/registry are reset per case, so
    // `.max_concurrent_scenarios(1)` plus per-case reset keeps the statics
    // isolated.
    WiredPropsWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            // Anchor to CARGO_MANIFEST_DIR (= workspace root for the root crate):
            // nextest does not guarantee the test cwd is the workspace root, so a
            // bare relative path makes cucumber fail with "Could not read path"
            // under the nix-sandbox `cargoNextest` (which runs from another cwd).
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/console/server_wire.properties.feature"
            ),
            |_, _, _| true,
        )
        .await;
}
