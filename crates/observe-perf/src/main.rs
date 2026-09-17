//! Full-matrix performance harness for observe.
//!
//! One invocation measures the complete workload matrix and emits
//! `METRIC name=value` lines consumed by the performance harness:
//!
//! - primary observations/s (the frozen sequential workload);
//! - uncompleted pair-creation throughput;
//! - observer-cancellation throughput;
//! - fanout at representative observer counts (1/2/4/8);
//! - hot-path and wait-round-trip latency p50/p99;
//! - contention scaling at 1/2/4/8/16 threads;
//! - per-operation allocation count and bytes;
//! - affine await allocation count and bytes, including its one slot;
//! - retained bytes/blocks per publisher+observer and the after-drop residue.
//!
//! All workloads use fixed operation counts and independent pairs, with no
//! RNG, so runs are reproducible. A counting global allocator wraps the
//! system allocator for the allocation and retention phases; its counters
//! are Relaxed atomics read only after the measured window, so the
//! throughput phases are unaffected. The allocator is a measurement tool; it
//! does not touch the mechanism's code.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use observe::{affine_pair, pair};

/// Operations per throughput scenario.
const N: u64 = 1_000_000;
/// Operations for the percentile-latency scenario.
const N_LAT: u64 = 200_000;
/// Representative fanout observer counts.
const FANOUT_COUNTS: [usize; 4] = [1, 2, 4, 8];
/// Publisher+observer pairs per fanout scenario.
const M_FANOUT: u64 = 100_000;
/// Operation count for the allocation-accounting phase.
const N_ALLOC: u64 = 200_000;
/// Live publisher+observer pairs held for retention accounting.
const M_RETAINED: u64 = 10_000;

/// Total allocation events (`alloc` + `realloc`), monotonic.
static TOTAL_BLOCKS: AtomicU64 = AtomicU64::new(0);
/// Total bytes handed out (alloc sizes + realloc new sizes), monotonic.
static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
/// Live heap blocks; rises on `alloc`, falls on `dealloc`, unchanged by `realloc`.
static CURRENT_BLOCKS: AtomicU64 = AtomicU64::new(0);
/// Live heap bytes; rises on `alloc`, falls on `dealloc`, adjusted on `realloc`.
static CURRENT_BYTES: AtomicI64 = AtomicI64::new(0);

/// Global allocator that delegates to [`System`] and counts.
///
/// # Invariants
/// - Every request is forwarded unchanged to the system allocator, so the
///   returned pointers and accepted layouts are exactly what [`System`]
///   would produce; counting never influences allocation decisions.
/// - `alloc`/`dealloc` are paired with the same `Layout` by the caller (the
///   global allocator contract), so live-byte accounting stays consistent.
/// - `realloc` counts one new block, adds the new size to the totals, and
///   adjusts the live bytes by the difference against the old layout's size.
/// - Counters use `Relaxed` ordering: the throughput phases run
///   multi-threaded but the counters are only read after the measured work
///   has finished, so no ordering is required between counter updates.
struct CountingAllocator;

// SAFETY: the struct is stateless; every method forwards to System's
// implementation of the same contract and only updates counters.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegated to System with the caller-provided layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = u64::try_from(layout.size()).expect("layout size fits u64");
            TOTAL_BLOCKS.fetch_add(1, Ordering::Relaxed);
            TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
            CURRENT_BLOCKS.fetch_add(1, Ordering::Relaxed);
            CURRENT_BYTES.fetch_add(
                i64::try_from(size).expect("layout size fits i64"),
                Ordering::Relaxed,
            );
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: delegated to System; the caller guarantees the layout
        // matches the one passed to `alloc`.
        unsafe { System.dealloc(ptr, layout) };
        let size = u64::try_from(layout.size()).expect("layout size fits u64");
        CURRENT_BLOCKS.fetch_sub(1, Ordering::Relaxed);
        CURRENT_BYTES.fetch_sub(
            i64::try_from(size).expect("layout size fits i64"),
            Ordering::Relaxed,
        );
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: delegated to System with the caller-provided pointer/layout.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            TOTAL_BLOCKS.fetch_add(1, Ordering::Relaxed);
            TOTAL_BYTES.fetch_add(
                u64::try_from(new_size).expect("size fits u64"),
                Ordering::Relaxed,
            );
            let new = i64::try_from(new_size).expect("size fits i64");
            let old = i64::try_from(layout.size()).expect("layout size fits i64");
            CURRENT_BYTES.fetch_add(new - old, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn main() {
    // Primary: the frozen sequential workload (pair + observe + complete
    // + read; the observation is captured before completion by
    // construction).
    throughput("observations_per_second", seq_observe_first);
    throughput("seq_observe_first_ops_per_second", seq_observe_first);
    // Uncompleted pair churn: establish and tear down an observation
    // channel that never publishes.
    throughput("seq_pair_create_ops_per_second", seq_pair_create);
    // Cancellation: dropping observers must not obstruct completion.
    throughput("seq_cancel_observer_ops_per_second", seq_cancel_observer);
    // Fanout at representative observer counts.
    for &count in &FANOUT_COUNTS {
        fanout(count);
    }
    // Latency percentiles of the hot path and the blocking wait path.
    latency();
    wait_latency();
    // Contention scaling: 1/2/4/8/16 threads (16 oversubscribes the
    // machine's 12 physical cores, exposing scheduling degradation).
    for threads in [1_usize, 2, 4, 8, 16] {
        contention(threads);
    }
    // Allocation and retention accounting.
    allocation_and_retention();
}

/// Observe-first registration (the shape of the frozen workload): the
/// observation handle exists before the publisher completes.
fn seq_observe_first() -> Duration {
    let started = Instant::now();
    for outcome in 0..N {
        let (publisher, observation) = pair::<u64>();
        publisher.complete(outcome);
        black_box(observation.try_get());
    }
    started.elapsed()
}

/// Uncompleted pair churn: creation and drop of a pending channel.
fn seq_pair_create() -> Duration {
    let started = Instant::now();
    for _ in 0..N {
        drop(pair::<u64>());
    }
    started.elapsed()
}

/// Cancellation: dropping observers must not obstruct completion.
fn seq_cancel_observer() -> Duration {
    let started = Instant::now();
    for outcome in 0..N {
        let (publisher, observation) = pair::<u64>();
        drop(observation);
        publisher.complete(outcome);
    }
    started.elapsed()
}

/// Peer fanout: one publisher, `count` observers, complete, all read.
fn fanout(count: usize) {
    let started = Instant::now();
    for outcome in 0..M_FANOUT {
        let (publisher, observation) = pair::<u64>();
        let mut observations = Vec::with_capacity(count);
        for _ in 0..count {
            observations.push(observation.clone());
        }
        publisher.complete(outcome);
        for observation in &observations {
            black_box(observation.try_get());
        }
    }
    let elapsed = started.elapsed();
    emit_rate(
        &format!("fanout_{count}_observations_per_second"),
        M_FANOUT * u64::try_from(count).expect("fits u64"),
        elapsed,
    );
}

/// Latency percentiles of the hot path (observe-first).
fn latency() {
    let mut latencies = Vec::with_capacity(usize::try_from(N_LAT).expect("fits usize"));
    for outcome in 0..N_LAT {
        let started = Instant::now();
        let (publisher, observation) = pair::<u64>();
        publisher.complete(outcome);
        black_box(observation.try_get());
        latencies.push(started.elapsed());
    }
    latencies.sort_unstable();
    let p50 = latencies[latencies.len() / 2].as_nanos();
    let p99 = latencies[latencies.len() - latencies.len() / 100].as_nanos();
    println!("METRIC hot_path_p50_ns={p50}");
    println!("METRIC hot_path_p99_ns={p99}");
}

/// Wait round-trip latency: one persistent waiter thread races completion
/// across rounds; measures the blocking wait path end to end.
fn wait_latency() {
    const ROUNDS: u64 = 50_000;
    // The waiter signals after receiving each round's observation handle,
    // so the main thread never completes a pair whose observation the
    // waiter has not taken charge of.
    let (sender, receiver) = std::sync::mpsc::channel::<observe::Observation<u64>>();
    let observed = Arc::new(AtomicU64::new(0));
    let waiter = {
        let observed = Arc::clone(&observed);
        std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(usize::try_from(ROUNDS).expect("fits usize"));
            for round in 0..ROUNDS {
                let observation = receiver
                    .recv()
                    .expect("main thread sends one observation per round");
                observed.store(round + 1, Ordering::Release);
                let started = Instant::now();
                let outcome = observation.wait();
                latencies.push(started.elapsed());
                assert_eq!(outcome, round);
            }
            latencies
        })
    };
    for round in 0..ROUNDS {
        let (publisher, observation) = pair::<u64>();
        sender.send(observation).expect("waiter consumes handles");
        while observed.load(Ordering::Acquire) < round + 1 {
            std::thread::yield_now();
        }
        publisher.complete(round);
    }
    drop(sender);
    let mut latencies = waiter.join().expect("waiter completes");
    latencies.sort_unstable();
    let p50 = latencies[latencies.len() / 2].as_nanos();
    let p99 = latencies[latencies.len() - latencies.len() / 100].as_nanos();
    println!("METRIC wait_roundtrip_p50_ns={p50}");
    println!("METRIC wait_roundtrip_p99_ns={p99}");
}

/// Contention scaling: independent pairs raced by `threads` workers.
fn contention(threads: usize) {
    let per_thread = N / u64::try_from(threads).expect("fits u64");
    let barrier = Barrier::new(threads + 1);
    let started = std::thread::scope(|scope| {
        for worker in 0..threads {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                let mut checked = 0_u64;
                let base = u64::try_from(worker).expect("fits u64") * per_thread;
                for offset in 0..per_thread {
                    let (publisher, observation) = pair::<u64>();
                    publisher.complete(base + offset);
                    if observation.try_get().is_some() {
                        checked += 1;
                    }
                }
                black_box(checked);
            });
        }
        barrier.wait();
        Instant::now()
    });
    let elapsed = started.elapsed();
    emit_rate(&format!("contention_{threads}t_ops_per_second"), N, elapsed);
}

/// Allocation and retention accounting (see the module doc for the exact
/// phases). Uses the monotonic totals for per-operation cost and the live
/// counters for retention.
fn allocation_and_retention() {
    // Process-start baseline: the after-drop residue below is measured
    // against this, so it reflects only what the mechanism itself retains.
    let start = snapshot();

    // Phase 1: per-operation allocation cost of pair + complete + read.
    // Uses the monotonic totals: live bytes stay roughly flat here because
    // every publisher/observer is dropped within its own iteration.
    let before = snapshot();
    for outcome in 0..N_ALLOC {
        let (publisher, observation) = pair::<u64>();
        publisher.complete(outcome);
        black_box(observation.try_get());
    }
    let after = snapshot();
    emit_ratio(
        "alloc_bytes_per_op",
        after.total_bytes - before.total_bytes,
        N_ALLOC,
    );
    emit_ratio(
        "alloc_blocks_per_op",
        after.total_blocks - before.total_blocks,
        N_ALLOC,
    );

    // Phase 1b: an affine pair is polled pending, completed, and polled ready.
    // Its sole waiter stays inline, so the pair's observation slot must be
    // the only allocation in each operation.
    let before = snapshot();
    let mut context = Context::from_waker(Waker::noop());
    for outcome in 0..N_ALLOC {
        let (publisher, mut observation) = affine_pair();
        assert!(Pin::new(&mut observation).poll(&mut context).is_pending());
        publisher.complete(outcome);
        assert_eq!(
            Pin::new(&mut observation).poll(&mut context),
            Poll::Ready(outcome)
        );
    }
    let after = snapshot();
    emit_ratio(
        "affine_alloc_bytes_per_op",
        after.total_bytes - before.total_bytes,
        N_ALLOC,
    );
    emit_ratio(
        "affine_alloc_blocks_per_op",
        after.total_blocks - before.total_blocks,
        N_ALLOC,
    );

    // Phase 2: retained heap while M live publisher+observer pairs stay
    // pending (uncompleted publications).
    let mut publishers = Vec::with_capacity(usize::try_from(M_RETAINED).expect("fits usize"));
    let mut observations = Vec::with_capacity(usize::try_from(M_RETAINED).expect("fits usize"));
    let baseline = snapshot();
    for _ in 0..M_RETAINED {
        let (publisher, observation) = pair::<u64>();
        publishers.push(publisher);
        observations.push(observation);
    }
    let live = snapshot();
    emit_ratio(
        "retained_bytes_per_publisher_observer",
        u64::try_from(live.current_bytes - baseline.current_bytes).expect("live heap grows"),
        M_RETAINED,
    );
    emit_ratio(
        "retained_blocks_per_publisher_observer",
        live.current_blocks - baseline.current_blocks,
        M_RETAINED,
    );

    // Sanity: after dropping everything, the live heap must return to the
    // process-start baseline (only the stdout buffer and runtime remain).
    drop(publishers);
    drop(observations);
    let after_drop = snapshot();
    let residue = (after_drop.current_bytes - start.current_bytes).max(0);
    println!("METRIC retained_after_drop_bytes={residue}");
}

fn throughput(name: &str, run: impl FnOnce() -> Duration) {
    let elapsed = run();
    emit_rate(name, N, elapsed);
}

#[allow(
    clippy::cast_precision_loss,
    reason = "operation counts are <= 2^32 and exact in f64"
)]
fn emit_rate(name: &str, operations: u64, elapsed: Duration) {
    let rate = operations as f64 / elapsed.as_secs_f64();
    println!("METRIC {name}={rate:.3}");
}

fn emit_ratio(name: &str, total: u64, count: u64) {
    let per = total / count;
    println!("METRIC {name}={per}");
}

struct Snapshot {
    total_bytes: u64,
    total_blocks: u64,
    current_bytes: i64,
    current_blocks: u64,
}

fn snapshot() -> Snapshot {
    Snapshot {
        total_bytes: TOTAL_BYTES.load(Ordering::Relaxed),
        total_blocks: TOTAL_BLOCKS.load(Ordering::Relaxed),
        current_bytes: CURRENT_BYTES.load(Ordering::Relaxed),
        current_blocks: CURRENT_BLOCKS.load(Ordering::Relaxed),
    }
}
