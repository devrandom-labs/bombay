//! Channel-primitive evaluation for the mailbox redesign (card #133).
//!
//! The mailbox needs an **async, bounded MPSC** (one consumer = the run-loop,
//! many producers = `ActorRef` clones). This benches the real async candidates
//! under that access pattern, plus crossbeam as a *sync* throughput ceiling
//! (not a viable mailbox — it has no async `recv`; shown to price the cost of
//! async integration).
//!
//! Same workload for every channel: `PRODUCERS` tasks each send `PER` messages
//! into a `CAP`-capacity channel; one consumer drains until all senders drop.
//! All async candidates run on one shared tokio multi-thread runtime, so the
//! executor is held constant and only the channel varies.

use std::thread;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

const CAP: usize = 256;
const PER: u64 = 4_000;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .expect("runtime")
}

async fn tokio_run(producers: u64) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(CAP);
    for _ in 0..producers {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..PER {
                tx.send(i).await.expect("send");
            }
        });
    }
    drop(tx);
    let mut count = 0u64;
    while rx.recv().await.is_some() {
        count += 1;
    }
    black_box(count);
}

async fn flume_run(producers: u64) {
    let (tx, rx) = flume::bounded::<u64>(CAP);
    for _ in 0..producers {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..PER {
                tx.send_async(i).await.expect("send");
            }
        });
    }
    drop(tx);
    let mut count = 0u64;
    while rx.recv_async().await.is_ok() {
        count += 1;
    }
    black_box(count);
}

async fn async_channel_run(producers: u64) {
    let (tx, rx) = async_channel::bounded::<u64>(CAP);
    for _ in 0..producers {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..PER {
                tx.send(i).await.expect("send");
            }
        });
    }
    drop(tx);
    let mut count = 0u64;
    while rx.recv().await.is_ok() {
        count += 1;
    }
    black_box(count);
}

async fn thingbuf_run(producers: u64) {
    let (tx, rx) = thingbuf::mpsc::channel::<u64>(CAP);
    for _ in 0..producers {
        let tx = tx.clone();
        tokio::spawn(async move {
            for i in 0..PER {
                tx.send(i).await.expect("send");
            }
        });
    }
    drop(tx);
    let mut count = 0u64;
    while rx.recv().await.is_some() {
        count += 1;
    }
    black_box(count);
}

/// Sync ceiling — std threads, not tasks. Not a viable async mailbox; here only
/// to show the raw lock-free throughput async has to pay to integrate with.
fn crossbeam_run(producers: u64) {
    let (tx, rx) = crossbeam_channel::bounded::<u64>(CAP);
    thread::scope(|scope| {
        for _ in 0..producers {
            let tx = tx.clone();
            scope.spawn(move || {
                for i in 0..PER {
                    tx.send(i).expect("send");
                }
            });
        }
        drop(tx);
        let mut count = 0u64;
        while rx.recv().is_ok() {
            count += 1;
        }
        black_box(count);
    });
}

fn bench(c: &mut Criterion, producers: u64, label: &str) {
    let rt = runtime();
    let mut group = c.benchmark_group(label);
    group.throughput(Throughput::Elements(producers * PER));

    group.bench_function("tokio_mpsc", |b| {
        b.iter(|| rt.block_on(tokio_run(producers)))
    });
    group.bench_function("flume", |b| b.iter(|| rt.block_on(flume_run(producers))));
    group.bench_function("async_channel", |b| {
        b.iter(|| rt.block_on(async_channel_run(producers)));
    });
    group.bench_function("thingbuf", |b| {
        b.iter(|| rt.block_on(thingbuf_run(producers)))
    });
    group.bench_function("crossbeam_sync_ceiling", |b| {
        b.iter(|| crossbeam_run(producers))
    });

    group.finish();
}

fn uncontended(c: &mut Criterion) {
    bench(c, 1, "uncontended_1p_1c");
}

fn contended(c: &mut Criterion) {
    bench(c, 4, "contended_4p_1c");
}

criterion_group!(benches, uncontended, contended);
criterion_main!(benches);
