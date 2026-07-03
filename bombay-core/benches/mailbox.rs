//! Criterion benches for the mailbox hot path.
//!
//! The card's premise — a zero-box `tell` is cheaper than kameo's
//! `Box<dyn DynMessage>` enqueue — is un-templated: no framework ships this
//! shape, so we *measure* rather than assume (#112). These numbers are the
//! baseline the later two-tier wiring (#114/#118) is compared against.

use std::num::NonZeroUsize;

use bombay_core::mailbox::{Capacity, Mailboxed, Signal, bounded};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

struct Bench;
impl Mailboxed for Bench {
    type Msg = u64;
}

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("within max")
}

/// Pure enqueue cost: how long to `try_send` 1_000 messages into a mailbox with
/// spare capacity. `iter_batched_ref` keeps the `bounded()` setup out of the
/// measured region, and the fresh mailbox per batch never fills, so this isolates
/// the move-into-slot cost of a `tell`.
fn enqueue(c: &mut Criterion) {
    c.bench_function("tell_try_send_1k_u64", |b| {
        b.iter_batched_ref(
            || bounded::<Bench>(cap(1024)),
            |(tx, _rx)| {
                for i in 0..1000u64 {
                    tx.try_send(Signal::Message(black_box(i)))
                        .expect("capacity available");
                }
            },
            BatchSize::SmallInput,
        );
    });
}

/// End-to-end throughput: 1_000 `send`s and 1_000 `recv`s across a producer task
/// and the consumer, on a current-thread runtime.
fn roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    c.bench_function("send_recv_roundtrip_1k_u64", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = bounded::<Bench>(cap(1024));
                let producer = tokio::spawn(async move {
                    for i in 0..1000u64 {
                        tx.send(Signal::Message(black_box(i))).await.expect("send");
                    }
                });

                let mut received = 0u32;
                while received < 1000 {
                    let Some(Signal::Message(_)) = rx.recv().await else {
                        break;
                    };
                    received += 1;
                }
                producer.await.expect("producer");
                black_box(received)
            });
        });
    });
}

criterion_group!(benches, enqueue, roundtrip);
criterion_main!(benches);
