//! Criterion benches for the mailbox hot path.
//!
//! The card's premise — a zero-box `tell` is cheaper than kameo's
//! `Box<dyn DynMessage>` enqueue — is un-templated: no framework ships this
//! shape, so we *measure* rather than assume (#112/#133).
//!
//! Payload is a realistically-sized command (~40 B), not a bare `u64`, so the
//! by-value copy cost that a real `Signal` slot pays is measured honestly.

use std::num::NonZeroUsize;

use bombay_core::mailbox::{Capacity, Mailbox, Mailboxed, Signal};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

/// A realistically-sized actor command (~40 bytes) — a handful of fields, closer
/// to a real closed-enum `Msg` variant than a bare `u64`.
#[derive(Clone, Copy, Default)]
struct Command {
    id: u64,
    correlation: u64,
    kind: u32,
    amount: i64,
    flags: u64,
}

struct Bench;
impl Mailboxed for Bench {
    type Msg = Command;
}

fn command(i: u64) -> Command {
    Command {
        id: i,
        correlation: i ^ 0x5555_5555,
        kind: (i & 0xff) as u32,
        amount: i as i64,
        flags: i.rotate_left(7),
    }
}

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("within max")
}

/// Pure enqueue cost: how long to `try_send` 1_000 commands into a mailbox with
/// spare capacity. `iter_batched_ref` keeps the `bounded()` setup out of the
/// measured region, and the fresh mailbox per batch never fills, so this isolates
/// the move-into-slot cost of a `tell`.
fn enqueue(c: &mut Criterion) {
    c.bench_function("tell_try_send_1k_command", |b| {
        b.iter_batched_ref(
            || Mailbox::<Bench>::bounded(cap(1024)),
            |(tx, _rx)| {
                for i in 0..1000u64 {
                    tx.try_send(Signal::Message(black_box(command(i))))
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

    c.bench_function("send_recv_roundtrip_1k_command", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (tx, mut rx) = Mailbox::<Bench>::bounded(cap(1024));
                let producer = tokio::spawn(async move {
                    for i in 0..1000u64 {
                        tx.send(Signal::Message(black_box(command(i))))
                            .await
                            .expect("send");
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
