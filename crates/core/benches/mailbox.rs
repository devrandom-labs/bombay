//! Criterion benches for the mailbox hot path.
//!
//! The card's premise — a zero-box `tell` is cheaper than kameo's
//! `Box<dyn DynMessage>` enqueue — is un-templated: no framework ships this
//! shape, so we *measure* rather than assume (#112/#133).
//!
//! Payload is a realistically-sized command (~40 B), not a bare `u64`, so the
//! by-value copy cost that a real `Signal` slot pays is measured honestly.

use std::num::NonZeroUsize;

use bombay::SendContext;
use bombay::mailbox::{ActorId, Capacity, ControlSignal, Mailbox, Mailboxed, Recv, Signal};
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
            || Mailbox::<Bench>::bounded(cap(1024), ActorId::from_raw_for_test(0)),
            |(tx, _rx)| {
                for i in 0..1000u64 {
                    tx.try_send(Signal::Message {
                        msg: black_box(command(i)),
                        self_sender: tx.clone(),
                        ctx: SendContext::capture(),
                    })
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
                let (tx, mut rx) =
                    Mailbox::<Bench>::bounded(cap(1024), ActorId::from_raw_for_test(0));
                let producer = tokio::spawn(async move {
                    for i in 0..1000u64 {
                        tx.send(Signal::Message {
                            msg: black_box(command(i)),
                            self_sender: tx.clone(),
                            ctx: SendContext::capture(),
                        })
                        .await
                        .expect("send");
                    }
                });

                let mut received = 0u32;
                while received < 1000 {
                    let Some(Recv::Signal(Signal::Message { .. })) = rx.recv().await else {
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

/// Control-delivery latency vs user-queue depth (card #225, ADR-0021): one
/// `send_control` + one `recv` of that control signal, with the user lane
/// pre-filled to {0, 64, 1024} of a 1024-deep mailbox, plus a fully-saturated
/// 64-deep one (at-cap). A FLAT curve is the invariant — a watch/supervision
/// op must not queue behind the user backlog; the pre-#225 shape was a send
/// that BLOCKED at `at-cap`.
fn control_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    let mut group = c.benchmark_group("control_delivery_latency");
    for (cap_n, depth, label) in [
        (1024_usize, 0_u64, "depth_0"),
        (1024, 64, "depth_64"),
        (1024, 1024, "depth_1024"),
        (64, 64, "at_cap_64"),
    ] {
        group.bench_function(label, |b| {
            b.iter_batched_ref(
                || {
                    let (tx, rx) =
                        Mailbox::<Bench>::bounded(cap(cap_n), ActorId::from_raw_for_test(0));
                    for i in 0..depth {
                        tx.try_send(Signal::Message {
                            msg: command(i),
                            self_sender: tx.clone(),
                            ctx: SendContext::capture(),
                        })
                        .expect("setup depth fits capacity");
                    }
                    (tx, rx)
                },
                |(tx, rx)| {
                    tx.send_control(ControlSignal::Unwatch(ActorId::from_raw_for_test(1)))
                        .expect("the unbounded lane accepts");
                    let delivered = rt
                        .block_on(rx.recv())
                        .expect("the control signal is delivered ahead of the backlog");
                    assert!(
                        matches!(delivered, Recv::Control(_)),
                        "the control signal overtakes the pre-filled backlog",
                    );
                    black_box(delivered);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, enqueue, roundtrip, control_latency);
criterion_main!(benches);
