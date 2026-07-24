//! Criterion bench: fan-out to N watchers over the real send/handle path (#147).
//!
//! Baseline for a future slab/registry optimization (#122): today a fan-out
//! iterates a flat collection of watcher handles and enqueues to each. When that
//! collection becomes a slab/registry, this bench is the number to beat — so it
//! must measure the *production* send/handle path (real `MailboxSender` /
//! `Actor::handle`), never a reimplementation (CLAUDE rule 0: measure, don't
//! assume). The link/death-watch graph (#120) is not built yet, so the honest
//! fan-out this can measure is a notification delivered to N watcher mailboxes —
//! one event cloned out to every watcher, exactly as `error.rs` records the
//! semantics ("a death reason fans out to every watcher").
//!
//! Two arms isolate the two costs a slab/registry would move:
//!
//! * `watcher_fanout_dispatch` — pure fan-out enqueue: clone one notification
//!   into N production mailboxes via `try_send_message`, with no actors running.
//!   `iter_batched_ref` keeps the fleet construction out of the timed region, so
//!   this isolates the dispatch loop (iterate the registry, enqueue to each,
//!   including the per-send strong `self_sender` clone the #117 design pays).
//! * `watcher_fanout_roundtrip` — full send + handle: N spawned actors whose real
//!   `handle` acks receipt, so the producer observes that the fan-out actually
//!   reached and was processed by every watcher (real scheduler + handler cost).
//!
//! Both arms sweep the fan-out width, so the scaling curve (what a slab/registry
//! flattens) is visible, and `Throughput::Elements` reports per-watcher time.
//!
//! Measured 2026-07-18 (M-series laptop, criterion defaults, #186 before →
//! after the single-allocation `ActorRef`, ADR-0010):
//!
//! | group           | width | before    | after     | change |
//! |-----------------|-------|-----------|-----------|--------|
//! | dispatch        | 16    | 387 ns    | 390 ns    | noise  |
//! | dispatch        | 128   | 3.49 µs   | 3.18 µs   | −5.8%  |
//! | dispatch        | 1024  | 25.2 µs   | 26.1 µs   | +4.1%  |
//! | roundtrip       | 16    | 2.44 µs   | 2.02 µs   | −17.2% |
//! | roundtrip       | 128   | 19.5 µs   | 17.0 µs   | −13.1% |
//! | roundtrip       | 1024  | 157.6 µs  | 133.2 µs  | −14.8% |
//!
//! Reading: the dispatch arm drives bare `MailboxSender`s — no `ActorRef` in
//! the timed region — so its ±4–6% swings are machine noise on an untouched
//! path. The roundtrip arm includes the loop's per-message self-ref lift
//! (5 RMWs → 2 under ADR-0010), which is where the uniform −13…−17% comes
//! from.

use bombay_core::actor::{Actor, ActorRef, Spawn};
use bombay_core::error::Infallible;
use bombay_core::mailbox::{ActorId, Capacity, Mailbox, MailboxReceiver, MailboxSender, Mailboxed};
use bombay_core::message::Msg;
use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::mpsc;

/// Fan-out widths: how many watchers one event is dispatched to. Spans two orders
/// of magnitude so the per-watcher scaling a slab/registry would flatten is visible.
const WIDTHS: [usize; 3] = [16, 128, 1024];

/// A watcher notification — a small reason-like event cloned to each watcher. Kept
/// `Copy` and cold: the fan-out cost that matters is the enqueue plus the per-send
/// `self_sender` clone, not the payload.
#[derive(Clone, Copy, Debug)]
struct Notify {
    source: u64,
    code: u32,
}

fn notification() -> Notify {
    Notify {
        source: 0xABCD_ABCD,
        code: 7,
    }
}

fn cap(n: usize) -> Capacity {
    Capacity::try_from(n).expect("valid bench capacity")
}

/// Keys a bare mailbox for the dispatch arm — no running actor, so the fan-out
/// enqueue is measured with zero scheduler noise.
struct Sink;
impl Mailboxed for Sink {
    type Msg = Notify;
}

/// N fresh watcher mailboxes. The receivers are kept alive alongside the senders so
/// `try_send_message` sees an open, non-full channel (a dropped receiver would make
/// every send fail `Closed`).
fn build_mailboxes(n: usize) -> Vec<(MailboxSender<Sink>, MailboxReceiver<Sink>)> {
    (0..n)
        .map(|_| Mailbox::<Sink>::bounded(cap(4), ActorId::new(0)))
        .collect()
}

/// Pure fan-out enqueue: one notification cloned into each of N watcher mailboxes.
fn watcher_fanout_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("watcher_fanout_dispatch");
    for &n in &WIDTHS {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched_ref(
                || build_mailboxes(n),
                |fleet| {
                    let notify = black_box(notification());
                    for (tx, _rx) in fleet.iter() {
                        tx.try_send_message(notify)
                            .expect("fresh watcher mailbox has capacity");
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// A watcher that acknowledges each notification through its real `handle`, so the
/// producer can observe that every watcher processed the fan-out.
struct Watcher {
    ack: mpsc::UnboundedSender<()>,
}
impl Mailboxed for Watcher {
    type Msg = Notify;
}
impl Msg for Notify {}
impl Actor for Watcher {
    type Args = mpsc::UnboundedSender<()>;
    type Error = Infallible;

    async fn on_start(ack: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { ack })
    }

    async fn handle(
        &mut self,
        _: Notify,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        let _ = self.ack.send(());
        Ok(())
    }
}

/// Full send + handle: fan one notification out to N spawned watchers, then wait
/// for all N to acknowledge — the real scheduler + handler cost of a fan-out.
fn watcher_fanout_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");

    let mut group = c.benchmark_group("watcher_fanout_roundtrip");
    for &n in &WIDTHS {
        group.throughput(Throughput::Elements(n as u64));

        // Setup (not timed): a long-lived fleet reused across iterations — spawning
        // N actors is setup, not the fan-out we measure. Each watcher holds a clone
        // of the ack sender; the producer keeps only the receiver.
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<()>();
        let fleet: Vec<ActorRef<Watcher>> =
            rt.block_on(async { (0..n).map(|_| Watcher::spawn(ack_tx.clone())).collect() });
        drop(ack_tx);

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let notify = black_box(notification());
                    for watcher in &fleet {
                        watcher.tell(notify).await.expect("watcher alive");
                    }
                    for _ in 0..n {
                        ack_rx.recv().await.expect("every watcher acknowledges");
                    }
                });
            });
        });
        // `fleet` drops here: the watchers ref-count stop before the next width.
    }
    group.finish();
}

criterion_group!(benches, watcher_fanout_dispatch, watcher_fanout_roundtrip);
criterion_main!(benches);
