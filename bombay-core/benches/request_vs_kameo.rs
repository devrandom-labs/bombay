//! Head-to-head: the #118 request surface vs the vendored kameo fork.
//!
//! The component wins are measured elsewhere (channel: ADR-0001, ~2×; reply
//! port: ADR-0002, ~1.5×; allocations: `tests/alloc_request.rs`, 0/1 vs
//! kameo's per-message `Box::new` + `BoxReply`) — this bench measures whether
//! they *compose* end-to-end: a live actor, the real spawn/loop/handler path
//! on both sides, identical shape everywhere else.
//!
//! Method: same ~40 B command, same bounded(64) mailbox, same 2-worker tokio
//! runtime. `tell_pipeline_1k` awaits 1 000 tells then fences with one ask
//! (FIFO ⇒ the fence proves every tell was handled), so it measures the whole
//! pipeline including backpressure, not just enqueue. `tell_contended_4x250`
//! is the same element count from 4 concurrent producers. `ask_roundtrip` is
//! one request/reply per iteration.
//!
//! Measured 2026-07-18 (M-series laptop, sample-size 30, PR #184):
//!
//! | group                | bombay-core            | kameo                  | delta |
//! |----------------------|------------------------|------------------------|-------|
//! | tell_pipeline_1k     | 218.8 µs (4.57 Mmsg/s) | 352.3 µs (2.84 Mmsg/s) | 1.61× |
//! | tell_contended_4x250 | 242.1 µs (4.13 Mmsg/s) | 445.4 µs (2.25 Mmsg/s) | 1.84× |
//! | ask_roundtrip        | 7.75 µs                | 7.71 µs                | ≈1×   |
//!
//! The tell pipeline composes the component wins (~1.6–1.8×, tracking the
//! channel's 2×/2.7× discounted by the shared actor-loop cost). The ask round
//! trip is **parity**: one request/reply is dominated by two cross-task
//! wakeups (~µs each), which drown the ~11 ns typed-reply win and the
//! allocation delta — the honest number, recorded as such.

use std::{hint::black_box, num::NonZeroUsize};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::{Builder, Runtime};

/// A realistically-sized actor command (~40 bytes) — mirrors `benches/mailbox.rs`.
/// Only `id` is read by the handlers; the rest exist so the by-value move cost
/// a real `Signal` slot pays is measured honestly.
#[derive(Clone, Copy, Default, Debug)]
struct Command {
    id: u64,
    #[expect(dead_code, reason = "payload exists for its size, not its value")]
    correlation: u64,
    #[expect(dead_code, reason = "payload exists for its size, not its value")]
    kind: u32,
    #[expect(dead_code, reason = "payload exists for its size, not its value")]
    amount: i64,
    #[expect(dead_code, reason = "payload exists for its size, not its value")]
    flags: u64,
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

const CAP: usize = 64;
const PIPE: u64 = 1_000;
const PRODUCERS: u64 = 4;

mod core_side {
    use bombay_core::{
        actor::{Actor, ActorRef, Spawn as _},
        mailbox::{Capacity, Mailboxed},
        message::Msg,
        reply::ReplySender,
    };

    use super::{CAP, Command, NonZeroUsize};

    pub struct Counter {
        seen: u64,
    }
    #[derive(Debug)]
    pub enum CounterMsg {
        Note(Command),
        Get { reply: ReplySender<u64> },
    }
    impl Msg for CounterMsg {}
    impl Mailboxed for Counter {
        type Msg = CounterMsg;
    }
    impl Actor for Counter {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { seen: 0 })
        }
        async fn handle(
            &mut self,
            msg: CounterMsg,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            match msg {
                CounterMsg::Note(cmd) => self.seen = self.seen.wrapping_add(cmd.id),
                CounterMsg::Get { reply } => {
                    let _ = reply.send(self.seen);
                }
            }
            Ok(())
        }
    }

    pub fn spawn() -> ActorRef<Counter> {
        let cap = Capacity::new(NonZeroUsize::new(CAP).expect("nonzero")).expect("within max");
        Counter::spawn_with_capacity(cap, ())
    }
}

mod kameo_side {
    use bombay::prelude::*;

    use super::{CAP, Command};

    #[derive(Actor, Default)]
    pub struct Counter {
        seen: u64,
    }

    pub struct Note(pub Command);
    impl Message<Note> for Counter {
        type Reply = ();
        async fn handle(&mut self, Note(cmd): Note, _: &mut Context<Self, Self::Reply>) {
            self.seen = self.seen.wrapping_add(cmd.id);
        }
    }

    pub struct Get;
    impl Message<Get> for Counter {
        type Reply = u64;
        async fn handle(&mut self, _: Get, _: &mut Context<Self, Self::Reply>) -> u64 {
            self.seen
        }
    }

    pub fn spawn() -> ActorRef<Counter> {
        Counter::spawn_with_mailbox(Counter::default(), bombay::mailbox::bounded(CAP))
    }
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("bench runtime")
}

fn tell_pipeline(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("tell_pipeline_1k");
    group.throughput(Throughput::Elements(PIPE));

    let core_ref = rt.block_on(async { core_side::spawn() });
    group.bench_function("bombay_core", |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..PIPE {
                core_ref
                    .tell(core_side::CounterMsg::Note(command(i)))
                    .await
                    .expect("delivered");
            }
            let fence = core_ref
                .ask(|reply| core_side::CounterMsg::Get { reply })
                .await
                .expect("fence reply");
            black_box(fence)
        });
    });

    let kameo_ref = rt.block_on(async { kameo_side::spawn() });
    group.bench_function("kameo", |b| {
        b.to_async(&rt).iter(|| async {
            for i in 0..PIPE {
                kameo_ref
                    .tell(kameo_side::Note(command(i)))
                    .await
                    .expect("delivered");
            }
            let fence = kameo_ref.ask(kameo_side::Get).await.expect("fence reply");
            black_box(fence)
        });
    });

    group.finish();
}

fn tell_contended(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("tell_contended_4x250");
    group.throughput(Throughput::Elements(PIPE));
    let per_producer = PIPE / PRODUCERS;

    let core_ref = rt.block_on(async { core_side::spawn() });
    group.bench_function("bombay_core", |b| {
        b.to_async(&rt).iter(|| async {
            let workers: Vec<_> = (0..PRODUCERS)
                .map(|p| {
                    let target = core_ref.clone();
                    tokio::spawn(async move {
                        for i in 0..per_producer {
                            target
                                .tell(core_side::CounterMsg::Note(command(p * per_producer + i)))
                                .await
                                .expect("delivered");
                        }
                    })
                })
                .collect();
            for worker in workers {
                worker.await.expect("producer");
            }
            let fence = core_ref
                .ask(|reply| core_side::CounterMsg::Get { reply })
                .await
                .expect("fence reply");
            black_box(fence)
        });
    });

    let kameo_ref = rt.block_on(async { kameo_side::spawn() });
    group.bench_function("kameo", |b| {
        b.to_async(&rt).iter(|| async {
            let workers: Vec<_> = (0..PRODUCERS)
                .map(|p| {
                    let target = kameo_ref.clone();
                    tokio::spawn(async move {
                        for i in 0..per_producer {
                            target
                                .tell(kameo_side::Note(command(p * per_producer + i)))
                                .await
                                .expect("delivered");
                        }
                    })
                })
                .collect();
            for worker in workers {
                worker.await.expect("producer");
            }
            let fence = kameo_ref.ask(kameo_side::Get).await.expect("fence reply");
            black_box(fence)
        });
    });

    group.finish();
}

fn ask_roundtrip(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("ask_roundtrip");
    group.throughput(Throughput::Elements(1));

    let core_ref = rt.block_on(async { core_side::spawn() });
    group.bench_function("bombay_core", |b| {
        b.to_async(&rt).iter(|| async {
            let reply = core_ref
                .ask(|reply| core_side::CounterMsg::Get { reply })
                .await
                .expect("reply");
            black_box(reply)
        });
    });

    let kameo_ref = rt.block_on(async { kameo_side::spawn() });
    group.bench_function("kameo", |b| {
        b.to_async(&rt).iter(|| async {
            let reply = kameo_ref.ask(kameo_side::Get).await.expect("reply");
            black_box(reply)
        });
    });

    group.finish();
}

criterion_group!(benches, tell_pipeline, tell_contended, ask_roundtrip);
criterion_main!(benches);
