//! Armed-timer cost vs a shared-wheel baseline (card #223 D3).
//!
//! Measures only the arming path: 10_000 long-delay timers are scheduled and
//! immediately detached. The timer tasks are intentionally leaked across
//! iterations because cancelling them would measure cancellation/teardown work,
//! not pure arming cost. A long delay (3600 s) ensures the timers never fire
//! inside the measurement window.

use core::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use tokio_util::time::DelayQueue;

use bombay::{
    actor::{Actor, ActorRef, Flow},
    mailbox::Mailboxed,
    message::Msg,
};

struct Bench;

#[derive(Debug)]
enum BenchMsg {
    Tick(u32),
}

impl Msg for BenchMsg {}
impl Mailboxed for Bench {
    type Msg = BenchMsg;
}

impl Actor for Bench {
    type Args = ();
    type Error = core::convert::Infallible;

    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Bench)
    }

    async fn handle(&mut self, _: BenchMsg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

fn timer_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer_arm");
    group.throughput(Throughput::Elements(10_000));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("runtime");

    group.bench_function("arm_send_after_10k", |b| {
        b.to_async(&rt).iter_with_setup(
            || spawn_plain::<Bench>(()),
            |actor_ref| async move {
                let mut handles = Vec::with_capacity(10_000);
                for i in 0..10_000 {
                    handles
                        .push(actor_ref.send_after(Duration::from_secs(3600), BenchMsg::Tick(i)));
                }
                // Detach the timers; the tasks remain parked on their long
                // sleeps and are intentionally leaked across iterations so
                // the measurement reflects arming cost only.
                black_box(handles.len());
                drop(handles);
            },
        );
    });

    group.bench_function("arm_delay_queue_insert_10k", |b| {
        b.to_async(&rt)
            .iter_with_setup(DelayQueue::<u32>::new, |mut queue| async move {
                for i in 0..10_000 {
                    queue.insert(i, Duration::from_secs(3600));
                }
                black_box(queue.len());
            });
    });

    group.finish();
}

criterion_group!(benches, timer_bench);
criterion_main!(benches);

/// The removed `Spawn` verb, bench-local over the public floor.
fn spawn_plain<A: bombay::actor::Actor>(args: A::Args) -> bombay::actor::ActorRef<A> {
    spawn_plain_cfg::<A>(bombay::actor::SpawnConfig::default(), args)
}

fn spawn_plain_cfg<A: bombay::actor::Actor>(
    config: bombay::actor::SpawnConfig,
    args: A::Args,
) -> bombay::actor::ActorRef<A> {
    let prepared = bombay::actor::PreparedActor::<A>::new(config);
    let actor_ref = prepared.actor_ref().clone();
    let _join = prepared.spawn(args);
    actor_ref
}
