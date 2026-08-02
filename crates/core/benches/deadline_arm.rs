//! ADR-0025 deadline-arm cost (card #281, plan S5): per-message throughput
//! of the plain capability loop with the arm DISABLED (`next_deadline = None` —
//! the price every existing actor pays for the plane) vs ARMED at a
//! far-future instant (the steady re-arm cost: one `sleep_until` recreated
//! per iteration; O(1) wheel ops — hierarchical timing wheel, Varghese &
//! Lauck, IEEE/ACM ToN 5(6) 1997). If the armed delta hurts, the named
//! optimization is a pinned `Sleep` with `reset()`-on-change.

use core::{convert::Infallible, time::Duration};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use tokio::time::Instant;

use bombay::{
    actor::{Flow, WeakActorRef},
    capability::{
        Actor, ByState, CapSet, Ctx, DeadlinePolicy, Deadlined, Handle, Never, Shell, Step, spawn,
    },
    reply::ReplySender,
};

const BATCH: u32 = 10_000;

/// A minimal pump: `Tick` is a no-op; `Sync` replies so an iteration can
/// await full drainage (measurement covers delivery + the loop's arming
/// read per step).
struct Pump {
    due: Option<Instant>,
}

#[derive(Debug, bombay_macros::Msg)]
enum PumpMsg {
    Tick(u32),
    Sync { reply: ReplySender<()> },
}

struct PumpDl;
impl DeadlinePolicy<ByState<Pump>> for PumpDl {
    fn build(_: &Option<Duration>) -> Self {
        Self
    }
    fn next_deadline(&self, actor: &Pump, (): ()) -> Option<Instant> {
        actor.due
    }
    async fn on_deadline(
        &self,
        _: &mut Pump,
        (): (),
        _: WeakActorRef<Shell<Pump>>,
    ) -> Result<Step<Never>, Infallible> {
        unreachable!("the bench deadline sits 1 h out and never fires");
    }
}

#[derive(bombay_macros::Provide)]
struct PumpCaps {
    deadlined: Deadlined<PumpDl>,
}

impl CapSet<Pump> for PumpCaps {
    fn build(args: &Option<Duration>) -> Self {
        Self {
            deadlined: Deadlined::build(args),
        }
    }
}

impl Actor for Pump {
    type Msg = PumpMsg;
    type Args = Option<Duration>;
    type Error = Infallible;
    type Caps = PumpCaps;

    async fn init(due_in: Self::Args, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self {
            due: due_in.map(|d| Instant::now() + d),
        })
    }

    async fn handle(&mut self, msg: PumpMsg, _: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        match msg {
            PumpMsg::Tick(n) => {
                black_box(n);
            }
            PumpMsg::Sync { reply } => {
                let _ = reply.send(());
            }
        }
        Ok(Flow::Continue)
    }
}

async fn pump_batch(actor_ref: &Handle<Pump>) {
    for i in 0..BATCH {
        actor_ref
            .tell(PumpMsg::Tick(i))
            .await
            .expect("bench actor alive");
    }
    actor_ref
        .ask(|reply| PumpMsg::Sync { reply })
        .await
        .expect("bench actor alive");
}

fn deadline_arm_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("deadline_arm");
    group.throughput(Throughput::Elements(u64::from(BATCH)));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("runtime");

    group.bench_function("disabled_10k_msgs", |b| {
        b.to_async(&rt).iter_with_setup(
            || spawn::<Pump>(None),
            |actor_ref| async move { pump_batch(&actor_ref).await },
        );
    });

    group.bench_function("armed_far_future_10k_msgs", |b| {
        b.to_async(&rt).iter_with_setup(
            || spawn::<Pump>(Some(Duration::from_secs(3600))),
            |actor_ref| async move { pump_batch(&actor_ref).await },
        );
    });

    group.finish();
}

criterion_group!(benches, deadline_arm_bench);
criterion_main!(benches);
