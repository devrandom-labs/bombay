//! `send_after` / `send_interval` (card #223): the sanctioned non-pinning timer
//! surface. Design record: docs/superpowers/specs/2026-07-28-223-timers-design.md,
//! ADR-0018.

use core::{future::Future, ops::ControlFlow, time::Duration};
use std::panic::AssertUnwindSafe;

use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, ActorRef, Recipient, WeakActorRef, WeakRecipient},
    id::ActorId,
    trace,
};

/// Cancel handle for a scheduled send. Dropping it detaches (the timer still
/// fires); cancellation is only ever explicit.
#[derive(Debug)]
pub struct TimerHandle {
    token: CancellationToken,
}

impl TimerHandle {
    /// Cancels the timer. Idempotent. Wins iff it lands before the sleep
    /// expires: a cancelled-before-fire timer never delivers; once the sleep
    /// has completed the send is committed and `cancel` is a no-op.
    pub fn cancel(&self) {
        self.token.cancel();
    }
}

/// A weak, upgradeable delivery target. `fire` upgrades, sends with
/// backpressure, and reports whether the target can ever accept again.
trait WeakTarget: Send + 'static {
    type M: Send + 'static;
    fn id(&self) -> ActorId;
    fn fire(&self, msg: Self::M) -> impl Future<Output = ControlFlow<()>> + Send;
}

impl<A: Actor> WeakTarget for WeakActorRef<A> {
    type M = A::Msg;

    fn id(&self) -> ActorId {
        Self::id(self)
    }

    async fn fire(&self, msg: A::Msg) -> ControlFlow<()> {
        let Some(strong) = self.upgrade() else {
            trace::timer_fire_dropped(self.id());
            return ControlFlow::Break(());
        };
        if strong.tell(msg).await.is_err() {
            trace::timer_fire_dropped(strong.id());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

impl<M: Clone + Send + 'static> WeakTarget for WeakRecipient<M> {
    type M = M;

    fn id(&self) -> ActorId {
        Self::id(self)
    }

    async fn fire(&self, msg: M) -> ControlFlow<()> {
        let Some(strong) = self.upgrade() else {
            trace::timer_fire_dropped(self.id());
            return ControlFlow::Break(());
        };
        if strong.tell(msg).await.is_err() {
            trace::timer_fire_dropped(strong.id());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn spawn_once_with_guard<W, G>(weak: W, delay: Duration, msg: W::M, guard: G) -> TimerHandle
where
    W: WeakTarget,
    G: Send + 'static,
{
    let token = CancellationToken::new();
    let task_token = token.clone();
    let _join = tokio::spawn(async move {
        // The guard is owned by the spawned task; its Drop signals task exit.
        let _guard = guard;

        tokio::select! {
            biased;
            () = task_token.cancelled() => {
                trace::timer_cancelled(weak.id());
                return;
            }
            () = tokio::time::sleep(delay) => {}
        }
        // Fired: from here the send is committed — cancellation no longer
        // applies (aborting mid-send would hit flume's indeterminate-cancel
        // window, ADR-0008).
        let _ = weak.fire(msg).await;
    });
    TimerHandle { token }
}

impl<A: Actor> ActorRef<A> {
    /// Delivers `msg` to this actor after `delay`, as an ordinary menu
    /// message through the mailbox. The armed timer holds only a weak
    /// handle — it never keeps the actor alive (ADR-0003). Full mailbox at
    /// fire time = backpressure: delivered late, never lost.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_after(&self, delay: Duration, msg: A::Msg) -> TimerHandle {
        spawn_once_with_guard(self.downgrade(), delay, msg, ())
    }
}

impl<M: Clone + Send + 'static> Recipient<M> {
    /// [`ActorRef::send_after`] for the erased tell-side handle.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_after(&self, delay: Duration, msg: M) -> TimerHandle {
        spawn_once_with_guard(self.downgrade(), delay, msg, ())
    }
}

fn spawn_interval<W, F, G>(weak: W, period: Duration, mut make_msg: F, guard: G) -> TimerHandle
where
    W: WeakTarget,
    F: FnMut() -> W::M + Send + 'static,
    G: Send + 'static,
{
    let token = CancellationToken::new();
    let task_token = token.clone();
    let _join = tokio::spawn(async move {
        // The guard is owned by the spawned task; its Drop signals task exit.
        let _guard = guard;

        loop {
            tokio::select! {
                biased;
                () = task_token.cancelled() => {
                    trace::timer_cancelled(weak.id());
                    return;
                }
                () = tokio::time::sleep(period) => {}
            }
            // Fresh message per tick; a panicking factory kills only this
            // task (traced), never the actor (spec D7).
            let Ok(msg) = std::panic::catch_unwind(AssertUnwindSafe(&mut make_msg)) else {
                trace::timer_factory_panicked(weak.id());
                return;
            };
            // Arm-after-enqueue: the next sleep starts only after this send
            // completed (backpressure included) — no burst catch-up.
            if weak.fire(msg).await.is_break() {
                return;
            }
        }
    });
    TimerHandle { token }
}

impl<A: Actor> ActorRef<A> {
    /// Delivers a fresh `msg` to this actor every `period`, as ordinary menu
    /// messages through the mailbox. The first tick fires at `t = period`.
    /// The next tick arms only after the prior tick's message is enqueued
    /// (arm-after-enqueue), so a stalled mailbox does not burst on recovery.
    /// The armed timer holds only a weak handle and self-reaps if the target
    /// dies.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_interval<F>(&self, period: Duration, make_msg: F) -> TimerHandle
    where
        F: FnMut() -> A::Msg + Send + 'static,
    {
        spawn_interval(self.downgrade(), period, make_msg, ())
    }
}

impl<M: Clone + Send + 'static> Recipient<M> {
    /// [`ActorRef::send_interval`] for the erased tell-side handle.
    #[must_use = "dropping the handle detaches the timer; keep it to be able to cancel"]
    pub fn send_interval<F>(&self, period: Duration, make_msg: F) -> TimerHandle
    where
        F: FnMut() -> M + Send + 'static,
    {
        spawn_interval(self.downgrade(), period, make_msg, ())
    }
}

#[cfg(test)]
impl<A: Actor> ActorRef<A> {
    /// Test seam: like `send_interval` but holds `probe` inside the timer task
    /// so tests observe task exit via the guard's Drop.
    fn send_interval_probed<G, F>(&self, period: Duration, make_msg: F, probe: G) -> TimerHandle
    where
        G: Send + 'static,
        F: FnMut() -> A::Msg + Send + 'static,
    {
        spawn_interval(self.downgrade(), period, make_msg, probe)
    }
}

#[cfg(test)]
impl<A: Actor> ActorRef<A> {
    /// Test seam: like `send_after` but holds `probe` inside the timer task so
    /// tests observe task exit via the guard's Drop.
    fn send_after_probed<G: Send + 'static>(
        &self,
        delay: Duration,
        msg: A::Msg,
        probe: G,
    ) -> TimerHandle {
        spawn_once_with_guard(self.downgrade(), delay, msg, probe)
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use crate::{
        actor::{
            Actor, ActorRef, Flow, PreparedActor, Recipient, SpawnConfig,
            test_verbs::TestSpawn as _,
        },
        mailbox::{Capacity, Mailboxed},
        message::Msg,
        reply::ReplySender,
        test_support::terminate_bound,
    };

    /// Accumulates timer ticks so tests can ask what arrived.
    struct Sink {
        seen: Vec<u32>,
    }

    #[derive(Debug)]
    enum SinkMsg {
        Tick(u32),
        Read(ReplySender<Vec<u32>>),
    }

    impl Msg for SinkMsg {}
    impl Mailboxed for Sink {
        type Msg = SinkMsg;
    }

    impl Actor for Sink {
        type Args = ();
        type Error = core::convert::Infallible;

        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self { seen: Vec::new() })
        }

        async fn handle(&mut self, msg: Self::Msg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            match msg {
                SinkMsg::Tick(n) => self.seen.push(n),
                SinkMsg::Read(reply) => drop(reply.send(self.seen.clone())),
            }
            Ok(Flow::Continue)
        }
    }

    impl From<u32> for SinkMsg {
        fn from(v: u32) -> Self {
            SinkMsg::Tick(v)
        }
    }

    /// Fires a oneshot when dropped — how tests observe the detached timer
    /// task exited, whichever path it took.
    struct ExitGuard(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for ExitGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _sent = tx.send(());
            }
        }
    }

    /// Polls the sink until `n` ticks arrived.
    async fn wait_for_seen(actor_ref: &ActorRef<Sink>, n: usize) -> Vec<u32> {
        loop {
            let seen = actor_ref
                .ask(|reply| SinkMsg::Read(reply))
                .await
                .expect("sink replies while alive");
            if seen.len() >= n {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Invariant 1: the delayed message arrives as the exact menu value, through
    /// the mailbox, after the delay — and not before.
    #[tokio::test(start_paused = true)]
    async fn send_after_fires_exact_value_after_delay() {
        let actor_ref = Sink::spawn(());
        let _handle = actor_ref.send_after(Duration::from_secs(10), SinkMsg::Tick(42));

        // Before the deadline nothing may arrive: drain the runtime briefly at
        // a time strictly inside the delay window.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let seen = actor_ref
            .ask(|reply| SinkMsg::Read(reply))
            .await
            .expect("alive");
        assert_eq!(
            seen,
            Vec::<u32>::new(),
            "nothing may fire before the deadline"
        );

        // Cross the deadline; the tick must land.
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
            .await
            .expect("fired tick must arrive once the deadline passes");
        assert_eq!(seen, vec![42], "the exact scheduled value round-trips");
    }

    /// Invariant 3 + reaping: cancel before the deadline — advancing far past the
    /// deadline afterwards delivers NOTHING and the task exits.
    #[tokio::test(start_paused = true)]
    async fn cancel_before_fire_never_delivers_and_reaps_task() {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
        let actor_ref = Sink::spawn(());
        let handle = actor_ref.send_after_probed(
            Duration::from_secs(10),
            SinkMsg::Tick(1),
            ExitGuard(Some(exit_tx)),
        );
        handle.cancel();

        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("cancelled timer task must exit within the bound")
            .expect("guard dropped, not leaked");
        tokio::time::sleep(Duration::from_secs(60)).await; // far past the deadline
        let seen = actor_ref
            .ask(|reply| SinkMsg::Read(reply))
            .await
            .expect("alive");
        assert_eq!(
            seen,
            Vec::<u32>::new(),
            "a cancelled timer must never deliver"
        );
    }

    /// Invariant 3, fired edge: cancel AFTER the deadline is a no-op — the
    /// message still arrives (it is ordinary mail by then).
    #[tokio::test(start_paused = true)]
    async fn cancel_after_fire_is_noop() {
        let actor_ref = Sink::spawn(());
        let handle = actor_ref.send_after(Duration::from_secs(1), SinkMsg::Tick(7));
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
            .await
            .expect("tick arrives");
        handle.cancel(); // idempotent + late: both must be harmless
        handle.cancel();
        assert_eq!(seen, vec![7]);
    }

    /// Invariant 2: an actor whose ONLY remaining tie is an armed timer still
    /// ref-count-stops (the timer holds a weak handle; kameo deviation).
    #[tokio::test]
    async fn armed_timer_does_not_pin_refcount_stop() {
        let actor_ref = Sink::spawn(());
        let _handle = actor_ref.send_after(Duration::from_secs(3600), SinkMsg::Tick(1));
        let weak = actor_ref.downgrade();
        drop(actor_ref);

        tokio::time::timeout(terminate_bound(), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("with only an armed timer left, the actor must ref-count-stop");
    }

    /// Invariant 7 fate: target dead at fire — no panic, no delivery, task exits.
    #[tokio::test(start_paused = true)]
    async fn dead_target_at_fire_drops_cleanly() {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
        let actor_ref = Sink::spawn(());
        let _handle = actor_ref.send_after_probed(
            Duration::from_secs(10),
            SinkMsg::Tick(1),
            ExitGuard(Some(exit_tx)),
        );
        let weak = actor_ref.downgrade();
        drop(actor_ref);
        tokio::time::timeout(terminate_bound(), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("actor stops");

        // Cross the deadline; the task must exit cleanly (upgrade fails, drop+trace).
        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("timer task must exit after firing at a dead target")
            .expect("guard dropped, not leaked");
    }

    /// A sink whose handler blocks on the first tick until an external gate
    /// opens, so tests can pin a mailbox full condition.
    struct GatedSink {
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
        seen: Vec<u32>,
    }

    #[derive(Debug)]
    enum GatedMsg {
        Tick(u32),
        Read(ReplySender<Vec<u32>>),
    }

    impl Msg for GatedMsg {}
    impl Mailboxed for GatedSink {
        type Msg = GatedMsg;
    }

    impl Actor for GatedSink {
        type Args = tokio::sync::oneshot::Receiver<()>;
        type Error = core::convert::Infallible;

        async fn on_start(gate: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self {
                gate: Some(gate),
                seen: Vec::new(),
            })
        }

        async fn handle(&mut self, msg: Self::Msg, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            match msg {
                GatedMsg::Tick(n) => {
                    if let Some(gate) = self.gate.take() {
                        let _ = gate.await;
                    }
                    self.seen.push(n);
                }
                GatedMsg::Read(reply) => drop(reply.send(self.seen.clone())),
            }
            Ok(Flow::Continue)
        }
    }

    impl GatedSink {
        fn spawn_with_capacity(
            gate: tokio::sync::oneshot::Receiver<()>,
            capacity: usize,
        ) -> ActorRef<Self> {
            let cap = Capacity::try_from(capacity).expect("valid test capacity");
            let prepared = PreparedActor::<Self>::new(SpawnConfig {
                capacity: cap,
                ..Default::default()
            });
            let actor_ref = prepared.actor_ref().clone();
            let _join = prepared.spawn(gate);
            actor_ref
        }
    }

    async fn gated_wait_for_seen(actor_ref: &ActorRef<GatedSink>, n: usize) -> Vec<u32> {
        loop {
            let seen = actor_ref
                .ask(|reply| GatedMsg::Read(reply))
                .await
                .expect("sink replies while alive");
            if seen.len() >= n {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Interval delivers fresh messages per tick at the period cadence.
    #[tokio::test(start_paused = true)]
    async fn interval_ticks_arrive_with_fresh_messages() {
        let actor_ref = Sink::spawn(());
        let mut n = 0u32;
        let handle = actor_ref.send_interval(Duration::from_secs(1), move || {
            n += 1;
            SinkMsg::Tick(n)
        });
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 3))
            .await
            .expect("three ticks arrive");
        handle.cancel();
        assert_eq!(seen[..3], [1, 2, 3], "factory runs once per tick, in order");
    }

    /// Invariant 5 (arm-after-enqueue) + invariant 4 boundary: with consumption
    /// blocked and the mailbox full, ticks do NOT pile up beyond the structural
    /// bound (one in the handler + capacity queued + one awaiting enqueue). No
    /// burst catch-up after the stall clears.
    #[tokio::test(start_paused = true)]
    async fn interval_does_not_overlap_or_burst_when_mailbox_full() {
        // Gated sink with capacity 1: handler blocks until the gate opens.
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let actor_ref = GatedSink::spawn_with_capacity(gate_rx, 1);
        let mut n = 0u32;
        let handle = actor_ref.send_interval(Duration::from_secs(1), move || {
            n += 1;
            GatedMsg::Tick(n)
        });

        // 10 periods pass while consumption is blocked. Arm-after-enqueue means
        // at most: 1 tick in the blocked handler + 1 queued + 1 awaiting
        // capacity = ticks 1..=3 ever created; a free-running/bursting interval
        // would have created ~10.
        tokio::time::sleep(Duration::from_secs(10)).await;
        gate_tx.send(()).expect("handler is waiting on the gate");

        let seen = tokio::time::timeout(terminate_bound(), gated_wait_for_seen(&actor_ref, 3))
            .await
            .expect("blocked ticks drain after the gate opens");
        handle.cancel();
        assert!(
            seen.len() <= 4,
            "arm-after-enqueue bounds queued ticks structurally, got {}: {seen:?}",
            seen.len(),
        );
        assert_eq!(
            seen[..3],
            [1, 2, 3],
            "ticks stay ordered, none replayed or skipped-then-burst"
        );
    }

    /// Invariant 6: the interval loop reaps itself when the target dies.
    #[tokio::test(start_paused = true)]
    async fn interval_self_reaps_on_target_death() {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
        let actor_ref = Sink::spawn(());
        let _handle = actor_ref.send_interval_probed(
            Duration::from_secs(1),
            || SinkMsg::Tick(1),
            ExitGuard(Some(exit_tx)),
        );
        let weak = actor_ref.downgrade();
        drop(actor_ref);
        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("interval task must reap itself once the target is gone")
            .expect("guard dropped, not leaked");
        drop(weak);
    }

    /// D7 containment: a panicking factory kills only the timer task (traced);
    /// the actor keeps running.
    #[tokio::test(start_paused = true)]
    async fn interval_factory_panic_kills_timer_not_actor() {
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
        let actor_ref = Sink::spawn(());
        let _handle = actor_ref.send_interval_probed(
            Duration::from_secs(1),
            || -> SinkMsg { panic!("factory boom") },
            ExitGuard(Some(exit_tx)),
        );
        tokio::time::timeout(terminate_bound(), exit_rx)
            .await
            .expect("timer task must exit on factory panic")
            .expect("guard dropped, not leaked");
        assert!(
            actor_ref.is_alive(),
            "a factory panic must never touch the actor"
        );
    }

    /// The erased tell-side handle gets the same verbs: fire through a
    /// `Recipient<u32>` (menu conversion via `From`), cancel still works.
    #[tokio::test(start_paused = true)]
    async fn recipient_send_after_fires_through_erasure() {
        let actor_ref = Sink::spawn(());
        let recipient: Recipient<u32> = actor_ref.recipient();
        let _handle = recipient.send_after(Duration::from_secs(1), 5u32);
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 1))
            .await
            .expect("erased tick arrives");
        assert_eq!(seen, vec![5]);
    }

    /// Recipient interval on the erased path; cancel after two ticks stops it.
    #[tokio::test(start_paused = true)]
    async fn recipient_interval_and_cancel() {
        let actor_ref = Sink::spawn(());
        let recipient: Recipient<u32> = actor_ref.recipient();
        let mut n = 0u32;
        let handle = recipient.send_interval(Duration::from_secs(1), move || {
            n += 1;
            n
        });
        let seen = tokio::time::timeout(terminate_bound(), wait_for_seen(&actor_ref, 2))
            .await
            .expect("two erased ticks arrive");
        handle.cancel();
        assert_eq!(seen[..2], [1, 2]);
    }
}
