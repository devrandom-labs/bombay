//! Card #281 — `Phased<P>` behavior on the caps surface (ADR-0024 D1–D10
//! re-seated per ADR-0026): the gate trio, replay-in-the-new-phase,
//! overflow handback, `goto(current)` ≡ no-op, and the
//! staleness-unrepresentable phase deadline riding the ADR-0025 plane.
//!
//! The full 6-scenario idiom-vs-phased equivalence oracle lives in
//! `phase_equivalence.rs`; this file pins the machine's own laws.

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};

use tokio::time::{Instant, sleep, timeout};

use bombay::{
    actor::{Flow, WeakActorRef},
    caps::{
        Actor, CapSet, Ctx, Disposition, Overflow, PhasePolicy, Phased, Shell, Stashing, Step,
        spawn,
    },
    error::ActorStopReason,
    mailbox::Capacity,
    test_support::terminate_bound,
};

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

async fn bounded<F: core::future::IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("await must resolve within the bound")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Applied {
        last: bool,
    },
    Processed(u32),
    /// The overridden overflow hook refused this payload — intact.
    ShedFull(u32),
    /// `handle` saw a message its phase declared away (must never appear).
    GateLeaked,
    TimedOutAt(Instant),
    Stopped(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Loading,
    Ready,
}

#[derive(Debug, Clone, bombay_macros::Msg)]
enum GMsg {
    /// Loading only; `last: true` transitions to Ready.
    Load { last: bool },
    /// Deferred by declaration while Loading; processed in Ready.
    Cmd(u32),
    /// Declared noise in Ready (`Ignore`) — dropped before the handler.
    Noise,
    /// `goto(Loading)` while Loading — the D3 `Goto(current)` ≡ no-op.
    GotoSame,
    /// In-band FIFO stop.
    Quit,
}

/// One plugged unit: phases, admission, deadline magnitude (Args-tunable
/// through the policy instance — the D8 channel), timeout reaction, and a
/// loud overflow refusal.
struct GPolicy {
    load_deadline: Option<Duration>,
}

impl PhasePolicy for GPolicy {
    type Actor = G;
    type Phase = Phase;

    fn build(args: &GArgs) -> Self {
        Self {
            load_deadline: args.load_deadline,
        }
    }

    fn initial(_: &GArgs) -> Phase {
        Phase::Loading
    }

    fn stash_capacity(args: &GArgs) -> Capacity {
        args.stash_cap
    }

    fn gate(phase: Phase, msg: &GMsg) -> Disposition {
        match (phase, msg) {
            (Phase::Loading, GMsg::Cmd(_)) => Disposition::Defer,
            (Phase::Ready, GMsg::Noise) => Disposition::Ignore,
            _ => Disposition::Deliver,
        }
    }

    fn phase_deadline(&self, phase: Phase) -> Option<Duration> {
        match phase {
            Phase::Loading => self.load_deadline,
            Phase::Ready => None,
        }
    }

    async fn on_phase_timeout(
        actor: &mut G,
        _: Phase,
        _: WeakActorRef<Shell<G>>,
        _: &mut Stashing<GMsg>,
    ) -> Result<Step<Phase>, Infallible> {
        let _ = actor.probe.send(Ev::TimedOutAt(Instant::now()));
        Ok(Step::Stop)
    }

    /// Loud typed refusal with the INTACT payload (D6 override).
    async fn on_defer_full(
        actor: &mut G,
        _: Phase,
        msg: GMsg,
        _: &mut Stashing<GMsg>,
    ) -> Result<Overflow<GMsg, Phase>, Infallible> {
        if let GMsg::Cmd(n) = msg {
            let _ = actor.probe.send(Ev::ShedFull(n));
        }
        Ok(Overflow::Handled(Step::Stay))
    }
}

struct GArgs {
    stash_cap: Capacity,
    load_deadline: Option<Duration>,
    probe: flume::Sender<Ev>,
}

struct G {
    probe: flume::Sender<Ev>,
}

#[derive(bombay_macros::Provide)]
struct GCaps {
    phased: Phased<GPolicy>,
}

impl CapSet<G> for GCaps {
    fn build(args: &GArgs) -> Self {
        Self {
            phased: Phased::build(args),
        }
    }
}

impl Actor for G {
    type Msg = GMsg;
    type Args = GArgs;
    type Error = Infallible;
    type Caps = GCaps;

    async fn init(args: GArgs, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { probe: args.probe })
    }

    async fn handle(&mut self, msg: GMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let phase = cx.cap::<Phased<GPolicy>>().phase();
        match (phase, msg) {
            (Phase::Loading, GMsg::Load { last }) => {
                let _ = self.probe.send(Ev::Applied { last });
                if last {
                    cx.cap::<Phased<GPolicy>>().goto(Phase::Ready);
                }
            }
            (Phase::Ready, GMsg::Cmd(n)) => {
                let _ = self.probe.send(Ev::Processed(n));
            }
            (Phase::Loading, GMsg::GotoSame) => {
                // D3: committing to the CURRENT phase must be a no-op —
                // no unstash, no deadline-anchor reset.
                cx.cap::<Phased<GPolicy>>().goto(Phase::Loading);
            }
            (_, GMsg::Quit) => return Ok(Flow::Stop),
            // Every remaining pair is gated Defer/Ignore; reaching here
            // means the gate leaked (Rust's exhaustiveness cannot see the
            // gate — the recorded ADR-0024 wart — so the arm exists and
            // trips the oracle instead of staying silent).
            _ => {
                let _ = self.probe.send(Ev::GateLeaked);
            }
        }
        Ok(Flow::Continue)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Shell<Self>>,
        reason: ActorStopReason,
    ) -> Result<(), Infallible> {
        let _ = self.probe.send(Ev::Stopped(reason.is_normal()));
        Ok(())
    }
}

fn spawn_g(
    stash_cap: usize,
    load_deadline: Option<Duration>,
) -> (bombay::caps::Handle<G>, flume::Receiver<Ev>) {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<G>(GArgs {
        stash_cap: cap(stash_cap),
        load_deadline,
        probe: tx,
    });
    (h, rx)
}

async fn collect_until_stopped(rx: &flume::Receiver<Ev>) -> Vec<Ev> {
    bounded(async {
        let mut got = Vec::new();
        loop {
            let ev = rx
                .recv_async()
                .await
                .expect("probe channel must not close before Stopped");
            let done = matches!(ev, Ev::Stopped(_));
            got.push(ev);
            if done {
                return got;
            }
        }
    })
    .await
}

/// The gate trio + D4 replay order in one run: commands deferred by
/// declaration while Loading replay IN Ready, ahead of the mailbox
/// backlog, in arrival order; declared noise is dropped before the
/// handler; the handler never sees a gated-away message.
#[tokio::test(start_paused = true)]
async fn gate_defers_ignores_and_replays_in_the_new_phase() {
    let (h, rx) = spawn_g(8, None);
    for msg in [
        GMsg::Load { last: false },
        GMsg::Cmd(1),
        GMsg::Cmd(2),
        GMsg::Load { last: true },
        GMsg::Cmd(3),
        GMsg::Noise,
        GMsg::Quit,
    ] {
        bounded(h.tell(msg)).await.expect("queued");
    }
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(
        evs,
        vec![
            Ev::Applied { last: false },
            Ev::Applied { last: true },
            Ev::Processed(1),
            Ev::Processed(2),
            Ev::Processed(3),
            Ev::Stopped(true),
        ],
        "replay lands in the NEW phase, ahead of the backlog, in arrival \
         order; Noise is a declared drop; the gate never leaks",
    );
}

/// D6 override: a deferral that finds the stash full routes the INTACT
/// message to `on_defer_full`, whose loud refusal absorbs it; the
/// retained command still replays on transition.
#[tokio::test(start_paused = true)]
async fn defer_overflow_hands_the_intact_message_to_the_hook() {
    let (h, rx) = spawn_g(1, None);
    for msg in [
        GMsg::Load { last: false },
        GMsg::Cmd(10),
        GMsg::Cmd(11),
        GMsg::Load { last: true },
        GMsg::Quit,
    ] {
        bounded(h.tell(msg)).await.expect("queued");
    }
    let evs = collect_until_stopped(&rx).await;

    assert_eq!(
        evs,
        vec![
            Ev::Applied { last: false },
            Ev::ShedFull(11),
            Ev::Applied { last: true },
            Ev::Processed(10),
            Ev::Stopped(true),
        ],
        "the overflowed payload reaches the hook intact; the retained one \
         replays on transition",
    );
}

/// D3: `goto(current)` commits to a no-op — the deferred command is NOT
/// released, and the deadline anchor is NOT reset: the Loading deadline
/// still fires at `init + 30 ms`, not `goto_time + 30 ms`.
#[tokio::test(start_paused = true)]
async fn goto_current_releases_nothing_and_keeps_the_deadline_anchor() {
    let start = Instant::now();
    let (h, rx) = spawn_g(8, Some(Duration::from_millis(30)));
    bounded(h.tell(GMsg::Cmd(1))).await.expect("queued");
    sleep(Duration::from_millis(10)).await;
    bounded(h.tell(GMsg::GotoSame)).await.expect("queued");

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::TimedOutAt(start + Duration::from_millis(30)),
            Ev::Stopped(true),
        ],
        "no Processed(1) — the stash stayed held — and the fire lands at \
         init+30ms, proving goto(current) never reset entered_at",
    );
}

/// The staleness-unrepresentable pin (replaces ADR-0024's epoch tests): a
/// 30 ms Loading deadline, transition to Ready at t≈10, clock run far past
/// 30 — the timeout NEVER fires, because the declarative slot changed with
/// the phase and the loop re-reads it every iteration.
#[tokio::test(start_paused = true)]
async fn a_left_phases_deadline_never_fires() {
    let (h, rx) = spawn_g(8, Some(Duration::from_millis(30)));
    bounded(h.tell(GMsg::Load { last: false }))
        .await
        .expect("queued");
    sleep(Duration::from_millis(10)).await;
    bounded(h.tell(GMsg::Load { last: true }))
        .await
        .expect("queued");
    sleep(Duration::from_millis(60)).await; // far past the Loading deadline
    bounded(h.tell(GMsg::Cmd(5))).await.expect("queued");
    bounded(h.tell(GMsg::Quit)).await.expect("queued");

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::Applied { last: false },
            Ev::Applied { last: true },
            Ev::Processed(5),
            Ev::Stopped(true),
        ],
        "no TimedOutAt anywhere: a left phase's deadline is unrepresentable",
    );
}

/// Spawn/tell/`Recipient`-mint smoke: a phased actor is an ordinary caps
/// actor — the closed menu holds (`Fsm::Msg == S::Msg` re-seated: no
/// envelope), so tell-side erasure works unchanged.
#[tokio::test(start_paused = true)]
async fn phased_actor_serves_recipients_unchanged() {
    let (h, rx) = spawn_g(8, None);
    bounded(h.tell(GMsg::Load { last: true }))
        .await
        .expect("queued");

    let recipient = h.recipient::<GMsg>();
    bounded(recipient.tell(GMsg::Cmd(9))).await.expect("queued");
    bounded(h.tell(GMsg::Quit)).await.expect("queued");

    let evs = collect_until_stopped(&rx).await;
    assert_eq!(
        evs,
        vec![
            Ev::Applied { last: true },
            Ev::Processed(9),
            Ev::Stopped(true),
        ],
        "a Recipient-minted tell rides the gate exactly as a direct tell",
    );
}
