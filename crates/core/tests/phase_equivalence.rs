//! Card #281 — the #231 6-scenario mode-blind equivalence oracle, ported
//! in-repo onto the capability surface (plan S8; spike record in the #231
//! design spec): the same abstract script drives BOTH lifecycle variants
//! — the idiom (a `Stashing` actor with a manual phase field, a PUBLIC
//! `LoadDeadline` menu variant armed via `send_after`, and every [F#]
//! bookkeeping obligation written by hand) and the machine (`Phased<P>`)
//! — and their observable probe sequences must be identical.
//!
//! Any forgotten idiom obligation ([F1] timer field, [F2] arm at init,
//! [F3]/[F6] unstash per exit edge, [F4]/[F4-dup] cancel per exit edge,
//! [F5] stale guards) breaks a scenario here; the machine concentrates
//! all of it into one policy declaration.
//!
//! Mode adaptation (spike S4): the idiom variant forges the late
//! `LoadDeadline` tell a queued-message timeout cannot avoid; the phased
//! variant has NO such message (the plane replaced it), so its runner
//! lets the clock run far past the left phase's deadline instead — both
//! must end with identical probes and no `StaleTimeoutLeaked`.

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};

use tokio::time::{Instant, sleep, timeout};

use bombay::{
    actor::{Flow, Normal, TimerHandle, WeakActorRef},
    capability::{
        Actor, Bounded, ByPhase, CapSet, Ctx, DeadlinePolicy, Deferred, Disposition, Handle,
        Overflow, PhasePolicy, PhaseView, Phased, Shell, StashFull, StashPolicy, Stashing, Step,
        spawn,
    },
    mailbox::Capacity,
    test_support::terminate_bound,
};

const LOAD_DEADLINE: Duration = Duration::from_secs(30);

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

/// The mode-blind observable vocabulary (spike `Probe`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Probe {
    Applied(u64),
    Processed(u64),
    Refused(u64),
    ShedFull(u64),
    DrainStarted,
    Snapshotted,
    LoadTimedOut,
    /// A timeout observed outside its phase — must NEVER appear.
    StaleTimeoutLeaked,
}

/// Variant-agnostic script operations (spike `Op`).
#[derive(Debug, Clone, Copy)]
enum Op {
    Replay(u64, bool),
    Cmd(u64),
    Drain,
    /// The stale-timeout race: the idiom forges the late `LoadDeadline`
    /// tell; the phased variant advances the clock far past the left
    /// phase's deadline (no message exists to forge — the point).
    StaleDeadline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Loading,
    Ready,
    Draining,
}

// ------------------------------------------------------------- phased ----

/// The machine's closed menu: 4 variants — no deadline variant (the state
/// timeout is framework-side, the spike's measured menu win).
#[derive(Debug, bombay_macros::Msg)]
enum FsmMsg {
    Replay { ev: u64, last: bool },
    Cmd { id: u64 },
    Drain,
    FlushDone,
}

/// The whole lifecycle protocol as ONE declaration (states, admission,
/// deadline, reactions).
struct AggPolicy;

impl PhasePolicy for AggPolicy {
    type Actor = AggFsm;
    type Phase = Phase;
    type Deferral = Bounded<AggStash>;
    type Timeout = AggDeadline;

    fn initial(_: &AggArgs) -> Phase {
        Phase::Loading
    }

    fn gate(phase: Phase, msg: &FsmMsg) -> Disposition<Deferred> {
        match (phase, msg) {
            // Commands cannot be decided before the fold completes.
            (Phase::Loading, FsmMsg::Cmd { .. }) => Disposition::Defer(Deferred),
            // Late/duplicate signals outside their phase: declared noise.
            (Phase::Ready | Phase::Draining, FsmMsg::Replay { .. })
            | (Phase::Loading | Phase::Ready, FsmMsg::FlushDone)
            | (Phase::Draining, FsmMsg::Drain) => Disposition::Ignore,
            _ => Disposition::Deliver,
        }
    }

    /// Loud shedding at stash capacity — the same typed-refusal behavior
    /// as the idiom's overflow branch.
    async fn on_defer_full(
        actor: &mut AggFsm,
        _: Phase,
        msg: FsmMsg,
        _: &mut Stashing<FsmMsg>,
    ) -> Result<Overflow<FsmMsg, Phase>, Infallible> {
        if let FsmMsg::Cmd { id } = msg {
            let _ = actor.probe.send(Probe::ShedFull(id));
        }
        Ok(Overflow::Handled(Step::Continue))
    }
}

/// The deferral bound, spelled once on the reused `StashPolicy`.
struct AggStash;
impl StashPolicy<AggFsm> for AggStash {
    fn capacity(args: &AggArgs) -> Capacity {
        args.0
    }
}

/// The plugged timeout seat: only Loading carries a deadline.
struct AggDeadline;

impl DeadlinePolicy<ByPhase<AggPolicy>> for AggDeadline {
    fn build(_: &AggArgs) -> Self {
        Self
    }

    fn next_deadline(&self, _: &AggFsm, view: PhaseView<AggPolicy>) -> Option<Instant> {
        match view.phase {
            Phase::Loading => view.entered_at.checked_add(LOAD_DEADLINE),
            Phase::Ready | Phase::Draining => None,
        }
    }

    async fn on_deadline(
        &self,
        actor: &mut AggFsm,
        view: PhaseView<AggPolicy>,
        _: WeakActorRef<Shell<AggFsm>>,
    ) -> Result<Step<Phase>, Infallible> {
        Ok(match view.phase {
            Phase::Loading => {
                let _ = actor.probe.send(Probe::LoadTimedOut);
                Step::Stop(Normal)
            }
            // Unreachable: no deadline is declared for these phases and a
            // left phase's deadline cannot fire. Kept total; a leak trips
            // every scenario's oracle assertion.
            Phase::Ready | Phase::Draining => {
                let _ = actor.probe.send(Probe::StaleTimeoutLeaked);
                Step::Continue
            }
        })
    }
}

type AggArgs = (Capacity, flume::Sender<Probe>);

struct AggFsm {
    probe: flume::Sender<Probe>,
    /// Folded rehydration state (stands in for the aggregate's real state).
    applied: u64,
}

#[derive(bombay_macros::Provide)]
struct AggFsmCaps {
    phased: Phased<AggPolicy>,
}

impl CapSet<AggFsm> for AggFsmCaps {
    fn build(args: &AggArgs) -> Self {
        Self {
            phased: Phased::build(args),
        }
    }
}

impl Actor for AggFsm {
    type Msg = FsmMsg;
    type Args = AggArgs;
    type Error = Infallible;
    type Caps = AggFsmCaps;

    async fn init((_, probe): AggArgs, _: Ctx<'_, Self>) -> Result<Self, Infallible> {
        Ok(Self { probe, applied: 0 })
    }

    async fn handle(&mut self, msg: FsmMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        let phase = cx.cap::<Phased<AggPolicy>>().phase();
        match (phase, msg) {
            (Phase::Loading, FsmMsg::Replay { ev, last }) => {
                self.applied = self.applied.wrapping_add(ev);
                let _ = self.probe.send(Probe::Applied(ev));
                if last {
                    cx.cap::<Phased<AggPolicy>>().goto(Phase::Ready);
                }
            }
            (Phase::Ready, FsmMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Processed(id));
            }
            (Phase::Ready | Phase::Loading, FsmMsg::Drain) => {
                let _ = self.probe.send(Probe::DrainStarted);
                // Simulated async snapshot flush completing later. The
                // closed menu holds: a plain self-tell, no envelope (the
                // spike's mock wart is gone by construction).
                let _ = cx.self_ref().tell(FsmMsg::FlushDone).await;
                cx.cap::<Phased<AggPolicy>>().goto(Phase::Draining);
            }
            (Phase::Draining, FsmMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Refused(id));
            }
            (Phase::Draining, FsmMsg::FlushDone) => {
                let _ = self.probe.send(Probe::Snapshotted);
                return Ok(Flow::Stop(Normal));
            }
            // Unreachable by declaration (gated Defer/Ignore); Rust's
            // exhaustiveness cannot see the gate (recorded ADR-0024 wart).
            _ => {}
        }
        Ok(Flow::Continue)
    }
}

// -------------------------------------------------------------- idiom ----

/// The idiom's menu: 5 variants — the rehydration deadline MUST be a
/// public message (the spike's menu-cost finding).
#[derive(Debug, bombay_macros::Msg)]
enum IdiomMsg {
    Replay { ev: u64, last: bool },
    Cmd { id: u64 },
    Drain,
    FlushDone,
    LoadDeadline,
}

struct AggIdiom {
    probe: flume::Sender<Probe>,
    applied: u64,
    /// Manual phase field the framework cannot observe.
    phase: Phase,
    /// [F1] the timer handle carried as a field.
    timer: Option<TimerHandle>,
}

type IdiomArgs = (Capacity, flume::Sender<Probe>);

#[derive(bombay_macros::Provide)]
struct AggIdiomCaps {
    stash: Stashing<IdiomMsg>,
}

impl CapSet<AggIdiom> for AggIdiomCaps {
    fn build((capacity, _): &IdiomArgs) -> Self {
        Self {
            stash: Stashing::bounded(*capacity),
        }
    }
}

impl Actor for AggIdiom {
    type Msg = IdiomMsg;
    type Args = IdiomArgs;
    type Error = Infallible;
    type Caps = AggIdiomCaps;

    async fn init((_, probe): IdiomArgs, cx: Ctx<'_, Self>) -> Result<Self, Infallible> {
        // [F2] arm the rehydration deadline at startup, by hand.
        let timer = cx
            .self_ref()
            .send_after(LOAD_DEADLINE, IdiomMsg::LoadDeadline);
        Ok(Self {
            probe,
            applied: 0,
            phase: Phase::Loading,
            timer: Some(timer),
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the point of the idiom variant: every [F#] transition \
                  obligation spelled out inline is exactly the surface the \
                  oracle compares against the one-declaration machine"
    )]
    async fn handle(&mut self, msg: IdiomMsg, mut cx: Ctx<'_, Self>) -> Result<Flow, Infallible> {
        match (self.phase, msg) {
            (Phase::Loading, IdiomMsg::Replay { ev, last }) => {
                self.applied = self.applied.wrapping_add(ev);
                let _ = self.probe.send(Probe::Applied(ev));
                if last {
                    // [F4] cancel the deadline on the Loading→Ready edge.
                    if let Some(t) = self.timer.take() {
                        t.cancel();
                    }
                    self.phase = Phase::Ready;
                    // [F3] release the stash on this edge.
                    cx.cap::<Stashing<IdiomMsg>>().unstash_all();
                }
            }
            (Phase::Loading, msg @ IdiomMsg::Cmd { .. }) => {
                // Manual deferral, with the overflow branch written here.
                if let Err(full) = cx.cap::<Stashing<IdiomMsg>>().stash(msg) {
                    let StashFull { .. } = &full;
                    if let IdiomMsg::Cmd { id } = full.msg() {
                        let _ = self.probe.send(Probe::ShedFull(id));
                    }
                }
            }
            (Phase::Ready, IdiomMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Processed(id));
            }
            (Phase::Loading | Phase::Ready, IdiomMsg::Drain) => {
                let _ = self.probe.send(Probe::DrainStarted);
                if self.phase == Phase::Loading {
                    // [F4-dup] cancel AGAIN on the second exit edge —
                    // per-EDGE, not per-state.
                    if let Some(t) = self.timer.take() {
                        t.cancel();
                    }
                    // [F6] release on this edge too.
                    cx.cap::<Stashing<IdiomMsg>>().unstash_all();
                }
                self.phase = Phase::Draining;
                let _ = cx.self_ref().tell(IdiomMsg::FlushDone).await;
            }
            (Phase::Draining, IdiomMsg::Cmd { id }) => {
                let _ = self.probe.send(Probe::Refused(id));
            }
            (Phase::Draining, IdiomMsg::FlushDone) => {
                let _ = self.probe.send(Probe::Snapshotted);
                return Ok(Flow::Stop(Normal));
            }
            (Phase::Loading, IdiomMsg::LoadDeadline) => {
                let _ = self.probe.send(Probe::LoadTimedOut);
                return Ok(Flow::Stop(Normal));
            }
            // [F5] stale-deadline guard arms in every other phase: a
            // fired-and-queued deadline must be swallowed silently.
            (Phase::Ready | Phase::Draining, IdiomMsg::LoadDeadline) => {}
            // Late/duplicate signals outside their phase, ignored by hand.
            (Phase::Ready | Phase::Draining, IdiomMsg::Replay { .. })
            | (Phase::Loading | Phase::Ready, IdiomMsg::FlushDone)
            | (Phase::Draining, IdiomMsg::Drain) => {}
        }
        Ok(Flow::Continue)
    }
}

// -------------------------------------------------------------- oracle ----

/// The guard must outlast the LONGEST in-test timer: under a paused
/// clock, auto-advance fires the earliest pending timer first — a 15 s
/// guard would fire before the 30 s load deadline it is guarding.
fn guard() -> Duration {
    terminate_bound().max(LOAD_DEADLINE * 3)
}

async fn run_idiom(capacity: usize, ops: &[Op]) -> Vec<Probe> {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<AggIdiom>((cap(capacity), tx));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => IdiomMsg::Replay { ev, last },
            Op::Cmd(id) => IdiomMsg::Cmd { id },
            Op::Drain => IdiomMsg::Drain,
            Op::StaleDeadline => IdiomMsg::LoadDeadline,
        };
        h.tell(msg).await.expect("tell during script");
    }
    // Keep the handle alive through collection: S3's actor must reach its
    // 30 s deadline, not get ref-count-collected the moment tells finish.
    let got = collect(rx).await;
    drop(h);
    got
}

async fn run_fsm(capacity: usize, ops: &[Op]) -> Vec<Probe> {
    let (tx, rx) = flume::unbounded();
    let h = spawn::<AggFsm>((cap(capacity), tx));
    for op in ops {
        let msg = match *op {
            Op::Replay(ev, last) => FsmMsg::Replay { ev, last },
            Op::Cmd(id) => FsmMsg::Cmd { id },
            Op::Drain => FsmMsg::Drain,
            // Mode adaptation: no timeout message exists to forge — let
            // the clock run far past the (left) Loading deadline instead.
            Op::StaleDeadline => {
                sleep(LOAD_DEADLINE * 2).await;
                continue;
            }
        };
        h.tell(msg).await.expect("tell during script");
    }
    let got = collect(rx).await;
    drop(h);
    got
}

/// Drains the probe channel until the actor stops (drops its sender).
async fn collect(rx: flume::Receiver<Probe>) -> Vec<Probe> {
    timeout(guard(), async {
        let mut got = Vec::new();
        while let Ok(p) = rx.recv_async().await {
            got.push(p);
        }
        got
    })
    .await
    .expect("actor must stop and close the probe channel")
}

async fn assert_equivalent(capacity: usize, ops: &[Op], expected: &[Probe]) {
    let idiom = run_idiom(capacity, ops).await;
    let fsm = run_fsm(capacity, ops).await;
    assert_eq!(idiom, fsm, "variants diverged");
    assert_eq!(idiom, expected, "both variants match, but not the spec");
    assert!(
        !idiom.contains(&Probe::StaleTimeoutLeaked),
        "stale timeout observed by user code"
    );
}

/// S1: happy path — rehydrate with interleaved commands; deferred
/// commands replay ahead of the mailbox backlog on entering Ready; drain
/// refuses nothing (queue empty) and stops after the flush.
#[tokio::test(start_paused = true)]
async fn s1_happy_path() {
    let ops = [
        Op::Replay(1, false),
        Op::Cmd(10),
        Op::Cmd(11),
        Op::Replay(2, false),
        Op::Replay(3, true),
        Op::Cmd(12),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::Applied(2),
        Probe::Applied(3),
        Probe::Processed(10),
        Probe::Processed(11),
        Probe::Processed(12),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(8, &ops, &expected).await;
}

/// S2: bounded deferral — the stash refuses loudly at capacity; the shed
/// command is reported, the retained ones replay on Ready.
#[tokio::test(start_paused = true)]
async fn s2_stash_overflow_sheds_loudly() {
    let ops = [
        Op::Replay(1, false),
        Op::Cmd(10),
        Op::Cmd(11),
        Op::Cmd(12),
        Op::Replay(2, true),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::ShedFull(12),
        Probe::Applied(2),
        Probe::Processed(10),
        Probe::Processed(11),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(2, &ops, &expected).await;
}

/// S3: the rehydration deadline fires (paused clock auto-advances) — the
/// actor reports the timeout and stops; the deferred command dies with
/// the incarnation, refused by silence, exactly equal in both.
#[tokio::test(start_paused = true)]
async fn s3_load_deadline_fires() {
    let ops = [Op::Replay(1, false), Op::Cmd(10)];
    let expected = [Probe::Applied(1), Probe::LoadTimedOut];
    assert_equivalent(8, &ops, &expected).await;
}

/// S4: the stale-timeout race — a deadline that fired-and-queued just
/// before its cancellation must be invisible: the idiom needs guard arms
/// ([F5]); the machine makes it unrepresentable (the slot changed with
/// the phase — nothing exists to guard).
#[tokio::test(start_paused = true)]
async fn s4_stale_deadline_is_invisible() {
    let ops = [
        Op::Replay(1, true),
        Op::StaleDeadline,
        Op::Cmd(20),
        Op::Drain,
    ];
    let expected = [
        Probe::Applied(1),
        Probe::Processed(20),
        Probe::DrainStarted,
        Probe::Snapshotted,
    ];
    assert_equivalent(8, &ops, &expected).await;
}

/// S5: drain during Loading — deferred commands must be RELEASED into
/// Draining and explicitly refused (gen_statem: postponed events retried
/// on every state change), never silently dropped ([F6], [F4-dup]).
#[tokio::test(start_paused = true)]
async fn s5_drain_during_loading_refuses_deferred() {
    let ops = [Op::Replay(1, false), Op::Cmd(10), Op::Cmd(11), Op::Drain];
    let expected = [
        Probe::Applied(1),
        Probe::DrainStarted,
        Probe::Refused(10),
        Probe::Refused(11),
        Probe::Snapshotted,
    ];
    assert_equivalent(4, &ops, &expected).await;
}

/// S6 (guard for S3): the deadline must NOT fire when rehydration
/// completes in time — cancellation on transition works in both variants.
/// Open-ended: the actors never stop; the clock runs far past the
/// deadline, then whatever was observed is compared.
#[tokio::test(start_paused = true)]
async fn s6_deadline_cancelled_on_ready() {
    let ops = [Op::Replay(1, true), Op::Cmd(20)];
    let expected = [Probe::Applied(1), Probe::Processed(20)];

    let (tx, rx) = flume::unbounded();
    let h: Handle<AggIdiom> = spawn::<AggIdiom>((cap(8), tx));
    for op in &ops {
        match *op {
            Op::Replay(ev, last) => h
                .tell(IdiomMsg::Replay { ev, last })
                .await
                .expect("tell during script"),
            Op::Cmd(id) => h
                .tell(IdiomMsg::Cmd { id })
                .await
                .expect("tell during script"),
            _ => unreachable!("S6 script has no drain/stale ops"),
        }
    }
    sleep(LOAD_DEADLINE * 3).await;
    let mut idiom = Vec::new();
    while let Ok(p) = rx.try_recv() {
        idiom.push(p);
    }
    drop(h);

    let (tx, rx) = flume::unbounded();
    let h: Handle<AggFsm> = spawn::<AggFsm>((cap(8), tx));
    for op in &ops {
        match *op {
            Op::Replay(ev, last) => h
                .tell(FsmMsg::Replay { ev, last })
                .await
                .expect("tell during script"),
            Op::Cmd(id) => h
                .tell(FsmMsg::Cmd { id })
                .await
                .expect("tell during script"),
            _ => unreachable!("S6 script has no drain/stale ops"),
        }
    }
    sleep(LOAD_DEADLINE * 3).await;
    let mut fsm = Vec::new();
    while let Ok(p) = rx.try_recv() {
        fsm.push(p);
    }
    drop(h);

    assert_eq!(idiom, fsm, "variants diverged");
    assert_eq!(idiom, expected, "a timely transition cancels the deadline");
}
