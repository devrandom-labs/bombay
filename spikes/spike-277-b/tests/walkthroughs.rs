//! The four canonical walkthroughs (card #277) against candidate (b).
//! Each module header records the OBLIGATION COUNT for the metrics table.

use std::sync::mpsc;
use std::time::Duration;

use spike_277_b::{
    Actor, Ctx, Disposition, Flow, OtpPropagation, PhasePolicy, Phased, StashPolicy, Stashing,
    Watching, spawn,
};

// ================================================== W1: plain ask actor
// Obligations: 1 trait impl (Actor) · 2 methods (init, handle) ·
// 2 type decls (Msg enum, actor struct) · decisions: Args, Error,
// Caps = () (explicit, one line).
// Baseline (a): 3 impls (Msg, Mailboxed, Actor) · 2 methods · 2 types · 4 decisions.

#[derive(Debug)]
enum CounterMsg {
    Get { reply: mpsc::Sender<u64> },
    Add(u64),
}

struct Counter {
    n: u64,
}

impl Actor for Counter {
    type Msg = CounterMsg;
    type Args = u64;
    type Error = std::convert::Infallible;
    type Caps = ();

    async fn init(args: u64, _: &mut Ctx<Self>) -> Result<Self, Self::Error> {
        Ok(Self { n: args })
    }

    async fn handle(&mut self, msg: CounterMsg, _: &mut Ctx<Self>) -> Result<Flow, Self::Error> {
        match msg {
            CounterMsg::Add(k) => self.n += k,
            CounterMsg::Get { reply } => {
                let _ = reply.send(self.n);
            }
        }
        Ok(Flow::Continue)
    }
}

#[tokio::test]
async fn w1_plain_ask() {
    let mut a = spawn::<Counter>(40).await.expect("init");
    let (tx, rx) = mpsc::channel();
    a.handle.tell(CounterMsg::Add(2)).expect("tell");
    a.handle.tell(CounterMsg::Get { reply: tx }).expect("tell");
    a.drain().await.expect("drive");
    assert_eq!(rx.recv().expect("reply"), 42);
}

// ============================================== W2: deferring actor
// Obligations: 2 impls (Actor + StashPolicy — the policy is a NAMED
// plugged type, cannot be forgotten: Caps requires it) · 3 methods
// (init, handle, capacity) · 3 type decls (Msg, actor, policy ZST) ·
// decisions: capacity magnitude, when to unstash.
// Baseline (a): 3 impls · 3 methods · 2 types · 4 decisions + the
// spawn-the-wrapper trap (`Stashed::<Intake>::spawn`) — which does NOT
// exist here: `spawn::<Intake>` is the same call as W1's.

#[derive(Debug)]
enum IntakeMsg {
    Pause,
    Resume,
    Submit(u64),
}

struct Intake {
    open: bool,
    accepted: Vec<u64>,
}

struct IntakeStash;
impl StashPolicy<Intake> for IntakeStash {
    fn capacity(_: &()) -> usize {
        4
    }
}

impl Actor for Intake {
    type Msg = IntakeMsg;
    type Args = ();
    type Error = std::convert::Infallible;
    type Caps = (Stashing<Intake>, std::marker::PhantomData<IntakeStash>);

    async fn init((): (), _: &mut Ctx<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            open: true,
            accepted: Vec::new(),
        })
    }

    async fn handle(&mut self, msg: IntakeMsg, cx: &mut Ctx<Self>) -> Result<Flow, Self::Error> {
        match msg {
            IntakeMsg::Pause => self.open = false,
            IntakeMsg::Resume => {
                self.open = true;
                cx.stash().unstash_all();
            }
            IntakeMsg::Submit(n) if !self.open => {
                let _ = cx.stash().stash(IntakeMsg::Submit(n));
            }
            IntakeMsg::Submit(n) => self.accepted.push(n),
        }
        Ok(Flow::Continue)
    }
}

#[tokio::test]
async fn w2_deferring() {
    let mut a = spawn::<Intake>(()).await.expect("init");
    a.handle.tell(IntakeMsg::Pause).expect("t");
    a.handle.tell(IntakeMsg::Submit(1)).expect("t");
    a.handle.tell(IntakeMsg::Submit(2)).expect("t");
    a.handle.tell(IntakeMsg::Resume).expect("t");
    a.handle.tell(IntakeMsg::Submit(3)).expect("t");
    a.drain_stashed().await.expect("drive");
    assert!(a.actor.open);
    assert_eq!(
        a.actor.accepted,
        vec![1, 2, 3],
        "deferred submissions replay in arrival order after Resume, ahead of later traffic"
    );
}

// ================================================== W3: phased actor
// Obligations: 2 impls (Actor + PhasePolicy — the MACHINE is one plugged
// unit: states, admission, deadlines together; no item can be silently
// defaulted) · methods: policy 4 (initial, capacity, gate, timeout) +
// actor 2 (init, handle) = 6 · 4 type decls (Msg, actor, State enum,
// policy ZST) · decisions: same as policy items (they ARE the decisions).
// Baseline (planned FsmActor): 3 impls · 7 required methods · 3 types ·
// ≥6 decisions + spawn_fsm.
// DELTA: -1 impl, -1 method, and the machine is one coherent unit
// instead of items scattered across a 10-item trait.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ph {
    Loading,
    Ready,
    Draining,
}

#[derive(Debug)]
enum AggMsg {
    Replay { last: bool },
    Cmd(u64),
    Drain,
    LoadTimedOut,
}

struct Agg {
    applied: u64,
    done: Vec<u64>,
    refused: Vec<u64>,
    timed_out: bool,
}

struct AggMachine;
impl PhasePolicy for AggMachine {
    type State = Ph;
    type Msg = AggMsg;
    type Args = ();
    fn initial(_: &()) -> Ph {
        Ph::Loading
    }
    fn stash_capacity(_: &()) -> usize {
        8
    }
    fn gate(state: &Ph, msg: &AggMsg) -> Disposition {
        match (state, msg) {
            (Ph::Loading, AggMsg::Cmd(_)) => Disposition::Defer,
            (Ph::Ready | Ph::Draining, AggMsg::Replay { .. }) => Disposition::Ignore,
            _ => Disposition::Deliver,
        }
    }
    fn state_timeout(state: &Ph) -> Option<Duration> {
        matches!(state, Ph::Loading).then(|| Duration::from_millis(30))
    }
}

impl Actor for Agg {
    type Msg = AggMsg;
    type Args = ();
    type Error = std::convert::Infallible;
    type Caps = Phased<AggMachine>;

    async fn init((): (), _: &mut Ctx<Self>) -> Result<Self, Self::Error> {
        Ok(Self {
            applied: 0,
            done: Vec::new(),
            refused: Vec::new(),
            timed_out: false,
        })
    }

    async fn handle(&mut self, msg: AggMsg, cx: &mut Ctx<Self>) -> Result<Flow, Self::Error> {
        match (cx.phase(), msg) {
            (Ph::Loading, AggMsg::Replay { last }) => {
                self.applied += 1;
                if last {
                    cx.goto(Ph::Ready);
                }
            }
            (Ph::Ready, AggMsg::Cmd(id)) => self.done.push(id),
            (Ph::Draining, AggMsg::Cmd(id)) => self.refused.push(id),
            (_, AggMsg::Drain) => cx.goto(Ph::Draining),
            (Ph::Loading, AggMsg::LoadTimedOut) => {
                self.timed_out = true;
                return Ok(Flow::Stop);
            }
            _ => {}
        }
        Ok(Flow::Continue)
    }
}

#[tokio::test]
async fn w3_phased_happy_path() {
    let mut a = spawn::<Agg>(()).await.expect("init");
    a.handle.tell(AggMsg::Replay { last: false }).expect("t");
    a.handle.tell(AggMsg::Cmd(10)).expect("t");
    a.handle.tell(AggMsg::Replay { last: true }).expect("t");
    a.handle.tell(AggMsg::Cmd(11)).expect("t");
    a.drain_phased().await.expect("drive");
    assert_eq!(a.actor.applied, 2);
    assert_eq!(a.actor.done, vec![10, 11], "deferred 10 replayed, then 11");
    assert!(a.actor.refused.is_empty());
}

#[tokio::test]
async fn w3_phased_deadline() {
    let mut a = spawn::<Agg>(()).await.expect("init");
    a.handle.tell(AggMsg::Replay { last: false }).expect("t");
    a.drain_phased().await.expect("drive");
    a.advance(31, |st| matches!(st, Ph::Loading).then_some(AggMsg::LoadTimedOut))
        .await;
    a.drain_phased().await.expect("drive");
    assert!(a.actor.timed_out, "loading deadline fired");
}

// ================================================ W4: watching actor
// Obligations: 1 impl (Actor) + Caps = Watching<OtpPropagation> — the
// death POLICY is chosen BY NAME (or a custom WatchPolicy impl = +1
// small impl). NO empty `impl Watch {}`, NO `impl Supervisor {}`
// ceremony, NO separate spawn verb: same spawn::<W>() as W1.
// Baseline (a): parent side needed 4 impls incl. 2 EMPTY marker impls +
// a dedicated SpawnSupervised verb.
// (Supervision runtime not modeled — declaration ergonomics measured.)

struct Sentinel;

impl Actor for Sentinel {
    type Msg = ();
    type Args = ();
    type Error = std::convert::Infallible;
    type Caps = Watching<OtpPropagation>;

    async fn init((): (), _: &mut Ctx<Self>) -> Result<Self, Self::Error> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Ctx<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

#[tokio::test]
async fn w4_watching_declares_policy_by_name() {
    let a = spawn::<Sentinel>(()).await.expect("init");
    assert!(a.cx.caps.notices.is_empty());
}
