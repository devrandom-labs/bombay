//! Card #280 — the stage-3 cross-cap composition proof: ONE actor plugging
//! `Stashing` + `Watching` + `Supervising` — the deferring supervisor the
//! trait tiers made UNREPRESENTABLE (ADR-0026's decisive fact: there was no
//! `impl Watch for Stashed<S>`). The compile-time loop selection routes it
//! onto the three-arm supervised loop, where all three capabilities are
//! exercised behaviorally in one run: bounded deferral with in-step replay,
//! a watch edge on a non-child peer, and a supervised child rebuilt after a
//! kill.

use core::{convert::Infallible, num::NonZeroUsize, time::Duration};
use std::sync::{Arc, Mutex};

use tokio::time::timeout;

use bombay::{
    ActorId,
    actor::{Actor, ActorRef, Flow, PreparedActor, SpawnConfig},
    caps,
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    reply::ReplySender,
    restart::{RestartConfig, RestartPolicy},
    test_support::{set_supervisor_rng_seed, terminate_bound},
};

fn cap(n: usize) -> Capacity {
    Capacity::new(NonZeroUsize::new(n).expect("nonzero")).expect("valid")
}

/// Bounds a terminal await (#148 discipline).
async fn bounded<F: core::future::IntoFuture>(fut: F) -> F::Output {
    timeout(terminate_bound(), fut)
        .await
        .expect("await must resolve within the bound")
}

// ------------------------------------------------------------ leaf child ----

/// The supervised child: reports each incarnation's birth on an unbounded
/// tape. A plain caps actor (`Caps = ()`).
struct Leaf;

#[derive(Debug)]
struct LeafMsg;
impl Msg for LeafMsg {}

impl caps::Actor for Leaf {
    type Msg = LeafMsg;
    type Args = flume::Sender<()>;
    type Error = Infallible;
    type Caps = ();
    async fn init(births: Self::Args, _: caps::Ctx<'_, Self>) -> Result<Self, Infallible> {
        births.send(()).expect("the birth tape is unbounded");
        Ok(Self)
    }
    async fn handle(&mut self, _: LeafMsg, _: caps::Ctx<'_, Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}

// ------------------------------------------------------- watched target ----

/// A plain runtime actor the deferring supervisor WATCHES (not a child):
/// being watched is universal, so the expert floor's plain actor works.
struct Peer;
#[derive(Debug)]
struct PeerMsg;
impl Msg for PeerMsg {}
impl Mailboxed for Peer {
    type Msg = PeerMsg;
}
impl Actor for Peer {
    type Args = ();
    type Error = Infallible;
    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Infallible> {
        Ok(Self)
    }
    async fn handle(&mut self, _: PeerMsg, _: ActorRef<Self>) -> Result<Flow, Infallible> {
        Ok(Flow::Continue)
    }
}

// -------------------------------------------------- the deferring supervisor -

/// Records a non-child death through `&mut actor` — a supervised child's
/// death routes to the restart machinery instead and never lands here.
struct RecordPeerDeath;
impl caps::WatchPolicy<DeferSup> for RecordPeerDeath {
    async fn on_link_died(
        actor: &mut DeferSup,
        id: ActorId,
        reason: ActorStopReason,
        _linked: bool,
    ) -> Result<core::ops::ControlFlow<ActorStopReason>, Infallible> {
        actor.peer_deaths.push((id, reason.is_normal()));
        Ok(core::ops::ControlFlow::Continue(()))
    }
}

#[derive(Debug, bombay_macros::Msg)]
enum DeferMsg {
    /// Deferred while the gate is closed; served (taped) once open.
    Item(u32),
    /// Opens the gate and releases the deferred batch (in-step replay).
    Open,
    /// Supervise a fresh [`Leaf`] child; replies with its id.
    Supervise { reply: ReplySender<ActorId> },
    /// Watch `peer` (a non-child) — the Watching cap's verb from a handler.
    WatchPeer { reply: ReplySender<Result<(), ()>> },
    /// Reads (taped items, recorded peer deaths).
    #[msg(budget = 96)]
    Read {
        reply: ReplySender<(Vec<u32>, Vec<(ActorId, bool)>)>,
    },
}

/// The composition the tiers could not express, as three plain fields.
#[derive(bombay_macros::Provide)]
struct DeferSupCaps {
    stash: caps::Stashing<DeferMsg>,
    watching: caps::Watching<RecordPeerDeath>,
    supervising: caps::Supervising<caps::OneForOne>,
}

impl caps::CapSet<DeferSup> for DeferSupCaps {
    fn build(_: &<DeferSup as caps::Actor>::Args) -> Self {
        Self {
            stash: caps::Stashing::bounded(cap(8)),
            watching: caps::Watching::new(),
            supervising: caps::Supervising::new(),
        }
    }
}

/// The per-incarnation liveness anchors the factory fills (the external
/// strong refs production code would hold).
type Anchors = Arc<Mutex<Vec<caps::Handle<Leaf>>>>;

struct DeferSup {
    open: bool,
    tape: Vec<u32>,
    peer_deaths: Vec<(ActorId, bool)>,
    births: flume::Sender<()>,
    anchors: Anchors,
    peer: Option<ActorRef<Peer>>,
}

impl caps::Actor for DeferSup {
    type Msg = DeferMsg;
    type Args = (flume::Sender<()>, Anchors, ActorRef<Peer>);
    type Error = Infallible;
    type Caps = DeferSupCaps;

    async fn init(
        (births, anchors, peer): Self::Args,
        _: caps::Ctx<'_, Self>,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            open: false,
            tape: Vec::new(),
            peer_deaths: Vec::new(),
            births,
            anchors,
            peer: Some(peer),
        })
    }

    async fn handle(
        &mut self,
        msg: DeferMsg,
        mut cx: caps::Ctx<'_, Self>,
    ) -> Result<Flow, Infallible> {
        match msg {
            item @ DeferMsg::Item(_) if !self.open => {
                cx.cap::<caps::Stashing<DeferMsg>>()
                    .stash(item)
                    .expect("stash sized for the scenario");
            }
            DeferMsg::Item(n) => self.tape.push(n),
            DeferMsg::Open => {
                self.open = true;
                cx.cap::<caps::Stashing<DeferMsg>>().unstash_all();
            }
            DeferMsg::Supervise { reply } => {
                let births = self.births.clone();
                let anchors = Arc::clone(&self.anchors);
                let config = RestartConfig::new(RestartPolicy::Permanent)
                    .with_min_backoff(Duration::from_millis(1))
                    .with_max_backoff(Duration::from_millis(1));
                let id = bounded(cx.self_ref().supervise(config, move || {
                    // Anchor each incarnation externally: an unanchored
                    // child ref-count-stops as `Collected` and is left dead
                    // by every policy (ADR-0020) — the rebuild probe needs a
                    // live handle to kill.
                    let child = caps::spawn::<Leaf>(births.clone());
                    anchors.lock().expect("anchors").push(child.clone());
                    child
                }))
                .await
                .expect("supervisor alive");
                let _ = reply.send(id);
            }
            DeferMsg::WatchPeer { reply } => {
                let peer = self.peer.take().expect("one WatchPeer per run");
                let outcome = bounded(cx.self_ref().watch(&peer)).await.map_err(|_| ());
                let _ = reply.send(outcome);
                // do not pin the peer — its death must stay observable
                drop(peer);
            }
            DeferMsg::Read { reply } => {
                let _ = reply.send((self.tape.clone(), self.peer_deaths.clone()));
            }
        }
        Ok(Flow::Continue)
    }
}

/// The full composition, one run: (1) `Stashing` defers `Item(1)`/`Item(2)`
/// and replays them in-step ahead of the backlog on `Open` — on the
/// SUPERVISED loop; (2) `Supervising` rebuilds the killed `Leaf` (second
/// birth); (3) `Watching`'s verb installs an edge on a non-child peer whose
/// death reaches the named policy. Fails if any capability stops working
/// when composed with the other two.
#[tokio::test(start_paused = true)]
async fn deferring_supervisor_composes_all_three_caps() {
    set_supervisor_rng_seed(Some(7));
    let (births_tx, births_rx) = flume::unbounded::<()>();
    let anchors: Anchors = Arc::new(Mutex::new(Vec::new()));

    // The watched (non-child) peer, on the expert floor.
    let peer_prepared = PreparedActor::<Peer>::new(SpawnConfig::default());
    let peer_ref = peer_prepared.actor_ref().clone();
    let peer_id = peer_ref.id();
    let _peer_join = peer_prepared.spawn(());

    let sup = caps::spawn::<DeferSup>((births_tx, Arc::clone(&anchors), peer_ref.clone()));

    // (1) Deferral before the gate opens: 1 and 2 stash; 3 arrives after
    // Open and must serve BEHIND the replayed batch.
    bounded(sup.tell(DeferMsg::Item(1))).await.expect("deliver");
    bounded(sup.tell(DeferMsg::Item(2))).await.expect("deliver");

    // (2) Supervise a child mid-deferral — the previously-unrepresentable
    // combination in action.
    let child_id = bounded(sup.ask(|reply| DeferMsg::Supervise { reply }))
        .await
        .expect("supervise replied");
    bounded(births_rx.recv_async()).await.expect("first birth");

    // (3) Watch the non-child peer.
    let watched = bounded(sup.ask(|reply| DeferMsg::WatchPeer { reply }))
        .await
        .expect("watch replied");
    assert_eq!(
        watched,
        Ok(()),
        "the Watching cap provides the link channel"
    );

    // Open the gate, then a post-open item.
    bounded(sup.tell(DeferMsg::Open)).await.expect("deliver");
    bounded(sup.tell(DeferMsg::Item(3))).await.expect("deliver");
    let (tape, _) = bounded(sup.ask(|reply| DeferMsg::Read { reply }))
        .await
        .expect("read replied");
    assert_eq!(
        tape,
        vec![1, 2, 3],
        "the stash replays in-step, in arrival order, ahead of the backlog — \
         on the supervised loop",
    );

    // Kill incarnation 0: the supervised loop's restart arm must rebuild it
    // (second birth) — the restart-table path, distinct from the policy path
    // the peer death takes below.
    let _ = child_id;
    anchors
        .lock()
        .expect("anchors")
        .first()
        .expect("incarnation 0 anchored")
        .kill();
    bounded(async {
        loop {
            if births_rx.try_recv().is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;

    // The peer dies (killed): the policy must record exactly one abnormal
    // non-child death.
    peer_ref.kill();
    drop(peer_ref);
    let deaths = bounded(async {
        loop {
            let (_, deaths) = sup
                .ask(|reply| DeferMsg::Read { reply })
                .await
                .expect("alive");
            if !deaths.is_empty() {
                return deaths;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        deaths,
        vec![(peer_id, false)],
        "the non-child death reaches the NAMED policy with the true reason",
    );
}
