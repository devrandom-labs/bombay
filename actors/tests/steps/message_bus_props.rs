//! `MessageBus` property/model laws (`message_bus.properties.feature`, card #78).
//!
//! These layer universally-quantified laws over the example scenarios in
//! `message_bus.feature`. The SUT (`bombay_actors::message_bus`) is async and
//! actor-global, which `proptest!`'s synchronous runner cannot drive cleanly, so
//! each law is a **documented bounded boundary-loop** over the exact input set
//! named in the feature's `# GEN:` comment (CLAUDE.md rule 8: hit the boundaries
//! — sizes `{0, 1, 2, 8, 32}`, all five `DeliveryStrategy` variants incl.
//! `Duration::ZERO`/`50ms`, the same recipient under two types, the same
//! `(recipient, type)` pair twice). The oracle is an INDEPENDENT
//! `recipient × type → registration-count` model built from scratch — it never
//! calls the SUT to decide the expected value.
//!
//! Kept separate from `steps/message_bus.rs` so the proven example harness stays
//! untouched; this module re-declares its own minimal `Recorder` actors.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bombay::{error::Infallible, mailbox, prelude::*};
use bombay_actors::{
    DeliveryStrategy,
    message_bus::{MessageBus, Publish, Register},
};
use cucumber::{World, given, then, when};
use tokio::sync::{Barrier, watch};

// ===========================================================================
// Test actors + the three routed types
// ===========================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Ty {
    Ping,
    Pong,
    Pang,
}

impl Ty {
    const ALL: [Ty; 3] = [Ty::Ping, Ty::Pong, Ty::Pang];
}

#[derive(Clone, Copy)]
struct Ping;
#[derive(Clone, Copy)]
struct Pong;
#[derive(Clone, Copy)]
struct Pang;

struct Hold(watch::Receiver<bool>);

/// Per-type received counters, shared between a `Recorder` and the harness.
#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    ping: u32,
    pong: u32,
    pang: u32,
}

impl Counts {
    fn of(&self, ty: Ty) -> u32 {
        match ty {
            Ty::Ping => self.ping,
            Ty::Pong => self.pong,
            Ty::Pang => self.pang,
        }
    }
}

#[derive(Clone)]
struct Recorder {
    counts: Arc<Mutex<Counts>>,
}

impl Actor for Recorder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Ping> for Recorder {
    type Reply = ();
    async fn handle(&mut self, _m: Ping, _c: &mut Context<Self, Self::Reply>) {
        self.counts.lock().unwrap().ping += 1;
    }
}
impl Message<Pong> for Recorder {
    type Reply = ();
    async fn handle(&mut self, _m: Pong, _c: &mut Context<Self, Self::Reply>) {
        self.counts.lock().unwrap().pong += 1;
    }
}
impl Message<Pang> for Recorder {
    type Reply = ();
    async fn handle(&mut self, _m: Pang, _c: &mut Context<Self, Self::Reply>) {
        self.counts.lock().unwrap().pang += 1;
    }
}
impl Message<Hold> for Recorder {
    type Reply = ();
    async fn handle(&mut self, m: Hold, _c: &mut Context<Self, Self::Reply>) {
        let mut rx = m.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

async fn spawn_recorder(cap: Option<usize>) -> (ActorRef<Recorder>, Arc<Mutex<Counts>>) {
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mbox = cap.map_or_else(mailbox::unbounded, mailbox::bounded);
    let reff = Recorder::spawn_with_mailbox(
        Recorder {
            counts: Arc::clone(&counts),
        },
        mbox,
    );
    reff.wait_for_startup().await;
    (reff, counts)
}

async fn register(bus: &ActorRef<MessageBus>, reff: &ActorRef<Recorder>, ty: Ty) {
    match ty {
        Ty::Ping => {
            bus.tell(Register(reff.clone().recipient::<Ping>()))
                .await
                .expect("register");
        }
        Ty::Pong => {
            bus.tell(Register(reff.clone().recipient::<Pong>()))
                .await
                .expect("register");
        }
        Ty::Pang => {
            bus.tell(Register(reff.clone().recipient::<Pang>()))
                .await
                .expect("register");
        }
    }
}

async fn publish(bus: &ActorRef<MessageBus>, ty: Ty) {
    match ty {
        Ty::Ping => bus.tell(Publish(Ping)).await.expect("publish"),
        Ty::Pong => bus.tell(Publish(Pong)).await.expect("publish"),
        Ty::Pang => bus.tell(Publish(Pang)).await.expect("publish"),
    }
}

/// Polls (bounded) until `pred` holds, returning whether it ever did.
async fn settle<F: Fn() -> bool>(pred: F) -> bool {
    for _ in 0..400 {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    pred()
}

/// One registration shape: a list of `(recipient_index, type)` pairs. The
/// boundary set below is the GEN-named coverage, deterministic (no RNG — Rust
/// `Math::random` equivalents are banned and would break reproducibility).
fn boundary_shapes() -> Vec<Vec<(usize, Ty)>> {
    vec![
        // size 0 — empty (a publish of any type must be a graceful no-op).
        vec![],
        // size 1.
        vec![(0, Ty::Ping)],
        // size 2 — same recipient under TWO distinct types.
        vec![(0, Ty::Ping), (0, Ty::Pong)],
        // size 2 — the SAME (recipient, type) pair twice (fan-out multiplicity 2).
        vec![(0, Ty::Ping), (0, Ty::Ping)],
        // size 8 — mix across all three types, a double registration, a 2-type actor.
        vec![
            (0, Ty::Ping),
            (0, Ty::Ping),
            (1, Ty::Pong),
            (1, Ty::Ping),
            (2, Ty::Pang),
            (2, Ty::Pong),
            (3, Ty::Ping),
            (3, Ty::Pang),
        ],
        // size 32 — every recipient under every type, plus duplicates.
        (0..32).map(|i| (i % 4, Ty::ALL[(i / 4) % 3])).collect(),
    ]
}

/// Independent oracle: expected received count of `ty` for recipient `idx` ==
/// the number of `(idx, ty)` registrations in the shape (each publish of a type
/// happens exactly once per law-1/2/3 case).
fn expected_count(shape: &[(usize, Ty)], idx: usize, ty: Ty) -> u32 {
    shape
        .iter()
        .filter(|&&(r, t)| r == idx && t == ty)
        .count()
        .try_into()
        .expect("registration count fits u32")
}

fn recipients_in(shape: &[(usize, Ty)]) -> Vec<usize> {
    let mut v: Vec<usize> = shape.iter().map(|&(r, _)| r).collect();
    v.sort_unstable();
    v.dedup();
    v
}

// ===========================================================================
// World
// ===========================================================================

/// Which law the shared `When a "Ping" message is published` step drives, set by
/// that law's distinguishing Given (laws 2 and 3 share the When text).
#[derive(Debug, Clone, Copy)]
enum PingMode {
    DeadPrune,
    FullSkip,
}

#[derive(Debug, Default, World)]
pub struct MessageBusPropsWorld {
    ping_mode: Option<PingMode>,
    routing_isolation_ok: Option<bool>,
    prune_inline_ok: Option<bool>,
    prune_async_ok: Option<bool>,
    other_type_untouched_ok: Option<bool>,
    never_panics_ok: Option<bool>,
    full_not_pruned_ok: Option<bool>,
    full_not_received_ok: Option<bool>,
    model_counts_ok: Option<bool>,
    model_no_crosstalk_ok: Option<bool>,
}

// ===========================================================================
// Law 1 — @property @sequence: TypeId routing isolation + fan-out multiplicity
// ===========================================================================

#[given(regex = r#"^a running MessageBus with delivery strategy "Guaranteed"$"#)]
async fn given_guaranteed(_w: &mut MessageBusPropsWorld) {
    // Each law builds its own bus(es) inside the driving step (the boundary-loop
    // spans many fresh buses), so this Given is a phrasing marker.
}

#[given(
    regex = r#"^any set of \(recipient, message-type\) registrations across several distinct types$"#
)]
async fn given_any_registrations(_w: &mut MessageBusPropsWorld) {}

#[when(regex = r#"^a value of type M is published$"#)]
async fn when_value_published(w: &mut MessageBusPropsWorld) {
    let mut all_ok = true;
    for shape in boundary_shapes() {
        let bus = MessageBus::spawn(MessageBus::new(DeliveryStrategy::Guaranteed));
        bus.wait_for_startup().await;
        // Spawn the recipients this shape needs.
        let mut recs: HashMap<usize, (ActorRef<Recorder>, Arc<Mutex<Counts>>)> = HashMap::new();
        for idx in recipients_in(&shape) {
            recs.insert(idx, spawn_recorder(None).await);
        }
        for &(idx, ty) in &shape {
            let reff = recs[&idx].0.clone();
            register(&bus, &reff, ty).await;
        }
        // Publish each distinct type exactly once.
        for ty in Ty::ALL {
            publish(&bus, ty).await;
        }
        // Assert every recipient's per-type observed count == the oracle.
        for (&idx, (_, counts)) in &recs {
            for ty in Ty::ALL {
                let want = expected_count(&shape, idx, ty);
                let got = settle(|| counts.lock().unwrap().of(ty) == want).await;
                if !got {
                    all_ok = false;
                }
            }
        }
        bus.kill();
    }
    w.routing_isolation_ok = Some(all_ok);
}

#[then(
    regex = r#"^every recipient registered for TypeId\(M\) receives the value once per such registration$"#
)]
async fn then_multiplicity(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.routing_isolation_ok,
        Some(true),
        "per-(recipient,type) received counts must equal the registration multiset"
    );
}

#[then(regex = r#"^no recipient registered only for a type other than M receives it$"#)]
async fn then_isolation(w: &mut MessageBusPropsWorld) {
    // The oracle in the When already asserts the count for EVERY (recipient, type)
    // including the zero cases (a recipient registered only for another type has
    // expected 0 for M), so a leak would have flipped routing_isolation_ok.
    assert_eq!(w.routing_isolation_ok, Some(true), "no cross-type leak");
}

// ===========================================================================
// Law 2 — @property @lifecycle: dead recipient pruned iff ActorNotRunning, by type
// ===========================================================================

#[given(regex = r#"^a running MessageBus with any delivery strategy d$"#)]
async fn given_any_strategy(_w: &mut MessageBusPropsWorld) {}

#[given(regex = r#"^a recipient A registered for type "Ping" whose actor is not running$"#)]
async fn given_dead_a(w: &mut MessageBusPropsWorld) {
    w.ping_mode = Some(PingMode::DeadPrune);
}

/// Shared `When` for laws 2 and 3 (identical feature text); dispatches on the
/// `PingMode` set by each law's distinguishing Given.
#[when(regex = r#"^a "Ping" message is published$"#)]
async fn when_ping_published(w: &mut MessageBusPropsWorld) {
    match w
        .ping_mode
        .expect("a law-2/law-3 Given must set the PingMode")
    {
        PingMode::DeadPrune => when_dead_prune(w).await,
        PingMode::FullSkip => when_full_skip(w).await,
    }
}

async fn when_dead_prune(w: &mut MessageBusPropsWorld) {
    let inline = [
        DeliveryStrategy::Guaranteed,
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    let asyncs = [
        DeliveryStrategy::Spawned,
        DeliveryStrategy::SpawnedWithTimeout(Duration::ZERO),
        DeliveryStrategy::SpawnedWithTimeout(Duration::from_millis(50)),
    ];

    let mut inline_ok = true;
    let mut async_ok = true;
    let mut other_ok = true;
    let mut alive_ok = true;

    for (strategies, is_inline) in [(&inline[..], true), (&asyncs[..], false)] {
        for &d in strategies {
            let bus = MessageBus::spawn(MessageBus::new(d));
            bus.wait_for_startup().await;
            // A registered for Ping AND Pong; A then stopped.
            let (a, _ac) = spawn_recorder(None).await;
            register(&bus, &a, Ty::Ping).await;
            register(&bus, &a, Ty::Pong).await;
            a.kill();
            a.wait_for_shutdown().await;

            publish(&bus, Ty::Ping).await;

            let pruned = settle_regs(&bus, Ty::Ping, 0).await;
            if is_inline {
                inline_ok &= pruned;
            } else {
                async_ok &= pruned;
            }
            // A's Pong registration is never touched by a Ping publish.
            other_ok &= count_regs(&bus, Ty::Pong).await == 1;
            // The bus never panics for any d (it still answers a query).
            alive_ok &= bus.ask(reg_query(Ty::Ping)).await.is_ok();
            bus.kill();
        }
    }
    w.prune_inline_ok = Some(inline_ok);
    w.prune_async_ok = Some(async_ok);
    w.other_type_untouched_ok = Some(other_ok);
    w.never_panics_ok = Some(alive_ok);
}

#[then(
    regex = r#"^A is removed from the registrations for "Ping" iff d delivers inline \(Guaranteed, BestEffort, TimedDelivery\), pruned synchronously after the loop$"#
)]
async fn then_pruned_inline(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.prune_inline_ok,
        Some(true),
        "inline strategies prune synchronously"
    );
}

#[then(
    regex = r#"^for Spawned and SpawnedWithTimeout A is removed asynchronously via a self-sent Unregister::<Ping>::new\(A\)$"#
)]
async fn then_pruned_async(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.prune_async_ok,
        Some(true),
        "spawned strategies prune via self-sent Unregister"
    );
}

#[then(regex = r#"^A's registrations for any OTHER type are never touched by a Ping publish$"#)]
async fn then_other_untouched(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.other_type_untouched_ok,
        Some(true),
        "the Pong bucket is unaffected"
    );
}

#[then(regex = r#"^the MessageBus actor never panics for any d$"#)]
async fn then_never_panics(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.never_panics_ok,
        Some(true),
        "no strategy panics the run-loop"
    );
}

// ===========================================================================
// Law 3 — @property @boundary: only ActorNotRunning prunes; full/slow never does
// ===========================================================================

#[given(regex = r#"^a running MessageBus with delivery strategy "BestEffort" or "TimedDelivery"$"#)]
async fn given_besteffort_or_timed(_w: &mut MessageBusPropsWorld) {}

#[given(regex = r#"^a recipient A registered for "Ping" whose mailbox is full past any timeout$"#)]
async fn given_full_a(w: &mut MessageBusPropsWorld) {
    w.ping_mode = Some(PingMode::FullSkip);
}

async fn when_full_skip(w: &mut MessageBusPropsWorld) {
    let strategies = [
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(1)),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    let mut not_pruned = true;
    let mut not_received = true;
    let mut alive = true;
    for d in strategies {
        let bus = MessageBus::spawn(MessageBus::new(d));
        bus.wait_for_startup().await;
        let (a, counts) = spawn_recorder(Some(1)).await;
        // Fill A's bounded(1) mailbox so it stays full past any timeout.
        let (_tx, rx) = watch::channel(false);
        a.tell(Hold(rx.clone())).send().await.expect("first hold");
        tokio::time::sleep(Duration::from_millis(20)).await;
        a.tell(Hold(rx)).try_send().expect("second hold fills slot");
        register(&bus, &a, Ty::Ping).await;

        publish(&bus, Ty::Ping).await;
        tokio::time::sleep(Duration::from_millis(120)).await;

        not_pruned &= count_regs(&bus, Ty::Ping).await == 1;
        not_received &= counts.lock().unwrap().ping == 0;
        alive &= bus.ask(reg_query(Ty::Ping)).await.is_ok();
        bus.kill();
        a.kill();
    }
    w.full_not_pruned_ok = Some(not_pruned);
    w.full_not_received_ok = Some(not_received);
    w.never_panics_ok = Some(alive);
}

#[then(regex = r#"^A does not receive the message$"#)]
async fn then_full_not_received(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.full_not_received_ok,
        Some(true),
        "a full mailbox drops the message"
    );
}

#[then(regex = r#"^A remains registered for any timeout value$"#)]
async fn then_full_not_pruned(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.full_not_pruned_ok,
        Some(true),
        "MailboxFull/Timeout never prune"
    );
}

#[then(regex = r#"^the MessageBus actor does not panic$"#)]
async fn then_no_panic_law3(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.never_panics_ok,
        Some(true),
        "no panic on a full recipient"
    );
}

// ===========================================================================
// Law 4 — @model @linearizability: concurrent multi-type publishes refine counters
// ===========================================================================

#[given(regex = r#"^any fixed set of registrations across distinct types$"#)]
async fn given_fixed_regs(_w: &mut MessageBusPropsWorld) {}

#[when(regex = r#"^N values are published concurrently from P tasks across the registered types$"#)]
async fn when_concurrent_multitype(w: &mut MessageBusPropsWorld) {
    // Fixed registration set: R0 for Ping, R1 for Pong, R2 for both, R3 twice for Ping.
    let shape: Vec<(usize, Ty)> = vec![
        (0, Ty::Ping),
        (1, Ty::Pong),
        (2, Ty::Ping),
        (2, Ty::Pong),
        (3, Ty::Ping),
        (3, Ty::Ping),
    ];
    // Per-type publish counts incl. the boundary {0 publishes of Pang, a registered-but-
    // unused type is absent here}; P in [2, 10].
    let ping_pubs = 40u32;
    let pong_pubs = 30u32;
    let p = 10u32;

    let bus = MessageBus::spawn(MessageBus::new(DeliveryStrategy::Guaranteed));
    bus.wait_for_startup().await;
    let mut recs: HashMap<usize, (ActorRef<Recorder>, Arc<Mutex<Counts>>)> = HashMap::new();
    for idx in recipients_in(&shape) {
        recs.insert(idx, spawn_recorder(None).await);
    }
    for &(idx, ty) in &shape {
        let reff = recs[&idx].0.clone();
        register(&bus, &reff, ty).await;
    }

    // P tasks publish concurrently: half drive Ping, half drive Pong.
    let half = p / 2;
    let per_ping = ping_pubs / half;
    let per_pong = pong_pubs / (p - half);
    let barrier = Arc::new(Barrier::new(p as usize));
    let mut handles = Vec::new();
    for t in 0..p {
        let bus = bus.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            if t < half {
                for _ in 0..per_ping {
                    bus.tell(Publish(Ping)).await.unwrap();
                }
            } else {
                for _ in 0..per_pong {
                    bus.tell(Publish(Pong)).await.unwrap();
                }
            }
        }));
    }
    for h in handles {
        h.await.expect("publish task join");
    }

    // Oracle: received[idx][ty] == (#publishes of ty) * (registrations of (idx, ty)).
    let total_ping = per_ping * half;
    let total_pong = per_pong * (p - half);
    let mut counts_ok = true;
    let mut crosstalk_ok = true;
    for (&idx, (_, counts)) in &recs {
        let want_ping = expected_count(&shape, idx, Ty::Ping) * total_ping;
        let want_pong = expected_count(&shape, idx, Ty::Pong) * total_pong;
        counts_ok &= settle(|| {
            let c = *counts.lock().unwrap();
            c.ping == want_ping && c.pong == want_pong
        })
        .await;
        // No recipient receives a type it is not registered for (Pang: nobody).
        crosstalk_ok &= counts.lock().unwrap().pang == 0;
    }
    bus.kill();
    w.model_counts_ok = Some(counts_ok);
    w.model_no_crosstalk_ok = Some(crosstalk_ok);
}

#[then(
    regex = r#"^each recipient's received count of type M equals the number of M-publishes times its registration count for M — no loss, no duplication$"#
)]
async fn then_model_counts(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.model_counts_ok,
        Some(true),
        "per-(type,recipient) counts must equal publishes × registration multiplicity, no loss/dup"
    );
}

#[then(regex = r#"^no recipient ever receives a value of a type it is not registered for$"#)]
async fn then_model_no_crosstalk(w: &mut MessageBusPropsWorld) {
    assert_eq!(
        w.model_no_crosstalk_ok,
        Some(true),
        "no cross-type delivery under concurrency"
    );
}

// ===========================================================================
// Shared registration-count query (gated `testing` surface on the SUT)
// ===========================================================================

use bombay_actors::message_bus::CountRegistrations;

fn reg_query(ty: Ty) -> CountRegistrations<()> {
    let _ = ty;
    CountRegistrations::<()>::new()
}

async fn count_regs(bus: &ActorRef<MessageBus>, ty: Ty) -> usize {
    let q = "the MessageBus must answer the registration-count query";
    match ty {
        Ty::Ping => bus.ask(CountRegistrations::<Ping>::new()).await.expect(q),
        Ty::Pong => bus.ask(CountRegistrations::<Pong>::new()).await.expect(q),
        Ty::Pang => bus.ask(CountRegistrations::<Pang>::new()).await.expect(q),
    }
}

async fn settle_regs(bus: &ActorRef<MessageBus>, ty: Ty, want: usize) -> bool {
    for _ in 0..400 {
        if count_regs(bus, ty).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    count_regs(bus, ty).await == want
}
