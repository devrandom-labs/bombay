//! Shared `MessageBus` World + step definitions for the `actors/message_bus`
//! scenarios (card #78).
//!
//! Wired by the runner that `#[path]`-includes this module:
//!   * `message_bus_bdd.rs` — the example feature (message_bus.feature)
//!
//! The SUT is `bombay_actors::message_bus` (the type-routed `MessageBus` actor:
//! `Register<M>` / `Unregister<M>` / `Publish<M>` under a `DeliveryStrategy`),
//! driven against REAL SPAWNED ACTORS reached through `bombay::prelude::*`.
//! Recipients are `Recorder` actors that count the `Ping`/`Pong` values they
//! handle; the bus's private `subscriptions` map is inspected through the
//! test-only `CountRegistrations<M>` query (gated behind the `testing` feature).
//!
//! Full-mailbox scenarios park a `Recorder` on a `watch` release gate (the same
//! idiom as the core `request_tell` wiring) so the bounded(1) mailbox stays
//! observably full while the bus tries to deliver; the test asserts the
//! strategy-specific observable, then releases the gate or kills the actor.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bombay::{error::Infallible, mailbox, prelude::*};
use bombay_actors::{
    DeliveryStrategy,
    message_bus::{CountRegistrations, MessageBus, Publish, Register, Unregister},
};
use cucumber::{World, given, then, when};
use tokio::sync::{Barrier, watch};

// ===========================================================================
// Test messages + recipient actor
// ===========================================================================

/// The two routed value types. `Clone` is required by `Publish<M>`; `Eq`/`Debug`
/// let `SendError<Probe>` equality be asserted against the exact returned value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Ping;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pong;

/// A capacity probe sent directly to a recipient (never through the bus) to
/// observe a full bounded mailbox; its handler is a no-op so a stray delivery
/// would be visible as neither a `ping` nor a `pong`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Probe;

/// Parks the recipient's run-loop on a `watch` gate until it flips to `true`,
/// holding a bounded mailbox slot so the mailbox stays full.
struct Hold(watch::Receiver<bool>);

/// Per-type delivery counters shared between a `Recorder` and the World.
#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    ping: u32,
    pong: u32,
}

/// A recipient that records every `Ping`/`Pong` it handles into shared `Counts`.
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

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self, Self::Reply>) {
        self.counts.lock().unwrap().ping += 1;
    }
}

impl Message<Pong> for Recorder {
    type Reply = ();

    async fn handle(&mut self, _msg: Pong, _ctx: &mut Context<Self, Self::Reply>) {
        self.counts.lock().unwrap().pong += 1;
    }
}

impl Message<Probe> for Recorder {
    type Reply = ();

    async fn handle(&mut self, _msg: Probe, _ctx: &mut Context<Self, Self::Reply>) {}
}

impl Message<Hold> for Recorder {
    type Reply = ();

    async fn handle(&mut self, msg: Hold, _ctx: &mut Context<Self, Self::Reply>) {
        let mut rx = msg.0;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

// ===========================================================================
// World
// ===========================================================================

/// A handle to one spawned `Recorder` recipient, keyed by its feature label.
#[derive(Debug)]
struct Rec {
    reff: ActorRef<Recorder>,
    counts: Arc<Mutex<Counts>>,
    /// Release gate for a parked (full-mailbox) recipient, if any.
    release: Option<watch::Sender<bool>>,
}

#[derive(Debug, Default, World)]
pub struct MessageBusWorld {
    bus: Option<ActorRef<MessageBus>>,
    recipients: HashMap<String, Rec>,
    /// Expected live registration counts per type (maintained as the scenario
    /// registers / unregisters / prunes), so "remains registered" can assert the
    /// exact bucket length rather than a weak `>= 1`.
    ping_regs: usize,
    pong_regs: usize,
    /// Wall-clock the last publish took (for the @timing bounds).
    publish_elapsed: Option<Duration>,
}

// ===========================================================================
// Helpers
// ===========================================================================

fn the_bus(world: &MessageBusWorld) -> ActorRef<MessageBus> {
    world
        .bus
        .clone()
        .expect("a MessageBus was spawned by a Given")
}

fn parse_strategy(name: &str, ms: Option<u64>) -> DeliveryStrategy {
    match name {
        "Guaranteed" => DeliveryStrategy::Guaranteed,
        "BestEffort" => DeliveryStrategy::BestEffort,
        "TimedDelivery" => DeliveryStrategy::TimedDelivery(Duration::from_millis(ms.unwrap())),
        "Spawned" => DeliveryStrategy::Spawned,
        "SpawnedWithTimeout" => {
            DeliveryStrategy::SpawnedWithTimeout(Duration::from_millis(ms.unwrap()))
        }
        other => panic!("unknown delivery strategy {other:?}"),
    }
}

async fn spawn_recorder(cap: Option<usize>) -> Rec {
    let counts = Arc::new(Mutex::new(Counts::default()));
    let mbox = match cap {
        Some(n) => mailbox::bounded(n),
        None => mailbox::unbounded(),
    };
    let reff = Recorder::spawn_with_mailbox(
        Recorder {
            counts: Arc::clone(&counts),
        },
        mbox,
    );
    reff.wait_for_startup().await;
    Rec {
        reff,
        counts,
        release: None,
    }
}

/// Makes `rec`'s bounded(1) mailbox observably full by parking two `Hold`
/// handlers on a release gate, then proving a `try_send(Probe)` is refused.
async fn make_full(rec: &mut Rec) {
    let (tx, rx) = watch::channel(false);
    rec.reff
        .tell(Hold(rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued into the handler");
    tokio::time::sleep(Duration::from_millis(20)).await;
    rec.reff
        .tell(Hold(rx))
        .try_send()
        .expect("second hold fills the single buffer slot");
    for _ in 0..200 {
        if matches!(
            rec.reff.tell(Probe).try_send(),
            Err(SendError::MailboxFull(Probe))
        ) {
            rec.release = Some(tx);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the recipient's bounded mailbox never became observably full");
}

async fn register(world: &mut MessageBusWorld, name: &str, ty: &str) {
    let bus = the_bus(world);
    let rec = world.recipients.get(name).expect("recipient spawned");
    match ty {
        "Ping" => {
            bus.tell(Register(rec.reff.clone().recipient::<Ping>()))
                .await
                .expect("register Ping");
            world.ping_regs += 1;
        }
        "Pong" => {
            bus.tell(Register(rec.reff.clone().recipient::<Pong>()))
                .await
                .expect("register Pong");
            world.pong_regs += 1;
        }
        other => panic!("unknown message type {other:?}"),
    }
}

async fn publish_once(world: &mut MessageBusWorld, ty: &str) {
    let bus = the_bus(world);
    let started = Instant::now();
    match ty {
        "Ping" => bus.tell(Publish(Ping)).await.expect("publish Ping"),
        "Pong" => bus.tell(Publish(Pong)).await.expect("publish Pong"),
        other => panic!("unknown message type {other:?}"),
    }
    world.publish_elapsed = Some(started.elapsed());
}

async fn count_regs(bus: &ActorRef<MessageBus>, ty: &str) -> usize {
    match ty {
        "Ping" => bus
            .ask(CountRegistrations::<Ping>::new())
            .await
            .expect("the MessageBus must still be running to answer the query"),
        "Pong" => bus
            .ask(CountRegistrations::<Pong>::new())
            .await
            .expect("the MessageBus must still be running to answer the query"),
        other => panic!("unknown message type {other:?}"),
    }
}

fn counts_of(world: &MessageBusWorld, name: &str) -> Counts {
    *world
        .recipients
        .get(name)
        .expect("recipient spawned")
        .counts
        .lock()
        .unwrap()
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

fn ping_of(world: &MessageBusWorld, name: &str) -> u32 {
    counts_of(world, name).ping
}

fn pong_of(world: &MessageBusWorld, name: &str) -> u32 {
    counts_of(world, name).pong
}

// ===========================================================================
// Given — bus, recipients, registrations
// ===========================================================================

#[given(regex = r#"^a running MessageBus with delivery strategy "([A-Za-z]+)"$"#)]
async fn given_bus(world: &mut MessageBusWorld, strategy: String) {
    let bus = MessageBus::spawn(MessageBus::new(parse_strategy(&strategy, None)));
    bus.wait_for_startup().await;
    world.bus = Some(bus);
}

#[given(
    regex = r#"^a running MessageBus with delivery strategy "([A-Za-z]+)" of (\d+) milliseconds$"#
)]
async fn given_bus_timed(world: &mut MessageBusWorld, strategy: String, ms: u64) {
    let bus = MessageBus::spawn(MessageBus::new(parse_strategy(&strategy, Some(ms))));
    bus.wait_for_startup().await;
    world.bus = Some(bus);
}

#[given(regex = r#"^recipients A and B are registered for message type "(Ping|Pong)"$"#)]
async fn given_a_and_b(world: &mut MessageBusWorld, ty: String) {
    for name in ["A", "B"] {
        world
            .recipients
            .insert(name.to_owned(), spawn_recorder(None).await);
        register(world, name, &ty).await;
    }
}

#[given(regex = r#"^recipient (\w+) is registered for message type "(Ping|Pong)"$"#)]
async fn given_registered(world: &mut MessageBusWorld, name: String, ty: String) {
    world
        .recipients
        .entry(name.clone())
        .or_insert(spawn_recorder(None).await);
    register(world, &name, &ty).await;
}

#[given(regex = r#"^recipient (\w+) is registered for message type "(Ping|Pong)" again$"#)]
async fn given_registered_again(world: &mut MessageBusWorld, name: String, ty: String) {
    register(world, &name, &ty).await;
}

#[given(regex = r#"^the same actor (\w+) is also registered for message type "(Ping|Pong)"$"#)]
async fn given_also_registered(world: &mut MessageBusWorld, name: String, ty: String) {
    register(world, &name, &ty).await;
}

#[given(
    regex = r#"^recipient (\w+) is the only recipient registered for message type "(Ping|Pong)"$"#
)]
async fn given_only_recipient(world: &mut MessageBusWorld, name: String, ty: String) {
    world
        .recipients
        .insert(name.clone(), spawn_recorder(None).await);
    register(world, &name, &ty).await;
}

#[given(regex = r#"^no recipients are registered for message type "(Ping|Pong)"$"#)]
async fn given_no_recipients(_world: &mut MessageBusWorld, _ty: String) {
    // Intentionally empty: the bucket is absent, which the publish must treat as
    // a graceful no-op.
}

#[given(regex = r#"^recipient (\w+) exists but is not yet registered$"#)]
async fn given_exists_unregistered(world: &mut MessageBusWorld, name: String) {
    world.recipients.insert(name, spawn_recorder(None).await);
}

#[given(
    regex = r#"^recipient (\w+) whose mailbox is full(?: but later drains)? is registered for message type "(Ping|Pong)"$"#
)]
async fn given_full_registered(world: &mut MessageBusWorld, name: String, ty: String) {
    let mut rec = spawn_recorder(Some(1)).await;
    make_full(&mut rec).await;
    world.recipients.insert(name.clone(), rec);
    register(world, &name, &ty).await;
}

#[given(
    regex = r#"^recipient (\w+) whose mailbox stays full past the timeout is registered for message type "(Ping|Pong)"$"#
)]
async fn given_full_past_timeout(world: &mut MessageBusWorld, name: String, ty: String) {
    let mut rec = spawn_recorder(Some(1)).await;
    make_full(&mut rec).await;
    world.recipients.insert(name.clone(), rec);
    register(world, &name, &ty).await;
}

#[given(
    regex = r#"^recipient (\w+) with spare capacity is registered for message type "(Ping|Pong)"$"#
)]
async fn given_spare_registered(world: &mut MessageBusWorld, name: String, ty: String) {
    world
        .recipients
        .insert(name.clone(), spawn_recorder(Some(16)).await);
    register(world, &name, &ty).await;
}

#[given(regex = r#"^recipient (\w+) has stopped so its actor is not running$"#)]
async fn given_stopped(world: &mut MessageBusWorld, name: String) {
    let rec = world.recipients.get(&name).expect("recipient spawned");
    rec.reff.kill();
    rec.reff.wait_for_shutdown().await;
}

// ===========================================================================
// When — publish, unregister, concurrent
// ===========================================================================

#[when(regex = r#"^a "(Ping|Pong)" message is published$"#)]
async fn when_publish(world: &mut MessageBusWorld, ty: String) {
    publish_once(world, &ty).await;
}

#[when(regex = r#"^(\d+) "(Ping|Pong)" messages are published$"#)]
async fn when_publish_n(world: &mut MessageBusWorld, n: u32, ty: String) {
    for _ in 0..n {
        publish_once(world, &ty).await;
    }
}

#[when(regex = r#"^recipient (\w+) is unregistered for message type "(Ping|Pong)"$"#)]
async fn when_unregister(world: &mut MessageBusWorld, name: String, ty: String) {
    let bus = the_bus(world);
    let id = world
        .recipients
        .get(&name)
        .expect("recipient spawned")
        .reff
        .id();
    match ty.as_str() {
        "Ping" => {
            bus.tell(Unregister::<Ping>::new(id))
                .await
                .expect("unregister");
            world.ping_regs = world.ping_regs.saturating_sub(1);
        }
        "Pong" => {
            bus.tell(Unregister::<Pong>::new(id))
                .await
                .expect("unregister");
            world.pong_regs = world.pong_regs.saturating_sub(1);
        }
        _ => unreachable!(),
    }
}

#[when(regex = r#"^an unknown actor id is unregistered for message type "(Ping|Pong)"$"#)]
async fn when_unregister_unknown(world: &mut MessageBusWorld, ty: String) {
    let bus = the_bus(world);
    // A freshly-spawned-then-stopped actor's id is guaranteed unknown to the bus.
    let stranger = spawn_recorder(None).await;
    let id = stranger.reff.id();
    stranger.reff.kill();
    match ty.as_str() {
        "Ping" => bus
            .tell(Unregister::<Ping>::new(id))
            .await
            .expect("unregister"),
        "Pong" => bus
            .tell(Unregister::<Pong>::new(id))
            .await
            .expect("unregister"),
        _ => unreachable!(),
    }
}

#[when(
    regex = r#"^(\w+) registers for message type "(Ping|Pong)" while a "(Ping|Pong)" message is published$"#
)]
async fn when_register_while_publish(
    world: &mut MessageBusWorld,
    name: String,
    reg_ty: String,
    pub_ty: String,
) {
    let bus = the_bus(world);
    let rec_ref = world
        .recipients
        .get(&name)
        .expect("recipient spawned")
        .reff
        .clone();
    let barrier = Arc::new(Barrier::new(2));
    let (b1, b2) = (Arc::clone(&barrier), Arc::clone(&barrier));
    let bus_reg = bus.clone();
    let reg = tokio::spawn(async move {
        b1.wait().await;
        match reg_ty.as_str() {
            "Ping" => bus_reg
                .tell(Register(rec_ref.recipient::<Ping>()))
                .await
                .unwrap(),
            "Pong" => bus_reg
                .tell(Register(rec_ref.recipient::<Pong>()))
                .await
                .unwrap(),
            _ => unreachable!(),
        }
    });
    let bus_pub = bus.clone();
    let pubg = tokio::spawn(async move {
        b2.wait().await;
        match pub_ty.as_str() {
            "Ping" => bus_pub.tell(Publish(Ping)).await.unwrap(),
            "Pong" => bus_pub.tell(Publish(Pong)).await.unwrap(),
            _ => unreachable!(),
        }
    });
    reg.await.expect("register task");
    pubg.await.expect("publish task");
}

#[when(regex = r#"^(\d+) "(Ping|Pong)" messages are published concurrently from (\d+) tasks$"#)]
async fn when_publish_concurrent(world: &mut MessageBusWorld, total: u32, ty: String, tasks: u32) {
    let bus = the_bus(world);
    let per = total / tasks;
    let barrier = Arc::new(Barrier::new(tasks as usize));
    let mut handles = Vec::new();
    for _ in 0..tasks {
        let bus = bus.clone();
        let barrier = Arc::clone(&barrier);
        let ty = ty.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..per {
                match ty.as_str() {
                    "Ping" => bus.tell(Publish(Ping)).await.unwrap(),
                    "Pong" => bus.tell(Publish(Pong)).await.unwrap(),
                    _ => unreachable!(),
                }
            }
        }));
    }
    for h in handles {
        h.await.expect("publish task join");
    }
}

#[when(
    regex = r#"^(\d+) "(Ping|Pong)" and (\d+) "(Ping|Pong)" messages are published concurrently from (\d+) tasks$"#
)]
async fn when_publish_two_types(
    world: &mut MessageBusWorld,
    n1: u32,
    _ty1: String,
    n2: u32,
    _ty2: String,
    tasks: u32,
) {
    let bus = the_bus(world);
    let half = tasks / 2;
    let per_ping = n1 / half;
    let per_pong = n2 / (tasks - half);
    let barrier = Arc::new(Barrier::new(tasks as usize));
    let mut handles = Vec::new();
    for t in 0..tasks {
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
}

// ===========================================================================
// Then — counts, registration state, liveness
// ===========================================================================

#[then(regex = r#"^recipient (\w+) receives exactly (\d+) "(Ping|Pong)" messages?$"#)]
async fn then_receives_exactly(world: &mut MessageBusWorld, name: String, n: u32, ty: String) {
    let ok = settle(|| match ty.as_str() {
        "Ping" => ping_of(world, &name) == n,
        "Pong" => pong_of(world, &name) == n,
        _ => unreachable!(),
    })
    .await;
    let got = counts_of(world, &name);
    assert!(
        ok,
        "recipient {name} should receive exactly {n} {ty}, got {got:?}"
    );
}

#[then(regex = r#"^recipient (\w+) receives (\d+) "(Ping|Pong)" messages$"#)]
async fn then_receives_n_typed(world: &mut MessageBusWorld, name: String, n: u32, ty: String) {
    // Settle a beat so a wrongful delivery would have time to land, then assert.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = match ty.as_str() {
        "Ping" => ping_of(world, &name),
        "Pong" => pong_of(world, &name),
        _ => unreachable!(),
    };
    assert_eq!(got, n, "recipient {name} should receive {n} {ty} messages");
}

#[then(regex = r#"^recipient (\w+) receives (\d+) messages$"#)]
async fn then_receives_total(world: &mut MessageBusWorld, name: String, n: u32) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    let c = counts_of(world, &name);
    assert_eq!(
        c.ping + c.pong,
        n,
        "recipient {name} total received should be {n}, got {c:?}"
    );
}

#[then(
    regex = r#"^recipient (\w+) receives exactly (\d+) "(Ping|Pong)" messages with no loss or duplication$"#
)]
async fn then_receives_exactly_no_loss(
    world: &mut MessageBusWorld,
    name: String,
    n: u32,
    ty: String,
) {
    then_receives_exactly(world, name, n, ty).await;
}

#[then(
    regex = r#"^once (\w+)'s mailbox drains, (\w+) eventually receives exactly 1 "(Ping|Pong)" message$"#
)]
async fn then_drains_then_receives(
    world: &mut MessageBusWorld,
    drain: String,
    name: String,
    ty: String,
) {
    if let Some(tx) = world
        .recipients
        .get_mut(&drain)
        .and_then(|r| r.release.take())
    {
        let _ = tx.send(true);
    }
    let ok = settle(|| match ty.as_str() {
        "Ping" => ping_of(world, &name) == 1,
        "Pong" => pong_of(world, &name) == 1,
        _ => unreachable!(),
    })
    .await;
    assert!(
        ok,
        "once {drain} drains, {name} must eventually receive exactly 1 {ty}"
    );
}

#[then(regex = r#"^recipient (\w+) does not receive the message(?: after the timeout elapses)?$"#)]
async fn then_does_not_receive(world: &mut MessageBusWorld, name: String) {
    // Wait beyond any per-recipient timeout used in the features (<= 50ms) so an
    // erroneous delivery would have landed.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let c = counts_of(world, &name);
    assert_eq!(
        c.ping + c.pong,
        0,
        "recipient {name} must not receive the skipped message, got {c:?}"
    );
}

#[then(regex = r#"^recipient (\w+) remains registered$"#)]
async fn then_remains_registered(world: &mut MessageBusWorld, _name: String) {
    let bus = the_bus(world);
    // All "remains registered" scenarios register their recipients for Ping.
    let live = count_regs(&bus, "Ping").await;
    assert_eq!(
        live, world.ping_regs,
        "no Ping registration should have been pruned (expected {}, live {live})",
        world.ping_regs
    );
}

#[then(regex = r#"^recipient (\w+) is removed from the registrations for "(Ping|Pong)"$"#)]
async fn then_removed(world: &mut MessageBusWorld, _name: String, ty: String) {
    let bus = the_bus(world);
    let ok = settle_async(&bus, &ty, 0).await;
    assert!(ok, "the dead recipient must be pruned from the {ty} bucket");
}

#[then(
    regex = r#"^recipient (\w+) is eventually removed from the registrations for "(Ping|Pong)"$"#
)]
async fn then_eventually_removed(world: &mut MessageBusWorld, _name: String, ty: String) {
    let bus = the_bus(world);
    let ok = settle_async(&bus, &ty, 0).await;
    assert!(
        ok,
        "the dead recipient must eventually be pruned from the {ty} bucket"
    );
}

#[then(regex = r#"^message type "(Ping|Pong)" no longer has any registration$"#)]
async fn then_no_registration(world: &mut MessageBusWorld, ty: String) {
    let bus = the_bus(world);
    assert_eq!(
        count_regs(&bus, &ty).await,
        0,
        "the {ty} bucket must be empty"
    );
}

#[then(regex = r#"^the publish completes without (?:error|blocking)$"#)]
async fn then_publish_completes(world: &mut MessageBusWorld) {
    assert!(
        world.publish_elapsed.is_some(),
        "the publish must have returned"
    );
    let bus = the_bus(world);
    // Liveness: the bus answers a query => it did not panic in the run-loop.
    let _ = count_regs(&bus, "Ping").await;
}

#[then(regex = r#"^the publish completes only after (\w+) has accepted the message$"#)]
async fn then_publish_after_accept(world: &mut MessageBusWorld, name: String) {
    // Guaranteed delivery `tell(..).await`s each recipient, so the publish only
    // returns once A has accepted; A then handles it.
    assert!(
        world.publish_elapsed.is_some(),
        "the publish must have returned"
    );
    let ok = settle(|| ping_of(world, &name) == 1).await;
    assert!(
        ok,
        "recipient {name} must have accepted and handled the message"
    );
}

#[then(regex = r#"^the publish returns within approximately the timeout$"#)]
async fn then_publish_within_timeout(world: &mut MessageBusWorld) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(1000),
        "TimedDelivery must bound the wait; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^the publish returns without waiting for (\w+) to accept$"#)]
async fn then_publish_no_wait(world: &mut MessageBusWorld, _name: String) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(500),
        "Spawned delivery returns immediately; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^the MessageBus actor does not panic$"#)]
async fn then_no_panic(world: &mut MessageBusWorld) {
    let bus = the_bus(world);
    // If the run-loop had panicked, this ask would fail with ActorNotRunning.
    let r = bus.ask(CountRegistrations::<Ping>::new()).await;
    assert!(
        r.is_ok(),
        "the MessageBus actor must still be running (no panic)"
    );
}

#[then(
    regex = r#"^recipient (\w+) receives the message exactly once or not at all, never a partial delivery$"#
)]
async fn then_atomic_delivery(world: &mut MessageBusWorld, name: String) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = ping_of(world, &name);
    assert!(
        got <= 1,
        "register|publish race must deliver 0 or 1, never partial; got {got}"
    );
}

#[then(regex = r#"^A receives the message exactly once or not at all, never a partial delivery$"#)]
async fn then_atomic_delivery_a(world: &mut MessageBusWorld) {
    then_atomic_delivery(world, "A".to_owned()).await;
}

#[then(regex = r#"^neither recipient receives a message of the other type$"#)]
async fn then_no_cross_talk(world: &mut MessageBusWorld) {
    assert_eq!(pong_of(world, "A"), 0, "A (Ping) must receive no Pong");
    assert_eq!(ping_of(world, "B"), 0, "B (Pong) must receive no Ping");
}

/// Polls the bus until the `ty` bucket length equals `want`.
async fn settle_async(bus: &ActorRef<MessageBus>, ty: &str, want: usize) -> bool {
    for _ in 0..400 {
        if count_regs(bus, ty).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    count_regs(bus, ty).await == want
}
