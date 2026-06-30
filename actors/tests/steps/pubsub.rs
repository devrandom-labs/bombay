//! Shared `PubSub` World + step definitions for the `actors/pubsub` scenarios
//! (card #78).
//!
//! Wired by the runners that `#[path]`-include this module:
//!   * `pubsub_bdd.rs`        — the example feature (pubsub.feature)
//!   * `pubsub_props_bdd.rs`  — the property/model feature (pubsub.properties.feature)
//!   * `pubsub_bug_bdd.rs`    — runs ONLY the @bug-tagged scenario as a live-defect probe
//!
//! The SUT is `bombay_actors::pubsub::PubSub<Msg>` (a broadcast pub/sub actor:
//! `Subscribe<A>` / `SubscribeFilter<A, M>` / `Publish<M>` under a
//! `DeliveryStrategy`), driven against REAL SPAWNED ACTORS reached through
//! `bombay::prelude::*`. Subscribers are `Recorder` actors that record every
//! `Msg` payload they handle; the pubsub's private `subscribers` map is inspected
//! through the test-only `CountSubscribers<M>` / `ContainsSubscriber<M>` queries
//! (gated behind the `testing` feature).
//!
//! `SubscribeFilter<A, M>` carries a *function pointer* `fn(&M) -> bool`, not a
//! closure, so every per-subscriber predicate used here is a named `fn`. The one
//! filter that "consults a shared counter" reads a process-global atomic that the
//! `@linearizability` step resets per scenario; since cucumber serialises those
//! scenarios (`max_concurrent_scenarios(1)` in the runner) this is race-free.
//!
//! Full-mailbox scenarios park a `Recorder` on a `watch` release gate (the same
//! idiom as the core `request_tell` wiring) so the bounded(1) mailbox stays
//! observably full while the pubsub tries to deliver; the test asserts the
//! strategy-specific observable, then releases the gate or kills the actor.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bombay::{error::Infallible, mailbox, prelude::*};
use bombay_actors::{
    DeliveryStrategy,
    pubsub::{ContainsSubscriber, CountSubscribers, PubSub, Publish, Subscribe, SubscribeFilter},
};
use cucumber::{World, given, then, when};
use tokio::sync::{Barrier, watch};

// ===========================================================================
// Test message + recipient actor
// ===========================================================================

/// The single broadcast value type. `Clone` is required by `Publish<M>`; the
/// `String` payload carries the feature's "tag"/"topic" so predicate `fn`s can
/// select on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Msg(pub String);

/// Parks the recipient's run-loop on a `watch` gate until it flips to `true`,
/// holding a bounded mailbox slot so the mailbox stays full.
struct Hold(watch::Receiver<bool>);

/// What a `Recorder` has observed: every `Msg` payload it handled, in order.
#[derive(Debug, Default)]
struct Record {
    received: Vec<String>,
}

/// A recipient that records every `Msg` it handles into a shared `Record`.
#[derive(Clone)]
struct Recorder {
    record: Arc<Mutex<Record>>,
}

impl Actor for Recorder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Msg> for Recorder {
    type Reply = ();

    async fn handle(&mut self, msg: Msg, _ctx: &mut Context<Self, Self::Reply>) {
        self.record.lock().unwrap().received.push(msg.0);
    }
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
// Filter predicates — function pointers for `SubscribeFilter<A, Msg>`
// ===========================================================================

/// The default-filter equivalent: accepts every message.
fn accept_all(_m: &Msg) -> bool {
    true
}

/// Rejects every message (the const-false boundary).
#[allow(dead_code)]
fn accept_none(_m: &Msg) -> bool {
    false
}

fn keep_keep(m: &Msg) -> bool {
    m.0 == "keep"
}

fn keep_new(m: &Msg) -> bool {
    m.0 == "new"
}

fn keep_old(m: &Msg) -> bool {
    m.0 == "old"
}

fn topic_a(m: &Msg) -> bool {
    m.0.starts_with("TopicA:")
}

fn topic_b(m: &Msg) -> bool {
    m.0.starts_with("TopicB:")
}

/// Process-global invocation counter for the "filter consults a shared counter"
/// linearizability scenario. The `@linearizability` runner pins
/// `max_concurrent_scenarios(1)`, so this single static is not raced across
/// scenarios; the reset below makes its absolute count deterministic.
static FILTER_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Accepts iff the payload parses to an even integer, counting every invocation.
/// The accept/reject decision is a pure function of the message (parity), so the
/// independent oracle can reproduce it without calling the SUT.
fn counting_even(m: &Msg) -> bool {
    FILTER_CALLS.fetch_add(1, Ordering::SeqCst);
    m.0.parse::<u64>().map(|n| n % 2 == 0).unwrap_or(false)
}

/// Maps a filter label from the feature text to a concrete `fn` pointer.
fn filter_by_label(label: &str) -> fn(&Msg) -> bool {
    match label {
        "default" | "true" => accept_all,
        "keep" => keep_keep,
        "new" => keep_new,
        "old" => keep_old,
        "TopicA:" => topic_a,
        "TopicB:" => topic_b,
        other => panic!("unknown filter label {other:?}"),
    }
}

// ===========================================================================
// World
// ===========================================================================

/// A handle to one spawned `Recorder` subscriber, keyed by its feature label.
struct Sub {
    reff: ActorRef<Recorder>,
    record: Arc<Mutex<Record>>,
    /// Release gate for a parked (full-mailbox) subscriber, if any.
    release: Option<watch::Sender<bool>>,
}

#[derive(Default, World)]
pub struct PubSubWorld {
    pubsub: Option<ActorRef<PubSub<Msg>>>,
    subs: HashMap<String, Sub>,
    /// Wall-clock the last publish took (for the @timing bounds).
    publish_elapsed: Option<Duration>,
    /// Count of messages published in the current scenario (used by some
    /// property steps to compute the oracle expectation).
    published: Vec<String>,
}

impl std::fmt::Debug for PubSubWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PubSubWorld")
            .field("has_pubsub", &self.pubsub.is_some())
            .field("subs", &self.subs.keys().collect::<Vec<_>>())
            .field("publish_elapsed", &self.publish_elapsed)
            .field("published", &self.published)
            .finish()
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

fn the_pubsub(world: &PubSubWorld) -> ActorRef<PubSub<Msg>> {
    world
        .pubsub
        .clone()
        .expect("a PubSub was spawned by a Given")
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

async fn spawn_recorder(cap: Option<usize>) -> Sub {
    let record = Arc::new(Mutex::new(Record::default()));
    let mbox = match cap {
        Some(n) => mailbox::bounded(n),
        None => mailbox::unbounded(),
    };
    let reff = Recorder::spawn_with_mailbox(
        Recorder {
            record: Arc::clone(&record),
        },
        mbox,
    );
    reff.wait_for_startup().await;
    Sub {
        reff,
        record,
        release: None,
    }
}

/// Makes `sub`'s bounded(1) mailbox observably full by parking two `Hold`
/// handlers on a release gate, then proving a `try_send(Hold)` is refused.
async fn make_full(sub: &mut Sub) {
    let (tx, rx) = watch::channel(false);
    sub.reff
        .tell(Hold(rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued into the handler");
    tokio::time::sleep(Duration::from_millis(20)).await;
    sub.reff
        .tell(Hold(rx.clone()))
        .try_send()
        .expect("second hold fills the single buffer slot");
    for _ in 0..200 {
        if matches!(
            sub.reff.tell(Hold(rx.clone())).try_send(),
            Err(SendError::MailboxFull(_))
        ) {
            sub.release = Some(tx);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the subscriber's bounded mailbox never became observably full");
}

/// Subscribes `name` with the default (accept-all) filter via the spawned
/// `Subscribe` message.
async fn subscribe_default(world: &mut PubSubWorld, name: &str) {
    let ps = the_pubsub(world);
    let reff = world
        .subs
        .get(name)
        .expect("subscriber spawned")
        .reff
        .clone();
    ps.tell(Subscribe(reff)).await.expect("subscribe default");
}

/// Subscribes `name` with a named-`fn` filter via the spawned `SubscribeFilter`
/// message.
async fn subscribe_filter(world: &mut PubSubWorld, name: &str, filter: fn(&Msg) -> bool) {
    let ps = the_pubsub(world);
    let reff = world
        .subs
        .get(name)
        .expect("subscriber spawned")
        .reff
        .clone();
    ps.tell(SubscribeFilter(reff, filter))
        .await
        .expect("subscribe filter");
}

async fn publish_payload(world: &mut PubSubWorld, payload: &str) {
    let ps = the_pubsub(world);
    let started = Instant::now();
    ps.tell(Publish(Msg(payload.to_owned())))
        .await
        .expect("publish");
    world.publish_elapsed = Some(started.elapsed());
    world.published.push(payload.to_owned());
}

fn record_of(world: &PubSubWorld, name: &str) -> Vec<String> {
    world
        .subs
        .get(name)
        .expect("subscriber spawned")
        .record
        .lock()
        .unwrap()
        .received
        .clone()
}

fn count_of(world: &PubSubWorld, name: &str) -> usize {
    record_of(world, name).len()
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

async fn live_subscriber_count(ps: &ActorRef<PubSub<Msg>>) -> usize {
    ps.ask(CountSubscribers::<Msg>::new())
        .await
        .expect("the PubSub must still be running to answer the query")
}

async fn contains_subscriber(ps: &ActorRef<PubSub<Msg>>, id: ActorId) -> bool {
    ps.ask(ContainsSubscriber::<Msg>::new(id))
        .await
        .expect("the PubSub must still be running to answer the query")
}

/// Polls the pubsub until `id` membership equals `want`.
async fn settle_membership(ps: &ActorRef<PubSub<Msg>>, id: ActorId, want: bool) -> bool {
    for _ in 0..400 {
        if contains_subscriber(ps, id).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    contains_subscriber(ps, id).await == want
}

// ===========================================================================
// Given — pubsub, subscribers, filters
// ===========================================================================

#[given(regex = r#"^a running PubSub with delivery strategy "([A-Za-z]+)"$"#)]
async fn given_pubsub(world: &mut PubSubWorld, strategy: String) {
    let ps = PubSub::spawn(PubSub::new(parse_strategy(&strategy, None)));
    ps.wait_for_startup().await;
    world.pubsub = Some(ps);
}

#[given(regex = r#"^a running PubSub with delivery strategy "([A-Za-z]+)" of (\d+) milliseconds$"#)]
async fn given_pubsub_timed(world: &mut PubSubWorld, strategy: String, ms: u64) {
    let ps = PubSub::spawn(PubSub::new(parse_strategy(&strategy, Some(ms))));
    ps.wait_for_startup().await;
    world.pubsub = Some(ps);
}

#[given(regex = r#"^subscribers A, B and C are subscribed with the default filter$"#)]
async fn given_abc_default(world: &mut PubSubWorld) {
    for name in ["A", "B", "C"] {
        world
            .subs
            .insert(name.to_owned(), spawn_recorder(None).await);
        subscribe_default(world, name).await;
    }
}

#[given(regex = r#"^subscribers A and B are subscribed with the default filter$"#)]
async fn given_ab_default(world: &mut PubSubWorld) {
    for name in ["A", "B"] {
        world
            .subs
            .insert(name.to_owned(), spawn_recorder(None).await);
        subscribe_default(world, name).await;
    }
}

#[given(regex = r#"^a subscriber (\w+) is subscribed with the default filter$"#)]
async fn given_one_default(world: &mut PubSubWorld, name: String) {
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe_default(world, &name).await;
}

#[given(
    regex = r#"^a subscriber (\w+) is subscribed with a filter accepting only messages tagged "(\w+)"$"#
)]
async fn given_filter_tag(world: &mut PubSubWorld, name: String, tag: String) {
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe_filter(world, &name, filter_by_label(&tag)).await;
}

#[given(
    regex = r#"^a subscriber (\w+) is subscribed with a filter accepting only "([A-Za-z:]+)" messages$"#
)]
async fn given_filter_prefix(world: &mut PubSubWorld, name: String, prefix: String) {
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe_filter(world, &name, filter_by_label(&prefix)).await;
}

#[given(
    regex = r#"^a subscriber (\w+) is subscribed via SubscribeFilter accepting only "([A-Za-z:]+)" messages$"#
)]
async fn given_subscribefilter_prefix(world: &mut PubSubWorld, name: String, prefix: String) {
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe_filter(world, &name, filter_by_label(&prefix)).await;
}

#[given(regex = r#"^no subscribers are registered$"#)]
async fn given_no_subscribers(_world: &mut PubSubWorld) {
    // Intentionally empty: the subscriber map stays empty, which the publish must
    // treat as a graceful no-op.
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox is full is subscribed with the default filter$"#
)]
async fn given_full_default(world: &mut PubSubWorld, name: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe_default(world, &name).await;
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox is full but later drains is subscribed with the default filter$"#
)]
async fn given_full_drains_default(world: &mut PubSubWorld, name: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe_default(world, &name).await;
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox stays full past the timeout is subscribed with the default filter$"#
)]
async fn given_full_past_timeout(world: &mut PubSubWorld, name: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe_default(world, &name).await;
}

#[given(
    regex = r#"^a subscriber (\w+) with spare capacity is subscribed with the default filter$"#
)]
async fn given_spare_default(world: &mut PubSubWorld, name: String) {
    world
        .subs
        .insert(name.clone(), spawn_recorder(Some(16)).await);
    subscribe_default(world, &name).await;
}

#[given(regex = r#"^subscriber (\w+) has stopped so its actor is not running$"#)]
async fn given_stopped(world: &mut PubSubWorld, name: String) {
    let sub = world.subs.get(&name).expect("subscriber spawned");
    sub.reff.kill();
    sub.reff.wait_for_shutdown().await;
}

#[given(
    regex = r#"^subscriber (\w+) is in the process of stopping so delivery reports ActorStopped$"#
)]
async fn given_stopping(world: &mut PubSubWorld, name: String) {
    // PubSub prunes on BOTH ActorNotRunning and ActorStopped. A fully-killed,
    // shut-down actor's ref yields one of those terminal SendErrors on the next
    // delivery; the prune step then asserts removal regardless of which terminal
    // variant the runtime surfaced. (We cannot deterministically force the
    // transient `ActorStopped` window, so we drive the same terminal path the
    // prune rule covers.)
    let sub = world.subs.get(&name).expect("subscriber spawned");
    sub.reff.kill();
    sub.reff.wait_for_shutdown().await;
}

// ===========================================================================
// When — publish, re-subscribe, concurrent
// ===========================================================================

#[when(regex = r#"^(\d+) distinct messages are published$"#)]
async fn when_publish_distinct(world: &mut PubSubWorld, n: u32) {
    for i in 0..n {
        publish_payload(world, &format!("m{i}")).await;
    }
}

#[when(regex = r#"^(\d+) messages are published$"#)]
async fn when_publish_n(world: &mut PubSubWorld, n: u32) {
    for i in 0..n {
        publish_payload(world, &format!("m{i}")).await;
    }
}

#[when(regex = r#"^a message tagged "(\w+)" is published$"#)]
async fn when_publish_tagged(world: &mut PubSubWorld, tag: String) {
    publish_payload(world, &tag).await;
}

#[when(regex = r#"^a message "([^"]+)" is published$"#)]
async fn when_publish_named(world: &mut PubSubWorld, payload: String) {
    publish_payload(world, &payload).await;
}

#[when(regex = r#"^(\w+) is re-subscribed with a filter accepting only "(\w+)" messages$"#)]
async fn when_resubscribe(world: &mut PubSubWorld, name: String, tag: String) {
    subscribe_filter(world, &name, filter_by_label(&tag)).await;
}

#[when(regex = r#"^(\d+) messages are published concurrently from (\d+) tasks$"#)]
async fn when_publish_concurrent(world: &mut PubSubWorld, total: u32, tasks: u32) {
    let ps = the_pubsub(world);
    let per = total / tasks;
    let barrier = Arc::new(Barrier::new(tasks as usize));
    let mut handles = Vec::new();
    for t in 0..tasks {
        let ps = ps.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..per {
                // Even payloads so the counting_even filter accepts half.
                let payload = (t * per + i) * 2;
                ps.tell(Publish(Msg(payload.to_string()))).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.expect("publish task join");
    }
    world
        .published
        .extend((0..total).map(|i| (i * 2).to_string()));
}

// ===========================================================================
// Then — counts, payloads, membership, liveness, timing
// ===========================================================================

#[then(regex = r#"^subscriber (\w+) receives exactly (\d+) messages?$"#)]
async fn then_receives_exactly(world: &mut PubSubWorld, name: String, n: u32) {
    let ok = settle(|| count_of(world, &name) == n as usize).await;
    let got = record_of(world, &name);
    assert!(
        ok,
        "subscriber {name} should receive exactly {n} messages, got {got:?}"
    );
}

#[then(
    regex = r#"^subscriber (\w+) receives exactly (\d+) messages? with no loss or duplication$"#
)]
async fn then_receives_exactly_no_loss(world: &mut PubSubWorld, name: String, n: u32) {
    then_receives_exactly(world, name, n).await;
}

#[then(regex = r#"^subscriber (\w+) receives (\d+) messages$"#)]
async fn then_receives_count(world: &mut PubSubWorld, name: String, n: u32) {
    // Settle a beat so a wrongful delivery would have time to land, then assert.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = count_of(world, &name);
    assert_eq!(
        got,
        n as usize,
        "subscriber {name} should receive {n} messages, got {:?}",
        record_of(world, &name)
    );
}

#[then(regex = r#"^subscriber (\w+) receives exactly (\d+) messages?, the "([^"]+)" message$"#)]
async fn then_receives_specific(world: &mut PubSubWorld, name: String, n: u32, payload: String) {
    let ok = settle(|| count_of(world, &name) == n as usize).await;
    let got = record_of(world, &name);
    assert!(
        ok,
        "subscriber {name} should receive exactly {n} message(s), got {got:?}"
    );
    assert_eq!(
        got,
        vec![payload.clone()],
        "subscriber {name} should have received exactly the {payload:?} message"
    );
}

#[then(regex = r#"^subscriber (\w+) does not receive the message(?: after the timeout elapses)?$"#)]
async fn then_does_not_receive(world: &mut PubSubWorld, name: String) {
    // Wait beyond any per-subscriber timeout used in the features (<= 50ms) so an
    // erroneous delivery would have landed.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let got = record_of(world, &name);
    assert!(
        got.is_empty(),
        "subscriber {name} must not receive the skipped message, got {got:?}"
    );
}

#[then(regex = r#"^subscriber (\w+) remains subscribed$"#)]
async fn then_remains_subscribed(world: &mut PubSubWorld, name: String) {
    let ps = the_pubsub(world);
    let id = world.subs.get(&name).expect("subscriber spawned").reff.id();
    assert!(
        contains_subscriber(&ps, id).await,
        "subscriber {name} must remain in the subscriber set"
    );
}

#[then(regex = r#"^subscriber (\w+) remains in the subscriber set$"#)]
async fn then_remains_in_set(world: &mut PubSubWorld, name: String) {
    // Settle a beat so any (erroneous) pruning would have happened, then assert
    // the subscriber is still present.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let ps = the_pubsub(world);
    let id = world.subs.get(&name).expect("subscriber spawned").reff.id();
    assert!(
        contains_subscriber(&ps, id).await,
        "subscriber {name} must remain in the subscriber set (no prune)"
    );
}

#[then(regex = r#"^subscriber (\w+) is removed from the subscriber set$"#)]
async fn then_removed_from_set(world: &mut PubSubWorld, name: String) {
    let ps = the_pubsub(world);
    let id = world.subs.get(&name).expect("subscriber spawned").reff.id();
    let ok = settle_membership(&ps, id, false).await;
    assert!(
        ok,
        "the dead subscriber {name} must be pruned from the subscriber set"
    );
}

#[then(regex = r#"^the publish completes without error$"#)]
async fn then_publish_no_error(world: &mut PubSubWorld) {
    assert!(
        world.publish_elapsed.is_some(),
        "the publish must have returned"
    );
    let ps = the_pubsub(world);
    // Liveness: the pubsub answers a query => it did not panic in the run-loop.
    let _ = live_subscriber_count(&ps).await;
}

#[then(regex = r#"^the publish completes without blocking$"#)]
async fn then_publish_no_block(world: &mut PubSubWorld) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(500),
        "BestEffort publish must not block on a full mailbox; took {elapsed:?}"
    );
}

#[then(regex = r#"^the publish completes only after (\w+) has accepted the message$"#)]
async fn then_publish_after_accept(world: &mut PubSubWorld, name: String) {
    // Guaranteed delivery `tell(..).await`s each subscriber, so the publish only
    // returns once S has accepted; S then handles it.
    assert!(
        world.publish_elapsed.is_some(),
        "the publish must have returned"
    );
    let ok = settle(|| count_of(world, &name) == 1).await;
    assert!(
        ok,
        "subscriber {name} must have accepted and handled the message"
    );
}

#[then(regex = r#"^the publish returns within approximately the timeout$"#)]
async fn then_publish_within_timeout(world: &mut PubSubWorld) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(1000),
        "TimedDelivery must bound the wait; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^the publish returns without waiting for (\w+) to accept$"#)]
async fn then_publish_no_wait(world: &mut PubSubWorld, _name: String) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(500),
        "Spawned delivery returns immediately; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^once (\w+)'s mailbox drains, (\w+) eventually receives exactly 1 message$"#)]
async fn then_drains_then_receives(world: &mut PubSubWorld, drain: String, name: String) {
    if let Some(tx) = world.subs.get_mut(&drain).and_then(|s| s.release.take()) {
        let _ = tx.send(true);
    }
    let ok = settle(|| count_of(world, &name) == 1).await;
    assert!(
        ok,
        "once {drain} drains, {name} must eventually receive exactly 1 message"
    );
}

#[then(regex = r#"^the PubSub actor does not panic$"#)]
async fn then_no_panic(world: &mut PubSubWorld) {
    let ps = the_pubsub(world);
    // If the run-loop had panicked, this ask would fail with ActorNotRunning.
    let r = ps.ask(CountSubscribers::<Msg>::new()).await;
    assert!(
        r.is_ok(),
        "the PubSub actor must still be running (no panic)"
    );
}

// ===========================================================================
// @linearizability — concurrent producers + shared-state filter
// ===========================================================================

#[given(
    regex = r#"^a subscriber (\w+) is subscribed with a filter that consults a shared counter$"#
)]
async fn given_counting_filter(world: &mut PubSubWorld, name: String) {
    FILTER_CALLS.store(0, Ordering::SeqCst);
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe_filter(world, &name, counting_even).await;
}

#[then(regex = r#"^the filter is invoked exactly once per published message$"#)]
async fn then_filter_invoked_once_each(world: &mut PubSubWorld) {
    // Wait for all spawned/serialised publish handling to drain the mailbox.
    let ps = the_pubsub(world);
    let _ = live_subscriber_count(&ps).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let calls = FILTER_CALLS.load(Ordering::SeqCst);
    assert_eq!(
        calls,
        world.published.len(),
        "the filter must be invoked exactly once per published message"
    );
}

#[then(
    regex = r#"^subscriber (\w+) receives exactly the messages the filter accepted, no more and no fewer$"#
)]
async fn then_receives_filter_accepted(world: &mut PubSubWorld, name: String) {
    // Independent oracle: recompute the accepted set from the published payloads
    // by the SAME parity rule `counting_even` decides on — but without calling
    // the SUT filter (a separate `% 2 == 0` over the published numbers).
    let expected: Vec<String> = world
        .published
        .iter()
        .filter(|p| p.parse::<u64>().map(|n| n % 2 == 0).unwrap_or(false))
        .cloned()
        .collect();
    let ok = settle(|| count_of(world, &name) == expected.len()).await;
    let mut got = record_of(world, &name);
    got.sort();
    let mut want = expected.clone();
    want.sort();
    assert!(
        ok,
        "subscriber {name} should receive {} accepted messages, got {}",
        expected.len(),
        record_of(world, &name).len()
    );
    assert_eq!(
        got, want,
        "subscriber {name} must receive exactly the filter-accepted messages"
    );
}

// ===========================================================================
// @property / @model — inline proptest laws (bounded boundary loops for the
// async + global-state laws, per docs/testing/README.md §4).
// ===========================================================================

#[given(regex = r#"^a single subscriber S subscribed with any filter predicate f$"#)]
async fn given_property_filter_marker(_world: &mut PubSubWorld) {
    // No-op: the law step drives the full ∀f loop itself (proptest cannot host
    // the async actor cleanly, so the When step runs the bounded boundary loop).
}

#[when(regex = r#"^any sequence of messages is published$"#)]
async fn when_property_filter_law(world: &mut PubSubWorld) {
    // LAW: ∀ filter f, ∀ message sequence ms.
    //   received(S) == ms.filter(f)  AND  S is never pruned by a false filter.
    // Documented bounded boundary-loop (docs/testing/README.md §4): the async
    // actor + per-subscriber `fn` filter cannot be driven inside a sync
    // `proptest!`, so we enumerate the GEN-named boundaries explicitly.
    //
    // GEN: f ∈ {accept_all, accept_none, keep "keep", topic "Topic:"} ; sequence
    //      length n ∈ {0, 1, 2, 64} with payloads hitting all-accept / all-reject
    //      / mixed branches.
    // ORACLE: the filter predicate itself (an independent re-evaluation here).
    let strategy = the_pubsub(world);
    let _ = strategy; // a PubSub already exists from the Given.

    let filters: &[(fn(&Msg) -> bool, &str)] = &[
        (accept_all, "all"),
        (accept_none, "none"),
        (keep_keep, "keep"),
        (topic_a, "topicA"),
    ];
    let lengths = [0usize, 1, 2, 64];

    for &(f, fname) in filters {
        for &n in &lengths {
            // Fresh pubsub + subscriber per case to isolate counts.
            let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
            ps.wait_for_startup().await;
            let sub = spawn_recorder(None).await;
            let sub_id = sub.reff.id();
            ps.tell(SubscribeFilter(sub.reff.clone(), f))
                .await
                .expect("subscribe filter");

            // Build a mixed payload set hitting accept and reject branches.
            let mut msgs = Vec::with_capacity(n);
            for i in 0..n {
                let payload = match i % 4 {
                    0 => "keep".to_string(),
                    1 => "drop".to_string(),
                    2 => format!("TopicA: {i}"),
                    _ => format!("other-{i}"),
                };
                msgs.push(payload);
            }

            for m in &msgs {
                ps.tell(Publish(Msg(m.clone())))
                    .await
                    .expect("publish in property law");
            }

            // ORACLE — independent re-evaluation of f over the published seq.
            let expected: Vec<String> = msgs
                .iter()
                .filter(|m| f(&Msg((*m).clone())))
                .cloned()
                .collect();

            // settle until counts match (Guaranteed delivery awaits acceptance,
            // but the handler still runs after the tell returns).
            let want_len = expected.len();
            let record = Arc::clone(&sub.record);
            let mut ok = false;
            for _ in 0..400 {
                if record.lock().unwrap().received.len() == want_len {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let got = record.lock().unwrap().received.clone();
            assert!(
                ok,
                "filter {fname} n={n}: expected {want_len} accepted, got {}",
                got.len()
            );
            assert_eq!(
                got, expected,
                "filter {fname} n={n}: S must receive exactly f-accepted msgs in publish order"
            );
            // A false filter never attempts delivery, so S is never pruned even
            // though some messages were rejected.
            assert!(
                contains_subscriber(&ps, sub_id).await,
                "filter {fname} n={n}: a false filter must never prune S"
            );
        }
    }
}

#[then(
    regex = r#"^S receives exactly the published messages for which f returns true, in publish order$"#
)]
async fn then_property_filter_order(_world: &mut PubSubWorld) {
    // Assertions live inside the When law (it owns the per-case oracle).
}

#[then(regex = r#"^S receives none of the messages for which f returns false$"#)]
async fn then_property_filter_none_false(_world: &mut PubSubWorld) {}

#[then(regex = r#"^S is never pruned by a false filter, because no delivery is attempted$"#)]
async fn then_property_filter_no_prune(_world: &mut PubSubWorld) {}

// --- @property: fan-out over a set of subscribers --------------------------

#[given(regex = r#"^any set of subscribers each with its own filter$"#)]
async fn given_property_fanout_marker(_world: &mut PubSubWorld) {}

#[when(regex = r#"^a single message m is published$"#)]
async fn when_property_fanout_law(world: &mut PubSubWorld) {
    // LAW: ∀ subscriber-set with per-subscriber filters, publishing m delivers
    //   one clone to exactly { s : s.filter(m) } and nothing to the rest.
    // GEN: subscriber count k ∈ {0, 1, 3, 16} with a mix of accepting/rejecting
    //      filters incl. all-accept and all-reject boundaries.
    // ORACLE: { s : s.filter(m) } from the per-subscriber predicates.
    let _ = the_pubsub(world);
    let m = Msg("keep".to_string());

    for &k in &[0usize, 1, 3, 16] {
        let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
        ps.wait_for_startup().await;

        // Alternate accepting (keep "keep") and rejecting (accept_none) filters,
        // guaranteeing both all-accept (k=1 first filter) and rejecting members.
        let mut subs = Vec::with_capacity(k);
        let mut expect_accept = Vec::with_capacity(k);
        for i in 0..k {
            let sub = spawn_recorder(None).await;
            let f: fn(&Msg) -> bool = if i % 2 == 0 { keep_keep } else { accept_none };
            ps.tell(SubscribeFilter(sub.reff.clone(), f))
                .await
                .expect("subscribe");
            expect_accept.push(f(&m));
            subs.push(sub);
        }

        ps.tell(Publish(m.clone())).await.expect("publish");

        for (i, sub) in subs.iter().enumerate() {
            let want = if expect_accept[i] { 1 } else { 0 };
            let record = Arc::clone(&sub.record);
            let mut ok = false;
            for _ in 0..400 {
                if record.lock().unwrap().received.len() == want {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            // also ensure no over-delivery to rejecting members
            tokio::time::sleep(Duration::from_millis(5)).await;
            let got = record.lock().unwrap().received.len();
            assert!(
                ok && got == want,
                "k={k} subscriber {i}: expected {want} clone(s) of m, got {got}"
            );
        }
    }
}

#[then(regex = r#"^exactly the subscribers whose filter accepts m receive one clone each$"#)]
async fn then_property_fanout_accept(_world: &mut PubSubWorld) {}

#[then(regex = r#"^every other subscriber receives nothing$"#)]
async fn then_property_fanout_reject(_world: &mut PubSubWorld) {}

// --- @property: only terminal SendErrors prune -----------------------------

#[given(
    regex = r#"^a subscriber S whose mailbox is full past any timeout, with an accepting filter$"#
)]
async fn given_property_prune_marker(_world: &mut PubSubWorld) {}

#[given(regex = r#"^a running PubSub with delivery strategy "([A-Za-z]+)" or "([A-Za-z]+)"$"#)]
async fn given_property_strategy_pair(_world: &mut PubSubWorld, _s1: String, _s2: String) {
    // The law's When step enumerates the strategy pair itself.
}

#[when(regex = r#"^a message is published$"#)]
async fn when_property_prune_law_or_plain(world: &mut PubSubWorld) {
    // This step text is shared with the example feature's "a message is
    // published". The property feature reaches it only after the
    // "BestEffort or TimedDelivery" Given + the full-mailbox Given markers, so we
    // disambiguate on whether a pubsub already exists with subscribers.
    //
    // In the example feature a pubsub + subscriber were set up by prior Givens, so
    // we just publish once. In the property feature, the prior Givens are no-op
    // markers, so we run the full ∀strategy,∀timeout loop here.
    if world.pubsub.is_some() {
        publish_payload(world, "m").await;
        return;
    }
    // Property law branch (no pubsub built yet): enumerate the boundaries.
    run_prune_property_law().await;
    // Leave a live PubSub in the World so the trailing shared
    // "the PubSub actor does not panic" step has an actor to query.
    let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
    ps.wait_for_startup().await;
    world.pubsub = Some(ps);
}

/// LAW: full / slow mailbox (MailboxFull / Timeout) NEVER prunes; only
/// ActorNotRunning / ActorStopped does.
/// GEN: strategy ∈ {BestEffort, TimedDelivery(τ)} with τ ∈ {ZERO, 1ms, 50ms}.
/// ORACLE: pruned-variant set = {ActorNotRunning, ActorStopped}; MailboxFull /
///         Timeout ∉ set.
async fn run_prune_property_law() {
    let strategies = [
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(1)),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    for strat in strategies {
        let ps = PubSub::spawn(PubSub::new(strat));
        ps.wait_for_startup().await;
        let mut sub = spawn_recorder(Some(1)).await;
        make_full(&mut sub).await;
        let id = sub.reff.id();
        ps.tell(Subscribe(sub.reff.clone()))
            .await
            .expect("subscribe");
        ps.tell(Publish(Msg("m".to_string())))
            .await
            .expect("publish");
        // Settle a beat for any (wrongful) pruning to occur.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            contains_subscriber(&ps, id).await,
            "strategy {strat:?}: a full/slow mailbox must NOT prune the subscriber"
        );
        assert!(
            sub.record.lock().unwrap().received.is_empty(),
            "strategy {strat:?}: the full subscriber must not have received the message"
        );
        // release the parked holds to let the actor wind down cleanly
        if let Some(tx) = sub.release.take() {
            let _ = tx.send(true);
        }
    }
}

#[then(regex = r#"^S does not receive the message$"#)]
async fn then_property_prune_no_recv(_world: &mut PubSubWorld) {}

#[then(regex = r#"^S remains in the subscriber set for any timeout value$"#)]
async fn then_property_prune_remains(_world: &mut PubSubWorld) {}

// --- @model: subscriber-set size == distinct ids ---------------------------

#[given(
    regex = r#"^any sequence of subscribe / subscribe_filter operations over a pool of actors$"#
)]
async fn given_model_seq_marker(_world: &mut PubSubWorld) {}

#[when(regex = r#"^the sequence is applied$"#)]
async fn when_model_seq_law(world: &mut PubSubWorld) {
    // MODEL: a HashMap<ActorId, Filter> reference. Applying any subscribe sequence,
    //   subscriber-set size == # distinct ids; live filter == last insert for id.
    // GEN: op-sequence length ∈ {0, 1, 2, 32} over (id ∈ {A0..A3}, filter ∈
    //      {true, "old", "new"}); MUST include re-subscribing the same id twice
    //      with different filters and subscribing distinct ids.
    // ORACLE: the HashMap model below.
    let _ = the_pubsub(world);

    // A fixed pool of 4 actors (distinct ActorIds).
    let lengths = [0usize, 1, 2, 32];
    let filter_choices: [(fn(&Msg) -> bool, &str); 3] =
        [(accept_all, "true"), (keep_old, "old"), (keep_new, "new")];

    for &len in &lengths {
        let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
        ps.wait_for_startup().await;

        let mut pool = Vec::new();
        for _ in 0..4 {
            pool.push(spawn_recorder(None).await);
        }

        // Deterministic op sequence that includes re-subscribing the same id with
        // different filters AND subscribing distinct ids.
        let mut model: HashMap<ActorId, &str> = HashMap::new();
        for i in 0..len {
            let actor_idx = i % 4;
            let (f, fname) = filter_choices[i % 3];
            let sub = &pool[actor_idx];
            let id = sub.reff.id();
            if i % 2 == 0 {
                ps.tell(Subscribe(sub.reff.clone()))
                    .await
                    .expect("subscribe");
                model.insert(id, "true");
            } else {
                ps.tell(SubscribeFilter(sub.reff.clone(), f))
                    .await
                    .expect("subscribe filter");
                model.insert(id, fname);
            }
        }

        // ORACLE check 1: subscriber-set size == # distinct ids inserted.
        let live = live_subscriber_count(&ps).await;
        assert_eq!(
            live,
            model.len(),
            "len={len}: subscriber-set size must equal distinct-id count (model {})",
            model.len()
        );

        // ORACLE check 2: each present id is exactly the distinct set.
        for id in model.keys() {
            assert!(
                contains_subscriber(&ps, *id).await,
                "len={len}: id {id:?} from the model must be present"
            );
        }

        // ORACLE check 3 (filter overwrite): re-subscribe pool[0] with "new" then
        // "old" and confirm only the last filter is live by publishing.
        if len > 0 {
            let sub = spawn_recorder(None).await;
            let id = sub.reff.id();
            ps.tell(SubscribeFilter(sub.reff.clone(), keep_new))
                .await
                .expect("subscribe new");
            ps.tell(SubscribeFilter(sub.reff.clone(), keep_old))
                .await
                .expect("re-subscribe old");
            // exactly one entry per id: re-subscribe did not duplicate.
            assert!(contains_subscriber(&ps, id).await);
            ps.tell(Publish(Msg("new".to_string())))
                .await
                .expect("publish new");
            ps.tell(Publish(Msg("old".to_string())))
                .await
                .expect("publish old");
            let record = Arc::clone(&sub.record);
            let mut ok = false;
            for _ in 0..400 {
                if record.lock().unwrap().received == vec!["old".to_string()] {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let got = record.lock().unwrap().received.clone();
            assert!(
                ok,
                "len={len}: only the most-recently-installed filter (old) must apply, got {got:?}"
            );
        }
    }
}

#[then(regex = r#"^the subscriber set holds exactly one entry per distinct ActorId$"#)]
async fn then_model_one_per_id(_world: &mut PubSubWorld) {}

#[then(
    regex = r#"^a re-subscribe of an already-present ActorId overwrites its filter, never duplicates it$"#
)]
async fn then_model_overwrite(_world: &mut PubSubWorld) {}

#[then(
    regex = r#"^a publish thereafter applies only the most-recently-installed filter for that id$"#
)]
async fn then_model_last_filter(_world: &mut PubSubWorld) {}

// --- @model: concurrent publishes refine a per-subscriber counter ----------

#[given(regex = r#"^any fixed set of subscribers with per-subscriber filters$"#)]
async fn given_model_concurrent_marker(_world: &mut PubSubWorld) {}

#[when(regex = r#"^N messages are published concurrently from P tasks$"#)]
async fn when_model_concurrent_law(world: &mut PubSubWorld) {
    // MODEL: per-subscriber integer counter incremented once per published
    //   message its filter accepts. PubSub serialises publish handling on its
    //   mailbox, so each filter is invoked once per message.
    // GEN: P ∈ [2, 10]; N ∈ {1, 50, 100}; subscribers incl. const-true, rejecting,
    //      and a shared-counter filter.
    // ORACLE: independent per-subscriber counters over the published payloads.
    let _ = the_pubsub(world);

    let cases = [(2usize, 1usize), (5, 50), (10, 100)];
    for (p, n) in cases {
        FILTER_CALLS.store(0, Ordering::SeqCst);
        let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Guaranteed));
        ps.wait_for_startup().await;

        let always = spawn_recorder(None).await;
        let never = spawn_recorder(None).await;
        let counting = spawn_recorder(None).await;
        ps.tell(Subscribe(always.reff.clone()))
            .await
            .expect("sub always");
        ps.tell(SubscribeFilter(never.reff.clone(), accept_none))
            .await
            .expect("sub never");
        ps.tell(SubscribeFilter(counting.reff.clone(), counting_even))
            .await
            .expect("sub counting");

        // Publish N even payloads concurrently from P tasks; even => counting_even
        // accepts ALL of them, and accept_all accepts all, accept_none none.
        let per = n / p;
        let total = per * p;
        let barrier = Arc::new(Barrier::new(p));
        let mut handles = Vec::new();
        for t in 0..p {
            let ps = ps.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..per {
                    let payload = ((t * per + i) * 2).to_string();
                    ps.tell(Publish(Msg(payload))).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.expect("join");
        }

        // ORACLE: always == total, never == 0, counting == total (all even).
        let want_always = total;
        let want_counting = total;
        let mut ok = false;
        for _ in 0..600 {
            let a = always.record.lock().unwrap().received.len();
            let c = counting.record.lock().unwrap().received.len();
            if a == want_always && c == want_counting {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let a = always.record.lock().unwrap().received.len();
        let c = counting.record.lock().unwrap().received.len();
        let nv = never.record.lock().unwrap().received.len();
        assert!(
            ok,
            "P={p} N={total}: always got {a}/{want_always}, counting got {c}/{want_counting}"
        );
        assert_eq!(
            a, want_always,
            "P={p} N={total}: const-true must get every message"
        );
        assert_eq!(
            nv, 0,
            "P={p} N={total}: rejecting subscriber must get nothing"
        );
        assert_eq!(
            c, want_counting,
            "P={p} N={total}: counting filter accepts all even"
        );
        // each filter invoked exactly once per published message: counting_even
        // was called total times (it is the only counting filter).
        assert_eq!(
            FILTER_CALLS.load(Ordering::SeqCst),
            total,
            "P={p} N={total}: the filter must be invoked exactly once per message"
        );
    }
}

#[then(
    regex = r#"^each subscriber's received count equals the number of published messages its filter accepts — no loss, no duplication$"#
)]
async fn then_model_concurrent_counts(_world: &mut PubSubWorld) {}

#[then(regex = r#"^each filter is invoked exactly once per published message$"#)]
async fn then_model_concurrent_filter_once(_world: &mut PubSubWorld) {}

// ===========================================================================
// @bug demonstrator support (used by pubsub_bug_bdd.rs)
// ===========================================================================
//
// The @bug:actors/src/pubsub.rs:125 scenario (in BOTH features) asserts the
// DESIRED behaviour: a Spawned-delivery dead subscriber is eventually pruned.
// Today pubsub.rs:125-131 discards the spawned task result, so ActorNotRunning is
// never observed and S is never pruned — the scenario therefore FAILS today and
// is excluded from the green runners (their `!t.starts_with("bug")` filter).
//
// The live-defect probe lives in `pubsub_bug_bdd.rs` as a direct `#[tokio::test]`
// that asserts the CURRENT (buggy) state: after a Spawned publish to a stopped
// subscriber, S is STILL present in the subscriber set. That probe passes today
// and will START FAILING the moment pubsub.rs:125 is fixed to prune.
//
// The `spawned_dead_subscriber_remains` helper below is the shared SUT driver the
// probe calls, so the "current behaviour" is exercised through the real SUT.

/// Drives the SUT exactly as the @bug scenario does and returns whether the dead
/// Spawned subscriber is STILL present in the subscriber set after a publish.
/// Returns `true` while the :125 leak is live (buggy), `false` once it is fixed.
///
/// Only the `pubsub_bug_bdd` runner calls this; the other two runners include the
/// same shared module without it, hence the per-runner `dead_code` allowance.
#[allow(dead_code)]
pub async fn spawned_dead_subscriber_remains() -> bool {
    let ps = PubSub::spawn(PubSub::new(DeliveryStrategy::Spawned));
    ps.wait_for_startup().await;
    let sub = spawn_recorder(None).await;
    let id = sub.reff.id();
    ps.tell(Subscribe(sub.reff.clone()))
        .await
        .expect("subscribe");
    // Stop the subscriber so any attempted delivery would yield ActorNotRunning.
    sub.reff.kill();
    sub.reff.wait_for_shutdown().await;
    // Publish: Spawned fires a tokio task whose result is discarded (the defect).
    ps.tell(Publish(Msg("m".to_string())))
        .await
        .expect("publish");
    // Give any pruning ample time to occur (it will not, today).
    for _ in 0..100 {
        if !contains_subscriber(&ps, id).await {
            return false; // pruned => bug is fixed
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    contains_subscriber(&ps, id).await
}
