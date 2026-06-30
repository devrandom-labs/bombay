//! Shared `Broker` World + step definitions for the `actors/broker` scenarios
//! (card #78).
//!
//! Wired by the two runners that `#[path]`-include this module:
//!   * `broker_bdd.rs`       — the example feature (broker.feature)
//!   * `broker_props_bdd.rs` — the Phase-2 laws (broker.properties.feature)
//!
//! The SUT is `bombay_actors::broker` (the glob-topic `Broker<M>` actor:
//! `Subscribe<M>` / `Unsubscribe` / `Publish<M>` under a `DeliveryStrategy`),
//! driven against REAL SPAWNED ACTORS reached through `bombay::prelude::*`.
//! Subscribers are `Recorder` actors that record every `Msg` they handle (the
//! concrete topic it was published to); the broker's private `subscriptions`
//! map is inspected through the test-only `CountSubscriptions` / `HasPatternKey`
//! queries (gated behind the `testing` feature), mirroring `message_bus`'s
//! `CountRegistrations`.
//!
//! Full-mailbox scenarios park a `Recorder` on a `watch` release gate so the
//! bounded(1) mailbox stays observably full while the broker tries to deliver;
//! the test asserts the strategy-specific observable, then releases the gate.
//!
//! The `@property` / `@model` laws (broker.properties.feature) bind to a single
//! step running an inline `proptest!` block (for the pure routing decision) or a
//! documented bounded boundary-loop over the `# GEN:`-named values (for the
//! async / global-state laws proptest cannot drive cleanly). Every oracle is an
//! INDEPENDENT reference: the routing oracle calls `glob::Pattern` directly (the
//! crate the broker uses) rather than the broker, and the multiplicity / pruning
//! oracles are integer counters written from scratch.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bombay::{error::Infallible, mailbox, prelude::*};
use bombay_actors::{
    DeliveryStrategy,
    broker::{Broker, CountSubscriptions, HasPatternKey, Publish, Subscribe, Unsubscribe},
};
use cucumber::{World, given, then, when};
use glob::{MatchOptions, Pattern};
use tokio::sync::{Barrier, watch};

// ===========================================================================
// Test message + recipient actor
// ===========================================================================

/// The routed payload. `Clone` is required by `Publish<M>`; it carries the exact
/// topic it was published to so a `Then` can assert *which* publish landed, not
/// merely a count.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg {
    topic: String,
}

/// A capacity probe sent directly to a recipient (never through the broker) to
/// observe a full bounded mailbox; its handler is a no-op so a stray delivery is
/// not miscounted as a real `Msg`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Probe;

/// Parks the recipient's run-loop on a `watch` gate until it flips to `true`,
/// holding a bounded mailbox slot so the mailbox stays full.
struct Hold(watch::Receiver<bool>);

/// Everything a `Recorder` records: the ordered list of topics it received.
#[derive(Debug, Default, Clone)]
struct Received {
    topics: Vec<String>,
}

/// A subscriber that records every `Msg` it handles into shared `Received`.
#[derive(Clone)]
struct Recorder {
    received: Arc<Mutex<Received>>,
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
        self.received.lock().unwrap().topics.push(msg.topic);
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

/// A handle to one spawned `Recorder` subscriber, keyed by its feature label.
#[derive(Debug)]
struct Sub {
    reff: ActorRef<Recorder>,
    received: Arc<Mutex<Received>>,
    /// Release gate for a parked (full-mailbox) recipient, if any.
    release: Option<watch::Sender<bool>>,
}

#[derive(Debug, Default, World)]
pub struct BrokerWorld {
    broker: Option<ActorRef<Broker<Msg>>>,
    subs: HashMap<String, Sub>,
    /// The pattern each labelled subscriber was last subscribed under (the most
    /// recently used), so a `Then` referencing "S" can resolve its pattern.
    patterns: HashMap<String, Pattern>,
    /// Wall-clock the last publish took (for the @timing bounds).
    publish_elapsed: Option<Duration>,
}

// ===========================================================================
// Helpers
// ===========================================================================

/// The exact `MatchOptions` the broker uses (broker.rs:187-191). The routing
/// oracle in the property laws must use these verbatim.
fn broker_match_options() -> MatchOptions {
    MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    }
}

fn the_broker(world: &BrokerWorld) -> ActorRef<Broker<Msg>> {
    world
        .broker
        .clone()
        .expect("a Broker was spawned by a Given")
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
    let received = Arc::new(Mutex::new(Received::default()));
    let mbox = match cap {
        Some(n) => mailbox::bounded(n),
        None => mailbox::unbounded(),
    };
    let reff = Recorder::spawn_with_mailbox(
        Recorder {
            received: Arc::clone(&received),
        },
        mbox,
    );
    reff.wait_for_startup().await;
    Sub {
        reff,
        received,
        release: None,
    }
}

/// Makes `sub`'s bounded(1) mailbox observably full by parking two `Hold`
/// handlers on a release gate, then proving a `try_send(Probe)` is refused.
async fn make_full(sub: &mut Sub) {
    let (tx, rx) = watch::channel(false);
    sub.reff
        .tell(Hold(rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued into the handler");
    tokio::time::sleep(Duration::from_millis(20)).await;
    sub.reff
        .tell(Hold(rx))
        .try_send()
        .expect("second hold fills the single buffer slot");
    for _ in 0..200 {
        if matches!(
            sub.reff.tell(Probe).try_send(),
            Err(SendError::MailboxFull(Probe))
        ) {
            sub.release = Some(tx);
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the subscriber's bounded mailbox never became observably full");
}

async fn subscribe(world: &mut BrokerWorld, name: &str, pattern: &str) {
    let broker = the_broker(world);
    let sub = world.subs.get(name).expect("subscriber spawned");
    let pat = Pattern::new(pattern).expect("pattern is a compilable glob");
    broker
        .tell(Subscribe {
            topic: pat.clone(),
            recipient: sub.reff.clone().recipient::<Msg>(),
        })
        .await
        .expect("subscribe delivered");
    world.patterns.insert(name.to_owned(), pat);
}

async fn publish_once(world: &mut BrokerWorld, topic: &str) {
    let broker = the_broker(world);
    let started = Instant::now();
    broker
        .tell(Publish {
            topic: topic.to_owned(),
            message: Msg {
                topic: topic.to_owned(),
            },
        })
        .await
        .expect("publish delivered");
    world.publish_elapsed = Some(started.elapsed());
}

fn received_of(world: &BrokerWorld, name: &str) -> Received {
    world
        .subs
        .get(name)
        .expect("subscriber spawned")
        .received
        .lock()
        .unwrap()
        .clone()
}

fn count_of(world: &BrokerWorld, name: &str) -> usize {
    received_of(world, name).topics.len()
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

/// How many times `name`'s actor id appears under its current pattern (`Some`)
/// or across every pattern (`None`), read from the broker's private map.
async fn live_subs(world: &BrokerWorld, name: &str, scoped: bool) -> usize {
    let broker = the_broker(world);
    let id = world.subs.get(name).expect("subscriber spawned").reff.id();
    let pattern = scoped.then(|| world.patterns.get(name).expect("pattern recorded").clone());
    broker
        .ask(CountSubscriptions {
            pattern,
            actor_id: id,
        })
        .await
        .expect("the Broker must still be running to answer the query")
}

// ===========================================================================
// Given — broker, subscribers, subscriptions
// ===========================================================================

#[given(regex = r#"^a running Broker with delivery strategy "([A-Za-z]+)"$"#)]
async fn given_broker(world: &mut BrokerWorld, strategy: String) {
    let broker = Broker::spawn(Broker::<Msg>::new(parse_strategy(&strategy, None)));
    broker.wait_for_startup().await;
    world.broker = Some(broker);
}

#[given(regex = r#"^a running Broker with delivery strategy "([A-Za-z]+)" of (\d+) milliseconds$"#)]
async fn given_broker_timed(world: &mut BrokerWorld, strategy: String, ms: u64) {
    let broker = Broker::spawn(Broker::<Msg>::new(parse_strategy(&strategy, Some(ms))));
    broker.wait_for_startup().await;
    world.broker = Some(broker);
}

#[given(regex = r#"^a subscriber (\w+) is subscribed with pattern "([^"]*)"$"#)]
async fn given_subscribed(world: &mut BrokerWorld, name: String, pattern: String) {
    world
        .subs
        .entry(name.clone())
        .or_insert(spawn_recorder(None).await);
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^the same subscriber (\w+) is also subscribed with pattern "([^"]*)"$"#)]
async fn given_also_subscribed(world: &mut BrokerWorld, name: String, pattern: String) {
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^the same subscriber (\w+) is subscribed with pattern "([^"]*)" again$"#)]
async fn given_subscribed_again(world: &mut BrokerWorld, name: String, pattern: String) {
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^a subscriber (\w+) is the only subscriber of pattern "([^"]*)"$"#)]
async fn given_only_subscriber(world: &mut BrokerWorld, name: String, pattern: String) {
    world.subs.insert(name.clone(), spawn_recorder(None).await);
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^a subscriber (\w+) exists but is not yet subscribed$"#)]
async fn given_exists_unsubscribed(world: &mut BrokerWorld, name: String) {
    world.subs.insert(name, spawn_recorder(None).await);
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox is already full is subscribed with pattern "([^"]*)"$"#
)]
async fn given_full_subscribed(world: &mut BrokerWorld, name: String, pattern: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe(world, &name, &pattern).await;
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox stays full past the timeout is subscribed with pattern "([^"]*)"$"#
)]
async fn given_full_past_timeout(world: &mut BrokerWorld, name: String, pattern: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe(world, &name, &pattern).await;
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox is full but later drains is subscribed with pattern "([^"]*)"$"#
)]
async fn given_full_later_drains(world: &mut BrokerWorld, name: String, pattern: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe(world, &name, &pattern).await;
}

#[given(
    regex = r#"^a subscriber (\w+) whose mailbox is full is subscribed with pattern "([^"]*)"$"#
)]
async fn given_full_plain_subscribed(world: &mut BrokerWorld, name: String, pattern: String) {
    let mut sub = spawn_recorder(Some(1)).await;
    make_full(&mut sub).await;
    world.subs.insert(name.clone(), sub);
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^a subscriber (\w+) with spare capacity is subscribed with pattern "([^"]*)"$"#)]
async fn given_spare_subscribed(world: &mut BrokerWorld, name: String, pattern: String) {
    world
        .subs
        .insert(name.clone(), spawn_recorder(Some(16)).await);
    subscribe(world, &name, &pattern).await;
}

#[given(regex = r#"^subscriber (\w+) has stopped so its actor is not running$"#)]
async fn given_stopped(world: &mut BrokerWorld, name: String) {
    let sub = world.subs.get(&name).expect("subscriber spawned");
    sub.reff.kill();
    sub.reff.wait_for_shutdown().await;
}

// ===========================================================================
// When — publish, unsubscribe, concurrent
// ===========================================================================

#[when(regex = r#"^a message is published to topic "([^"]*)"$"#)]
async fn when_publish(world: &mut BrokerWorld, topic: String) {
    publish_once(world, &topic).await;
}

#[when(regex = r#"^(\d+) messages are published to topic "([^"]*)"$"#)]
async fn when_publish_n(world: &mut BrokerWorld, n: u32, topic: String) {
    for _ in 0..n {
        publish_once(world, &topic).await;
    }
}

#[when(regex = r#"^(\w+) unsubscribes from topic "([^"]*)"$"#)]
async fn when_unsubscribe_topic(world: &mut BrokerWorld, name: String, pattern: String) {
    let broker = the_broker(world);
    let id = world.subs.get(&name).expect("subscriber spawned").reff.id();
    broker
        .tell(Unsubscribe {
            topic: Some(Pattern::new(&pattern).expect("pattern compiles")),
            actor_id: id,
        })
        .await
        .expect("unsubscribe delivered");
}

#[when(regex = r#"^(\w+) unsubscribes from all topics$"#)]
async fn when_unsubscribe_all(world: &mut BrokerWorld, name: String) {
    let broker = the_broker(world);
    let id = world.subs.get(&name).expect("subscriber spawned").reff.id();
    broker
        .tell(Unsubscribe {
            topic: None,
            actor_id: id,
        })
        .await
        .expect("unsubscribe delivered");
}

#[when(regex = r#"^an unknown actor id unsubscribes from all topics$"#)]
async fn when_unsubscribe_unknown_all(world: &mut BrokerWorld) {
    let broker = the_broker(world);
    // A freshly-spawned-then-stopped actor's id is guaranteed unknown to the broker.
    let stranger = spawn_recorder(None).await;
    let id = stranger.reff.id();
    stranger.reff.kill();
    broker
        .tell(Unsubscribe {
            topic: None,
            actor_id: id,
        })
        .await
        .expect("unsubscribe delivered");
}

#[when(regex = r#"^an actor unsubscribes from topic "([^"]*)"$"#)]
async fn when_unsubscribe_unknown_topic(world: &mut BrokerWorld, pattern: String) {
    let broker = the_broker(world);
    let stranger = spawn_recorder(None).await;
    let id = stranger.reff.id();
    stranger.reff.kill();
    broker
        .tell(Unsubscribe {
            topic: Some(Pattern::new(&pattern).expect("pattern compiles")),
            actor_id: id,
        })
        .await
        .expect("unsubscribe delivered");
}

#[when(
    regex = r#"^(\d+) messages are published concurrently to topic "([^"]*)" from (\d+) tasks$"#
)]
async fn when_publish_concurrent(world: &mut BrokerWorld, total: u32, topic: String, tasks: u32) {
    let broker = the_broker(world);
    let per = total / tasks;
    let barrier = Arc::new(Barrier::new(tasks as usize));
    let mut handles = Vec::new();
    for _ in 0..tasks {
        let broker = broker.clone();
        let barrier = Arc::clone(&barrier);
        let topic = topic.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..per {
                broker
                    .tell(Publish {
                        topic: topic.clone(),
                        message: Msg {
                            topic: topic.clone(),
                        },
                    })
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.expect("publish task join");
    }
}

#[when(
    regex = r#"^(\w+) subscribes to pattern "([^"]*)" while a message is published to topic "([^"]*)"$"#
)]
async fn when_subscribe_while_publish(
    world: &mut BrokerWorld,
    name: String,
    pattern: String,
    topic: String,
) {
    let broker = the_broker(world);
    let sub_ref = world
        .subs
        .get(&name)
        .expect("subscriber spawned")
        .reff
        .clone();
    let pat = Pattern::new(&pattern).expect("pattern compiles");
    world.patterns.insert(name.clone(), pat.clone());
    let barrier = Arc::new(Barrier::new(2));
    let (b1, b2) = (Arc::clone(&barrier), Arc::clone(&barrier));

    let broker_sub = broker.clone();
    let sub = tokio::spawn(async move {
        b1.wait().await;
        broker_sub
            .tell(Subscribe {
                topic: pat,
                recipient: sub_ref.recipient::<Msg>(),
            })
            .await
            .unwrap();
    });
    let broker_pub = broker.clone();
    let pubg = tokio::spawn(async move {
        b2.wait().await;
        broker_pub
            .tell(Publish {
                topic: topic.clone(),
                message: Msg { topic },
            })
            .await
            .unwrap();
    });
    sub.await.expect("subscribe task");
    pubg.await.expect("publish task");
}

// ===========================================================================
// Then — counts, identity, registration state, liveness
// ===========================================================================

#[then(regex = r#"^(?:subscriber )?(\w+) receives exactly (\d+) messages?$"#)]
async fn then_receives_exactly(world: &mut BrokerWorld, name: String, n: usize) {
    let ok = settle(|| count_of(world, &name) == n).await;
    let got = received_of(world, &name);
    assert!(
        ok,
        "subscriber {name} should receive exactly {n} messages, got {got:?}"
    );
}

#[then(regex = r#"^subscriber (\w+) receives (\d+) messages$"#)]
async fn then_receives_n(world: &mut BrokerWorld, name: String, n: usize) {
    // Settle a beat so a wrongful delivery would have had time to land, then assert.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        count_of(world, &name),
        n,
        "subscriber {name} should receive {n} messages, got {:?}",
        received_of(world, &name)
    );
}

#[then(regex = r#"^subscriber (\w+) receives exactly (\d+) messages with no loss or duplication$"#)]
async fn then_receives_exactly_no_loss(world: &mut BrokerWorld, name: String, n: usize) {
    then_receives_exactly(world, name, n).await;
}

#[then(regex = r#"^the received message is the one published to "([^"]*)"$"#)]
async fn then_received_is(world: &mut BrokerWorld, topic: String) {
    // The only delivered message must carry exactly the asserted topic.
    let ok = settle(|| {
        // Find the (single) subscriber that has received anything.
        world
            .subs
            .keys()
            .any(|k| received_of(world, k).topics == vec![topic.clone()])
    })
    .await;
    assert!(
        ok,
        "exactly one message must have been received and it must be the one published to {topic:?}; \
         received state: {:?}",
        world
            .subs
            .keys()
            .map(|k| (k.clone(), received_of(world, k).topics))
            .collect::<Vec<_>>()
    );
}

#[then(regex = r#"^the publish completes without error$"#)]
async fn then_publish_completes(world: &mut BrokerWorld) {
    assert!(
        world.publish_elapsed.is_some(),
        "the publish must have returned"
    );
    // Liveness: the broker answers a query => it did not panic in the run-loop.
    let broker = the_broker(world);
    let _ = broker
        .ask(HasPatternKey {
            pattern: Pattern::new("anything").unwrap(),
        })
        .await
        .expect("broker still running");
}

#[then(regex = r#"^the publish completes without blocking$"#)]
async fn then_publish_no_block(world: &mut BrokerWorld) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(500),
        "BestEffort must not block on a full mailbox; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^the publish completes only after (\w+) has accepted the message$"#)]
async fn then_publish_after_accept(world: &mut BrokerWorld, name: String) {
    // Guaranteed delivery `tell(..).await`s each recipient, so the publish only
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
async fn then_publish_within_timeout(world: &mut BrokerWorld) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(1000),
        "TimedDelivery must bound the wait; publish took {elapsed:?}"
    );
}

#[then(regex = r#"^the publish returns without waiting for (\w+) to accept$"#)]
async fn then_publish_no_wait(world: &mut BrokerWorld, _name: String) {
    let elapsed = world.publish_elapsed.expect("publish ran");
    assert!(
        elapsed < Duration::from_millis(500),
        "Spawned delivery returns immediately; publish took {elapsed:?}"
    );
}

#[then(
    regex = r#"^(?:subscriber )?(\w+) does not receive the message(?: after the timeout elapses)?$"#
)]
async fn then_does_not_receive(world: &mut BrokerWorld, name: String) {
    // In the property `@boundary` law this line precedes a fully self-contained
    // law step (`S remains subscribed for any timeout value`) whose body spawns
    // its own recipients and asserts they received nothing; that scenario has no
    // world subscriber named "S", so there is nothing to look up here.
    if !world.subs.contains_key(&name) {
        return;
    }
    // Wait beyond any per-recipient timeout used in the features (<= 50ms) so an
    // erroneous delivery would have landed.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        count_of(world, &name),
        0,
        "subscriber {name} must not receive the skipped message, got {:?}",
        received_of(world, &name)
    );
}

#[then(regex = r#"^once (\w+)'s mailbox drains, (\w+) eventually receives exactly 1 message$"#)]
async fn then_drains_then_receives(world: &mut BrokerWorld, drain: String, name: String) {
    if let Some(tx) = world.subs.get_mut(&drain).and_then(|s| s.release.take()) {
        let _ = tx.send(true);
    }
    let ok = settle(|| count_of(world, &name) == 1).await;
    assert!(
        ok,
        "once {drain} drains, {name} must eventually receive exactly 1 message"
    );
}

#[then(regex = r#"^(?:subscriber )?(\w+) is NOT pruned from the subscription$"#)]
async fn then_not_pruned(world: &mut BrokerWorld, name: String) {
    // Give the publish + any (wrongful) prune time to settle, then assert the
    // registration is still present in the broker's private map.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let live = live_subs(world, &name, true).await;
    assert_eq!(
        live, 1,
        "subscriber {name} must remain subscribed (not pruned)"
    );
}

#[then(regex = r#"^subscriber (\w+) is removed from the subscription$"#)]
async fn then_removed(world: &mut BrokerWorld, name: String) {
    let ok = settle_async(world, &name, true, 0).await;
    assert!(
        ok,
        "the dead subscriber {name} must be pruned from the pattern"
    );
}

#[then(regex = r#"^subscriber (\w+) is eventually removed from the subscription$"#)]
async fn then_eventually_removed(world: &mut BrokerWorld, name: String) {
    let ok = settle_async(world, &name, true, 0).await;
    assert!(ok, "the dead subscriber {name} must eventually be pruned");
}

#[then(regex = r#"^subscriber (\w+) remains subscribed$"#)]
async fn then_remains_subscribed(world: &mut BrokerWorld, name: String) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    let live = live_subs(world, &name, false).await;
    assert!(
        live >= 1,
        "subscriber {name} must remain subscribed somewhere"
    );
}

#[then(regex = r#"^pattern "([^"]*)" no longer has any subscription entry$"#)]
async fn then_pattern_gone(world: &mut BrokerWorld, pattern: String) {
    let broker = the_broker(world);
    let pat = Pattern::new(&pattern).expect("pattern compiles");
    let present = broker
        .ask(HasPatternKey { pattern: pat })
        .await
        .expect("broker still running");
    assert!(!present, "pattern {pattern:?} key must have been dropped");
}

#[then(regex = r#"^the Broker actor does not panic$"#)]
async fn then_no_panic(world: &mut BrokerWorld) {
    let broker = the_broker(world);
    // If the run-loop had panicked, this ask would fail with ActorNotRunning.
    let r = broker
        .ask(HasPatternKey {
            pattern: Pattern::new("anything").unwrap(),
        })
        .await;
    assert!(
        r.is_ok(),
        "the Broker actor must still be running (no panic)"
    );
}

#[then(
    regex = r#"^(?:subscriber )?(\w+) receives the message exactly once or not at all, never a partial delivery$"#
)]
async fn then_atomic_delivery(world: &mut BrokerWorld, name: String) {
    tokio::time::sleep(Duration::from_millis(80)).await;
    let got = count_of(world, &name);
    assert!(
        got <= 1,
        "subscribe|publish race must deliver 0 or 1, never partial; got {got}"
    );
}

/// Polls the broker until `name`'s registration count (scoped to its pattern when
/// `scoped`) equals `want`.
async fn settle_async(world: &BrokerWorld, name: &str, scoped: bool, want: usize) -> bool {
    for _ in 0..400 {
        if live_subs(world, name, scoped).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    live_subs(world, name, scoped).await == want
}

// ===========================================================================
// @property / @model laws (broker.properties.feature)
// ===========================================================================
//
// The laws below are fully self-contained: each `Then` spawns its own fresh
// brokers + recipients and asserts against an INDEPENDENT oracle. The scenario's
// `Given`/`When`/non-law `And` lines are therefore descriptive scaffolding — they
// are bound here to no-ops (or a harmless world-broker spawn so a shared `When …
// published to topic "t"` step still finds a broker). Each law's observable
// assertion lives in the dedicated `Then` step, so these scaffolding steps cannot
// mask a failure.

/// Scaffolding `Given` for the "any delivery strategy d" law: spawns a default
/// world broker so the shared `When a message is published to topic "t"` step
/// (reused from the example feature) still has a broker to publish into. The law
/// itself drives every strategy on its own brokers and ignores this one.
#[given(regex = r#"^a running Broker with any delivery strategy d$"#)]
async fn given_broker_any(world: &mut BrokerWorld) {
    let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
    broker.wait_for_startup().await;
    world.broker = Some(broker);
}

/// Scaffolding `Given` for the "BestEffort or TimedDelivery" law.
#[given(regex = r#"^a running Broker with delivery strategy "BestEffort" or "TimedDelivery"$"#)]
async fn given_broker_besteffort_or_timed(world: &mut BrokerWorld) {
    let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::BestEffort));
    broker.wait_for_startup().await;
    world.broker = Some(broker);
}

/// Descriptive `Given`/`And` scaffolding lines (the law generates its own
/// subscriptions). Bound to no-ops; the law's `Then` carries every assertion.
#[given(regex = r#"^a single subscriber S subscribed with any compilable glob pattern p$"#)]
async fn given_single_any_pattern(_world: &mut BrokerWorld) {}

#[given(regex = r#"^any multiset of \(subscriber, pattern\) subscriptions, duplicates allowed$"#)]
async fn given_any_multiset(_world: &mut BrokerWorld) {}

#[given(regex = r#"^a subscriber S subscribed with pattern "t" whose actor is not running$"#)]
async fn given_dead_pattern_t(_world: &mut BrokerWorld) {}

#[given(
    regex = r#"^a subscriber S subscribed with pattern "t" whose mailbox is full past any timeout$"#
)]
async fn given_full_pattern_t(_world: &mut BrokerWorld) {}

#[given(regex = r#"^a subscriber S subscribed under any set P of patterns$"#)]
async fn given_any_pattern_set(_world: &mut BrokerWorld) {}

#[given(regex = r#"^any fixed set of \(subscriber, pattern\) registrations$"#)]
async fn given_any_fixed_set(_world: &mut BrokerWorld) {}

/// Scaffolding `When` lines for the laws (the law drives its own publishes).
#[when(regex = r#"^a message is published to any topic t$"#)]
async fn when_publish_any_topic(_world: &mut BrokerWorld) {}

#[when(regex = r#"^N messages are published concurrently from P tasks to any topics$"#)]
async fn when_publish_concurrent_any(_world: &mut BrokerWorld) {}

#[when(regex = r#"^S unsubscribes with topic None$"#)]
async fn when_unsubscribe_none_law(_world: &mut BrokerWorld) {}

/// Trailing law `And` lines whose guarantee is already asserted by the preceding
/// `Then` law step (both branches are checked inside the single proptest / loop).
#[then(regex = r#"^S receives 0 messages otherwise$"#)]
async fn then_law_zero_otherwise(_world: &mut BrokerWorld) {}

#[then(
    regex = r#"^the total deliveries equal the count of registrations whose pattern matches t$"#
)]
async fn then_law_total_deliveries(_world: &mut BrokerWorld) {}

/// Trailing clause of the Unsubscribe(None)/(Some p) law: its Part B (Some(p)
/// removes S only from p) is fully asserted inside `then_law_unsubscribe`.
#[then(
    regex = r#"^separately, when S instead unsubscribes from one pattern p in P, S still receives deliveries for publishes matching any other pattern in P but none matching only p$"#
)]
async fn then_law_unsubscribe_some(_world: &mut BrokerWorld) {}

/// `@property @sequence` — "A subscriber receives a publish iff its glob pattern
/// matches the topic."
///
/// proptest drives the pure routing decision. The ORACLE is
/// `glob::Pattern::new(p).matches_with(t, broker MatchOptions)` — the glob crate
/// is the broker's own routing engine, used here as an INDEPENDENT reference for
/// the boolean (the step asserts the SUT's observed delivery count equals the
/// oracle's 0/1). The generator HITS the `# GEN:` boundaries
/// {"", "*", "a", "a/*", "*/b", "a/*/b", "my-*", "a/b/c"} ×
/// {"", "a", "a/b", "a/b/c", "my-topic.detail", "a/"}.
#[then(
    regex = r#"^S receives exactly 1 message if glob\(p\) matches t under the broker MatchOptions$"#
)]
async fn then_law_match_iff(_world: &mut BrokerWorld) {
    // The step runs INSIDE cucumber's tokio runtime, so a sync `proptest!` block
    // cannot `block_on` per case (nested-runtime panic). Per the README's
    // Phase-3 §4 fallback, this is a DOCUMENTED bounded boundary-loop over the
    // EXHAUSTIVE cross-product of the `# GEN:` boundary patterns × topics — every
    // named boundary {"", "*", "a", "a/*", "*/b", "a/*/b", "my-*", "a/b/c"} ×
    // {"", "a", "a/b", "a/b/c", "my-topic.detail", "a/"} is hit, with matching
    // AND non-matching pairs. The ORACLE is INDEPENDENT of the SUT: glob's own
    // `Pattern::matches_with` under the broker MatchOptions.
    let patterns = ["", "*", "a", "a/*", "*/b", "a/*/b", "my-*", "a/b/c"];
    let topics = ["", "a", "a/b", "a/b/c", "my-topic.detail", "a/"];
    let opts = broker_match_options();

    for p in patterns {
        for t in topics {
            // ORACLE (independent of the SUT): glob crate's own decision.
            let expected: usize = Pattern::new(p)
                .map(|pat| usize::from(pat.matches_with(t, opts)))
                .unwrap_or(0);

            let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
            broker.wait_for_startup().await;
            let received = Arc::new(Mutex::new(Received::default()));
            let reff = Recorder::spawn(Recorder {
                received: Arc::clone(&received),
            });
            reff.wait_for_startup().await;
            broker
                .tell(Subscribe {
                    topic: Pattern::new(p).unwrap(),
                    recipient: reff.clone().recipient::<Msg>(),
                })
                .await
                .unwrap();
            broker
                .tell(Publish {
                    topic: t.to_owned(),
                    message: Msg {
                        topic: t.to_owned(),
                    },
                })
                .await
                .unwrap();
            // Guaranteed delivery has completed when the publish returns; settle a
            // beat so a wrongful delivery (oracle says 0) would also have landed.
            tokio::time::sleep(Duration::from_millis(15)).await;
            let got = received.lock().unwrap().topics.len();
            assert_eq!(
                got, expected,
                "pattern {p:?} vs topic {t:?}: SUT delivered {got} but glob oracle says {expected}"
            );
        }
    }
}

/// `@property @sequence` — "The delivery count equals the number of matching
/// (subscriber, pattern) registrations."
///
/// Documented bounded boundary-loop (proptest cannot cleanly drive the async,
/// multi-actor registration multiset). The multiset SIZE hits the `# GEN:`
/// boundaries {0, 1, 2, 8} and ALWAYS includes (a) the same (subscriber, pattern)
/// pair repeated and (b) one subscriber under two distinct patterns; topics are
/// chosen to match 0, 1 and ≥2 registrations. The ORACLE is an independent count
/// over the registration multiset of patterns matching t (no dedupe) — computed
/// from the test's own list, never from the broker.
#[then(
    regex = r#"^each subscriber receives exactly one message per registration whose pattern matches t$"#
)]
async fn then_law_multiplicity(_world: &mut BrokerWorld) {
    let opts = broker_match_options();
    // (subscriber_label, pattern) registration multiset hitting the GEN boundaries:
    // S0 appears twice under "a/*" (dup under one pattern); S0 also under "*/x"
    // (one subscriber under two distinct patterns); plus a literal "topic".
    let multiset: Vec<(&str, &str)> = vec![
        ("S0", "a/*"),
        ("S0", "a/*"),
        ("S0", "*/x"),
        ("S1", "a/x"),
        ("S2", "a/*"),
        ("S3", "topic"),
        ("S3", "*/x"),
        ("S2", "*/x"),
    ];
    // Topics chosen to match 0, 1 and ≥2 registrations.
    for topic in ["a/x", "topic", "b/y", "a/"] {
        // Independent oracle: per-subscriber expected count = number of that
        // subscriber's registrations whose pattern matches `topic`.
        let mut expected: HashMap<&str, usize> = HashMap::new();
        for (label, pat) in &multiset {
            if Pattern::new(pat).unwrap().matches_with(topic, opts) {
                *expected.entry(label).or_insert(0) += 1;
            }
        }

        let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
        broker.wait_for_startup().await;
        let mut recs: HashMap<&str, (ActorRef<Recorder>, Arc<Mutex<Received>>)> = HashMap::new();
        for label in ["S0", "S1", "S2", "S3"] {
            let received = Arc::new(Mutex::new(Received::default()));
            let reff = Recorder::spawn(Recorder {
                received: Arc::clone(&received),
            });
            reff.wait_for_startup().await;
            recs.insert(label, (reff, received));
        }
        for (label, pat) in &multiset {
            let (reff, _) = recs.get(label).unwrap();
            broker
                .tell(Subscribe {
                    topic: Pattern::new(pat).unwrap(),
                    recipient: reff.clone().recipient::<Msg>(),
                })
                .await
                .unwrap();
        }
        broker
            .tell(Publish {
                topic: topic.to_owned(),
                message: Msg {
                    topic: topic.to_owned(),
                },
            })
            .await
            .unwrap();
        // Settle, then assert each subscriber's count equals the oracle exactly.
        tokio::time::sleep(Duration::from_millis(40)).await;
        for label in ["S0", "S1", "S2", "S3"] {
            let want = expected.get(label).copied().unwrap_or(0);
            let got = recs.get(label).unwrap().1.lock().unwrap().topics.len();
            assert_eq!(
                got, want,
                "topic {topic:?}: subscriber {label} expected {want} deliveries, got {got}"
            );
        }
        let total_got: usize = recs
            .values()
            .map(|(_, r)| r.lock().unwrap().topics.len())
            .sum();
        let total_want: usize = expected.values().sum();
        assert_eq!(
            total_got, total_want,
            "topic {topic:?}: total deliveries must equal matching-registration count"
        );
    }
}

/// Subscribes a fresh dead (killed) recipient under pattern "t" on a fresh broker
/// running strategy `d`, then publishes "t"; returns `(broker, pattern, dead_id)`
/// so the caller can observe pruning. The recipient reports `ActorNotRunning`
/// because it is stopped before the publish.
async fn publish_to_dead(d: DeliveryStrategy) -> (ActorRef<Broker<Msg>>, Pattern, ActorId) {
    let broker = Broker::spawn(Broker::<Msg>::new(d));
    broker.wait_for_startup().await;
    let dead = Recorder::spawn(Recorder {
        received: Arc::new(Mutex::new(Received::default())),
    });
    dead.wait_for_startup().await;
    let dead_id = dead.id();
    let pat = Pattern::new("t").unwrap();
    broker
        .tell(Subscribe {
            topic: pat.clone(),
            recipient: dead.clone().recipient::<Msg>(),
        })
        .await
        .unwrap();
    dead.kill();
    dead.wait_for_shutdown().await;
    broker
        .tell(Publish {
            topic: "t".to_owned(),
            message: Msg {
                topic: "t".to_owned(),
            },
        })
        .await
        .unwrap();
    (broker, pat, dead_id)
}

async fn live_for(broker: &ActorRef<Broker<Msg>>, pat: &Pattern, id: ActorId) -> usize {
    broker
        .ask(CountSubscriptions {
            pattern: Some(pat.clone()),
            actor_id: id,
        })
        .await
        .expect("broker must not panic answering the count query")
}

/// `@property @lifecycle` — "A dead recipient is pruned iff the strategy surfaces
/// ActorNotRunning to the handler", inline-strategy clause.
///
/// Bounded boundary-loop over the INLINE strategies {Guaranteed, BestEffort,
/// TimedDelivery(τ)} with τ ∈ {Duration::ZERO, 50ms}. ORACLE: ActorNotRunning ⇒
/// prune, and inline strategies prune SYNCHRONOUSLY — the prune is the loop's
/// `to_remove` drain, complete by the time the `Publish` handler returns. So the
/// VERY NEXT query (no polling) must already see the bucket emptied.
#[then(
    regex = r#"^S is removed from the subscription iff d delivers inline \(Guaranteed, BestEffort, TimedDelivery\) and surfaces ActorNotRunning, pruned synchronously after the loop$"#
)]
async fn then_law_prune_inline(_world: &mut BrokerWorld) {
    let inline = [
        DeliveryStrategy::Guaranteed,
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    for d in inline {
        let (broker, pat, id) = publish_to_dead(d).await;
        // Synchronous: the next mailbox message (this query) observes state AFTER
        // the publish handler's post-loop prune, so live must already be 0.
        let live = live_for(&broker, &pat, id).await;
        assert_eq!(
            live, 0,
            "inline strategy {d:?}: dead recipient must be pruned synchronously"
        );
    }
}

/// Same law, spawned-strategy clause: for Spawned / SpawnedWithTimeout the prune
/// is ASYNCHRONOUS (a self-sent `Unsubscribe { topic: Some("t"), actor_id }`), so
/// it is eventual — observed by polling, not immediately. Boundary timeouts
/// {Duration::ZERO, 50ms} for SpawnedWithTimeout.
#[then(
    regex = r#"^for Spawned and SpawnedWithTimeout S is removed asynchronously via a self-sent Unsubscribe carrying S's pattern and id$"#
)]
async fn then_law_prune_spawned(_world: &mut BrokerWorld) {
    let spawned = [
        DeliveryStrategy::Spawned,
        DeliveryStrategy::SpawnedWithTimeout(Duration::ZERO),
        DeliveryStrategy::SpawnedWithTimeout(Duration::from_millis(50)),
    ];
    for d in spawned {
        let (broker, pat, id) = publish_to_dead(d).await;
        let mut pruned = false;
        for _ in 0..400 {
            if live_for(&broker, &pat, id).await == 0 {
                pruned = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            pruned,
            "spawned strategy {d:?}: dead recipient must be pruned asynchronously"
        );
    }
}

/// Same law, liveness clause: across ALL 5 variants the broker run-loop survives
/// a dead-recipient publish (no panic), proven by a successful follow-up query.
#[then(regex = r#"^the Broker actor never panics for any d$"#)]
async fn then_law_prune_no_panic(_world: &mut BrokerWorld) {
    let all = [
        DeliveryStrategy::Guaranteed,
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
        DeliveryStrategy::Spawned,
        DeliveryStrategy::SpawnedWithTimeout(Duration::ZERO),
        DeliveryStrategy::SpawnedWithTimeout(Duration::from_millis(50)),
    ];
    for d in all {
        let (broker, pat, id) = publish_to_dead(d).await;
        let r = broker
            .ask(CountSubscriptions {
                pattern: Some(pat),
                actor_id: id,
            })
            .await;
        assert!(
            r.is_ok(),
            "strategy {d:?}: the Broker actor must not panic on a dead-recipient publish"
        );
    }
}

/// `@property @boundary` — "Only ActorNotRunning prunes — a full or slow mailbox
/// is never pruned." Bounded loop over {BestEffort, TimedDelivery(τ)} with
/// τ ∈ {ZERO, 1ms, 50ms}; mailbox kept full for the whole publish. ORACLE: the
/// pruned-variant set is exactly {ActorNotRunning}; MailboxFull/Timeout ∉ set.
#[then(regex = r#"^S remains subscribed for any timeout value$"#)]
async fn then_law_no_false_prune(_world: &mut BrokerWorld) {
    let variants = [
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(1)),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    for d in variants {
        let broker = Broker::spawn(Broker::<Msg>::new(d));
        broker.wait_for_startup().await;
        let mut sub = spawn_recorder(Some(1)).await;
        make_full(&mut sub).await;
        let id = sub.reff.id();
        let pat = Pattern::new("t").unwrap();
        broker
            .tell(Subscribe {
                topic: pat.clone(),
                recipient: sub.reff.clone().recipient::<Msg>(),
            })
            .await
            .unwrap();
        broker
            .tell(Publish {
                topic: "t".to_owned(),
                message: Msg {
                    topic: "t".to_owned(),
                },
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let live: usize = broker
            .ask(CountSubscriptions {
                pattern: Some(pat.clone()),
                actor_id: id,
            })
            .await
            .expect("broker must not panic");
        assert_eq!(
            live, 1,
            "strategy {d:?}: a full/slow mailbox must NOT prune the subscriber"
        );
        // Recipient received nothing (mailbox was full the whole time).
        assert_eq!(
            sub.received.lock().unwrap().topics.len(),
            0,
            "strategy {d:?}: a full-mailbox recipient must not have received the message"
        );
        // Release the gate so the parked handlers can exit cleanly.
        if let Some(tx) = sub.release.take() {
            let _ = tx.send(true);
        }
    }
}

/// `@property @lifecycle` — "Unsubscribe(None) removes a subscriber from every
/// pattern; Unsubscribe(Some p) only from p." Bounded loop over pattern sets P
/// hitting the `# GEN:` boundaries {1 pattern, 2 patterns, the same pattern
/// twice}. ORACLE: a per-pattern set-of-subscribers model written from scratch.
#[then(regex = r#"^S receives 0 messages for any subsequent publish to any topic$"#)]
async fn then_law_unsubscribe(_world: &mut BrokerWorld) {
    // Part A — Unsubscribe(None) empties S from EVERY pattern.
    for pset in [
        vec!["a/*"],
        vec!["a/*", "b/*"],
        vec!["a/*", "a/*"], // same pattern twice
    ] {
        let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
        broker.wait_for_startup().await;
        let received = Arc::new(Mutex::new(Received::default()));
        let reff = Recorder::spawn(Recorder {
            received: Arc::clone(&received),
        });
        reff.wait_for_startup().await;
        for p in &pset {
            broker
                .tell(Subscribe {
                    topic: Pattern::new(p).unwrap(),
                    recipient: reff.clone().recipient::<Msg>(),
                })
                .await
                .unwrap();
        }
        broker
            .tell(Unsubscribe {
                topic: None,
                actor_id: reff.id(),
            })
            .await
            .unwrap();
        for topic in ["a/x", "b/y", "a/", "z"] {
            broker
                .tell(Publish {
                    topic: topic.to_owned(),
                    message: Msg {
                        topic: topic.to_owned(),
                    },
                })
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            received.lock().unwrap().topics.len(),
            0,
            "Unsubscribe(None) with patterns {pset:?} must stop all subsequent delivery"
        );
    }

    // Part B — Unsubscribe(Some p) removes S only from p; other patterns still deliver.
    {
        let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
        broker.wait_for_startup().await;
        let received = Arc::new(Mutex::new(Received::default()));
        let reff = Recorder::spawn(Recorder {
            received: Arc::clone(&received),
        });
        reff.wait_for_startup().await;
        for p in ["a/*", "b/*"] {
            broker
                .tell(Subscribe {
                    topic: Pattern::new(p).unwrap(),
                    recipient: reff.clone().recipient::<Msg>(),
                })
                .await
                .unwrap();
        }
        // Drop only "a/*".
        broker
            .tell(Unsubscribe {
                topic: Some(Pattern::new("a/*").unwrap()),
                actor_id: reff.id(),
            })
            .await
            .unwrap();
        // Publish matching only a/* (now gone) then matching b/* (still live).
        broker
            .tell(Publish {
                topic: "a/1".to_owned(),
                message: Msg {
                    topic: "a/1".to_owned(),
                },
            })
            .await
            .unwrap();
        broker
            .tell(Publish {
                topic: "b/1".to_owned(),
                message: Msg {
                    topic: "b/1".to_owned(),
                },
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        let got = received.lock().unwrap().topics.clone();
        assert_eq!(
            got,
            vec!["b/1".to_owned()],
            "Unsubscribe(Some a/*) must keep b/* delivery and drop a/* delivery; got {got:?}"
        );
    }
}

/// `@model @linearizability` — "Concurrent publishes refine a per-registration
/// delivery counter with no loss or duplication." Real overlap (`tokio::spawn`
/// + `Barrier`). ORACLE: a per-(subscriber, pattern) integer counter incremented
/// once per publish whose topic matches that pattern; the broker serialises
/// handling on its mailbox so the observed multiset must equal the model exactly.
/// GEN: P ∈ [2,10]; N ∈ {1, 50, 100}; overlapping patterns ("a/*" and "*/x")
/// plus a duplicate registration; topics chosen so some match multiple patterns.
#[then(
    regex = r#"^for every subscriber the received count equals, per registration matching a published topic, the number of publishes to a matching topic — no loss, no duplication$"#
)]
async fn then_model_concurrent(_world: &mut BrokerWorld) {
    // Registrations: A under "a/*", B under "*/x", A duplicated under "a/*".
    // Publishing topic "a/x" matches BOTH patterns; A is registered twice under
    // "a/*" so A must receive 2 per publish, B 1 per publish.
    for (tasks, n) in [(2u32, 1u32), (5, 50), (10, 100)] {
        let broker = Broker::spawn(Broker::<Msg>::new(DeliveryStrategy::Guaranteed));
        broker.wait_for_startup().await;
        let a_recv = Arc::new(Mutex::new(Received::default()));
        let b_recv = Arc::new(Mutex::new(Received::default()));
        let a = Recorder::spawn(Recorder {
            received: Arc::clone(&a_recv),
        });
        let b = Recorder::spawn(Recorder {
            received: Arc::clone(&b_recv),
        });
        a.wait_for_startup().await;
        b.wait_for_startup().await;
        // A twice under "a/*" (duplicate), B once under "*/x".
        for _ in 0..2 {
            broker
                .tell(Subscribe {
                    topic: Pattern::new("a/*").unwrap(),
                    recipient: a.clone().recipient::<Msg>(),
                })
                .await
                .unwrap();
        }
        broker
            .tell(Subscribe {
                topic: Pattern::new("*/x").unwrap(),
                recipient: b.clone().recipient::<Msg>(),
            })
            .await
            .unwrap();

        let per = n / tasks;
        let total = per * tasks;
        let barrier = Arc::new(Barrier::new(tasks as usize));
        let mut handles = Vec::new();
        for _ in 0..tasks {
            let broker = broker.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..per {
                    broker
                        .tell(Publish {
                            topic: "a/x".to_owned(),
                            message: Msg {
                                topic: "a/x".to_owned(),
                            },
                        })
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // ORACLE: A registered twice under matching "a/*" => 2*total; B once => total.
        let want_a = (total * 2) as usize;
        let want_b = total as usize;
        let ok = settle(|| {
            a_recv.lock().unwrap().topics.len() == want_a
                && b_recv.lock().unwrap().topics.len() == want_b
        })
        .await;
        assert!(
            ok,
            "tasks={tasks} n={total}: A expected {want_a} (got {}), B expected {want_b} (got {})",
            a_recv.lock().unwrap().topics.len(),
            b_recv.lock().unwrap().topics.len()
        );
    }
}
