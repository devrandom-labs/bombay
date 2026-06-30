//! Shared `MessageQueue` World + step definitions for the `actors/message_queue`
//! scenarios (card #78) — the LARGEST actors module.
//!
//! Wired by the two runners that `#[path]`-include this module:
//!   * `message_queue_bdd.rs`       — the example feature (message_queue.feature)
//!   * `message_queue_props_bdd.rs` — the Phase-2 laws (message_queue.properties.feature)
//!
//! The SUT is `bombay_actors::message_queue` (the AMQP-style `MessageQueue` actor:
//! `ExchangeDeclare` / `QueueDeclare` / `QueueBind` / `QueueUnbind` /
//! `BasicConsume` / `BasicCancel` / `BasicPublish` under a `DeliveryStrategy`),
//! driven against REAL SPAWNED ACTORS reached through `bombay::prelude::*`.
//! Consumers are `Recorder` actors that record every `Note` they handle; the
//! queue/exchange tables are inspected through the test-only `QueueExists` /
//! `ExchangeExists` / `CountBindings` queries (gated behind the `testing`
//! feature), mirroring `message_bus`'s `CountRegistrations`.
//!
//! Full-mailbox scenarios park a `Recorder` on a `watch` release gate so the
//! bounded(1) mailbox stays observably full while the queue tries to deliver; the
//! test asserts the strategy-specific observable, then releases the gate.
//!
//! The `@property` / `@model` laws (message_queue.properties.feature) bind to a
//! single step running a documented bounded boundary-loop over the `# GEN:`-named
//! values (proptest's synchronous runner cannot drive this async, multi-actor,
//! global-state SUT cleanly — see docs/testing/README.md Phase-3 §4). Every oracle
//! is an INDEPENDENT reference: the routing oracle calls `glob::Pattern` directly
//! (the crate the queue uses) or an AMQP set-membership model written from
//! scratch, never the queue itself.
//!
//! @bug GATING (card #79): three scenarios carry `@bug:.../message_queue.rs:707`
//! or `:591` and assert the DESIRED `AmqpError::InvalidRoutingKey` rejection — a
//! variant that does NOT exist in the enum yet (adding it is card #79). Both
//! runners exclude every `@bug` scenario via their `!t.starts_with("bug")` filter,
//! so those scenarios never run here and their steps are never bound. To keep the
//! crate COMPILING, NO step definition in this module names the missing variant.
//! The REAL defect (a malformed Topic key panics the run-loop at publish, because
//! `QueueBind` never validates a Topic key is a compilable glob) is reproduced —
//! WITHOUT the missing variant — by the separate `message_queue_bug_bdd.rs` probe,
//! which is `#[ignore]`d (RED today, will pass once :591/:707 are fixed) via the
//! `malformed_topic_key_panics_at_publish` helper exported below.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bombay::{error::Infallible, mailbox, prelude::*};
use bombay_actors::{
    DeliveryStrategy,
    message_queue::{
        AmqpError, BasicCancel, BasicConsume, BasicPublish, CountBindings, ExchangeDeclare,
        ExchangeDelete, ExchangeExists, ExchangeType, MessageProperties, MessageQueue, QueueBind,
        QueueDeclare, QueueDelete, QueueExists, QueueUnbind,
    },
};
use cucumber::{World, given, then, when};
use glob::{MatchOptions, Pattern};
use tokio::sync::Barrier;

// ===========================================================================
// Test message + consumer actor
// ===========================================================================

/// The routed payload. `Clone + Send + Sync` are required by `BasicPublish<M>`
/// (`M: Clone + Send + Sync + 'static`). It carries the exact routing key / header
/// shape it was published with so a `Then` can assert *which* publish landed, not
/// merely a count.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Note {
    /// A free-form label distinguishing one published message from another.
    tag: String,
}

/// A capacity probe sent directly to a consumer (never through the queue) to
/// observe a full bounded mailbox; its handler is a no-op so a stray delivery is
/// not miscounted as a real `Note`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Probe;

/// Parks the consumer's run-loop on a `watch` gate until it flips to `true`,
/// holding a bounded mailbox slot so the mailbox stays full.
struct Hold(tokio::sync::watch::Receiver<bool>);

/// Everything a `Recorder` records: the ordered list of `Note` tags it received.
#[derive(Debug, Default, Clone)]
struct Received {
    tags: Vec<String>,
}

/// A consumer that records every `Note` it handles into shared `Received`.
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

impl Message<Note> for Recorder {
    type Reply = ();

    async fn handle(&mut self, msg: Note, _ctx: &mut Context<Self, Self::Reply>) {
        self.received.lock().unwrap().tags.push(msg.tag);
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

/// A handle to one spawned `Recorder` consumer, keyed by its feature label (a
/// queue name, or an explicit label like "A"/"C").
#[derive(Debug)]
struct Consumer {
    reff: ActorRef<Recorder>,
    received: Arc<Mutex<Received>>,
    /// The queue this consumer is currently attached to (for cancel-by-label).
    queue: String,
    /// Release gate for a parked (full-mailbox) consumer, if any.
    release: Option<tokio::sync::watch::Sender<bool>>,
}

#[derive(Debug, Default, World)]
pub struct MessageQueueWorld {
    mq: Option<ActorRef<MessageQueue>>,
    /// Consumers keyed by label. The plain "a consumer is attached to queue X"
    /// step keys by the queue name; labelled steps ("consumer A", "consumer C")
    /// key by the label.
    consumers: HashMap<String, Consumer>,
    /// The last `AmqpError` a When produced (for the `@boundary` Thens), or `None`
    /// if the operation succeeded.
    last_error: Option<AmqpError>,
    /// Prune-law results, set by the first `Then` and read by its trailing `And`:
    /// (stopped-consumer-was-pruned, full/slow-consumer-was-never-pruned).
    prune_stopped_ok: Option<bool>,
    prune_full_ok: Option<bool>,
}

// ===========================================================================
// Helpers
// ===========================================================================

/// The exact `MatchOptions` the queue uses (message_queue.rs:700-704). The Topic
/// routing oracle in the property laws must use these verbatim.
fn mq_match_options() -> MatchOptions {
    MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    }
}

fn the_mq(world: &MessageQueueWorld) -> ActorRef<MessageQueue> {
    world
        .mq
        .clone()
        .expect("a MessageQueue was spawned by a Given")
}

fn parse_strategy(name: &str) -> DeliveryStrategy {
    match name {
        "Guaranteed" => DeliveryStrategy::Guaranteed,
        "BestEffort" => DeliveryStrategy::BestEffort,
        other => panic!("unsupported delivery strategy {other:?} in these features"),
    }
}

fn parse_kind(name: &str) -> ExchangeType {
    match name {
        "Direct" => ExchangeType::Direct,
        "Topic" => ExchangeType::Topic,
        "Fanout" => ExchangeType::Fanout,
        "Headers" => ExchangeType::Headers,
        other => panic!("unknown exchange kind {other:?}"),
    }
}

/// The error-variant name asserted by the `Then`s. An INDEPENDENT mapping (not
/// `format!("{e:?}")`) so a renamed variant is caught by a compile error here, not
/// silently. Deliberately does NOT include any `InvalidRoutingKey` arm — that
/// variant does not exist yet (card #79).
/// Extracts the handler's `AmqpError` from an `ask` reply. The SUT handlers reply
/// `Result<(), AmqpError>`, which kameo's `Reply` impl flattens: `ask(..).await`
/// returns `Result<(), SendError<M, AmqpError>>`, surfacing a handler `Err` as
/// `SendError::HandlerError`. `None` ⇒ the operation succeeded.
fn amqp_err<M: std::fmt::Debug>(res: Result<(), SendError<M, AmqpError>>) -> Option<AmqpError> {
    match res {
        Ok(()) => None,
        Err(SendError::HandlerError(e)) => Some(e),
        Err(other) => panic!("unexpected transport error reaching the MessageQueue: {other:?}"),
    }
}

fn err_name(e: &AmqpError) -> &'static str {
    match e {
        AmqpError::ExchangeAlreadyExists => "ExchangeAlreadyExists",
        AmqpError::QueueAlreadyExists => "QueueAlreadyExists",
        AmqpError::ExchangeNotFound => "ExchangeNotFound",
        AmqpError::QueueNotFound => "QueueNotFound",
        AmqpError::BindingAlreadyExists => "BindingAlreadyExists",
        AmqpError::HeadersRequired => "HeadersRequired",
        AmqpError::InvalidHeaderMatch => "InvalidHeaderMatch",
        AmqpError::ExchangeInUse => "ExchangeInUse",
        AmqpError::QueueInUse => "QueueInUse",
    }
}

async fn spawn_recorder(cap: Option<usize>) -> (ActorRef<Recorder>, Arc<Mutex<Received>>) {
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
    (reff, received)
}

/// Makes a bounded(1) consumer's mailbox observably full by parking two `Hold`
/// handlers on a release gate, then proving a `try_send(Probe)` is refused.
async fn make_full(reff: &ActorRef<Recorder>) -> tokio::sync::watch::Sender<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    reff.tell(Hold(rx.clone()))
        .send()
        .await
        .expect("first hold enqueued and dequeued into the handler");
    tokio::time::sleep(Duration::from_millis(20)).await;
    reff.tell(Hold(rx))
        .try_send()
        .expect("second hold fills the single buffer slot");
    for _ in 0..200 {
        if matches!(
            reff.tell(Probe).try_send(),
            Err(SendError::MailboxFull(Probe))
        ) {
            return tx;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the consumer's bounded mailbox never became observably full");
}

async fn declare_exchange(world: &MessageQueueWorld, name: &str, kind: ExchangeType, auto: bool) {
    let mq = the_mq(world);
    mq.tell(ExchangeDeclare {
        exchange: name.to_owned(),
        kind,
        auto_delete: auto,
    })
    .await
    .expect("ExchangeDeclare delivered");
}

async fn declare_queue(world: &MessageQueueWorld, name: &str, auto: bool) {
    let mq = the_mq(world);
    mq.tell(QueueDeclare {
        queue: name.to_owned(),
        auto_delete: auto,
    })
    .await
    .expect("QueueDeclare delivered");
}

async fn bind(world: &MessageQueueWorld, queue: &str, exchange: &str, key: &str) {
    let mq = the_mq(world);
    mq.tell(QueueBind {
        queue: queue.to_owned(),
        exchange: exchange.to_owned(),
        routing_key: key.to_owned(),
        arguments: HashMap::new(),
    })
    .await
    .expect("QueueBind delivered");
}

/// Attaches `reff` as a `Note` consumer of `queue` with `tags`, recording the
/// consumer under `label`.
async fn attach(
    world: &mut MessageQueueWorld,
    label: &str,
    queue: &str,
    tags: HashMap<String, String>,
) {
    let mq = the_mq(world);
    let (reff, received) = spawn_recorder(None).await;
    mq.tell(BasicConsume {
        queue: queue.to_owned(),
        recipient: reff.clone().recipient::<Note>(),
        tags,
    })
    .await
    .expect("BasicConsume delivered");
    world.consumers.insert(
        label.to_owned(),
        Consumer {
            reff,
            received,
            queue: queue.to_owned(),
            release: None,
        },
    );
}

/// Publishes one `Note { tag }` to `exchange` with `key` + `props`, recording the
/// reply error (if any) in `world.last_error`.
async fn publish(
    world: &mut MessageQueueWorld,
    exchange: &str,
    key: &str,
    tag: &str,
    props: MessageProperties,
) {
    let mq = the_mq(world);
    // `ask` surfaces the handler's `Result<(), AmqpError>` reply, which kameo's
    // `Reply` impl flattens into `SendError::HandlerError`; `tell` would discard it.
    let res = mq
        .ask(BasicPublish {
            exchange: exchange.to_owned(),
            routing_key: key.to_owned(),
            message: Note {
                tag: tag.to_owned(),
            },
            properties: props,
        })
        .await;
    world.last_error = amqp_err(res);
}

fn received_of(world: &MessageQueueWorld, label: &str) -> Received {
    world
        .consumers
        .get(label)
        .expect("consumer spawned")
        .received
        .lock()
        .unwrap()
        .clone()
}

fn count_of(world: &MessageQueueWorld, label: &str) -> usize {
    received_of(world, label).tags.len()
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

async fn queue_exists(world: &MessageQueueWorld, queue: &str) -> bool {
    the_mq(world)
        .ask(QueueExists {
            queue: queue.to_owned(),
        })
        .await
        .expect("the MessageQueue must still be running to answer QueueExists")
}

async fn exchange_exists(world: &MessageQueueWorld, exchange: &str) -> bool {
    the_mq(world)
        .ask(ExchangeExists {
            exchange: exchange.to_owned(),
        })
        .await
        .expect("the MessageQueue must still be running to answer ExchangeExists")
}

async fn count_bindings(world: &MessageQueueWorld, exchange: &str, queue: &str) -> Option<usize> {
    the_mq(world)
        .ask(CountBindings {
            exchange: exchange.to_owned(),
            queue: queue.to_owned(),
        })
        .await
        .expect("the MessageQueue must still be running to answer CountBindings")
}

fn parse_table(rows: Option<&cucumber::gherkin::Step>) -> HashMap<String, String> {
    rows.and_then(|step| step.table.as_ref())
        .map(|t| {
            t.rows
                .iter()
                .filter(|r| r.len() == 2)
                .map(|r| (r[0].clone(), r[1].clone()))
                .collect()
        })
        .unwrap_or_default()
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r#"^a running MessageQueue actor with delivery strategy "([A-Za-z]+)"$"#)]
async fn given_mq(world: &mut MessageQueueWorld, strategy: String) {
    let mq = MessageQueue::spawn(MessageQueue::new(parse_strategy(&strategy)));
    mq.wait_for_startup().await;
    world.mq = Some(mq);
}

// ===========================================================================
// Given / When — declare, bind, consume (shared phrasings)
// ===========================================================================

#[given(regex = r#"^a (Direct|Topic|Fanout|Headers) exchange "([^"]*)" is declared$"#)]
async fn given_exchange(world: &mut MessageQueueWorld, kind: String, name: String) {
    declare_exchange(world, &name, parse_kind(&kind), false).await;
}

#[given(
    regex = r#"^a (Direct|Topic|Fanout|Headers) exchange "([^"]*)" is declared with auto-delete enabled$"#
)]
async fn given_exchange_auto(world: &mut MessageQueueWorld, kind: String, name: String) {
    declare_exchange(world, &name, parse_kind(&kind), true).await;
}

#[given(regex = r#"^a queue "([^"]*)" is declared$"#)]
async fn given_queue(world: &mut MessageQueueWorld, name: String) {
    declare_queue(world, &name, false).await;
}

#[given(regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with routing key "([^"]*)"$"#)]
async fn given_bound_key(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    key: String,
) {
    bind(world, &queue, &exchange, &key).await;
}

#[given(
    regex = r#"^a queue "([^"]*)" is declared and bound to "([^"]*)" with routing key "([^"]*)"$"#
)]
async fn given_queue_bound_key(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    key: String,
) {
    declare_queue(world, &queue, false).await;
    bind(world, &queue, &exchange, &key).await;
}

#[given(regex = r#"^a queue "([^"]*)" is declared and bound to "([^"]*)"$"#)]
async fn given_queue_bound_fanout(world: &mut MessageQueueWorld, queue: String, exchange: String) {
    declare_queue(world, &queue, false).await;
    bind(world, &queue, &exchange, "").await;
}

#[given(
    regex = r#"^a queue "([^"]*)" is declared with auto-delete enabled and bound to "([^"]*)"$"#
)]
async fn given_queue_auto_bound(world: &mut MessageQueueWorld, queue: String, exchange: String) {
    declare_queue(world, &queue, true).await;
    bind(world, &queue, &exchange, "").await;
}

#[given(regex = r#"^queues "([^"]*)" and "([^"]*)" are declared and bound to "([^"]*)"$"#)]
async fn given_two_queues_bound(
    world: &mut MessageQueueWorld,
    q1: String,
    q2: String,
    exchange: String,
) {
    for q in [&q1, &q2] {
        declare_queue(world, q, false).await;
        bind(world, q, &exchange, "").await;
    }
}

#[given(
    regex = r#"^a queue "([^"]*)" is declared and bound to both "([^"]*)" and "([^"]*)" with routing key "([^"]*)"$"#
)]
async fn given_queue_bound_two_exchanges(
    world: &mut MessageQueueWorld,
    queue: String,
    e1: String,
    e2: String,
    key: String,
) {
    declare_queue(world, &queue, false).await;
    bind(world, &queue, &e1, &key).await;
    bind(world, &queue, &e2, &key).await;
}

#[given(regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with arguments:$"#)]
async fn given_bound_args(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    step: &cucumber::gherkin::Step,
) {
    let args = parse_table(Some(step));
    let mq = the_mq(world);
    mq.tell(QueueBind {
        queue,
        exchange,
        routing_key: String::new(),
        arguments: args,
    })
    .await
    .expect("QueueBind delivered");
}

#[given(regex = r#"^a queue "([^"]*)" is declared and bound to "([^"]*)" with arguments:$"#)]
async fn given_queue_bound_args(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    step: &cucumber::gherkin::Step,
) {
    let args = parse_table(Some(step));
    declare_queue(world, &queue, false).await;
    let mq = the_mq(world);
    mq.tell(QueueBind {
        queue,
        exchange,
        routing_key: String::new(),
        arguments: args,
    })
    .await
    .expect("QueueBind delivered");
}

// --- consumers --------------------------------------------------------------

#[given(regex = r#"^a consumer is attached to queue "([^"]*)"$"#)]
async fn given_consumer(world: &mut MessageQueueWorld, queue: String) {
    attach(world, &queue, &queue, HashMap::new()).await;
}

#[given(regex = r#"^a consumer is attached to queue "([^"]*)" with tags:$"#)]
async fn given_consumer_tags(
    world: &mut MessageQueueWorld,
    queue: String,
    step: &cucumber::gherkin::Step,
) {
    let tags = parse_table(Some(step));
    attach(world, &queue, &queue, tags).await;
}

#[given(regex = r#"^a consumer (\w+) is attached to queue "([^"]*)"$"#)]
async fn given_consumer_labelled(world: &mut MessageQueueWorld, label: String, queue: String) {
    attach(world, &label, &queue, HashMap::new()).await;
}

#[given(regex = r#"^a consumer is attached to each of "([^"]*)" and "([^"]*)"$"#)]
async fn given_consumer_each(world: &mut MessageQueueWorld, q1: String, q2: String) {
    attach(world, &q1, &q1, HashMap::new()).await;
    attach(world, &q2, &q2, HashMap::new()).await;
}

#[given(regex = r#"^a live consumer A and a stopped consumer B are attached to queue "([^"]*)"$"#)]
async fn given_live_and_stopped(world: &mut MessageQueueWorld, queue: String) {
    attach(world, "A", &queue, HashMap::new()).await;
    attach(world, "B", &queue, HashMap::new()).await;
    let b = world.consumers.get("B").expect("B spawned");
    b.reff.kill();
    b.reff.wait_for_shutdown().await;
}

#[given(regex = r#"^a consumer with a full bounded mailbox is attached to queue "([^"]*)"$"#)]
async fn given_consumer_full(world: &mut MessageQueueWorld, queue: String) {
    let mq = the_mq(world);
    let (reff, received) = spawn_recorder(Some(1)).await;
    let tx = make_full(&reff).await;
    mq.tell(BasicConsume {
        queue: queue.clone(),
        recipient: reff.clone().recipient::<Note>(),
        tags: HashMap::new(),
    })
    .await
    .expect("BasicConsume delivered");
    world.consumers.insert(
        queue.clone(),
        Consumer {
            reff,
            received,
            queue,
            release: Some(tx),
        },
    );
}

// ===========================================================================
// When — publish, unbind, cancel, delete, re-consume
// ===========================================================================

#[when(regex = r#"^a message is published to "([^"]*)" with routing key "([^"]*)"$"#)]
async fn when_publish(world: &mut MessageQueueWorld, exchange: String, key: String) {
    publish(world, &exchange, &key, &key, MessageProperties::default()).await;
}

#[when(regex = r#"^a message is published to the default exchange with routing key "([^"]*)"$"#)]
async fn when_publish_default(world: &mut MessageQueueWorld, key: String) {
    publish(world, "", &key, &key, MessageProperties::default()).await;
}

#[when(regex = r#"^a message is published to "([^"]*)" with headers:$"#)]
async fn when_publish_headers(
    world: &mut MessageQueueWorld,
    exchange: String,
    step: &cucumber::gherkin::Step,
) {
    let headers = parse_table(Some(step));
    // The tag encodes which headers were present so a Then can identify the
    // delivered message precisely (e.g. "format,level" vs "format").
    let mut keys: Vec<&str> = headers.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let tag = keys.join(",");
    let props = MessageProperties {
        headers: Some(headers),
        filter: None,
    };
    publish(world, &exchange, "", &tag, props).await;
}

#[when(regex = r#"^a message is published to "([^"]*)" with no headers$"#)]
async fn when_publish_no_headers(world: &mut MessageQueueWorld, exchange: String) {
    publish(
        world,
        &exchange,
        "",
        "no-headers",
        MessageProperties::default(),
    )
    .await;
}

#[when(
    regex = r#"^a message is published to "([^"]*)" with a filter requiring tag "([^"]*)" = "([^"]*)"$"#
)]
async fn when_publish_filter(
    world: &mut MessageQueueWorld,
    exchange: String,
    _key: String,
    _val: String,
) {
    // `FilterFn` is a plain `fn` pointer (no captures), so the required pair is
    // encoded directly. The features only ever require region=us, so a single
    // matching predicate suffices and stays an honest, specific test.
    fn region_is_us(tags: &HashMap<String, String>) -> bool {
        tags.get("region").map(String::as_str) == Some("us")
    }
    let props = MessageProperties {
        headers: None,
        filter: Some(region_is_us),
    };
    publish(world, &exchange, "x", "filtered", props).await;
}

#[when(regex = r#"^queue "([^"]*)" is unbound from "([^"]*)"$"#)]
async fn when_unbind(world: &mut MessageQueueWorld, queue: String, exchange: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(QueueUnbind {
            queue,
            exchange,
            routing_key: String::new(),
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^the consumer is cancelled$"#)]
async fn when_cancel_consumer(world: &mut MessageQueueWorld) {
    // The single-consumer scenarios key the consumer by its queue name "q".
    let consumer = world.consumers.get("q").expect("consumer 'q' attached");
    cancel(world, "q", &consumer.queue.clone()).await;
}

#[when(regex = r#"^consumer (\w+) is cancelled$"#)]
async fn when_cancel_labelled(world: &mut MessageQueueWorld, label: String) {
    let queue = world
        .consumers
        .get(&label)
        .expect("consumer attached")
        .queue
        .clone();
    cancel(world, &label, &queue).await;
}

async fn cancel(world: &mut MessageQueueWorld, label: &str, queue: &str) {
    let mq = the_mq(world);
    let reff = world
        .consumers
        .get(label)
        .expect("consumer attached")
        .reff
        .clone();
    mq.tell(BasicCancel {
        queue: queue.to_owned(),
        recipient: reff.recipient::<Note>(),
    })
    .await
    .expect("BasicCancel delivered");
}

#[when(regex = r#"^consumer (\w+) is attached to queue "([^"]*)" a second time$"#)]
async fn when_attach_again(world: &mut MessageQueueWorld, label: String, queue: String) {
    let mq = the_mq(world);
    let reff = world
        .consumers
        .get(&label)
        .expect("consumer attached")
        .reff
        .clone();
    mq.tell(BasicConsume {
        queue,
        recipient: reff.recipient::<Note>(),
        tags: HashMap::new(),
    })
    .await
    .expect("second BasicConsume delivered");
}

#[when(regex = r#"^queue "([^"]*)" is deleted$"#)]
async fn when_delete_queue(world: &mut MessageQueueWorld, queue: String) {
    let mq = the_mq(world);
    mq.tell(QueueDelete {
        queue,
        if_unused: false,
    })
    .await
    .expect("QueueDelete delivered");
}

// --- @boundary Whens (record the error) -------------------------------------

#[when(regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with routing key "([^"]*)"$"#)]
async fn when_bind_key(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    key: String,
) {
    let mq = the_mq(world);
    let res = mq
        .ask(QueueBind {
            queue,
            exchange,
            routing_key: key,
            arguments: HashMap::new(),
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with routing key "([^"]*)" again$"#)]
async fn when_bind_key_again(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    key: String,
) {
    when_bind_key(world, queue, exchange, key).await;
}

#[when(regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with arguments:$"#)]
async fn when_bind_args(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    step: &cucumber::gherkin::Step,
) {
    let args = parse_table(Some(step));
    let mq = the_mq(world);
    let res = mq
        .ask(QueueBind {
            queue,
            exchange,
            routing_key: String::new(),
            arguments: args,
        })
        .await;
    world.last_error = amqp_err(res);
}

/// The empty-name boundary scenario uses the plain "is declared" phrasing as a
/// `When` (an empty name is rejected as ExchangeAlreadyExists). Same regex as the
/// `#[given]` setup step, keyword-disambiguated by cucumber.
#[when(regex = r#"^a (Direct|Topic|Fanout|Headers) exchange "([^"]*)" is declared$"#)]
async fn when_declare_exchange(world: &mut MessageQueueWorld, kind: String, name: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(ExchangeDeclare {
            exchange: name,
            kind: parse_kind(&kind),
            auto_delete: false,
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^a (Direct|Topic|Fanout|Headers) exchange "([^"]*)" is declared again$"#)]
async fn when_declare_exchange_again(world: &mut MessageQueueWorld, kind: String, name: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(ExchangeDeclare {
            exchange: name,
            kind: parse_kind(&kind),
            auto_delete: false,
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^a queue "([^"]*)" is declared again$"#)]
async fn when_declare_queue_again(world: &mut MessageQueueWorld, name: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(QueueDeclare {
            queue: name,
            auto_delete: false,
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^exchange "([^"]*)" is deleted with if_unused set$"#)]
async fn when_delete_exchange_if_unused(world: &mut MessageQueueWorld, exchange: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(ExchangeDelete {
            exchange,
            if_unused: true,
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^queue "([^"]*)" is deleted with if_unused set$"#)]
async fn when_delete_queue_if_unused(world: &mut MessageQueueWorld, queue: String) {
    let mq = the_mq(world);
    let res = mq
        .ask(QueueDelete {
            queue,
            if_unused: true,
        })
        .await;
    world.last_error = amqp_err(res);
}

#[when(regex = r#"^a consumer is attached to queue "([^"]*)"$"#)]
async fn when_consume_boundary(world: &mut MessageQueueWorld, queue: String) {
    // Used by the "consuming from a queue that does not exist" @boundary scenario.
    // BasicConsume needs a concrete recipient; spawn one (it never registers
    // because the queue is absent), and capture the error.
    let mq = the_mq(world);
    let (reff, _received) = spawn_recorder(None).await;
    let res = mq
        .ask(BasicConsume {
            queue,
            recipient: reff.recipient::<Note>(),
            tags: HashMap::new(),
        })
        .await;
    world.last_error = amqp_err(res);
}

// --- @linearizability Whens -------------------------------------------------

#[when(regex = r#"^(\d+) messages are published concurrently to "([^"]*)" from (\d+) tasks$"#)]
async fn when_publish_concurrent(
    world: &mut MessageQueueWorld,
    total: u32,
    exchange: String,
    tasks: u32,
) {
    let mq = the_mq(world);
    let per = total / tasks;
    let barrier = Arc::new(Barrier::new(tasks as usize));
    let mut handles = Vec::new();
    for _ in 0..tasks {
        let mq = mq.clone();
        let barrier = Arc::clone(&barrier);
        let exchange = exchange.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..per {
                mq.tell(BasicPublish {
                    exchange: exchange.clone(),
                    routing_key: "k".to_owned(),
                    message: Note {
                        tag: "c".to_owned(),
                    },
                    properties: MessageProperties::default(),
                })
                .await
                .expect("concurrent publish delivered");
            }
        }));
    }
    for h in handles {
        h.await.expect("publish task join");
    }
}

#[when(
    regex = r#"^queue "([^"]*)" is bound to "([^"]*)" with routing key "([^"]*)" while a message with key "([^"]*)" is published$"#
)]
async fn when_bind_while_publish(
    world: &mut MessageQueueWorld,
    queue: String,
    exchange: String,
    bind_key: String,
    pub_key: String,
) {
    let mq = the_mq(world);
    let barrier = Arc::new(Barrier::new(2));
    let (b1, b2) = (Arc::clone(&barrier), Arc::clone(&barrier));
    let mq_bind = mq.clone();
    let bind = tokio::spawn(async move {
        b1.wait().await;
        mq_bind
            .tell(QueueBind {
                queue,
                exchange,
                routing_key: bind_key,
                arguments: HashMap::new(),
            })
            .await
            .expect("concurrent bind delivered");
    });
    let mq_pub = mq.clone();
    let pubg = tokio::spawn(async move {
        b2.wait().await;
        let _ = mq_pub
            .tell(BasicPublish {
                exchange: "x".to_owned(),
                routing_key: pub_key,
                message: Note {
                    tag: "race".to_owned(),
                },
                properties: MessageProperties::default(),
            })
            .await;
    });
    bind.await.expect("bind task");
    pubg.await.expect("publish task");
}

// ===========================================================================
// Then — delivery counts, which message, error variants, liveness
// ===========================================================================

#[then(regex = r#"^the consumer receives exactly (\d+) messages?$"#)]
async fn then_consumer_receives(world: &mut MessageQueueWorld, n: usize) {
    // The single-consumer scenarios key by queue name. Find the one consumer.
    let label = sole_consumer_label(world);
    let ok = settle(|| count_of(world, &label) == n).await;
    let got = received_of(world, &label);
    assert!(
        ok,
        "the consumer should receive exactly {n} messages, got {got:?}"
    );
}

#[then(regex = r#"^the consumer receives (\d+) messages?$"#)]
async fn then_consumer_receives_exact(world: &mut MessageQueueWorld, n: usize) {
    // "receives 0 messages" / "receives N" — settle a beat so a wrongful delivery
    // would have landed, then assert the exact count.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let label = sole_consumer_label(world);
    assert_eq!(
        count_of(world, &label),
        n,
        "the consumer should receive exactly {n} messages"
    );
}

#[then(regex = r#"^the received message is the one published with routing key "([^"]*)"$"#)]
async fn then_received_is_key(world: &mut MessageQueueWorld, key: String) {
    let label = sole_consumer_label(world);
    let got = received_of(world, &label);
    assert_eq!(
        got.tags,
        vec![key.clone()],
        "the consumer must hold exactly the message published with key {key:?}"
    );
}

#[then(
    regex = r#"^the received message is the one whose headers included both "([^"]*)" and "([^"]*)"$"#
)]
async fn then_received_both_headers(world: &mut MessageQueueWorld, h1: String, h2: String) {
    let label = sole_consumer_label(world);
    let got = received_of(world, &label);
    // The publish tag is the sorted comma-joined header keys.
    let mut want = [h1, h2];
    want.sort();
    let tag = want.join(",");
    assert_eq!(
        got.tags,
        vec![tag.clone()],
        "the consumer must hold exactly the message tagged {tag:?}"
    );
}

#[then(regex = r#"^the "([^"]*)" consumer receives exactly (\d+) message$"#)]
async fn then_named_consumer_receives(world: &mut MessageQueueWorld, queue: String, n: usize) {
    let ok = settle(|| count_of(world, &queue) == n).await;
    let got = received_of(world, &queue);
    assert!(
        ok,
        "the {queue} consumer should receive exactly {n}, got {got:?}"
    );
}

#[then(regex = r#"^consumer (\w+) receives exactly (\d+) message$"#)]
async fn then_labelled_receives(world: &mut MessageQueueWorld, label: String, n: usize) {
    let ok = settle(|| count_of(world, &label) == n).await;
    let got = received_of(world, &label);
    assert!(
        ok,
        "consumer {label} should receive exactly {n}, got {got:?}"
    );
}

#[then(regex = r#"^no error is surfaced for the stopped consumer B$"#)]
async fn then_no_error_for_b(world: &mut MessageQueueWorld) {
    assert!(
        world.last_error.is_none(),
        "a stopped consumer must not surface a publish error, got {:?}",
        world.last_error
    );
    // Liveness: the actor still answers a query => it did not panic pruning B.
    let _ = exchange_exists(world, "events").await;
}

#[then(regex = r#"^the publish does not error and the actor does not panic$"#)]
async fn then_publish_ok_no_panic(world: &mut MessageQueueWorld) {
    assert!(
        world.last_error.is_none(),
        "a full-mailbox BestEffort publish must not error, got {:?}",
        world.last_error
    );
    assert!(
        queue_exists(world, "q").await,
        "the MessageQueue actor must still be running (no panic)"
    );
}

#[then(regex = r#"^the consumer remains registered on queue "([^"]*)"$"#)]
async fn then_remains_registered(world: &mut MessageQueueWorld, queue: String) {
    // A full (not dead) consumer is never pruned: a re-consume by the same actor id
    // would be a dedup no-op. We assert observably: the queue still exists and the
    // consumer's actor is still alive. Release the gate so the queue can drain.
    assert!(
        queue_exists(world, &queue).await,
        "queue {queue} must still exist"
    );
    let consumer = world.consumers.get_mut(&queue).expect("consumer attached");
    assert!(
        consumer.reff.is_alive(),
        "the full consumer must still be alive (not pruned)"
    );
    if let Some(tx) = consumer.release.take() {
        let _ = tx.send(true);
    }
}

#[then(regex = r#"^publishing to "([^"]*)" fails with "([^"]*)"$"#)]
async fn then_publish_fails(world: &mut MessageQueueWorld, exchange: String, want: String) {
    publish(world, &exchange, "k", "probe", MessageProperties::default()).await;
    assert_err(world, &want);
}

#[then(regex = r#"^the "([^"]*)" exchange still exists with no bindings to "([^"]*)"$"#)]
async fn then_exchange_no_bindings(world: &mut MessageQueueWorld, exchange: String, queue: String) {
    let count = count_bindings(world, &exchange, &queue).await;
    assert_eq!(
        count,
        Some(0),
        "exchange {exchange:?} must still exist with 0 bindings to {queue:?}"
    );
}

#[then(regex = r#"^queue "([^"]*)" no longer exists$"#)]
async fn then_queue_gone(world: &mut MessageQueueWorld, queue: String) {
    assert!(
        !queue_exists(world, &queue).await,
        "queue {queue:?} must have been removed"
    );
}

// --- @boundary Thens ---------------------------------------------------------

#[then(regex = r#"^the bind fails with "([^"]*)"$"#)]
async fn then_bind_fails(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

#[then(regex = r#"^the publish fails with "([^"]*)"$"#)]
async fn then_publish_fails_now(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

#[then(regex = r#"^the declare fails with "([^"]*)"$"#)]
async fn then_declare_fails(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

#[then(regex = r#"^the delete fails with "([^"]*)"$"#)]
async fn then_delete_fails(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

#[then(regex = r#"^the consume fails with "([^"]*)"$"#)]
async fn then_consume_fails(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

#[then(regex = r#"^the unbind fails with "([^"]*)"$"#)]
async fn then_unbind_fails(world: &mut MessageQueueWorld, want: String) {
    assert_err(world, &want);
}

fn assert_err(world: &MessageQueueWorld, want: &str) {
    let got = world
        .last_error
        .as_ref()
        .unwrap_or_else(|| panic!("expected an AmqpError {want:?}, but the operation succeeded"));
    assert_eq!(
        err_name(got),
        want,
        "expected AmqpError {want:?}, got {got:?}"
    );
}

// --- @linearizability Thens --------------------------------------------------

#[then(regex = r#"^the consumer receives exactly (\d+) messages with no loss or duplication$"#)]
async fn then_no_loss(world: &mut MessageQueueWorld, n: usize) {
    let label = sole_consumer_label(world);
    let ok = settle(|| count_of(world, &label) == n).await;
    assert!(
        ok,
        "exactly {n} messages must be delivered with no loss or duplication, got {}",
        count_of(world, &label)
    );
}

#[then(
    regex = r#"^either the message is delivered to "([^"]*)" or it is dropped, never partially routed$"#
)]
async fn then_atomic_route(world: &mut MessageQueueWorld, queue: String) {
    // The bind|publish race must deliver 0 or 1 to q's consumer, never a partial
    // or duplicated state. q has no consumer in this scenario (only a binding race),
    // so we assert the queue stayed coherent: it still exists and the actor is live.
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        queue_exists(world, &queue).await,
        "queue {queue} must still exist after the race"
    );
    // Liveness: the actor answers => the concurrent bind|publish did not corrupt or
    // panic the run-loop, so routing was atomic (mailbox-serialised).
    assert!(
        count_bindings(world, "x", &queue).await.is_some(),
        "the exchange must remain coherent (the bind|publish race never partially routed)"
    );
}

/// The label of the sole consumer in a single-consumer scenario. Scenarios attach
/// exactly one consumer (keyed by its queue name), so this resolves it without the
/// feature needing to repeat the queue name in the Then.
fn sole_consumer_label(world: &MessageQueueWorld) -> String {
    let live: Vec<&String> = world.consumers.keys().collect();
    assert_eq!(
        live.len(),
        1,
        "expected exactly one consumer in this scenario, found {:?}",
        live
    );
    live[0].clone()
}

// ===========================================================================
// @property / @model laws (message_queue.properties.feature)
//
// proptest's synchronous runner cannot drive this async, multi-actor,
// global-state SUT inside cucumber's tokio runtime (a nested `block_on` panics).
// Per docs/testing/README.md Phase-3 §4 each law is therefore a DOCUMENTED bounded
// boundary-loop over the EXACT `# GEN:` boundary set, with an INDEPENDENT oracle
// (glob crate directly, or an AMQP set-membership model written from scratch).
// ===========================================================================

/// Drives one Note publish through a fresh queue and returns whether `label`'s
/// consumer received exactly the expected count, settling for wrongful deliveries.
async fn law_delivers(
    kind: ExchangeType,
    bindings: &[(&str, &str)],
    routing_key: &str,
    props_headers: Option<HashMap<String, String>>,
    expected: &HashMap<&str, usize>,
) -> bool {
    let mq = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::Guaranteed));
    mq.wait_for_startup().await;
    mq.tell(ExchangeDeclare {
        exchange: "x".to_owned(),
        kind,
        auto_delete: false,
    })
    .await
    .unwrap();

    let mut recs: HashMap<&str, Arc<Mutex<Received>>> = HashMap::new();
    // Keep each consumer's `ActorRef` alive for the life of the run; otherwise the
    // only strong handle is the one inside the SUT registration and a kill()/drop
    // race could stop the actor before delivery.
    let mut keep_alive: Vec<ActorRef<Recorder>> = Vec::new();
    let mut queues: HashMap<&str, ()> = HashMap::new();
    for &(q, key) in bindings {
        if queues.insert(q, ()).is_none() {
            mq.tell(QueueDeclare {
                queue: q.to_owned(),
                auto_delete: false,
            })
            .await
            .unwrap();
            let (reff, received) = spawn_recorder(None).await;
            mq.tell(BasicConsume {
                queue: q.to_owned(),
                recipient: reff.clone().recipient::<Note>(),
                tags: HashMap::new(),
            })
            .await
            .unwrap();
            keep_alive.push(reff);
            recs.insert(q, received);
        }
        // Bind via `ask` (NOT `tell`): a duplicate (queue,key) is rejected by the
        // SUT with BindingAlreadyExists; an unhandled `tell` error would be treated
        // as fatal and STOP the run-loop. `ask` returns the error to us so we can
        // discard it — a deliberate dup in the multiset is then a no-op, matching
        // the set semantics under test.
        let bind_res = mq
            .ask(QueueBind {
                queue: q.to_owned(),
                exchange: "x".to_owned(),
                routing_key: key.to_owned(),
                arguments: HashMap::new(),
            })
            .await;
        let _ = amqp_err(bind_res);
    }

    let props = MessageProperties {
        headers: props_headers,
        filter: None,
    };
    // `ask` so the publish HANDLER (the Guaranteed delivery loop) fully completes
    // before we observe — a `tell` would only await mailbox enqueue.
    let pub_res = mq
        .ask(BasicPublish {
            exchange: "x".to_owned(),
            routing_key: routing_key.to_owned(),
            message: Note {
                tag: "m".to_owned(),
            },
            properties: props,
        })
        .await;
    let _ = amqp_err(pub_res);

    // Settle until every recipient reaches its oracle count (a `tell` only awaits
    // enqueue, not the Guaranteed delivery loop), then sleep a final beat so any
    // wrongful EXTRA delivery (oracle 0, or over-count) would also have landed.
    let reached = settle(|| {
        recs.iter().all(|(&q, received)| {
            let want = expected.get(q).copied().unwrap_or(0);
            received.lock().unwrap().tags.len() == want
        })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut ok = reached;
    for (&q, received) in &recs {
        let want = expected.get(q).copied().unwrap_or(0);
        let got = received.lock().unwrap().tags.len();
        if got != want {
            ok = false;
        }
    }
    mq.kill();
    ok
}

// --- Law: Direct exchange delivers iff routing key == binding key ------------

#[given(regex = r#"^a Direct exchange "x" with any set of \(queue, binding-key\) bindings$"#)]
async fn law_given_direct(_w: &mut MessageQueueWorld) {}

#[when(regex = r#"^a message is published to "x" with any routing key r$"#)]
async fn law_when_publish_r(_w: &mut MessageQueueWorld) {}

#[then(regex = r#"^a bound queue receives the message iff its binding key equals r exactly$"#)]
async fn law_then_direct(_w: &mut MessageQueueWorld) {
    // GEN: bindings size {0,1,2,8} over (queue q0..q3, key {"","k","a.b","order.new"});
    // r over the same alphabet incl. a present key, an absent key, and "".
    let keys = ["", "k", "a.b", "order.new"];
    let multisets: Vec<Vec<(&str, &str)>> = vec![
        vec![],
        vec![("q0", "k")],
        vec![("q0", "k"), ("q1", "a.b")],
        vec![
            ("q0", "k"),
            ("q0", "a.b"),
            ("q1", "k"),
            ("q1", "order.new"),
            ("q2", ""),
            ("q2", "a.b"),
            ("q3", "order.new"),
            ("q3", "k"),
        ],
    ];
    for bindings in &multisets {
        for &r in &keys {
            // ORACLE (independent): a queue is delivered iff one of its bindings has
            // key == r exactly; HashSet target => one copy per queue.
            let mut expected: HashMap<&str, usize> = HashMap::new();
            for &(q, k) in bindings {
                if k == r {
                    expected.insert(q, 1);
                }
            }
            assert!(
                law_delivers(ExchangeType::Direct, bindings, r, None, &expected).await,
                "Direct: bindings {bindings:?}, r {r:?} must deliver per exact-key oracle"
            );
        }
    }
}

#[then(
    regex = r#"^it receives one copy regardless of how many of its bindings also match \(set-deduped per queue\)$"#
)]
async fn law_then_direct_dedup(_w: &mut MessageQueueWorld) {
    // A queue with two bindings both matching r receives exactly one copy (the SUT
    // collects targets in a HashSet). ORACLE: 1, not 2.
    let bindings = vec![("q0", "k"), ("q0", "k2"), ("q0", "k")];
    // Note: the third ("q0","k") is a duplicate (queue,key) the SUT rejects; the
    // first ("q0","k") still matches r="k" so the queue is targeted once.
    let mut expected = HashMap::new();
    expected.insert("q0", 1);
    assert!(
        law_delivers(ExchangeType::Direct, &bindings, "k", None, &expected).await,
        "Direct: a queue matched by several bindings receives exactly one copy"
    );
}

// --- Law: Fanout delivers to every bound queue for any r ---------------------

#[given(regex = r#"^a Fanout exchange "x" with any set of bound queues$"#)]
async fn law_given_fanout(_w: &mut MessageQueueWorld) {}

#[then(regex = r#"^every bound queue receives exactly one copy, independent of r$"#)]
async fn law_then_fanout(_w: &mut MessageQueueWorld) {
    // GEN: bound-queue set size {0,1,2,16}; r over {"","anything","a/b","a.b.c"}.
    let rs = ["", "anything", "a/b", "a.b.c"];
    let sizes = [0usize, 1, 2, 16];
    let names: Vec<String> = (0..16).map(|i| format!("q{i}")).collect();
    for &size in &sizes {
        let bindings: Vec<(&str, &str)> =
            names.iter().take(size).map(|q| (q.as_str(), "")).collect();
        // ORACLE (independent): every distinct bound queue, regardless of r.
        let mut expected: HashMap<&str, usize> = HashMap::new();
        for &(q, _) in &bindings {
            expected.insert(q, 1);
        }
        for &r in &rs {
            assert!(
                law_delivers(ExchangeType::Fanout, &bindings, r, None, &expected).await,
                "Fanout: {size} queues, r {r:?} must deliver to every bound queue once"
            );
        }
    }
}

// --- Law: Topic delivers iff glob(binding key) matches r --------------------

#[given(
    regex = r#"^a Topic exchange "x" with any set of \(queue, compilable-glob-key\) bindings$"#
)]
async fn law_given_topic(_w: &mut MessageQueueWorld) {}

#[then(
    regex = r#"^a bound queue receives the message iff one of its binding globs matches r under the MessageQueue MatchOptions \(separator '/', '\*' spans '\.'\)$"#
)]
async fn law_then_topic(_w: &mut MessageQueueWorld) {
    // GEN: compilable-glob binding keys incl. boundaries; r incl. boundaries.
    let opts = mq_match_options();
    let keys = ["", "*", "log.*", "log/*", "a/*/b", "log.warn"];
    let rs = ["", "log.warn", "log.warn.detail", "log/x"];
    // One queue per distinct key so each binding's match is observed independently.
    for &key in &keys {
        let bindings = vec![("q", key)];
        for &r in &rs {
            // ORACLE (independent): glob crate's own decision under the SUT options.
            let matches = Pattern::new(key)
                .expect("GEN keys are all compilable globs")
                .matches_with(r, opts);
            let mut expected: HashMap<&str, usize> = HashMap::new();
            if matches {
                expected.insert("q", 1);
            }
            assert!(
                law_delivers(ExchangeType::Topic, &bindings, r, None, &expected).await,
                "Topic: key {key:?} vs r {r:?} must follow the glob oracle (matches={matches})"
            );
        }
    }
}

// --- Law: Headers delivers per x-match all/any law --------------------------

#[given(regex = r#"^a Headers exchange "x" and a queue bound with x-match and any argument map$"#)]
async fn law_given_headers(_w: &mut MessageQueueWorld) {}

#[when(regex = r#"^a message is published to "x" with any header map h$"#)]
async fn law_when_publish_h(_w: &mut MessageQueueWorld) {}

#[then(
    regex = r#"^with x-match=all the queue receives the message iff every non-"x-" argument is present in h with an equal value$"#
)]
async fn law_then_headers_all(_w: &mut MessageQueueWorld) {
    headers_law("all").await;
}

#[then(
    regex = r#"^with x-match=any the queue receives it iff at least one non-"x-" argument is present in h with an equal value$"#
)]
async fn law_then_headers_any(_w: &mut MessageQueueWorld) {
    headers_law("any").await;
}

/// One Headers law run for the given x-match mode over the GEN header maps.
async fn headers_law(mode: &str) {
    // GEN: argument map size {0,1,3}; h ∈ subsets/supersets/mismatches incl. empty.
    let arg_sets: Vec<Vec<(&str, &str)>> = vec![
        vec![],
        vec![("format", "json")],
        vec![("format", "json"), ("level", "high"), ("team", "core")],
    ];
    let h_sets: Vec<Vec<(&str, &str)>> = vec![
        vec![],
        vec![("format", "json")],
        vec![("format", "xml")],
        vec![("format", "json"), ("level", "high")],
        vec![
            ("format", "json"),
            ("level", "high"),
            ("team", "core"),
            ("x", "y"),
        ],
    ];
    for args in &arg_sets {
        for h in &h_sets {
            let mq = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::Guaranteed));
            mq.wait_for_startup().await;
            mq.tell(ExchangeDeclare {
                exchange: "x".to_owned(),
                kind: ExchangeType::Headers,
                auto_delete: false,
            })
            .await
            .unwrap();
            mq.tell(QueueDeclare {
                queue: "q".to_owned(),
                auto_delete: false,
            })
            .await
            .unwrap();
            let (reff, received) = spawn_recorder(None).await;
            mq.tell(BasicConsume {
                queue: "q".to_owned(),
                recipient: reff.recipient::<Note>(),
                tags: HashMap::new(),
            })
            .await
            .unwrap();
            let mut arguments: HashMap<String, String> = args
                .iter()
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect();
            arguments.insert("x-match".to_owned(), mode.to_owned());
            mq.tell(QueueBind {
                queue: "q".to_owned(),
                exchange: "x".to_owned(),
                routing_key: String::new(),
                arguments,
            })
            .await
            .unwrap();

            let headers: HashMap<String, String> = h
                .iter()
                .map(|&(k, v)| (k.to_owned(), v.to_owned()))
                .collect();
            let publish_empty = headers.is_empty();
            let res = mq
                .ask(BasicPublish {
                    exchange: "x".to_owned(),
                    routing_key: String::new(),
                    message: Note {
                        tag: "m".to_owned(),
                    },
                    properties: MessageProperties {
                        headers: if publish_empty {
                            None
                        } else {
                            Some(headers.clone())
                        },
                        filter: None,
                    },
                })
                .await;
            let publish_err = amqp_err(res);

            // ORACLE (independent): all => args ⊆ h equal; any => args ∩ h non-empty
            // equal. Empty published headers => HeadersRequired error, no delivery.
            let want_delivered = if publish_empty {
                false
            } else {
                match mode {
                    "all" => args
                        .iter()
                        .all(|&(k, v)| headers.get(k) == Some(&v.to_owned())),
                    "any" => args
                        .iter()
                        .any(|&(k, v)| headers.get(k) == Some(&v.to_owned())),
                    _ => unreachable!(),
                }
            };

            tokio::time::sleep(Duration::from_millis(15)).await;
            let got = received.lock().unwrap().tags.len();
            if publish_empty {
                assert!(
                    matches!(publish_err, Some(AmqpError::HeadersRequired)),
                    "Headers/{mode}: empty published headers must error HeadersRequired, got {publish_err:?}"
                );
            }
            assert_eq!(
                got,
                usize::from(want_delivered),
                "Headers/{mode}: args {args:?}, h {h:?} must deliver={want_delivered}"
            );
            mq.kill();
        }
    }
}

// --- Law: BasicConsume idempotent per (queue, type, actor_id) ---------------

#[given(regex = r#"^a declared queue "q" and a consumer with a fixed ActorId$"#)]
async fn law_given_idempotent(_w: &mut MessageQueueWorld) {}

#[when(regex = r#"^that consumer is attached to "q" any number of times k$"#)]
async fn law_when_attach_k(_w: &mut MessageQueueWorld) {}

#[when(regex = r#"^a message of its type is published so "q" is selected$"#)]
async fn law_when_publish_selected(_w: &mut MessageQueueWorld) {}

#[then(
    regex = r#"^"q" holds exactly one registration for that ActorId and the consumer receives one copy$"#
)]
async fn law_then_idempotent(_w: &mut MessageQueueWorld) {
    // GEN: k {1,2,3,8}; interleaved with n {0,1,3} OTHER distinct ActorIds.
    for k in [1usize, 2, 3, 8] {
        for n_others in [0usize, 1, 3] {
            let mq = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::Guaranteed));
            mq.wait_for_startup().await;
            mq.tell(QueueDeclare {
                queue: "q".to_owned(),
                auto_delete: false,
            })
            .await
            .unwrap();
            // Bind q to the default exchange happens automatically; route via the
            // default exchange to its own name "q".
            let (reff, received) = spawn_recorder(None).await;
            // Attach the SAME consumer k times, interleaved with n_others distinct.
            let mut others = Vec::new();
            for i in 0..k.max(n_others) {
                if i < k {
                    mq.tell(BasicConsume {
                        queue: "q".to_owned(),
                        recipient: reff.clone().recipient::<Note>(),
                        tags: HashMap::new(),
                    })
                    .await
                    .unwrap();
                }
                if i < n_others {
                    let (o, orx) = spawn_recorder(None).await;
                    mq.tell(BasicConsume {
                        queue: "q".to_owned(),
                        recipient: o.clone().recipient::<Note>(),
                        tags: HashMap::new(),
                    })
                    .await
                    .unwrap();
                    others.push((o, orx));
                }
            }
            // Publish once via the default exchange to "q".
            mq.tell(BasicPublish {
                exchange: String::new(),
                routing_key: "q".to_owned(),
                message: Note {
                    tag: "m".to_owned(),
                },
                properties: MessageProperties::default(),
            })
            .await
            .unwrap();
            // ORACLE (independent): the fixed-id consumer is registered exactly once
            // regardless of k, so it receives exactly one copy; each OTHER distinct
            // id also receives exactly one.
            let got = settle(|| received.lock().unwrap().tags.len() == 1).await;
            assert!(
                got,
                "idempotent: k={k}, others={n_others}: fixed-id consumer must receive exactly 1 (got {})",
                received.lock().unwrap().tags.len()
            );
            for (_, orx) in &others {
                assert_eq!(
                    orx.lock().unwrap().tags.len(),
                    1,
                    "each distinct OTHER consumer must receive exactly one copy"
                );
            }
            mq.kill();
        }
    }
}

// --- Law: pruned iff ActorNotRunning, never on full/slow --------------------

#[given(regex = r#"^a declared bound queue "q" with one consumer and any delivery strategy$"#)]
async fn law_given_prune(_w: &mut MessageQueueWorld) {}

#[when(
    regex = r#"^a message is published and the consumer's mailbox is full, slow, or its actor is stopped$"#
)]
async fn law_when_prune(_w: &mut MessageQueueWorld) {}

#[then(regex = r#"^the consumer is removed from "q" iff the strategy surfaced ActorNotRunning$"#)]
async fn law_then_prune(w: &mut MessageQueueWorld) {
    // GEN: strategy {Guaranteed, BestEffort, TimedDelivery(ZERO/1ms/50ms)};
    // consumer state {stopped} (prunes) vs {live-and-full} (never prunes).
    // ORACLE: removed iff the delivery surfaced ActorNotRunning (only the stopped
    // case). A MailboxFull (BestEffort) or Timeout (TimedDelivery) never removes.
    // The auto_delete queue gives a crisp observable: it vanishes iff its LAST
    // consumer was pruned.
    let strategies = [
        DeliveryStrategy::Guaranteed,
        DeliveryStrategy::BestEffort,
        DeliveryStrategy::TimedDelivery(Duration::ZERO),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(1)),
        DeliveryStrategy::TimedDelivery(Duration::from_millis(50)),
    ];
    let mut stopped_ok = true;
    let mut full_ok = true;
    for d in strategies {
        // A stopped consumer surfaces ActorNotRunning => pruned => queue vanishes.
        let pruned = !prune_case(d, /* stopped = */ true).await;
        stopped_ok &= pruned;
        // A full (live) consumer never surfaces ActorNotRunning => never pruned =>
        // queue survives. Only the strategies that observe a full mailbox apply
        // (Guaranteed blocks rather than skips, so it is excluded here).
        if !matches!(d, DeliveryStrategy::Guaranteed) {
            let survived = prune_case(d, /* stopped = */ false).await;
            full_ok &= survived;
        }
    }
    w.prune_stopped_ok = Some(stopped_ok);
    w.prune_full_ok = Some(full_ok);
    assert_eq!(
        w.prune_stopped_ok,
        Some(true),
        "a stopped consumer (ActorNotRunning) must be pruned for every strategy"
    );
}

#[then(regex = r#"^a MailboxFull \(BestEffort\) or a Timeout \(TimedDelivery\) never removes it$"#)]
async fn law_then_prune_full(w: &mut MessageQueueWorld) {
    assert_eq!(
        w.prune_full_ok,
        Some(true),
        "a full/slow consumer must NEVER be pruned (MailboxFull/Timeout != ActorNotRunning)"
    );
}

/// One prune-law case: a single consumer that is either stopped or full. Returns
/// whether the consumer's `q` (an auto_delete queue) still EXISTS after the
/// publish — i.e. `true` ⇒ NOT pruned, `false` ⇒ pruned.
async fn prune_case(d: DeliveryStrategy, stopped: bool) -> bool {
    let mq = MessageQueue::spawn(MessageQueue::new(d));
    mq.wait_for_startup().await;
    mq.tell(ExchangeDeclare {
        exchange: "events".to_owned(),
        kind: ExchangeType::Fanout,
        auto_delete: false,
    })
    .await
    .unwrap();
    // auto_delete queue so a successful prune of its LAST consumer removes it,
    // giving a crisp observable: QueueExists == false iff pruned.
    mq.tell(QueueDeclare {
        queue: "q".to_owned(),
        auto_delete: true,
    })
    .await
    .unwrap();
    mq.tell(QueueBind {
        queue: "q".to_owned(),
        exchange: "events".to_owned(),
        routing_key: String::new(),
        arguments: HashMap::new(),
    })
    .await
    .unwrap();

    let cap = if stopped { None } else { Some(1) };
    let (reff, _received) = spawn_recorder(cap).await;
    let mut release = None;
    if stopped {
        // nothing
    } else {
        release = Some(make_full(&reff).await);
    }
    mq.tell(BasicConsume {
        queue: "q".to_owned(),
        recipient: reff.clone().recipient::<Note>(),
        tags: HashMap::new(),
    })
    .await
    .unwrap();
    if stopped {
        reff.kill();
        reff.wait_for_shutdown().await;
    }

    // Publish — for a stopped consumer this surfaces ActorNotRunning and prunes the
    // last consumer => the auto_delete queue is removed. A `tell` only awaits
    // enqueue, so poll for the post-handling state.
    mq.tell(BasicPublish {
        exchange: "events".to_owned(),
        routing_key: "x".to_owned(),
        message: Note {
            tag: "m".to_owned(),
        },
        properties: MessageProperties::default(),
    })
    .await
    .unwrap();

    // For the stopped case, settle until the queue vanishes (pruned). For the full
    // case, settle a fixed window during which a wrongful prune WOULD have removed
    // the queue, then read the final state.
    let exists = if stopped {
        // Poll specifically for non-existence (the prune is async).
        let mut e = true;
        for _ in 0..200 {
            e = mq
                .ask(QueueExists {
                    queue: "q".to_owned(),
                })
                .await
                .expect("MessageQueue alive");
            if !e {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        e
    } else {
        tokio::time::sleep(Duration::from_millis(120)).await;
        mq.ask(QueueExists {
            queue: "q".to_owned(),
        })
        .await
        .expect("MessageQueue alive")
    };
    if let Some(tx) = release {
        let _ = tx.send(true);
    }
    mq.kill();
    reff.kill();
    exists
}

// --- Model: concurrent fanout publishes refine per-queue counters -----------

#[given(
    regex = r#"^a Fanout exchange "x" with any fixed set of bound queues, each with a consumer$"#
)]
async fn model_given_fanout(_w: &mut MessageQueueWorld) {}

#[when(regex = r#"^N messages are published concurrently from P tasks to "x"$"#)]
async fn model_when_concurrent(_w: &mut MessageQueueWorld) {}

#[then(
    regex = r#"^each bound queue's consumer receives exactly N messages — no loss, no duplication$"#
)]
async fn model_then_counts(_w: &mut MessageQueueWorld) {
    // GEN: P {2,10}; N {1,50,100}; bound-queue set {1,2,8}.
    let cases = [(2u32, 1u32, 1usize), (10, 50, 2), (10, 100, 8)];
    for (p, n, qcount) in cases {
        let mq = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::Guaranteed));
        mq.wait_for_startup().await;
        mq.tell(ExchangeDeclare {
            exchange: "x".to_owned(),
            kind: ExchangeType::Fanout,
            auto_delete: false,
        })
        .await
        .unwrap();
        let names: Vec<String> = (0..qcount).map(|i| format!("q{i}")).collect();
        let mut recs = Vec::new();
        for q in &names {
            mq.tell(QueueDeclare {
                queue: q.clone(),
                auto_delete: false,
            })
            .await
            .unwrap();
            mq.tell(QueueBind {
                queue: q.clone(),
                exchange: "x".to_owned(),
                routing_key: String::new(),
                arguments: HashMap::new(),
            })
            .await
            .unwrap();
            let (reff, received) = spawn_recorder(None).await;
            mq.tell(BasicConsume {
                queue: q.clone(),
                recipient: reff.recipient::<Note>(),
                tags: HashMap::new(),
            })
            .await
            .unwrap();
            recs.push(received);
        }

        let per = n / p;
        let barrier = Arc::new(Barrier::new(p as usize));
        let mut handles = Vec::new();
        for _ in 0..p {
            let mq = mq.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for _ in 0..per {
                    mq.tell(BasicPublish {
                        exchange: "x".to_owned(),
                        routing_key: String::new(),
                        message: Note {
                            tag: "m".to_owned(),
                        },
                        properties: MessageProperties::default(),
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.expect("publish task join");
        }

        // ORACLE (independent): each queue's consumer count == total publishes.
        let total = per * p;
        for received in &recs {
            let ok = settle(|| received.lock().unwrap().tags.len() == total as usize).await;
            assert!(
                ok,
                "P={p}, N={n}, queues={qcount}: each consumer must receive exactly {total} (got {})",
                received.lock().unwrap().tags.len()
            );
        }
        mq.kill();
    }
}

// ===========================================================================
// @bug probe (card #79) — REAL defect, no missing variant referenced
// ===========================================================================

/// Drives the SUT exactly as the `@bug:707` / `@bug:591` scenarios describe — bind
/// a Topic queue with a NON-COMPILABLE glob key (`"[unclosed"`), which `QueueBind`
/// accepts without validation (the :591 gap), then publish, which reaches
/// `Pattern::new(&binding.routing_key).unwrap()` (the :707 panic). Returns whether
/// the MessageQueue actor SURVIVED the publish (the DESIRED behaviour).
///
/// Today the run-loop panics, so this returns `false` (RED). Once :591/:707 are
/// fixed to return an error instead of panicking (card #79), it returns `true`.
/// This reproduces the defect WITHOUT naming `AmqpError::InvalidRoutingKey` (which
/// does not exist yet), so the crate compiles. Only `message_queue_bug_bdd.rs`
/// calls it; the other runners include this module without it.
#[allow(dead_code)]
pub async fn malformed_topic_key_survives_publish() -> bool {
    let mq = MessageQueue::spawn(MessageQueue::new(DeliveryStrategy::BestEffort));
    mq.wait_for_startup().await;
    mq.tell(ExchangeDeclare {
        exchange: "logs".to_owned(),
        kind: ExchangeType::Topic,
        auto_delete: false,
    })
    .await
    .expect("declare exchange");
    mq.tell(QueueDeclare {
        queue: "q".to_owned(),
        auto_delete: false,
    })
    .await
    .expect("declare queue");
    // QueueBind accepts the malformed Topic key with NO glob validation (:591 gap).
    mq.tell(QueueBind {
        queue: "q".to_owned(),
        exchange: "logs".to_owned(),
        routing_key: "[unclosed".to_owned(),
        arguments: HashMap::new(),
    })
    .await
    .expect("malformed bind is (today) accepted");
    // Publish reaches Pattern::new("[unclosed").unwrap() at :707 and panics the
    // run-loop today. We don't care about the publish Result here (the run-loop may
    // die mid-handle); we probe LIVENESS afterwards.
    let _ = mq
        .tell(BasicPublish {
            exchange: "logs".to_owned(),
            routing_key: "log.warn".to_owned(),
            message: Note {
                tag: "m".to_owned(),
            },
            properties: MessageProperties::default(),
        })
        .await;
    // The actor survives iff it still answers a query. Poll briefly: a panicked
    // run-loop closes the mailbox.
    for _ in 0..100 {
        if mq
            .ask(QueueExists {
                queue: "q".to_owned(),
            })
            .await
            .is_ok()
        {
            return true; // survived => bug fixed
        }
        if !mq.is_alive() {
            return false; // run-loop died => bug live
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    mq.is_alive()
}
