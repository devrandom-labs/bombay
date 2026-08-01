//! Card #209: capture-subscriber assertions over the `tracing` feature's
//! lifecycle spans/events. The whole file is feature-gated — a
//! `--no-default-features` build has no surface to test.
#![cfg(feature = "tracing")]

use core::convert::Infallible;

use core::time::Duration;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use bombay::{
    actor::{
        Actor, ActorRef, DEFAULT_MAILBOX_CAPACITY, Flow, PreparedActor, RunResult, SpawnConfig,
        WeakActorRef,
    },
    caps,
    error::{ActorStopReason, PanicError, PanicReason},
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    restart::{RestartConfig, RestartPolicy, jittered_backoff},
    test_support::{set_supervisor_rng_seed, terminate_bound},
};
use tokio::{sync::mpsc, time::timeout};

use capture::field;

mod capture {
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex};

    use tracing::{
        Event, Id, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Record},
    };
    use tracing_subscriber::{
        Layer, layer::Context, layer::SubscriberExt as _, registry::LookupSpan,
    };

    /// One span seen by the subscriber. `parent` is the resolved parent span id
    /// (explicit or contextual); `follows` collects `follows_from` links.
    #[derive(Debug, Clone)]
    pub struct SpanRec {
        pub id: u64,
        pub name: String,
        pub parent: Option<u64>,
        pub follows: Vec<u64>,
        pub fields: Vec<(String, String)>,
    }

    /// One event: its level (as string), the span it fired inside, its fields
    /// (the `message` field carries the format message).
    #[derive(Debug, Clone)]
    pub struct EventRec {
        pub level: String,
        pub span: Option<u64>,
        pub fields: Vec<(String, String)>,
    }

    #[derive(Debug, Default)]
    pub struct Store {
        pub spans: Vec<SpanRec>,
        pub events: Vec<EventRec>,
    }

    impl Store {
        pub fn span(&self, name: &str) -> Option<&SpanRec> {
            self.spans.iter().find(|s| s.name == name)
        }
        pub fn events_at(&self, level: &str) -> Vec<&EventRec> {
            self.events.iter().filter(|e| e.level == level).collect()
        }
    }

    pub fn field(fields: &[(String, String)], name: &str) -> Option<String> {
        fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    }

    struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

    impl Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, f: &Field, value: &dyn core::fmt::Debug) {
            let mut s = String::new();
            let _ = write!(s, "{value:?}");
            self.0.push((f.name().to_owned(), s));
        }
        fn record_str(&mut self, f: &Field, value: &str) {
            self.0.push((f.name().to_owned(), value.to_owned()));
        }
        fn record_u64(&mut self, f: &Field, value: u64) {
            self.0.push((f.name().to_owned(), value.to_string()));
        }
        fn record_i64(&mut self, f: &Field, value: i64) {
            self.0.push((f.name().to_owned(), value.to_string()));
        }
        fn record_bool(&mut self, f: &Field, value: bool) {
            self.0.push((f.name().to_owned(), value.to_string()));
        }
    }

    #[derive(Clone, Default)]
    pub struct CaptureLayer {
        pub store: Arc<Mutex<Store>>,
    }

    impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let mut fields = Vec::new();
            attrs.record(&mut FieldVisitor(&mut fields));
            let parent = attrs
                .parent()
                .cloned()
                .or_else(|| {
                    if attrs.is_root() {
                        None
                    } else {
                        ctx.current_span().id().cloned()
                    }
                })
                .map(|id| id.into_u64());
            self.store.lock().unwrap().spans.push(SpanRec {
                id: id.into_u64(),
                name: attrs.metadata().name().to_owned(),
                parent,
                follows: Vec::new(),
                fields,
            });
        }

        // Registry reuses ids after close: always update the LAST span with the
        // id, which is the live incarnation in these short single-actor tests.
        fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
            let mut fields = Vec::new();
            values.record(&mut FieldVisitor(&mut fields));
            let mut store = self.store.lock().unwrap();
            if let Some(rec) = store.spans.iter_mut().rev().find(|s| s.id == id.into_u64()) {
                rec.fields.extend(fields);
            }
        }

        fn on_follows_from(&self, id: &Id, follows: &Id, _ctx: Context<'_, S>) {
            let mut store = self.store.lock().unwrap();
            if let Some(rec) = store.spans.iter_mut().rev().find(|s| s.id == id.into_u64()) {
                rec.follows.push(follows.into_u64());
            }
        }

        fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
            let mut fields = Vec::new();
            event.record(&mut FieldVisitor(&mut fields));
            let span = event
                .parent()
                .cloned()
                .or_else(|| ctx.current_span().id().cloned())
                .map(|id| id.into_u64());
            self.store.lock().unwrap().events.push(EventRec {
                level: event.metadata().level().to_string(),
                span,
                fields,
            });
        }
    }

    /// Installs a fresh capture subscriber as the THREAD default. Tests must
    /// run on a current-thread runtime (`#[tokio::test]` default) so every
    /// actor task emits on this thread.
    pub fn install() -> (Arc<Mutex<Store>>, tracing::subscriber::DefaultGuard) {
        let layer = CaptureLayer::default();
        let store = Arc::clone(&layer.store);
        let subscriber = tracing_subscriber::registry().with(layer);
        (store, tracing::subscriber::set_default(subscriber))
    }
}

#[derive(Debug)]
struct Ping;
impl Msg for Ping {}

struct Probe;
impl Mailboxed for Probe {
    type Msg = Ping;
}
impl Actor for Probe {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Probe)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// The removed `Spawn` verb, suite-local over the public floor.
fn spawn_plain<A: Actor>(args: A::Args) -> ActorRef<A> {
    let prepared = PreparedActor::<A>::new(SpawnConfig::default());
    let actor_ref = prepared.actor_ref().clone();
    let _join = prepared.spawn(args);
    actor_ref
}

fn default_cap() -> Capacity {
    Capacity::try_from(DEFAULT_MAILBOX_CAPACITY).expect("valid default capacity")
}

/// Spec D5: one root `actor.lifecycle` span per actor carrying `actor.name` +
/// `actor.id` at creation and `stop.reason` recorded at teardown; the spawn
/// site is a `follows_from` link, never a parent.
#[tokio::test]
async fn lifecycle_span_carries_identity_and_records_stop_reason() {
    let (store, _guard) = capture::install();

    let spawn_site = tracing::info_span!("spawn_site");
    let spawn_site_id = spawn_site
        .id()
        .expect("enabled by capture layer")
        .into_u64();

    let prepared = PreparedActor::<Probe>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let id = prepared.actor_ref().id();
    // Construct the run future INSIDE the spawn-site span: the lifecycle span
    // is created eagerly at `run()` call time, which is what `spawn()` relies
    // on to capture the spawn site.
    let run = spawn_site.in_scope(|| prepared.run(()));
    let result = timeout(terminate_bound(), run)
        .await
        .expect("actor must stop inside the bound");
    assert!(
        matches!(
            result,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "all-senders-gone collected stop, got {result:?}",
    );

    let store = store.lock().unwrap();
    let span = store
        .span("actor.lifecycle")
        .expect("lifecycle span emitted");
    assert_eq!(
        field(&span.fields, "actor.name").as_deref(),
        Some(core::any::type_name::<Probe>()),
        "actor.name = A::name() as a structured field",
    );
    assert_eq!(
        field(&span.fields, "actor.id"),
        Some(format!("{id:?}")),
        "actor.id as a structured field",
    );
    assert_eq!(
        field(&span.fields, "stop.reason"),
        Some(ActorStopReason::Collected.to_string()),
        "stop.reason recorded onto the lifecycle span at teardown",
    );
    assert_eq!(
        span.parent, None,
        "lifecycle span is a ROOT — the spawn site must not contain the actor's lifetime",
    );
    assert_eq!(
        span.follows,
        vec![spawn_site_id],
        "spawn site linked via follows_from",
    );

    // The three lifecycle trace events fire exactly once each — the emissions
    // themselves, not just the span, are part of the observable surface.
    let traces = store.events_at("TRACE");
    for expected in ["actor spawned", "actor started", "actor stopped"] {
        assert_eq!(
            traces
                .iter()
                .filter(|e| field(&e.fields, "message").as_deref() == Some(expected))
                .count(),
            1,
            "exactly one `{expected}` trace event",
        );
    }
    let stopped = store
        .events
        .iter()
        .find(|e| field(&e.fields, "message").as_deref() == Some("actor stopped"))
        .expect("actor stopped event present");
    assert_eq!(
        field(&stopped.fields, "reason"),
        Some(ActorStopReason::Collected.to_string()),
        "the stopped event carries the reason as a structured field",
    );
}

#[derive(Debug)]
struct StopErr;

struct FailingStop;
impl Mailboxed for FailingStop {
    type Msg = Ping;
}
impl Actor for FailingStop {
    type Args = ();
    type Error = StopErr;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(FailingStop)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Err(StopErr)
    }
}

/// Spec D8: an `on_stop` error is an `error!` event with structured
/// `reason` + `err` fields — the eprintln replacement can actually fail.
#[tokio::test]
async fn on_stop_error_emits_one_error_event_with_fields() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<FailingStop>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");
    assert!(matches!(result, RunResult::Stopped { .. }));

    let store = store.lock().unwrap();
    let errors = store.events_at("ERROR");
    assert_eq!(errors.len(), 1, "exactly one error event, got {errors:?}");
    assert_eq!(
        field(&errors[0].fields, "message").as_deref(),
        Some("on_stop returned an error"),
    );
    assert_eq!(
        field(&errors[0].fields, "reason"),
        Some(ActorStopReason::Collected.to_string()),
        "reason is a structured field, not formatted into the message",
    );
    assert_eq!(field(&errors[0].fields, "err").as_deref(), Some("StopErr"));
}

struct FailingStart;
impl Mailboxed for FailingStart {
    type Msg = Ping;
}
impl Actor for FailingStart {
    type Args = ();
    type Error = StopErr;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Err(StopErr)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// A failed `on_start` is an `error!` event carrying the wrapped hook error —
/// the one emission on the no-actor-was-ever-built path.
#[tokio::test]
async fn on_start_failure_emits_one_error_event() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<FailingStart>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("startup failure must resolve inside the bound");
    assert!(
        matches!(result, RunResult::StartupFailed(_)),
        "an on_start Err is a startup failure, got {result:?}",
    );

    let store = store.lock().unwrap();
    let errors = store.events_at("ERROR");
    assert_eq!(errors.len(), 1, "exactly one error event, got {errors:?}");
    assert_eq!(
        field(&errors[0].fields, "message").as_deref(),
        Some("on_start failed"),
    );
    // The production path wraps the hook's Err in exactly this constructor, so
    // the `?err` Debug rendering is reproducible rather than a substring guess.
    let expected = format!(
        "{:?}",
        PanicError::new(Box::new(StopErr), PanicReason::OnStart)
    );
    assert_eq!(field(&errors[0].fields, "err"), Some(expected));
}

struct PanickingStop;
impl Mailboxed for PanickingStop {
    type Msg = Ping;
}
impl Actor for PanickingStop {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(PanickingStop)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        panic!("boom")
    }
}

/// Spec D8: a panicking `on_stop` is caught by the runtime and surfaced as an
/// `error!` event with the preserved stop reason as a structured field.
#[tokio::test]
async fn on_stop_panic_emits_one_error_event() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<PanickingStop>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");
    assert!(matches!(result, RunResult::Stopped { .. }));

    let store = store.lock().unwrap();
    let errors = store.events_at("ERROR");
    assert_eq!(errors.len(), 1, "exactly one error event, got {errors:?}");
    assert_eq!(
        field(&errors[0].fields, "message").as_deref(),
        Some("on_stop panicked"),
    );
    assert_eq!(
        field(&errors[0].fields, "reason"),
        Some(ActorStopReason::Collected.to_string()),
        "reason is a structured field, not formatted into the message",
    );
}

struct HangingStop;
impl Mailboxed for HangingStop {
    type Msg = Ping;
}
impl Actor for HangingStop {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(HangingStop)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Self::Error> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// Spec D6: the handler's `actor.handle` span parents to the SENDER's span
/// captured at enqueue — cross-actor traces stitch into one tree.
#[tokio::test]
async fn handle_span_parents_to_the_callers_span() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<Probe>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();

    let send_site = tracing::info_span!("send_site");
    let send_site_id = send_site.id().expect("enabled").into_u64();
    send_site.in_scope(|| {
        actor_ref
            .tell(Ping)
            .try_send()
            .expect("mailbox has capacity");
    });
    drop(actor_ref);

    let _ = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");

    let store = store.lock().unwrap();
    let handle = store.span("actor.handle").expect("handle span emitted");
    assert_eq!(
        handle.parent,
        Some(send_site_id),
        "handle span parents to the caller's span, not the lifecycle span",
    );
    assert_eq!(
        field(&handle.fields, "msg.kind").as_deref(),
        Some(core::any::type_name::<Ping>()),
    );
    assert_eq!(
        field(&handle.fields, "actor.name").as_deref(),
        Some(core::any::type_name::<Probe>()),
    );
}

/// No caller span at enqueue → the handle span falls back to the contextual
/// parent, the actor's own lifecycle span.
#[tokio::test]
async fn handle_span_without_caller_parents_to_lifecycle() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<Probe>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    actor_ref
        .tell(Ping)
        .try_send()
        .expect("mailbox has capacity");
    drop(actor_ref);
    let _ = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");

    let store = store.lock().unwrap();
    let lifecycle_id = store.span("actor.lifecycle").expect("lifecycle span").id;
    let handle = store.span("actor.handle").expect("handle span emitted");
    assert_eq!(handle.parent, Some(lifecycle_id));
}

struct CrashingHandle;
impl Mailboxed for CrashingHandle {
    type Msg = Ping;
}
impl Actor for CrashingHandle {
    type Args = ();
    type Error = StopErr;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(CrashingHandle)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Err(StopErr)
    }
}

/// A handler's returned `Err` (controlled crash) is an `error!` event that
/// fires INSIDE the message's `actor.handle` span.
#[tokio::test]
async fn handler_crash_emits_one_error_event_inside_the_handle_span() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<CrashingHandle>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let actor_ref = prepared.actor_ref().clone();
    actor_ref
        .tell(Ping)
        .try_send()
        .expect("mailbox has capacity");
    drop(actor_ref);
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");
    assert!(
        matches!(
            result,
            RunResult::Stopped {
                reason: ActorStopReason::Panicked(_),
                ..
            }
        ),
        "a handler Err is a controlled crash, got {result:?}",
    );

    let store = store.lock().unwrap();
    let errors = store.events_at("ERROR");
    assert_eq!(errors.len(), 1, "exactly one error event, got {errors:?}");
    assert_eq!(
        field(&errors[0].fields, "message").as_deref(),
        Some("handler crashed"),
    );
    let handle_id = store.span("actor.handle").expect("handle span").id;
    assert_eq!(
        errors[0].span,
        Some(handle_id),
        "the crash event fires inside the handle span",
    );
}

/// Spec D8: an `on_stop` that outlives the 5 s notice grace is abandoned, and
/// the abandonment is an `error!` event carrying `reason` + `grace` fields.
/// Paused clock: tokio auto-advances past the grace instantly.
#[tokio::test(start_paused = true)]
async fn on_stop_abandoned_emits_one_error_event() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<HangingStop>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");
    assert!(matches!(result, RunResult::Stopped { .. }));

    let store = store.lock().unwrap();
    let errors = store.events_at("ERROR");
    assert_eq!(errors.len(), 1, "exactly one error event, got {errors:?}");
    assert_eq!(
        field(&errors[0].fields, "message").as_deref(),
        Some("on_stop exceeded the notice grace and was abandoned"),
    );
    assert_eq!(
        field(&errors[0].fields, "reason"),
        Some(ActorStopReason::Collected.to_string()),
        "reason is a structured field, not formatted into the message",
    );
    assert_eq!(
        field(&errors[0].fields, "grace"),
        Some(format!("{:?}", Duration::from_secs(5))),
        "grace rides the event as a structured field",
    );
}

/// An idle watcher: it observes deaths (the named OTP policy) and is never
/// messaged.
struct Watcher;

#[derive(bombay_macros::Provide)]
struct WatcherCaps {
    watching: caps::Watching<caps::OtpPropagation>,
}
impl caps::CapSet<Watcher> for WatcherCaps {
    fn build((): &()) -> Self {
        Self {
            watching: caps::Watching::new(),
        }
    }
}
impl caps::Actor for Watcher {
    type Msg = Ping;
    type Args = ();
    type Error = Infallible;
    type Caps = WatcherCaps;
    async fn init((): (), _: caps::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Watcher)
    }
    async fn handle(&mut self, _: Ping, _: caps::Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// Card #209 Task 7: delivering a death notice to a watcher is a `trace!` event
/// carrying the watcher's id, the stop reason, and the cleanup outcome —
/// emitted once per edge, from the dying actor's teardown.
#[tokio::test]
async fn death_notice_delivery_emits_one_trace_event_per_edge() {
    let (store, _guard) = capture::install();

    let watcher = caps::spawn::<Watcher>(());
    let watcher_id = watcher.id();

    let prepared = PreparedActor::<Probe>::new(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let target_ref = prepared.actor_ref().clone();
    timeout(terminate_bound(), watcher.watch(&target_ref))
        .await
        .expect("watch registration must complete inside the bound")
        .expect("a linked watcher can watch");
    drop(target_ref);

    // All senders gone => Collected stop (#253/ADR-0020); the notice fires in
    // the target's teardown, strictly BEFORE its RunResult resolves.
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("target must stop inside the bound");
    assert!(
        matches!(
            result,
            RunResult::Stopped {
                reason: ActorStopReason::Collected,
                ..
            }
        ),
        "all-senders-gone collected stop, got {result:?}",
    );

    let store = store.lock().unwrap();
    let notices: Vec<_> = store
        .events
        .iter()
        .filter(|e| field(&e.fields, "message").as_deref() == Some("death notice delivered"))
        .collect();
    assert_eq!(notices.len(), 1, "one watcher, one notice");
    assert_eq!(notices[0].level, "TRACE");
    assert_eq!(
        field(&notices[0].fields, "watcher.id"),
        Some(format!("{watcher_id:?}")),
    );
    assert_eq!(
        field(&notices[0].fields, "reason"),
        Some(ActorStopReason::Collected.to_string()),
    );
    assert_eq!(
        field(&notices[0].fields, "cleanup_failed").as_deref(),
        Some("false")
    );
}

/// An idle supervisor: the scenario is driven entirely through a child crash,
/// never supervisor messages.
#[derive(Debug)]
struct SupMsg;
impl Msg for SupMsg {}

struct Sup;

#[derive(bombay_macros::Provide)]
struct SupCaps {
    watching: caps::Watching<caps::OtpPropagation>,
    supervising: caps::Supervising<caps::OneForOne>,
}
impl caps::CapSet<Sup> for SupCaps {
    fn build((): &()) -> Self {
        Self {
            watching: caps::Watching::new(),
            supervising: caps::Supervising::new(),
        }
    }
}
impl caps::Actor for Sup {
    type Msg = SupMsg;
    type Args = ();
    type Error = Infallible;
    type Caps = SupCaps;
    async fn init((): (), _: caps::Ctx<'_, Self>) -> Result<Self, Self::Error> {
        Ok(Sup)
    }
    async fn handle(&mut self, _: SupMsg, _: caps::Ctx<'_, Self>) -> Result<Flow, Self::Error> {
        Ok(Flow::Continue)
    }
}

/// Spec D7: a scheduled child restart is a `warn` event carrying exact
/// structured `restart.attempt` / `restart.delay` fields (seeded RNG). The
/// paused clock auto-advances the backoff so the rebuild — which happens
/// strictly AFTER the warn is emitted — is the synchronization point.
#[tokio::test(start_paused = true)]
async fn scheduled_restart_emits_warn_with_attempt_and_delay() {
    const SEED: u64 = 7;
    // Seeds the supervisor's jitter RNG for this thread; must precede the spawn.
    set_supervisor_rng_seed(Some(SEED));
    let (store, _guard) = capture::install();

    let cfg = RestartConfig::new(RestartPolicy::Permanent);
    let sup = caps::spawn::<Sup>(());
    let (births_tx, mut births_rx) = mpsc::unbounded_channel::<()>();
    // Anchors every incarnation: an unanchored rebuild would ref-count-stop and
    // (under `Permanent`) churn through further restarts, each with its own warn.
    let anchors: Arc<Mutex<Vec<ActorRef<CrashingHandle>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_anchors = Arc::clone(&anchors);
    let child_id = timeout(
        terminate_bound(),
        sup.supervise(cfg, move || {
            let child = spawn_plain::<CrashingHandle>(());
            factory_anchors
                .lock()
                .expect("anchor lock")
                .push(child.clone());
            let _ = births_tx.send(());
            child
        }),
    )
    .await
    .expect("supervise must not hang")
    .expect("the supervisor is alive");

    // The first incarnation spawns inline in `supervise`; discard its birth.
    timeout(terminate_bound(), births_rx.recv())
        .await
        .expect("first birth within the bound")
        .expect("the birth tape is open");
    let first = anchors.lock().expect("anchor lock")[0].clone();
    // One controlled crash: the handler's Err kills the first incarnation.
    first.tell(Ping).try_send().expect("mailbox has capacity");
    // The rebuild's birth proves the restart was scheduled AND its backoff
    // elapsed — the warn fires synchronously before either.
    timeout(terminate_bound(), births_rx.recv())
        .await
        .expect("rebuild within the bound")
        .expect("the birth tape is open");

    // Mirrors production: the seeded supervisor RNG's FIRST draw is this
    // backoff's jitter (nothing else consumes it before the first crash).
    let expected = jittered_backoff(&cfg, 1, &mut fastrand::Rng::with_seed(SEED));

    let store = store.lock().unwrap();
    let warns = store.events_at("WARN");
    assert_eq!(
        warns.len(),
        1,
        "exactly one restart-scheduled warn, got {warns:?}"
    );
    assert_eq!(
        field(&warns[0].fields, "message").as_deref(),
        Some("child restart scheduled"),
    );
    assert_eq!(
        field(&warns[0].fields, "restart.attempt").as_deref(),
        Some("1"),
        "first consecutive failure is attempt 1",
    );
    assert_eq!(
        field(&warns[0].fields, "restart.delay"),
        Some(format!("{expected:?}")),
        "seeded jitter makes the delay exact",
    );
    assert_eq!(
        field(&warns[0].fields, "child.id"),
        Some(format!("{child_id:?}")),
        "the warn names the dead incarnation",
    );
}

/// Spec D7: a tripped restart budget is an `error!` event carrying the exact
/// lifetime rebuild count and the child's id. A zero budget makes the very
/// first failure the trip — `record_failure` counts the tripping failure
/// itself, so `rebuilds` is 1 (`GiveUp::Yes { rebuilds }` semantics).
#[tokio::test]
async fn restart_give_up_emits_one_error_event() {
    let (store, _guard) = capture::install();

    let (prepared, link_rx) = PreparedActor::<caps::Shell<Sup>>::new_linked(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let sup = prepared.actor_ref().clone();
    let join = prepared.spawn_supervised_task((), link_rx);

    // Zero budget: one failure is one too many — the first crash escalates,
    // so no backoff ever arms and no paused clock is needed.
    let cfg = RestartConfig::new(RestartPolicy::Permanent).with_max_restarts(0);
    let (births_tx, mut births_rx) = mpsc::unbounded_channel::<()>();
    // Anchors the incarnation: an unanchored child would ref-count-stop and
    // (under `Permanent`) trip the zero budget on its own schedule.
    let anchors: Arc<Mutex<Vec<ActorRef<CrashingHandle>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_anchors = Arc::clone(&anchors);
    let child_id = timeout(
        terminate_bound(),
        sup.supervise(cfg, move || {
            let child = spawn_plain::<CrashingHandle>(());
            factory_anchors
                .lock()
                .expect("anchor lock")
                .push(child.clone());
            let _ = births_tx.send(());
            child
        }),
    )
    .await
    .expect("supervise must not hang")
    .expect("the supervisor is alive");

    timeout(terminate_bound(), births_rx.recv())
        .await
        .expect("first birth within the bound")
        .expect("the birth tape is open");
    let first = anchors.lock().expect("anchor lock")[0].clone();
    // One controlled crash: the handler's Err trips the zero budget at once.
    first.tell(Ping).try_send().expect("mailbox has capacity");

    // The supervisor gives up and stops — the synchronization point: the error
    // event fires strictly before its RunResult resolves.
    let outcome = timeout(terminate_bound(), join)
        .await
        .expect("the escalating supervisor stops inside the bound")
        .expect("join");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::RestartLimitExceeded { child, rebuilds: 1 },
                ..
            } if child == child_id
        ),
        "the zero budget trips as RestartLimitExceeded, got {outcome:?}",
    );

    // The crashing child also emits a `handler crashed` error — filter by
    // message, never by total ERROR count.
    let store = store.lock().unwrap();
    let gave_up: Vec<_> = store
        .events
        .iter()
        .filter(|e| {
            field(&e.fields, "message").as_deref() == Some("restart budget exhausted, giving up")
        })
        .collect();
    assert_eq!(
        gave_up.len(),
        1,
        "exactly one give-up event, got {gave_up:?}"
    );
    assert_eq!(gave_up[0].level, "ERROR");
    assert_eq!(
        field(&gave_up[0].fields, "restart.rebuilds").as_deref(),
        Some("1"),
        "the tripping failure itself is counted",
    );
    assert_eq!(
        field(&gave_up[0].fields, "child.id"),
        Some(format!("{child_id:?}")),
        "the event names the child whose budget tripped",
    );
}

/// #253/ADR-0020: a supervised `Permanent` child whose factory anchors nothing
/// ref-count-collects once. The quiet death is witnessed by a `debug!` event
/// carrying the child's id; no `restart_scheduled` warn is emitted because no
/// policy rebuilds a collected child.
#[tokio::test(start_paused = true)]
async fn collected_child_emits_debug_event_and_no_restart_scheduled() {
    let (store, _guard) = capture::install();

    let sup = caps::spawn::<Sup>(());
    let child_id = timeout(
        terminate_bound(),
        sup.supervise(RestartConfig::new(RestartPolicy::Permanent), || {
            spawn_plain::<Probe>(())
        }),
    )
    .await
    .expect("supervise must not hang")
    .expect("the supervisor is alive");

    // Let the supervisor install the watch edge and receive the Collected notice.
    tokio::time::sleep(Duration::from_secs(1)).await;

    drop(sup);

    let store = store.lock().unwrap();
    let collected: Vec<_> = store
        .events
        .iter()
        .filter(|e| {
            field(&e.fields, "message").as_deref()
                == Some("supervised child collected (all refs dropped); left dead")
        })
        .collect();
    assert_eq!(
        collected.len(),
        1,
        "exactly one child_collected event, got {collected:?}"
    );
    assert_eq!(collected[0].level, "DEBUG");
    assert_eq!(
        field(&collected[0].fields, "child.id"),
        Some(format!("{child_id:?}")),
        "the event names the collected child",
    );

    let warns = store.events_at("WARN");
    assert!(
        warns.is_empty(),
        "no restart_scheduled warn for a collected child, got {warns:?}"
    );
}

/// A child whose FIRST incarnation starts fine (and crashes in its handler),
/// while the REBUILD's `on_start` panics — the knowable crash loop the
/// supervisor escalates on instead of burning budget.
struct RebuildBomb;
impl Mailboxed for RebuildBomb {
    type Msg = Ping;
}
impl Actor for RebuildBomb {
    type Args = Arc<AtomicBool>;
    type Error = StopErr;
    async fn on_start(started_before: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        if started_before.swap(true, Ordering::SeqCst) {
            panic!("rebuild on_start boom");
        }
        Ok(RebuildBomb)
    }
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
        Err(StopErr)
    }
}

/// Spec D7: a child dying in a lifecycle hook is refused a restart — the
/// escalation is an `error!` event naming the incarnation that died in the
/// hook (the REBUILD's id, not the first incarnation's), and the supervisor
/// stops with `ChildLifecycleFailed`.
///
/// The first incarnation must die a NON-hook death: an `on_start` panic on the
/// very first spawn can race the watch install and reach the supervisor as a
/// synthetic `AlreadyDead` (restart-worthy), never escalating. The rebuild
/// path has no such race — the loop installs the watch synchronously, before
/// the fresh incarnation ever polls — so the hook panic is staged there.
#[tokio::test(start_paused = true)]
async fn child_lifecycle_failure_escalates_with_error_event() {
    let (store, _guard) = capture::install();

    let (prepared, link_rx) = PreparedActor::<caps::Shell<Sup>>::new_linked(SpawnConfig {
        capacity: default_cap(),
        ..Default::default()
    });
    let sup = prepared.actor_ref().clone();
    let join = prepared.spawn_supervised_task((), link_rx);

    let started_before = Arc::new(AtomicBool::new(false));
    let (births_tx, mut births_rx) = mpsc::unbounded_channel();
    let anchors: Arc<Mutex<Vec<ActorRef<RebuildBomb>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_anchors = Arc::clone(&anchors);
    let factory_flag = Arc::clone(&started_before);
    let first_id = timeout(
        terminate_bound(),
        sup.supervise(RestartConfig::new(RestartPolicy::Permanent), move || {
            let child = spawn_plain::<RebuildBomb>(Arc::clone(&factory_flag));
            factory_anchors
                .lock()
                .expect("anchor lock")
                .push(child.clone());
            let _ = births_tx.send(child.id());
            child
        }),
    )
    .await
    .expect("supervise must not hang")
    .expect("the supervisor is alive");

    let born_first = timeout(terminate_bound(), births_rx.recv())
        .await
        .expect("first birth within the bound")
        .expect("the birth tape is open");
    assert_eq!(
        born_first, first_id,
        "the first incarnation is the returned id"
    );
    let first = anchors.lock().expect("anchor lock")[0].clone();
    // A handler crash (non-hook death) schedules an ordinary restart; the
    // paused clock auto-advances its backoff.
    first.tell(Ping).try_send().expect("mailbox has capacity");
    // The rebuild's birth: its `on_start` panics, which the supervisor refuses
    // to restart — escalation names THIS id.
    let rebuild_id = timeout(terminate_bound(), births_rx.recv())
        .await
        .expect("rebuild within the bound")
        .expect("the birth tape is open");

    let outcome = timeout(terminate_bound(), join)
        .await
        .expect("the escalating supervisor stops inside the bound")
        .expect("join");
    assert!(
        matches!(
            outcome,
            RunResult::Stopped {
                reason: ActorStopReason::ChildLifecycleFailed { child },
                ..
            } if child == rebuild_id
        ),
        "a hook death escalates as ChildLifecycleFailed, got {outcome:?}",
    );

    // Other ERROR events fire on this path (`handler crashed`, `on_start
    // failed`) — filter by message, never by total ERROR count.
    let store = store.lock().unwrap();
    let escalations: Vec<_> = store
        .events
        .iter()
        .filter(|e| {
            field(&e.fields, "message").as_deref() == Some("child lifecycle-hook failure escalated")
        })
        .collect();
    assert_eq!(
        escalations.len(),
        1,
        "exactly one escalation event, got {escalations:?}"
    );
    assert_eq!(escalations[0].level, "ERROR");
    assert_eq!(
        field(&escalations[0].fields, "child.id"),
        Some(format!("{rebuild_id:?}")),
        "the escalation names the incarnation that died in the hook",
    );
}

/// Card #226: a pipe whose mapper panics emits exactly one `error!` event
/// with the actor name, and the pipe task exits cleanly.
#[tokio::test]
async fn pipe_mapper_panic_emits_one_error_event() {
    let (store, _guard) = capture::install();

    struct Sink;
    #[derive(Debug)]
    struct M;
    impl Msg for M {}
    impl Mailboxed for Sink {
        type Msg = M;
    }
    impl Actor for Sink {
        type Args = ();
        type Error = Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(&mut self, _: M, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
    }

    let actor_ref = spawn_plain::<Sink>(());
    actor_ref.pipe_to_self(async { 1u32 }, |_res: Result<u32, PanicError>| {
        panic!("mapper boom")
    });

    // The mapper panic fires in a detached task; give it a bounded moment.
    timeout(terminate_bound(), async {
        loop {
            {
                let store = store.lock().unwrap();
                let events: Vec<_> = store
                    .events
                    .iter()
                    .filter(|e| {
                        field(&e.fields, "message").as_deref()
                            == Some("pipe_to_self mapper panicked; result dropped")
                    })
                    .collect();
                if events.len() == 1 {
                    assert_eq!(events[0].level, "ERROR");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the mapper-panic event must be emitted within the bound");
}

/// Card #226: a piped result that resolves after its target has stopped
/// emits exactly one `debug!` event and exits cleanly.
#[tokio::test]
async fn pipe_result_dropped_after_stop_emits_one_debug_event() {
    let (store, _guard) = capture::install();

    struct Sink;
    #[derive(Debug)]
    struct M;
    impl Msg for M {}
    impl Mailboxed for Sink {
        type Msg = M;
    }
    impl Actor for Sink {
        type Args = ();
        type Error = Infallible;
        async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(&mut self, _: M, _: ActorRef<Self>) -> Result<Flow, Self::Error> {
            Ok(Flow::Continue)
        }
    }

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let actor_ref = spawn_plain::<Sink>(());
    actor_ref.pipe_to_self(
        async move {
            let _ = gate_rx.await;
            1u32
        },
        |_res: Result<u32, PanicError>| M,
    );

    let weak = actor_ref.downgrade();
    drop(actor_ref);
    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actor stops once its last strong ref drops");

    gate_tx
        .send(())
        .expect("pipe future is waiting on the gate");

    timeout(terminate_bound(), async {
        loop {
            {
                let store = store.lock().unwrap();
                let events: Vec<_> = store
                    .events
                    .iter()
                    .filter(|e| {
                        field(&e.fields, "message").as_deref()
                            == Some("piped result arrived after stop; dropped")
                    })
                    .collect();
                if events.len() == 1 {
                    assert_eq!(events[0].level, "DEBUG");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the post-stop drop event must be emitted within the bound");
}

/// Card #223: cancelling a `send_after` timer before it fires emits exactly one
/// `trace!` event carrying the target id.
#[tokio::test(start_paused = true)]
async fn timer_cancel_before_fire_emits_trace_event() {
    let (store, _guard) = capture::install();
    let actor_ref = spawn_plain::<Probe>(());
    let id = actor_ref.id();
    let handle = actor_ref.send_after(Duration::from_secs(1), Ping);
    handle.cancel();

    timeout(terminate_bound(), async {
        loop {
            {
                let store = store.lock().unwrap();
                let events: Vec<_> = store
                    .events
                    .iter()
                    .filter(|e| {
                        field(&e.fields, "message").as_deref()
                            == Some("timer cancelled before fire")
                    })
                    .collect();
                if events.len() == 1 {
                    assert_eq!(events[0].level, "TRACE");
                    assert_eq!(
                        field(&events[0].fields, "target.id"),
                        Some(format!("{:?}", id)),
                        "the event names the target actor id",
                    );
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the timer-cancelled event must be emitted within the bound");
}

/// Card #223: a timer that fires after its target has stopped emits exactly one
/// `debug!` event carrying the target id.
#[tokio::test]
async fn timer_fire_at_dead_target_emits_debug_event() {
    let (store, _guard) = capture::install();
    let actor_ref = spawn_plain::<Probe>(());
    let id = actor_ref.id();
    let _handle = actor_ref.send_after(Duration::from_millis(10), Ping);
    let weak = actor_ref.downgrade();
    drop(actor_ref);

    timeout(terminate_bound(), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actor stops once its last strong ref drops");

    timeout(terminate_bound(), async {
        loop {
            {
                let store = store.lock().unwrap();
                let events: Vec<_> = store
                    .events
                    .iter()
                    .filter(|e| {
                        field(&e.fields, "message").as_deref()
                            == Some("timer fired after target stopped; message dropped")
                    })
                    .collect();
                if events.len() == 1 {
                    assert_eq!(events[0].level, "DEBUG");
                    assert_eq!(
                        field(&events[0].fields, "target.id"),
                        Some(format!("{:?}", id)),
                        "the event names the target actor id",
                    );
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the dead-target drop event must be emitted within the bound");
}

/// Card #223: a panicking `send_interval` message factory emits exactly one
/// `error!` event carrying the target id; the actor is untouched.
#[tokio::test(start_paused = true)]
async fn timer_interval_factory_panic_emits_error_event() {
    let (store, _guard) = capture::install();
    let actor_ref = spawn_plain::<Probe>(());
    let id = actor_ref.id();
    let _handle = actor_ref.send_interval(Duration::from_secs(1), || -> Ping {
        panic!("factory boom")
    });

    timeout(terminate_bound(), async {
        loop {
            {
                let store = store.lock().unwrap();
                let events: Vec<_> = store
                    .events
                    .iter()
                    .filter(|e| {
                        field(&e.fields, "message").as_deref()
                            == Some("send_interval message factory panicked; timer stopped")
                    })
                    .collect();
                if events.len() == 1 {
                    assert_eq!(events[0].level, "ERROR");
                    assert_eq!(
                        field(&events[0].fields, "target.id"),
                        Some(format!("{:?}", id)),
                        "the event names the target actor id",
                    );
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the factory-panic event must be emitted within the bound");

    assert!(
        actor_ref.is_alive(),
        "a factory panic must never touch the actor",
    );
}
