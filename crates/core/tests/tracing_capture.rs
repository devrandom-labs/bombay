//! Card #209: capture-subscriber assertions over the `tracing` feature's
//! lifecycle spans/events. The whole file is feature-gated — a
//! `--no-default-features` build has no surface to test.
#![cfg(feature = "tracing")]

use core::convert::Infallible;

use core::time::Duration;
use std::sync::{Arc, Mutex};

use bombay::{
    actor::{
        Actor, ActorRef, DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult, Spawn, SpawnLinked,
        SpawnSupervised, Supervisor, Watch, WeakActorRef,
    },
    error::ActorStopReason,
    mailbox::{Capacity, Mailboxed},
    message::Msg,
    restart::{RestartConfig, RestartPolicy, SupervisionStrategy, jittered_backoff},
    test_support::{set_supervisor_rng_seed, terminate_bound},
};
use tokio::{sync::mpsc, time::timeout};

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

use capture::field;

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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
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

    let prepared = PreparedActor::<Probe>::new(default_cap());
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
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "all-senders-gone normal stop, got {result:?}",
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
        Some(ActorStopReason::Normal.to_string()),
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
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
    let prepared = PreparedActor::<FailingStop>::new(default_cap());
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
        Some(ActorStopReason::Normal.to_string()),
        "reason is a structured field, not formatted into the message",
    );
    assert_eq!(field(&errors[0].fields, "err").as_deref(), Some("StopErr"));
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
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
    let prepared = PreparedActor::<PanickingStop>::new(default_cap());
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
        Some(ActorStopReason::Normal.to_string()),
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
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
    let prepared = PreparedActor::<Probe>::new(default_cap());
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
    let prepared = PreparedActor::<Probe>::new(default_cap());
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
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Err(StopErr)
    }
}

/// A handler's returned `Err` (controlled crash) is an `error!` event that
/// fires INSIDE the message's `actor.handle` span.
#[tokio::test]
async fn handler_crash_emits_one_error_event_inside_the_handle_span() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<CrashingHandle>::new(default_cap());
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
    let prepared = PreparedActor::<HangingStop>::new(default_cap());
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
        Some(ActorStopReason::Normal.to_string()),
        "reason is a structured field, not formatted into the message",
    );
    assert_eq!(
        field(&errors[0].fields, "grace"),
        Some(format!("{:?}", Duration::from_secs(5))),
        "grace rides the event as a structured field",
    );
}

/// An idle watcher: it observes deaths (default OTP hook) and is never messaged.
struct Watcher;
impl Mailboxed for Watcher {
    type Msg = Ping;
}
impl Actor for Watcher {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Watcher)
    }
    async fn handle(
        &mut self,
        _: Ping,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl Watch for Watcher {}

/// Card #209 Task 7: delivering a death notice to a watcher is a `trace!` event
/// carrying the watcher's id, the stop reason, and the cleanup outcome —
/// emitted once per edge, from the dying actor's teardown.
#[tokio::test]
async fn death_notice_delivery_emits_one_trace_event_per_edge() {
    let (store, _guard) = capture::install();

    let watcher = Watcher::spawn_linked(());
    let watcher_id = watcher.id();

    let prepared = PreparedActor::<Probe>::new(default_cap());
    let target_ref = prepared.actor_ref().clone();
    watcher
        .watch(&target_ref)
        .await
        .expect("a linked watcher can watch");
    drop(target_ref);

    // All senders gone => Normal stop; the notice fires in the target's
    // teardown, strictly BEFORE its RunResult resolves.
    let result = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("target must stop inside the bound");
    assert!(
        matches!(
            result,
            RunResult::Stopped {
                reason: ActorStopReason::Normal,
                ..
            }
        ),
        "all-senders-gone normal stop, got {result:?}",
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
        Some(ActorStopReason::Normal.to_string()),
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
impl Mailboxed for Sup {
    type Msg = SupMsg;
}
impl Actor for Sup {
    type Args = ();
    type Error = Infallible;
    async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Sup)
    }
    async fn handle(
        &mut self,
        _: SupMsg,
        _: ActorRef<Self>,
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl Watch for Sup {}
impl Supervisor for Sup {
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::OneForOne
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
    let sup = Sup::spawn_supervised(());
    let (births_tx, mut births_rx) = mpsc::unbounded_channel::<()>();
    // Anchors every incarnation: an unanchored rebuild would ref-count-stop and
    // (under `Permanent`) churn through further restarts, each with its own warn.
    let anchors: Arc<Mutex<Vec<ActorRef<CrashingHandle>>>> = Arc::new(Mutex::new(Vec::new()));
    let factory_anchors = Arc::clone(&anchors);
    let child_id = timeout(
        terminate_bound(),
        sup.supervise(cfg, move || {
            let child = CrashingHandle::spawn(());
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
