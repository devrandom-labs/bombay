# Card #209 — `tracing` Feature Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the `tracing` feature into bombay-core: actor-lifecycle spans/events with structured fields, caller-span propagation through the mailbox, retire the 3 `eprintln!` placeholders — default-on, compile-out when disabled.

**Architecture:** All `#[cfg(feature = "tracing")]` lives in one new module `crates/core/src/trace.rs` with mirrored on/off halves (`imp` modules); call sites are cfg-free. One root `actor.lifecycle` span per actor (`follows_from` the spawn site, `stop.reason` recorded at teardown), one `actor.handle` span per message (parented to the caller's span captured at enqueue via a `SendContext` envelope field — ZST when off). Spec: `docs/superpowers/specs/2026-07-27-209-tracing-feature-design.md`.

**Tech Stack:** `tracing 0.1` (optional, `default-features = false, features = ["std"]` — no proc macro), `tracing-subscriber 0.3` (dev-only, capture layer), nix/crane gate check for the off-build.

**Branch:** `feat/209-tracing` (already exists, spec committed).

## Project ground rules (read first)

- All commands through the dev shell: `nix develop --command <cmd>`. Never raw cargo from a bare shell, never `nix run nixpkgs#`.
- `cargo fmt` before EVERY commit (fmt gate is strict). `git add` new files before `nix flake check` — untracked files make checks pass vacuously.
- No Claude/Anthropic attribution in commits/PRs. Conventional commits with scope.
- `cargo hakari` is NOT wired in bombay — skip it despite CLAUDE.md.
- New/changed bombay deps require a `fuzz/Cargo.lock` refresh or the flake gate breaks.
- New/renamed functions MUST get `mutants-baseline.json` entries (the gate fails on Unaccounted). The baseline has TWO path-keyed sections: the floors map AND the `known_zero_viable` LIST.
- clippy bar is deny-everything (`all`+`pedantic`+`nursery`+restriction). Expect to appease `must_use_candidate`, `missing_const_for_fn` (nursery), `unused_self`, `shadow_reuse`. Every `#[expect]` carries a `reason`.
- Integration tests: bound every await with `test_support::terminate_bound()` via `tokio::time::timeout` — an unbounded await turns a mutant into a sweep TIMEOUT.
- Don't name any test `prop_*` unless it is a proptest (MIRI sweep contract).

### Deviation from spec D2 (amended in Task 9)

Spec D2 said "cfg at each site kameo-style". During planning this was refined: the envelope field and the `impl Future` return types force cfg-carrying *types* anyway, so all cfg concentrates in `trace.rs` with mirrored on/off halves and call sites stay clean. Zero-cost-off is unchanged (off half = inert ZSTs + no-op `const fn`s that inline to nothing). Task 9 amends the spec to record this.

---

### Task 1: Dependency + feature wiring

**Files:**
- Modify: `Cargo.toml` (workspace root, `[workspace.dependencies]`)
- Modify: `crates/core/Cargo.toml`
- Modify: `fuzz/Cargo.lock` (regenerated)

- [ ] **Step 1.1: Add the workspace dep** in root `Cargo.toml` under `[workspace.dependencies]` (next to the other commented deps):

```toml
# Actor-lifecycle instrumentation (card #209): the library emits spans/events;
# subscribers (fmt, otel, console) are always downstream's choice. No default
# features: `attributes` (the #[instrument] proc macro + syn subtree) is unused
# — all spans here are manual.
tracing = { version = "0.1", default-features = false, features = ["std"] }
```

- [ ] **Step 1.2: Wire the feature** in `crates/core/Cargo.toml`. Replace the `[features]` section:

```toml
[features]
default = ["tracing"]
# Structured actor-lifecycle spans/events (card #209). Default-on like axum and
# upstream kameo — with no subscriber installed the per-event cost is one
# static-atomic interest check. Opt out with `default-features = false`; every
# call site then compiles out (gate check `bombay-tracing-off` proves it).
tracing = ["dep:tracing"]
test-support = []
```

Keep the existing `test-support` doc comment above the section. Add to `[dependencies]`:

```toml
tracing = { workspace = true, optional = true }
```

Add to `[dev-dependencies]` (tests build spans and install a capture subscriber):

```toml
tracing = { workspace = true }
tracing-subscriber = { version = "0.3", default-features = false, features = [
  "registry",
  "std",
] }
```

- [ ] **Step 1.3: Compile check**

Run: `nix develop --command cargo check -p bombay`
Expected: clean (feature exists, dep resolves, nothing uses it yet).

- [ ] **Step 1.4: Refresh the fuzz lockfile** (bombay's dep graph changed):

Run: `nix develop --command bash -c "cd fuzz && cargo update -p bombay"`
If that errors (package spec not found), run `nix develop --command bash -c "cd fuzz && cargo generate-lockfile"` instead and inspect the diff — only bombay-related lines should change.

- [ ] **Step 1.5: Format + commit**

```bash
nix develop --command cargo fmt
git add Cargo.toml crates/core/Cargo.toml fuzz/Cargo.lock
git commit -m "feat(trace): tracing feature wiring — optional dep, default-on [#209]"
```

---

### Task 2: Capture harness + first failing test (lifecycle span)

**Files:**
- Create: `crates/core/tests/tracing_capture.rs`

- [ ] **Step 2.1: Write the capture layer + the lifecycle test.** Full file:

```rust
//! Card #209: capture-subscriber assertions over the `tracing` feature's
//! lifecycle spans/events. The whole file is feature-gated — a
//! `--no-default-features` build has no surface to test.
#![cfg(feature = "tracing")]

use core::convert::Infallible;
use core::time::Duration;
use std::sync::{Arc, Mutex};

use bombay::{
    actor::{Actor, ActorRef, DEFAULT_MAILBOX_CAPACITY, PreparedActor, RunResult},
    error::ActorStopReason,
    mailbox::Mailboxed,
    message::Msg,
    test_support::terminate_bound,
};
use tokio::time::timeout;

mod capture {
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex};

    use tracing::{
        Event, Id, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Record},
    };
    use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt as _, registry::LookupSpan};

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
        fields.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
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
                .map(Id::into_u64);
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
                .map(Id::into_u64);
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
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Spec D5: one root `actor.lifecycle` span per actor carrying `actor.name` +
/// `actor.id` at creation and `stop.reason` recorded at teardown; the spawn
/// site is a `follows_from` link, never a parent.
#[tokio::test]
async fn lifecycle_span_carries_identity_and_records_stop_reason() {
    let (store, _guard) = capture::install();

    let spawn_site = tracing::info_span!("spawn_site");
    let spawn_site_id = spawn_site.id().expect("enabled by capture layer").into_u64();

    let prepared = PreparedActor::<Probe>::new(DEFAULT_MAILBOX_CAPACITY);
    let id = prepared.actor_ref().id();
    // Construct the run future INSIDE the spawn-site span: the lifecycle span
    // is created eagerly at `run()` call time, which is what `spawn()` relies
    // on to capture the spawn site.
    let run = spawn_site.in_scope(|| prepared.run(()));
    let result = timeout(terminate_bound(), run)
        .await
        .expect("actor must stop inside the bound");
    assert!(
        matches!(result, RunResult::Stopped { reason: ActorStopReason::Normal, .. }),
        "all-senders-gone normal stop, got {result:?}",
    );

    let store = store.lock().unwrap();
    let span = store.span("actor.lifecycle").expect("lifecycle span emitted");
    assert_eq!(
        field(&span.fields, "actor.name").as_deref(),
        Some(core::any::type_name::<Probe>()),
        "actor.name = A::name() as a structured field",
    );
    assert_eq!(
        field(&span.fields, "actor.id"),
        Some(format!("{id}")),
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
```

**Executor notes for this step (verify, don't assume):**
- `ActorId` Display: the current eprintln sites never print ids. Check `crates/core/src/id.rs` for `impl fmt::Display`. If absent, do NOT add one silently — use `format!("{id:?}")` in the test and `?id` (Debug) for the span field in Task 3, and note it in the PR body.
- `ActorStopReason` Display exists (the eprintln used `{reason}`).
- `test_support` import needs the `test-support` feature — already on for the test target via the self-dev-dep `bombay = { path = ".", features = ["test-support"] }`.
- If `tracing_subscriber::layer::SubscriberExt` path differs, it is `tracing_subscriber::prelude::*` — adjust imports, not the assertions.

- [ ] **Step 2.2: Run it — must FAIL (red)**

Run: `nix develop --command cargo test -p bombay --test tracing_capture`
Expected: FAIL at `expect("lifecycle span emitted")` — no instrumentation exists yet. If it fails to *compile* for API-name reasons, fix imports/names, not assertions.

- [ ] **Step 2.3: Commit the red test**

```bash
nix develop --command cargo fmt
git add crates/core/tests/tracing_capture.rs
git commit -m "test(trace): capture-layer harness + failing lifecycle-span test [#209]"
```

---

### Task 3: `trace` module + lifecycle span wiring (green)

**Files:**
- Create: `crates/core/src/trace.rs`
- Modify: `crates/core/src/lib.rs` (add `mod trace;` + `pub use trace::SendContext;` — match the existing private-mod + `pub use` style)
- Modify: `crates/core/src/actor/spawn.rs` (`PreparedActor::run` / `run_linked` / `run_supervised`, `start_actor`, `finish_actor`)

- [ ] **Step 3.1: Write `crates/core/src/trace.rs`:**

```rust
//! Actor-lifecycle instrumentation (card #209): the `tracing` feature's SINGLE
//! cfg surface. Call sites are cfg-free — this module swaps the real
//! implementation for inert no-ops when the feature is off, so an off build
//! compiles every span and event out (gate check `bombay-tracing-off`).
//!
//! Span model (spec D5/D6): one root `actor.lifecycle` span per actor that
//! `follows_from` the spawn site (the actor OUTLIVES its spawner — a link, not
//! a parent), with `stop.reason` recorded at teardown; one `actor.handle` span
//! per message, parented to the caller's span captured at enqueue.

#[cfg(feature = "tracing")]
mod imp {
    use core::future::Future;
    use core::time::Duration;

    use tracing::Instrument as _;

    use crate::{actor::Actor, error::ActorStopReason, error::PanicError, id::ActorId};

    pub(crate) use tracing::Span;

    /// Caller-side trace context captured at enqueue time: the sender's current
    /// span rides the mailbox envelope so the handler's span parents to it and
    /// cross-actor traces stitch into one tree. A ZST when `tracing` is off.
    pub struct SendContext {
        caller: tracing::Span,
    }

    impl SendContext {
        /// Captures the sender's current span (zero-cost with no subscriber).
        #[must_use]
        pub fn capture() -> Self {
            Self {
                caller: tracing::Span::current(),
            }
        }

        /// The per-message `actor.handle` span: parented to the captured caller
        /// span when enabled, else contextually to the lifecycle span.
        pub(crate) fn handle_span<A: Actor>(&self) -> Span {
            if self.caller.is_disabled() {
                tracing::debug_span!(
                    "actor.handle",
                    actor.name = A::name(),
                    msg.kind = core::any::type_name::<A::Msg>(),
                )
            } else {
                tracing::debug_span!(
                    parent: &self.caller,
                    "actor.handle",
                    actor.name = A::name(),
                    msg.kind = core::any::type_name::<A::Msg>(),
                )
            }
        }
    }

    /// The per-actor root span. `parent: None` is load-bearing: without it the
    /// span would contextually parent to the spawn site, nesting the actor's
    /// whole lifetime under it.
    pub(crate) fn lifecycle_span<A: Actor>(id: ActorId) -> Span {
        let span = tracing::info_span!(
            parent: None,
            "actor.lifecycle",
            actor.name = A::name(),
            actor.id = %id,
            stop.reason = tracing::field::Empty,
        );
        span.follows_from(tracing::Span::current().id());
        span
    }

    /// Attaches `span` to `fut`, entered per-poll (never across an await).
    pub(crate) fn instrument<F: Future>(fut: F, span: Span) -> tracing::instrument::Instrumented<F> {
        fut.instrument(span)
    }

    /// Records the terminal stop reason onto the current (lifecycle) span.
    pub(crate) fn record_stop_reason(reason: &ActorStopReason) {
        tracing::Span::current().record("stop.reason", tracing::field::display(reason));
    }

    pub(crate) fn spawned() {
        tracing::trace!("actor spawned");
    }

    pub(crate) fn on_start_ok() {
        tracing::trace!("actor started");
    }

    pub(crate) fn on_start_failed(err: &PanicError) {
        tracing::error!(?err, "on_start failed");
    }

    pub(crate) fn on_stop_ok(reason: &ActorStopReason) {
        tracing::trace!(%reason, "actor stopped");
    }

    pub(crate) fn on_stop_failed<E: core::fmt::Debug>(reason: &ActorStopReason, err: &E) {
        tracing::error!(%reason, ?err, "on_stop returned an error");
    }

    pub(crate) fn on_stop_panicked(reason: &ActorStopReason) {
        tracing::error!(%reason, "on_stop panicked");
    }

    pub(crate) fn on_stop_abandoned(reason: &ActorStopReason, grace: Duration) {
        tracing::error!(%reason, ?grace, "on_stop exceeded the notice grace and was abandoned");
    }

    pub(crate) fn handler_crashed(err: &PanicError) {
        tracing::error!(?err, "handler crashed");
    }

    pub(crate) fn restart_scheduled(child: ActorId, attempt: u32, delay: Duration) {
        tracing::warn!(child.id = %child, restart.attempt = attempt, restart.delay = ?delay, "child restart scheduled");
    }

    pub(crate) fn restart_gave_up(child: ActorId, rebuilds: u32) {
        tracing::error!(child.id = %child, restart.rebuilds = rebuilds, "restart budget exhausted, giving up");
    }

    pub(crate) fn child_escalated(child: ActorId) {
        tracing::error!(child.id = %child, "child lifecycle-hook failure escalated");
    }

    pub(crate) fn death_notice(watcher: ActorId, reason: &ActorStopReason, cleanup_failed: bool) {
        tracing::trace!(watcher.id = %watcher, %reason, cleanup_failed, "death notice delivered");
    }
}

#[cfg(not(feature = "tracing"))]
mod imp {
    use core::future::Future;
    use core::time::Duration;

    use crate::{actor::Actor, error::ActorStopReason, error::PanicError, id::ActorId};

    /// Inert stand-in for `tracing::Span` in an off build.
    pub(crate) struct Span;

    /// Inert stand-in: a ZST with the same API as the tracing-on version.
    pub struct SendContext;

    impl SendContext {
        #[must_use]
        pub const fn capture() -> Self {
            Self
        }

        #[expect(
            clippy::unused_self,
            reason = "mirrors the tracing-on API so call sites stay cfg-free"
        )]
        pub(crate) const fn handle_span<A: Actor>(&self) -> Span {
            Span
        }
    }

    pub(crate) const fn lifecycle_span<A: Actor>(_id: ActorId) -> Span {
        Span
    }

    pub(crate) const fn instrument<F: Future>(fut: F, _span: Span) -> F {
        fut
    }

    pub(crate) const fn record_stop_reason(_reason: &ActorStopReason) {}
    pub(crate) const fn spawned() {}
    pub(crate) const fn on_start_ok() {}
    pub(crate) const fn on_start_failed(_err: &PanicError) {}
    pub(crate) const fn on_stop_ok(_reason: &ActorStopReason) {}
    pub(crate) const fn on_stop_failed<E: core::fmt::Debug>(_reason: &ActorStopReason, _err: &E) {}
    pub(crate) const fn on_stop_panicked(_reason: &ActorStopReason) {}
    pub(crate) const fn on_stop_abandoned(_reason: &ActorStopReason, _grace: Duration) {}
    pub(crate) const fn handler_crashed(_err: &PanicError) {}
    pub(crate) const fn restart_scheduled(_child: ActorId, _attempt: u32, _delay: Duration) {}
    pub(crate) const fn restart_gave_up(_child: ActorId, _rebuilds: u32) {}
    pub(crate) const fn child_escalated(_child: ActorId) {}
    pub(crate) const fn death_notice(_watcher: ActorId, _reason: &ActorStopReason, _cleanup_failed: bool) {}
}

pub use imp::SendContext;
pub(crate) use imp::{
    Span, child_escalated, death_notice, handler_crashed, instrument, lifecycle_span, on_start_failed,
    on_start_ok, on_stop_abandoned, on_stop_failed, on_stop_ok, on_stop_panicked, record_stop_reason,
    restart_gave_up, restart_scheduled, spawned,
};
```

Clippy iteration is expected here (`missing_const_for_fn` may demand or reject `const` on some no-ops; `unused imports` if a helper lands in a later task — if the re-export list trips `unused`, trim it to what compiles NOW and extend it in the task that uses the rest). The `Span` re-export may be unused until Task 5 — same rule.

- [ ] **Step 3.2: Wire `lib.rs`.** Add `mod trace;` alongside the other modules and `pub use trace::SendContext;` in the existing re-export block. Check `crates/core/src/lib.rs` for the established style first.

- [ ] **Step 3.3: Instrument the three run entry points** in `crates/core/src/actor/spawn.rs`. All three change from `pub async fn` to `pub fn -> impl Future` so the lifecycle span is created **eagerly at call time** (that is what makes `spawn()` capture the spawn-site span — inside the spawned task, `Span::current()` would be empty). Keep each fn's existing doc comment, add one line noting the eager span. `Self` destructuring avoids partial-move issues:

```rust
    pub fn run(self, args: A::Args) -> impl Future<Output = RunResult<A>> {
        let Self {
            actor_ref,
            mailbox_rx,
            abort_registration,
        } = self;
        let span = crate::trace::lifecycle_span::<A>(actor_ref.id());
        crate::trace::instrument(
            async move {
                crate::trace::spawned();
                let lifecycle = run_lifecycle(args, actor_ref, mailbox_rx);
                Abortable::new(lifecycle, abort_registration)
                    .await
                    .unwrap_or(RunResult::Killed)
            },
            span,
        )
    }
```

Apply the same shape to `run_linked` (wraps `run_lifecycle_linked(args, actor_ref, mailbox_rx, link_rx)`) and `run_supervised` (wraps `run_lifecycle_supervised(...)`). Per project style, `use crate::trace;` at the top of the file and call `trace::lifecycle_span::<A>(...)` etc. — no inline paths.

- [ ] **Step 3.4: `on_start` events** in `start_actor` (spawn.rs:268). Rebind to avoid `shadow_reuse`:

```rust
    let state = match started {
        Ok(Ok(actor)) => actor,
        Ok(Err(err)) => {
            let panic_err = PanicError::new(Box::new(err), PanicReason::OnStart);
            trace::on_start_failed(&panic_err);
            return Err(panic_err);
        }
        Err(payload) => {
            let panic_err = PanicError::from_panic_any(payload, PanicReason::OnStart);
            trace::on_start_failed(&panic_err);
            return Err(panic_err);
        }
    };
    trace::on_start_ok();
```

- [ ] **Step 3.5: Record the stop reason** — first line of `finish_actor` (spawn.rs:399):

```rust
    trace::record_stop_reason(&reason);
```

- [ ] **Step 3.6: Run the Task-2 test — must PASS**

Run: `nix develop --command cargo test -p bombay --test tracing_capture`
Expected: PASS. Then the full suite: `nix develop --command cargo nextest run -p bombay` — the signature change must not break any existing caller (`.await` on `impl Future` is source-compatible; `tokio::spawn(prepared.run(...))` in tests keeps working because the future stays `Send`).

- [ ] **Step 3.7: Clippy + fmt + commit**

```bash
nix develop --command cargo clippy -p bombay --all-features
nix develop --command cargo fmt
git add crates/core/src/trace.rs crates/core/src/lib.rs crates/core/src/actor/spawn.rs
git commit -m "feat(trace): trace module + actor.lifecycle span, follows_from spawn site [#209]"
```

---

### Task 4: Retire the `eprintln!`s (on_stop outcome events)

**Files:**
- Modify: `crates/core/tests/tracing_capture.rs` (3 new tests)
- Modify: `crates/core/src/actor/spawn.rs` (`log_on_stop_outcome`, `log_on_stop_abandoned`)

- [ ] **Step 4.1: Write the 3 failing tests** in `tracing_capture.rs`. They need probe actors with failing `on_stop` hooks. Check `crates/core/src/error.rs` for how `ReplyError` is satisfied (existing tests use `Infallible`; a `#[derive(Debug)] struct StopErr;` works if `ReplyError` has a blanket impl over `Debug + Send + 'static` — verify, and if not, use whatever the existing failing-hook tests in `spawn.rs`'s test module use as an error type):

```rust
use bombay::actor::WeakActorRef;

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
    async fn handle(&mut self, _: Ping, _: ActorRef<Self>, _: &mut bool) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<(), Self::Error> {
        Err(StopErr)
    }
}

/// Spec D8: an `on_stop` error is an `error!` event with structured
/// `reason` + `err` fields — the eprintln replacement can actually fail.
#[tokio::test]
async fn on_stop_error_emits_one_error_event_with_fields() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<FailingStop>::new(DEFAULT_MAILBOX_CAPACITY);
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
```

Add the sibling tests the same way:
- `on_stop_panic_emits_one_error_event`: probe whose `on_stop` body is `panic!("boom")`; assert one ERROR event with `message == "on_stop panicked"` and the `reason` field. (A caught `on_stop` panic must NOT abort the test — the runtime catches it; if the existing suite sets a panic hook helper for quiet output, reuse it.)
- `on_stop_abandoned_emits_one_error_event`: `#[tokio::test(start_paused = true)]`, probe whose `on_stop` is `std::future::pending::<()>().await; Ok(())` (unreachable Ok for the type). The paused clock auto-advances past the 5 s `ON_STOP_NOTICE_GRACE`. Assert one ERROR event with `message == "on_stop exceeded the notice grace and was abandoned"` and fields `reason` + `grace`.

- [ ] **Step 4.2: Run — the 3 new tests FAIL** (`errors.len() == 0`; the diagnostics still go to stderr).

Run: `nix develop --command cargo test -p bombay --test tracing_capture`

- [ ] **Step 4.3: Replace the eprintlns.** In `spawn.rs`, rewrite `log_on_stop_outcome` (drop its `#[expect(clippy::print_stderr)]`, keep the doc comment's no-unwrap rationale):

```rust
fn log_on_stop_outcome<A: Actor>(
    reason: &ActorStopReason,
    stop_result: Result<Result<(), A::Error>, Box<dyn std::any::Any + Send>>,
) {
    match stop_result {
        Ok(Ok(())) => trace::on_stop_ok(reason),
        Ok(Err(err)) => trace::on_stop_failed(reason, &err),
        Err(_payload) => trace::on_stop_panicked(reason),
    }
}
```

Delete `log_on_stop_abandoned` entirely and replace its call site (finish_actor's `Err(_elapsed)` arm) with:

```rust
            trace::on_stop_abandoned(&reason, ON_STOP_NOTICE_GRACE);
```

Note `Ok(Ok(()))` now emits `on_stop_ok` (spec D7's trace-level "actor stopped") where it was silent — intended.

- [ ] **Step 4.4: Run — all tracing tests PASS; no `print_stderr` expects remain**

Run: `nix develop --command cargo test -p bombay --test tracing_capture && rg -n "print_stderr|eprintln" crates/core/src`
Expected: tests pass; rg finds nothing in production code.

- [ ] **Step 4.5: Full suite + commit**

```bash
nix develop --command cargo nextest run -p bombay
nix develop --command cargo fmt
git add crates/core/src/actor/spawn.rs crates/core/tests/tracing_capture.rs
git commit -m "feat(trace): on_stop outcome events retire the eprintln placeholders [#209]"
```

---

### Task 5: Caller-span propagation (`SendContext` envelope + `actor.handle` span)

**Files:**
- Modify: `crates/core/tests/tracing_capture.rs` (2 new tests)
- Modify: `crates/core/src/mailbox.rs` (`Signal::Message` + `send_message` + `try_send_message` + test constructions)
- Modify: `crates/core/src/actor/kind.rs` (`handle_mailbox_step` Message arm; `handler_crashed` event)

- [ ] **Step 5.1: Write the 2 failing tests.** Sending API: check `crates/core/src/request.rs` / `actor_ref.rs` for the #118 tell builder's exact names (`actor_ref.tell(msg)` returning a builder with a try/send path). Use whatever existing integration tests use to fire a plain tell.

```rust
/// Spec D6: the handler's `actor.handle` span parents to the SENDER's span
/// captured at enqueue — cross-actor traces stitch into one tree.
#[tokio::test]
async fn handle_span_parents_to_the_callers_span() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<Probe>::new(DEFAULT_MAILBOX_CAPACITY);
    let actor_ref = prepared.actor_ref().clone();

    let send_site = tracing::info_span!("send_site");
    let send_site_id = send_site.id().expect("enabled").into_u64();
    send_site.in_scope(|| {
        // Adjust to the real #118 tell API (e.g. `.tell(Ping).try_send()`).
        actor_ref.tell(Ping).try_send().expect("mailbox has capacity");
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
}

/// No caller span at enqueue → the handle span falls back to the contextual
/// parent, the actor's own lifecycle span.
#[tokio::test]
async fn handle_span_without_caller_parents_to_lifecycle() {
    let (store, _guard) = capture::install();
    let prepared = PreparedActor::<Probe>::new(DEFAULT_MAILBOX_CAPACITY);
    let actor_ref = prepared.actor_ref().clone();
    actor_ref.tell(Ping).try_send().expect("mailbox has capacity");
    drop(actor_ref);
    let _ = timeout(terminate_bound(), prepared.run(()))
        .await
        .expect("actor must stop inside the bound");

    let store = store.lock().unwrap();
    let lifecycle_id = store.span("actor.lifecycle").expect("lifecycle span").id;
    let handle = store.span("actor.handle").expect("handle span emitted");
    assert_eq!(handle.parent, Some(lifecycle_id));
}
```

Caveat for test 2: `set_default` is thread-wide, so the *test body itself* runs outside any span only if `#[tokio::test]` doesn't wrap it in one — it doesn't. `Span::current()` at the tell site is `none` → disabled → fallback path.

- [ ] **Step 5.2: Run — both FAIL** (no `actor.handle` span exists).

- [ ] **Step 5.3: Add the envelope field.** In `mailbox.rs`, extend the variant:

```rust
    Message {
        /// The domain message.
        msg: A::Msg,
        /// A strong clone of the enqueuing sender (the actor's own mailbox).
        self_sender: MailboxSender<A>,
        /// Caller-side trace context captured at enqueue (card #209): the
        /// sender's current span, so the handler's span parents to it. A ZST
        /// when the `tracing` feature is off.
        ctx: SendContext,
    },
```

Add `use crate::trace::SendContext;` to the file's imports. Update `send_message` (line ~248) and `try_send_message` (line ~265) to add `ctx: SendContext::capture(),` to their `Signal::Message` literals. Then sweep EVERY construction site in the WHOLE repo (memory rule: `cargo test --lib` misses examples + fuzz):

Run: `rg -n "Signal::Message \{" --glob '!target'`
Every *construction* gets `ctx: SendContext::capture(),`; every *pattern* with `..` is untouched; a pattern binding all fields explicitly gets `ctx: _` or `..`. Expect ~15 sites in `mailbox.rs` tests; check `fuzz/` and `crates/core/tests/` too. The `unreachable!` destructure at mailbox.rs:439-443 uses a pattern — extend with `..` if it lists fields exhaustively.

- [ ] **Step 5.4: Create + attach the handle span.** In `kind.rs` `handle_mailbox_step` (line ~187):

```rust
        Signal::Message { msg, self_sender, ctx } => {
            // (keep the existing comment block)
            let actor_ref = self_ref.upgrade().unwrap_or_else(|| {
                ActorRef::new(
                    self_ref.id(),
                    self_sender,
                    handles.cancel.clone(),
                    handles.abort.clone(),
                    None,
                )
            });
            let span = ctx.handle_span::<A>();
            trace::instrument(handle_message(state, actor_ref, self_ref, msg), span).await
        }
```

Add `use crate::trace;` to kind.rs imports.

- [ ] **Step 5.5: `handler_crashed` event.** In kind.rs find where `handle_message` turns a handler panic/`Err` into a `PanicError` (near line 880, `AssertUnwindSafe(state.handle(...))`). Immediately after the `PanicError` is constructed (both the panic-payload arm and the returned-`Err` arm if they construct separately), add `trace::handler_crashed(&err);` with the actual binding name at that site. The event fires inside the handle span (we are within the instrumented future).

- [ ] **Step 5.6: Run — both tests PASS; full suite + fuzz-workspace compile**

```bash
nix develop --command cargo test -p bombay --test tracing_capture
nix develop --command cargo nextest run -p bombay
nix develop --command bash -c "cd fuzz && cargo check"
```

- [ ] **Step 5.7: Commit**

```bash
nix develop --command cargo fmt
git add crates/core/src/mailbox.rs crates/core/src/actor/kind.rs crates/core/tests/tracing_capture.rs
git commit -m "feat(trace): SendContext caller-span propagation + actor.handle span [#209]"
```

---

### Task 6: Restart events (warn on schedule, error on give-up/escalate)

**Files:**
- Modify: `crates/core/tests/tracing_capture.rs` (1 test; a give-up test if cheap)
- Modify: `crates/core/src/actor/kind.rs` (`restart_or_give_up`, `handle_child_death`)

- [ ] **Step 6.1: Failing test.** A supervisor whose child crashes once. Copy the supervisor+child setup from an existing #196/#199 integration test (check `crates/core/tests/` for restart/supervision tests; if none are integration-level, adapt the public-API pattern: `SpawnSupervised::spawn_supervised` + `ActorRef::supervise` — check `actor_ref.rs` for the supervise verb's exact signature and `RestartConfig`'s public constructor in `restart.rs`). Use the seed seam for an exact delay:

```rust
use bombay::test_support::set_supervisor_rng_seed;
use bombay::restart::{jittered_backoff, RestartConfig};  // adjust paths to the real re-exports

/// Spec D7: a scheduled child restart is a `warn` event carrying exact
/// structured `restart.attempt` / `restart.delay` fields (seeded RNG).
#[tokio::test]
async fn scheduled_restart_emits_warn_with_attempt_and_delay() {
    const SEED: u64 = 7;
    set_supervisor_rng_seed(Some(SEED));
    let (store, _guard) = capture::install();

    // ... supervisor + always-crashing child, policy that restarts once ...
    // drive one crash, await the restart (existing tests show the pattern),
    // then stop the supervisor.

    let cfg = RestartConfig::default(); // match whatever the child was registered with
    let expected = jittered_backoff(&cfg, 1, &mut fastrand::Rng::with_seed(SEED));

    let store = store.lock().unwrap();
    let warns = store.events_at("WARN");
    assert_eq!(warns.len(), 1, "exactly one restart-scheduled warn, got {warns:?}");
    assert_eq!(field(&warns[0].fields, "restart.attempt").as_deref(), Some("1"));
    assert_eq!(
        field(&warns[0].fields, "restart.delay"),
        Some(format!("{expected:?}")),
        "seeded jitter makes the delay exact",
    );
}
```

`fastrand` is already a workspace dep — add `fastrand = { workspace = true }` to core's `[dev-dependencies]` if not present. Verify how `supervisor_rng` consumes the seed (spawn.rs `supervisor_rng`) — if it seeds `fastrand::Rng::with_seed(seed)` directly, the expected-value computation above is exact; if it derives differently, mirror that derivation. The `u32` field values arrive via the visitor as `"1"` (numeric record) — if the visitor records them through `record_debug`, `"1"` still holds; adjust only if the run shows a different rendering (e.g. add `record_u64`/`record_i64` to the visitor for exact numeric capture — preferred).

- [ ] **Step 6.2: Run — FAILS** (no WARN events exist anywhere yet).

- [ ] **Step 6.3: Emit the events.** In kind.rs `restart_or_give_up` (line ~698), restructure to expose `attempt` (rename the inner binding to avoid `shadow_reuse`):

```rust
    let delay = match child.tracker.record_failure(&child.config, Instant::now()) {
        GiveUp::Yes { rebuilds } => {
            trace::restart_gave_up(id, rebuilds);
            return ControlFlow::Break(ActorStopReason::RestartLimitExceeded {
                child: id,
                rebuilds,
            });
        }
        GiveUp::No { attempt } => {
            let backoff = jittered_backoff(&child.config, attempt, rng);
            trace::restart_scheduled(id, attempt, backoff);
            backoff
        }
    };
```

In `handle_child_death` (line ~590), the `Escalate` arm:

```rust
        RestartVerdict::Escalate => {
            trace::child_escalated(notice.id);
            ControlFlow::Break(ActorStopReason::ChildLifecycleFailed { child: notice.id })
        }
```

- [ ] **Step 6.4: Run — test PASSES; unit tests in kind.rs still green**

Run: `nix develop --command cargo nextest run -p bombay`

- [ ] **Step 6.5: Commit**

```bash
nix develop --command cargo fmt
git add crates/core/src/actor/kind.rs crates/core/tests/tracing_capture.rs crates/core/Cargo.toml
git commit -m "feat(trace): restart scheduled/give-up/escalate events [#209]"
```

---

### Task 7: Death-watch notice event

**Files:**
- Modify: `crates/core/tests/tracing_capture.rs` (1 test)
- Modify: `crates/core/src/watch.rs` (`Watchers::drop`)

- [ ] **Step 7.1: Failing test.** A linked watcher observing a plain target's death. Copy the watch pattern from an existing #195 integration test (`watcher.watch(&target)` — spawn.rs:3309 shows the verb). Sketch:

```rust
/// Spec D7: each delivered death notice is a `trace` event with the watcher id,
/// the stop reason, and the cleanup outcome as structured fields.
#[tokio::test]
async fn death_notice_delivery_emits_trace_event() {
    let (store, _guard) = capture::install();
    // spawn_linked a Watch probe; Spawn a plain Probe target;
    // watcher.watch(&target); stop the target; await the watcher observing it
    // (bound every await with terminate_bound()).
    // ...
    let store = store.lock().unwrap();
    let notices: Vec<_> = store
        .events
        .iter()
        .filter(|e| field(&e.fields, "message").as_deref() == Some("death notice delivered"))
        .collect();
    assert_eq!(notices.len(), 1, "one watcher, one notice");
    assert_eq!(notices[0].level, "TRACE");
    assert_eq!(field(&notices[0].fields, "watcher.id"), Some(format!("{watcher_id}")));
    assert_eq!(
        field(&notices[0].fields, "reason"),
        Some(ActorStopReason::Normal.to_string()),
    );
    assert_eq!(field(&notices[0].fields, "cleanup_failed").as_deref(), Some("false"));
}
```

- [ ] **Step 7.2: Run — FAILS.**

- [ ] **Step 7.3: Emit in `Watchers::drop`** (watch.rs, the `for edge in self.list.drain(..)` loop). The edge struct carries the watcher id (it is the `remove(id)` key — verify the field name in `watch.rs`):

```rust
        for edge in self.list.drain(..) {
            trace::death_notice(edge.watcher, &reason, cleanup_failed);
            let _ = edge.tx.try_send(LinkDied {
                id: me,
                reason: reason.clone(),
                linked: edge.linked,
                cleanup_failed,
            });
        }
```

Add `use crate::trace;` to watch.rs imports. The Drop runs inside `finish_actor` within the instrumented future, so the event lands in the dying actor's lifecycle span.

- [ ] **Step 7.4: Run — PASSES. Commit.**

```bash
nix develop --command cargo nextest run -p bombay
nix develop --command cargo fmt
git add crates/core/src/watch.rs crates/core/tests/tracing_capture.rs
git commit -m "feat(trace): death-notice delivery event [#209]"
```

---

### Task 8: Off-build gate (`bombay-tracing-off`)

**Files:**
- Modify: `flake.nix` (new entry in `checks`)

- [ ] **Step 8.1: Local off-build sanity first**

Run: `nix develop --command cargo check -p bombay --no-default-features`
Expected: clean compile (the off half of `trace.rs` covers every call site). Fix any hole now — this is the only compile of the off configuration.

- [ ] **Step 8.2: Add the check** to `flake.nix` `checks` (after `bombay-fuzz-replay`, following its `mkCargoDerivation` shape):

```nix
          # Card #209: the `tracing` feature must be zero-cost when off — an
          # opt-out build compiles clean AND carries no tracing crate in its
          # resolved normal-dep graph (spec D9's "events compile out" bullet,
          # made mechanical).
          bombay-tracing-off = craneLib.mkCargoDerivation (
            commonArgs
            // {
              inherit cargoArtifacts;
              pnameSuffix = "-tracing-off";
              buildPhaseCargoCommand = ''
                cargo check -p bombay --no-default-features
                if cargo tree -p bombay --no-default-features -e normal --prefix none \
                  | grep -q '^tracing '; then
                  echo 'tracing leaked into the --no-default-features dep graph' >&2
                  exit 1
                fi
              '';
              doInstallCargoArtifacts = false;
              doCheck = false;
            }
          );
```

- [ ] **Step 8.3: Prove the check can fail (falsifiability probe).** Temporarily make `tracing` non-optional (`tracing = { workspace = true }` in core's `[dependencies]`, feature list `tracing = []`), run only this check, expect RED; revert. Command:

Run: `git add flake.nix && nix build .#checks.aarch64-darwin.bombay-tracing-off -L` (adjust system string; `git add` first — flake sources the git tree)
Expected with the probe in place: FAIL with "tracing leaked". After revert: PASS, logging `building '...-tracing-off.drv'` (a silent result means cached, i.e. it did not run).

- [ ] **Step 8.4: Commit**

```bash
nix develop --command cargo fmt
git add flake.nix
git commit -m "check(flake): bombay-tracing-off gate — off build carries no tracing dep [#209]"
```

---

### Task 9: Docs — README, spec amendment, ADR, coverage baseline

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-27-209-tracing-feature-design.md` (D2 amendment)
- Create: ADR (locate the numbering: `rg -l "ADR-001" docs/` — follow the existing ADR file pattern, next number after ADR-0015)
- Modify: `docs/testing/coverage-baseline.md`

- [ ] **Step 9.1: README** (public API changed — feature flag + `SendContext` + eager-span `run`). Add a short "Observability" section: `tracing` default-on; what a subscriber sees (`actor.lifecycle` root span with `stop.reason`, `actor.handle` spans stitched to caller spans, error events for on_stop failures); otel via `tracing-opentelemetry` downstream; opt-out `default-features = false`; static stripping via tracing's `release_max_level_*`. Update the features/"public API at a glance" bullets. Keep the README's ~120-line budget — tighten elsewhere if needed.

- [ ] **Step 9.2: Spec D2 amendment.** In the spec, rewrite D2's heading/body to record the refinement: cfg concentrated in `trace.rs` on/off halves, call sites cfg-free; rationale (envelope field + `impl Future` returns force cfg-carrying types anyway); kameo-style per-site cfg rejected in execution.

- [ ] **Step 9.3: ADR.** One page: tracing-direct over observer-trait/log facade; default-on; `follows_from` spawn linkage; `SendContext` envelope propagation; zero-cost-off enforcement. Link the spec and card #209.

- [ ] **Step 9.4: Coverage baseline.** Add the `tracing_capture.rs` suite to `docs/testing/coverage-baseline.md` per its existing format.

- [ ] **Step 9.5: Commit**

```bash
git add README.md docs/
git commit -m "docs: observability section, ADR + spec amendment for tracing feature [#209]"
```

---

### Task 10: Mutants baseline, full gate, PR

- [ ] **Step 10.1: Mutants baseline entries.** New fns needing accounting: everything in `trace.rs` (both halves — only the compiled half generates mutants), `log_on_stop_outcome` (changed), the restructured `restart_or_give_up`. Run the scoped sweep to see real numbers (long — ~40 min; queue it):

Run: `git add -A && nix build .#mutants -L`
Then read `result/mutants-gate-report.txt` + `result/mutants.out`, and add entries to `mutants-baseline.json`: floors for fns whose event-emission mutants the capture tests catch; `known_zero_viable` for the off-half no-ops if the sweep compiles them (it runs with default features, so the off half should generate nothing — verify, don't assume). Both path-keyed sections must be updated for any touched file.

- [ ] **Step 10.2: Full gate**

```bash
nix develop --command cargo fmt
git add -A
git status   # nothing untracked — flake checks only see tracked files
nix flake check -L
```

Expected: all checks green including the new `bombay-tracing-off`. Fix and re-run until green. (MIRI is a scheduled lane, not in this gate.)

- [ ] **Step 10.3: Push + PR** (gh account `joeldsouzax`; HTTPS if SSH flakes):

```bash
gh auth status   # must show joeldsouzax active
git push -u origin feat/209-tracing
gh pr create --repo devrandom-labs/bombay --title "feat(trace): tracing feature — lifecycle spans/events, caller-span propagation [#209]" --body "<summary per template below>"
```

PR body must include: closes #209; the default-on amendment (card said default-off — decision 2026-07-27, axum/kameo parity, zero-cost opt-out enforced by `bombay-tracing-off`); the D2 refinement; any deferrals named explicitly (silence is not a deferral); NO attribution lines.

- [ ] **Step 10.4: Card close hygiene.** After merge: comment on #209 mapping every scope bullet to its shipped test/check (bullet 1 → `tracing_capture.rs` tests; bullet 2 → Task 4 commit, rg proof; bullet 3 → `bombay-tracing-off`; bullet 4 → field assertions), note the default-on amendment, close COMPLETED. Auto-merge is DISABLED on this repo — merge manually when `Nix Flake Check` is green. Remember: a red check in ~10-20s on a new branch is usually a GitHub 503 at flake-input eval — rerun before diagnosing.

---

## Self-review notes (done at planning time)

- **Spec coverage:** D1/D3 (Task 1), D2-as-amended (Tasks 3, 9), D4 (Task 1), D5 (Tasks 2-3), D6 (Task 5), D7 (Tasks 4, 6, 7), D8 (Task 4), D9 (Task 8), D10 (Task 9), testing section (Tasks 2, 4-7), mutants (Task 10).
- **Known verify-points for the executor** (facts checked where possible, flagged where not): `ActorId` Display impl (Task 2), `ReplyError` blanket impl (Task 4), #118 tell-builder method names (Task 5), `supervisor_rng` seed derivation + `RestartConfig`/`jittered_backoff` public paths (Task 6), watcher-edge field name (Task 7), crane `mkCargoDerivation` arg compatibility with `cargoArtifacts` (Task 8).
- **Type consistency:** `SendContext::capture()` / `handle_span::<A>()` / `trace::instrument(fut, span)` used identically in Tasks 3 and 5; `Span` is `tracing::Span` on, ZST off, both named `trace::Span`.
