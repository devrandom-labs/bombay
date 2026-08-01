# bombay

Fault-tolerant async actors on Tokio — a Zenoh-native rebuild of the [kameo](https://github.com/tqwewe/kameo) actor framework, pairing with [nexus](https://github.com/devrandom-labs/nexus) for event-sourced, single-writer aggregates. The core is transport- and domain-agnostic: an actor is a single-writer consistency boundary, whether ephemeral, stateful, or nexus-backed.

> **Status:** the local actor spine (the `bombay` crate, `crates/core/`) is rebuilt from scratch and works today; the Zenoh remote layer is under active development. The vendored kameo fork that used to live beside it is gone — upstream [kameo](https://github.com/tqwewe/kameo) (tag `v0.22.2`) serves as the read-only reference oracle. Process, roadmap, and engineering rules live in [`CLAUDE.md`](CLAUDE.md).

## Using bombay

An actor owns its state, declares one closed message enum, and handles messages one at a time. Replies travel on typed one-shot ports carried inside the message:

```rust
use bombay::{
    actor::{Actor, ActorRef, Flow, Spawn as _},
    mailbox::Mailboxed,
    message::Msg,
    reply::ReplySender,
};

struct Counter {
    count: i64,
}

#[derive(Debug)]
enum CounterMsg {
    Inc(u32),
    Get { reply: ReplySender<i64> },
}
impl Msg for CounterMsg {}

impl Mailboxed for Counter {
    type Msg = CounterMsg;
}

impl Actor for Counter {
    type Args = ();
    type Error = std::convert::Infallible;

    async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(Self { count: 0 })
    }

    async fn handle(
        &mut self,
        msg: CounterMsg,
        _: ActorRef<Self>,
    ) -> Result<Flow, Self::Error> {
        match msg {
            CounterMsg::Inc(n) => self.count += i64::from(n),
            CounterMsg::Get { reply } => {
                let _ = reply.send(self.count);
            }
        }
        Ok(Flow::Continue)
    }
}

#[tokio::main]
async fn main() {
    let counter = Counter::spawn(());
    counter.tell(CounterMsg::Inc(3)).await.expect("delivered");
    let count = counter
        .ask(|reply| CounterMsg::Get { reply })
        .await
        .expect("reply");
    assert_eq!(count, 3);
}
```

`#[derive(bombay_macros::Msg)]` derives the marker trait with a compile-time slot-size tripwire: a message enum whose `size_of` exceeds its budget (default 256 B) fails the build — box the fat variant or raise it with `#[msg(budget = N)]`.

For the whole spine composed — supervised workers, crash-and-rebuild, re-queued
jobs, name lookup, timers, pipes, and a drained shutdown — run the flagship
example: `cargo run -p bombay --example job_queue` (source:
[`crates/core/examples/job_queue/`](crates/core/examples/job_queue/), gate test:
[`crates/core/tests/app_job_queue.rs`](crates/core/tests/app_job_queue.rs)).

## The public API at a glance

- **Actor** — the ONE user trait is `caps::Actor` (`init` / `handle(msg, cx)` + defaulted `on_panic` / `on_stop`); everything else — deferral, watching, supervising — is a capability TYPE plugged into `type Caps`. `handle` returns its continuation decision: `Ok(Flow::Continue)` keeps the actor running, `Ok(Flow::Stop)` stops it cleanly (reason `Normal`) after the current message, and a returned `Err` is a controlled crash. Spawn via the ONE `caps::spawn` / `caps::spawn_with(SpawnConfig { capacity, on_stop_grace }, args)` — the run-loop shape (plain / linked / supervised) is selected from `Caps` at **compile time**, monomorphized, no runtime branch and no per-shape spawn verbs (ADR-0026 stage 3). The expert floor remains: the runtime `Actor` trait (`Mailboxed` subtrait) plus `PreparedActor::new(SpawnConfig { .. })` to hand out an `ActorRef` and pre-send before the loop starts.
- **`ActorRef`** — two words, one shared allocation, so a clone is a single refcount bump. `tell` (fire-and-forget) and `ask` (request/reply) are builders: `.await` either one, give it a `.timeout(..)`, or resolve a `tell` without waiting via `.try_send()`. Plus `stop()` (graceful — the in-flight handler finishes), `kill()` (hard — no `on_stop`), `downgrade()` → `WeakActorRef`, and type-erased `Recipient` / `ReplyRecipient`. Dropping the last strong `ActorRef` stops the actor once its backlog drains.
- **Pipe, don't block** — `pipe_to_self(future, mapper)` runs any future off-turn and re-enters its result as an ordinary message (panic surfaced typed, in-flight pipes never pin the actor); `pipe_ask(target, make_msg, mapper)` is the ask-shaped sugar with the whole failure union flattened to one `PipeAskError` match.
- **Timers** — `send_after(delay, msg)` and `send_interval(period, make_msg)` on both `ActorRef` and type-erased `Recipient`; both return a `TimerHandle` whose `cancel()` is idempotent and explicit (dropping detaches, the timer still fires). Fired messages are ordinary menu messages through the bounded mailbox, so backpressure is preserved, and armed timers hold a weak handle so they never pin the actor.
- **Death-watch** — being watched is universal and passive; watching is the `caps::Watching<WP>` capability: plug it into your cap set and the actor runs on the linked loop with the `watch` / `link` / `unwatch` verbs on its handle (compile-gated — a plain actor's handle simply has no `watch`). The reaction is the plugged `WatchPolicy` — chosen by NAME, never inherited: ship-provided `OtpPropagation` is OTP's rule (a **linked** *abnormal* death propagates, anything else is observed), or write your own policy reacting through `&mut actor`. Death travels on its own unbounded channel and is fired from a task-owned guard's `Drop`, so no notice is lost to a full mailbox, a panic, or a hard kill.
- **Supervision** — the `caps::Supervising<SS>` capability, which **requires** `Watching` in the same cap set (an invalid stack does not compile — the composition law rides a supertrait, and the derive rejects it with a readable error). The restart-set strategy `SS` is a TYPE (`caps::OneForOne` / `RestForOne` / `OneForAll`) with **no default** — like the per-child policy, it is required by construction. Register a child with `supervise(config, factory)` under an **explicit** `RestartPolicy` (`Permanent` / `Transient` / `Never`) — no default policy either: the surveyed systems (OTP, Kubernetes, Akka) disagree across the whole range, so the caller must state it (ADR-0012). A dead child is **rebuilt** from its factory, never resumed (a fresh actor with a new `ActorId`); restarts are spaced by exponential backoff + jitter and bounded by two counters — a consecutive-failure trip that resets on healthy uptime, and a never-reset lifetime budget. A child that ref-count-collects is reported as `ActorStopReason::Collected` and left dead under every policy — collection is not failure (ADR-0020). When a child fails past its budget or in a lifecycle hook, the supervisor stops with `RestartLimitExceeded` / `ChildLifecycleFailed`, which is itself a death notice to *its* watcher (the escalation ladder). `RestForOne` also cycles the failed child's *younger* siblings, `OneForAll` the whole set — the ladder's coarser rungs, stopped crash-only and rebuilt in birth order without blocking the supervisor (ADR-0014). `stop_child` terminates a child gracefully (cancel → `stop_grace` → abort); `unsupervise` detaches one without stopping it. A supervisor's own exit — graceful stop, hard kill, or escalation — tears down every remaining supervised child (cancel → bounded join → abort), so children never outlive their supervisor. Because capabilities compose, a supervisor can also carry a `Stashing` cap — the deferring supervisor the old trait tiers could not express. See ADR-0012, ADR-0014, ADR-0019, ADR-0026.
- **Registry** — a process-local, lock-free-read name registry: register an actor under a name, look it up (weak handles — a registered actor can still die), remove it.
- **`ActorId`** — a process-local, unforgeable **pure-name** routing key (`bombay::ActorId`): minted at spawn and obtainable only from a spawned actor — no public constructor, no readable `u64` (no getter / `From` / `Display`), and deliberately **not** serializable. It names an actor for the mailbox, death-watch, and supervision *inside this process*; the dataspace identity of an actor is its future KERI AID (#121), a separate coordinate — never this handle. Holding an `ActorId` grants nothing; send-authority lives only in `ActorRef` (ADR-0015).
- **Mailbox** — bounded only: `Mailbox::<A>::bounded(capacity, id)`. Backpressure via `send`, fail-fast via `try_send`; a queued message keeps the actor alive until it is handled, and carries a one-word `SendContext` (the sender's span, for trace stitching).
- **Bounded stash** — the first real capability (ADR-0026 stage 2): defer
  messages your current state can't accept. Put a `caps::Stashing<Msg>` in your
  `caps::Actor`'s `Caps` set (a `#[derive(Provide)]` struct whose capacity
  comes from a required `StashPolicy`), then
  `cx.cap::<Stashing<Msg>>().stash(msg)` / `.unstash_all()`. The loop replays
  released messages in-step, ahead of the mailbox backlog, in arrival order;
  overflow hands the message back (`StashFull`); a stashed message never keeps
  a dying actor alive. The replay wiring is derive-emitted — automatic and
  impossible to forget (ADR-0022 semantics on the caps surface).
- **Capabilities (staged)** — the distilled surface (ADR-0026): implement
  `caps::Actor` — one trait (`init` / `handle(msg, cx)`), with everything
  else plugged in as capability TYPES on `type Caps` (`()` for a plain
  actor) and reached through the compile-gated `cx.cap::<C>()` (a
  capability your set doesn't declare is a compile error, never a runtime
  check). Cap-set structs get their per-field access impls from
  `#[derive(bombay_macros::Provide)]`; any crate can define its own
  capability. Spawn via `caps::spawn` / `caps::spawn_with`:

  ```rust
  struct Audit { entries: u64 }
  impl caps::Actor for Audit {
      type Msg = AuditMsg; type Args = (); type Error = Infallible;
      type Caps = ();   // plain — no capability ceremony
      async fn init((): (), _: caps::Ctx<'_, Self>) -> Result<Self, Infallible> {
          Ok(Self { entries: 0 })
      }
      async fn handle(&mut self, msg: AuditMsg, _: caps::Ctx<'_, Self>)
          -> Result<Flow, Infallible> { /* … */ Ok(Flow::Continue) }
  }
  let audit = caps::spawn::<Audit>(());
  ```

  Stage 3 (ADR-0026) made this THE surface: the former `Watch` /
  `Supervisor` trait tiers and the six `Spawn*` verbs are gone — watching
  and supervising are the `Watching<WP>` / `Supervising<SS>` capability
  types above, and the one `caps::spawn` picks the loop shape from the cap
  set at compile time.
- **Deadlines** — the `caps::Deadlined<DP>` capability (ADR-0025's
  declarative plane): the plugged `DeadlinePolicy` declares
  `next_deadline(&actor) -> Option<Instant>` as a **pure function of actor
  state** — no arm/cancel verbs, nothing to forget, nothing to race — and
  the run loop re-reads it every iteration (all three loop shapes), firing
  `on_deadline(&mut actor, WeakActorRef)` once per value at a turn
  boundary, under the same catch/crash treatment as a handler
  (`PanicReason::OnDeadline`, restart-eligible). A due deadline preempts
  the mailbox backlog but never a ready death notice; a disabled slot
  costs nothing (armed, ~3% per-message throughput). Sliding idle timers
  are one policy away: declare `last_activity + T`.
- **Phases** — the `caps::Phased<P>` capability: a `PhasePolicy` is the
  whole machine as ONE plugged unit — phase tag enum, declarative
  per-phase admission (`gate → Deliver | Defer | Ignore`, P-style: the
  handler never sees a message its phase declared away), per-phase
  deadlines (`phase_deadline(&self, phase)`, magnitudes from your spawn
  args), and the required timeout reaction. Transition with
  `cx.cap::<Phased<P>>().goto(next)` — committed only after your handler
  returns `Ok`, releasing the embedded stash so deferred messages replay
  re-gated in the NEW phase, ahead of the backlog; a left phase's deadline
  is *unrepresentable*, not filtered (no epochs, no timer tasks, zero
  allocations on the transition path). `Phased` embeds its own stash and
  deadline seat — plugging `Stashing`/`Deadlined` beside it is rejected at
  derive time.
- **Errors** — `TellError` and `AskError`, which classify retry-safety by method (`is_retryable` / `is_terminal`) and hand the undelivered message back; `PanicError` + `PanicReason`; and `ActorStopReason` (`Normal`, `Collected`, `Killed`, `Panicked`, `SupervisorRestart`, `LinkDied`, `AlreadyDead`, `RestartLimitExceeded`, `ChildLifecycleFailed`).

## Observability

The `tracing` feature is on by default: hook up any [`tracing`](https://docs.rs/tracing) subscriber (fmt logs, `tracing-opentelemetry` for OpenTelemetry export, …) — bombay only emits, it never installs one. What a subscriber sees:

- a root **`actor.lifecycle`** span per actor (`actor.name`, `actor.id`), linked `follows_from` its spawn site and recording `stop.reason` at teardown (a hard kill or startup failure skips teardown, closing the span with the field empty);
- a per-message **`actor.handle`** span parented to the sender's span captured at enqueue, so cross-actor traces stitch into one tree;
- `error!` events for lifecycle failures (`on_start` failure, handler crash, `on_stop` error/panic/abandonment, restart budget exhausted) and a `warn!` for each scheduled child restart.

With no subscriber the per-call-site cost is one static-atomic interest check and sends allocate nothing. Opt out with `default-features = false` — every span and event compiles out, and a dedicated gate check proves the `tracing` crate leaves the dependency graph. To keep the feature but strip levels statically, enable `tracing`'s `release_max_level_*` features in your own build.

## Building

Bombay builds on stable Rust (edition 2024, ≥ 1.85). The pinned toolchain lives in `rust-toolchain.toml`, so plain `rustup` and Nix resolve the same compiler.

```bash
nix develop                 # dev shell with the pinned toolchain (or use your own rustup stable)
cargo build
```

## Running the tests

```bash
cargo nextest run                       # the whole workspace
cargo test --doc                        # doc-tests (nextest does not run these)
```

Or run everything the CI gate runs in one shot:

```bash
nix flake check                         # build + clippy + fmt + audit + deny + nextest + doctest + fuzz replay + lints
nix build .#coverage -L                 # llvm-cov HTML report -> ./result/html/index.html
```

Coverage is produced by `cargo-llvm-cov` through `nix build .#coverage` (a `cargo-tarpaulin` engine is also wired as a Linux opt-in via `.#coverage-tarpaulin`), and a standing mutation gate runs through `nix build .#mutants` (nightly `mutants.yml`); the per-file baseline and gap triage for both are in [`docs/testing/coverage-baseline.md`](docs/testing/coverage-baseline.md).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option, carrying kameo's upstream attribution (see [`NOTICE`](NOTICE)).
