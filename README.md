# bombay

Fault-tolerant async actors on Tokio — a Zenoh-native rebuild of the [kameo](https://github.com/tqwewe/kameo) actor framework, pairing with [nexus](https://github.com/devrandom-labs/nexus) for event-sourced, single-writer aggregates. The core is transport- and domain-agnostic: an actor is a single-writer consistency boundary, whether ephemeral, stateful, or nexus-backed.

> **Status:** the local actor spine (the `bombay` crate, `crates/core/`) is rebuilt from scratch and works today; the Zenoh remote layer is under active development. The vendored kameo fork that used to live beside it is gone — upstream [kameo](https://github.com/tqwewe/kameo) (tag `v0.22.2`) serves as the read-only reference oracle. Process, roadmap, and engineering rules live in [`CLAUDE.md`](CLAUDE.md).

## Using bombay

An actor owns its state, declares one closed message enum, and handles messages one at a time. Replies travel on typed one-shot ports carried inside the message:

```rust
use bombay::{
    actor::{Actor, ActorRef, Spawn as _},
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
        _: &mut bool,
    ) -> Result<(), Self::Error> {
        match msg {
            CounterMsg::Inc(n) => self.count += i64::from(n),
            CounterMsg::Get { reply } => {
                let _ = reply.send(self.count);
            }
        }
        Ok(())
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

## The public API at a glance

- **Actor** — `Actor` (a `Mailboxed` subtrait, so the mailbox is keyed on the actor) with `on_start` / `handle` / `on_panic` / `on_stop`. Spawn via `Actor::spawn` or `spawn_with_capacity`, or build a `PreparedActor` to hand out its `ActorRef` and pre-send before the loop starts.
- **`ActorRef`** — two words, one shared allocation, so a clone is a single refcount bump. `tell` (fire-and-forget) and `ask` (request/reply) are builders: `.await` either one, give it a `.timeout(..)`, or resolve a `tell` without waiting via `.try_send()`. Plus `stop()` (graceful — the in-flight handler finishes), `kill()` (hard — no `on_stop`), `downgrade()` → `WeakActorRef`, and type-erased `Recipient` / `ReplyRecipient`. Dropping the last strong `ActorRef` stops the actor once its backlog drains.
- **Death-watch** — being watched is universal and passive; watching is the opt-in `Watch: Actor` supertrait with the `on_link_died` hook. Its default is OTP's rule: a **linked** *abnormal* death propagates, anything else is observed and the actor continues (override it to trap). Spawn a watcher with `spawn_linked`, then `watch` (one-directional, notify-only), `link` (bidirectional, propagating), or `unwatch`; each returns `Err(ActorNotLinked)` if the actor was not spawned linked. Death travels on its own unbounded channel and is fired from a task-owned guard's `Drop`, so no notice is lost to a full mailbox, a panic, or a hard kill.
- **Supervision** — an opt-in `Supervisor: Watch` supertrait spawned via `spawn_supervised`. Register a child with `supervise(config, factory)` under an **explicit** `RestartPolicy` (`Permanent` / `Transient` / `Never`) — there is no default policy: the surveyed systems (OTP, Kubernetes, Akka) disagree across the whole range, so the caller must state it (ADR-0012). A dead child is **rebuilt** from its factory, never resumed (a fresh actor with a new `ActorId`); restarts are spaced by exponential backoff + jitter and bounded by two counters — a consecutive-failure trip that resets on healthy uptime, and a never-reset lifetime budget. When a child fails past its budget, the supervisor stops with `RestartLimitExceeded` / `ChildLifecycleFailed`, which is itself a death notice to *its* watcher (the escalation ladder). Which siblings share a failed child's fate is the supervisor's `supervision_strategy()` (default `OneForOne` — the failed child alone): `RestForOne` also cycles the failed child's *younger* siblings, `OneForAll` the whole set — the ladder's coarser rungs, stopped crash-only and rebuilt in birth order without blocking the supervisor (ADR-0014). `stop_child` terminates a child gracefully (cancel → `stop_grace` → abort); `unsupervise` detaches one without stopping it. See ADR-0012 (accounting), ADR-0013 (lazy reactivation stays out of the core), ADR-0014 (set-cycle coordination).
- **Registry** — a process-local, lock-free-read name registry: register an actor under a name, look it up (weak handles — a registered actor can still die), remove it.
- **`ActorId`** — a process-local, unforgeable **pure-name** routing key (`bombay::ActorId`): minted at spawn and obtainable only from a spawned actor — no public constructor, no readable `u64` (no getter / `From` / `Display`), and deliberately **not** serializable. It names an actor for the mailbox, death-watch, and supervision *inside this process*; the dataspace identity of an actor is its future KERI AID (#121), a separate coordinate — never this handle. Holding an `ActorId` grants nothing; send-authority lives only in `ActorRef` (ADR-0015).
- **Mailbox** — bounded only: `Mailbox::<A>::bounded(capacity, id)`. Backpressure via `send`, fail-fast via `try_send`; a queued message keeps the actor alive until it is handled, and carries a one-word `SendContext` (the sender's span, for trace stitching).
- **Errors** — `TellError` and `AskError`, which classify retry-safety by method (`is_retryable` / `is_terminal`) and hand the undelivered message back; `PanicError` + `PanicReason`; and `ActorStopReason` (`Normal`, `Killed`, `Panicked`, `SupervisorRestart`, `LinkDied`, `AlreadyDead`, `RestartLimitExceeded`, `ChildLifecycleFailed`).

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
