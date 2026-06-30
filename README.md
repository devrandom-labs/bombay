# bombay

Fault-tolerant async actors on Tokio — a Zenoh-native fork of the [kameo](https://github.com/tqwewe/kameo) actor framework. Bombay keeps kameo's local actor core (single-writer message handling, supervision, links, a name registry) and is replacing its libp2p remote layer with a thin [Zenoh](https://zenoh.io) `Session` layer, pairing with [nexus](https://github.com/devrandom-labs/nexus) for event-sourced, single-writer aggregates.

> **Status:** the local actor core (forked from kameo 0.21) is in-tree and works today; the Zenoh remote layer and the nexus adapter are under active development. Until those land, the public API below *is* the kameo actor API. Process, roadmap, and engineering rules live in [`CLAUDE.md`](CLAUDE.md).

## Using bombay

Derive `Actor`, implement `Message<M>` for each message a type handles, spawn it, then `ask` (request/reply) or `tell` (fire-and-forget):

```rust
use bombay::prelude::*;

#[derive(Actor, Default)]
struct Counter {
    count: i64,
}

struct Inc(u32);

impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, Inc(n): Inc, _ctx: &mut Context<Self, Self::Reply>) -> i64 {
        self.count += n as i64;
        self.count
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let counter = Counter::spawn(Counter::default());

    let count = counter.ask(Inc(3)).await?; // request/reply -> 3
    counter.tell(Inc(50)).await?;           // fire-and-forget
    println!("count = {count}");

    Ok(())
}
```

### The public API at a glance

Everything below is re-exported from `bombay::prelude`:

- **Actor** — `#[derive(Actor)]` or `impl Actor` by hand. Lifecycle hooks: `on_start`, `on_panic`, `on_link_died`, `on_stop`. Spawn with `Actor::spawn`, `spawn_with_mailbox`, `spawn_in_thread`, or build one with `prepare` and run it later.
- **Messages** — `impl Message<M> for A { type Reply; async fn handle(&mut self, msg, ctx) -> Reply }`. The `Context` exposes the actor's own `ActorRef`, the reply channel (`reply_sender`), `forward`/`try_forward` to another actor, and `attach_stream` for `StreamMessage`.
- **ActorRef** — `ask` (request/reply) and `tell` (fire-and-forget), each a builder: `.mailbox_timeout(..)`, `.reply_timeout(..)`, `.send()` / `.try_send()` / `.blocking_send()`, `tell`'s `.send_after(..)`, `.forward(..)`, or `await` it directly (`IntoFuture`). Plus `downgrade()` → `WeakActorRef`, strong/weak reference counts, `link`/`unlink`, and type-erased `Recipient` / `ReplyRecipient`.
- **Reply** — any `Reply` type, including `Result<T, E>` and infallible scalars/collections. `ForwardedReply`, `DelegatedReply`, and a single-use `ReplySender` for replying out-of-band.
- **Supervision** — `RestartPolicy` (`Permanent` / `Transient` / `Never`), `SupervisionStrategy` (`OneForOne` / `OneForAll` / `RestForOne`), restart-intensity limits (max restarts within a sliding window), and death-watch via links + `on_link_died`.
- **Registry** — a process-local `ActorRegistry`: register an actor under a name, look it up, remove it.
- **Mailbox** — bounded (`mailbox::bounded(n)`) or unbounded (`mailbox::unbounded()`); backpressure via `send` vs fail-fast `try_send`.
- **Errors** — `SendError` (`ActorNotRunning` / `ActorStopped` / `MailboxFull` / `HandlerError` / `Timeout`), `PanicError`, `ActorStopReason`.

Runnable examples live in [`examples/`](examples/) — `basic`, `supervision`, `registry`, `stream`, `forward`, `pool`, `pubsub`, `broker`, `message_bus`, `message_queue`, and more. Run one with:

```bash
cargo run --example basic
```

## Building

Bombay builds on stable Rust (edition 2024, ≥ 1.85). The pinned toolchain lives in `rust-toolchain.toml`, so plain `rustup` and Nix resolve the same compiler.

```bash
nix develop                 # dev shell with the pinned toolchain (or use your own rustup stable)
cargo build
cargo run --example basic
```

## Running the tests

```bash
cargo nextest run                       # the whole workspace
cargo test --doc                        # doc-tests (nextest does not run these)
cargo test -p bombay_console            # one crate
cargo test --test core_actor_id_bdd     # one cucumber suite
```

Or run everything the CI gate runs in one shot:

```bash
nix flake check                         # build + clippy + fmt + audit + deny + nextest + doctest + actionlint
nix build .#coverage -L                 # llvm-cov HTML report -> ./result/html/index.html
```

Behaviour is captured as Gherkin `.feature` files under [`tests/features/`](tests/features/) and wired to the real code by cucumber runners in `tests/` and each crate's `tests/`. Coverage is produced by `cargo-llvm-cov` through `nix build .#coverage` (a `cargo-tarpaulin` engine is also wired as a Linux opt-in via `.#coverage-tarpaulin`); the per-file baseline and gap triage are in [`docs/testing/coverage-baseline.md`](docs/testing/coverage-baseline.md).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option, carrying kameo's upstream attribution (see [`NOTICE`](NOTICE)).
