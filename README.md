# bombay

Bombay is a statically typed runtime composition for pure actor behaviors.
It connects five independently owned primitives:

- `behavior` supplies the deterministic Agha-style behavior algebra;
- Bombay Communication supplies the bounded, two-lane mailbox;
- Bombay Address supplies exact-generation address ownership and routing;
- Bombay Observe and Bombay Timers supply completion and deadline primitives.

Bombay owns the composition: `System::spawn`, transactional
`System::activate`, actor execution, effect interpretation, child liveness,
delivery, cancellation, and ordered incarnation retirement. It does not own supervision policy, discovery registries,
request/reply, persistence, or remote transport.

The `bombay-framework` prelude is the application-facing entry point. The
canonical ceremony is:

```rust,ignore
use bombay_framework::prelude::*;

let system = System::new(MailboxConfig::bounded(32), router);
let behavior = Spec::new(state).stop_on_shutdown();
let actor = Actor::new(address, behavior);
let handle = system.spawn(actor)?;
handle.actor_ref().send(from, message).await?;
handle.actor_ref().request_shutdown()?;
let outcome = handle.outcome().await;
```

Run the complete typed example with:

```console
nix develop -c cargo run -p bombay-framework --example local_runtime
```

The reference application is
[`crates/bombay-framework/examples/local_runtime.rs`](crates/bombay-framework/examples/local_runtime.rs).
It composes typed delivery, child observation, an absolute timer, a
receive-timeout inactivity watchdog, stable-proxy restart, coordinated
shutdown, transitive retirement, and immediate root-tree address reuse in one
runnable program.

The bombay-native port of Bombay's at-least-once job queue is also runnable:

```console
nix develop -c cargo run -p bombay-framework --example job_queue
```

Its pure queue policy requeues an outstanding poison job from Behavior's
existing worker-stop event, then lets `Supervising` replace the worker behind
the same stable proxy. A typed priority shutdown enters a queue-level drain
phase: accepted work finishes, later work is returned as refused, and the
existing timer algebra returns work still outstanding at grace expiry as
abandoned. It uses no registry, framework request/reply API, or runtime drain
object.

## Typed effect composition

Heterogeneous behavior effects use Bombay Behavior's `SendProduct`. Select a
lane with semantic aliases over `Own` and `Inner<Path>`, then emit through
`SendAlgebra::send`; application code must not traverse `.inner`/`.own` or
implement product routing. Bombay's generic `RouteSends` recursion feeds each
product leaf into the destination behavior selected by `Delivery<B>`. The
current low-level routing adapters are compatibility infrastructure while E5
moves that wiring behind the runtime boundary.

Active architecture and cleanup work is tracked in
[`docs/open-design-ledger.md`](docs/open-design-ledger.md). Passing feature
tests means `feature-complete`, not done. The local-runtime V1 release audit
has now minimized its types, objects, interfaces, and ownership seams; later
roadmap slices retain their own feature and milestone distillation gates.

Current runtime boundaries and examples live in `docs/`; historical campaign
logs and the superseded Bombay implementation are intentionally not carried in
this repository.
