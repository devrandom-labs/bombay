# Examples

Root examples demonstrate only Bombay's public API. Internal mailbox, address,
observation, timer, Driver, incarnation, and interpreter tests belong inside
their owning crates.

`basic.rs` shows the public `#[bombay::main]` entry over an explicit Behavior
and closed local-hosting manifest. The same application can call `run(Basic)`
directly. See
[`../docs/open-design-ledger.md`](../docs/open-design-ledger.md) before treating
its manifest or generated topology details as stable.

`supervision.rs` is the next composition layer: a root creates a
path-aware `DynamicSupervisorWithParent`, its shutdown-capable proxy/worker
subtree, a typed `MessageAdapter`, and a `OneShot` timer actor. The parent
ingress path is compile-time evidence for proxy reports; the proxy's hosted
public protocol remains stable and path-independent. The application still exposes only
`#[bombay::main] fn main() { System }`; Bombay constructs the Guardian,
mailboxes, addresses, observations, timer queue, tasks, and ownership fallback.

This layer still exposes one deliberate acceptance gap:
`Recipient::global(MailAddr(0))` is required to point the adapter back at the
automatically created Guardian. It remains visible so the example does not
pretend that current Behavior routing can express an address-independent
parent/root destination. The Akka-IoT reference application is blocked on that
upstream destination contract rather than hiding it behind Bombay plumbing.
