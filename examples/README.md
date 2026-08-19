# Examples

Root examples demonstrate only Bombay's public API. Internal mailbox, address,
observation, timer, Driver, incarnation, and interpreter tests belong inside
their owning crates.

`basic.rs` shows an ordinary `fn main`, a `Machine` composed with `OneShot`, a
named local actor-space product, and its handwritten `Hosts<P>` mapping. It
contains no handwritten `Behavior`. Bombay consumes the complete value through
`App::new(root, actors).run()`.

`supervision.rs` is the next composition layer: a root creates a
path-aware `DynamicSupervisorWithParent`, its shutdown-capable proxy/worker
subtree, a typed `MessageAdapter`, and a `OneShot` timer actor. The parent
ingress path is compile-time evidence for proxy reports; the proxy's hosted
public protocol remains stable and path-independent. The application exposes
only an ordinary `fn main() -> Result<(), RunError>`; Bombay constructs the Guardian,
mailboxes, addresses, observations, timer queue, tasks, and ownership fallback.

This layer still exposes one deliberate acceptance gap:
`Recipient::global(MailAddr(0))` is required to point the adapter back at the
automatically created Guardian. It remains visible so the example does not
pretend that current Behavior routing can express an address-independent
parent/root destination. The Akka-IoT reference application is blocked on that
upstream destination contract rather than hiding it behind Bombay plumbing.
