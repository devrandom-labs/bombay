# Examples

Root examples demonstrate only Bombay's public API. Internal mailbox, address,
observation, timer, Driver, incarnation, and interpreter tests belong inside
their owning crates.

`basic.rs` shows an ordinary `fn main`, a `Machine` composed with `OneShot`, a
named local actor-space product, and its handwritten `Hosts<P>` mapping. It
contains no handwritten `Behavior`. Bombay consumes the complete value through
`App::new(root, actors).run()`.

`supervision.rs` is the next composition layer: a root creates a dynamic
supervisor and a FIFO worker pool, starts dynamic membership, submits one
typed job, and uses message adapters for both reply directions. A `OneShot`
timer initiates a three-phase heterogeneous shutdown; the supervisor and pool
drain their own proxy/worker subtrees before the root advances. The application
exposes only an ordinary `fn main() -> Result<(), RunError>`; Bombay constructs
the Guardian, mailboxes, addresses, observations, timer queue, tasks, and
ownership fallback.

This layer still exposes one deliberate acceptance gap:
`Recipient::global(MailAddr(0))` is required to point the adapter back at the
automatically created Guardian. The pool worker likewise receives its typed
completion destination through `PoolAssignment`, while its factory still
configures the pool's established address. These remain visible so the example
does not pretend that current Behavior routing can express an
address-independent parent/root destination.
