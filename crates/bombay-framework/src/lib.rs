//! Application-facing composition for the bombay local runtime.
//!
//! This crate adds no runtime machinery. Its prelude selects the public pieces
//! needed to author and run local actors while Bombay Behavior and bombay
//! retain ownership of their respective algebra and runtime contracts.

/// Construct the existing local [`bombay::System`] with named semantic
/// inputs while preserving the concrete router type.
///
/// This macro expands directly to [`bombay::System::new`]; it creates no
/// alternate runtime, spawn path, task, registry, or policy object.
///
/// ```
/// use bombay_framework::{local_system, prelude::*};
///
/// let _: System<AddressRouter<MailAddr, ()>> = local_system!(
///     mailbox = MailboxConfig::bounded(8),
///     routes = AddressRouter::default(),
/// );
/// ```
///
/// Named inputs intentionally reject misspellings at the authoring boundary:
///
/// ```compile_fail
/// use bombay_framework::{local_system, prelude::*};
///
/// let _ = local_system!(
///     capacity = MailboxConfig::bounded(8),
///     routes = AddressRouter::<MailAddr, ()>::default(),
/// );
/// ```
#[macro_export]
macro_rules! local_system {
    (mailbox = $mailbox:expr, routes = $routes:expr $(,)?) => {
        $crate::prelude::System::new($mailbox, $routes)
    };
}

/// The local application authoring surface.
pub mod prelude {
    pub use crate::local_system;
    pub use bombay::behavior;
    pub use bombay::behavior::{
        Actions, Address, Behavior, BehaviorFn, Births, Compose, Crash, Create, Deadline, Delivery,
        Exit, Handler, MailAddr, Never, NoBirths, Proxy, ProxyCommand, Pure, ReceiveTimeout,
        Recipient, RestartPolicy, SendProduct, ServiceSends, Step, StopOnShutdown, Strategy,
        SupervisionEvent, Supervisor, User, Watch, WorkerStopped,
    };
    pub use bombay::{
        ActorRef, AddressInUse, AddressRouter, DeliveryRouter, EndpointRegistry,
        IncarnationEndpoint, MailboxAnchor, MailboxConfig, RunExit, System, TaskOutcome,
    };
}

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn prelude_selects_the_existing_runtime_types() {
        fn same_type<T>(_: &T, _: &T) {}

        let direct: System<AddressRouter<MailAddr, ()>> =
            System::new(MailboxConfig::bounded(8), AddressRouter::default());
        let facade: System<AddressRouter<MailAddr, ()>> = local_system!(
            mailbox = MailboxConfig::bounded(8),
            routes = AddressRouter::default(),
        );
        same_type(&direct, &facade);
    }
}
