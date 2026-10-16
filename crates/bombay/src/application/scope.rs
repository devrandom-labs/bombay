use crate::address::{ApplicationAddresses, MailAddr};
use crate::application::{Application, ExternalActor, ExternalActorError, MailboxCapacity};
use crate::launch::ActorSpace;
use behavior::{Behavior, Protocol};
use core::fmt;

/// Build an application after establishing the endpoints its root needs.
///
/// Establish external endpoints here when pure root construction needs their
/// recipients. Receivers stay independently owned. Consuming the scope transfers
/// its endpoints' address allocation source into one Application. Early endpoints
/// and later clients share that source; this scope cannot be cloned or used for
/// a second root.
pub struct ApplicationScope {
    pub(super) allocations: ApplicationAddresses,
    pub(super) client_capacity: MailboxCapacity,
    pub(super) mailbox_capacity: MailboxCapacity,
}

impl ApplicationScope {
    /// Begin an application without constructing or starting its root.
    #[must_use]
    pub fn new() -> Self {
        Self {
            allocations: ApplicationAddresses::new(),
            client_capacity: MailboxCapacity::default(),
            mailbox_capacity: MailboxCapacity::default(),
        }
    }

    /// Select the bounded size for endpoints established after this choice.
    /// Already established endpoints retain their original mailbox size.
    #[must_use]
    pub const fn client_capacity(mut self, capacity: MailboxCapacity) -> Self {
        self.client_capacity = capacity;
        self
    }

    /// Select the user mailbox size for this application's actors.
    /// The root, fresh children, cold members and Entity actors inherit this
    /// limit. External reply endpoints retain their independent client capacity.
    #[must_use]
    pub const fn mailbox_capacity(mut self, capacity: MailboxCapacity) -> Self {
        self.mailbox_capacity = capacity;
        self
    }

    /// Establish one real external endpoint before constructing the root.
    ///
    /// # Errors
    /// Returns the exact allocation rejection, leaving this scope untouched.
    pub fn external<P>(&self) -> Result<ExternalActor<P>, ExternalActorError>
    where
        P: Protocol<Addr = MailAddr>,
        P::Msg: Send,
    {
        ExternalActor::establish(&self.allocations, self.client_capacity)
    }

    /// Assemble an Application with the constructed root and its early endpoints.
    #[must_use]
    pub fn with_root<Root>(self, root: Root) -> Application<Root, ActorSpace<Root::Protocol>>
    where
        Root: Behavior<Protocol: Protocol<Addr = MailAddr>>,
    {
        self.with_topology(root, ActorSpace::new())
    }

    /// Assemble an Application with the root, topology and early endpoints.
    #[must_use]
    pub fn with_topology<Root, Spaces>(
        self,
        root: Root,
        spaces: Spaces,
    ) -> Application<Root, Spaces> {
        Application {
            root,
            spaces,
            members: (),
            families: (),
            scope: Some(self),
        }
    }
}

impl Default for ApplicationScope {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ApplicationScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationScope")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{ApplicationScope, MailboxCapacity};
    use crate::address::{ApplicationAddresses, MailAddr};
    use crate::application::ExternalActorError;
    use behavior::{AllocationRejection, MessageProtocol};

    #[test]
    fn exhausted_namespace_returns_allocation_rejection_without_consuming_root_authority() {
        let scope = ApplicationScope {
            allocations: ApplicationAddresses::from_next(u64::MAX),
            client_capacity: MailboxCapacity::default(),
            mailbox_capacity: MailboxCapacity::default(),
        };
        for _ in 0..2 {
            let allocation = scope.external::<MessageProtocol<MailAddr, u64>>();
            assert!(matches!(
                allocation,
                Err(ExternalActorError::Allocation(
                    AllocationRejection::Exhausted
                ))
            ));
        }
        assert_eq!(format!("{scope:?}"), "ApplicationScope { .. }");
        drop(scope);
    }
}
