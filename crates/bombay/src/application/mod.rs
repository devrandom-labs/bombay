mod composition;
mod execution;
#[cfg(feature = "axum")]
mod http;
mod interface;
mod mailbox;
mod outcome;
mod scope;
mod work;

pub use mailbox::{MailboxCapacity, MailboxCapacityError};
pub use scope::ApplicationScope;

pub use composition::{ApplicationBehavior, ApplicationDefinitionError, ApplicationStagingError};
pub use execution::{ApplicationCleanupError, RunError};
pub use interface::{
    ActorInterface, ApplicationClient, ApplicationHandle, ApplicationLifecycle, ApplicationSender,
    ExternalActor, ExternalActorError, ExternalTarget,
};
pub use outcome::{ApplicationOutcome, ApplicationRetirement};
pub use work::WorkOutcome;

use crate::address::{ApplicationAddresses, MailAddr};
use crate::entity::EntityDefinition;
use crate::launch::ActorSpace;
use behavior::{Behavior, Never, Protocol};

impl<Root> Application<Root, ActorSpace<Root::Protocol>>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>>,
{
    /// Declare one concrete root behavior in its local protocol space.
    #[must_use]
    pub fn new(root: Root) -> Self {
        Self::with_topology(root, ActorSpace::new())
    }
}

impl<Root, Spaces> Application<Root, Spaces> {
    /// Own one root and its selected local topology.
    #[must_use]
    pub const fn with_topology(root: Root, spaces: Spaces) -> Self {
        Self {
            root,
            spaces,
            members: (),
            families: (),
            scope: None,
        }
    }
}

impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families> {
    /// Select the bounded reply mailbox size for clients established during execution.
    /// Existing early endpoints retain their own previously selected capacity.
    ///
    /// Unvalidated integers cannot select a client mailbox:
    ///
    /// ```compile_fail,E0308
    /// let scope = bombay::ApplicationScope::new().client_capacity(3);
    /// ```
    #[must_use]
    pub fn client_capacity(mut self, capacity: MailboxCapacity) -> Self {
        let scope = self.scope.get_or_insert_with(ApplicationScope::new);
        scope.client_capacity = capacity;
        self
    }

    /// Append one application-owned actor under a nominal semantic role.
    ///
    /// The actor expression determines its complete concrete type. The role
    /// identifies the declaration without becoming an actor protocol, runtime
    /// address, or structural birth position.
    #[must_use]
    pub fn child<Role, Actor>(
        self,
        role: Role,
        actor: Actor,
    ) -> Application<Root, Spaces, (Role, Actor, Members), Families>
    where
        Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    {
        Application {
            root: self.root,
            scope: self.scope,
            spaces: self.spaces,
            families: self.families,
            members: (role, actor, self.members),
        }
    }

    /// Borrow the root before execution begins.
    #[must_use]
    pub const fn root(&self) -> &Root {
        &self.root
    }

    pub(crate) fn into_parts(self) -> (Root, Spaces, Members, Families, Option<ApplicationScope>) {
        (
            self.root,
            self.spaces,
            self.members,
            self.families,
            self.scope,
        )
    }

    pub(super) fn allocations(&self) -> ApplicationAddresses {
        match &self.scope {
            Some(scope) => scope.allocations.clone(),
            None => ApplicationAddresses::new(),
        }
    }
}

impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families> {
    /// Declare one application-owned native Entity family under a semantic role.
    ///
    /// Directory configuration is validated before any application input
    /// transfers. Declaration starts no task and retains all existing inputs.
    #[must_use]
    #[allow(
        clippy::type_complexity,
        reason = "the inferred role-indexed family product is the public static composition"
    )]
    pub fn entity_family<Role, D>(
        self,
        role: Role,
        definition: D,
        directory: crate::entity::DirectoryConfig,
        capacity: crate::entity::EntityCapacity,
    ) -> Application<
        Root,
        Spaces,
        Members,
        (
            Role,
            D,
            crate::entity::DirectoryConfig,
            crate::entity::EntityCapacity,
            Families,
        ),
    >
    where
        D: EntityDefinition<Hosts = Spaces>,
    {
        Application {
            root: self.root,
            scope: self.scope,
            spaces: self.spaces,
            members: self.members,
            families: (role, definition, directory, capacity, self.families),
        }
    }
}

/// One statically typed local application declaration.
///
/// Owns the root, selected topology, cold actors and cold Entity families.
/// Chained declarations infer their complete products.
#[derive(Debug)]
pub struct Application<Root, Spaces, Members = (), Families = ()> {
    root: Root,
    spaces: Spaces,
    members: Members,
    families: Families,
    scope: Option<ApplicationScope>,
}
