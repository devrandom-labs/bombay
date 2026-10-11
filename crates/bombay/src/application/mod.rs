mod composition;
mod execution;
#[cfg(feature = "axum")]
mod http;
mod interface;

pub use composition::{ApplicationBehavior, ApplicationDefinitionError, ApplicationStagingError};
pub use execution::{ApplicationCleanupError, ApplicationOutcome, RunError};
pub use interface::{
    ActorInterface, ApplicationHandle, ApplicationLifecycle, ExternalActor, ExternalActorError,
    ExternalTarget,
};

use crate::address::MailAddr;
use crate::entity::EntityDefinition;
use crate::launch::ActorSpace;
use behavior::{Behavior, BehaviorMessage, Never, Protocol};

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
        }
    }
}

impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families> {
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

    pub(crate) fn into_parts(self) -> (Root, Spaces, Members, Families) {
        (self.root, self.spaces, self.members, self.families)
    }
}

impl<Root, Spaces, Members, Families> Application<Root, Spaces, Members, Families> {
    /// Declare one application-owned native Entity family under a semantic role.
    ///
    /// # Errors
    ///
    /// Returns the existing directory configuration error before application
    /// startup. This consuming call drops its inputs on rejection.
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
    ) -> Result<
        Application<
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
        >,
        crate::entity::DirectoryError<BehaviorMessage<D::Behavior>>,
    >
    where
        D: EntityDefinition<Hosts = Spaces>,
    {
        if !directory.shards.get().is_power_of_two() {
            return Err(crate::entity::DirectoryError::InvalidShardCount);
        }
        Ok(Application {
            root: self.root,
            spaces: self.spaces,
            members: self.members,
            families: (role, definition, directory, capacity, self.families),
        })
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
}
