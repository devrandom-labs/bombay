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
use behavior::{Behavior, BehaviorMessage, Never, Protocol};

impl<Root> Application<Root> {
    /// Declare one concrete root behavior for local execution.
    #[must_use]
    pub const fn new(root: Root) -> Self {
        Self { root, members: () }
    }
}

impl<Root, Members> Application<Root, Members> {
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
    ) -> Application<Root, (Role, Actor, Members)>
    where
        Actor: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    {
        Application {
            root: self.root,
            members: (role, actor, self.members),
        }
    }

    /// Borrow the root before execution begins.
    #[must_use]
    pub const fn root(&self) -> &Root {
        &self.root
    }

    pub(crate) fn into_parts(self) -> (Root, Members) {
        (self.root, self.members)
    }
}

impl<Root, Spaces> App<Root, Spaces> {
    #[must_use]
    pub const fn new(root: Root, spaces: Spaces) -> Self {
        Self {
            root,
            spaces,
            families: (),
        }
    }
}

impl<Root, Spaces, Families> App<Root, Spaces, Families> {
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
        App<
            Root,
            Spaces,
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
        Ok(App {
            root: self.root,
            spaces: self.spaces,
            families: (role, definition, directory, capacity, self.families),
        })
    }
}

/// One statically typed local application declaration.
///
/// `Root` owns the application behavior. `Members` is inferred from chained
/// [`Application::child`] calls and remains ordinary tuple composition; users
/// name semantic roles and actor values, never the resulting product type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Application<Root, Members = ()> {
    root: Root,
    members: Members,
}

/// Explicit advanced composition of one root and its logical protocol spaces.
#[derive(Debug)]
pub struct App<Root, Spaces, Families = ()> {
    root: Root,
    spaces: Spaces,
    families: Families,
}
