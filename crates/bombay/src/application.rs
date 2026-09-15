//! Pure root-first application declarations.

use behavior::{Behavior, Never, Protocol};

use crate::address::MailAddr;

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
