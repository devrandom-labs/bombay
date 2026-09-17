//! Stable typed identity, on-demand activation, and exact-incarnation lifecycle.
//!
//! Entity routing belongs to Bombay's actor layer. A logical [`EntityId`]
//! remains stable while its actor incarnation can activate, drain, retire, and
//! later be replaced. The directory preserves command ownership, admits one
//! activation attempt for concurrent first commands, and rejects stale
//! generation facts.

use core::fmt;
use core::hash::Hash;

mod bombay;
mod directory;
mod family;
mod lifecycle;

#[cfg(test)]
mod lifecycle_tests;

pub(crate) use bombay::{FenceFailure, NativeEntityHost};
pub use bombay_machine::{Decision, Reducer};
pub use directory::{
    AdmissionFailure, DirectoryConfig, DirectoryError, DirectoryOutput, DispatchOutput,
    EffectInterpreter, EntityShutdown, LocalDirectory, Passivation,
};
pub use family::{
    Entities, EntityActivationError, EntityAdmission, EntityApplicationFamilies, EntityCapacity,
    EntityDefinition, EntityFamilyAt, EntityMetrics, EntityRef,
};
pub(crate) use family::{InstallEntityFamilies, InstalledEntityFamilies};
pub use lifecycle::{
    ActivatingSlot, ActivationId, ActiveSlot, DispatchId, DrainFailure, DrainStage, DrainingSlot,
    EntitySlot, LIFECYCLE_TOPOLOGY, LifecycleEdge, LifecycleMachine, LifecycleOutput,
    LifecyclePhase, LifecycleTopologyError, LifecycleTrigger, Refusal, RetirementMode,
    SlotDecision, SlotEffect, SlotEffectBatch, SlotEvent, SlotReducer, TransitionEvidence,
    lifecycle_machine, validate_lifecycle_topology,
};

/// A stable, typed identifier for a logical entity.
///
/// The identifier is independent of any particular actor address or
/// incarnation. Its inner domain value determines equality, ordering, and
/// hashing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityId<T>(T);

impl<T> EntityId<T> {
    /// Wrap a domain identifier as an entity identifier.
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrow the domain identifier.
    pub const fn get(&self) -> &T {
        &self.0
    }

    /// Return the wrapped domain identifier.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for EntityId<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EntityId").field(&self.0).finish()
    }
}

impl<T: fmt::Display> fmt::Display for EntityId<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::EntityId;

    #[test]
    fn entity_id_preserves_domain_identity() {
        let id = EntityId::new(42_u64);

        assert_eq!(id.get(), &42);
        assert_eq!(id.to_string(), "42");
        assert_eq!(id.into_inner(), 42);
    }
}
