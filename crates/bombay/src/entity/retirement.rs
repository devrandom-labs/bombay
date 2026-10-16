use std::any::Any;

use super::{EntityMetrics, EntityRetirementFailure, EntityShutdown};
use behavior::{Here, Inside};
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

/// Complete shutdown, metrics and original disposal failure of one Entity family.
#[must_use = "family shutdown and independent disposal custody require explicit consumption"]
pub struct FamilyRetirement<Id> {
    shutdown: EntityShutdown<JoinError, EntityRetirementFailure, Id>,
    metrics: EntityMetrics,
    disposal_failure: Option<Box<dyn Any + Send>>,
}

impl<Id> FamilyRetirement<Id> {
    pub(crate) fn new(
        shutdown: EntityShutdown<JoinError, EntityRetirementFailure, Id>,
        metrics: EntityMetrics,
        disposal_failure: Option<Box<dyn Any + Send>>,
    ) -> Self {
        Self {
            shutdown,
            metrics,
            disposal_failure,
        }
    }

    /// Borrow every shutdown failure retained by this family.
    #[must_use]
    pub fn shutdown(&self) -> &EntityShutdown<JoinError, EntityRetirementFailure, Id> {
        &self.shutdown
    }
    /// Borrow the metrics captured at family shutdown.
    #[must_use]
    pub fn metrics(&self) -> &EntityMetrics {
        &self.metrics
    }
    /// Borrow the original native panic payload from family disposal, if any.
    #[must_use]
    pub fn disposal_failure(&self) -> Option<&Box<dyn Any + Send>> {
        self.disposal_failure.as_ref()
    }
    /// Consume this family report without discarding any independent result.
    #[expect(
        clippy::type_complexity,
        reason = "owned family consumption returns its complete shutdown, metrics and original native disposal failure without another alias"
    )]
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        EntityShutdown<JoinError, EntityRetirementFailure, Id>,
        EntityMetrics,
        Option<Box<dyn Any + Send>>,
    ) {
        (self.shutdown, self.metrics, self.disposal_failure)
    }
}

pub(crate) trait FamilyAt<Role, Position, Id> {
    fn family(&self, role: Role) -> Result<&FamilyRetirement<Id>, &RecvError>;
}

impl<Role, Id, Tail> FamilyAt<Role, Here, Id>
    for (Result<(Role, FamilyRetirement<Id>), RecvError>, Tail)
{
    fn family(&self, _role: Role) -> Result<&FamilyRetirement<Id>, &RecvError> {
        self.0.as_ref().map(|(_, retirement)| retirement)
    }
}

impl<Role, Position, Id, HeadRole, HeadId, Tail> FamilyAt<Role, Inside<Position>, Id>
    for (
        Result<(HeadRole, FamilyRetirement<HeadId>), RecvError>,
        Tail,
    )
where
    Tail: FamilyAt<Role, Position, Id>,
{
    fn family(&self, role: Role) -> Result<&FamilyRetirement<Id>, &RecvError> {
        self.1.family(role)
    }
}
