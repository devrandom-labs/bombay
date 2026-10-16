use super::WorkOutcome;
use crate::entity::{FamilyAt, FamilyRetirement};
use crate::{ActorNotificationReceipts, ApplicationCleanupError};
use tokio::sync::oneshot::error::RecvError;

/// All acquired results from one application execution.
/// Work, root retirement, notification receipts and family results coexist independently.
/// Inspection consumes none; rejecting completed selection returns this entire value.
#[must_use = "application work and all independent retirement results remain owned"]
pub struct ApplicationOutcome<Work, Root, Families> {
    work: Work,
    root: Result<Root, RecvError>,
    notifications: Result<ActorNotificationReceipts, RecvError>,
    families: Families,
}

impl<Work, Root, Families> ApplicationOutcome<Work, Root, Families> {
    pub(crate) fn new(
        work: Work,
        root: Result<Root, RecvError>,
        notifications: Result<ActorNotificationReceipts, RecvError>,
        families: Families,
    ) -> Self {
        Self {
            work,
            root,
            notifications,
            families,
        }
    }

    /// Borrow the complete original work phase without consuming its inputs.
    pub fn work(&self) -> &Work {
        &self.work
    }

    /// Borrow the original root retirement or receiving failure.
    pub fn retirement(&self) -> &Result<Root, RecvError> {
        &self.root
    }
    /// Borrow notification receipts independently of work and retirement.
    pub fn notifications(&self) -> &Result<ActorNotificationReceipts, RecvError> {
        &self.notifications
    }
    /// Borrow the selected family by its role; the product position is inferred.
    ///
    /// # Errors
    /// Returns the original receiving failure if that family report was not acquired.
    #[allow(
        private_bounds,
        reason = "role projection is a library-owned sealed product law; callers infer its position and identifier"
    )]
    pub fn family<Role, Position, Id>(
        &self,
        role: Role,
    ) -> Result<&FamilyRetirement<Id>, &RecvError>
    where
        Families: FamilyAt<Role, Position, Id>,
    {
        self.families.family(role)
    }

    /// Consume this report into all of its independent owned results.
    pub fn into_parts(
        self,
    ) -> (
        Work,
        Result<Root, RecvError>,
        Result<ActorNotificationReceipts, RecvError>,
        Families,
    ) {
        (self.work, self.root, self.notifications, self.families)
    }
}

impl<Inputs, Work, Output, Staging, Prepared, Cold, Root, Families>
    ApplicationOutcome<
        WorkOutcome<Inputs, Work, Output, (), Staging, Prepared, Cold>,
        Root,
        Families,
    >
{
    /// Borrow completed work, including an application-level error value.
    pub fn work_output(&self) -> Option<&Output> {
        self.work.work_output()
    }
    /// Borrow cleanup when the work phase has acquired it.
    pub fn cleanup(&self) -> Option<&Result<(), ApplicationCleanupError>> {
        self.work.cleanup()
    }

    /// Consume completed work and retain every independent retirement result.
    ///
    /// # Errors
    /// Returns this complete original report for every other work phase.
    /// Cleanup or retirement failure does not change whether work completed.
    pub fn try_into_completed(
        self,
    ) -> Result<(Output, ApplicationRetirement<Root, Families>), Self> {
        let Self {
            work,
            root,
            notifications,
            families,
        } = self;
        match work.try_into_completed() {
            Ok((output, cleanup)) => Ok((
                output,
                ApplicationRetirement {
                    cleanup,
                    root,
                    notifications,
                    families,
                },
            )),
            Err(work) => Err(Self {
                work,
                root,
                notifications,
                families,
            }),
        }
    }
}

/// Complete cleanup and retirement custody after completed work is consumed.
/// Cleanup failure, native failure and notification failure remain independent.
#[must_use = "all independent cleanup and retirement results remain owned"]
pub struct ApplicationRetirement<Root, Families> {
    cleanup: Result<(), ApplicationCleanupError>,
    root: Result<Root, RecvError>,
    notifications: Result<ActorNotificationReceipts, RecvError>,
    families: Families,
}

impl<Root, Families> ApplicationRetirement<Root, Families> {
    /// Borrow cleanup independently of the consumed work value.
    pub fn cleanup(&self) -> &Result<(), ApplicationCleanupError> {
        &self.cleanup
    }
    /// Borrow the original root retirement or receiving failure.
    pub fn retirement(&self) -> &Result<Root, RecvError> {
        &self.root
    }
    /// Borrow notification receipts independently of work and retirement.
    pub fn notifications(&self) -> &Result<ActorNotificationReceipts, RecvError> {
        &self.notifications
    }
    /// Borrow the selected family by its role; the product position is inferred.
    ///
    /// # Errors
    /// Returns the original receiving failure if that family report was not acquired.
    #[allow(
        private_bounds,
        reason = "role projection is a library-owned sealed product law; callers infer its position and identifier"
    )]
    pub fn family<Role, Position, Id>(
        &self,
        role: Role,
    ) -> Result<&FamilyRetirement<Id>, &RecvError>
    where
        Families: FamilyAt<Role, Position, Id>,
    {
        self.families.family(role)
    }

    #[expect(
        clippy::type_complexity,
        reason = "owned peeling returns every independent cleanup, root, notification and family value without another alias"
    )]
    /// Consume this report into all of its independent owned results.
    pub fn into_parts(
        self,
    ) -> (
        Result<(), ApplicationCleanupError>,
        Result<Root, RecvError>,
        Result<ActorNotificationReceipts, RecvError>,
        Families,
    ) {
        (self.cleanup, self.root, self.notifications, self.families)
    }
}
