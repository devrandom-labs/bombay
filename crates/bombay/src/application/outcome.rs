use super::WorkOutcome;
use crate::entity::{FamilyAt, FamilyRetirement};
use crate::{ActorNotificationReceipts, ActorRetirementReport, ApplicationCleanupError};
use tokio::sync::oneshot::error::RecvError;

/// All acquired results from one application execution.
/// Work, root report, root retirement, notification receipts and family results coexist independently.
/// Inspection consumes none; rejecting completed selection returns this entire value.
#[must_use = "application work and all independent retirement results remain owned"]
pub struct ApplicationOutcome<Work, Root, Families> {
    work: Work,
    root_report: Option<ActorRetirementReport>,
    root: Result<Root, RecvError>,
    notifications: Result<ActorNotificationReceipts, RecvError>,
    families: Families,
}

impl<Work, Root, Families> ApplicationOutcome<Work, Root, Families> {
    pub(crate) fn new(
        work: Work,
        root_report: Option<ActorRetirementReport>,
        root: Result<Root, RecvError>,
        notifications: Result<ActorNotificationReceipts, RecvError>,
        families: Families,
    ) -> Self {
        Self {
            work,
            root_report,
            root,
            notifications,
            families,
        }
    }

    /// Borrow the complete original work phase without consuming its inputs.
    pub fn work(&self) -> &Work {
        &self.work
    }

    /// Inspect the runtime-issued report for the root and its owned subtree.
    ///
    /// Absence means no report was published; it never certifies success.
    /// Work, notification and family failures remain independent. Inspection
    /// leaves all original actor state and failure values in this outcome.
    #[must_use]
    pub const fn root_report(&self) -> Option<ActorRetirementReport> {
        self.root_report
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
        reason = "owned decomposition returns every work, report, root, notification and family value without another wrapper"
    )]
    /// Consume this report into all of its independent owned results.
    pub fn into_parts(
        self,
    ) -> (
        Work,
        Option<ActorRetirementReport>,
        Result<Root, RecvError>,
        Result<ActorNotificationReceipts, RecvError>,
        Families,
    ) {
        (
            self.work,
            self.root_report,
            self.root,
            self.notifications,
            self.families,
        )
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
            root_report,
            root,
            notifications,
            families,
        } = self;
        match work.try_into_completed() {
            Ok((output, cleanup)) => Ok((
                output,
                ApplicationRetirement {
                    cleanup,
                    root_report,
                    root,
                    notifications,
                    families,
                },
            )),
            Err(work) => Err(Self {
                work,
                root_report,
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
    root_report: Option<ActorRetirementReport>,
    root: Result<Root, RecvError>,
    notifications: Result<ActorNotificationReceipts, RecvError>,
    families: Families,
}

impl<Root, Families> ApplicationRetirement<Root, Families> {
    /// Borrow cleanup independently of the consumed work value.
    pub fn cleanup(&self) -> &Result<(), ApplicationCleanupError> {
        &self.cleanup
    }
    /// Inspect the runtime-issued report for the root and its owned subtree.
    ///
    /// Absence means no report was published; it never certifies success.
    /// Work, notification and family failures remain independent. Inspection
    /// leaves all original actor state and failure values in this outcome.
    #[must_use]
    pub const fn root_report(&self) -> Option<ActorRetirementReport> {
        self.root_report
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
        reason = "owned peeling returns every independent cleanup, report, root, notification and family value without another alias"
    )]
    /// Consume this report into all of its independent owned results.
    pub fn into_parts(
        self,
    ) -> (
        Result<(), ApplicationCleanupError>,
        Option<ActorRetirementReport>,
        Result<Root, RecvError>,
        Result<ActorNotificationReceipts, RecvError>,
        Families,
    ) {
        (
            self.cleanup,
            self.root_report,
            self.root,
            self.notifications,
            self.families,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ApplicationOutcome;
    use crate::behavior::Never;
    use crate::{
        ActorFailureAssessment, ActorNotificationReceipts, ActorRetirementReport,
        ApplicationCleanupError, RetirementAssessment, WorkOutcome,
    };
    use tokio::sync::oneshot;

    #[test]
    fn completed_selection_preserves_report_beside_all_independent_failures() {
        let errors = [(); 3].map(|()| {
            let (publication, receiving) = oneshot::channel::<()>();
            drop(publication);
            receiving
                .blocking_recv()
                .expect_err("the original publication is closed")
        });
        let [cleanup_error, root_error, notification_error] = errors;
        let value = vec![11_u8, 13].into_boxed_slice();
        let value_allocation = value.as_ptr();
        let families = vec![17_u8, 19].into_boxed_slice();
        let family_allocation = families.as_ptr();
        let report = ActorRetirementReport::new(
            RetirementAssessment::Established,
            ActorFailureAssessment::FailuresFound,
        );
        let work: WorkOutcome<Never, Never, Box<[u8]>, ()> = WorkOutcome::Completed {
            output: value,
            cleanup: Err(ApplicationCleanupError::PublicationClosed(cleanup_error)),
        };
        let root: Result<Box<[u8]>, _> = Err(root_error);
        let outcome =
            ApplicationOutcome::new(work, Some(report), root, Err(notification_error), families);
        assert_eq!(outcome.root_report(), Some(report));
        let (value, retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("independent failures cannot erase the completed value"));
        assert_eq!(value.as_ptr(), value_allocation);
        assert_eq!(retirement.root_report(), Some(report));
        let (cleanup, retained_report, root, notifications, families) = retirement.into_parts();
        assert!(matches!(
            cleanup,
            Err(ApplicationCleanupError::PublicationClosed(_))
        ));
        assert_eq!(retained_report, Some(report));
        assert!(root.is_err());
        assert!(notifications.is_err());
        assert_eq!(families.as_ptr(), family_allocation);
        assert_eq!(&*families, &[17, 19]);
    }

    #[test]
    fn interrupted_selection_returns_the_entire_original_product() {
        let root = vec![23_u8, 29].into_boxed_slice();
        let root_allocation = root.as_ptr();
        let families = vec![31_u8, 37].into_boxed_slice();
        let family_allocation = families.as_ptr();
        let report = ActorRetirementReport::new(
            RetirementAssessment::NotEstablished,
            ActorFailureAssessment::Incomplete,
        );
        let work: WorkOutcome<Never, Never, Never, ()> =
            WorkOutcome::Interrupted { cleanup: Ok(()) };
        let outcome = ApplicationOutcome::new(
            work,
            Some(report),
            Ok(root),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            families,
        );
        let returned = outcome
            .try_into_completed()
            .err()
            .expect("interruption returns every acquired axis");
        assert_eq!(returned.root_report(), Some(report));
        let (work, retained_report, root, notifications, families) = returned.into_parts();
        assert!(matches!(work, WorkOutcome::Interrupted { cleanup: Ok(()) }));
        assert_eq!(retained_report, Some(report));
        let root = root.expect("the independently acquired native result remains owned");
        assert_eq!((root.as_ptr(), &*root), (root_allocation, &[23, 29][..]));
        assert_eq!(
            (families.as_ptr(), &*families),
            (family_allocation, &[31, 37][..])
        );
        let notifications =
            notifications.expect("both original notification receipts remain owned");
        notifications
            .termination
            .expect("termination receipt survives");
        notifications
            .retirement
            .expect("retirement receipt survives");
    }
}
