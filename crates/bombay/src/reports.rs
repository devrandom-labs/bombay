//! Runtime-owned translation of structural reporting service lanes.

use behavior::{
    Address, Exit, InjectEvent, ReportSupervisionFailure, ReportWorkerCreationResolved,
    ReportWorkerStopped, WorkerCreationResolved, WorkerStopped,
};
use communication::ControlSender;

use crate::generation::TerminalOverride;
use crate::interpret::RetireCapabilities;

pub(crate) struct LocalParentReports<A: Address, E> {
    parent_child_nonce: A::Nonce,
    parent: ControlSender<E>,
}

pub(crate) trait ParentReporting<A: Address, Path> {
    fn stopped(&self, report: ReportWorkerStopped<A, Path>);
    fn created(&self, report: ReportWorkerCreationResolved<A::Nonce, Path>);
}

impl<A: Address, E> LocalParentReports<A, E> {
    pub(crate) const fn new(parent_child_nonce: A::Nonce, parent: ControlSender<E>) -> Self {
        Self {
            parent_child_nonce,
            parent,
        }
    }

    pub(crate) fn worker_stopped<Path>(&self, report: ReportWorkerStopped<A, Path>)
    where
        E: InjectEvent<WorkerStopped<A>, Path>,
    {
        let ingress = report.ingress;
        let event = WorkerStopped::from((self.parent_child_nonce, report));
        let _ = self.parent.send(ingress.event(event));
    }

    pub(crate) fn worker_created<Path>(&self, report: ReportWorkerCreationResolved<A::Nonce, Path>)
    where
        E: InjectEvent<WorkerCreationResolved<A::Nonce>, Path>,
    {
        let ingress = report.ingress;
        let event = WorkerCreationResolved::from((self.parent_child_nonce, report));
        let _ = self.parent.send(ingress.event(event));
    }
}

impl<A, E, Path> ParentReporting<A, Path> for LocalParentReports<A, E>
where
    A: Address,
    E: InjectEvent<WorkerStopped<A>, Path> + InjectEvent<WorkerCreationResolved<A::Nonce>, Path>,
{
    fn stopped(&self, report: ReportWorkerStopped<A, Path>) {
        self.worker_stopped(report);
    }
    fn created(&self, report: ReportWorkerCreationResolved<A::Nonce, Path>) {
        self.worker_created(report);
    }
}

impl<A, E> RetireCapabilities for LocalParentReports<A, E>
where
    A: Address + Send,
    A::Nonce: Send,
    E: Send,
{
    async fn retire(self) {}
}

pub(crate) struct LocalSupervisionReports<A: Address> {
    termination: std::sync::Arc<TerminalOverride<A>>,
}

impl<A: Address> LocalSupervisionReports<A> {
    pub(crate) const fn new(termination: std::sync::Arc<TerminalOverride<A>>) -> Self {
        Self { termination }
    }

    pub(crate) fn report(&self, report: ReportSupervisionFailure<A>) {
        self.termination
            .set(Ok(Exit::SupervisionFailed(report.failure.reason)));
    }
}

impl<A> RetireCapabilities for LocalSupervisionReports<A>
where
    A: Address + Send + Sync,
{
    async fn retire(self) {}
}

#[cfg(test)]
mod tests {
    use behavior::{Crash, MailAddr, RestartDenial, SupervisionFailure, SupervisionFailureReason};
    use bombay_engine::Completion;

    use super::*;
    use crate::generation::NormalizedRetirement;
    use crate::{IncarnationOutcome, Retirement};

    #[tokio::test]
    async fn supervision_report_overrides_an_ordinary_stop_classification() {
        let (publisher, observation) = observe::pair();
        let terminal_override = std::sync::Arc::new(TerminalOverride::<MailAddr>::new());
        let reports = LocalSupervisionReports::new(terminal_override.clone());
        let reason = SupervisionFailureReason::RestartDenied(RestartDenial::BudgetExceeded {
            restarts_in_window: 4,
            replacements_requested: 1,
            maximum_restarts: 3,
        });
        reports.report(ReportSupervisionFailure::new(SupervisionFailure::new(
            7,
            Err(Crash::Failed),
            reason,
        )));

        NormalizedRetirement::new(publisher, terminal_override)
            .retire(IncarnationOutcome::<(), ()>::Completed(Completion::Stopped));
        assert_eq!(observation.await, Ok(Exit::SupervisionFailed(reason)));
    }
}
