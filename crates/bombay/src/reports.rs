//! Runtime-owned translation of structural parent-report requests.

use core::mem;

use behavior::{Behavior, ChildReport, CreationId, EventIngress};
use behavior_actors::ReportTerminalOutcome;
use communication::{ControlClosed, ControlSender};
use tokio::sync::oneshot;

use crate::address::MailAddr;
use crate::local::Termination;
use crate::termination::{TerminalReportDisposition, TerminationSelection};

pub(crate) trait TerminalReportTransaction {
    fn finish_terminal_reports(&mut self, disposition: TerminalReportDisposition);
}

pub(crate) struct LocalTerminalReports {
    selection: TerminationSelection<MailAddr>,
    report: Option<oneshot::Sender<Termination<MailAddr>>>,
}

impl LocalTerminalReports {
    pub(crate) const fn new(report: oneshot::Sender<Termination<MailAddr>>) -> Self {
        Self {
            selection: TerminationSelection::new(),
            report: Some(report),
        }
    }

    pub(crate) fn finish(&mut self, disposition: TerminalReportDisposition) {
        self.selection.finish(disposition);
    }

    pub(crate) fn report_outcome(&mut self, report: ReportTerminalOutcome<MailAddr>) {
        let outcome: Termination<MailAddr> = report.outcome;
        self.selection.select(outcome);
    }

    /// Acquire the original one-shot result outside terminal-report disposal.
    /// A refused outcome does not recreate the consumed sender.
    pub(crate) fn receive_retirement(
        &mut self,
        publication: &mut Option<Result<(), Termination<MailAddr>>>,
    ) {
        if publication.is_some() {
            return;
        }
        if matches!(self.selection, TerminationSelection::Selected(_)) && self.report.is_none() {
            return;
        }
        let outcome = match mem::replace(&mut self.selection, TerminationSelection::new()) {
            TerminationSelection::Selected(outcome) => outcome,
            unselected => {
                self.selection = unselected;
                drop(self.report.take());
                return;
            }
        };
        let report = self
            .report
            .take()
            .expect("the original selected report owns its sender");
        *publication = Some(report.send(outcome));
        if matches!(publication, Some(Err(_))) {
            unreachable!("the incarnation retirement owns the report receiver");
        }
    }

    /// Retirement is complete when the original selection and sender are gone.
    /// An acquired refusal remains in the outside publication result.
    pub(crate) fn retirement_complete(&self) -> bool {
        self.report.is_none() && !matches!(self.selection, TerminationSelection::Selected(_))
    }

    pub(crate) fn retire(mut self) {
        let mut publication = None;
        self.receive_retirement(&mut publication);
    }
}

pub(crate) struct LocalParentReports<Event, Child, Position> {
    child: CreationId,
    parent: ControlSender<Event>,
    occurrence: core::marker::PhantomData<fn() -> (Child, Position)>,
}

pub(crate) trait ParentReporting<Report> {
    fn report(&self, report: Report);
}

impl<Event, Child, Position> LocalParentReports<Event, Child, Position> {
    pub(crate) const fn new(child: CreationId, parent: ControlSender<Event>) -> Self {
        Self {
            child,
            parent,
            occurrence: core::marker::PhantomData,
        }
    }

    pub(crate) fn report<Report>(&self, report: Report)
    where
        Child: Behavior,
        Event: EventIngress<Position, ChildReport<Report>>,
    {
        let event = Event::ingress(ChildReport::new(self.child, report));
        match self.parent.send(event) {
            Ok(()) => {}
            Err(ControlClosed(_event)) => {
                unreachable!("the parent control lane outlives every owned child task")
            }
        }
    }
}

impl<Event, Child, Position, Report> ParentReporting<Report>
    for LocalParentReports<Event, Child, Position>
where
    Child: Behavior,
    Event: EventIngress<Position, ChildReport<Report>>,
{
    fn report(&self, report: Report) {
        LocalParentReports::report(self, report);
    }
}
