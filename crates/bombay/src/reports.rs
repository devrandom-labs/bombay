//! Runtime-owned translation of structural parent-report requests.

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
    report: oneshot::Sender<Termination<MailAddr>>,
}

impl LocalTerminalReports {
    pub(crate) const fn new(report: oneshot::Sender<Termination<MailAddr>>) -> Self {
        Self {
            selection: TerminationSelection::new(),
            report,
        }
    }

    pub(crate) fn finish(&mut self, disposition: TerminalReportDisposition) {
        self.selection.finish(disposition);
    }

    pub(crate) fn report_outcome(&mut self, report: ReportTerminalOutcome<MailAddr>) {
        let outcome: Termination<MailAddr> = report.outcome;
        self.selection.select(outcome);
    }

    pub(crate) fn retire(self) {
        if let TerminationSelection::Selected(outcome) = self.selection {
            match self.report.send(outcome) {
                Ok(()) => {}
                Err(_) => unreachable!("the incarnation retirement owns the report receiver"),
            }
        }
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
