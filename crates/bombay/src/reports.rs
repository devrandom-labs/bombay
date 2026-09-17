//! Runtime-owned translation of structural parent-report requests.

use std::sync::Arc;

use behavior::{Behavior, ChildReport, CreationId, EventIngress};
use behavior_actors::ReportTerminalOutcome;
use communication::{ControlClosed, ControlSender};

use crate::address::MailAddr;
use crate::local::Termination;
use crate::termination::{TerminalReportDisposition, TerminationSelection};

pub(crate) trait TerminalReportTransaction {
    fn begin_terminal_reports(&self);
    fn finish_terminal_reports(&self, disposition: TerminalReportDisposition);
}

pub(crate) struct LocalTerminalReports {
    selection: Arc<TerminationSelection<MailAddr>>,
}

impl LocalTerminalReports {
    pub(crate) const fn new(selection: Arc<TerminationSelection<MailAddr>>) -> Self {
        Self { selection }
    }

    pub(crate) fn begin(&self) {
        self.selection.begin();
    }

    pub(crate) fn finish(&self, disposition: TerminalReportDisposition) {
        self.selection.finish(disposition);
    }

    pub(crate) fn report_outcome(&self, report: ReportTerminalOutcome<MailAddr>) {
        let outcome: Termination<MailAddr> = report.outcome;
        self.selection.select(outcome);
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
