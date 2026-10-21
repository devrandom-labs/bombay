use bombay::behavior::{Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, ChildReport, CreateChild, CreationSequence, Creations, EventIngress, InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoSends, ReportToParent, RetirementBirths, User, UserEvent};
use bombay::actors::ActorExt;
use bombay::MailAddr;
struct Trace { visits: usize }
struct Readiness { original: Box<[u8]> }
struct Reporter;
#[bombay::actor(message = Never, sends = { traces: InterpreterRequests<ReportToParent<Trace>>, readiness: InterpreterRequests<ReportToParent<Readiness>> })]
impl Reporter {
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont().send_traces(ReportToParent::new(Trace { visits: 1 })).send_readiness(ReportToParent::new(Readiness { original: Box::from([13,17]) })))
    }
}
enum RootEvent { Trace(ChildReport<Trace>), Ready(ChildReport<Readiness>) }
impl UserEvent for RootEvent {
    type Addr = MailAddr; type Message = Never;
    fn user(_: MailAddr, message: Never) -> Self { match message {} }
    fn into_user(self) -> Result<User<MailAddr, Never>, Self> { Err(self) }
}

impl<Source: bombay::behavior::BirthMode> EventIngress<Source, ChildReport<Readiness>> for RootEvent { fn ingress(report: ChildReport<Readiness>) -> Self { Self::Ready(report) } }
struct Root { visits: usize, originals: Vec<Box<[u8]>> }
impl BehaviorBase for Root { type Base = Self; fn base(&self) -> &Self { self } }
impl Behavior for Root {
    type Protocol = MessageProtocol<MailAddr, Never>; type Event = RootEvent;
    type Sends = NoSends; type Ph = Never; type Error = Never;
    type Birth = RetirementBirths<Reporter>;
    fn init(&mut self, _: bombay::behavior::InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one fresh reporter request");
        Ok(Actions::create(Creations::one(CreateChild::birth(id, Reporter))))
    }
    fn transition(&mut self, _: ActiveTurn, event: RootEvent) -> BehaviorActed<Self> {
        match event { RootEvent::Trace(report) => self.visits += report.report.visits, RootEvent::Ready(report) => self.originals.push(report.report.original) }
        Ok(Actions::cont())
    }
}
bombay::topology! { reports { root: Root { Reporter => reporter; } reporter: Reporter {} } }
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let outcome = bombay::Application::with_topology(Root { visits: 0, originals: Vec::new() }.stop_on_shutdown(), reports::Topology::new())
        .run_with(|application| async move { application.lifecycle().shutdown().expect("real shutdown operation"); })
        .await;
    drop(outcome);
}
