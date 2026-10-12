use bombay::MailAddr;
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, ActiveTurn, Behavior, BehaviorActed, BehaviorBase, ChildReport, CreateChild,
    CreationSequence, Creations, EventIngress, InterpreterRequests, MessageProtocol, Never,
    NoSends, ReportToParent, RetirementBirths, User, UserEvent,
};
struct Trace {
    visits: usize,
}
struct Readiness {
    original: Box<[u8]>,
}
struct Reporter;
#[bombay::actor(message = Never, sends = { traces: InterpreterRequests<ReportToParent<Trace>>, readiness: InterpreterRequests<ReportToParent<Readiness>> })]
impl Reporter {
    #[expect(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the actor initialization contract requires self and its exact typed success/error carrier"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont()
            .send_traces(ReportToParent::new(Trace { visits: 1 }))
            .send_readiness(ReportToParent::new(Readiness {
                original: Box::from([13, 17]),
            })))
    }
}
enum RootEvent {
    Trace(ChildReport<Trace>),
    Ready(ChildReport<Readiness>),
}
impl UserEvent for RootEvent {
    type Addr = MailAddr;
    type Message = Never;
    fn user(_: MailAddr, message: Never) -> Self {
        match message {}
    }
    fn into_user(self) -> Result<User<MailAddr, Never>, Self> {
        Err(self)
    }
}

impl<Source: bombay::behavior::BirthMode> EventIngress<Source, ChildReport<Readiness>>
    for RootEvent
{
    fn ingress(report: ChildReport<Readiness>) -> Self {
        Self::Ready(report)
    }
}
impl<Source: bombay::behavior::BirthMode> EventIngress<Source, ChildReport<Trace>> for RootEvent {
    fn ingress(report: ChildReport<Trace>) -> Self {
        Self::Trace(report)
    }
}
struct Root {
    visits: usize,
    originals: Vec<Box<[u8]>>,
}
impl BehaviorBase for Root {
    type Base = Self;
    fn base(&self) -> &Self {
        self
    }
}
impl Behavior for Root {
    type Protocol = MessageProtocol<MailAddr, Never>;
    type Event = RootEvent;
    type Sends = NoSends;
    type Ph = Never;
    type Error = Never;
    type Birth = RetirementBirths<Reporter>;
    fn init(&mut self, _: bombay::behavior::InitializationTurn) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one fresh reporter request");
        Ok(Actions::create(Creations::one(CreateChild::birth(
            id, Reporter,
        ))))
    }
    fn transition(&mut self, _: ActiveTurn, event: RootEvent) -> BehaviorActed<Self> {
        match event {
            RootEvent::Trace(report) => self.visits += report.report.visits,
            RootEvent::Ready(report) => self.originals.push(report.report.original),
        }
        Ok(Actions::cont())
    }
}
struct ReporterRole;
impl bombay::behavior::ChildRole<Root> for ReporterRole {
    type Child = Reporter;
    type Position = bombay::behavior::ChildHead;
}
bombay::topology! { reports { root { Reporter(ReporterRole) => reporter; } reporter: Reporter {} } }
#[tokio::test(flavor = "current_thread")]
async fn named_birth_delivers_both_reports_and_preserves_retirement() {
    let outcome = bombay::Application::with_topology(
        Root {
            visits: 0,
            originals: Vec::new(),
        }
        .stop_on_shutdown(),
        reports::Topology::new(),
    )
    .run_with(|application| async move {
        application
            .lifecycle()
            .request_shutdown()
            .expect("real shutdown operation");
    })
    .await;
    let outcome = outcome.unwrap_or_else(|original| {
        drop(original);
        panic!("the original application is retained on rejection");
    });
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("the complete original outcome is retained");
    });
    let (origin, retirement) = cleanup.expect("original root retirement");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let bombay::ActorRetirement::Completed { custody, .. } = retirement else {
        panic!("root completes after exact initialization reports");
    };
    assert_eq!(custody.behavior.base().visits, 1);
    assert_eq!(custody.behavior.base().originals.len(), 1);
    assert_eq!(custody.behavior.base().originals[0].as_ref(), &[13, 17]);
    assert_eq!(custody.descendants.len(), 1);
    assert_eq!(custody.child_failures.len(), 0);
    let child = custody
        .descendants
        .into_iter()
        .next()
        .expect("the exact reporter remains owned");
    assert_ne!(child.origin.address(), MailAddr::APPLICATION_ROOT);
    assert!(matches!(
        child.retirement.as_ref(),
        bombay::ActorRetirement::OwnerCancelled { .. }
    ));
}
