#![no_main]

use std::collections::VecDeque;
use std::future::{Future, pending};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Creations, EventLayer, Interpretation,
    MailAddr, Never, NoBirths, SettlementStatus, SourceCustody, Step, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Driver, Environment};
use libfuzzer_sys::fuzz_target;

#[derive(Clone)]
enum FoldChoice {
    Continue,
    Stop,
    Fail,
}

#[derive(Clone)]
enum SourceChoice {
    Exhaust,
    Retain,
    Admit,
    Close,
    Pending,
}

#[derive(Clone)]
enum ActivationChoice {
    Establish,
    Fail,
}

#[derive(Clone)]
struct Turn {
    id: u8,
    fold: FoldChoice,
    settlement: SettlementStatus,
    source: SourceChoice,
}

#[derive(Clone)]
enum Operation {
    Turn(Turn),
    Malformed(Vec<u8>),
}

#[derive(Clone, Debug)]
enum Fact {
    Commit(u8),
    RequestedOrdinary,
    RequestedSource,
    Offered(u8),
    Published,
    ActiveRetired(Vec<u8>),
    PreparedRetired,
    ActivationFailed,
}

fn decode(input: &[u8]) -> (Operation, ActivationChoice, VecDeque<Operation>) {
    let mut chunks = input.chunks(6).map(|chunk| {
        let [id, fold, settlement, source, _, activation] = chunk else {
            return (Operation::Malformed(chunk.to_vec()), ActivationChoice::Establish);
        };
        let fold = match fold % 3 {
            0 => FoldChoice::Continue,
            1 => FoldChoice::Stop,
            _ => FoldChoice::Fail,
        };
        let settlement = match settlement % 3 {
            0 => SettlementStatus::Accepted,
            1 => SettlementStatus::Rejected,
            _ => SettlementStatus::Corrupt,
        };
        let source = match source % 5 {
            0 => SourceChoice::Exhaust,
            1 => SourceChoice::Retain,
            2 => SourceChoice::Admit,
            3 => SourceChoice::Close,
            _ => SourceChoice::Pending,
        };
        let activation = match activation % 2 {
            0 => ActivationChoice::Establish,
            _ => ActivationChoice::Fail,
        };
        (Operation::Turn(Turn { id: *id, fold, settlement, source }), activation)
    });
    let (initialization, activation) = chunks.next().unwrap_or_else(|| {
        (Operation::Turn(Turn {
            id: 0,
            fold: FoldChoice::Continue,
            settlement: SettlementStatus::Accepted,
            source: SourceChoice::Exhaust,
        }), ActivationChoice::Establish)
    });
    let ordinary = chunks.map(|(operation, _)| operation).collect();
    (initialization, activation, ordinary)
}

struct FuzzBehavior {
    initialization: Operation,
    folded: usize,
    dropped: Arc<AtomicUsize>,
}

impl Drop for FuzzBehavior {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl FuzzBehavior {
    fn decide(operation: Operation) -> BehaviorActed<Self> {
        match operation {
            Operation::Malformed(bytes) => {
                drop(bytes);
                Err("malformed operation")
            }
            Operation::Turn(turn) => match turn.fold {
                FoldChoice::Fail => Err("controlled fold failure"),
                FoldChoice::Continue => Ok(Actions::send(vec![turn])),
                FoldChoice::Stop => Ok(Actions::new(
                    vec![turn],
                    Creations::empty(),
                    Step::Stop(behavior::Stopped),
                )),
            },
        }
    }
}

impl Behavior for FuzzBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, Operation>;
    type Event = EventLayer<Operation, User<MailAddr, Operation>>;
    type Sends = Vec<Turn>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        Self::decide(self.initialization.clone())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.folded += 1;
        match event {
            EventLayer::Owned(operation) => Self::decide(operation),
            EventLayer::Inner(event) => Self::decide(event.message),
        }
    }
}

struct FuzzSettlement {
    id: u8,
    status: SettlementStatus,
    source: SourceChoice,
}

impl ClassifySettlement for FuzzSettlement {
    fn settlement_status(&self) -> SettlementStatus {
        self.status
    }
}

struct FuzzEnvironment {
    activation: ActivationChoice,
    ordinary: VecDeque<Operation>,
    source: VecDeque<Operation>,
    facts: Arc<Mutex<Vec<Fact>>>,
    dropped: Arc<AtomicUsize>,
}

impl Drop for FuzzEnvironment {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl FuzzEnvironment {
    fn commit(&mut self, actions: ActionsOf<FuzzBehavior>) -> Interpretation<FuzzSettlement> {
        let mut turns = actions.sends.into_iter();
        let turn = turns.next().expect("one scripted effect per action");
        let extra = turns.next();
        assert!(extra.is_none());
        self.facts.lock().unwrap().push(Fact::Commit(turn.id));
        let settlement = FuzzSettlement {
            id: turn.id,
            status: turn.settlement,
            source: turn.source,
        };
        match settlement.status {
            SettlementStatus::Accepted | SettlementStatus::Rejected => {
                Interpretation::Complete(settlement)
            }
            SettlementStatus::Corrupt => Interpretation::Corrupt(settlement),
        }
    }
}

impl Environment<FuzzBehavior> for FuzzEnvironment {
    type Active = Self;
    type Settlement = FuzzSettlement;
    type Error = &'static str;
    type Residual = ();

    async fn activate(
        mut self,
        actions: ActionsOf<FuzzBehavior>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        match self.activation {
            ActivationChoice::Establish => {
                let interpretation = self.commit(actions);
                Ok((self, interpretation))
            }
            ActivationChoice::Fail => {
                self.facts.lock().unwrap().push(Fact::ActivationFailed);
                Err(("scripted activation failure", ()))
            }
        }
    }

    async fn retire(self) {
        self.facts.lock().unwrap().push(Fact::PreparedRetired);
    }
}

impl ActiveEnvironment<FuzzBehavior> for FuzzEnvironment {
    type Settlement = FuzzSettlement;
    type Residual = ();

    async fn next(&mut self) -> Option<<FuzzBehavior as Behavior>::Event> {
        self.facts.lock().unwrap().push(Fact::RequestedOrdinary);
        self.ordinary
            .pop_front()
            .map(|operation| EventLayer::Inner(User::new(MailAddr(1), operation)))
    }

    async fn next_source(&mut self) -> Option<<FuzzBehavior as Behavior>::Event> {
        self.facts.lock().unwrap().push(Fact::RequestedSource);
        self.source.pop_front().map(EventLayer::Owned)
    }

    async fn apply(&mut self, actions: ActionsOf<FuzzBehavior>) -> Interpretation<Self::Settlement> {
        self.commit(actions)
    }

    async fn offer_next(
        &mut self,
        mut settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        self.facts.lock().unwrap().push(Fact::Offered(settlement.id));
        match std::mem::replace(&mut settlement.source, SourceChoice::Exhaust) {
            SourceChoice::Exhaust => SourceCustody::Exhausted(settlement),
            SourceChoice::Retain => SourceCustody::Retained(settlement),
            SourceChoice::Admit => {
                self.source.push_back(Operation::Turn(Turn {
                    id: settlement.id,
                    fold: FoldChoice::Continue,
                    settlement: SettlementStatus::Accepted,
                    source: SourceChoice::Exhaust,
                }));
                SourceCustody::Admitted(settlement)
            }
            SourceChoice::Close => SourceCustody::Closed(settlement),
            SourceChoice::Pending => pending().await,
        }
    }

    fn publish(&mut self) {
        self.facts.lock().unwrap().push(Fact::Published);
    }

    async fn retire(self, settlements: Vec<Self::Settlement>) {
        let ids = settlements.into_iter().map(|settlement| settlement.id).collect();
        self.facts.lock().unwrap().push(Fact::ActiveRetired(ids));
    }
}

fn run(input: &[u8]) {
    let (initialization, activation, ordinary) = decode(input);
    let facts = Arc::new(Mutex::new(Vec::new()));
    let behavior_dropped = Arc::new(AtomicUsize::new(0));
    let environment_dropped = Arc::new(AtomicUsize::new(0));
    let behavior = FuzzBehavior {
        initialization,
        folded: 0,
        dropped: behavior_dropped.clone(),
    };
    let environment = FuzzEnvironment {
        activation,
        ordinary,
        source: VecDeque::new(),
        facts: facts.clone(),
        dropped: environment_dropped.clone(),
    };
    let mut execution = Box::pin(Driver::new(behavior, environment).run());
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let polled = execution.as_mut().poll(&mut context);
    match polled {
        Poll::Ready(retirement) => {
            let folded = retirement.behavior.folded;
            let recorded = facts.lock().unwrap().clone();
            let committed = recorded.iter().filter(|fact| matches!(fact, Fact::Commit(_))).count();
            assert!(committed <= folded + 1);
            assert!(recorded.iter().any(|fact| matches!(fact, Fact::ActiveRetired(_) | Fact::PreparedRetired | Fact::ActivationFailed)));
            drop(retirement);
        }
        Poll::Pending => {
            let recorded = facts.lock().unwrap();
            assert!(!recorded.iter().any(|fact| matches!(fact, Fact::ActiveRetired(_) | Fact::PreparedRetired)));
        }
    }
    drop(execution);
    assert_eq!(behavior_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(environment_dropped.load(Ordering::SeqCst), 1);
    let recorded = facts.lock().unwrap();
    let committed_ids: Vec<_> = recorded.iter().filter_map(|fact| match fact {
        Fact::Commit(id) => Some(*id),
        _ => None,
    }).collect();
    for fact in recorded.iter() {
        match fact {
            Fact::Offered(id) => assert!(committed_ids.contains(id)),
            Fact::ActiveRetired(ids) => {
                for id in ids {
                    assert!(committed_ids.contains(id));
                }
            }
            _ => {}
        }
    }
    let publications = recorded.iter().filter(|fact| matches!(fact, Fact::Published)).count();
    assert!(publications <= 1);
    if let Some(index) = recorded.iter().position(|fact| matches!(fact, Fact::ActiveRetired(_) | Fact::PreparedRetired)) {
        assert_eq!(index + 1, recorded.len());
    }
}

fuzz_target!(|input: &[u8]| run(input));
