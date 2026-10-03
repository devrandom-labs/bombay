#![cfg_attr(not(test), no_main)]

use core::ops::ControlFlow;
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Creations, EventLayer, Interpretation,
    MailAddr, Never, NoBirths, SettlementStatus, SourceCustody, Step, User,
};
use bombay_engine::{ActionsOf, ActiveEnvironment, Completion, Driver, Environment};
#[cfg(not(test))]
use libfuzzer_sys::fuzz_target;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ActorDecision {
    Continue,
    Stop,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct Turn {
    id: u8,
    decision: ActorDecision,
    settlement: SettlementStatus,
    source: SourceChoice,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ActorInput {
    Turn(Turn),
    Malformed(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcquisitionPort {
    Ordinary,
    Source,
}

// This payload is affine. Its audit belongs to the host, never Behavior state.
struct RetirementRequest {
    payload: Box<[u8]>,
    released: Arc<AtomicUsize>,
}

impl Drop for RetirementRequest {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SettlementReceipt {
    ordinal: usize,
    turn: Turn,
    allocation: usize,
    payload: Vec<u8>,
}

#[derive(Default)]
struct ExecutionTrace {
    delivered: Vec<ActorInput>,
    committed: Vec<SettlementReceipt>,
    discharged: Vec<SettlementReceipt>,
    offered: Vec<usize>,
    acquired: Vec<AcquisitionPort>,
    ordinary_requests: usize,
    source_requests: usize,
    publications: usize,
    retirements: usize,
}

fn decode(
    input: &[u8],
) -> (
    ActorInput,
    ActivationChoice,
    VecDeque<ActorInput>,
    Option<(AcquisitionPort, usize)>,
) {
    let mut chunks = input.chunks(6).map(|chunk| {
        let [id, decision, settlement, source, _, activation] = chunk else {
            return (
                ActorInput::Malformed(chunk.to_vec()),
                ActivationChoice::Establish,
            );
        };
        let decision = match decision % 3 {
            0 => ActorDecision::Continue,
            1 => ActorDecision::Stop,
            _ => ActorDecision::Fail,
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
        (
            ActorInput::Turn(Turn {
                id: *id,
                decision,
                settlement,
                source,
            }),
            activation,
        )
    });
    let (initialization, activation) = chunks.next().unwrap_or({
        (
            ActorInput::Turn(Turn {
                id: 0,
                decision: ActorDecision::Continue,
                settlement: SettlementStatus::Accepted,
                source: SourceChoice::Exhaust,
            }),
            ActivationChoice::Establish,
        )
    });
    let plan = match input.get(4).copied().unwrap_or(0) % 3 {
        0 => None,
        1 => Some((
            AcquisitionPort::Ordinary,
            1 + input.get(6).copied().unwrap_or(0) as usize % 4,
        )),
        _ => Some((
            AcquisitionPort::Source,
            1 + input.get(6).copied().unwrap_or(0) as usize % 4,
        )),
    };
    (
        initialization,
        activation,
        chunks.map(|(operation, _)| operation).collect(),
        plan,
    )
}

struct SettlementActor {
    initialization: ActorInput,
    attempted: Vec<ActorInput>,
}

impl SettlementActor {
    fn decide(&mut self, operation: ActorInput) -> BehaviorActed<Self> {
        self.attempted.push(operation.clone());
        match operation {
            ActorInput::Malformed(_) => Err("malformed operation"),
            ActorInput::Turn(turn) => match turn.decision {
                ActorDecision::Fail => Err("controlled actor decision failure"),
                ActorDecision::Continue => Ok(Actions::send(vec![turn])),
                ActorDecision::Stop => Ok(Actions::new(
                    vec![turn],
                    Creations::empty(),
                    Step::Stop(behavior::Stopped),
                )),
            },
        }
    }
}

impl Behavior for SettlementActor {
    type Protocol = behavior::MessageProtocol<MailAddr, ActorInput>;
    type Event = EventLayer<ActorInput, User<MailAddr, ActorInput>>;
    type Sends = Vec<Turn>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        self.decide(self.initialization.clone())
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        match event {
            EventLayer::Owned(operation) => self.decide(operation),
            EventLayer::Inner(event) => self.decide(event.message),
        }
    }
}

struct ActionSettlement {
    receipt: SettlementReceipt,
    payload: Box<[u8]>,
    remaining: SourceChoice,
}

impl ClassifySettlement for ActionSettlement {
    fn settlement_status(&self) -> SettlementStatus {
        self.receipt.turn.settlement
    }
}

struct SettlementEnvironment {
    activation: ActivationChoice,
    ordinary: VecDeque<ActorInput>,
    source: VecDeque<ActorInput>,
    trace: Arc<Mutex<ExecutionTrace>>,
    dropped: Arc<AtomicUsize>,
    plan: Option<(AcquisitionPort, usize)>,
    request: Option<RetirementRequest>,
    retained: Vec<ActionSettlement>,
}

impl Drop for SettlementEnvironment {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl SettlementEnvironment {
    fn commit(&mut self, actions: ActionsOf<SettlementActor>) -> Interpretation<ActionSettlement> {
        let mut turns = actions.sends.into_iter();
        let turn = turns.next().expect("one scripted send per action");
        let extra = turns.next();
        assert!(extra.is_none());
        assert_eq!(actions.creates, Creations::empty());
        let expected_step = match turn.decision {
            ActorDecision::Continue => Step::Continue,
            ActorDecision::Stop => Step::Stop(behavior::Stopped),
            ActorDecision::Fail => panic!("a failed decision cannot emit Actions"),
        };
        assert_eq!(actions.become_, expected_step);
        let payload =
            vec![turn.id, turn.id.wrapping_add(1), turn.id.wrapping_sub(1)].into_boxed_slice();
        let mut trace = self.trace.lock().unwrap();
        let receipt = SettlementReceipt {
            ordinal: trace.committed.len(),
            turn: turn.clone(),
            allocation: payload.as_ptr() as usize,
            payload: payload.to_vec(),
        };
        trace.committed.push(receipt.clone());
        let settlement = ActionSettlement {
            receipt,
            payload,
            remaining: turn.source,
        };
        match settlement.settlement_status() {
            SettlementStatus::Accepted | SettlementStatus::Rejected => {
                Interpretation::Complete(settlement)
            }
            SettlementStatus::Corrupt => Interpretation::Corrupt(settlement),
        }
    }

    fn acquire(&mut self, port: AcquisitionPort) -> Option<RetirementRequest> {
        let trace = self.trace.lock().unwrap();
        let count = match port {
            AcquisitionPort::Ordinary => trace.ordinary_requests,
            AcquisitionPort::Source => trace.source_requests,
        };
        drop(trace);
        match self.plan {
            Some((selected, occurrence)) if selected == port && count >= occurrence => {
                let request = self.request.take();
                if request.is_some() {
                    self.trace.lock().unwrap().acquired.push(port);
                }
                request
            }
            None | Some((_, _)) => None,
        }
    }
}

impl Environment<SettlementActor> for SettlementEnvironment {
    type Active = Self;
    type Settlement = ActionSettlement;
    type Error = &'static str;
    type Residual = Self;
    type RetirementRequest = RetirementRequest;

    async fn activate(
        mut self,
        actions: ActionsOf<SettlementActor>,
    ) -> Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>
    {
        match self.activation {
            ActivationChoice::Establish => {
                let interpretation = self.commit(actions);
                Ok((self, interpretation))
            }
            ActivationChoice::Fail => {
                self.trace.lock().unwrap().retirements += 1;
                Err(("scripted activation failure", self))
            }
        }
    }

    async fn retire(self) -> Self::Residual {
        self.trace.lock().unwrap().retirements += 1;
        self
    }
}

impl ActiveEnvironment<SettlementActor> for SettlementEnvironment {
    type Settlement = ActionSettlement;
    type Residual = Self;
    type RetirementRequest = RetirementRequest;

    async fn next(
        &mut self,
    ) -> ControlFlow<RetirementRequest, Option<<SettlementActor as Behavior>::Event>> {
        self.trace.lock().unwrap().ordinary_requests += 1;
        if let Some(request) = self.acquire(AcquisitionPort::Ordinary) {
            return ControlFlow::Break(request);
        }
        let operation = self.ordinary.pop_front();
        if let Some(operation) = &operation {
            self.trace.lock().unwrap().delivered.push(operation.clone());
        }
        ControlFlow::Continue(
            operation.map(|operation| EventLayer::Inner(User::new(MailAddr(1), operation))),
        )
    }

    async fn next_source(
        &mut self,
    ) -> ControlFlow<RetirementRequest, Option<<SettlementActor as Behavior>::Event>> {
        self.trace.lock().unwrap().source_requests += 1;
        if let Some(request) = self.acquire(AcquisitionPort::Source) {
            return ControlFlow::Break(request);
        }
        let operation = self.source.pop_front();
        if let Some(operation) = &operation {
            self.trace.lock().unwrap().delivered.push(operation.clone());
        }
        ControlFlow::Continue(operation.map(EventLayer::Owned))
    }

    async fn apply(
        &mut self,
        actions: ActionsOf<SettlementActor>,
    ) -> Interpretation<Self::Settlement> {
        self.commit(actions)
    }

    async fn offer_next(
        &mut self,
        mut settlement: Self::Settlement,
    ) -> SourceCustody<Self::Settlement> {
        self.trace
            .lock()
            .unwrap()
            .offered
            .push(settlement.receipt.ordinal);
        match std::mem::replace(&mut settlement.remaining, SourceChoice::Exhaust) {
            SourceChoice::Exhaust => {
                self.trace
                    .lock()
                    .unwrap()
                    .discharged
                    .push(settlement.receipt.clone());
                SourceCustody::Exhausted(settlement)
            }
            SourceChoice::Retain => SourceCustody::Retained(settlement),
            SourceChoice::Admit => {
                self.source.push_back(ActorInput::Turn(Turn {
                    id: settlement.receipt.turn.id,
                    decision: ActorDecision::Continue,
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
        self.trace.lock().unwrap().publications += 1;
    }

    async fn retire(mut self, settlements: Vec<Self::Settlement>) -> Self::Residual {
        self.trace.lock().unwrap().retirements += 1;
        self.retained = settlements;
        self
    }
}

fn poll_once<F: Future>(execution: F) -> Poll<F::Output> {
    let mut execution = Box::pin(execution);
    let mut context = Context::from_waker(Waker::noop());
    execution.as_mut().poll(&mut context)
}

fn run(input: &[u8]) -> Option<AcquisitionPort> {
    let (initialization, activation, ordinary, plan) = decode(input);
    let trace = Arc::new(Mutex::new(ExecutionTrace::default()));
    let environment_dropped = Arc::new(AtomicUsize::new(0));
    let request_released = Arc::new(AtomicUsize::new(0));
    let payload = input.to_vec().into_boxed_slice();
    let allocation = payload.as_ptr() as usize;
    let request = RetirementRequest {
        payload,
        released: request_released.clone(),
    };
    let behavior = SettlementActor {
        initialization: initialization.clone(),
        attempted: Vec::new(),
    };
    let environment = SettlementEnvironment {
        activation,
        ordinary,
        source: VecDeque::new(),
        trace: trace.clone(),
        dropped: environment_dropped.clone(),
        plan,
        request: Some(request),
        retained: Vec::new(),
    };
    let mut execution = Box::pin(Driver::new(behavior, environment).run());
    let mut context = Context::from_waker(Waker::noop());
    let polled = execution.as_mut().poll(&mut context);
    match polled {
        Poll::Ready(mut retirement) => {
            let recorded = trace.lock().unwrap();
            let expected_attempts: Vec<_> = std::iter::once(initialization.clone())
                .chain(recorded.delivered.iter().cloned())
                .collect();
            assert_eq!(retirement.behavior.initialization, initialization);
            assert_eq!(retirement.behavior.attempted, expected_attempts);
            assert_eq!(recorded.retirements, 1);
            let expected_retained: Vec<_> = recorded
                .committed
                .iter()
                .rev()
                .filter(|receipt| {
                    !recorded
                        .discharged
                        .iter()
                        .any(|discharged| discharged.ordinal == receipt.ordinal)
                })
                .cloned()
                .collect();
            let actual_retained: Vec<_> = retirement
                .residual
                .retained
                .iter()
                .map(|settlement| {
                    assert_eq!(
                        settlement.payload.as_ptr() as usize,
                        settlement.receipt.allocation
                    );
                    assert_eq!(settlement.payload.as_ref(), settlement.receipt.payload);
                    let expected_remaining =
                        if recorded.offered.contains(&settlement.receipt.ordinal) {
                            SourceChoice::Exhaust
                        } else {
                            settlement.receipt.turn.source.clone()
                        };
                    assert_eq!(settlement.remaining, expected_remaining);
                    settlement.receipt.clone()
                })
                .collect();
            assert_eq!(
                actual_retained, expected_retained,
                "complete unoffered settlement suffix must survive acquisition"
            );
            for receipt in &recorded.discharged {
                assert_eq!(recorded.committed[receipt.ordinal], *receipt);
            }
            assert_eq!(
                actual_retained.len() + recorded.discharged.len(),
                recorded.committed.len()
            );
            for ordinal in &recorded.offered {
                assert!(*ordinal < recorded.committed.len());
            }
            match recorded.acquired.as_slice() {
                [] => {
                    assert!(!matches!(
                        retirement.disposition,
                        Ok(Completion::RetirementRequested(_))
                    ));
                    let request = retirement
                        .residual
                        .request
                        .as_ref()
                        .expect("arming alone must not acquire the request");
                    assert_eq!(request.payload.as_ptr() as usize, allocation);
                    assert_eq!(request.payload.as_ref(), input);
                    assert_eq!(request_released.load(Ordering::SeqCst), 0);
                }
                [_] => {
                    let request = match &retirement.disposition {
                        Ok(Completion::RetirementRequested(request)) => request,
                        _ => panic!(
                            "acquired retirement request must never become false exhaustion or another terminal cause"
                        ),
                    };
                    assert_eq!(request.payload.as_ptr() as usize, allocation);
                    assert_eq!(request.payload.as_ref(), input);
                    assert!(retirement.residual.request.is_none());
                    assert_eq!(request_released.load(Ordering::SeqCst), 0);
                }
                _ => panic!("an affine request cannot be acquired twice"),
            }
            let acquired_count = recorded.acquired.len();
            assert!(recorded.publications <= 1);
            drop(recorded);
            // Probe the same retired host, not a restarted or resumed Driver.
            if acquired_count == 1 {
                retirement.residual.ordinary.clear();
                retirement.residual.source.clear();
                let ordinary_replay = poll_once(retirement.residual.next());
                let source_replay = poll_once(retirement.residual.next_source());
                assert!(matches!(
                    ordinary_replay,
                    Poll::Ready(ControlFlow::Continue(None))
                ));
                assert!(matches!(
                    source_replay,
                    Poll::Ready(ControlFlow::Continue(None))
                ));
                assert_eq!(trace.lock().unwrap().acquired.len(), 1);
            }
            drop(retirement);
        }
        Poll::Pending => {
            let recorded = trace.lock().unwrap();
            assert_eq!(recorded.retirements, 0);
            assert!(recorded.acquired.is_empty());
            assert_eq!(request_released.load(Ordering::SeqCst), 0);
        }
    }
    drop(execution);
    assert_eq!(environment_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(request_released.load(Ordering::SeqCst), 1);
    trace.lock().unwrap().acquired.first().copied()
}

#[cfg(not(test))]
fuzz_target!(|input: &[u8]| {
    let _acquired_port = run(input);
});

#[cfg(test)]
mod retirement_custody {
    use super::{AcquisitionPort, run};

    #[test]
    fn source_acquisition_preserves_affine_request_and_complete_settlement_suffix() {
        // Initialization retained; an ordinary action admits a source successor.
        // Source acquisition occurs with both newest and older settlements owned.
        let source = run(&[9, 0, 0, 1, 2, 0, 0, 0, 0, 2, 0, 0]);
        assert_eq!(source, Some(AcquisitionPort::Source));
    }

    #[test]
    fn ordinary_acquisition_preserves_affine_request_and_complete_settlement_suffix() {
        // Ordinary acquisition follows initialization and an explicitly retained action.
        let ordinary = run(&[9, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0]);
        assert_eq!(ordinary, Some(AcquisitionPort::Ordinary));
    }

    #[test]
    fn arming_does_not_reclassify_stop_failure_closure_or_pending_cancellation() {
        for plan in [1, 2] {
            for decision in [1, 2] {
                let acquired = run(&[9, decision, 0, 0, plan, 0]);
                assert_eq!(acquired, None);
                let acquired = run(&[9, 0, 0, 1, plan, 0, 3, decision, 0, 0, 0, 0]);
                assert_eq!(acquired, None);
            }
            let acquired = run(&[9, 0, 0, 0, plan, 1]);
            assert_eq!(acquired, None);
            for settlement in [1, 2] {
                let acquired = run(&[9, 0, settlement, 0, plan, 0]);
                assert_eq!(acquired, None);
                let acquired = run(&[9, 0, 0, 1, plan, 0, 3, 0, settlement, 0, 0, 0]);
                assert_eq!(acquired, None);
            }
            let acquired = run(&[9, 0, 0, 3, plan, 0]);
            assert_eq!(acquired, None);
            let acquired = run(&[9, 0, 0, 4, plan, 0]);
            assert_eq!(acquired, None);
        }
    }
}
