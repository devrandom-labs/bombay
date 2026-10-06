use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::future::{pending, poll_fn};
use std::ops::ControlFlow;
use std::panic::{panic_any, resume_unwind};
use std::pin::pin;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use behavior::{
    Actions, Behavior, BehaviorActed, ClassifySettlement, Creations, Interpretation, MailAddr,
    Never, NoBirths, SettlementStatus, SourceCustody, Step, User,
};
use core::fmt::Debug;

use bombay_engine::{
    ActionsOf, ActiveEnvironment, Completion, Driver, DriverError, DriverRetirement, Environment,
    SettlementFailure,
};

#[derive(Debug, PartialEq, Eq)]
struct CustodyBehavior {
    value: u64,
    initialization_failure: Option<&'static str>,
    initialization_decision: InitializationDecision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitializationDecision {
    Continue,
    Stop,
}

impl Behavior for CustodyBehavior {
    type Protocol = behavior::MessageProtocol<MailAddr, u64>;
    type Event = User<MailAddr, u64>;
    type Sends = Vec<u64>;
    type Ph = Never;
    type Error = &'static str;
    type Birth = NoBirths;

    fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
        self.value += 1;
        if let Some(error) = self.initialization_failure {
            Err(error)
        } else {
            let decision = match self.initialization_decision {
                InitializationDecision::Continue => Step::Continue,
                InitializationDecision::Stop => Step::Stop(behavior::Stopped),
            };
            Ok(Actions::new(vec![self.value], Creations::empty(), decision))
        }
    }

    fn transition(&mut self, _: behavior::ActiveTurn, event: Self::Event) -> BehaviorActed<Self> {
        self.value += event.message;
        match event.message {
            13 => Err("transition"),
            0 => Ok(Actions::new(
                vec![self.value],
                Creations::empty(),
                Step::Stop(behavior::Stopped),
            )),
            _ => Ok(Actions::send(vec![self.value])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResidualPhase {
    Prepared,
    Active,
}

#[derive(Debug, PartialEq, Eq)]
struct Residual {
    #[expect(
        clippy::type_complexity,
        reason = "Preserve the complete typed ingress and original retirement request in the observed residual."
    )]
    acquired_ingress: Option<ControlFlow<Box<[u64]>, Option<User<MailAddr, u64>>>>,
    received_interpretation: Option<Interpretation<ActionSettlement>>,
    phase: ResidualPhase,
    committed: Vec<u64>,
    settlements: Vec<ActionSettlement>,
    remaining_events: Vec<u64>,
    publication: Publication,
    retirements: usize,
    publication_request: Option<Box<[u64]>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publication {
    Withheld,
    Published,
    Repeated,
}

#[derive(Debug, PartialEq, Eq)]
enum ActionSettlement {
    Applied(Vec<u64>),
    Unclassified {
        committed: Vec<u64>,
        classification: Arc<NativeClassification>,
    },
    AppliedWithDisposal {
        committed: Vec<u64>,
        payload: Option<Arc<NativeCause>>,
    },
    Rejected {
        committed: Vec<u64>,
        reason: &'static str,
    },
    Failed {
        committed: Vec<u64>,
        error: &'static str,
    },
}

impl ClassifySettlement for ActionSettlement {
    fn settlement_status(&self) -> SettlementStatus {
        match self {
            Self::Unclassified { classification, .. } => {
                classification
                    .observations
                    .lock()
                    .unwrap()
                    .push(ClassificationObservation::ClassificationRequested);
                let payload = classification
                    .payload
                    .lock()
                    .unwrap()
                    .take()
                    .expect("classification owns one original native cause");
                resume_unwind(payload);
            }
            Self::Applied(_) | Self::AppliedWithDisposal { .. } => SettlementStatus::Accepted,
            Self::Rejected { .. } => SettlementStatus::Rejected,
            Self::Failed { .. } => SettlementStatus::Corrupt,
        }
    }
}

impl Drop for ActionSettlement {
    fn drop(&mut self) {
        if let Self::AppliedWithDisposal { payload, .. } = self
            && let Some(payload) = payload.take()
        {
            panic_any(payload);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DisposalOrigin {
    Activation,
    SourceOffer,
    SourceIngress,
    Settlement,
}

#[derive(Debug)]
struct NativeCause {
    origin: DisposalOrigin,
    discharge: Arc<Mutex<Vec<DisposalOrigin>>>,
}

impl PartialEq for NativeCause {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && Arc::ptr_eq(&self.discharge, &other.discharge)
    }
}
impl Eq for NativeCause {}

impl Drop for NativeCause {
    fn drop(&mut self) {
        self.discharge.lock().unwrap().push(self.origin);
    }
}

struct NativeDisposal {
    payload: Option<Arc<NativeCause>>,
}

impl Drop for NativeDisposal {
    fn drop(&mut self) {
        if let Some(payload) = self.payload.take() {
            panic_any(payload);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ClassificationObservation {
    InterpretationPrepared { allocation: usize },
    ClassificationRequested,
    SourceOfferConstructed,
    SourceIngressConstructed,
    PublicationRequested,
    OrdinaryRequested,
    RetirementRequested,
    RetirementReceived,
}

#[derive(Debug)]
struct NativeClassification {
    payload: Mutex<Option<Box<dyn Any + Send>>>,
    observations: Arc<Mutex<Vec<ClassificationObservation>>>,
}

impl PartialEq for NativeClassification {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self, other)
    }
}
impl Eq for NativeClassification {}

#[derive(Clone)]
enum SettlementPlan {
    Accepted,
    ClassificationPanicked(Arc<NativeClassification>),
    AcceptedWithDisposal(Arc<NativeCause>),
    HostPanicked,
    Rejected(&'static str),
    Corrupt(&'static str),
}

fn settle_actions(committed: Vec<u64>, plan: SettlementPlan) -> Interpretation<ActionSettlement> {
    match plan {
        SettlementPlan::ClassificationPanicked(classification) => {
            classification.observations.lock().unwrap().push(
                ClassificationObservation::InterpretationPrepared {
                    allocation: committed.as_ptr() as usize,
                },
            );
            Interpretation::Complete(ActionSettlement::Unclassified {
                committed,
                classification,
            })
        }
        SettlementPlan::Accepted => Interpretation::Complete(ActionSettlement::Applied(committed)),
        SettlementPlan::AcceptedWithDisposal(payload) => {
            Interpretation::Complete(ActionSettlement::AppliedWithDisposal {
                committed,
                payload: Some(payload),
            })
        }
        SettlementPlan::HostPanicked => {
            panic!("the active host cannot complete its current action interpretation")
        }
        SettlementPlan::Rejected(reason) => {
            Interpretation::Complete(ActionSettlement::Rejected { committed, reason })
        }
        SettlementPlan::Corrupt(error) => {
            Interpretation::Corrupt(ActionSettlement::Failed { committed, error })
        }
    }
}

enum RetirementResponse {
    Complete,
    ReceiveInterpretation {
        observations: Arc<Mutex<Vec<ClassificationObservation>>>,
    },
    IncompleteRetirement {
        original: Arc<Vec<u64>>,
        callbacks: Arc<Mutex<Vec<usize>>>,
    },
    PanickedRetirement {
        original: Arc<Vec<u64>>,
        callbacks: Arc<Mutex<Vec<usize>>>,
        payload: Option<Box<dyn Any + Send>>,
    },
    PendingRetirement {
        original: Arc<Vec<u64>>,
        callbacks: Arc<Mutex<Vec<usize>>>,
    },
}

struct PreparedEnvironment {
    retirement: RetirementResponse,
    events: VecDeque<u64>,
    committed: Vec<u64>,
    activation_failure: Option<&'static str>,
    activation_disposal: Option<Arc<NativeCause>>,
    source_disposal: Option<Arc<NativeCause>>,
    source_none_disposal: Option<Arc<NativeCause>>,
    initialization_settlement: SettlementPlan,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
    publication_request: Option<Box<[u64]>>,
}

struct ActiveCustodyEnvironment {
    retirement: RetirementResponse,
    source_disposal: Option<Arc<NativeCause>>,
    source_none_disposal: Option<Arc<NativeCause>>,
    events: VecDeque<u64>,
    committed: Vec<u64>,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
    publication: Publication,
    publication_request: Option<Box<[u64]>>,
}

#[derive(Clone, Copy)]
enum SettlementCustody {
    Exhaust,
    Close,
    AdmitThenClose,
    ClosingAdmitted,
    RetainNext,
    Retained,
    AdmitThenRetain,
    FirstSourceAdmitted,
    RetainTransitive,
    OlderResidualAdmitted,
    SecondSourceAdmitted,
    ExhaustRemaining,
}

impl Environment<CustodyBehavior> for PreparedEnvironment {
    type Active = ActiveCustodyEnvironment;
    type Settlement = ActionSettlement;
    type RetirementRequest = Box<[u64]>;
    type Error = &'static str;
    type Residual = Residual;

    fn activate(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<CustodyBehavior>>,
        received: &mut Option<
            Result<(Self::Active, Interpretation<Self::Settlement>), (Self::Error, Self::Residual)>,
        >,
    ) -> impl Future<Output = ()> {
        let disposal = NativeDisposal {
            payload: environment
                .as_mut()
                .and_then(|original| original.activation_disposal.take()),
        };
        poll_fn(move |_| {
            let _ = &disposal;
            if received.is_some() || environment.is_none() || actions.is_none() {
                return Poll::Ready(());
            }
            let mut original = environment
                .take()
                .expect("original prepared environment is available");
            let actions = actions
                .take()
                .expect("original initialization actions are available");
            let committed = actions.sends;
            original.committed.extend(committed.iter().copied());
            if let Some(error) = original.activation_failure {
                *received = Some(Err((
                    error,
                    Residual {
                        received_interpretation: None,
                        acquired_ingress: None,
                        phase: ResidualPhase::Prepared,
                        committed: original.committed,
                        settlements: Vec::new(),
                        remaining_events: original.events.into(),
                        publication: Publication::Withheld,
                        retirements: 0,
                        publication_request: original.publication_request,
                    },
                )));
                return Poll::Ready(());
            }
            let interpretation = settle_actions(committed, original.initialization_settlement);
            *received = Some(Ok((
                ActiveCustodyEnvironment {
                    retirement: original.retirement,
                    source_disposal: original.source_disposal,
                    source_none_disposal: original.source_none_disposal,
                    events: original.events,
                    committed: original.committed,
                    active_settlement: original.active_settlement,
                    settlement_custody: original.settlement_custody,
                    publication: Publication::Withheld,
                    publication_request: original.publication_request,
                },
                interpretation,
            )));
            Poll::Ready(())
        })
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<CustodyBehavior>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || actions.is_some() {
            return;
        }
        if let Some(owner) = environment.as_mut() {
            match &mut owner.retirement {
                RetirementResponse::Complete => {}
                RetirementResponse::ReceiveInterpretation { .. } => return,
                RetirementResponse::IncompleteRetirement {
                    original,
                    callbacks,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    return;
                }
                RetirementResponse::PanickedRetirement {
                    original,
                    callbacks,
                    payload,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    let payload = payload
                        .take()
                        .expect("the original retirement panic is owned");
                    resume_unwind(payload);
                }
                RetirementResponse::PendingRetirement {
                    original,
                    callbacks,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    pending::<()>().await;
                    return;
                }
            }
        }
        let Some(original) = environment.take() else {
            return;
        };
        *received = Some(Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Prepared,
            committed: original.committed,
            settlements: Vec::new(),
            remaining_events: original.events.into(),
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: original.publication_request,
        });
    }
}

impl ActiveEnvironment<CustodyBehavior> for ActiveCustodyEnvironment {
    type Settlement = ActionSettlement;
    type RetirementRequest = Box<[u64]>;
    type Residual = Residual;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn next(
        &mut self,
    ) -> ControlFlow<Self::RetirementRequest, Option<<CustodyBehavior as Behavior>::Event>> {
        if let RetirementResponse::ReceiveInterpretation { observations } = &self.retirement {
            observations
                .lock()
                .unwrap()
                .push(ClassificationObservation::OrdinaryRequested);
        }
        ControlFlow::Continue(
            self.events
                .pop_front()
                .map(|event| User::new(MailAddr(7), event)),
        )
    }

    fn next_source(
        &mut self,
    ) -> impl Future<
        Output = ControlFlow<Self::RetirementRequest, Option<<CustodyBehavior as Behavior>::Event>>,
    > {
        if let RetirementResponse::ReceiveInterpretation { observations } = &self.retirement {
            observations
                .lock()
                .unwrap()
                .push(ClassificationObservation::SourceIngressConstructed);
        }
        let disposal = NativeDisposal {
            payload: self.source_none_disposal.take(),
        };
        poll_fn(move |_| {
            let _ = &disposal;
            let message = match self.settlement_custody {
                SettlementCustody::ClosingAdmitted => {
                    return Poll::Ready(ControlFlow::Continue(None));
                }
                SettlementCustody::FirstSourceAdmitted => {
                    self.settlement_custody = SettlementCustody::RetainTransitive;
                    2
                }
                SettlementCustody::SecondSourceAdmitted => {
                    self.settlement_custody = SettlementCustody::ExhaustRemaining;
                    4
                }
                _ => panic!("source input was requested without an admitted settlement"),
            };
            Poll::Ready(ControlFlow::Continue(Some(User::new(MailAddr(7), message))))
        })
    }

    async fn apply(
        &mut self,
        actions: &mut Option<ActionsOf<CustodyBehavior>>,
        received: &mut Option<Interpretation<Self::Settlement>>,
    ) {
        if received.is_some() {
            return;
        }
        let Some(actions) = actions.take() else {
            return;
        };
        let committed = actions.sends;
        self.committed.extend(committed.iter().copied());
        *received = Some(settle_actions(committed, self.active_settlement.clone()));
    }

    fn offer_next(
        &mut self,
        settlement: &mut Option<Self::Settlement>,
        received: &mut Option<SourceCustody<Self::Settlement>>,
    ) -> impl Future<Output = ()> {
        if let RetirementResponse::ReceiveInterpretation { observations } = &self.retirement {
            observations
                .lock()
                .unwrap()
                .push(ClassificationObservation::SourceOfferConstructed);
        }
        let disposal = NativeDisposal {
            payload: self.source_disposal.take(),
        };
        poll_fn(move |_| {
            let _ = &disposal;
            if received.is_some() {
                return Poll::Ready(());
            }
            let Some(settlement) = settlement.take() else {
                return Poll::Ready(());
            };
            *received = Some(match self.settlement_custody {
                SettlementCustody::Close => SourceCustody::Closed(settlement),
                SettlementCustody::AdmitThenClose => {
                    self.settlement_custody = SettlementCustody::ClosingAdmitted;
                    SourceCustody::Admitted(settlement)
                }
                SettlementCustody::ClosingAdmitted => {
                    panic!("admitted closing source was not acquired")
                }
                SettlementCustody::Exhaust | SettlementCustody::ExhaustRemaining => {
                    SourceCustody::Exhausted(settlement)
                }
                SettlementCustody::RetainNext => {
                    self.settlement_custody = SettlementCustody::Retained;
                    SourceCustody::Retained(settlement)
                }
                SettlementCustody::Retained => {
                    panic!("a retained settlement was offered to its source again")
                }
                SettlementCustody::AdmitThenRetain => {
                    self.settlement_custody = SettlementCustody::FirstSourceAdmitted;
                    SourceCustody::Admitted(settlement)
                }
                SettlementCustody::RetainTransitive => {
                    self.settlement_custody = SettlementCustody::OlderResidualAdmitted;
                    SourceCustody::Retained(settlement)
                }
                SettlementCustody::OlderResidualAdmitted => {
                    self.settlement_custody = SettlementCustody::SecondSourceAdmitted;
                    SourceCustody::Admitted(settlement)
                }
                SettlementCustody::FirstSourceAdmitted
                | SettlementCustody::SecondSourceAdmitted => {
                    panic!("another settlement was offered before the admitted source was read")
                }
            });
            Poll::Ready(())
        })
    }

    fn publish(&mut self) -> ControlFlow<Self::RetirementRequest, ()> {
        if let RetirementResponse::ReceiveInterpretation { observations } = &self.retirement {
            observations
                .lock()
                .unwrap()
                .push(ClassificationObservation::PublicationRequested);
        }
        if let Some(request) = self.publication_request.take() {
            return ControlFlow::Break(request);
        }
        self.publication = match self.publication {
            Publication::Withheld => Publication::Published,
            Publication::Published | Publication::Repeated => Publication::Repeated,
        };
        ControlFlow::Continue(())
    }

    async fn retire(
        environment: &mut Option<Self>,
        actions: &mut Option<ActionsOf<CustodyBehavior>>,
        interpretation: &mut Option<Interpretation<Self::Settlement>>,
        source: &mut Option<SourceCustody<Self::Settlement>>,
        source_index: &mut Option<usize>,
        ingress: &mut Option<
            ControlFlow<Self::RetirementRequest, Option<<CustodyBehavior as Behavior>::Event>>,
        >,
        settlements: &mut Option<Vec<Self::Settlement>>,
        received: &mut Option<Self::Residual>,
    ) {
        if received.is_some() || actions.is_some() || source.is_some() || source_index.is_some() {
            return;
        }
        if interpretation.is_some() {
            match environment.as_ref().map(|owner| &owner.retirement) {
                Some(RetirementResponse::ReceiveInterpretation { .. }) => {}
                _ => return,
            }
        }
        if environment.is_none() || settlements.is_none() {
            return;
        }
        if let Some(owner) = environment.as_mut() {
            match &mut owner.retirement {
                RetirementResponse::Complete => {}
                RetirementResponse::ReceiveInterpretation { observations } => {
                    if interpretation.is_none() {
                        return;
                    }
                    observations
                        .lock()
                        .unwrap()
                        .push(ClassificationObservation::RetirementRequested);
                }
                RetirementResponse::IncompleteRetirement {
                    original,
                    callbacks,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    return;
                }
                RetirementResponse::PanickedRetirement {
                    original,
                    callbacks,
                    payload,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    let payload = payload
                        .take()
                        .expect("the original retirement panic is owned");
                    resume_unwind(payload);
                }
                RetirementResponse::PendingRetirement {
                    original,
                    callbacks,
                } => {
                    callbacks
                        .lock()
                        .unwrap()
                        .push(Arc::as_ptr(original) as usize);
                    pending::<()>().await;
                    return;
                }
            }
        }
        let original = environment
            .take()
            .expect("original active environment is available");
        let observations = match original.retirement {
            RetirementResponse::ReceiveInterpretation { observations } => Some(observations),
            _ => None,
        };
        let received_interpretation = interpretation.take();
        let settlements = settlements
            .take()
            .expect("original ordered settlement rows are available");
        *received = Some(Residual {
            received_interpretation,
            acquired_ingress: ingress.take(),
            phase: ResidualPhase::Active,
            committed: original.committed,
            settlements,
            remaining_events: original.events.into(),
            publication: original.publication,
            retirements: 1,
            publication_request: original.publication_request,
        });
        if let Some(observations) = observations {
            observations
                .lock()
                .unwrap()
                .push(ClassificationObservation::RetirementReceived);
        }
    }
}

fn driver(
    behavior: CustodyBehavior,
    events: impl IntoIterator<Item = u64>,
    activation_failure: Option<&'static str>,
    initialization_settlement: SettlementPlan,
    active_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
) -> Driver<CustodyBehavior, PreparedEnvironment> {
    Driver::new(
        behavior,
        PreparedEnvironment {
            retirement: RetirementResponse::Complete,
            events: events.into_iter().collect(),
            committed: Vec::new(),
            activation_failure,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement,
            active_settlement,
            settlement_custody,
            publication_request: None,
        },
    )
}

fn assert_retirement(
    retirement: DriverRetirement<
        CustodyBehavior,
        Residual,
        DriverError<&'static str, &'static str>,
        Box<[u64]>,
    >,
    expected_behavior: CustodyBehavior,
    expected_residual: Residual,
    expected_disposition: Result<Completion<Box<[u64]>>, DriverError<&'static str, &'static str>>,
) {
    let expected = DriverRetirement {
        behavior: expected_behavior,
        residual: expected_residual,
        disposition: expected_disposition,
        additional_failures: Vec::new(),
    };
    assert_eq!(retirement.behavior, expected.behavior);
    assert_eq!(retirement.residual, expected.residual);
    assert_driver_disposition(&retirement.disposition, &expected.disposition);
    assert!(retirement.additional_failures.is_empty());
    drop(retirement);
}

#[tokio::test]
async fn stop_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [3, 0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 8,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![5, 8, 8],
            settlements: vec![ActionSettlement::Applied(vec![8])],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Ok(Completion::Stopped),
    );
}

#[tokio::test]
async fn exhaustion_returns_final_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [2],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![2, 4],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn behavior_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 5,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [13],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 19,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![6],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Behavior("transition")),
    );
}

#[tokio::test]
async fn initialization_failure_returns_mutated_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 8,
            initialization_failure: Some("initialization"),
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 9,
            initialization_failure: Some("initialization"),
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Prepared,
            committed: vec![],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Behavior("initialization")),
    );
}

#[tokio::test]
async fn activation_failure_returns_behavior_and_prepared_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 2,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        Some("activation"),
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 3,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Prepared,
            committed: vec![3],
            settlements: vec![],
            remaining_events: vec![],
            publication: Publication::Withheld,
            retirements: 0,
            publication_request: None,
        },
        Err(DriverError::Activation("activation")),
    );
}

#[tokio::test]
async fn apply_failure_returns_mutated_behavior_and_active_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 10,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [4],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Corrupt("apply"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 15,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![11, 15],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![15],
                error: "apply",
            }],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn retained_source_settlement_reaches_retirement_without_reoffer() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::RetainNext,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Applied(vec![1])],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn retained_transitive_head_does_not_hide_an_older_source_residual() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Accepted,
        SettlementCustody::AdmitThenRetain,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 7,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1, 3, 7],
            settlements: vec![ActionSettlement::Applied(vec![3])],
            remaining_events: vec![],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Ok(Completion::Exhausted),
    );
}

#[tokio::test]
async fn stopping_turn_corruption_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Corrupt("stopping turn"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1, 1],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![1],
                error: "stopping turn",
            }],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn stopping_turn_rejection_preserves_stop_and_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [0, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::Rejected("stopping turn"),
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1, 1],
            settlements: vec![ActionSettlement::Rejected {
                committed: vec![1],
                reason: "stopping turn",
            }],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
        Ok(Completion::Stopped),
    );
}

#[tokio::test]
async fn stopping_initialization_rejection_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        [99],
        None,
        SettlementPlan::Rejected("initialization"),
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Rejected {
                committed: vec![1],
                reason: "initialization",
            }],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Settlement(SettlementFailure::Rejected)),
    );
}

#[tokio::test]
async fn stopping_initialization_corruption_overrides_stop_and_preserves_settlement() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        [99],
        None,
        SettlementPlan::Corrupt("initialization"),
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    assert_retirement(
        retirement,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Failed {
                committed: vec![1],
                error: "initialization",
            }],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None,
        },
        Err(DriverError::Settlement(SettlementFailure::Corrupt)),
    );
}

#[tokio::test]
async fn publication_retirement_preserves_request_and_retained_initialization() {
    let request = vec![17, 29, 41].into_boxed_slice();
    let original_request = request.as_ptr();
    let retirement = Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::Complete,
            events: [0, 99].into(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::RetainNext,
            publication_request: Some(request),
        },
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    let DriverRetirement {
        behavior,
        residual,
        disposition,
        additional_failures,
    } = retirement;
    assert!(additional_failures.is_empty());
    assert_eq!(
        behavior,
        CustodyBehavior {
            value: 5,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        }
    );
    assert_eq!(
        residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![5],
            settlements: vec![ActionSettlement::Applied(vec![5])],
            remaining_events: vec![0, 99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None,
        }
    );
    let request = match disposition {
        Ok(Completion::RetirementRequested(request)) => request,
        other => panic!("publication did not return its original retirement request: {other:?}"),
    };
    assert_eq!(request.as_ptr(), original_request);
    assert_eq!(request.as_ref(), [17, 29, 41]);
    drop(request);
}

#[tokio::test]
async fn activation_rejection_preserves_unacquired_publication_request() {
    let request = vec![17, 29, 41].into_boxed_slice();
    let original_request = request.as_ptr();
    let retirement = Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::Complete,
            events: [0, 99].into(),
            committed: Vec::new(),
            activation_failure: Some("activation"),
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::RetainNext,
            publication_request: Some(request),
        },
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    let DriverRetirement {
        behavior,
        residual,
        disposition,
        additional_failures,
    } = retirement;
    assert!(additional_failures.is_empty());
    let Residual {
        acquired_ingress,
        received_interpretation,
        phase,
        committed,
        settlements,
        remaining_events,
        publication,
        retirements,
        publication_request,
    } = residual;
    assert!(received_interpretation.is_none());
    assert!(acquired_ingress.is_none());
    let request = publication_request.expect("activation rejection retains unacquired input");
    assert_eq!(request.as_ptr(), original_request);
    assert_retirement(
        DriverRetirement {
            behavior,
            residual: Residual {
                acquired_ingress,
                received_interpretation,
                phase,
                committed,
                settlements,
                remaining_events,
                publication,
                retirements,
                publication_request: None,
            },
            disposition,
            additional_failures,
        },
        CustodyBehavior {
            value: 5,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Prepared,
            committed: vec![5],
            settlements: vec![],
            remaining_events: vec![0, 99],
            publication: Publication::Withheld,
            retirements: 0,
            publication_request: None,
        },
        Err(DriverError::Activation("activation")),
    );
    assert_eq!(request.as_ref(), [17, 29, 41]);
    drop(request);
}

fn assert_driver_disposition<Request: PartialEq + Debug>(
    actual: &Result<Completion<Request>, DriverError<&'static str, &'static str>>,
    expected: &Result<Completion<Request>, DriverError<&'static str, &'static str>>,
) {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
        (Err(DriverError::Behavior(actual)), Err(DriverError::Behavior(expected)))
        | (Err(DriverError::Activation(actual)), Err(DriverError::Activation(expected))) => {
            assert_eq!(actual, expected);
        }
        (Err(DriverError::Settlement(actual)), Err(DriverError::Settlement(expected))) => {
            assert_eq!(actual, expected);
        }
        (
            Err(DriverError::InitializationPanicked(_)),
            Err(DriverError::InitializationPanicked(_)),
        ) => {}
        (actual, expected) => panic!(
            "unexpected disposition in this non-native-panic law: actual={actual:?}, expected={expected:?}"
        ),
    }
}

#[tokio::test]
async fn retained_source_settlement_survives_active_host_panic() {
    let retirement = driver(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        [7, 99],
        None,
        SettlementPlan::Accepted,
        SettlementPlan::HostPanicked,
        SettlementCustody::RetainNext,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the fixture must return its complete original retirement"));

    // The current interpretation never returned a settlement. The earlier
    // exact Retained row remained in the real Driver queue outside the catch.
    assert_eq!(
        retirement.behavior,
        CustodyBehavior {
            value: 8,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
    );
    assert_eq!(
        retirement.residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1, 8],
            settlements: vec![ActionSettlement::Applied(vec![1])],
            remaining_events: vec![99],
            publication: Publication::Published,
            retirements: 1,
            publication_request: None,
        },
    );
    assert!(matches!(
        &retirement.disposition,
        Err(DriverError::HostExecutionPanicked(_)),
    ));
    assert!(retirement.additional_failures.is_empty());
    drop(retirement);
}

fn disposal_driver(
    activation_failure: Option<&'static str>,
    initialization_settlement: SettlementPlan,
    settlement_custody: SettlementCustody,
    activation_disposal: Option<Arc<NativeCause>>,
    source_disposal: Option<Arc<NativeCause>>,
    source_none_disposal: Option<Arc<NativeCause>>,
) -> Driver<CustodyBehavior, PreparedEnvironment> {
    Driver::new(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::Complete,
            events: [99].into(),
            committed: Vec::new(),
            activation_failure,
            initialization_settlement,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody,
            activation_disposal,
            source_disposal,
            source_none_disposal,
            publication_request: None,
        },
    )
}

#[tokio::test]
async fn activation_rejection_precedes_original_producer_disposal_panic() {
    let discharge = Arc::new(Mutex::new(Vec::new()));
    let cause = Arc::new(NativeCause {
        origin: DisposalOrigin::Activation,
        discharge: discharge.clone(),
    });
    let original = Arc::downgrade(&cause);
    let retirement = disposal_driver(
        Some("activation"),
        SettlementPlan::Accepted,
        SettlementCustody::Exhaust,
        Some(cause),
        None,
        None,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the original rejected activation completed retirement"));
    assert_eq!(
        retirement.behavior,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue
        }
    );
    assert_eq!(
        retirement.residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Prepared,
            committed: vec![1],
            settlements: vec![],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 0,
            publication_request: None
        }
    );
    assert!(
        matches!(
            &retirement.disposition,
            Err(DriverError::Activation("activation"))
        ),
        "the acquired activation rejection must precede producer disposal"
    );
    assert_eq!(retirement.additional_failures.len(), 1);
    assert!(matches!(
        &retirement.additional_failures[0],
        DriverError::ActivationPanicked(_)
    ));
    assert_eq!(original.strong_count(), 1);
    assert!(discharge.lock().unwrap().is_empty());
    drop(retirement);
    assert_eq!(original.strong_count(), 0);
    assert_eq!(*discharge.lock().unwrap(), [DisposalOrigin::Activation]);
}

#[tokio::test]
async fn source_closure_precedes_original_producer_disposal_panic() {
    let discharge = Arc::new(Mutex::new(Vec::new()));
    let cause = Arc::new(NativeCause {
        origin: DisposalOrigin::SourceOffer,
        discharge: discharge.clone(),
    });
    let original = Arc::downgrade(&cause);
    let retirement = disposal_driver(
        None,
        SettlementPlan::Accepted,
        SettlementCustody::Close,
        None,
        Some(cause),
        None,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the original source closure completed retirement"));
    assert_eq!(
        retirement.behavior,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue
        }
    );
    assert_eq!(
        retirement.residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Applied(vec![1])],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None
        }
    );
    assert!(
        matches!(
            &retirement.disposition,
            Err(DriverError::Settlement(SettlementFailure::SourceClosed))
        ),
        "the acquired source closure must precede producer disposal"
    );
    assert_eq!(retirement.additional_failures.len(), 1);
    assert!(matches!(
        &retirement.additional_failures[0],
        DriverError::HostExecutionPanicked(_)
    ));
    assert_eq!(original.strong_count(), 1);
    assert!(discharge.lock().unwrap().is_empty());
    drop(retirement);
    assert_eq!(original.strong_count(), 0);
    assert_eq!(*discharge.lock().unwrap(), [DisposalOrigin::SourceOffer]);
}

#[tokio::test]
async fn source_ingress_closure_precedes_original_producer_disposal_panic() {
    let discharge = Arc::new(Mutex::new(Vec::new()));
    let cause = Arc::new(NativeCause {
        origin: DisposalOrigin::SourceIngress,
        discharge: discharge.clone(),
    });
    let original = Arc::downgrade(&cause);
    let retirement = disposal_driver(
        None,
        SettlementPlan::Accepted,
        SettlementCustody::AdmitThenClose,
        None,
        None,
        Some(cause),
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("the original closed admitted source completed retirement"));
    assert_eq!(
        retirement.behavior,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue
        }
    );
    assert_eq!(
        retirement.residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![ActionSettlement::Applied(vec![1])],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None
        }
    );
    assert!(
        matches!(
            &retirement.disposition,
            Err(DriverError::Settlement(SettlementFailure::SourceClosed))
        ),
        "the acquired admitted-source closure must precede producer disposal"
    );
    assert_eq!(retirement.additional_failures.len(), 1);
    assert!(matches!(
        &retirement.additional_failures[0],
        DriverError::HostExecutionPanicked(_)
    ));
    assert_eq!(original.strong_count(), 1);
    assert!(discharge.lock().unwrap().is_empty());
    drop(retirement);
    assert_eq!(original.strong_count(), 0);
    assert_eq!(*discharge.lock().unwrap(), [DisposalOrigin::SourceIngress]);
}

#[tokio::test]
async fn exhausted_source_preserves_producer_before_settlement_disposal_failure() {
    let discharge = Arc::new(Mutex::new(Vec::new()));
    let producer = Arc::new(NativeCause {
        origin: DisposalOrigin::SourceOffer,
        discharge: discharge.clone(),
    });
    let settlement = Arc::new(NativeCause {
        origin: DisposalOrigin::Settlement,
        discharge: discharge.clone(),
    });
    let original_producer = Arc::downgrade(&producer);
    let original_settlement = Arc::downgrade(&settlement);
    let retirement = disposal_driver(
        None,
        SettlementPlan::AcceptedWithDisposal(settlement),
        SettlementCustody::Exhaust,
        None,
        Some(producer),
        None,
    )
    .run()
    .await
    .unwrap_or_else(|_| panic!("both original disposal failures completed retirement"));
    assert_eq!(
        retirement.behavior,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue
        }
    );
    assert_eq!(
        retirement.residual,
        Residual {
            received_interpretation: None,
            acquired_ingress: None,
            phase: ResidualPhase::Active,
            committed: vec![1],
            settlements: vec![],
            remaining_events: vec![99],
            publication: Publication::Withheld,
            retirements: 1,
            publication_request: None
        }
    );
    assert!(matches!(
        &retirement.disposition,
        Err(DriverError::HostExecutionPanicked(_))
    ));
    assert_eq!(retirement.additional_failures.len(), 1);
    assert!(matches!(
        &retirement.additional_failures[0],
        DriverError::HostExecutionPanicked(_)
    ));
    assert_eq!(original_producer.strong_count(), 1);
    assert_eq!(original_settlement.strong_count(), 1);
    assert!(discharge.lock().unwrap().is_empty());
    let DriverRetirement {
        behavior: _,
        residual,
        disposition,
        additional_failures,
    } = retirement;
    drop(residual);
    drop(disposition);
    drop(additional_failures);
    assert_eq!(original_producer.strong_count(), 0);
    assert_eq!(original_settlement.strong_count(), 0);
    assert_eq!(
        *discharge.lock().unwrap(),
        [DisposalOrigin::SourceOffer, DisposalOrigin::Settlement],
        "opaque original causes must discharge in acquisition order"
    );
}

#[tokio::test]
async fn incomplete_retirement_returns_original_driver_without_cleanup_claim() {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let original = Arc::new(vec![41, 73]);
    let original_owner = Arc::downgrade(&original);
    let original_pointer = original_owner.as_ptr() as usize;
    let mut driver = Some(Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::IncompleteRetirement {
                original,
                callbacks: callbacks.clone(),
            },
            events: VecDeque::new(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::Exhaust,
            publication_request: None,
        },
    ));
    let mut received = None;
    Driver::receive_run(&mut driver, &mut received).await;
    let Some(Err(original_driver)) = received.take() else {
        panic!("incomplete retirement must return the original Driver without a residual");
    };
    let first_callbacks = callbacks.lock().unwrap().clone();
    let first_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("returned Driver owns its original environment value");
    let first_content = observed_original.as_slice().to_vec();
    let first_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    drop(original_driver);
    let final_count = original_owner.strong_count();
    assert!(driver.is_none());
    assert!(received.is_none());
    assert_eq!(first_count, 1);
    assert_eq!(first_pointer, original_pointer);
    assert_eq!(first_content, vec![41, 73]);
    assert_eq!(first_callbacks, vec![original_pointer]);
    assert_eq!(final_count, 0);
}

#[tokio::test]
async fn receiving_incomplete_driver_does_not_repeat_retirement() {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let original = Arc::new(vec![41, 73]);
    let original_owner = Arc::downgrade(&original);
    let original_pointer = original_owner.as_ptr() as usize;
    let mut driver = Some(Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::IncompleteRetirement {
                original,
                callbacks: callbacks.clone(),
            },
            events: VecDeque::new(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::Exhaust,
            publication_request: None,
        },
    ));
    let mut received = None;
    Driver::receive_run(&mut driver, &mut received).await;
    let Some(Err(original_driver)) = received.take() else {
        panic!("incomplete retirement must return the original Driver without a residual");
    };
    let first_callbacks = callbacks.lock().unwrap().clone();
    let first_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("returned Driver owns its original environment value");
    let first_content = observed_original.as_slice().to_vec();
    let first_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    driver = Some(original_driver);
    Driver::receive_run(&mut driver, &mut received).await;
    let Some(Err(original_driver)) = received.take() else {
        panic!("receiving again must preserve the incomplete original Driver");
    };
    let second_callbacks = callbacks.lock().unwrap().clone();
    let second_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("second returned Driver owns the same original value");
    let second_content = observed_original.as_slice().to_vec();
    let second_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    drop(original_driver);
    let final_count = original_owner.strong_count();
    assert!(driver.is_none());
    assert!(received.is_none());
    assert_eq!(first_count, 1);
    assert_eq!(second_count, 1);
    assert_eq!(first_pointer, original_pointer);
    assert_eq!(second_pointer, original_pointer);
    assert_eq!(first_content, vec![41, 73]);
    assert_eq!(second_content, vec![41, 73]);
    assert_eq!(first_callbacks, vec![original_pointer]);
    assert_eq!(final_count, 0);
    assert_eq!(
        second_callbacks.len(),
        1,
        "retirement callback must not be replayed"
    );
    assert_eq!(second_callbacks, vec![original_pointer]);
}

#[tokio::test]
async fn receiving_incomplete_prepared_driver_does_not_repeat_retirement() {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let original = Arc::new(vec![41, 73]);
    let original_owner = Arc::downgrade(&original);
    let original_pointer = original_owner.as_ptr() as usize;
    let mut driver = Some(Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: Some("initialization"),
            initialization_decision: InitializationDecision::Continue,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::IncompleteRetirement {
                original,
                callbacks: callbacks.clone(),
            },
            events: VecDeque::new(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::Exhaust,
            publication_request: None,
        },
    ));
    let mut received = None;
    Driver::receive_run(&mut driver, &mut received).await;
    let Some(Err(original_driver)) = received.take() else {
        panic!("incomplete retirement must return the original Driver without a residual");
    };
    let first_callbacks = callbacks.lock().unwrap().clone();
    let first_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("returned Driver owns its original environment value");
    let first_content = observed_original.as_slice().to_vec();
    let first_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    driver = Some(original_driver);
    Driver::receive_run(&mut driver, &mut received).await;
    let Some(Err(original_driver)) = received.take() else {
        panic!("receiving again must preserve the incomplete original Driver");
    };
    let second_callbacks = callbacks.lock().unwrap().clone();
    let second_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("second returned Driver owns the same original value");
    let second_content = observed_original.as_slice().to_vec();
    let second_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    drop(original_driver);
    let final_count = original_owner.strong_count();
    assert!(driver.is_none());
    assert!(received.is_none());
    assert_eq!(first_count, 1);
    assert_eq!(second_count, 1);
    assert_eq!(first_pointer, original_pointer);
    assert_eq!(second_pointer, original_pointer);
    assert_eq!(first_content, vec![41, 73]);
    assert_eq!(second_content, vec![41, 73]);
    assert_eq!(first_callbacks, vec![original_pointer]);
    assert_eq!(final_count, 0);
    assert_eq!(
        second_callbacks.len(),
        1,
        "retirement callback must not be replayed"
    );
    assert_eq!(second_callbacks, vec![original_pointer]);
}

#[tokio::test]
async fn cancelling_pending_retirement_does_not_reconstruct_its_callback() {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let original = Arc::new(vec![41, 73]);
    let original_owner = Arc::downgrade(&original);
    let original_pointer = original_owner.as_ptr() as usize;
    let mut driver = Some(Driver::new(
        CustodyBehavior {
            value: 4,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Stop,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::PendingRetirement {
                original,
                callbacks: callbacks.clone(),
            },
            events: VecDeque::new(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::Accepted,
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::Exhaust,
            publication_request: None,
        },
    ));
    let mut received = None;
    let first_poll = {
        let mut operation = pin!(Driver::receive_run(&mut driver, &mut received));
        let mut context = Context::from_waker(Waker::noop());
        operation.as_mut().poll(&mut context)
    };
    let first_callbacks = callbacks.lock().unwrap().clone();
    let first_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("cancelled borrowing work leaves its original environment outside");
    let first_content = observed_original.as_slice().to_vec();
    let first_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    let second_poll = {
        let mut operation = pin!(Driver::receive_run(&mut driver, &mut received));
        let mut context = Context::from_waker(Waker::noop());
        operation.as_mut().poll(&mut context)
    };
    let second_callbacks = callbacks.lock().unwrap().clone();
    let second_count = original_owner.strong_count();
    let observed_original = original_owner
        .upgrade()
        .expect("second receiving still owns the same original value");
    let second_content = observed_original.as_slice().to_vec();
    let second_pointer = Arc::as_ptr(&observed_original) as usize;
    drop(observed_original);
    drop(received);
    drop(driver);
    let final_count = original_owner.strong_count();
    assert_eq!(first_poll, Poll::Pending);
    assert_eq!(first_count, 1);
    assert_eq!(second_count, 1);
    assert_eq!(first_pointer, original_pointer);
    assert_eq!(second_pointer, original_pointer);
    assert_eq!(first_content, vec![41, 73]);
    assert_eq!(second_content, vec![41, 73]);
    assert_eq!(first_callbacks, vec![original_pointer]);
    assert_eq!(final_count, 0);
    assert_eq!(
        second_callbacks.len(),
        1,
        "cancelled retirement callback must not be reconstructed"
    );
    assert_eq!(second_poll, Poll::Ready(()));
    assert_eq!(second_callbacks, vec![original_pointer]);
}

#[expect(
    clippy::too_many_lines,
    reason = "observe one original native failure through both retirement phases, re-receiving and final discharge"
)]
#[tokio::test]
async fn incomplete_retirement_exposes_original_failure_without_replay() {
    for phase in [ResidualPhase::Prepared, ResidualPhase::Active] {
        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let original = Arc::new(vec![41, 73]);
        let original_owner = Arc::downgrade(&original);
        let original_pointer = original_owner.as_ptr() as usize;
        let native_cause = Arc::new(vec![17, 29]);
        let native_owner = Arc::downgrade(&native_cause);
        // This built-in opaque carrier is solely the original Rust panic payload.
        let payload: Box<dyn Any + Send> = Box::new(native_cause);
        let original_payload = ptr::from_ref(payload.as_ref());
        let (initialization_failure, initialization_decision) = match phase {
            ResidualPhase::Prepared => (Some("initialization"), InitializationDecision::Continue),
            ResidualPhase::Active => (None, InitializationDecision::Stop),
        };
        let driver = Driver::new(
            CustodyBehavior {
                value: 4,
                initialization_failure,
                initialization_decision,
            },
            PreparedEnvironment {
                retirement: RetirementResponse::PanickedRetirement {
                    original,
                    callbacks: callbacks.clone(),
                    payload: Some(payload),
                },
                events: VecDeque::new(),
                committed: Vec::new(),
                activation_failure: None,
                activation_disposal: None,
                source_disposal: None,
                source_none_disposal: None,
                initialization_settlement: SettlementPlan::Accepted,
                active_settlement: SettlementPlan::Accepted,
                settlement_custody: SettlementCustody::Exhaust,
                publication_request: None,
            },
        );
        let unselected_disposition = driver.disposition();
        let unselected_failures = driver.additional_failures();
        assert!(unselected_disposition.is_none());
        assert!(matches!(unselected_failures, []));
        let received = driver.run().await;
        let Err(original_driver) = received else {
            panic!("a pre-receipt retirement panic cannot fabricate a complete residual");
        };
        let primary = original_driver.disposition();
        match phase {
            ResidualPhase::Prepared => {
                assert!(matches!(
                    primary,
                    Some(Err(DriverError::Behavior("initialization")))
                ));
            }
            ResidualPhase::Active => {
                assert!(matches!(primary, Some(Ok(Completion::Stopped))));
            }
        }
        let failures = original_driver.additional_failures();
        let [DriverError::RetirementPanicked(cause)] = failures else {
            panic!("the complete failure lane must contain its sole original native cause");
        };
        let original_native_identity = ptr::eq(
            ptr::from_ref(cause.as_ref()).cast::<()>(),
            original_payload.cast::<()>(),
        );
        let retained_environment = original_owner.strong_count();
        let retained_native_cause = native_owner.strong_count();
        let acquired_callbacks = callbacks.lock().unwrap().clone();
        assert!(original_native_identity);
        assert_eq!(retained_environment, 1);
        assert_eq!(retained_native_cause, 1);
        assert_eq!(acquired_callbacks, vec![original_pointer]);

        let mut driver = Some(original_driver);
        let mut received = None;
        Driver::receive_run(&mut driver, &mut received).await;
        let Some(Err(original_driver)) = received.take() else {
            panic!("receiving again must return the same incomplete Driver");
        };
        let primary = original_driver.disposition();
        match phase {
            ResidualPhase::Prepared => {
                assert!(matches!(
                    primary,
                    Some(Err(DriverError::Behavior("initialization")))
                ));
            }
            ResidualPhase::Active => {
                assert!(matches!(primary, Some(Ok(Completion::Stopped))));
            }
        }
        let failures = original_driver.additional_failures();
        let [DriverError::RetirementPanicked(cause)] = failures else {
            panic!("receiving again must preserve the complete original failure lane");
        };
        let same_native_identity = ptr::eq(
            ptr::from_ref(cause.as_ref()).cast::<()>(),
            original_payload.cast::<()>(),
        );
        let callbacks_after_receiving = callbacks.lock().unwrap().clone();
        let environment_after_receiving = original_owner.strong_count();
        let native_after_receiving = native_owner.strong_count();
        assert!(same_native_identity);
        assert_eq!(callbacks_after_receiving, acquired_callbacks);
        assert_eq!(environment_after_receiving, 1);
        assert_eq!(native_after_receiving, 1);
        assert!(driver.is_none());
        assert!(received.is_none());

        drop(original_driver);
        let environment_after_discharge = original_owner.strong_count();
        let native_after_discharge = native_owner.strong_count();
        assert_eq!(environment_after_discharge, 0);
        assert_eq!(native_after_discharge, 0);
    }
}

#[tokio::test]
async fn classification_panic_preserves_original_interpretation_through_retirement() {
    let observations = Arc::new(Mutex::new(Vec::new()));
    let original_native: Box<[u64]> = vec![71, 73].into_boxed_slice();
    let native_allocation = original_native.as_ptr();
    let payload: Box<dyn Any + Send> = Box::new(original_native);
    let native_carrier = ptr::from_ref(payload.as_ref()).cast::<()>();
    let classification = Arc::new(NativeClassification {
        payload: Mutex::new(Some(payload)),
        observations: observations.clone(),
    });
    let received = Driver::new(
        CustodyBehavior {
            value: 0,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue,
        },
        PreparedEnvironment {
            retirement: RetirementResponse::ReceiveInterpretation {
                observations: observations.clone(),
            },
            events: [7, 99].into(),
            committed: Vec::new(),
            activation_failure: None,
            activation_disposal: None,
            source_disposal: None,
            source_none_disposal: None,
            initialization_settlement: SettlementPlan::ClassificationPanicked(
                classification.clone(),
            ),
            active_settlement: SettlementPlan::Accepted,
            settlement_custody: SettlementCustody::Exhaust,
            publication_request: None,
        },
    )
    .run()
    .await;
    let Ok(DriverRetirement {
        behavior,
        residual,
        disposition,
        additional_failures,
    }) = received
    else {
        panic!("active retirement must receive the complete original interpretation");
    };
    assert_eq!(
        behavior,
        CustodyBehavior {
            value: 1,
            initialization_failure: None,
            initialization_decision: InitializationDecision::Continue
        }
    );
    let Err(DriverError::HostExecutionPanicked(payload)) = disposition else {
        panic!("settlement classification keeps its native host provenance");
    };
    let native = payload
        .downcast_ref::<Box<[u64]>>()
        .expect("the original classification native cause remains concrete");
    assert_eq!(
        (
            ptr::from_ref(payload.as_ref()).cast::<()>(),
            native.as_ptr(),
            native.as_ref()
        ),
        (native_carrier, native_allocation, &[71, 73][..])
    );
    assert!(additional_failures.is_empty());
    let Residual {
        acquired_ingress: None,
        received_interpretation,
        phase: ResidualPhase::Active,
        committed,
        settlements,
        remaining_events,
        publication: Publication::Withheld,
        retirements: 1,
        publication_request: None,
    } = residual
    else {
        panic!(
            "the complete active residual owns the exact unclassified original without publication"
        );
    };
    let retained_residual = (committed, settlements, remaining_events);
    let expected_residual = (vec![1], vec![], vec![7, 99]);
    assert_eq!(retained_residual, expected_residual);
    let Some(Interpretation::Complete(ActionSettlement::Unclassified {
        committed: original,
        classification: received_classification,
    })) = received_interpretation.as_ref()
    else {
        panic!("the unchanged original complete interpretation reaches its receiving residual");
    };
    let observed = observations.lock().unwrap();
    let [
        ClassificationObservation::InterpretationPrepared { allocation },
        ClassificationObservation::ClassificationRequested,
        ClassificationObservation::RetirementRequested,
        ClassificationObservation::RetirementReceived,
    ] = observed.as_slice()
    else {
        panic!(
            "classification failure permits only the one receiving retirement, with no source or ordinary ingress"
        );
    };
    assert_eq!(
        (original.as_ptr() as usize, original.as_slice()),
        (*allocation, &[1][..])
    );
    let same_classification = Arc::ptr_eq(&classification, received_classification);
    assert!(same_classification);
    let transferred_native = classification.payload.lock().unwrap().take();
    assert!(transferred_native.is_none());
    drop(observed);
    drop(received_interpretation);
    drop(payload);
    drop(classification);
}
