//! Universal direct-Behavior Driver.
//!
//! One closed Behavior and one coherent typed environment cross this boundary.
//! Behavior's own [`Activate`] transition initializes the definition once and
//! yields its [`behavior::Active`] value. The Driver then obtains one event,
//! folds that active Behavior directly once, applies the complete action value
//! once, and only then requests another event. It contains no template,
//! routing, mailbox, scheduling, identity, retry, or machine-topology policy.

use core::future::{Future, poll_fn};
use core::ops::ControlFlow;
use core::pin::pin;
use core::task::Poll;
use std::any::Any;
use std::collections::VecDeque;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};

use behavior::{
    Actions, Behavior, ClassifySettlement, Interpretation, Never, SettlementStatus, SourceCustody,
    Step, Stopped,
};

use crate::{ActiveEnvironment, Environment};

/// The complete action value emitted by one closed Behavior decision.
pub type ActionsOf<B> = Actions<
    behavior::BehaviorAddr<B>,
    <B as Behavior>::Ph,
    <B as Behavior>::Sends,
    <B as Behavior>::Birth,
>;

/// The factual reason one Driver execution completed successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion<Request = Never> {
    /// The Behavior explicitly selected [`Step::Stop`].
    Stopped,
    /// The environment permanently exhausted its event source.
    Exhausted,
    /// The environment requested retirement before another event fold.
    /// This fact neither claims Behavior stop, source exhaustion, nor completed cleanup.
    RetirementRequested(Request),
}

/// Why the Driver could not finish custody of a complete action settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SettlementFailure {
    /// Installed initialization effects were lawfully rejected.
    #[error("installed behavior initialization effects were rejected")]
    Rejected,
    /// Action interpretation corrupted the complete retained settlement.
    #[error("action interpretation corrupted its complete settlement")]
    Corrupt,
    /// Source admission closed while a complete settlement remained in custody.
    #[error("source admission closed with a retained settlement")]
    SourceClosed,
}

/// A failure on either side of the Behavior/environment boundary.
#[derive(Debug, thiserror::Error)]
pub enum DriverError<B, A> {
    /// The Behavior rejected initialization or an event.
    #[error("behavior transition failed")]
    Behavior(#[source] B),
    /// The synchronous pure initialization fold panicked before host commitment.
    #[error("behavior initialization panicked")]
    InitializationPanicked(Box<dyn Any + Send>),
    /// A pure event fold panicked; retirement preserves its partially mutated behavior.
    #[error("behavior event fold panicked")]
    TransitionPanicked(Box<dyn Any + Send>),
    /// Active host execution unwound while its borrowed owners survived.
    #[error("active environment execution panicked")]
    HostExecutionPanicked(Box<dyn Any + Send>),
    /// The borrowing activation operation panicked before or after acquiring its reply.
    #[error("environment activation panicked")]
    ActivationPanicked(Box<dyn Any + Send>),
    /// The borrowing retirement operation panicked while its outside owners survived.
    #[error("environment retirement panicked")]
    RetirementPanicked(Box<dyn Any + Send>),
    /// A host returned without conserving its exact original input and reply.
    #[error("environment receiving contract failed")]
    InterpreterContractFailed,
    /// The prepared environment rejected initialization commitment or publication.
    #[error("environment activation failed")]
    Activation(#[source] A),
    /// Complete action-settlement custody could not lawfully continue.
    #[error("action settlement failed")]
    Settlement(#[source] SettlementFailure),
}

enum SettlementTurn<S> {
    Offer(S),
    AwaitSource(S),
    Retained(S),
}

impl<S> SettlementTurn<S> {
    fn into_settlement(self) -> S {
        match self {
            Self::Offer(settlement)
            | Self::AwaitSource(settlement)
            | Self::Retained(settlement) => settlement,
        }
    }
}

#[derive(Clone, Copy)]
enum ExecutionPhase {
    Prepared,
    Activating,
    Initializing,
    SettlingInitialization,
    Active,
    Retiring,
    RetirementAttempted,
}

#[derive(Clone, Copy)]
enum ActionDecision {
    Continue,
    Stop,
}

impl ActionDecision {
    fn from_step(step: Step<Never, Stopped>) -> Self {
        match step {
            Step::Continue => Self::Continue,
            Step::Goto(phase) => match phase {},
            Step::Stop(_) => Self::Stop,
        }
    }
}

/// Acquire the genuine reply outside its producer before disposing that producer.
///
/// All callers construct work from loans of the actual Driver fields. Constructor,
/// poll and disposal failures coexist in their original order. A failed future is
/// disposed exactly once and is never polled again. This cannot reconstruct facts
/// destroyed inside arbitrary consuming host work before its reply becomes Ready.
async fn receive_operation<F, Operation>(
    operation: Operation,
    received: &mut Option<F::Output>,
) -> Vec<Box<dyn Any + Send>>
where
    F: Future,
    Operation: FnOnce() -> F,
{
    let mut failures = Vec::new();
    if received.is_some() {
        return failures;
    }
    let future = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(future) => future,
        Err(payload) => {
            failures.push(payload);
            return failures;
        }
    };
    let disposal_failure = {
        let mut environment_reply = pin!(async {
            let mut future = pin!(future);
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(reply)) => {
                        *received = Some(reply);
                        Poll::Ready(())
                    }
                    Err(payload) => {
                        failures.push(payload);
                        Poll::Ready(())
                    }
                }
            })
            .await;
        });
        poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| {
                environment_reply.as_mut().poll(context)
            })) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(())) => Poll::Ready(None),
                Err(payload) => Poll::Ready(Some(payload)),
            }
        })
        .await
    };
    if let Some(payload) = disposal_failure {
        failures.push(payload);
    }
    failures
}

/// Complete original Behavior, residual, primary disposition and additional failures.
#[must_use = "final behavior, residual and every original failure require explicit custody"]
#[derive(Debug, PartialEq, Eq)]
pub struct DriverRetirement<B, R, E, Request = Never> {
    pub behavior: B,
    pub residual: R,
    pub disposition: Result<Completion<Request>, E>,
    /// Original construction, poll, disposal or classification failures that coexist
    /// with the primary disposition, in acquisition order.
    pub additional_failures: Vec<E>,
}

/// One affine Driver. The original prepared/active owners and current acquired
/// facts remain here while every host operation borrows them.
///
/// The private phase selects only the next causal operation. Availability slots
/// are actual affine owners, including malformed simultaneous input and reply;
/// absence never fabricates a residual, successful interpretation or exhaustion.
pub struct Driver<B, E>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    behavior: B,
    environment: Option<E>,
    active: Option<E::Active>,
    phase: ExecutionPhase,
    actions: Option<ActionsOf<B>>,
    #[expect(
        clippy::type_complexity,
        reason = "activation retains the exact active owner, interpretation, error and residual"
    )]
    activation: Option<Result<(E::Active, Interpretation<E::Settlement>), (E::Error, E::Residual)>>,
    interpretation: Option<Interpretation<E::Settlement>>,
    source_input: Option<E::Settlement>,
    source_received: Option<SourceCustody<E::Settlement>>,
    source_index: Option<usize>,
    ingress: Option<ControlFlow<E::RetirementRequest, Option<B::Event>>>,
    decision: Option<ActionDecision>,
    settlements: VecDeque<SettlementTurn<E::Settlement>>,
    retirement_input: Option<Vec<E::Settlement>>,
    residual: Option<E::Residual>,
    #[expect(
        clippy::type_complexity,
        reason = "the primary disposition preserves the exact completion request or owning error"
    )]
    disposition: Option<Result<Completion<E::RetirementRequest>, DriverError<B::Error, E::Error>>>,
    additional_failures: Vec<DriverError<B::Error, E::Error>>,
}

impl<B, E> Driver<B, E>
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    #[must_use]
    pub fn new(behavior: B, environment: E) -> Self {
        Self {
            behavior,
            environment: Some(environment),
            active: None,
            phase: ExecutionPhase::Prepared,
            actions: None,
            activation: None,
            interpretation: None,
            source_input: None,
            source_received: None,
            source_index: None,
            ingress: None,
            decision: None,
            settlements: VecDeque::new(),
            retirement_input: None,
            residual: None,
            disposition: None,
            additional_failures: Vec::new(),
        }
    }

    /// Borrow the selected primary completion or failure, if present.
    ///
    /// A disposition does not establish completed retirement or a residual.
    #[expect(
        clippy::type_complexity,
        reason = "borrow the exact existing completion request or owning error without a second product"
    )]
    pub fn disposition(
        &self,
    ) -> Option<&Result<Completion<E::RetirementRequest>, DriverError<B::Error, E::Error>>> {
        self.disposition.as_ref()
    }

    /// Borrow coexisting original failures in acquisition order without discharge.
    pub fn additional_failures(&self) -> &[DriverError<B::Error, E::Error>] {
        &self.additional_failures
    }

    fn reject(&mut self, error: DriverError<B::Error, E::Error>) {
        if self.disposition.is_none() {
            self.disposition = Some(Err(error));
        } else {
            self.additional_failures.push(error);
        }
        self.phase = ExecutionPhase::Retiring;
    }

    fn complete(&mut self, completion: Completion<E::RetirementRequest>) {
        if self.disposition.is_none() {
            self.disposition = Some(Ok(completion));
        }
        self.phase = ExecutionPhase::Retiring;
    }

    fn receive_activation_failures(&mut self, failures: Vec<Box<dyn Any + Send>>) {
        for payload in failures {
            self.reject(DriverError::ActivationPanicked(payload));
        }
    }

    fn receive_execution_failures(&mut self, failures: Vec<Box<dyn Any + Send>>) {
        for payload in failures {
            self.reject(DriverError::HostExecutionPanicked(payload));
        }
    }

    /// Run borrowed work with the actual Driver and final reply outside every
    /// disposable producer. Err returns the surviving Driver, including any
    /// acquired residual and remaining original inputs. Receiving that owner
    /// again never repeats an attempted retirement, fold or activation.
    ///
    /// Dropping this borrowing future preserves its outside owners. Dropping the
    /// complete outside Driver is a separate explicit discharge, not joined cleanup.
    ///
    /// # Panics
    ///
    /// Panics if an internal ownership invariant is violated. The acquired
    /// residual and disposition are checked before their synchronous transfer.
    #[expect(
        clippy::type_complexity,
        reason = "the complete retirement and surviving original Driver are distinct affine replies"
    )]
    pub async fn receive_run(
        driver: &mut Option<Self>,
        received: &mut Option<
            Result<
                DriverRetirement<
                    B,
                    E::Residual,
                    DriverError<B::Error, E::Error>,
                    E::RetirementRequest,
                >,
                Self,
            >,
        >,
    ) {
        if received.is_some() {
            return;
        }
        let Some(owner) = driver.as_mut() else {
            return;
        };
        owner.drive().await;
        let ready = owner.environment.is_none()
            && owner.active.is_none()
            && owner.actions.is_none()
            && owner.activation.is_none()
            && owner.interpretation.is_none()
            && owner.source_input.is_none()
            && owner.source_received.is_none()
            && owner.source_index.is_none()
            && owner.settlements.is_empty()
            && owner.ingress.is_none()
            && owner.retirement_input.is_none()
            && owner.residual.is_some()
            && owner.disposition.is_some();
        // This predicate only answers whether all real owning slots permit the
        // following synchronous move. It does not manufacture any phase fact.
        let Some(mut owner) = driver.take() else {
            return;
        };
        if ready {
            let residual = owner
                .residual
                .take()
                .expect("the acquired residual was observed");
            let disposition = owner
                .disposition
                .take()
                .expect("retirement has its original disposition");
            *received = Some(Ok(DriverRetirement {
                behavior: owner.behavior,
                residual,
                disposition,
                additional_failures: owner.additional_failures,
            }));
        } else {
            *received = Some(Err(owner));
        }
    }

    /// Convenience ownership transfer. Its Err is the exact incomplete Driver;
    /// cancellation of this whole consuming future is not retirement completion.
    ///
    /// # Errors
    ///
    /// Returns the original Driver when retirement is incomplete, preserving
    /// every surviving input, acquired reply and failure. Receiving it again never
    /// repeats an attempted retirement.
    ///
    /// # Panics
    ///
    /// Panics if the internal ownership invariant fails to produce either reply.
    #[expect(
        clippy::result_large_err,
        reason = "incomplete retirement returns the exact existing owner without another allocation"
    )]
    pub async fn run(
        self,
    ) -> Result<
        DriverRetirement<B, E::Residual, DriverError<B::Error, E::Error>, E::RetirementRequest>,
        Self,
    > {
        let mut driver = Some(self);
        let mut received = None;
        Self::receive_run(&mut driver, &mut received).await;
        received.expect("the installed original Driver produces one complete or surviving owner")
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one causal loop keeps the affine Driver slots and phase transitions under one authority"
    )]
    async fn drive(&mut self) {
        loop {
            match self.phase {
                ExecutionPhase::Prepared => {
                    self.phase = ExecutionPhase::Activating;
                    match catch_unwind(AssertUnwindSafe(|| {
                        behavior::initialize(&mut self.behavior)
                    })) {
                        Ok(Ok(actions)) => {
                            self.decision = Some(ActionDecision::from_step(actions.become_));
                            self.actions = Some(actions);
                        }
                        Ok(Err(error)) => {
                            self.reject(DriverError::Behavior(error));
                        }
                        Err(payload) => {
                            self.reject(DriverError::InitializationPanicked(payload));
                        }
                    }
                }
                ExecutionPhase::Activating => {
                    let failures = receive_operation(
                        || {
                            E::activate(
                                &mut self.environment,
                                &mut self.actions,
                                &mut self.activation,
                            )
                        },
                        &mut None,
                    )
                    .await;
                    if self.environment.is_some()
                        || self.actions.is_some()
                        || self.activation.is_none()
                    {
                        self.receive_activation_failures(failures);
                        if !matches!(self.phase, ExecutionPhase::Retiring) {
                            self.reject(DriverError::InterpreterContractFailed);
                        }
                        continue;
                    }
                    match self.activation.take() {
                        Some(Ok((active, interpretation))) => {
                            self.active = Some(active);
                            self.interpretation = Some(interpretation);
                            if !matches!(self.phase, ExecutionPhase::Retiring) {
                                self.phase = ExecutionPhase::Initializing;
                            }
                        }
                        Some(Err((error, residual))) => {
                            self.residual = Some(residual);
                            self.reject(DriverError::Activation(error));
                        }
                        None => {
                            self.reject(DriverError::InterpreterContractFailed);
                        }
                    }
                    self.receive_activation_failures(failures);
                }
                ExecutionPhase::Initializing
                | ExecutionPhase::SettlingInitialization
                | ExecutionPhase::Active => {
                    if self.interpretation.is_some() {
                        let status = catch_unwind(AssertUnwindSafe(|| {
                            self.interpretation
                                .as_ref()
                                .expect("the original interpretation is available")
                                .settlement_status()
                        }));
                        let status = match status {
                            Ok(status) => status,
                            Err(payload) => {
                                self.reject(DriverError::HostExecutionPanicked(payload));
                                continue;
                            }
                        };
                        let interpretation = self
                            .interpretation
                            .take()
                            .expect("classification borrowed this original interpretation");
                        self.settlements
                            .push_front(SettlementTurn::Offer(interpretation.into_settlement()));
                        match (status, self.decision.take()) {
                            (SettlementStatus::Corrupt, _) => {
                                self.reject(DriverError::Settlement(SettlementFailure::Corrupt));
                                continue;
                            }
                            (SettlementStatus::Rejected, _)
                                if matches!(self.phase, ExecutionPhase::Initializing) =>
                            {
                                self.reject(DriverError::Settlement(SettlementFailure::Rejected));
                                continue;
                            }
                            (_, Some(ActionDecision::Stop)) => {
                                self.complete(Completion::Stopped);
                                continue;
                            }
                            (_, Some(ActionDecision::Continue)) => {
                                if matches!(self.phase, ExecutionPhase::Initializing) {
                                    self.phase = ExecutionPhase::SettlingInitialization;
                                }
                            }
                            (_, None) => {
                                self.reject(DriverError::InterpreterContractFailed);
                                continue;
                            }
                        }
                    }
                    let Some(active) = self.active.as_mut() else {
                        self.reject(DriverError::InterpreterContractFailed);
                        continue;
                    };
                    if self.source_index.is_some() {
                        let failures = if self.source_received.is_none() {
                            receive_operation(
                                || {
                                    active.offer_next(
                                        &mut self.source_input,
                                        &mut self.source_received,
                                    )
                                },
                                &mut None,
                            )
                            .await
                        } else {
                            Vec::new()
                        };
                        if self.source_input.is_some() || self.source_received.is_none() {
                            self.receive_execution_failures(failures);
                            if !matches!(self.phase, ExecutionPhase::Retiring) {
                                self.reject(DriverError::InterpreterContractFailed);
                            }
                            continue;
                        }
                        let index = self
                            .source_index
                            .take()
                            .expect("the actual source row retained its original index");
                        match self
                            .source_received
                            .take()
                            .expect("the genuine source reply was acquired")
                        {
                            SourceCustody::Exhausted(settlement) => {
                                self.receive_execution_failures(failures);
                                if let Err(payload) =
                                    catch_unwind(AssertUnwindSafe(|| drop(settlement)))
                                {
                                    self.reject(DriverError::HostExecutionPanicked(payload));
                                }
                            }
                            SourceCustody::Retained(settlement) => {
                                self.settlements
                                    .insert(index, SettlementTurn::Retained(settlement));
                                self.receive_execution_failures(failures);
                            }
                            SourceCustody::Admitted(settlement) => {
                                self.settlements
                                    .insert(index, SettlementTurn::AwaitSource(settlement));
                                self.receive_execution_failures(failures);
                            }
                            SourceCustody::Closed(settlement) => {
                                self.settlements
                                    .insert(index, SettlementTurn::Offer(settlement));
                                self.reject(DriverError::Settlement(
                                    SettlementFailure::SourceClosed,
                                ));
                                self.receive_execution_failures(failures);
                            }
                        }
                        continue;
                    }
                    let next = self
                        .settlements
                        .iter()
                        .position(|turn| !matches!(turn, SettlementTurn::Retained(_)));
                    match next
                        .and_then(|index| self.settlements.remove(index).map(|turn| (index, turn)))
                    {
                        Some((index, SettlementTurn::Offer(settlement))) => {
                            self.source_index = Some(index);
                            self.source_input = Some(settlement);
                            continue;
                        }
                        Some((index, SettlementTurn::AwaitSource(settlement))) => {
                            self.settlements
                                .insert(index, SettlementTurn::Offer(settlement));
                            let failures =
                                receive_operation(|| active.next_source(), &mut self.ingress).await;
                            if matches!(self.ingress.as_ref(), Some(ControlFlow::Continue(None))) {
                                self.ingress = None;
                                self.reject(DriverError::Settlement(
                                    SettlementFailure::SourceClosed,
                                ));
                            }
                            self.receive_execution_failures(failures);
                            if matches!(self.phase, ExecutionPhase::Retiring) {
                                continue;
                            }
                            if self.ingress.is_none() {
                                self.reject(DriverError::InterpreterContractFailed);
                                continue;
                            }
                        }
                        Some((_, SettlementTurn::Retained(_))) => {
                            unreachable!("retained source rows are not selected")
                        }
                        None if matches!(self.phase, ExecutionPhase::SettlingInitialization) => {
                            let publication = catch_unwind(AssertUnwindSafe(|| active.publish()));
                            match publication {
                                Ok(ControlFlow::Continue(())) => {
                                    self.phase = ExecutionPhase::Active;
                                }
                                Ok(ControlFlow::Break(request)) => {
                                    self.complete(Completion::RetirementRequested(request));
                                }
                                Err(payload) => {
                                    self.reject(DriverError::HostExecutionPanicked(payload));
                                }
                            }
                            continue;
                        }
                        None => {
                            let failures =
                                receive_operation(|| active.next(), &mut self.ingress).await;
                            self.receive_execution_failures(failures);
                            if matches!(self.phase, ExecutionPhase::Retiring) {
                                continue;
                            }
                        }
                    }
                    let event = match self.ingress.take() {
                        Some(ControlFlow::Continue(Some(event))) => event,
                        Some(ControlFlow::Continue(None)) => {
                            self.complete(Completion::Exhausted);
                            continue;
                        }
                        Some(ControlFlow::Break(request)) => {
                            self.complete(Completion::RetirementRequested(request));
                            continue;
                        }
                        None => {
                            self.reject(DriverError::InterpreterContractFailed);
                            continue;
                        }
                    };
                    // A consuming application fold may destroy its supplied event;
                    // only the surviving original Behavior is promised after that call.
                    match catch_unwind(AssertUnwindSafe(|| {
                        behavior::delegate_transition(&mut self.behavior, event)
                    })) {
                        Ok(Ok(actions)) => {
                            self.decision = Some(ActionDecision::from_step(actions.become_));
                            self.actions = Some(actions);
                        }
                        Ok(Err(error)) => {
                            self.reject(DriverError::Behavior(error));
                            continue;
                        }
                        Err(payload) => {
                            self.reject(DriverError::TransitionPanicked(payload));
                            continue;
                        }
                    }
                    let Some(active) = self.active.as_mut() else {
                        self.reject(DriverError::InterpreterContractFailed);
                        continue;
                    };
                    let failures = receive_operation(
                        || active.apply(&mut self.actions, &mut self.interpretation),
                        &mut None,
                    )
                    .await;
                    self.receive_execution_failures(failures);
                    if (self.actions.is_some() || self.interpretation.is_none())
                        && !matches!(self.phase, ExecutionPhase::Retiring)
                    {
                        self.reject(DriverError::InterpreterContractFailed);
                    }
                }
                ExecutionPhase::Retiring => {
                    // A malformed source reply cannot be separated from the
                    // original input and its position in the ordered settlement.
                    // Return their actual owner before transferring either value.
                    if self.source_input.is_some() && self.source_received.is_some() {
                        return;
                    }
                    if self.retirement_input.is_none() && self.active.is_some() {
                        if let (Some(_), Some(index)) =
                            (self.source_input.as_ref(), self.source_index)
                            && let Some(settlement) = self.source_input.take()
                        {
                            self.source_index = None;
                            self.settlements
                                .insert(index, SettlementTurn::Retained(settlement));
                        }
                        self.retirement_input = Some(
                            Vec::from(mem::take(&mut self.settlements))
                                .into_iter()
                                .map(SettlementTurn::into_settlement)
                                .collect(),
                        );
                    }
                    let failures = match (self.environment.as_ref(), self.active.as_ref()) {
                        (Some(_), None) if self.residual.is_none() => {
                            self.phase = ExecutionPhase::RetirementAttempted;
                            receive_operation(
                                || {
                                    E::retire(
                                        &mut self.environment,
                                        &mut self.actions,
                                        &mut self.residual,
                                    )
                                },
                                &mut None,
                            )
                            .await
                        }
                        (None, Some(_)) if self.residual.is_none() => {
                            self.phase = ExecutionPhase::RetirementAttempted;
                            receive_operation(
                                || {
                                    <E::Active as ActiveEnvironment<B>>::retire(
                                        &mut self.active,
                                        &mut self.actions,
                                        &mut self.interpretation,
                                        &mut self.source_received,
                                        &mut self.source_index,
                                        &mut self.ingress,
                                        &mut self.retirement_input,
                                        &mut self.residual,
                                    )
                                },
                                &mut None,
                            )
                            .await
                        }
                        _ => Vec::new(),
                    };
                    for payload in failures {
                        self.additional_failures
                            .push(DriverError::RetirementPanicked(payload));
                    }
                    // Do not replay an activation or event when cleanup is incomplete.
                    // The surviving original Driver is returned synchronously next.
                    return;
                }
                ExecutionPhase::RetirementAttempted => return,
            }
        }
    }
}

#[cfg(test)]
mod source_offer_custody {
    use super::{
        Completion, Driver, DriverError, DriverRetirement, ExecutionPhase, SettlementFailure,
    };
    use crate::{ActionsOf, ActiveEnvironment, Environment};
    use behavior::{
        Actions, Behavior, BehaviorActed, Interpretation, ItemSettlement, MailAddr,
        MessageProtocol, Never, NoBirths, SourceCustody, SourceProgress, User,
    };
    use core::future::{Future, poll_fn, ready};
    use core::ops::ControlFlow;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    type SourceSettlement = ItemSettlement<Never, Vec<u64>, Never, Never>;
    type SourceTrace = Rc<RefCell<Vec<SourceObservation>>>;

    struct SourceBehavior {
        source: Option<Vec<u64>>,
        initializations: usize,
    }

    impl Behavior for SourceBehavior {
        type Protocol = MessageProtocol<MailAddr, Never>;
        type Event = User<MailAddr, Never>;
        type Sends = Vec<u64>;
        type Ph = Never;
        type Error = Never;
        type Birth = NoBirths;

        fn init(&mut self, _: behavior::InitializationTurn) -> BehaviorActed<Self> {
            self.initializations += 1;
            let source = self
                .source
                .take()
                .expect("one original initialization source");
            Ok(Actions::send(source))
        }

        fn transition(
            &mut self,
            _: behavior::ActiveTurn,
            event: Self::Event,
        ) -> BehaviorActed<Self> {
            match event.message {}
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SourceReceipt {
        Exhausted,
        Retained,
        Admitted,
        Closed,
    }

    #[derive(Clone, Copy)]
    enum SourcePermission {
        Withheld,
        Granted,
    }

    #[derive(Clone, Copy)]
    enum SourceBoundary {
        HostCustody,
        ReceivedCustody,
    }

    #[derive(Clone, Copy)]
    enum SourceLoan {
        RetainedFuture,
        FreshReceive,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum SourceObservation {
        ActivationReceived,
        OfferConstructed,
        OfferPolled,
        SourceTransferred,
        SourceReceived(SourceReceipt),
        OfferDropped,
        Published,
        OrdinaryRequested,
        SourceRequested,
        RetirementConstructed,
        RetirementPolled,
        RetirementReceived,
    }

    #[derive(Debug)]
    enum SourceRetirement {
        Admitted,
    }

    struct SourceEnvironment {
        source: Option<SourceProgress<SourceSettlement, Option<SourceSettlement>>>,
        permission: Rc<Cell<SourcePermission>>,
        boundary: SourceBoundary,
        receipt: SourceReceipt,
        trace: SourceTrace,
    }

    struct SourceResidual {
        source: Option<SourceProgress<SourceSettlement, Option<SourceSettlement>>>,
        actions: Option<ActionsOf<SourceBehavior>>,
        interpretation: Option<Interpretation<SourceSettlement>>,
        received: Option<SourceCustody<SourceSettlement>>,
        index: Option<usize>,
        ingress: Option<ControlFlow<SourceRetirement, Option<User<MailAddr, Never>>>>,
        settlements: Vec<SourceSettlement>,
    }

    impl Environment<SourceBehavior> for SourceEnvironment {
        type Active = Self;
        type Settlement = SourceSettlement;
        type RetirementRequest = SourceRetirement;
        type Error = Never;
        type Residual = SourceResidual;

        fn activate(
            environment: &mut Option<Self>,
            actions: &mut Option<ActionsOf<SourceBehavior>>,
            received: &mut Option<
                Result<(Self, Interpretation<SourceSettlement>), (Never, SourceResidual)>,
            >,
        ) -> impl Future<Output = ()> {
            let original = environment
                .take()
                .expect("original prepared source environment");
            let actions = actions
                .take()
                .expect("original pure initialization actions");
            original
                .trace
                .borrow_mut()
                .push(SourceObservation::ActivationReceived);
            *received = Some(Ok((
                original,
                Interpretation::Complete(ItemSettlement::Accepted(actions.sends)),
            )));
            ready(())
        }

        fn retire(
            _: &mut Option<Self>,
            _: &mut Option<ActionsOf<SourceBehavior>>,
            _: &mut Option<SourceResidual>,
        ) -> impl Future<Output = ()> {
            poll_fn(|_| {
                panic!("this accepted initialization never retires the prepared environment")
            })
        }
    }

    struct SourceOffer<'environment, 'input, 'received> {
        environment: &'environment mut SourceEnvironment,
        input: &'input mut Option<SourceSettlement>,
        received: &'received mut Option<SourceCustody<SourceSettlement>>,
    }

    impl Future for SourceOffer<'_, '_, '_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            let offer = self.get_mut();
            offer
                .environment
                .trace
                .borrow_mut()
                .push(SourceObservation::OfferPolled);
            if offer.received.is_some() {
                return Poll::Ready(());
            }
            if offer.environment.source.is_none() {
                let input = offer
                    .input
                    .take()
                    .expect("original input enters host custody once");
                offer.environment.source = Some(SourceProgress::Offering(Some(input)));
                offer
                    .environment
                    .trace
                    .borrow_mut()
                    .push(SourceObservation::SourceTransferred);
            }
            match (
                offer.environment.boundary,
                offer.environment.permission.get(),
            ) {
                (SourceBoundary::HostCustody, SourcePermission::Withheld) => return Poll::Pending,
                (SourceBoundary::ReceivedCustody, _) | (_, SourcePermission::Granted) => {}
            }
            let Some(SourceProgress::Offering(Some(original))) = offer.environment.source.take()
            else {
                panic!("the exact original source remains in its actual Offering owner");
            };
            let receipt = match offer.environment.receipt {
                SourceReceipt::Exhausted => SourceCustody::Exhausted(original),
                SourceReceipt::Retained => SourceCustody::Retained(original),
                SourceReceipt::Admitted => SourceCustody::Admitted(original),
                SourceReceipt::Closed => SourceCustody::Closed(original),
            };
            offer.environment.source = Some(SourceProgress::Completed(receipt));
            let Some(SourceProgress::Completed(receipt)) = offer.environment.source.take() else {
                panic!("actual complete source receipt");
            };
            *offer.received = Some(receipt);
            offer
                .environment
                .trace
                .borrow_mut()
                .push(SourceObservation::SourceReceived(offer.environment.receipt));
            match offer.environment.permission.get() {
                SourcePermission::Withheld => Poll::Pending,
                SourcePermission::Granted => Poll::Ready(()),
            }
        }
    }

    impl Drop for SourceOffer<'_, '_, '_> {
        fn drop(&mut self) {
            self.environment
                .trace
                .borrow_mut()
                .push(SourceObservation::OfferDropped);
        }
    }

    impl ActiveEnvironment<SourceBehavior> for SourceEnvironment {
        type Settlement = SourceSettlement;
        type RetirementRequest = SourceRetirement;
        type Residual = SourceResidual;

        fn next(
            &mut self,
        ) -> impl Future<Output = ControlFlow<SourceRetirement, Option<User<MailAddr, Never>>>>
        {
            self.trace
                .borrow_mut()
                .push(SourceObservation::OrdinaryRequested);
            ready(ControlFlow::Continue(None))
        }

        fn next_source(
            &mut self,
        ) -> impl Future<Output = ControlFlow<SourceRetirement, Option<User<MailAddr, Never>>>>
        {
            self.trace
                .borrow_mut()
                .push(SourceObservation::SourceRequested);
            ready(ControlFlow::Break(SourceRetirement::Admitted))
        }

        fn apply(
            &mut self,
            _: &mut Option<ActionsOf<SourceBehavior>>,
            _: &mut Option<Interpretation<SourceSettlement>>,
        ) -> impl Future<Output = ()> {
            poll_fn(|_| panic!("the source request retires without fabricating a Behavior event"))
        }

        fn offer_next(
            &mut self,
            input: &mut Option<SourceSettlement>,
            received: &mut Option<SourceCustody<SourceSettlement>>,
        ) -> impl Future<Output = ()> {
            self.trace
                .borrow_mut()
                .push(SourceObservation::OfferConstructed);
            SourceOffer {
                environment: self,
                input,
                received,
            }
        }

        fn publish(&mut self) -> ControlFlow<SourceRetirement, ()> {
            self.trace.borrow_mut().push(SourceObservation::Published);
            ControlFlow::Continue(())
        }

        fn retire(
            environment: &mut Option<Self>,
            actions: &mut Option<ActionsOf<SourceBehavior>>,
            interpretation: &mut Option<Interpretation<SourceSettlement>>,
            received_source: &mut Option<SourceCustody<SourceSettlement>>,
            index: &mut Option<usize>,
            ingress: &mut Option<ControlFlow<SourceRetirement, Option<User<MailAddr, Never>>>>,
            settlements: &mut Option<Vec<SourceSettlement>>,
            received: &mut Option<SourceResidual>,
        ) -> impl Future<Output = ()> {
            environment
                .as_ref()
                .expect("active source environment")
                .trace
                .borrow_mut()
                .push(SourceObservation::RetirementConstructed);
            poll_fn(move |_| {
                let original = environment
                    .take()
                    .expect("one active source retirement barrier");
                original
                    .trace
                    .borrow_mut()
                    .push(SourceObservation::RetirementPolled);
                *received = Some(SourceResidual {
                    source: original.source,
                    actions: actions.take(),
                    interpretation: interpretation.take(),
                    received: received_source.take(),
                    index: index.take(),
                    ingress: ingress.take(),
                    settlements: settlements.take().expect("complete original source queue"),
                });
                original
                    .trace
                    .borrow_mut()
                    .push(SourceObservation::RetirementReceived);
                Poll::Ready(())
            })
        }
    }

    fn source_fact(settlement: &SourceSettlement) -> &[u64] {
        match settlement {
            ItemSettlement::Accepted(source) => source,
            ItemSettlement::Rejected { reason, .. } => match *reason {},
            ItemSettlement::Blocked { prerequisite, .. } => match *prerequisite {},
            ItemSettlement::Corrupt { item, .. } => match *item {},
        }
    }

    fn inspect_pending_source(
        driver: &Driver<SourceBehavior, SourceEnvironment>,
        boundary: SourceBoundary,
        receipt: SourceReceipt,
        source_pointer: usize,
    ) {
        let Driver {
            behavior:
                SourceBehavior {
                    source: None,
                    initializations: 1,
                },
            environment: None,
            active: Some(active),
            phase: ExecutionPhase::SettlingInitialization,
            actions: None,
            activation: None,
            interpretation: None,
            source_input: None,
            source_received,
            source_index: Some(0),
            ingress: None,
            decision: None,
            settlements,
            retirement_input: None,
            residual: None,
            disposition: None,
            additional_failures,
        } = driver
        else {
            panic!("the full real Driver keeps its exact unpaid indexed source outside the queue");
        };
        let (source, observed_receipt) = match (boundary, active.source.as_ref(), source_received) {
            (SourceBoundary::HostCustody, Some(SourceProgress::Offering(Some(source))), None) => {
                (source, None)
            }
            (SourceBoundary::ReceivedCustody, None, Some(SourceCustody::Exhausted(source))) => {
                (source, Some(SourceReceipt::Exhausted))
            }
            (SourceBoundary::ReceivedCustody, None, Some(SourceCustody::Retained(source))) => {
                (source, Some(SourceReceipt::Retained))
            }
            (SourceBoundary::ReceivedCustody, None, Some(SourceCustody::Admitted(source))) => {
                (source, Some(SourceReceipt::Admitted))
            }
            (SourceBoundary::ReceivedCustody, None, Some(SourceCustody::Closed(source))) => {
                (source, Some(SourceReceipt::Closed))
            }
            _ => {
                panic!("actual host partial custody and acquired complete custody remain distinct")
            }
        };
        let source = source_fact(source);
        let expected_receipt = match boundary {
            SourceBoundary::HostCustody => None,
            SourceBoundary::ReceivedCustody => Some(receipt),
        };
        assert_eq!(
            (
                source.as_ptr() as usize,
                source,
                observed_receipt,
                settlements.len(),
                additional_failures.len()
            ),
            (source_pointer, &[41, 73][..], expected_receipt, 0, 0)
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exact outside owner spans source admission, loan disposal and joined retirement"
    )]
    fn receive_source_through_loan(
        boundary: SourceBoundary,
        receipt: SourceReceipt,
        loan: SourceLoan,
    ) {
        let source = vec![41, 73];
        let source_pointer = source.as_ptr() as usize;
        let permission = Rc::new(Cell::new(SourcePermission::Withheld));
        let trace = Rc::new(RefCell::new(Vec::new()));
        let mut driver = Some(Driver::new(
            SourceBehavior {
                source: Some(source),
                initializations: 0,
            },
            SourceEnvironment {
                source: None,
                permission: permission.clone(),
                boundary,
                receipt,
                trace: trace.clone(),
            },
        ));
        let mut received = None;
        let mut context = Context::from_waker(Waker::noop());
        match loan {
            SourceLoan::RetainedFuture => {
                let mut operation = Box::pin(Driver::receive_run(&mut driver, &mut received));
                let pending = operation.as_mut().poll(&mut context);
                assert_eq!(pending, Poll::Pending);
                permission.set(SourcePermission::Granted);
                let completion = operation.as_mut().poll(&mut context);
                assert_eq!(completion, Poll::Ready(()));
                drop(operation);
            }
            SourceLoan::FreshReceive => {
                let first = {
                    let mut operation = Box::pin(Driver::receive_run(&mut driver, &mut received));
                    operation.as_mut().poll(&mut context)
                };
                assert_eq!(first, Poll::Pending);
                inspect_pending_source(
                    driver.as_ref().expect("surviving original Driver"),
                    boundary,
                    receipt,
                    source_pointer,
                );
                assert!(received.is_none());
                permission.set(SourcePermission::Granted);
                let completion = {
                    let mut operation = Box::pin(Driver::receive_run(&mut driver, &mut received));
                    operation.as_mut().poll(&mut context)
                };
                assert_eq!(completion, Poll::Ready(()));
            }
        }
        let Some(Ok(DriverRetirement {
            behavior,
            residual,
            disposition,
            additional_failures,
        })) = received.take()
        else {
            panic!(
                "this actual host can finish the same original source after permission is granted"
            );
        };
        assert!(driver.is_none());
        let SourceBehavior {
            source: None,
            initializations: 1,
        } = behavior
        else {
            panic!("pure initialization transferred the original source once");
        };
        let SourceResidual {
            source: None,
            actions: None,
            interpretation: None,
            received: None,
            index: None,
            ingress: None,
            settlements,
        } = residual
        else {
            panic!(
                "the complete barrier returns all original source custody without a pending suffix"
            );
        };
        match (receipt, &disposition) {
            (SourceReceipt::Exhausted | SourceReceipt::Retained, Ok(Completion::Exhausted))
            | (
                SourceReceipt::Admitted,
                Ok(Completion::RetirementRequested(SourceRetirement::Admitted)),
            )
            | (
                SourceReceipt::Closed,
                Err(DriverError::Settlement(SettlementFailure::SourceClosed)),
            ) => {}
            _ => panic!("exact factual source disposition"),
        }
        let retained = settlements
            .iter()
            .map(|settlement| {
                let source = source_fact(settlement);
                (source.as_ptr() as usize, source.to_vec())
            })
            .collect::<Vec<_>>();
        let expected_retained = match receipt {
            SourceReceipt::Exhausted => Vec::new(),
            _ => vec![(source_pointer, vec![41, 73])],
        };
        assert_eq!(
            (retained, additional_failures.len()),
            (expected_retained, 0)
        );
        let mut expected = vec![
            SourceObservation::ActivationReceived,
            SourceObservation::OfferConstructed,
            SourceObservation::OfferPolled,
            SourceObservation::SourceTransferred,
        ];
        if matches!(boundary, SourceBoundary::ReceivedCustody) {
            expected.push(SourceObservation::SourceReceived(receipt));
        }
        match loan {
            SourceLoan::RetainedFuture => expected.push(SourceObservation::OfferPolled),
            SourceLoan::FreshReceive => {
                expected.push(SourceObservation::OfferDropped);
                if matches!(boundary, SourceBoundary::HostCustody) {
                    expected.extend([
                        SourceObservation::OfferConstructed,
                        SourceObservation::OfferPolled,
                    ]);
                }
            }
        }
        if matches!(boundary, SourceBoundary::HostCustody) {
            expected.push(SourceObservation::SourceReceived(receipt));
        }
        if !matches!(
            (loan, boundary),
            (SourceLoan::FreshReceive, SourceBoundary::ReceivedCustody)
        ) {
            expected.push(SourceObservation::OfferDropped);
        }
        match receipt {
            SourceReceipt::Exhausted | SourceReceipt::Retained => expected.extend([
                SourceObservation::Published,
                SourceObservation::OrdinaryRequested,
            ]),
            SourceReceipt::Admitted => expected.push(SourceObservation::SourceRequested),
            SourceReceipt::Closed => {}
        }
        expected.extend([
            SourceObservation::RetirementConstructed,
            SourceObservation::RetirementPolled,
            SourceObservation::RetirementReceived,
        ]);
        let observed = trace.borrow().clone();
        assert_eq!(observed, expected);
    }

    #[test]
    fn indexed_host_source_custody_finishes_through_the_same_or_a_fresh_loan() {
        for loan in [SourceLoan::RetainedFuture, SourceLoan::FreshReceive] {
            receive_source_through_loan(SourceBoundary::HostCustody, SourceReceipt::Retained, loan);
        }
    }

    #[test]
    fn acquired_source_custody_is_never_offered_again_after_a_fresh_loan() {
        for receipt in [
            SourceReceipt::Exhausted,
            SourceReceipt::Retained,
            SourceReceipt::Admitted,
            SourceReceipt::Closed,
        ] {
            for loan in [SourceLoan::RetainedFuture, SourceLoan::FreshReceive] {
                receive_source_through_loan(SourceBoundary::ReceivedCustody, receipt, loan);
            }
        }
    }
}
