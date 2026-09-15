//! Concurrent execution policies for pure representable machines.
//!
//! [`ExclusiveExecutor`] directly returns outputs to a caller that serializes
//! turns through exclusive access. [`SerializedExecutor`] provides
//! run-to-completion turns: it queues inputs
//! and does not advance the next transition until the preceding output consumer
//! returns. [`LinearizedExecutor`] advances inputs immediately under its lock,
//! then dispatches already-ordered outputs. The latter policy is appropriate
//! only when transition linearization may precede completion of earlier work.

#![deny(missing_docs)]

#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex};
use std::collections::VecDeque;
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex};

pub use crate::Machine;

#[derive(Debug)]
enum ExclusiveSeat<M> {
    Ready(M),
    Poisoned,
}

/// Observable state of an [`ExclusiveExecutor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusiveState {
    /// The successor machine is available for another turn.
    Ready,
    /// A machine transition panicked and consumed the previous machine.
    Poisoned,
}

/// Failure to recover a machine consumed by a panicking transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("executor was poisoned by a previous panic")]
pub struct ExclusivePoisoned;

/// Allocation-free execution of an affine machine through exclusive access.
///
/// A turn installs the poisoned state before calling [`Machine::step`]. If the
/// transition unwinds, no machine remains accessible and all future inputs are
/// rejected intact. Output consumption after a successful turn is outside this
/// poison boundary.
#[derive(Debug)]
pub struct ExclusiveExecutor<M: Machine> {
    seat: ExclusiveSeat<M>,
}

impl<M: Machine> ExclusiveExecutor<M> {
    /// Construct an executor containing `machine`.
    #[must_use]
    pub const fn new(machine: M) -> Self {
        Self {
            seat: ExclusiveSeat::Ready(machine),
        }
    }

    /// Execute one immediate turn and install its successor.
    ///
    /// # Errors
    ///
    /// Returns the supplied input without accepting it if an earlier transition
    /// poisoned the executor. An input consumed by a transition that panics is
    /// not recoverable.
    ///
    /// # Panics
    ///
    /// Propagates a panic from [`Machine::step`] after poisoning the executor.
    pub fn turn(&mut self, input: M::Input) -> Result<M::Output, PoisonedInput<M::Input>> {
        let machine = match core::mem::replace(&mut self.seat, ExclusiveSeat::Poisoned) {
            ExclusiveSeat::Ready(machine) => machine,
            ExclusiveSeat::Poisoned => return Err(PoisonedInput(input)),
        };
        let (output, successor) = machine.step(input);
        self.seat = ExclusiveSeat::Ready(successor);
        Ok(output)
    }

    /// Report whether a successor machine remains available.
    #[must_use]
    pub const fn state(&self) -> ExclusiveState {
        match &self.seat {
            ExclusiveSeat::Ready(_) => ExclusiveState::Ready,
            ExclusiveSeat::Poisoned => ExclusiveState::Poisoned,
        }
    }

    /// Borrow the current successor machine, or `None` if it was poisoned.
    #[must_use]
    pub const fn machine(&self) -> Option<&M> {
        match &self.seat {
            ExclusiveSeat::Ready(machine) => Some(machine),
            ExclusiveSeat::Poisoned => None,
        }
    }

    /// Recover the current successor machine.
    ///
    /// # Errors
    ///
    /// Returns [`ExclusivePoisoned`] if a panicking transition consumed it.
    pub fn into_inner(self) -> Result<M, ExclusivePoisoned> {
        match self.seat {
            ExclusiveSeat::Ready(machine) => Ok(machine),
            ExclusiveSeat::Poisoned => Err(ExclusivePoisoned),
        }
    }
}

/// Extracts small copyable evidence before an output is queued for dispatch.
pub trait OutputEvidence {
    /// Evidence returned to the submitting caller.
    type Evidence;

    /// Extract evidence without consuming the output.
    fn evidence(&self) -> Self::Evidence;
}

/// Result of waiting for a serialized turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnOutcome {
    /// The transition and its complete synchronous output handling finished.
    Completed,
    /// The executor was poisoned by a transition or output-consumer panic.
    Poisoned,
}

/// Completion receipt for one serialized input.
pub struct TurnReceipt(Arc<TurnCompletion>);

struct TurnCompletion {
    outcome: Mutex<Option<TurnOutcome>>,
    ready: Condvar,
}

impl TurnReceipt {
    /// Return the outcome without blocking, if the turn has finished.
    ///
    /// # Panics
    ///
    /// Panics if receipt synchronization was poisoned.
    #[must_use]
    pub fn outcome(&self) -> Option<TurnOutcome> {
        *self.0.outcome.lock().expect("turn receipt lock poisoned")
    }

    /// Block until the turn completes or its executor is poisoned.
    ///
    /// # Panics
    ///
    /// Panics if receipt synchronization was poisoned.
    #[must_use]
    pub fn wait(self) -> TurnOutcome {
        let mut outcome = self.0.outcome.lock().expect("turn receipt lock poisoned");
        loop {
            if let Some(outcome) = *outcome {
                return outcome;
            }
            outcome = self
                .0
                .ready
                .wait(outcome)
                .expect("turn receipt lock poisoned");
        }
    }
}

fn complete(completion: &TurnCompletion, outcome: TurnOutcome) {
    *completion
        .outcome
        .lock()
        .expect("turn receipt lock poisoned") = Some(outcome);
    // wait(self) consumes the receipt, so at most one waiter exists.
    completion.ready.notify_one();
}

/// Rejection of an input after a serialized executor was poisoned.
#[derive(Debug, thiserror::Error)]
#[error("executor was poisoned by a previous panic")]
pub struct PoisonedInput<I>(
    /// Input whose ownership was not accepted.
    pub I,
);

/// Serialized run-to-completion execution of one machine.
pub struct SerializedExecutor<M: Machine> {
    execution: Mutex<SerializedExecution<M>>,
}

struct SerializedExecution<M: Machine> {
    machine: Option<M>,
    inputs: VecDeque<(M::Input, Arc<TurnCompletion>)>,
    turn: TurnState,
}

/// Ownership phase of the serialized turn drain.
enum TurnState {
    /// No caller is draining turns.
    Idle,
    /// One caller owns the drain loop.
    Running,
    /// A transition or output-consumer panic poisoned the executor.
    Poisoned,
}

impl<M: Machine> SerializedExecutor<M> {
    /// Construct a serialized executor with an empty input queue.
    #[must_use]
    pub fn new(machine: M) -> Self {
        Self {
            execution: Mutex::new(SerializedExecution {
                machine: Some(machine),
                inputs: VecDeque::new(),
                turn: TurnState::Idle,
            }),
        }
    }

    /// Queue one input and, when this caller acquires ownership, drain turns.
    ///
    /// Reentrant and concurrent calls enqueue their input and return a receipt;
    /// they never advance a transition while an earlier output is being consumed.
    /// Only the drain owner's `consume` closure processes outputs: a caller that
    /// loses ownership has its input consumed by the owner's closure, while its
    /// receipt still reports completion of its own turn. Waiting on a receipt
    /// from inside the consumer would deadlock and must be deferred until the
    /// outer turn returns.
    ///
    /// # Errors
    ///
    /// Returns input ownership when a previous transition or consumer panicked.
    ///
    /// # Panics
    ///
    /// Propagates a machine transition or output-consumer panic after poisoning
    /// this executor and resolving every outstanding receipt.
    pub fn submit(
        &self,
        input: M::Input,
        consume: &impl Fn(M::Output),
    ) -> Result<TurnReceipt, PoisonedInput<M::Input>> {
        let completion = Arc::new(TurnCompletion {
            outcome: Mutex::new(None),
            ready: Condvar::new(),
        });
        let owns = {
            let mut execution = self.execution.lock().expect("executor lock poisoned");
            match execution.turn {
                TurnState::Poisoned => return Err(PoisonedInput(input)),
                TurnState::Running => {
                    execution.inputs.push_back((input, Arc::clone(&completion)));
                    false
                }
                TurnState::Idle => {
                    execution.inputs.push_back((input, Arc::clone(&completion)));
                    execution.turn = TurnState::Running;
                    true
                }
            }
        };
        if owns {
            self.drain(consume);
        }
        Ok(TurnReceipt(completion))
    }

    fn drain(&self, consume: &impl Fn(M::Output)) {
        let mut ownership = SerializedOwnership::new(&self.execution);
        loop {
            let Some((machine, input, completion)) = ownership.take_turn() else {
                return;
            };
            let (output, successor) = machine.step(input);
            ownership.install(successor, &completion);
            consume(output);
            complete(&completion, TurnOutcome::Completed);
            ownership.turn_completed();
        }
    }
}

struct SerializedOwnership<'a, M: Machine> {
    execution: Option<&'a Mutex<SerializedExecution<M>>>,
    active: Option<Arc<TurnCompletion>>,
}

impl<'a, M: Machine> SerializedOwnership<'a, M> {
    fn new(execution: &'a Mutex<SerializedExecution<M>>) -> Self {
        Self {
            execution: Some(execution),
            active: None,
        }
    }

    fn take_turn(&mut self) -> Option<(M, M::Input, Arc<TurnCompletion>)> {
        let execution = self.execution?;
        let mut state = execution.lock().expect("executor lock poisoned");
        let Some((input, completion)) = state.inputs.pop_front() else {
            state.turn = TurnState::Idle;
            // Normal exhaustion disarms the guard: dropping it must not poison.
            self.execution = None;
            return None;
        };
        let machine = state.machine.take().expect("executor machine missing");
        self.active = Some(Arc::clone(&completion));
        Some((machine, input, completion))
    }

    fn install(&self, machine: M, completion: &Arc<TurnCompletion>) {
        self.execution
            .expect("ownership armed")
            .lock()
            .expect("executor lock poisoned")
            .machine = Some(machine);
        debug_assert!(Arc::ptr_eq(
            self.active.as_ref().expect("active turn"),
            completion
        ));
    }

    fn turn_completed(&mut self) {
        self.active = None;
    }
}

impl<M: Machine> Drop for SerializedOwnership<'_, M> {
    fn drop(&mut self) {
        let Some(execution) = self.execution.take() else {
            return;
        };
        let mut state = execution
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.turn = TurnState::Poisoned;
        if let Some(active) = self.active.take() {
            complete(&active, TurnOutcome::Poisoned);
        }
        state
            .inputs
            .drain(..)
            .for_each(|(_, receipt)| complete(&receipt, TurnOutcome::Poisoned));
    }
}

/// Transition-linearized execution with separately ordered output dispatch.
pub struct LinearizedExecutor<M>
where
    M: Machine,
    M::Output: OutputEvidence,
    <M::Output as OutputEvidence>::Evidence: Clone,
{
    execution: Mutex<LinearizedExecution<M, M::Output, <M::Output as OutputEvidence>::Evidence>>,
}

struct LinearizedExecution<M, O, E> {
    machine: LinearizedMachine<M>,
    outputs: VecDeque<O>,
    evidence: Option<E>,
    dispatch: DispatchState,
}

enum LinearizedMachine<M> {
    Ready(M),
    Poisoned,
}

/// Ownership phase of output dispatch.
enum DispatchState {
    /// No caller is dispatching queued outputs.
    Idle,
    /// One caller owns output dispatch.
    Dispatching,
}

impl<M> LinearizedExecutor<M>
where
    M: Machine,
    M::Output: OutputEvidence,
    <M::Output as OutputEvidence>::Evidence: Clone,
{
    /// Construct an executor with an empty output queue.
    #[must_use]
    pub fn new(machine: M) -> Self {
        Self {
            execution: Mutex::new(LinearizedExecution {
                machine: LinearizedMachine::Ready(machine),
                outputs: VecDeque::new(),
                evidence: None,
                dispatch: DispatchState::Idle,
            }),
        }
    }

    /// Advance and enqueue one output at the same linearization point.
    ///
    /// # Panics
    ///
    /// Panics after synchronization poison or a transition panic that consumed
    /// the affine machine state.
    pub fn submit(&self, input: M::Input) -> <M::Output as OutputEvidence>::Evidence {
        let mut execution = self.execution.lock().expect("executor lock poisoned");
        let LinearizedMachine::Ready(machine) =
            core::mem::replace(&mut execution.machine, LinearizedMachine::Poisoned)
        else {
            panic!("executor machine poisoned");
        };
        let (output, successor) = machine.step(input);
        let evidence = output.evidence();
        execution.evidence = Some(evidence.clone());
        execution.machine = LinearizedMachine::Ready(successor);
        execution.outputs.push_back(output);
        evidence
    }

    /// Clone the evidence installed by the latest linearized transition.
    ///
    /// # Panics
    ///
    /// Panics if executor synchronization was poisoned.
    #[must_use]
    pub fn evidence(&self) -> Option<<M::Output as OutputEvidence>::Evidence> {
        self.execution
            .lock()
            .expect("executor lock poisoned")
            .evidence
            .clone()
    }

    /// Dispatch queued outputs until empty, or contribute them to another owner.
    ///
    /// [`DispatchOutcome::OwnedElsewhere`] means another caller owns dispatch
    /// and this call is fire-and-forget; it does not mean the caller's output
    /// completed. If a consumer panics, its owned output is dropped exactly once
    /// and a later call resumes with the remaining queue.
    ///
    /// # Panics
    ///
    /// Panics if executor synchronization was poisoned by a transition panic
    /// in [`LinearizedExecutor::submit`].
    pub fn dispatch_pending(&self, consume: &impl Fn(M::Output)) -> DispatchOutcome {
        let Some(mut ownership) = DispatchOwnership::acquire(&self.execution) else {
            return DispatchOutcome::OwnedElsewhere;
        };
        while let Some(output) = ownership.next() {
            consume(output);
        }
        DispatchOutcome::Drained
    }
}

/// Ownership result of one [`LinearizedExecutor::dispatch_pending`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// This call owned dispatch and drained the output queue.
    Drained,
    /// Another caller owns dispatch; queued outputs will be handled there.
    OwnedElsewhere,
}

struct DispatchOwnership<'a, M, O, E> {
    execution: Option<&'a Mutex<LinearizedExecution<M, O, E>>>,
}

impl<'a, M, O, E> DispatchOwnership<'a, M, O, E> {
    fn acquire(execution: &'a Mutex<LinearizedExecution<M, O, E>>) -> Option<Self> {
        let mut state = execution.lock().expect("executor lock poisoned");
        match state.dispatch {
            DispatchState::Dispatching => None,
            DispatchState::Idle => {
                state.dispatch = DispatchState::Dispatching;
                Some(Self {
                    execution: Some(execution),
                })
            }
        }
    }

    fn next(&mut self) -> Option<O> {
        let execution = self.execution?;
        let mut state = execution.lock().expect("executor lock poisoned");
        let output = state.outputs.pop_front();
        if output.is_none() {
            state.dispatch = DispatchState::Idle;
            // An exhausted queue releases ownership; dropping must not repeat it.
            self.execution = None;
        }
        output
    }
}

impl<M, O, E> Drop for DispatchOwnership<'_, M, O, E> {
    fn drop(&mut self) {
        if let Some(execution) = self.execution.take() {
            // This drop can run while its own `next` unwinds after another
            // thread poisoned the executor; a second panic here would abort
            // the process. Recovering the guard preserves the documented
            // panic-only contract.
            execution
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .dispatch = DispatchState::Idle;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, Condvar, Mutex, Weak};
    use std::thread;

    use crate::{Base, Topology, Vertex, VertexId};

    use super::{
        ExclusiveExecutor, ExclusiveState, LinearizedExecutor, Machine, OutputEvidence,
        SerializedExecutor, TurnOutcome, TurnReceipt,
    };

    const VERTICES: &[Vertex] = &[Vertex {
        id: VertexId(0),
        label: "ready",
    }];
    const TOPOLOGY: Topology = Topology {
        name: "test",
        initial: VertexId(0),
        vertices: VERTICES,
        transitions: &[],
    };

    #[derive(Debug)]
    struct Output(u8);

    impl OutputEvidence for Output {
        type Evidence = u8;
        fn evidence(&self) -> Self::Evidence {
            self.0
        }
    }

    fn machine() -> Base<u8, impl FnMut(u8, u8) -> (Output, u8), u8, Output> {
        Base::new(0, TOPOLOGY.validated().unwrap(), |state, input| {
            (Output(input), state + input)
        })
    }

    #[derive(Debug)]
    struct ExclusiveTestMachine {
        state: usize,
        steps: Arc<std::sync::atomic::AtomicUsize>,
        panic: bool,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct OwnedOutput(String);

    impl Machine for ExclusiveTestMachine {
        type Input = usize;
        type Output = OwnedOutput;

        fn step(self, input: Self::Input) -> (Self::Output, Self) {
            self.steps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!self.panic, "transition failure");
            let successor = Self {
                state: self.state + input,
                steps: self.steps,
                panic: false,
            };
            (OwnedOutput(format!("output-{input}")), successor)
        }

        fn describe<V: crate::Structure>(&self, visitor: &mut V) -> V::Output {
            visitor.base(TOPOLOGY)
        }
    }

    #[test]
    fn exclusive_turn_returns_output_and_installs_successor() {
        let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut executor = ExclusiveExecutor::new(ExclusiveTestMachine {
            state: 1,
            steps: Arc::clone(&steps),
            panic: false,
        });

        assert_eq!(executor.state(), ExclusiveState::Ready);
        assert_eq!(executor.machine().unwrap().state, 1);
        assert_eq!(executor.turn(2).unwrap(), OwnedOutput("output-2".into()));
        assert_eq!(executor.machine().unwrap().state, 3);
        assert_eq!(executor.turn(4).unwrap(), OwnedOutput("output-4".into()));
        assert_eq!(executor.into_inner().unwrap().state, 7);
        assert_eq!(steps.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn exclusive_transition_panic_permanently_refuses_later_input() {
        let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut executor = ExclusiveExecutor::new(ExclusiveTestMachine {
            state: 0,
            steps: Arc::clone(&steps),
            panic: true,
        });

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = executor.turn(1);
            }))
            .is_err()
        );
        assert_eq!(executor.state(), ExclusiveState::Poisoned);
        assert!(executor.machine().is_none());
        let Err(rejected) = executor.turn(9) else {
            panic!("poisoned executor accepted input")
        };
        assert_eq!(rejected.0, 9);
        assert_eq!(steps.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            executor.into_inner(),
            Err(super::ExclusivePoisoned)
        ));
    }

    #[test]
    fn exclusive_executor_inherits_machine_auto_traits() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ExclusiveExecutor<ExclusiveTestMachine>>();
    }

    #[derive(Debug)]
    struct DropSentinel(Arc<std::sync::atomic::AtomicUsize>);

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct TrackedInput {
        id: usize,
        _drop: DropSentinel,
    }

    #[derive(Debug)]
    struct TrackedOutput {
        id: usize,
        _drop: DropSentinel,
    }

    #[derive(Debug)]
    struct OwnershipMachine {
        panic: bool,
        steps: Arc<std::sync::atomic::AtomicUsize>,
        _machine_drop: DropSentinel,
        successor_drops: Arc<std::sync::atomic::AtomicUsize>,
        output_drops: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Machine for OwnershipMachine {
        type Input = TrackedInput;
        type Output = TrackedOutput;

        fn step(self, input: Self::Input) -> (Self::Output, Self) {
            self.steps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(!self.panic, "transition failure");
            let output = TrackedOutput {
                id: input.id,
                _drop: DropSentinel(Arc::clone(&self.output_drops)),
            };
            drop(input);
            let successor = Self {
                panic: false,
                steps: Arc::clone(&self.steps),
                _machine_drop: DropSentinel(Arc::clone(&self.successor_drops)),
                successor_drops: self.successor_drops,
                output_drops: self.output_drops,
            };
            (output, successor)
        }

        fn describe<V: crate::Structure>(&self, visitor: &mut V) -> V::Output {
            visitor.base(TOPOLOGY)
        }
    }

    #[test]
    fn exclusive_success_moves_each_owned_payload_exactly_once() {
        let original_machine_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let successor_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let input_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let output_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut executor = ExclusiveExecutor::new(OwnershipMachine {
            panic: false,
            steps: Arc::clone(&steps),
            _machine_drop: DropSentinel(Arc::clone(&original_machine_drops)),
            successor_drops: Arc::clone(&successor_drops),
            output_drops: Arc::clone(&output_drops),
        });

        let output = executor
            .turn(TrackedInput {
                id: 41,
                _drop: DropSentinel(Arc::clone(&input_drops)),
            })
            .unwrap();
        assert_eq!(output.id, 41);
        assert_eq!(steps.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            original_machine_drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(input_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(successor_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(output_drops.load(std::sync::atomic::Ordering::SeqCst), 0);

        drop(output);
        drop(executor.into_inner().unwrap());
        assert_eq!(output_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(successor_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn exclusive_panic_consumes_active_values_but_returns_later_input() {
        let machine_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_input_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let rejected_input_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut executor = ExclusiveExecutor::new(OwnershipMachine {
            panic: true,
            steps: Arc::clone(&steps),
            _machine_drop: DropSentinel(Arc::clone(&machine_drops)),
            successor_drops: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            output_drops: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = executor.turn(TrackedInput {
                    id: 1,
                    _drop: DropSentinel(Arc::clone(&accepted_input_drops)),
                });
            }))
            .is_err()
        );
        assert_eq!(machine_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            accepted_input_drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let Err(rejected) = executor.turn(TrackedInput {
            id: 73,
            _drop: DropSentinel(Arc::clone(&rejected_input_drops)),
        }) else {
            panic!("poisoned executor accepted input")
        };
        assert_eq!(rejected.0.id, 73);
        assert_eq!(steps.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            rejected_input_drops.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(rejected);
        assert_eq!(
            rejected_input_drops.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        drop(executor);
        assert_eq!(machine_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn poisoned_input_reports_the_rejection() {
        assert_eq!(
            super::PoisonedInput(7_u8).to_string(),
            "executor was poisoned by a previous panic"
        );
    }

    #[test]
    fn serialized_turns_finish_effects_before_the_next_transition() {
        let executor = SerializedExecutor::new(machine());
        let trace = Mutex::new(Vec::new());
        let receipt = executor
            .submit(1, &|output: Output| trace.lock().unwrap().push(output.0))
            .unwrap();
        assert_eq!(receipt.wait(), TurnOutcome::Completed);
        assert_eq!(*trace.lock().unwrap(), [1]);
    }

    type TestMachine = Base<u8, fn(u8, u8) -> (Output, u8), u8, Output>;

    struct ReentrantConsumer {
        executor: Weak<SerializedExecutor<TestMachine>>,
        trace: Arc<Mutex<Vec<u8>>>,
    }

    impl ReentrantConsumer {
        fn consume(&self, Output(value): Output) {
            self.trace.lock().unwrap().push(value);
            if value == 1 {
                let executor = self.executor.upgrade().unwrap();
                let receipt = executor.submit(2, &|output| self.consume(output)).unwrap();
                assert_eq!(receipt.outcome(), None);
            }
        }
    }

    #[test]
    fn reentrant_serialized_submission_waits_for_current_consumer() {
        fn transition(state: u8, input: u8) -> (Output, u8) {
            (Output(input), state + input)
        }
        let executor = Arc::new(SerializedExecutor::new(Base::new(
            0,
            TOPOLOGY.validated().unwrap(),
            transition as fn(u8, u8) -> (Output, u8),
        )));
        let trace = Arc::new(Mutex::new(Vec::new()));
        let consumer = ReentrantConsumer {
            executor: Arc::downgrade(&executor),
            trace: Arc::clone(&trace),
        };
        assert_eq!(
            executor
                .submit(1, &|output| consumer.consume(output))
                .unwrap()
                .wait(),
            TurnOutcome::Completed
        );
        assert_eq!(*trace.lock().unwrap(), [1, 2]);
    }

    #[test]
    fn linearized_dispatch_resumes_after_consumer_panic() {
        let executor = LinearizedExecutor::new(machine());
        assert_eq!(executor.submit(1), 1);
        assert_eq!(executor.submit(2), 2);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                executor.dispatch_pending(&|_: Output| panic!("consumer"));
            }))
            .is_err()
        );
        let seen = Mutex::new(Vec::new());
        assert_eq!(
            executor.dispatch_pending(&|output: Output| seen.lock().unwrap().push(output.0)),
            super::DispatchOutcome::Drained
        );
        assert_eq!(*seen.lock().unwrap(), [2]);
    }

    #[test]
    fn serialized_consumer_panic_poisons_future_submissions() {
        let executor = SerializedExecutor::new(machine());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = executor.submit(1, &|_: Output| panic!("consumer"));
            }))
            .is_err()
        );
        let Err(rejected) = executor.submit(2, &|_: Output| {}) else {
            panic!("poisoned executor accepted input")
        };
        assert_eq!(rejected.0, 2);
    }

    struct ReentrantPoisonConsumer {
        executor: Weak<SerializedExecutor<TestMachine>>,
        queued: Mutex<Option<TurnReceipt>>,
    }

    impl ReentrantPoisonConsumer {
        fn consume(&self, Output(value): Output) {
            if value == 1 {
                let receipt = self
                    .executor
                    .upgrade()
                    .unwrap()
                    .submit(2, &|output| self.consume(output))
                    .unwrap();
                *self.queued.lock().unwrap() = Some(receipt);
                panic!("consumer");
            }
        }
    }

    #[test]
    fn serialized_consumer_panic_resolves_queued_receipt_as_poisoned() {
        fn transition(state: u8, input: u8) -> (Output, u8) {
            (Output(input), state + input)
        }
        let executor = Arc::new(SerializedExecutor::new(Base::new(
            0,
            TOPOLOGY.validated().unwrap(),
            transition as fn(u8, u8) -> (Output, u8),
        )));
        let consumer = ReentrantPoisonConsumer {
            executor: Arc::downgrade(&executor),
            queued: Mutex::new(None),
        };

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                executor.submit(1, &|output| consumer.consume(output))
            }))
            .is_err()
        );
        let queued = consumer.queued.lock().unwrap().take().unwrap();
        assert_eq!(queued.outcome(), Some(TurnOutcome::Poisoned));
        assert_eq!(queued.wait(), TurnOutcome::Poisoned);
        assert!(matches!(
            executor.submit(3, &|output| consumer.consume(output)),
            Err(super::PoisonedInput(3))
        ));
    }

    impl super::OutputEvidence for usize {
        type Evidence = usize;

        fn evidence(&self) -> Self::Evidence {
            *self
        }
    }

    #[test]
    fn dispatch_guard_drop_recovers_during_poison_unwind() {
        let machine = Base::new(0_usize, TOPOLOGY.validated().unwrap(), |state, input| {
            assert_ne!(input, 9, "transition failure");
            (input, state + input)
        });
        let executor = Arc::new(LinearizedExecutor::new(machine));
        assert_eq!(executor.submit(1), 1);
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));

        let dispatcher = {
            let executor = Arc::clone(&executor);
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                catch_unwind(AssertUnwindSafe(|| {
                    executor.dispatch_pending(&|_output| {
                        let (lock, ready) = &*gate;
                        let mut phase = lock.lock().unwrap();
                        phase.0 = true;
                        ready.notify_one();
                        // Hold dispatch ownership until the transition panic
                        // has landed; returning loops into next(), which then
                        // observes the poisoned executor.
                        while !phase.1 {
                            phase = ready.wait(phase).unwrap();
                        }
                    });
                }))
            })
        };
        {
            let (lock, ready) = &*gate;
            let mut phase = lock.lock().unwrap();
            while !phase.0 {
                phase = ready.wait(phase).unwrap();
            }
        }
        // The dispatcher holds dispatch ownership inside the consumer; this
        // transition panic poisons the executor underneath it.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            executor.submit(9);
        }));
        {
            let (lock, ready) = &*gate;
            *lock.lock().unwrap() = (true, true);
            ready.notify_one();
        }
        let outcome = dispatcher.join().expect("dispatcher thread aborted");
        assert!(outcome.is_err(), "next() must observe the poisoned lock");
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                executor.submit(2);
            }))
            .is_err()
        );
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    use crate::{Base, Topology, Vertex, VertexId};

    use super::{LinearizedExecutor, OutputEvidence, SerializedExecutor, TurnOutcome};

    const VERTICES: &[Vertex] = &[Vertex {
        id: VertexId(0),
        label: "ready",
    }];
    const TOPOLOGY: Topology = Topology {
        name: "loom",
        initial: VertexId(0),
        vertices: VERTICES,
        transitions: &[],
    };

    struct Output(usize, Arc<AtomicUsize>);

    impl Drop for Output {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl OutputEvidence for Output {
        type Evidence = usize;

        fn evidence(&self) -> Self::Evidence {
            self.0
        }
    }

    #[test]
    fn real_linearized_executor_handles_submit_dispatch_boundary() {
        loom::model(|| {
            let drops = Arc::new(AtomicUsize::new(0));
            let machine_drops = Arc::clone(&drops);
            let machine = Base::new(0, TOPOLOGY.validated().unwrap(), move |state, input| {
                (Output(input, Arc::clone(&machine_drops)), state + input)
            });
            let executor = Arc::new(LinearizedExecutor::new(machine));
            let seen = Arc::new(Mutex::new(Vec::new()));

            let submitter = {
                let executor = Arc::clone(&executor);
                thread::spawn(move || {
                    executor.submit(1);
                    executor.submit(2);
                })
            };
            let dispatcher = {
                let executor = Arc::clone(&executor);
                let seen = Arc::clone(&seen);
                thread::spawn(move || {
                    executor.dispatch_pending(&|output: Output| {
                        seen.lock().unwrap().push(output.0);
                    });
                })
            };
            submitter.join().unwrap();
            dispatcher.join().unwrap();
            executor.dispatch_pending(&|output: Output| {
                seen.lock().unwrap().push(output.0);
            });
            assert_eq!(*seen.lock().unwrap(), [1, 2]);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        });
    }

    #[test]
    fn real_serialized_executor_keeps_each_turn_contiguous() {
        loom::model(|| {
            let trace = Arc::new(Mutex::new(Vec::new()));
            let machine_trace = Arc::clone(&trace);
            let machine = Base::new((), TOPOLOGY.validated().unwrap(), move |(), input| {
                machine_trace.lock().unwrap().push(input * 10);
                (input, ())
            });
            let executor = Arc::new(SerializedExecutor::new(machine));
            let mut threads = Vec::new();
            for input in [1, 2] {
                let executor = Arc::clone(&executor);
                let trace = Arc::clone(&trace);
                threads.push(thread::spawn(move || {
                    let receipt = executor
                        .submit(input, &|output| {
                            trace.lock().unwrap().push(output * 10 + 1);
                        })
                        .unwrap();
                    assert_eq!(receipt.wait(), TurnOutcome::Completed);
                }));
            }
            threads
                .into_iter()
                .for_each(|thread| thread.join().unwrap());
            let trace = trace.lock().unwrap();
            assert!(matches!(
                trace.as_slice(),
                [10, 11, 20, 21] | [20, 21, 10, 11]
            ));
        });
    }
}
