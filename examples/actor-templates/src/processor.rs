use bombay::behavior::Move;

#[derive(Clone)]
pub(crate) enum ProcessorMessage {
    Work(u64),
    Open,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessorPhase {
    Closed,
    Open,
}

#[derive(Clone)]
pub(crate) struct ProcessorState {
    completed: Vec<u64>,
}

impl ProcessorState {
    pub(crate) const fn new() -> Self {
        Self {
            completed: Vec::new(),
        }
    }

    pub(crate) fn completed(&self, work: u64) -> bool {
        self.completed == [work]
    }
}

#[derive(Debug)]
pub(crate) enum ProcessorError {
    AlreadyOpen,
}

pub(crate) fn transition(
    phase: ProcessorPhase,
    state: &mut ProcessorState,
    message: &ProcessorMessage,
) -> Result<Move<ProcessorPhase>, ProcessorError> {
    match (phase, message) {
        (ProcessorPhase::Closed, ProcessorMessage::Work(_)) => Ok(Move::Defer),
        (ProcessorPhase::Closed, ProcessorMessage::Open) => Ok(Move::Goto(ProcessorPhase::Open)),
        (ProcessorPhase::Open, ProcessorMessage::Work(work)) => {
            state.completed.push(*work);
            Ok(Move::Stay)
        }
        (ProcessorPhase::Open, ProcessorMessage::Open) => Err(ProcessorError::AlreadyOpen),
    }
}
