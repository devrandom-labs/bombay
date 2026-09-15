use bombay::behavior::{
    BehaviorActed, InterpreterRequests, PoolAssignment, PoolCompletion, ReportToParent,
    StopOnShutdown,
};
use bombay::prelude::Actions;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchJob {
    pub document: String,
    pub needle: char,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchResult {
    pub matches: usize,
}

pub struct SearchWorker;

#[bombay::actor(
    sends = {
        completion: InterpreterRequests<ReportToParent<PoolCompletion<SearchResult>>>,
    },
)]
impl SearchWorker {
    fn receive(&mut self, assignment: PoolAssignment<SearchJob>) -> BehaviorActed<Self> {
        let matches = assignment
            .payload
            .document
            .chars()
            .filter(|character| *character == assignment.payload.needle)
            .count();
        Ok(
            Actions::cont().send_completion(ReportToParent::new(PoolCompletion {
                assignment: assignment.assignment,
                result: SearchResult { matches },
            })),
        )
    }
}

pub type ManagedSearchWorker = StopOnShutdown<SearchWorker>;

#[allow(
    clippy::unnecessary_wraps,
    reason = "ChildTopology's owner contract models a potentially vacant worker slot"
)]
pub fn managed_search_worker(_: usize) -> Option<ManagedSearchWorker> {
    Some(StopOnShutdown::new(SearchWorker))
}

#[cfg(test)]
mod tests {
    use bombay::behavior::{Activate as _, AssignmentId, JobId, Step};
    use bombay::prelude::MailAddr;

    use super::*;

    #[test]
    fn one_assignment_returns_its_exact_completion_through_actions() {
        let initialized = SearchWorker
            .initialize()
            .expect("the worker initialization is infallible");
        assert!(initialized.actions.sends.completion.is_empty());
        assert!(initialized.actions.creates.is_empty());
        assert_eq!(initialized.actions.become_, Step::Continue);

        let mut worker = initialized.behavior;
        let assignment = PoolAssignment {
            assignment: AssignmentId(13),
            job: JobId(41),
            payload: SearchJob {
                document: "three e characters".to_owned(),
                needle: 'e',
            },
        };
        let actions = worker
            .receive(MailAddr(9), assignment)
            .expect("the worker transition is infallible");

        assert_eq!(
            actions.sends.completion.as_slice(),
            [ReportToParent::new(PoolCompletion {
                assignment: AssignmentId(13),
                result: SearchResult { matches: 4 },
            })]
        );
        assert!(actions.creates.is_empty());
        assert_eq!(actions.become_, Step::Continue);
    }
}
