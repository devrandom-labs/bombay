//! One task-termination edge paired with its published actor outcome.

use observe::Observation;

use super::TaskOutcome;

/// Completion of one exact running incarnation.
///
/// The Tokio task is only the temporal edge proving all task-owned resources
/// have dropped. [`TaskOutcome`] is published separately by the incarnation's
/// retirement guard, so executor join errors never become actor semantics.
pub(crate) struct Completion<T> {
    task: tokio::task::JoinHandle<()>,
    outcome: Observation<TaskOutcome<T>>,
}

impl<T> Completion<T> {
    pub(crate) const fn new(
        task: tokio::task::JoinHandle<()>,
        outcome: Observation<TaskOutcome<T>>,
    ) -> Self {
        Self { task, outcome }
    }

    pub(crate) async fn wait(self) -> TaskOutcome<T> {
        let _task_result = self.task.await;
        self.outcome
            .into_outcome()
            .expect("terminal actor task must publish one outcome before completion")
    }
}
