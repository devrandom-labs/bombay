use bombay::atomic::{Assignment, pool_worker};
use bombay::behavior::Actions;
use bombay::prelude::MailAddr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SearchJob {
    pub(crate) document: String,
    pub(crate) needle: char,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SearchResult {
    pub(crate) matches: usize,
}

pub(crate) struct SearchWorker;

#[pool_worker(addr = MailAddr, result = SearchResult)]
#[allow(
    clippy::unused_self,
    clippy::unnecessary_wraps,
    reason = "the pool-worker contract is a state-capable, fallible Behavior fold"
)]
impl SearchWorker {
    fn transition(&mut self, assignment: Assignment<SearchJob>) -> WorkerActed<Self> {
        let job = assignment.payload();
        let matches = job
            .document
            .chars()
            .filter(|character| *character == job.needle)
            .count();
        Ok(Actions::cont().with_send(assignment.complete(SearchResult { matches })))
    }
}
