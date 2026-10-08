use core::future::{Future, pending};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinSet};

impl<E> ActivationTasks<E> {
    pub(crate) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub(crate) fn spawn(&mut self, task: impl Future<Output = Result<(), E>> + Send + 'static)
    where
        E: Send + 'static,
    {
        self.tasks.spawn(task);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(crate) async fn next_event(&mut self) -> Result<E, JoinError>
    where
        E: 'static,
    {
        loop {
            match self.tasks.join_next().await {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(event))) => return Ok(event),
                Some(Err(failure)) => return Err(failure),
                None => pending().await,
            }
        }
    }

    /// Receive every original joined capability result into its surviving local lanes.
    ///
    /// The original task set and both receiving lanes remain outside this future.
    pub(crate) async fn receive_settlement(
        &mut self,
        control: &mut Vec<E>,
        failures: &mut Vec<JoinError>,
    ) where
        E: 'static,
    {
        while let Some(completed) = self.tasks.join_next().await {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(event)) => control.push(event),
                Err(failure) => failures.push(failure),
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn settle(mut self) -> (Vec<E>, Vec<JoinError>)
    where
        E: 'static,
    {
        let mut control = Vec::new();
        let mut failures = Vec::new();
        self.receive_settlement(&mut control, &mut failures).await;
        (control, failures)
    }
}

/// Actor-owned external activation work with exact closed-lane recovery.
pub(crate) struct ActivationTasks<E> {
    tasks: JoinSet<Result<(), E>>,
}

/// Affine fact that the actor's task owner requested forced retirement.
pub(crate) struct OwnerCancellation;

/// One acquired primary retirement cause, owned exactly once by Driver completion.
pub(crate) enum LocalRetirementRequest {
    OwnerCancellation(OwnerCancellation),
    CapabilityFailed(JoinError),
}

pub(in crate::local) fn close_owner_cancellation(
    mut receiver: oneshot::Receiver<OwnerCancellation>,
) -> Option<()> {
    receiver.close();
    match receiver.try_recv() {
        Ok(OwnerCancellation) => Some(()),
        Err(oneshot::error::TryRecvError::Empty | oneshot::error::TryRecvError::Closed) => None,
    }
}

#[cfg(test)]
mod capability_failure_custody {
    use std::any::Any;
    use std::future::{Future, pending};
    use std::panic::{panic_any, resume_unwind};
    use std::ptr;
    use std::task::{Context, Poll, Waker};

    use tokio::sync::oneshot;
    use tokio::task;

    use crate::local::execution::{ActivationTasks, OwnerCancellation, close_owner_cancellation};

    #[tokio::test]
    async fn settlement_joins_every_task_after_failure_and_retains_events_and_errors() {
        let first_payload = Box::new(vec![11_u64, 111]);
        let later_payload = Box::new(vec![22_u64, 122]);
        let event = vec![33_u64, 133];
        let event_allocation = event.as_ptr() as usize;
        let (ready, reached) = oneshot::channel();
        let (later_ready, later_reached) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut tasks = ActivationTasks::<Vec<u64>>::new();
        tasks.spawn(async move {
            ready
                .send(task::id())
                .expect("the caller observes the first task starting");
            panic_any(first_payload);
        });
        tasks.spawn(async move {
            later_ready
                .send(task::id())
                .expect("the caller observes the later task starting");
            panic_any(later_payload);
        });
        tasks.spawn(async move {
            released
                .await
                .expect("the owner explicitly releases the final task");
            Err(event)
        });
        let first_task_id = reached.await.expect("the first actual task started");
        let later_task_id = later_reached.await.expect("the later actual task started");
        let mut settlement = Box::pin(tasks.settle());
        let mut context = Context::from_waker(Waker::noop());
        let waiting = settlement.as_mut().poll(&mut context);
        assert!(matches!(waiting, Poll::Pending));
        release
            .send(())
            .expect("the blocked actual task receives its release");
        let (events, failures) = settlement.await;
        assert_eq!(events, [vec![33, 133]]);
        assert_eq!(events[0].as_ptr() as usize, event_allocation);
        assert_eq!(failures.len(), 2);
        let mut failed_task_ids = failures
            .iter()
            .map(|failure| {
                assert!(failure.is_panic());
                failure.id()
            })
            .collect::<Vec<_>>();
        failed_task_ids.sort();
        let mut original_task_ids = [first_task_id, later_task_id];
        original_task_ids.sort();
        assert_eq!(failed_task_ids, original_task_ids);
    }

    #[test]
    fn owner_receiver_close_retains_only_the_accepted_unread_occurrence() {
        let (sender, receiver) = oneshot::channel();
        let admitted = sender.send(OwnerCancellation);
        assert!(admitted.is_ok());
        let unread = close_owner_cancellation(receiver);
        assert_eq!(unread, Some(()));

        let (sender, receiver) = oneshot::channel();
        let unread = close_owner_cancellation(receiver);
        assert_eq!(unread, None);
        let refused = sender.send(OwnerCancellation);
        assert!(matches!(refused, Err(OwnerCancellation)));

        let (sender, mut receiver) = oneshot::channel();
        let admitted = sender.send(OwnerCancellation);
        assert!(admitted.is_ok());
        let acquired = receiver.try_recv();
        assert!(matches!(acquired, Ok(OwnerCancellation)));
        let unread = close_owner_cancellation(receiver);
        assert_eq!(unread, None);
    }

    #[tokio::test]
    async fn activation_task_custody_reports_pending_work_until_it_settles() {
        let mut tasks = ActivationTasks::<u64>::new();
        assert!(tasks.is_empty());
        tasks.spawn(async { Ok(()) });
        assert!(!tasks.is_empty());
        let (completed, failures) = tasks.settle().await;
        assert_eq!(failures.len(), 0);
        assert_eq!(completed.len(), 0);
    }

    #[tokio::test]
    async fn activation_task_event_returns_exact_panic_and_cancellation() {
        let panic_payload: Box<dyn Any + Send> = Box::new(vec![19_u64, 23]);
        let panic_payload_identity: *const (dyn Any + Send) = panic_payload.as_ref();
        let mut panicking = ActivationTasks::<u64>::new();
        panicking.spawn(async move { resume_unwind(panic_payload) });
        let failure = panicking
            .next_event()
            .await
            .expect_err("the exact task failure is returned");
        assert!(failure.is_panic());
        let payload = failure.into_panic();
        let received_payload_identity: *const (dyn Any + Send) = payload.as_ref();
        assert!(ptr::eq(received_payload_identity, panic_payload_identity));
        assert!(panicking.is_empty());

        let mut cancelled = ActivationTasks::<u64>::new();
        cancelled.spawn(async { pending::<Result<(), u64>>().await });
        cancelled.tasks.abort_all();
        let failure = cancelled
            .next_event()
            .await
            .expect_err("cancelled activation work is retained");
        assert!(failure.is_cancelled());
        assert!(cancelled.is_empty());
    }
}
