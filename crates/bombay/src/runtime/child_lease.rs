//! Affine ownership and optional completion of one child generation.

use behavior::{Never, ShutdownEvent};

use super::Completion;
use crate::{ActorRef, MailboxSender};

pub(crate) trait CoordinatedChild: Send {
    fn request_shutdown(&self);

    fn retire(self) -> impl core::future::Future<Output = ()> + Send;
}

pub(crate) trait ChildShutdownEdge: Send {
    fn request_shutdown(&self);
}

impl ChildShutdownEdge for () {
    fn request_shutdown(&self) {}
}

impl<A, E, L> ChildShutdownEdge for ActorRef<A, MailboxSender<E>, L>
where
    A: Send,
    E: ShutdownEvent + Send,
    L: crate::runtime::lifecycle::IncarnationReporter,
{
    fn request_shutdown(&self) {
        let _ = ActorRef::request_shutdown(self);
    }
}

impl CoordinatedChild for Never {
    fn request_shutdown(&self) {}

    async fn retire(self) {
        match self {}
    }
}

/// Runtime resources a parent retains to keep one child generation alive.
///
/// This is an ownership lease, not a universal public capability. Its inner
/// value is deliberately private; dropping the lease releases the resources
/// supplied by the configured child runtime.
#[must_use = "dropping a child lease releases the child runtime ownership"]
#[doc(hidden)]
pub struct ChildLease<R, T = ()> {
    edge: R,
    control: tokio::task::AbortHandle,
    completion: Option<Completion<T>>,
}

impl<R, T> ChildLease<R, T> {
    /// Retain child liveness and its separately consumable completion seat.
    pub(crate) const fn new(
        edge: R,
        control: tokio::task::AbortHandle,
        completion: Completion<T>,
    ) -> Self {
        Self {
            edge,
            control,
            completion: Some(completion),
        }
    }

    pub(crate) fn take_completion(&mut self) -> Option<Completion<T>> {
        self.completion.take()
    }
}

impl<R, T> CoordinatedChild for ChildLease<R, T>
where
    R: ChildShutdownEdge,
    T: Send + Sync,
{
    fn request_shutdown(&self) {
        self.edge.request_shutdown();
    }

    async fn retire(self) {
        let Self {
            edge,
            control,
            completion,
        } = self;
        drop(edge);
        drop(control);
        if let Some(completion) = completion {
            let _ = completion.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use observe::ObservationSpace;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use super::{ChildLease, CoordinatedChild};
    use crate::TaskOutcome;
    use crate::runtime::Completion;

    #[tokio::test]
    async fn retirement_waits_for_the_child_task_edge() {
        let space = ObservationSpace::new();
        let mut subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        subject.complete(TaskOutcome::Returned(()));
        drop(subject);
        let (finish, wait) = oneshot::channel();
        let task = tokio::spawn(async move {
            wait.await.expect("retirement test releases task");
        });
        let lease = ChildLease::new((), task.abort_handle(), Completion::new(task, observation));
        let retiring = tokio::spawn(lease.retire());

        yield_now().await;
        assert!(!retiring.is_finished());
        finish.send(()).expect("retirement still waiting");
        retiring.await.expect("retirement task");
    }
}
