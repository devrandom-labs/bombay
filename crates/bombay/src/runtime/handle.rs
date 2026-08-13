//! Edge-owned capabilities for one running actor incarnation.

use super::Completion;
use crate::{ChildLease, TaskOutcome};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Typed delivery, execution control, and completion for one incarnation.
///
/// The handle is one affine public ownership value: [`Self::outcome`] retains
/// its delivery and abort seats until completion, while [`Self::close`] drops
/// both seats before waiting for graceful retirement. Raw executor control and
/// completion observation are deliberately not exposed.
///
/// ```compile_fail
/// fn bypass<R, T>(handle: &bombay::Handle<R, T>) {
///     let _ = handle.control();
///     let _ = handle.observation();
/// }
/// ```
pub struct Handle<R, T> {
    actor_ref: R,
    control: tokio::task::AbortHandle,
    completion: Completion<T>,
    cancellation_requested: Arc<AtomicBool>,
}

impl<R, T> Handle<R, T> {
    pub(crate) const fn new(
        actor_ref: R,
        control: tokio::task::AbortHandle,
        completion: Completion<T>,
        cancellation_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            actor_ref,
            control,
            completion,
            cancellation_requested,
        }
    }

    /// Borrow the typed message-delivery capability.
    pub const fn actor_ref(&self) -> &R {
        &self.actor_ref
    }

    /// Request hard cancellation of this incarnation.
    pub fn abort(&self) {
        self.cancellation_requested.store(true, Ordering::Release);
        self.control.abort();
    }

    /// Wait for the classified terminal outcome.
    ///
    /// # Panics
    ///
    /// Panics only if the actor task violates its publication invariant.
    /// Actor failure, panic, and cancellation are returned as outcome data.
    pub async fn outcome(self) -> TaskOutcome<T> {
        let Self {
            actor_ref,
            control,
            completion,
            cancellation_requested: _,
        } = self;
        let outcome = completion.wait().await;
        drop(actor_ref);
        drop(control);
        outcome
    }

    /// Drop the delivery and control edges, then await graceful retirement.
    ///
    /// # Panics
    ///
    /// Panics under the same publication invariant violation as
    /// [`Self::outcome`].
    pub async fn close(self) -> TaskOutcome<T> {
        let Self {
            actor_ref,
            control,
            completion,
            cancellation_requested: _,
        } = self;
        drop(actor_ref);
        drop(control);
        completion.wait().await
    }

    pub(crate) fn into_child_lease(self) -> ChildLease<R, T> {
        ChildLease::new(self.actor_ref, self.control, self.completion)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use observe::ObservationSpace;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;

    use super::{Completion, Handle};
    use crate::TaskOutcome;

    struct Probe(Arc<AtomicUsize>);

    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn completion() -> (
        oneshot::Sender<()>,
        tokio::task::AbortHandle,
        Completion<u8>,
    ) {
        let space = ObservationSpace::new();
        let mut subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        subject.complete(TaskOutcome::Returned(7));
        let (send, receive) = oneshot::channel();
        let task = tokio::spawn(async move {
            receive.await.expect("task terminal outcome");
        });
        let control = task.abort_handle();
        (send, control, Completion::new(task, observation))
    }

    #[tokio::test]
    async fn outcome_retains_delivery_and_control_until_terminal_completion() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let (finish, control, completion) = completion();
        let handle = Handle::new(
            Probe(dropped.clone()),
            control,
            completion,
            Arc::new(AtomicBool::new(false)),
        );
        let outcome = tokio::spawn(handle.outcome());

        yield_now().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        finish.send(()).unwrap();
        assert_eq!(outcome.await.unwrap(), TaskOutcome::Returned(7));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn close_releases_delivery_and_control_before_waiting_for_terminal_completion() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let (finish, control, completion) = completion();
        let handle = Handle::new(
            Probe(dropped.clone()),
            control,
            completion,
            Arc::new(AtomicBool::new(false)),
        );
        let outcome = tokio::spawn(handle.close());

        yield_now().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        finish.send(()).unwrap();
        assert_eq!(outcome.await.unwrap(), TaskOutcome::Returned(7));
    }

    #[tokio::test]
    async fn child_extraction_retains_separate_completion_and_liveness_seats() {
        let liveness_dropped = Arc::new(AtomicUsize::new(0));
        let completion_dropped = Arc::new(AtomicUsize::new(0));
        let space = ObservationSpace::<(), TaskOutcome<Probe>>::new();
        let mut subject = space.subject(()).expect("fresh subject");
        let observation = space.observe(&()).expect("registered subject");
        subject.complete(TaskOutcome::Returned(Probe(completion_dropped.clone())));
        drop(subject);
        let task = tokio::spawn(async {});
        let control = task.abort_handle();
        let completion = Completion::new(task, observation);
        let handle = Handle::new(
            Probe(liveness_dropped.clone()),
            control,
            completion,
            Arc::new(AtomicBool::new(false)),
        );

        let lease = handle.into_child_lease();
        assert_eq!(completion_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(liveness_dropped.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(completion_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(liveness_dropped.load(Ordering::SeqCst), 1);
    }
}
