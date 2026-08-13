//! Runtime ownership of children created by one parent incarnation.

/// Live child capabilities retained by one parent generation.
///
/// The nonce is runtime bookkeeping only. Behavior keeps its own pure birth
/// state; this scope exists solely to prevent a successfully created child
/// from being destroyed when `create` returns.
pub(crate) struct ChildScope<N, C> {
    children: Vec<(N, Option<C>)>,
}

/// Failure to select one exact child completion generation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChildObservationError<N> {
    /// No live child is bound at the observed nonce.
    #[error("no live child is bound at nonce {0:?}")]
    Unknown(N),
}

/// Failure to select the committed result of one same-action creation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CreationObservationError<N> {
    /// The current transition staged no creation at the observed nonce.
    #[error("the current transition staged no creation at nonce {0:?}")]
    Unknown(N),
}

impl<N, C> Default for ChildScope<N, C> {
    fn default() -> Self {
        Self {
            children: Vec::new(),
        }
    }
}

impl<N, C> ChildScope<N, C> {
    pub(crate) const fn new() -> Self {
        Self {
            children: Vec::new(),
        }
    }
}

impl<N: Copy + Eq, C> ChildScope<N, C> {
    pub(crate) fn reserve(&mut self, nonce: N) {
        debug_assert!(self.child_nonce_is_fresh(nonce));
        self.children.push((nonce, None));
    }

    /// Cancel a reservation whose creation failed before installation.
    pub(crate) fn cancel_reservation(&mut self, nonce: N) {
        let index = self
            .children
            .iter()
            .position(|(candidate, slot)| *candidate == nonce && slot.is_none())
            .expect("only a reserved uninstalled nonce can be cancelled");
        self.children.remove(index);
    }

    pub(crate) fn install(&mut self, nonce: N, capability: C) {
        let slot = self
            .children
            .iter_mut()
            .find(|(candidate, _)| *candidate == nonce)
            .expect("child binding must be reserved before installation");
        debug_assert!(slot.1.is_none());
        slot.1 = Some(capability);
    }

    pub(crate) fn child_nonce_is_fresh(&self, nonce: N) -> bool {
        self.children
            .iter()
            .all(|(candidate, _)| *candidate != nonce)
    }

    /// Whether any child entry — reserved or installed — exists at `nonce`.
    pub(crate) fn contains(&self, nonce: N) -> bool {
        self.children
            .iter()
            .any(|(candidate, _)| *candidate == nonce)
    }

    pub(crate) fn get_mut(&mut self, nonce: N) -> Result<&mut C, ChildObservationError<N>> {
        self.children
            .iter_mut()
            .find(|(candidate, _)| *candidate == nonce)
            .and_then(|(_, child)| child.as_mut())
            .ok_or(ChildObservationError::Unknown(nonce))
    }
}

impl<N, C: super::CoordinatedChild> ChildScope<N, C> {
    pub(crate) async fn retire(&mut self) {
        for (_, child) in &self.children {
            if let Some(child) = child {
                child.request_shutdown();
            }
        }
        for (_, child) in self.children.drain(..) {
            if let Some(child) = child {
                child.retire().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::ChildLease;

    use super::ChildScope;

    struct OrderedChild(&'static str, Arc<std::sync::Mutex<Vec<&'static str>>>);

    impl super::super::CoordinatedChild for OrderedChild {
        fn request_shutdown(&self) {
            self.1
                .lock()
                .expect("retirement order lock")
                .push(match self.0 {
                    "first" => "request first",
                    "second" => "request second",
                    _ => unreachable!(),
                });
        }

        async fn retire(self) {
            self.1
                .lock()
                .expect("retirement order lock")
                .push(match self.0 {
                    "first" => "retire first",
                    "second" => "retire second",
                    _ => unreachable!(),
                });
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl crate::runtime::ChildShutdownEdge for DropProbe {
        fn request_shutdown(&self) {}
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dropping_parent_scope_releases_every_child_lease_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let mut children = ChildScope::new();
            let completion = || {
                let task = tokio::spawn(async {});
                let control = task.abort_handle();
                let space = observe::ObservationSpace::<(), crate::TaskOutcome<()>>::new();
                let subject = space.subject(()).expect("fresh completion");
                let observation = space.observe(&()).expect("registered completion");
                (
                    subject,
                    control,
                    crate::runtime::Completion::new(task, observation),
                )
            };
            let (_first_subject, first_control, first_completion) = completion();
            let (_second_subject, second_control, second_completion) = completion();
            children.reserve(1_u64);
            children.install(
                1_u64,
                ChildLease::new(DropProbe(drops.clone()), first_control, first_completion),
            );
            children.reserve(2_u64);
            children.install(
                2_u64,
                ChildLease::new(DropProbe(drops.clone()), second_control, second_completion),
            );
            assert_eq!(drops.load(Ordering::SeqCst), 0);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_reaches_every_child_before_waiting_in_creation_order() {
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut children = ChildScope::new();
        children.reserve(1_u64);
        children.install(1_u64, OrderedChild("first", order.clone()));
        children.reserve(2_u64);
        children.install(2_u64, OrderedChild("second", order.clone()));

        children.retire().await;

        assert_eq!(
            *order.lock().expect("retirement order lock"),
            [
                "request first",
                "request second",
                "retire first",
                "retire second"
            ]
        );
    }

    #[tokio::test]
    async fn taking_completion_keeps_liveness_in_the_parent_scope() {
        let liveness_drops = Arc::new(AtomicUsize::new(0));
        let space = observe::ObservationSpace::<(), crate::TaskOutcome<u8>>::new();
        let mut subject = space.subject(()).expect("fresh completion");
        let observation = space.observe(&()).expect("registered completion");
        subject.complete(crate::TaskOutcome::Returned(7));
        drop(subject);

        let task = tokio::spawn(async {});
        let control = task.abort_handle();
        let completion = crate::runtime::Completion::new(task, observation);
        let mut children = ChildScope::new();
        children.reserve(1_u64);
        children.install(
            1_u64,
            ChildLease::new(DropProbe(liveness_drops.clone()), control, completion),
        );

        let child = children.get_mut(1).expect("exact child generation");
        let completion = child.take_completion().expect("exact child completion");
        assert_eq!(liveness_drops.load(Ordering::SeqCst), 0);
        assert_eq!(completion.wait().await, crate::TaskOutcome::Returned(7));
        assert_eq!(liveness_drops.load(Ordering::SeqCst), 0);

        drop(children);
        assert_eq!(liveness_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn observed_nonce_never_becomes_fresh_again() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut children = ChildScope::new();
        children.reserve(1_u64);
        children.install(1_u64, DropProbe(drops.clone()));
        assert!(!children.child_nonce_is_fresh(1));

        {
            let _ = children.get_mut(1).expect("original generation");
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(!children.child_nonce_is_fresh(1));
        drop(children);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancelling_reservation_preserves_other_reservation() {
        let mut children = ChildScope::<u64, DropProbe>::new();
        children.reserve(1_u64);
        children.reserve(2_u64);

        children.cancel_reservation(2);

        assert!(children.contains(1));
        assert!(!children.contains(2));
    }
}
