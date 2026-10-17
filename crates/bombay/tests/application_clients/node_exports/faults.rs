//! A real provider result and its independently panicking producer destructor.
use super::super::protocol::recorded;
use super::super::wire::CounterInput;
use super::{RefusalStage, VerificationRefusal};
use std::cell::RefCell;
use std::future::Future;
use std::panic::panic_any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub(super) struct Verification<'scope> {
    pub(super) result: Option<Result<recorded::Receipt<CounterInput>, VerificationRefusal<'scope>>>,
    pub(super) panic: Option<Arc<Vec<u8>>>,
    pub(super) returned: &'scope RefCell<Option<oneshot::Sender<()>>>,
    pub(super) disposed: &'scope RefCell<Vec<RefusalStage>>,
    pub(super) stage: RefusalStage,
}

impl<'scope> Future for Verification<'scope> {
    type Output = Result<recorded::Receipt<CounterInput>, VerificationRefusal<'scope>>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            self.get_mut()
                .result
                .take()
                .expect("a provider is never repolled after Ready"),
        )
    }
}

impl Drop for Verification<'_> {
    fn drop(&mut self) {
        self.disposed.borrow_mut().push(self.stage);
        if let Some(original) = self.panic.take() {
            if let Some(publication) = self.returned.borrow_mut().take() {
                let published = publication.send(());
                published.expect("the actual private peer observes producer disposal");
            }
            panic_any(original);
        }
    }
}
