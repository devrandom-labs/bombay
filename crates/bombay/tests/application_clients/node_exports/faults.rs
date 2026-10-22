//! A real provider result and its independently panicking producer destructor.
use super::super::protocol::recorded;
use super::super::wire::{CounterInput, Refusal};
use super::{RefusalStage, VerificationRefusal};
use std::any::Any;
use std::cell::{RefCell, RefMut};
use std::future::Future;
use std::panic::panic_any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub(super) struct Verification<'scope> {
    pub(super) result: Option<Result<recorded::Receipt<CounterInput>, VerificationRefusal<'scope>>>,
    pub(super) gate: Option<oneshot::Receiver<()>>,
    pub(super) entered: Option<oneshot::Sender<()>>,
    pub(super) polling: Option<Arc<Vec<u8>>>,
    pub(super) panic: Option<Arc<Vec<u8>>>,
    pub(super) returned: &'scope RefCell<Option<oneshot::Sender<()>>>,
    pub(super) disposed: &'scope RefCell<Vec<RefusalStage>>,
    pub(super) stage: RefusalStage,
}

impl<'scope> Future for Verification<'scope> {
    type Output = Result<recorded::Receipt<CounterInput>, VerificationRefusal<'scope>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let verification = self.get_mut();
        if let Some(gate) = &mut verification.gate {
            match Pin::new(gate).poll(context) {
                Poll::Pending => {
                    if let Some(publication) = verification.entered.take() {
                        let published = publication.send(());
                        published.expect("the peer observes actual pending Counter verification");
                    }
                    return Poll::Pending;
                }
                Poll::Ready(original) => {
                    original.expect("the private peer releases this actual provider");
                    verification.gate = None;
                }
            }
        }
        if let Some(original) = verification.polling.take() {
            panic_any(original);
        }
        Poll::Ready(
            verification
                .result
                .take()
                .expect("a provider is never repolled after Ready"),
        )
    }
}

impl Drop for Verification<'_> {
    fn drop(&mut self) {
        self.disposed.borrow_mut().push(self.stage);
        match self.stage {
            RefusalStage::VerifiedDisposal
            | RefusalStage::RefusedDisposal
            | RefusalStage::VerificationPoll
            | RefusalStage::VerificationDoublePanic => {
                if let Some(publication) = self.returned.borrow_mut().take() {
                    let published = publication.send(());
                    published.expect("the actual private peer observes producer disposal");
                }
            }
            _ => {}
        }
        if let Some(original) = self.panic.take() {
            panic_any(original);
        }
    }
}

pub(super) struct Protection<'scope> {
    pub(super) permission: RefMut<'scope, Box<[u8]>>,
    pub(super) entered: Option<(oneshot::Sender<()>, oneshot::Sender<()>)>,
    pub(super) loans: &'scope RefCell<Vec<*const u8>>,
    pub(super) disposed: &'scope RefCell<Vec<()>>,
    pub(super) panic: Option<Arc<Vec<u8>>>,
}

impl Future for Protection<'_> {
    type Output = Result<Box<[u8]>, Refusal>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let protection = self.get_mut();
        if let Some((peer, execution)) = protection.entered.take() {
            protection
                .loans
                .borrow_mut()
                .push(protection.permission.as_ptr());
            let peer = peer.send(());
            let execution = execution.send(());
            peer.expect("the peer observes actual sibling protection");
            execution.expect("the borrowed execution wait observes actual sibling protection");
        }
        Poll::Pending
    }
}

impl Drop for Protection<'_> {
    fn drop(&mut self) {
        self.disposed.borrow_mut().push(());
        if let Some(original) = self.panic.take() {
            panic_any(original);
        }
    }
}

pub(super) fn assert_panic(original: Option<Box<dyn Any + Send>>, expected: &Arc<Vec<u8>>) {
    let original = original.expect("actual independent producer unwind payload");
    let original = original
        .downcast_ref::<Arc<Vec<u8>>>()
        .expect("actual native panic payload type");
    assert!(Arc::ptr_eq(original, expected));
    assert_eq!(original.as_slice(), expected.as_slice());
}
