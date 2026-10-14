#![cfg(feature = "remote")]

use bombay::remote::verify;
use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll, Waker};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::ptr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

struct ProtectedRequest {
    protected: Box<[u8]>,
    proof: Box<[u8]>,
}
struct VerificationReceipt {
    protected: Box<[u8]>,
    disposals: Arc<AtomicUsize>,
}
impl Drop for VerificationReceipt {
    fn drop(&mut self) {
        self.disposals.fetch_add(1, Ordering::SeqCst);
    }
}
struct ProviderVerification<'request> {
    request: &'request ProtectedRequest,
    receipt: Option<VerificationReceipt>,
    disposal: Option<Box<[u8; 2]>>,
    polls: &'request AtomicUsize,
}
impl Future for ProviderVerification<'_> {
    type Output = Result<VerificationReceipt, RecvError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let verification = self.get_mut();
        verification.polls.fetch_add(1, Ordering::SeqCst);
        let receipt = verification.receipt.take().expect("one original receipt");
        assert_eq!(
            receipt.protected.as_ref(),
            verification.request.protected.as_ref()
        );
        assert_eq!(verification.request.proof.as_ref(), &[19]);
        Poll::Ready(Ok(receipt))
    }
}
impl Drop for ProviderVerification<'_> {
    fn drop(&mut self) {
        if self.receipt.is_none()
            && let Some(cause) = self.disposal.take()
        {
            resume_unwind(cause);
        }
    }
}
async fn scheduled_verification(
    request: &ProtectedRequest,
    mut receiving: oneshot::Receiver<VerificationReceipt>,
    polls: &AtomicUsize,
) -> Result<VerificationReceipt, RecvError> {
    let received = poll_fn(|context| {
        polls.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut receiving).poll(context)
    })
    .await;
    if let Ok(receipt) = &received {
        assert_eq!(receipt.protected.as_ref(), request.protected.as_ref());
    }
    assert_eq!(request.proof.as_ref(), &[19]);
    received
}
#[derive(Clone, Copy)]
enum ProviderOutcome {
    Receipt,
    Unavailable,
}

#[test]
fn ready_verification_keeps_original_receipt_before_producer_disposal() {
    let request = ProtectedRequest {
        protected: Box::from([7, 11]),
        proof: Box::from([19]),
    };
    let protected_allocation = request.protected.as_ptr();
    let proof_allocation = request.proof.as_ptr();
    let disposals = Arc::new(AtomicUsize::new(0));
    let receipt = VerificationReceipt {
        protected: Box::from([7, 11]),
        disposals: disposals.clone(),
    };
    let receipt_allocation = receipt.protected.as_ptr();
    let cause = Box::new([13_u8, 17]);
    let cause_allocation = ptr::from_ref(cause.as_ref()).cast::<()>();
    let polls = AtomicUsize::new(0);
    let mut retained = None;
    let mut context = Context::from_waker(Waker::noop());
    let caught = {
        let mut verification = pin!(verify(&request, &mut retained, |request| {
            ProviderVerification {
                request,
                receipt: Some(receipt),
                disposal: Some(cause),
                polls: &polls,
            }
        }));
        catch_unwind(AssertUnwindSafe(|| {
            verification.as_mut().poll(&mut context)
        }))
    };
    let Err(cause) = caught else {
        panic!("actual producer disposal raises its original cause");
    };
    assert_eq!(ptr::from_ref(cause.as_ref()).cast::<()>(), cause_allocation);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(request.protected.as_ptr(), protected_allocation);
    assert_eq!(request.proof.as_ptr(), proof_allocation);
    assert_eq!(request.protected.as_ref(), &[7, 11]);
    assert_eq!(request.proof.as_ref(), &[19]);
    let Some(Ok(receipt)) = &retained else {
        panic!("provider Ready receipt was erased by producer disposal");
    };
    assert_eq!(receipt.protected.as_ptr(), receipt_allocation);
    assert_eq!(receipt.protected.as_ref(), &[7, 11]);
    assert_eq!(disposals.load(Ordering::SeqCst), 0);
    drop((retained, cause));
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
}

#[test]
fn scheduled_verification_keeps_originals_across_cancelled_borrowed_wait() {
    for outcome in [ProviderOutcome::Receipt, ProviderOutcome::Unavailable] {
        let request = ProtectedRequest {
            protected: Box::from([7, 11]),
            proof: Box::from([19]),
        };
        let protected_allocation = request.protected.as_ptr();
        let proof_allocation = request.proof.as_ptr();
        let disposals = Arc::new(AtomicUsize::new(0));
        let receipt = VerificationReceipt {
            protected: Box::from([7, 11]),
            disposals: disposals.clone(),
        };
        let receipt_allocation = receipt.protected.as_ptr();
        let (publication, receiving) = oneshot::channel();
        let polls = AtomicUsize::new(0);
        let mut retained = None;
        let mut context = Context::from_waker(Waker::noop());
        let (pending, published, resumed) = {
            let mut verification = pin!(verify(&request, &mut retained, |request| {
                scheduled_verification(request, receiving, &polls)
            }));
            let pending = {
                let mut wait = pin!(poll_fn(|context| verification.as_mut().poll(context)));
                wait.as_mut().poll(&mut context)
            };
            let published = match outcome {
                ProviderOutcome::Receipt => Some(publication.send(receipt)),
                ProviderOutcome::Unavailable => {
                    drop(publication);
                    drop(receipt);
                    None
                }
            };
            let resumed = {
                let mut wait = pin!(poll_fn(|context| verification.as_mut().poll(context)));
                wait.as_mut().poll(&mut context)
            };
            (pending, published, resumed)
        };
        assert!(matches!(pending, Poll::Pending));
        assert!(matches!(resumed, Poll::Ready(())));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(request.protected.as_ptr(), protected_allocation);
        assert_eq!(request.proof.as_ptr(), proof_allocation);
        assert_eq!(request.protected.as_ref(), &[7, 11]);
        assert_eq!(request.proof.as_ref(), &[19]);
        match outcome {
            ProviderOutcome::Receipt => {
                assert!(matches!(published, Some(Ok(()))));
                let Some(Ok(receipt)) = &retained else {
                    panic!("same resumed operation retains the original receipt");
                };
                assert_eq!(receipt.protected.as_ptr(), receipt_allocation);
                assert_eq!(disposals.load(Ordering::SeqCst), 0);
            }
            ProviderOutcome::Unavailable => {
                assert!(published.is_none());
                let Some(Err(_cause)) = &retained else {
                    panic!("actual closed receiver cause remains typed and owned");
                };
                assert_eq!(disposals.load(Ordering::SeqCst), 1);
            }
        }
        drop(retained);
        assert_eq!(disposals.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn acquired_receipt_or_error_prevents_both_provider_factories_and_polls() {
    for outcome in [ProviderOutcome::Receipt, ProviderOutcome::Unavailable] {
        let request = ProtectedRequest {
            protected: Box::from([7, 11]),
            proof: Box::from([19]),
        };
        let disposals = Arc::new(AtomicUsize::new(0));
        let mut context = Context::from_waker(Waker::noop());
        let original = match outcome {
            ProviderOutcome::Receipt => Ok(VerificationReceipt {
                protected: Box::from([7, 11]),
                disposals: disposals.clone(),
            }),
            ProviderOutcome::Unavailable => {
                let (publication, mut receiving) = oneshot::channel::<VerificationReceipt>();
                drop(publication);
                let received = Pin::new(&mut receiving).poll(&mut context);
                let Poll::Ready(Err(cause)) = received else {
                    panic!("original closed receiving cause");
                };
                Err(cause)
            }
        };
        let receipt_allocation = original
            .as_ref()
            .ok()
            .map(|receipt| receipt.protected.as_ptr());
        let mut retained = Some(original);
        let factories = AtomicUsize::new(0);
        let polls = AtomicUsize::new(0);
        let recorded = {
            let mut verification = pin!(verify(&request, &mut retained, |request| {
                factories.fetch_add(1, Ordering::SeqCst);
                ProviderVerification {
                    request,
                    receipt: Some(VerificationReceipt {
                        protected: Box::from([7, 11]),
                        disposals: disposals.clone(),
                    }),
                    disposal: None,
                    polls: &polls,
                }
            }));
            verification.as_mut().poll(&mut context)
        };
        let (publication, receiving) = oneshot::channel();
        let scheduled = {
            let mut verification = pin!(verify(&request, &mut retained, |request| {
                factories.fetch_add(1, Ordering::SeqCst);
                scheduled_verification(request, receiving, &polls)
            }));
            verification.as_mut().poll(&mut context)
        };
        drop(publication);
        assert_eq!(factories.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(matches!(recorded, Poll::Ready(())));
        assert!(matches!(scheduled, Poll::Ready(())));
        match outcome {
            ProviderOutcome::Receipt => {
                let Some(Ok(receipt)) = &retained else {
                    panic!("original acquired receipt survives both denied factories");
                };
                assert_eq!(Some(receipt.protected.as_ptr()), receipt_allocation);
                assert_eq!(disposals.load(Ordering::SeqCst), 0);
            }
            ProviderOutcome::Unavailable => {
                let Some(Err(_cause)) = &retained else {
                    panic!("original acquired error survives both denied factories");
                };
                assert_eq!(disposals.load(Ordering::SeqCst), 0);
            }
        }
        drop(retained);
        assert_eq!(
            disposals.load(Ordering::SeqCst),
            match outcome {
                ProviderOutcome::Receipt => 1,
                ProviderOutcome::Unavailable => 0,
            }
        );
    }
}
