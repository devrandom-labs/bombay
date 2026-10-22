use super::poll_verification;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

struct Refusal<'scope> {
    scope: &'scope str,
    original: Box<[u8]>,
}
struct Verification<'scope> {
    result: Option<Result<Box<[u8]>, Refusal<'scope>>>,
    polling: Option<Box<[u8; 2]>>,
    disposal: Option<Box<[u8; 2]>>,
    polls: &'scope Cell<usize>,
    disposals: &'scope Cell<usize>,
}
impl<'scope> Future for Verification<'scope> {
    type Output = Result<Box<[u8]>, Refusal<'scope>>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let verification = self.get_mut();
        verification.polls.set(verification.polls.get() + 1);
        if let Some(original) = verification.polling.take() {
            resume_unwind(original);
        }
        match verification.result.take() {
            Some(original) => Poll::Ready(original),
            None => Poll::Pending,
        }
    }
}
impl Drop for Verification<'_> {
    fn drop(&mut self) {
        self.disposals.set(self.disposals.get() + 1);
        if let Some(original) = self.disposal.take() {
            resume_unwind(original);
        }
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Receipt,
    Refusal,
}

#[test]
fn ready_receipt_survives_disposal_without_repolling() {
    preserve_ready_result(Outcome::Receipt);
}

#[test]
fn ready_refusal_survives_disposal_without_repolling() {
    preserve_ready_result(Outcome::Refusal);
}

fn preserve_ready_result(outcome: Outcome) {
    let scope = String::from("borrowed verifier scope");
    let original: Box<[u8]> = [173, 179].into();
    let allocation = original.as_ptr();
    let result = match outcome {
        Outcome::Receipt => Ok(original),
        Outcome::Refusal => Err(Refusal {
            scope: &scope,
            original,
        }),
    };
    let disposal = Box::new([181_u8, 191]);
    let disposal_allocation = disposal.as_ptr();
    let polls = Cell::new(0);
    let disposals = Cell::new(0);
    let mut retained = None;
    let mut verification = Box::pin(Verification {
        result: Some(result),
        polling: None,
        disposal: Some(disposal),
        polls: &polls,
        disposals: &disposals,
    });
    let mut context = Context::from_waker(Waker::noop());
    let acquired = poll_verification(verification.as_mut(), &mut retained, &mut context);
    // A lost result must still reach explicit disposal and the final independent oracle.
    let repeated = match &retained {
        Some(_) => Some(poll_verification(
            verification.as_mut(),
            &mut retained,
            &mut context,
        )),
        None => None,
    };
    let disposed = catch_unwind(AssertUnwindSafe(|| drop(verification)));
    let Err(payload) = disposed else {
        panic!("actual producer destructor panics");
    };
    let payload = payload
        .downcast_ref::<[u8; 2]>()
        .expect("original native disposal payload");
    assert_eq!(disposals.get(), 1);
    assert_eq!(payload.as_ptr(), disposal_allocation);
    assert_eq!(*payload, [181, 191]);
    assert!(matches!(acquired, Poll::Ready(())));
    assert_eq!(polls.get(), 1);
    match (outcome, retained) {
        (Outcome::Receipt, Some(Ok(original))) => {
            assert_eq!(original.as_ptr(), allocation);
            assert_eq!(original.as_ref(), [173, 179]);
        }
        (Outcome::Refusal, Some(Err(original))) => {
            assert_eq!(original.scope, scope);
            assert_eq!(original.original.as_ptr(), allocation);
            assert_eq!(original.original.as_ref(), [173, 179]);
        }
        _ => {
            panic!("the complete original Ready result must survive explicit producer disposal")
        }
    }
    assert!(matches!(repeated, Some(Poll::Ready(()))));
}

#[test]
fn poll_and_disposal_panics_keep_both_originals_without_a_provider_result() {
    let polling = Box::new([193_u8, 197]);
    let disposal = Box::new([199_u8, 211]);
    let polling_allocation = polling.as_ptr();
    let disposal_allocation = disposal.as_ptr();
    let polls = Cell::new(0);
    let disposals = Cell::new(0);
    let mut retained = None;
    let mut verification = Box::pin(Verification {
        result: None,
        polling: Some(polling),
        disposal: Some(disposal),
        polls: &polls,
        disposals: &disposals,
    });
    let mut context = Context::from_waker(Waker::noop());
    let polled = catch_unwind(AssertUnwindSafe(|| {
        poll_verification(verification.as_mut(), &mut retained, &mut context)
    }));
    let disposed = catch_unwind(AssertUnwindSafe(|| drop(verification)));
    let Err(polling) = polled else {
        panic!("actual provider poll panics");
    };
    let Err(disposal) = disposed else {
        panic!("separate actual producer disposal panics");
    };
    let polling = polling
        .downcast_ref::<[u8; 2]>()
        .expect("original poll payload");
    let disposal = disposal
        .downcast_ref::<[u8; 2]>()
        .expect("original disposal payload");
    assert_eq!(disposals.get(), 1);
    assert_eq!(polls.get(), 1);
    assert_eq!(polling.as_ptr(), polling_allocation);
    assert_eq!(disposal.as_ptr(), disposal_allocation);
    assert_eq!(*polling, [193, 197]);
    assert_eq!(*disposal, [199, 211]);
    assert!(retained.is_none());
}
