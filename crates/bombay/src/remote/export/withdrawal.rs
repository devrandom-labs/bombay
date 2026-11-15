//! Deterministic authority and original-result custody, without native declarations.
use super::{Export, ExportError};
use crate::observe;
use core::future::{Future, IntoFuture};
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use futures_util::FutureExt;
use std::cell::RefCell;
use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Wake;
use thiserror::Error;

// This unbound owner exercises only authority/result operations. The Node
// isolation witness separately exercises the real constructor and interpreter.
fn unbound_export() -> Export<(), (), ()> {
    let (authority, intent) = observe::pair();
    let (publication, withdrawal) = observe::pair();
    Export {
        inputs: (),
        declarations: (),
        startup: None,
        withdrawal,
        withdrawal_publication: Some(publication),
        withdrawal_authority: Some(authority),
        withdrawal_intent: intent,
        ingress: Arc::new(Mutex::new(Vec::new())),
        entries: RefCell::new(Vec::new()),
        declaration: RefCell::new(None),
        active: None,
    }
}

#[derive(Debug, Error)]
#[error("private native withdrawal detail")]
struct WithdrawalRefusal {
    original: Box<[u8]>,
    disposals: Arc<AtomicUsize>,
}

impl Drop for WithdrawalRefusal {
    fn drop(&mut self) {
        self.disposals.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn borrowed_withdrawal_wait_keeps_the_original_refusal() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let duplicate = export.take_withdrawal();
    let operation = withdraw();
    assert!(duplicate.is_none());
    assert_eq!(intent.try_get(), None);
    let mut operation = pin!(operation);
    let mut context = Context::from_waker(Waker::noop());
    let first = operation.as_mut().poll(&mut context);
    assert!(first.is_pending());
    assert_eq!(intent.try_get(), Some(()));
    let cancelled = operation.as_mut().now_or_never();
    assert!(cancelled.is_none());

    let disposals = Arc::new(AtomicUsize::new(0));
    let original = Box::new(WithdrawalRefusal {
        original: Box::from([137_u8, 139]),
        disposals: Arc::clone(&disposals),
    });
    let error_address = ptr::from_ref(original.as_ref());
    let payload_address = original.original.as_ptr();
    let original: Box<dyn Error + Send + Sync> = original;
    export.complete_withdrawal(Some(Err(original)));
    let observed = operation.as_mut().poll(&mut context);
    let Poll::Ready(Some(Err(observed))) = observed else {
        panic!("the retained operation observes the actual acquired refusal");
    };
    // Repeat finalization cannot replace the acquired Err with settled absence.
    export.complete_withdrawal(None);
    let final_result = export.into_parts().3.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(&observed, &final_result));
    assert!(matches!(observed.as_ref(), ExportError::Withdrawal { .. }));
    assert_eq!(observed.to_string(), "network withdrawal was refused");
    assert_eq!(
        format!("{observed:?}"),
        "ExportError(network withdrawal was refused)"
    );
    let source = observed
        .source()
        .expect("the root retains its original native cause");
    let source = source.downcast_ref::<WithdrawalRefusal>().unwrap();
    assert!(ptr::eq(source, error_address));
    assert_eq!(source.original.as_ptr(), payload_address);
    assert_eq!(source.original.as_ref(), [137, 139]);
    assert_eq!(source.to_string(), "private native withdrawal detail");
    assert_eq!(disposals.load(Ordering::SeqCst), 0);
    let forward =
        |withdrawal: Result<(), Arc<ExportError>>| -> Result<(), Box<dyn Error + Send + Sync>> {
            withdrawal?;
            Ok(())
        };
    let forwarded = forward(Err(Arc::clone(&observed))).unwrap_err();
    let shared = forwarded.downcast_ref::<Arc<ExportError>>().unwrap();
    assert!(Arc::ptr_eq(shared, &observed));
    let source = forwarded
        .source()
        .unwrap()
        .downcast_ref::<WithdrawalRefusal>()
        .unwrap();
    assert!(ptr::eq(source, error_address));
    drop(forwarded);
    drop(observed);
    assert_eq!(disposals.load(Ordering::SeqCst), 0);
    drop(final_result);
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
}

#[test]
fn unstarted_withdrawal_and_owner_surrender_publish_no_result() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let completion = export.withdrawal.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    drop(operation);
    assert_eq!(intent.try_get(), None);
    drop(export);
    let observed = completion.into_future().now_or_never();
    assert!(observed.is_none());
}

#[test]
fn started_operation_surrender_keeps_the_owner_result() {
    let mut export = unbound_export();
    let intent = export.withdrawal_intent.clone();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    {
        let mut operation = pin!(operation);
        let mut context = Context::from_waker(Waker::noop());
        let polled = operation.as_mut().poll(&mut context);
        assert!(polled.is_pending());
    }
    assert_eq!(intent.try_get(), Some(()));
    export.complete_withdrawal(Some(Ok(())));
    let retained = export.into_parts().3;
    assert!(matches!(retained, Some(Ok(()))));
}

#[test]
fn settled_absence_differs_from_pending_withdrawal() {
    let mut export = unbound_export();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let mut operation = pin!(operation);
    let mut context = Context::from_waker(Waker::noop());
    let pending = operation.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    export.complete_withdrawal(None);
    let observed = operation.as_mut().poll(&mut context);
    assert!(matches!(observed, Poll::Ready(None)));
    let retained = export.into_parts().3;
    assert!(retained.is_none());
}

#[test]
fn late_withdrawal_reads_the_completed_native_success() {
    let mut export = unbound_export();
    export.complete_withdrawal(Some(Ok(())));
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let observed = operation.now_or_never();
    assert!(matches!(observed, Some(Some(Ok(())))));
    export.complete_withdrawal(None);
    let retained = export.into_parts().3;
    assert!(matches!(retained, Some(Ok(()))));
}

struct WithdrawalWake;

impl Wake for WithdrawalWake {
    fn wake(self: Arc<Self>) {
        panic!("withdrawal observer notification panicked");
    }
}

#[test]
fn observer_notification_panic_keeps_the_original_native_refusal() {
    let mut export = unbound_export();
    let withdraw = export.take_withdrawal().unwrap();
    let operation = withdraw();
    let mut operation = pin!(operation);
    let waker = Waker::from(Arc::new(WithdrawalWake));
    let mut context = Context::from_waker(&waker);
    let pending = operation.as_mut().poll(&mut context);
    assert!(pending.is_pending());
    let original: Box<dyn Error + Send + Sync> = Box::new(WithdrawalRefusal {
        original: Box::from([149_u8, 151]),
        disposals: Arc::new(AtomicUsize::new(0)),
    });
    let address = ptr::from_ref(original.as_ref());
    let notification = catch_unwind(AssertUnwindSafe(|| {
        export.complete_withdrawal(Some(Err(original)));
    }));
    assert!(notification.is_err());
    let mut context = Context::from_waker(Waker::noop());
    let observed = operation.as_mut().poll(&mut context);
    let Poll::Ready(Some(Err(observed))) = observed else {
        panic!("publication retains the actual refusal before waking the observer");
    };
    export.complete_withdrawal(None);
    let retained = export.into_parts().3.unwrap().unwrap_err();
    assert!(Arc::ptr_eq(&observed, &retained));
    assert!(matches!(retained.as_ref(), ExportError::Withdrawal { .. }));
    let source = retained
        .source()
        .expect("the acquired native cause survives notification panic");
    assert!(ptr::addr_eq(source, address));
}
