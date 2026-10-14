#![cfg(feature = "remote")]

use bombay::remote::{IngressError, try_retain};
use std::error::Error;
use std::ptr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

struct Payload {
    marker: u8,
    bytes: Box<[u8]>,
    disposed: Arc<Mutex<Vec<u8>>>,
}

impl Payload {
    fn new(marker: u8, disposed: &Arc<Mutex<Vec<u8>>>) -> Self {
        Self {
            marker,
            bytes: vec![marker; 3].into_boxed_slice(),
            disposed: Arc::clone(disposed),
        }
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.disposed.lock().unwrap().push(self.marker);
    }
}

#[test]
fn count_covers_active_and_unread_originals() {
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let capacity = Arc::new(Semaphore::new(2));
    let (ingress, mut incoming) = mpsc::channel(1);
    let first = try_retain(&ingress, &capacity, Payload::new(0, &disposed));
    let unread = incoming.try_recv().unwrap();
    let second = try_retain(&ingress, &capacity, Payload::new(1, &disposed));
    let active = incoming.try_recv().unwrap();
    let original = Payload::new(2, &disposed);
    let pointer = original.bytes.as_ptr();
    let refused = try_retain(&ingress, &capacity, original);
    let charged = capacity.available_permits();
    let Err(IngressError::Capacity { original, cause }) = refused else {
        panic!("third original must be refused while two outside originals remain owned");
    };
    let refused_pointer = original.bytes.as_ptr();
    let refused_bytes = original.bytes.to_vec();
    let before_disposal = disposed.lock().unwrap().clone();
    drop(original);
    drop(active);
    let after_active = capacity.available_permits();
    drop(unread);
    let after_unread = capacity.available_permits();
    let after_disposal = disposed.lock().unwrap().clone();

    assert!(first.is_ok() && second.is_ok());
    assert!(matches!(cause, TryAcquireError::NoPermits));
    assert_eq!(refused_pointer, pointer);
    assert_eq!(refused_bytes, [2, 2, 2]);
    assert_eq!((charged, after_active, after_unread), (0, 1, 2));
    assert_eq!(before_disposal, []);
    assert_eq!(after_disposal, [2, 1, 0]);
}

#[test]
fn closed_capacity_preserves_the_unqueued_original() {
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let capacity = Arc::new(Semaphore::new(1));
    let (ingress, mut incoming) = mpsc::channel(1);
    capacity.close();
    let original = Payload::new(3, &disposed);
    let pointer = original.bytes.as_ptr();
    let refused = try_retain(&ingress, &capacity, original);
    let source_keeps_cause = match &refused {
        Err(error @ IngressError::Capacity { cause, .. }) => error
            .source()
            .and_then(|source| source.downcast_ref::<TryAcquireError>())
            .is_some_and(|source| ptr::eq(source, cause)),
        _ => false,
    };
    let Err(IngressError::Capacity { original, cause }) = refused else {
        panic!("closed capacity must retain the original with its actual closure");
    };
    let refused_pointer = original.bytes.as_ptr();
    let queued = incoming.try_recv();
    let before_disposal = disposed.lock().unwrap().clone();
    drop(original);
    let after_disposal = disposed.lock().unwrap().clone();

    assert!(matches!(cause, TryAcquireError::Closed));
    assert!(
        source_keeps_cause,
        "source retains the actual native capacity cause"
    );
    assert_eq!(refused_pointer, pointer);
    assert!(queued.is_err());
    assert_eq!(before_disposal, []);
    assert_eq!(after_disposal, [3]);
}

#[test]
fn full_channel_preserves_its_original_and_charge() {
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let capacity = Arc::new(Semaphore::new(2));
    let (ingress, mut incoming) = mpsc::channel(1);
    let accepted = try_retain(&ingress, &capacity, Payload::new(4, &disposed));
    let original = Payload::new(5, &disposed);
    let pointer = original.bytes.as_ptr();
    let refused = try_retain(&ingress, &capacity, original);
    let Err(error @ IngressError::Queue { .. }) = refused else {
        panic!("full channel must retain its native refusal");
    };
    let diagnostic = format!("{error:?}: {error}");
    let source = error
        .source()
        .expect("source retains the actual native channel cause");
    let source_cause = source
        .downcast_ref::<TrySendError<(Payload, OwnedSemaphorePermit)>>()
        .expect("source retains the actual native channel error type");
    let IngressError::Queue { cause } = &error else {
        panic!("the native channel cause remains owned by its queue refusal");
    };
    let source_keeps_cause = ptr::eq(source_cause, cause);
    let source_message = source.to_string();
    let IngressError::Queue {
        cause: TrySendError::Full((original, permit)),
    } = error
    else {
        panic!("native Full must not become a capacity failure or closure");
    };
    let refused_pointer = original.bytes.as_ptr();
    let charged = capacity.available_permits();
    let charge = permit.num_permits();
    let queued = incoming.try_recv().unwrap();
    let before_disposal = disposed.lock().unwrap().clone();
    drop((original, permit));
    drop(queued);
    let after_disposal = disposed.lock().unwrap().clone();
    let released = capacity.available_permits();

    assert!(accepted.is_ok());
    assert_eq!(refused_pointer, pointer);
    assert_eq!((charged, charge, released), (0, 1, 2));
    assert!(source_keeps_cause);
    assert_eq!(source_message, "no available capacity");
    assert!(diagnostic.contains("Full(..)"));
    assert_eq!(before_disposal, []);
    assert_eq!(after_disposal, [5, 4]);
}

#[test]
fn closed_channel_preserves_its_original_and_charge() {
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let capacity = Arc::new(Semaphore::new(1));
    let (ingress, mut incoming) = mpsc::channel(1);
    incoming.close();
    let original = Payload::new(6, &disposed);
    let pointer = original.bytes.as_ptr();
    let refused = try_retain(&ingress, &capacity, original);
    let Err(IngressError::Queue {
        cause: TrySendError::Closed((original, permit)),
    }) = refused
    else {
        panic!("native Closed must retain the original and its charge");
    };
    let refused_pointer = original.bytes.as_ptr();
    let charged = capacity.available_permits();
    let charge = permit.num_permits();
    let before_disposal = disposed.lock().unwrap().clone();
    drop((original, permit));
    let after_disposal = disposed.lock().unwrap().clone();
    let released = capacity.available_permits();

    assert_eq!(refused_pointer, pointer);
    assert_eq!((charged, charge, released), (0, 1, 1));
    assert_eq!(before_disposal, []);
    assert_eq!(after_disposal, [6]);
}
