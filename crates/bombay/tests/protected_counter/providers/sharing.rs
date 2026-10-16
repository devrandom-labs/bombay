//! Receipt sharing costs exclude verification and native Export lookup.
use super::providers::{
    CounterOperation, ProofScope, ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef,
    VerificationError, logical_scope, metadata, recorded, scheduled,
};
use super::requester::Scope;
use alloc_count::AllocSnapshot;
use bombay::remote::verify;
use core::ops::AsyncFn;
use std::cell::RefCell;
use std::mem::size_of;
use std::rc::Rc;
use tokio::runtime::Builder;

#[expect(
    clippy::too_many_lines,
    reason = "preserve four original verified receipts, exact cost windows and all ownership observations"
)]
async fn receipt_sharing<Receipt>(
    profile: ProviderProfile,
    provider: impl AsyncFn(
        &ProtectedMessage,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Receipt, VerificationError>,
    projection: fn(&Receipt) -> (&ProofScope, Scope),
    evidence: fn(&Receipt) -> Option<&[u8]>,
) {
    let current = metadata(profile, "caller".into(), "receiver".into());
    let mut second = metadata(profile, "caller".into(), "receiver".into());
    second.request_sequence = 24;
    let first_scope = logical_scope();
    let mut second_scope = first_scope;
    second_scope.identity.sequence = 24;
    let mut duplicate_scope = first_scope;
    duplicate_scope.authority_generation = 20;
    let first_bytes = serde_json::to_vec(&ReadRequestRef {
        bombay: &current,
        kind: Purpose::Request,
        operation: CounterOperation::Read,
    })
    .unwrap();
    let second_bytes = serde_json::to_vec(&ReadRequestRef {
        bombay: &second,
        kind: Purpose::Request,
        operation: CounterOperation::Read,
    })
    .unwrap();
    let mut conflict_bytes = first_bytes.clone();
    conflict_bytes.push(b' ');
    let mut verification = Vec::new();
    for (bytes, scope) in [
        (&first_bytes, first_scope),
        (&second_bytes, second_scope),
        (&first_bytes, duplicate_scope),
        (&conflict_bytes, first_scope),
    ] {
        let records = RefCell::new(vec![(
            scope,
            ProtectedMessage {
                protected: bytes.clone().into_boxed_slice(),
                proof: Box::from([7, 11]),
            },
        )]);
        let original = ProtectedMessage {
            protected: bytes.clone().into_boxed_slice(),
            proof: Box::from([7, 11]),
        };
        let buffers = (original.protected.as_ptr(), original.proof.as_ptr());
        let mut receipt = None;
        verify(&original, &mut receipt, |request| {
            provider(request, &records)
        })
        .await;
        let receipt = receipt
            .expect("completed provider")
            .unwrap_or_else(|_| panic!("original registered provider evidence"));
        let claims = projection(&receipt).0.bombay.caller.as_ptr();
        let proof = evidence(&receipt).map(<[u8]>::as_ptr);
        let before = AllocSnapshot::now();
        let receipt = Rc::new(receipt);
        let sharing = before.elapsed();
        verification.push((original, receipt, buffers, claims, proof, sharing, scope));
    }
    assert_eq!(verification.len(), 4);
    let request = &verification[0].1;
    let allocation = Rc::as_ptr(request);
    let before = AllocSnapshot::now();
    let consumer = Rc::clone(request);
    let reader_sharing = before.elapsed();
    assert_eq!(reader_sharing.alloc_calls, 0);
    assert_eq!(reader_sharing.realloc_calls, 0);
    assert_eq!(reader_sharing.bytes_allocated, 0);
    assert_eq!(Rc::as_ptr(&consumer), allocation);
    assert_eq!(Rc::strong_count(&consumer), 2);
    assert_eq!(projection(&consumer).1, first_scope);
    drop(consumer);
    let first_sharing_bytes = verification[0].5.bytes_allocated;
    for (index, (original, receipt, buffers, claims, proof, sharing, scope)) in
        verification.into_iter().enumerate()
    {
        assert_eq!(sharing.alloc_calls, 1);
        assert_eq!(sharing.realloc_calls, 0);
        assert!(sharing.bytes_allocated >= size_of::<Receipt>());
        assert_eq!(sharing.bytes_allocated, first_sharing_bytes);
        assert_eq!(Rc::strong_count(&receipt), 1);
        let receipt = Rc::try_unwrap(receipt)
            .unwrap_or_else(|_| panic!("complete original receipt is uniquely owned"));
        let (actual, logical) = projection(&receipt);
        assert_eq!(logical, scope);
        assert_eq!(logical.identity.sequence, [23, 24, 23, 23][index]);
        assert_eq!(logical.authority_generation, [19, 19, 20, 19][index]);
        assert_eq!(&actual.bombay, if index == 1 { &second } else { &current });
        assert_eq!(actual.kind, Purpose::Request);
        assert_eq!(actual.operation, CounterOperation::Read);
        assert_eq!(actual.bombay.caller.as_ptr(), claims);
        assert_eq!(evidence(&receipt).map(<[u8]>::as_ptr), proof);
        assert_eq!(
            (original.protected.as_ptr(), original.proof.as_ptr()),
            buffers
        );
        assert_eq!(original.proof.as_ref(), [7, 11]);
        let expected = [&first_bytes, &second_bytes, &first_bytes, &conflict_bytes][index];
        assert_eq!(original.protected.as_ref(), expected.as_slice());
        if let Some(proof) = evidence(&receipt) {
            assert_eq!(proof, [7, 11]);
        }
        eprintln!(
            "receipt sharing requested {} bytes",
            sharing.bytes_allocated
        );
    }
}

#[test]
fn recorded_receipt_sharing_preserves_originals() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(receipt_sharing(
            ProviderProfile::Recorded,
            recorded::verify,
            recorded::projection,
            |_| None,
        ));
}

#[test]
fn scheduled_receipt_sharing_preserves_originals() {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(receipt_sharing(
            ProviderProfile::Scheduled,
            scheduled::verify,
            scheduled::projection,
            |receipt| Some(scheduled::proof(receipt)),
        ));
}
