//! Outbound proof refusal precedes any protected request registration.
use super::Scope;
use super::providers::{
    CounterOperation, ProtectedMessage, ProviderProfile, Purpose, ReadRequestRef,
    VerificationError, logical_scope, metadata, recorded, scheduled,
};
use bombay::remote::{Metadata, verify};
use core::ops::AsyncFn;
use std::cell::RefCell;
use tokio::runtime::Builder;

async fn request_protection_refusals(
    profile: ProviderProfile,
    protect: impl AsyncFn(
        &[u8],
        &Metadata,
        Scope,
        &RefCell<Vec<(Scope, ProtectedMessage)>>,
    ) -> Result<Box<[u8]>, VerificationError>,
) {
    let configured = metadata(profile, "caller-session".into(), "receiver-session".into());
    let scope = logical_scope();
    let records = RefCell::new(Vec::with_capacity(2));
    let mut caller = metadata(profile, "caller-session".into(), "receiver-session".into());
    caller.caller = "foreign-caller".into();
    let mut controller = metadata(profile, "caller-session".into(), "receiver-session".into());
    controller.controller = "foreign-controller".into();
    let mut wrong_profile = metadata(profile, "caller-session".into(), "receiver-session".into());
    wrong_profile.provider_profile = "unconfigured-provider".into();
    let mut foreign_scope = scope;
    foreign_scope.identity.binding.actor = "foreign-actor";
    let mut other_generation = scope;
    other_generation.identity.binding.actor_generation += 1;
    let mut refusals = Vec::new();
    for (claims, trusted, purpose, expected_scope) in [
        (&caller, &configured, Purpose::Request, scope),
        (&controller, &configured, Purpose::Request, scope),
        (&configured, &configured, Purpose::ReadReply, scope),
        (&wrong_profile, &wrong_profile, Purpose::Request, scope),
        (&configured, &configured, Purpose::Request, foreign_scope),
        (&configured, &configured, Purpose::Request, other_generation),
    ] {
        let bytes = serde_json::to_vec(&ReadRequestRef {
            bombay: claims,
            kind: purpose,
            operation: CounterOperation::Read,
        })
        .unwrap();
        let mut result = None;
        verify(&bytes, &mut result, |bytes| {
            protect(bytes, trusted, expected_scope, &records)
        })
        .await;
        refusals.push((bytes, result, records.borrow().len()));
    }
    assert_eq!(refusals.len(), 6);
    for (bytes, result, retained) in &refusals {
        assert_ne!(bytes.as_slice(), [] as [u8; 0]);
        assert!(matches!(result, Some(Err(VerificationError::Invalid))));
        assert_eq!(
            *retained, 0,
            "refused declarations cannot register proof authority"
        );
    }
    let records = records.borrow();
    assert!(records.is_empty());
}

#[test]
fn recorded_outbound_request_protection_refuses_untrusted_declarations() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(request_protection_refusals(
            ProviderProfile::Recorded,
            recorded::protect_request,
        ));
}

#[test]
fn scheduled_outbound_request_protection_refuses_untrusted_declarations() {
    Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(request_protection_refusals(
            ProviderProfile::Scheduled,
            scheduled::protect_request,
        ));
}
