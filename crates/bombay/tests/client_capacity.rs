use bombay::actors::ActorExt;
use bombay::behavior::{MessageProtocol, Never};
use bombay::{Application, ApplicationScope, MailAddr, MailboxCapacity};
use communication::TrySendError;
use std::ptr;
use tokio::runtime::Builder;

struct Root;

#[bombay::actor(message = Never)]
impl Root {}

type Replies = MessageProtocol<MailAddr, Box<[u8]>>;

#[test]
fn early_and_late_clients_share_the_configured_limit_and_keep_rejected_values() {
    let capacity = MailboxCapacity::new(3).expect("a positive minimum rounds to four");
    assert_eq!(capacity.get(), 4);
    let scope = ApplicationScope::new().client_capacity(capacity);
    let mut early = scope
        .external::<Replies>()
        .expect("the early endpoint starts");
    let early_origin = early.address();
    let early_recipient = early.recipient();
    for value in 0..4 {
        let admitted = early.try_send(&early_recipient, vec![value].into_boxed_slice());
        assert!(admitted.is_ok());
    }
    let rejected = vec![41].into_boxed_slice();
    let identity = rejected.as_ptr();
    let Err(TrySendError::Full(original)) = early.try_send(&early_recipient, rejected) else {
        panic!("the fifth exact input must be returned on saturation");
    };
    assert!(ptr::eq(identity, original.as_ptr()));
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the native host starts");
    let outcome =
        host.block_on(scope.with_root(Root.stop_on_shutdown()).run_with(
            |application| async move {
                let mut late = application
                    .external::<Replies>()
                    .expect("the late endpoint starts");
                assert_ne!(late.address(), early_origin);
                let late_recipient = late.recipient();
                for value in 0..4 {
                    let admitted =
                        late.try_send(&late_recipient, vec![value + 10].into_boxed_slice());
                    assert!(admitted.is_ok());
                }
                let rejection = late.try_send(&late_recipient, original);
                let Err(TrySendError::Full(original)) = rejection else {
                    panic!("the later endpoint must inherit the same bound");
                };
                assert!(ptr::eq(identity, original.as_ptr()));
                let shutdown = application.shutdown();
                assert_eq!(shutdown, Ok(()));
                let report = application.retirement().await;
                assert_eq!(
                    report.retirement(),
                    bombay::RetirementAssessment::Established
                );
                early.close_admission();
                late.close_admission();
                for value in 0..4 {
                    let accepted = early
                        .receive()
                        .await
                        .expect("the early accepted prefix remains");
                    assert_eq!(accepted.from, early_origin);
                    assert_eq!(&*accepted.message, &[value]);
                    let accepted = late
                        .receive()
                        .await
                        .expect("the late accepted prefix remains");
                    assert_eq!(accepted.from, late.address());
                    assert_eq!(&*accepted.message, &[value + 10]);
                }
                assert!(early.receive().await.is_none());
                assert!(late.receive().await.is_none());
                original
            },
        ))
        .unwrap_or_else(|_| panic!("the actual application starts"));
    let (original, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the work and joined cleanup complete"));
    assert!(ptr::eq(identity, original.as_ptr()));
    assert!(retirement.cleanup().is_ok());
}

#[test]
fn invalid_client_limits_return_the_original_requested_minimum() {
    let zero = MailboxCapacity::new(0).expect_err("empty requested capacity is invalid");
    assert_eq!(zero.minimum, 0);
    let overflow =
        MailboxCapacity::new(usize::MAX).expect_err("rounding must be checked before allocation");
    assert_eq!(overflow.minimum, usize::MAX);
    assert_eq!(
        MailboxCapacity::new(1)
            .expect("the primitive floor is two")
            .get(),
        2
    );
}

#[test]
fn application_clients_and_exported_interfaces_use_the_same_small_reply_bound() {
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the native host starts");
    let outcome = host
        .block_on(
            Application::new(Root.stop_on_shutdown())
                .client_capacity(MailboxCapacity::new(2).expect("two reply slots"))
                .run_with(|application| async move {
                    let peer = application
                        .external::<Replies>()
                        .expect("a real sending identity");
                    let mut root_client = application
                        .client::<Replies>()
                        .unwrap_or_else(|_| panic!("the root client starts"));
                    let mut selected = application
                        .client_to::<_, Replies>(peer.recipient())
                        .unwrap_or_else(|_| panic!("the selected client starts"));
                    let interface = application.interface(peer.recipient());
                    let mut exported = interface
                        .external::<Replies>()
                        .expect("the exported endpoint starts");
                    let recipients = [
                        root_client.recipient(),
                        selected.recipient(),
                        exported.recipient(),
                    ];
                    for recipient in &recipients {
                        for value in 0..2 {
                            let admitted = peer.try_send(recipient, vec![value].into_boxed_slice());
                            assert!(admitted.is_ok());
                        }
                        let original = vec![77].into_boxed_slice();
                        let identity = original.as_ptr();
                        let Err(TrySendError::Full(original)) = peer.try_send(recipient, original)
                        else {
                            panic!("every endpoint factory must use the configured bound");
                        };
                        assert!(ptr::eq(identity, original.as_ptr()));
                    }
                    let shutdown = application.shutdown();
                    assert_eq!(shutdown, Ok(()));
                    let report = application.retirement().await;
                    assert_eq!(
                        report.retirement(),
                        bombay::RetirementAssessment::Established
                    );
                    root_client.close_admission();
                    selected.close_admission();
                    exported.close_admission();
                    for value in 0..2 {
                        for accepted in [
                            root_client.receive().await,
                            selected.receive().await,
                            exported.receive().await,
                        ] {
                            let accepted = accepted.expect("each accepted prefix remains owned");
                            assert_eq!(accepted.from, peer.address());
                            assert_eq!(&*accepted.message, &[value]);
                        }
                    }
                    assert!(root_client.receive().await.is_none());
                    assert!(selected.receive().await.is_none());
                    assert!(exported.receive().await.is_none());
                }),
        )
        .unwrap_or_else(|_| panic!("the actual application starts"));
    let ((), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work and cleanup complete"));
    assert!(retirement.cleanup().is_ok());
}

#[test]
fn missing_host_returns_the_configured_application_and_original_work_for_retry() {
    let application = Application::new(Root.stop_on_shutdown())
        .client_capacity(MailboxCapacity::new(2).expect("two reply slots"));
    let original = vec![91].into_boxed_slice();
    let identity = original.as_ptr();
    let Err((application, work, _error)) = application.execute_with(|application| async move {
        let mut caller = application
            .external::<Replies>()
            .expect("the recovered caller starts");
        let recipient = caller.recipient();
        for value in 0..2 {
            let admitted = caller.try_send(&recipient, vec![value].into_boxed_slice());
            assert!(admitted.is_ok());
        }
        let Err(TrySendError::Full(original)) = caller.try_send(&recipient, original) else {
            panic!("host rejection must preserve the configured client bound");
        };
        assert!(ptr::eq(identity, original.as_ptr()));
        caller.close_admission();
        for value in 0..2 {
            assert_eq!(
                &*caller
                    .receive()
                    .await
                    .expect("the accepted prefix remains")
                    .message,
                &[value]
            );
        }
        assert!(caller.receive().await.is_none());
        let shutdown = application.shutdown();
        assert_eq!(shutdown, Ok(()));
        original
    }) else {
        panic!("execution outside an entered host must return its original inputs");
    };
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the native host starts");
    let outcome = host
        .block_on(application.run_with(work))
        .unwrap_or_else(|_| panic!("the returned application retries"));
    let (original, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the recovered work completes"));
    assert!(ptr::eq(identity, original.as_ptr()));
    assert!(retirement.cleanup().is_ok());
}

#[test]
fn unsupported_mailbox_minimums_return_the_original_request() {
    for minimum in [
        (1_usize << 30) + 1,
        (1_usize << 31) - 1,
        1_usize << 31,
        (1_usize << 31) + 1,
        usize::MAX - 1,
        usize::MAX,
        0,
    ] {
        let rejection = MailboxCapacity::new(minimum)
            .expect_err("an unsupported minimum is refused before mailbox construction");
        assert_eq!(rejection.minimum, minimum);
    }
}

#[test]
fn supported_mailbox_minimums_preserve_the_rounded_capacity() {
    for (minimum, effective) in [
        (1, 2),
        (2, 2),
        (3, 4),
        (1_023, 1_024),
        (1_024, 1_024),
        ((1_usize << 29) + 1, 1_usize << 30),
        ((1_usize << 30) - 1, 1_usize << 30),
        (1_usize << 30, 1_usize << 30),
    ] {
        let capacity = MailboxCapacity::new(minimum)
            .expect("the native capacity range includes this rounded minimum");
        assert_eq!(capacity.get(), effective);
    }
}
