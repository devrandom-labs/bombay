//! The selected initial route wait preserves originals and provider results.
use super::super::{Client, ClientLimits};
use crate::MailAddr;
use crate::application::{ApplicationScope, MailboxCapacity};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::export::Exports;
use crate::remote::{Attempt, json};
use behavior::{EstablishedRecipient, MessageProtocol, Never, User};
use futures_util::FutureExt;
use futures_util::future::Either;
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::ptr;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::unconstrained;
use zenoh::sample::Locality;
use zenoh_config::{Config, ModeDependentValue, WhatAmI};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmatched_request_stays_unattempted_until_joined() {
    tokio::time::timeout(Duration::from_secs(10), unmatched())
        .await
        .expect("the whole unmatched native trace cleans up before query expiry");
}

#[expect(
    clippy::too_many_lines,
    reason = "the complete native trace retains originals and cleanup before its independent query oracle"
)]
async fn unmatched() {
    let session = zenoh::open(configuration())
        .await
        .expect("actual isolated Session");
    // Deliberately no Queryable: no asynchronous remote startup race is needed.
    let runtime = session.zid().to_string();
    let scope = ApplicationScope::new()
        .client_capacity(MailboxCapacity::new(1).expect("one accepted source original fits"));
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .expect("actual request Source");
    let target = scope
        .external::<MessageProtocol<MailAddr, ()>>()
        .expect("actual independent return endpoint");
    let source_address = source.address();
    let target_address = target.address();
    let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let original = Box::<[u8]>::from([17, 19, 23]);
    let original_address = original.as_ptr();
    let original_bytes = original.to_vec();
    let admitted = target.send(&source.recipient(), original).await;
    let encodings = Cell::new(0usize);
    let protections = Cell::new(0usize);
    let verifications = Cell::new(0usize);
    let maximum = NonZeroUsize::new(1).unwrap();
    let limits = limits();
    // This policy issues exactly one literal JSON request and proof. It does
    // not learn identity or a receipt from network input. No response exists.
    let mut client = Client::new(
        source,
        "bombay/outbound/no-queryable",
        ClientLimits {
            concurrent_requests: maximum,
            retained_requests: maximum,
            retained_responses: maximum,
        },
        limits,
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            encodings.set(encodings.get() + 1);
            if original.from != target_address
                || actual_target != target_address
                || actual_runtime != runtime
                || original.message.as_ptr() != original_address
                || original.message.as_ref() != original_bytes.as_slice()
            {
                return Err("the actual accepted original does not match this route");
            }
            Ok(Some(Box::<[u8]>::from(b"{\"read\":1}".as_slice())))
        },
        async |bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            protections.set(protections.get() + 1);
            if bytes != b"{\"read\":1}"
                || original.message.as_ptr() != original_address
                || original.from != target_address
            {
                return Err("the finite request policy refuses different originals");
            }
            Ok(Box::<[u8]>::from([29, 31]))
        },
        async |_: &[u8], _: &[u8]| {
            verifications.set(verifications.get() + 1);
            Err::<Never, _>("no reply pair is configured in this no-Queryable trace")
        },
        |receipt: &Never, _: &[u8], _: &User<MailAddr, Box<[u8]>>| -> Result<(), &'static str> {
            match *receipt {}
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
            {
                Ok(())
            } else {
                Err("this request route is not configured")
            }
        },
        |receipt: &Never,
         _: &User<MailAddr, Box<[u8]>>,
         _: &str,
         _: MailAddr|
         -> Result<(), &'static str> { match *receipt {} },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::declare(&mut client, &session)
    .await;
    let matching = client
        .querier
        .as_ref()
        .expect("successful actual declaration")
        .matching_status()
        .await;
    let (publication, joined) = oneshot::channel();
    let (first_poll, publication) = {
        let mut serving = pin!(unconstrained(
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, ()>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
            >>::serve(
                &mut client,
                (endpoint, target_address),
                runtime.clone(),
                joined.shared()
            )
        ));
        let first_poll = poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await;
        // Always release the existing completion input before any law oracle.
        let publication = publication.send(());
        if first_poll.is_pending() {
            serving.await;
        }
        (first_poll, publication)
    };
    <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::withdraw(&mut client)
    .await;
    let closed = session.close().await;
    let (source, _inputs, startup, withdrawal, joined, source_eof, rows, routes) =
        client.into_parts();
    let returned_address = source.address();
    let [row]: [_; 1] = rows
        .try_into()
        .unwrap_or_else(|_| panic!("the one acquired original is retained"));
    let (
        original,
        refusal,
        encoded,
        protected,
        permission,
        capacity,
        query,
        callback,
        replies,
        callback_eof,
        protection_panics,
        operation_panics,
        completed,
    ) = row;
    let original_pointer = original.message.as_ptr();
    let encoded = encoded
        .expect("actual source selection")
        .expect("actual encoding success")
        .expect("a real Get is selected");
    let protected = protected
        .expect("actual protection acquisition")
        .expect("the exact finite request protects");
    let matching = matching.expect("the actual native matching snapshot succeeds");
    let counts = (encodings.get(), protections.get(), verifications.get());
    let expected_encoded = b"{\"read\":1}";
    let expected_protected = [29, 31];
    assert!(admitted.is_ok() && declared.is_ok() && closed.is_ok());
    assert!(publication.is_ok() && matches!(first_poll, Poll::Pending));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(joined, Some(Ok(()))));
    assert!(matches!(routes, (Some(Ok(())), None, Some(Ok(())))));
    assert!(!matching.matching());
    assert_eq!(returned_address, source_address);
    assert_eq!(original.from, target_address);
    assert_eq!(original_pointer, original_address);
    assert_eq!(original.message.as_ref(), original_bytes.as_slice());
    assert_eq!(encoded.as_ref(), expected_encoded);
    assert_eq!(protected.as_ref(), expected_protected);
    assert!(refusal.is_none() && permission.is_none() && capacity.is_none() && callback.is_none());
    assert!(matches!(protection_panics, (None, None, None)));
    assert!(matches!(operation_panics, (None, None, None)));
    assert_eq!(counts, (1, 1, 0));
    assert!(replies.is_empty());
    // An unattempted Get has no fabricated callback EOF.
    // N=1 exhausts row budget: do not repoll or fabricate Source EOF.
    assert_eq!(source_eof, None);
    // Completion may race joined; its actual independent Option stays retained.
    let _originals = (source, original, completed);
    // Initial route waiting never fabricates a Get or callback EOF.
    // An unmatched request retains the source and protection originals; the
    // initial visibility observation remains absent through joined cancellation.
    assert!(matches!(
        (query, callback_eof),
        (Attempt::Unattempted, None)
    ));
}
fn configuration() -> Config {
    let mut configuration = Config::default();
    configuration
        .set_mode(Some(WhatAmI::Peer))
        .expect("explicit isolated native Peer");
    configuration
        .set_queries_max_retained(NonZeroUsize::new(1))
        .expect("one actual native Get fits");
    configuration
        .connect
        .set_endpoints(ModeDependentValue::Unique(Vec::new()))
        .expect("no outgoing endpoints");
    configuration
        .listen
        .set_endpoints(ModeDependentValue::Unique(Vec::new()))
        .expect("no listening endpoints");
    configuration
        .scouting
        .multicast
        .set_enabled(Some(false))
        .expect("no multicast");
    configuration
        .scouting
        .multicast
        .set_listen(Some(ModeDependentValue::Unique(false)))
        .expect("no multicast listener");
    configuration
        .scouting
        .gossip
        .set_enabled(Some(false))
        .expect("no gossip");
    configuration
}
fn limits() -> json::Limits {
    json::Limits {
        input_bytes: NonZeroUsize::new(128).unwrap(),
        proof_bytes: NonZeroUsize::new(16).unwrap(),
        token_bytes: NonZeroUsize::new(32).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(8).unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_get_completes_beside_a_request_waiting_for_visibility_and_capacity() {
    tokio::time::timeout(Duration::from_secs(10), no_get())
        .await
        .expect("both original rows and actual native retirement finish");
}
#[expect(
    clippy::too_many_lines,
    reason = "both source originals and native cleanup precede the independent sibling-progress oracle"
)]
async fn no_get() {
    let session = zenoh::open(configuration())
        .await
        .expect("actual isolated Session");
    let runtime = session.zid().to_string();
    let scope = ApplicationScope::new().client_capacity(MailboxCapacity::new(2).unwrap());
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .unwrap();
    let target = scope.external::<MessageProtocol<MailAddr, ()>>().unwrap();
    let source_address = source.address();
    let target_address = target.address();
    let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let read = Box::<[u8]>::from([17, 19, 23]);
    let read_address = read.as_ptr();
    let notice = Box::<[u8]>::from([41, 43, 47]);
    let notice_address = notice.as_ptr();
    let read_admitted = target.send(&source.recipient(), read).await;
    let notice_admitted = target.send(&source.recipient(), notice).await;
    let encodings = Cell::new(0usize);
    let protections = Cell::new(0usize);
    let verifications = Cell::new(0usize);
    let permissions = Cell::new(0usize);
    let mut client = Client::new(
        source,
        "bombay/outbound/no-get-beside-pending-get",
        ClientLimits {
            concurrent_requests: NonZeroUsize::new(1).unwrap(),
            retained_requests: NonZeroUsize::new(2).unwrap(),
            retained_responses: NonZeroUsize::new(1).unwrap(),
        },
        limits(),
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            encodings.set(encodings.get() + 1);
            if original.from != target_address
                || actual_target != target_address
                || actual_runtime != runtime
            {
                return Err("the original source route differs");
            }
            match original.message.as_ref() {
                [17, 19, 23] if original.message.as_ptr() == read_address => {
                    Ok(Some(Box::<[u8]>::from(b"{\"read\":1}".as_slice())))
                }
                [41, 43, 47] if original.message.as_ptr() == notice_address => Ok(None),
                _ => Err("neither configured original was preserved"),
            }
        },
        async |bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            protections.set(protections.get() + 1);
            if bytes == b"{\"read\":1}"
                && original.from == target_address
                && original.message.as_ptr() == read_address
            {
                Ok(Box::<[u8]>::from([29, 31]))
            } else {
                Err("only the configured Read has an outgoing proof")
            }
        },
        async |_: &[u8], _: &[u8]| {
            verifications.set(verifications.get() + 1);
            Err::<Never, _>("no reply exists without a Queryable")
        },
        |receipt: &Never, _: &[u8], _: &User<MailAddr, Box<[u8]>>| -> Result<(), &'static str> {
            match *receipt {}
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            permissions.set(permissions.get() + 1);
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
            {
                Ok(())
            } else {
                Err("the accepted original is not this actual target")
            }
        },
        |receipt: &Never,
         _: &User<MailAddr, Box<[u8]>>,
         _: &str,
         _: MailAddr|
         -> Result<(), &'static str> { match *receipt {} },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::declare(&mut client, &session)
    .await;
    let matching = client
        .querier
        .as_ref()
        .expect("actual Querier")
        .matching_status()
        .await;
    let (publication, joined) = oneshot::channel();
    let joined = joined.shared();
    let first_poll = {
        let mut serving = pin!(unconstrained(
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, ()>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
            >>::serve(
                &mut client,
                (endpoint, target_address),
                runtime.clone(),
                joined.clone()
            )
        ));
        poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await
        // Lexical cancellation disposes the real serving producer while the
        // retained rows remain in Client. Joined is still genuinely Pending.
    };
    let before_join = client
        .requests
        .borrow()
        .iter()
        .map(|row| *row.completed.borrow())
        .collect::<Vec<_>>();
    let counts_before_join = (
        encodings.get(),
        protections.get(),
        verifications.get(),
        permissions.get(),
    );
    // Release is unconditional and kept independently of the cancelled serving
    // producer. Do not fabricate a Client-observed joined result.
    let publication = publication.send(());
    let actual_joined = joined.await;
    <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::withdraw(&mut client)
    .await;
    let closed = session.close().await;
    let (source, _inputs, startup, withdrawal, observed_joined, source_eof, rows, route) =
        client.into_parts();
    let returned_source = source.address();
    let [read, notice]: [_; 2] = rows
        .try_into()
        .unwrap_or_else(|_| panic!("both actually accepted source originals remain"));
    let (
        read,
        read_refusal,
        read_encoded,
        read_protected,
        read_permission,
        read_capacity,
        read_query,
        read_callback,
        read_replies,
        read_eof,
        read_protection_panics,
        read_operation_panics,
        read_completed,
    ) = read;
    let (
        notice,
        notice_refusal,
        notice_encoded,
        notice_protected,
        notice_permission,
        notice_capacity,
        notice_query,
        notice_callback,
        notice_replies,
        notice_eof,
        notice_protection_panics,
        notice_operation_panics,
        notice_completed,
    ) = notice;
    let (listener_started, positive_observed, listener_withdrawn) = route;
    let matching = matching.expect("actual native matching snapshot");
    assert!(read_admitted.is_ok() && notice_admitted.is_ok() && declared.is_ok() && closed.is_ok());
    assert!(publication.is_ok() && actual_joined.is_ok() && matches!(first_poll, Poll::Pending));
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(observed_joined.is_none());
    assert_eq!(source_eof, None); // N2 exhausted; no invented Source EOF.
    assert_eq!(returned_source, source_address);
    assert!(!matching.matching());
    assert!(
        matches!(listener_started, Some(Ok(())))
            && positive_observed.is_none()
            && matches!(listener_withdrawn, Some(Ok(())))
    );
    assert_eq!(
        (read.from, read.message.as_ptr()),
        (target_address, read_address)
    );
    assert_eq!(read.message.as_ref(), [17, 19, 23]);
    assert_eq!(
        (notice.from, notice.message.as_ptr()),
        (target_address, notice_address)
    );
    assert_eq!(notice.message.as_ref(), [41, 43, 47]);
    assert!(
        read_refusal.is_none()
            && read_permission.is_none()
            && read_capacity.is_none()
            && read_callback.is_none()
    );
    assert!(
        notice_refusal.is_none()
            && notice_permission.is_none()
            && notice_capacity.is_none()
            && notice_callback.is_none()
    );
    assert!(
        matches!(read_protection_panics, (None, None, None))
            && matches!(read_operation_panics, (None, None, None))
    );
    assert!(
        matches!(notice_protection_panics, (None, None, None))
            && matches!(notice_operation_panics, (None, None, None))
    );
    assert!(read_replies.is_empty() && notice_replies.is_empty());
    assert!(read_eof.is_none() && notice_eof.is_none());
    let read_encoded = read_encoded
        .expect("actual Read selection")
        .expect("actual encoding")
        .expect("Read selects Get");
    let read_protected = read_protected
        .expect("actual protection")
        .expect("the configured proof");
    assert_eq!(read_encoded.as_ref(), b"{\"read\":1}");
    assert_eq!(read_protected.as_ref(), [29, 31]);
    assert!(matches!(notice_encoded, Some(Ok(None))) && notice_protected.is_none());
    assert!(
        matches!(read_query, Attempt::Unattempted) && matches!(notice_query, Attempt::Unattempted)
    );
    assert_eq!(counts_before_join, (2, 1, 0, 2));
    // Primary: NoGet completed while actual Read held P1 pending visibility.
    assert_eq!(before_join.as_slice(), [None, Some(())]);
    assert_eq!((read_completed, notice_completed), (None, Some(())));
    let _originals = (source, read, notice);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_get_waits_for_a_later_queryable_then_delivers_the_checked_original() {
    tokio::time::timeout(Duration::from_secs(10), later_queryable())
        .await
        .expect("actual visibility, checked reply and native cleanup finish");
}
#[expect(
    clippy::too_many_lines,
    reason = "the late native route, complete reply and original custody form one observable trace"
)]
async fn later_queryable() {
    let session = zenoh::open(configuration())
        .await
        .expect("actual isolated Session");
    let runtime = session.zid().to_string();
    let key = "bombay/outbound/later-queryable";
    let scope = ApplicationScope::new().client_capacity(MailboxCapacity::new(1).unwrap());
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .unwrap();
    let mut target = scope.external::<MessageProtocol<MailAddr, ()>>().unwrap();
    let source_address = source.address();
    let target_address = target.address();
    let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let original = Box::<[u8]>::from([17, 19, 23]);
    let original_address = original.as_ptr();
    let admitted = target.send(&source.recipient(), original).await;
    let encodings = Cell::new(0usize);
    let protections = Cell::new(0usize);
    let verifications = Cell::new(0usize);
    let decodings = Cell::new(0usize);
    let permissions = Cell::new(0usize);
    let reply_permissions = Cell::new(0usize);
    let maximum = NonZeroUsize::new(1).unwrap();
    let mut client = Client::new(
        source,
        key,
        ClientLimits {
            concurrent_requests: maximum,
            retained_requests: maximum,
            retained_responses: maximum,
        },
        limits(),
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            encodings.set(encodings.get() + 1);
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
                && original.message.as_ptr() == original_address
                && original.message.as_ref() == [17, 19, 23]
            {
                Ok(Some(Box::<[u8]>::from(b"{\"read\":1}".as_slice())))
            } else {
                Err("the complete configured original differs")
            }
        },
        async |bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            protections.set(protections.get() + 1);
            if bytes == b"{\"read\":1}"
                && original.message.as_ptr() == original_address
                && original.from == target_address
            {
                Ok(Box::<[u8]>::from([29, 31]))
            } else {
                Err("only the configured original request protects")
            }
        },
        async |bytes: &[u8], proof: &[u8]| {
            verifications.set(verifications.get() + 1);
            // Unit deliberately discharges validation only after BOTH exact
            // independently configured original slices match. Wire stays owned.
            if bytes == b"{\"value\":7}" && proof == [37, 41] {
                Ok(())
            } else {
                Err("the independently configured reply pair differs")
            }
        },
        |(): &(), bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            decodings.set(decodings.get() + 1);
            if bytes == b"{\"value\":7}"
                && original.message.as_ptr() == original_address
                && original.message.as_ref() == [17, 19, 23]
                && original.from == target_address
            {
                Ok(())
            } else {
                Err("the checked reply is not for this original request")
            }
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            permissions.set(permissions.get() + 1);
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
            {
                Ok(())
            } else {
                Err("this actual request route is not configured")
            }
        },
        |(): &(), original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            reply_permissions.set(reply_permissions.get() + 1);
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
            {
                Ok(())
            } else {
                Err("this actual reply route is not configured")
            }
        },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::declare(&mut client, &session)
    .await;
    let matching = client
        .querier
        .as_ref()
        .expect("actual Querier")
        .matching_status()
        .await;
    let (publication, joined) = oneshot::channel();
    let (first_poll, initial_counts, queryable, observations, publication) = {
        let mut serving = pin!(unconstrained(
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, ()>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
            >>::serve(
                &mut client,
                (endpoint, target_address),
                runtime.clone(),
                joined.shared()
            )
        ));
        let first_poll = poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await;
        let initial_counts = (
            encodings.get(),
            protections.get(),
            verifications.get(),
            decodings.get(),
            permissions.get(),
        );
        // The actual Source was accepted and serving was polled BEFORE this
        // native SessionLocal declaration. No sleeps or guessed route epoch.
        let queryable = session
            .declare_queryable(key)
            .allowed_origin(Locality::SessionLocal)
            .await
            .expect("actual later Queryable declaration");
        let native = async {
            // If a mutant already made its fresh permission call before the
            // route existed, do not hang awaiting the already-lost native Get.
            // Retain that actual count and fail its post-cleanup oracle instead.
            let observations = if initial_counts.4 == 1 && matches!(first_poll, Poll::Pending) {
                let query = queryable
                    .recv_async()
                    .await
                    .expect("the retained Get reaches the newly actual Queryable");
                let raw = query
                    .payload()
                    .expect("actual Get payload")
                    .to_bytes()
                    .into_owned();
                let proof = query
                    .attachment()
                    .expect("actual Get proof")
                    .to_bytes()
                    .into_owned();
                let published = query
                    .reply(key, b"{\"value\":7}".as_slice())
                    .attachment(vec![37, 41])
                    .await;
                drop(query); // Actual native last-owner FINAL; never fake EOF.
                let received = target.receive().await;
                Some((raw, proof, published, received))
            } else {
                None
            };
            let publication = publication.send(()); // unconditional release
            (observations, publication)
        };
        let ((), (observations, publication)) = tokio::join!(serving.as_mut(), native);
        (
            first_poll,
            initial_counts,
            queryable,
            observations,
            publication,
        )
    };
    <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::withdraw(&mut client)
    .await;
    let queryable_withdrawn = queryable.undeclare().await;
    let closed = session.close().await;
    let (source, _inputs, startup, withdrawal, observed_joined, source_eof, rows, route) =
        client.into_parts();
    let returned_source = source.address();
    let [row]: [_; 1] = rows
        .try_into()
        .unwrap_or_else(|_| panic!("one actual source original remains"));
    let (
        original,
        refusal,
        encoded,
        protected,
        permission,
        capacity,
        query,
        callback,
        replies,
        callback_eof,
        protection_panics,
        operation_panics,
        completed,
    ) = row;
    let (listener_started, positive_observed, listener_withdrawn) = route;
    let matching = matching.expect("actual initial native matching snapshot");
    let counts = (
        encodings.get(),
        protections.get(),
        verifications.get(),
        decodings.get(),
        permissions.get(),
        reply_permissions.get(),
    );
    assert!(admitted.is_ok() && declared.is_ok() && queryable_withdrawn.is_ok() && closed.is_ok());
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(
        publication.is_ok()
            && matches!(observed_joined, Some(Ok(())))
            && matches!(first_poll, Poll::Pending)
    );
    assert!(!matching.matching());
    assert_eq!(returned_source, source_address);
    assert_eq!(
        (original.from, original.message.as_ptr()),
        (target_address, original_address)
    );
    assert_eq!(original.message.as_ref(), [17, 19, 23]);
    assert!(refusal.is_none() && permission.is_none() && capacity.is_none() && callback.is_none());
    assert!(
        matches!(protection_panics, (None, None, None))
            && matches!(operation_panics, (None, None, None))
    );
    assert_eq!(source_eof, None); // One acquired row exhausts N1.
    // Primary readiness-placement oracle precedes facts absent on an early Get.
    assert_eq!(initial_counts, (1, 1, 0, 0, 1));
    let (raw, proof, published, received) =
        observations.expect("one real later native query/reply trace");
    let received =
        received.expect("the actual independent target receives the checked unit result");
    assert_eq!(raw.as_slice(), b"{\"read\":1}");
    assert_eq!(proof.as_slice(), [29, 31]);
    assert!(published.is_ok());
    assert_eq!((received.from, received.message), (source_address, ()));
    assert!(
        matches!(listener_started, Some(Ok(())))
            && matches!(positive_observed, Some(Ok(())))
            && matches!(listener_withdrawn, Some(Ok(())))
    );
    assert!(matches!(query, Attempt::Returned(Ok(()))) && callback_eof.is_some());
    let encoded = encoded
        .expect("actual selected encoding")
        .expect("finite encoding success")
        .expect("real Get selected");
    let protected = protected
        .expect("actual protected result")
        .expect("finite protection success");
    assert_eq!(encoded.as_ref(), b"{\"read\":1}");
    assert_eq!(protected.as_ref(), [29, 31]);
    assert_eq!(counts, (1, 1, 1, 1, 2, 2));
    let [response]: [_; 1] = replies
        .try_into()
        .unwrap_or_else(|_| panic!("one actual native response remains"));
    let payload = response.payload().flatten().copied().collect::<Vec<_>>();
    let proof = response
        .proof()
        .expect("actual native reply proof")
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let (cause, wire, shape, verified, decoded, reply_permission, panics) = response.into_parts();
    assert_eq!(payload.as_slice(), b"{\"value\":7}");
    assert_eq!(proof.as_slice(), [37, 41]);
    assert!(
        cause.is_none()
            && matches!(shape, Some(Ok(())))
            && matches!(verified, Some(Ok(())))
            && reply_permission.is_none()
    );
    assert!(matches!(
        decoded,
        Some(Ok(Either::Right(Attempt::Returned(Ok(())))))
    ));
    assert!(matches!(panics, (None, None, None)));
    let (wire_payload, wire_proof) = wire.expect("complete original protected wire remains");
    assert_eq!(wire_payload.as_ref(), payload.as_slice());
    assert_eq!(wire_proof.as_ref(), proof.as_slice());
    // Delivery may cause joined to dispose the still-pending query operation;
    // retain completion truthfully rather than requiring a fabricated Some.
    let _originals = (source, original, completed);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_native_listener_refuses_get_after_shared_wait_completes() {
    tokio::time::timeout(Duration::from_secs(10), closed_session())
        .await
        .expect("the whole native close/error trace cleans up before query expiry");
}

#[expect(
    clippy::too_many_lines,
    reason = "actual native error identity and cleanup precede the unattempted-query oracle"
)]
async fn closed_session() {
    let session = zenoh::open(configuration())
        .await
        .expect("actual isolated Session");
    // Deliberately no Queryable: no asynchronous remote startup race is needed.
    let runtime = session.zid().to_string();
    let scope = ApplicationScope::new()
        .client_capacity(MailboxCapacity::new(1).expect("one accepted source original fits"));
    let source = scope
        .external::<MessageProtocol<MailAddr, Box<[u8]>>>()
        .expect("actual request Source");
    let target = scope
        .external::<MessageProtocol<MailAddr, ()>>()
        .expect("actual independent return endpoint");
    let source_address = source.address();
    let target_address = target.address();
    let endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
    let original = Box::<[u8]>::from([17, 19, 23]);
    let original_address = original.as_ptr();
    let original_bytes = original.to_vec();
    let admitted = target.send(&source.recipient(), original).await;
    let encodings = Cell::new(0usize);
    let protections = Cell::new(0usize);
    let verifications = Cell::new(0usize);
    let maximum = NonZeroUsize::new(1).unwrap();
    let limits = limits();
    // This policy issues exactly one literal JSON request and proof. It does
    // not learn identity or a receipt from network input. No response exists.
    let mut client = Client::new(
        source,
        "bombay/outbound/no-queryable",
        ClientLimits {
            concurrent_requests: maximum,
            retained_requests: maximum,
            retained_responses: maximum,
        },
        limits,
        Duration::from_secs(20),
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            encodings.set(encodings.get() + 1);
            if original.from != target_address
                || actual_target != target_address
                || actual_runtime != runtime
                || original.message.as_ptr() != original_address
                || original.message.as_ref() != original_bytes.as_slice()
            {
                return Err("the actual accepted original does not match this route");
            }
            Ok(Some(Box::<[u8]>::from(b"{\"read\":1}".as_slice())))
        },
        async |bytes: &[u8], original: &User<MailAddr, Box<[u8]>>| {
            protections.set(protections.get() + 1);
            if bytes != b"{\"read\":1}"
                || original.message.as_ptr() != original_address
                || original.from != target_address
            {
                return Err("the finite request policy refuses different originals");
            }
            Ok(Box::<[u8]>::from([29, 31]))
        },
        async |_: &[u8], _: &[u8]| {
            verifications.set(verifications.get() + 1);
            Err::<Never, _>("no reply pair is configured in this no-Queryable trace")
        },
        |receipt: &Never, _: &[u8], _: &User<MailAddr, Box<[u8]>>| -> Result<(), &'static str> {
            match *receipt {}
        },
        |original: &User<MailAddr, Box<[u8]>>, actual_runtime: &str, actual_target| {
            if original.from == target_address
                && actual_target == target_address
                && actual_runtime == runtime
            {
                Ok(())
            } else {
                Err("this request route is not configured")
            }
        },
        |receipt: &Never,
         _: &User<MailAddr, Box<[u8]>>,
         _: &str,
         _: MailAddr|
         -> Result<(), &'static str> { match *receipt {} },
    );
    let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::declare(&mut client, &session)
    .await;
    let matching = client
        .querier
        .as_ref()
        .expect("successful actual declaration")
        .matching_status()
        .await;
    let (publication, joined) = oneshot::channel();
    let (first_poll, closed, second_poll, publication) = {
        let mut serving = pin!(unconstrained(
            <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
                MessageProtocol<MailAddr, ()>,
                Never,
                (),
                EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
            >>::serve(
                &mut client,
                (endpoint, target_address),
                runtime.clone(),
                joined.shared()
            )
        ));
        let first_poll = poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await;
        // First poll reaches the actual empty ring receive. Native close drops
        // the registered callback/sender, rather than fabricating a cause.
        let closed = session.close().await;
        // Observe native failure before joined cancellation can end the wait.
        let second_poll = match first_poll {
            Poll::Pending => {
                Some(poll_fn(|context| Poll::Ready(serving.as_mut().poll(context))).await)
            }
            Poll::Ready(()) => None,
        };
        // Always release the existing completion input before any law oracle.
        let publication = publication.send(());
        if let Some(Poll::Pending) = second_poll {
            serving.await;
        }
        (first_poll, closed, second_poll, publication)
    };
    <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
        MessageProtocol<MailAddr, ()>,
        Never,
        (),
        EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
    >>::withdraw(&mut client)
    .await;
    let original_error = {
        let visibility = client.visibility.borrow();
        let Some(Err(cause)) = visibility.as_ref() else {
            panic!("the actual closed native listener error must be retained");
        };
        ptr::from_ref(cause.as_ref()).cast::<()>()
    };
    let (source, _inputs, startup, withdrawal, joined, source_eof, rows, routes) =
        client.into_parts();
    let (listener, observed, listener_withdrawal) = routes;
    let observed = observed
        .expect("the second native poll returned a visibility result")
        .expect_err("closing the actual pending listener cannot be a positive snapshot");
    let projected_error = ptr::from_ref(observed.as_ref()).cast::<()>();
    let returned_address = source.address();
    let [row]: [_; 1] = rows
        .try_into()
        .unwrap_or_else(|_| panic!("the one acquired original is retained"));
    let (
        original,
        refusal,
        encoded,
        protected,
        permission,
        capacity,
        query,
        callback,
        replies,
        callback_eof,
        protection_panics,
        operation_panics,
        completed,
    ) = row;
    let original_pointer = original.message.as_ptr();
    let encoded = encoded
        .expect("actual source selection")
        .expect("actual encoding success")
        .expect("a real Get is selected");
    let protected = protected
        .expect("actual protection acquisition")
        .expect("the exact finite request protects");
    let matching = matching.expect("the actual native matching snapshot succeeds");
    let counts = (encodings.get(), protections.get(), verifications.get());
    let expected_encoded = b"{\"read\":1}";
    let expected_protected = [29, 31];
    assert!(admitted.is_ok() && declared.is_ok() && closed.is_ok());
    assert!(
        publication.is_ok()
            && matches!(first_poll, Poll::Pending)
            && matches!(second_poll, Some(Poll::Pending))
    );
    assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
    assert!(matches!(joined, Some(Ok(()))));
    assert!(matches!(listener, Some(Ok(()))) && matches!(listener_withdrawal, Some(Ok(()))));
    assert_eq!(projected_error, original_error);
    assert!(!matching.matching());
    assert_eq!(returned_address, source_address);
    assert_eq!(original.from, target_address);
    assert_eq!(original_pointer, original_address);
    assert_eq!(original.message.as_ref(), original_bytes.as_slice());
    assert_eq!(encoded.as_ref(), expected_encoded);
    assert_eq!(protected.as_ref(), expected_protected);
    assert!(refusal.is_none() && permission.is_none() && capacity.is_none() && callback.is_none());
    assert!(matches!(protection_panics, (None, None, None)));
    assert!(matches!(operation_panics, (None, None, None)));
    assert_eq!(counts, (1, 1, 0));
    assert!(replies.is_empty());
    // An unattempted Get has no fabricated callback EOF.
    // N=1 exhausts row budget: do not repoll or fabricate Source EOF.
    assert_eq!(source_eof, None);
    // The operation returned after the actual visibility Err and before joined.
    assert_eq!(completed, Some(()));
    let _originals = (source, original, observed);
    // Shared wait completion is not positive route visibility. Its exact
    // acquired native error above must gate both Get and callback creation.
    assert!(matches!(
        (query, callback_eof),
        (Attempt::Unattempted, None)
    ));
}
