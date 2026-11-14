//! Actual Source dequeue and joined cancellation preserve one selection outcome.
use super::super::{Client, ClientError, ClientLimits};
use crate::MailAddr;
use crate::application::{ApplicationScope, MailboxCapacity};
use crate::local::endpoint::ExtractLocalEndpoint;
use crate::remote::export::Exports;
use crate::remote::{Attempt, json};
use behavior::{EstablishedRecipient, MessageProtocol, Never, User};
use futures_util::FutureExt;
use std::cell::RefCell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Wake, Waker};
use std::time::Duration;
use tokio::sync::oneshot;
use zenoh_config::{Config, ModeDependentValue, WhatAmI};

struct SourceJoin {
    publication: Mutex<Option<oneshot::Sender<()>>>,
    returned: Mutex<Option<Result<(), ()>>>,
    wakes: AtomicUsize,
}

impl Wake for SourceJoin {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let publication = self
            .publication
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(publication) = publication {
            let returned = publication.send(());
            *self.returned.lock().unwrap_or_else(PoisonError::into_inner) = Some(returned);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one actual capacity wake and both original messages retain native cleanup and complete row facts before the selection oracle"
)]
async fn acquired_acknowledgment_is_selected_before_joined_cancels_its_operation() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut configuration = Config::default();
        configuration.set_mode(Some(WhatAmI::Peer))
            .expect("the private native fixture selects Peer");
        configuration.connect.set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .expect("no outgoing native endpoint");
        configuration.listen.set_endpoints(ModeDependentValue::Unique(Vec::new()))
            .expect("no listening native endpoint");
        configuration.scouting.multicast.set_enabled(Some(false))
            .expect("no multicast scouting");
        configuration.scouting.multicast.set_listen(Some(ModeDependentValue::Unique(false)))
            .expect("no multicast listening");
        configuration.scouting.gossip.set_enabled(Some(false))
            .expect("no gossip scouting");
        let session = zenoh::open(configuration).await
            .expect("the actual local native Session opens");

        // Communication rounds the requested capacity1 to its actual minimum2.
        // Both real originals fill that physical ring before the observer polls.
        let scope = ApplicationScope::new().client_capacity(
            MailboxCapacity::new(1).expect("the effective Source ring has two slots"));
        let source = scope.external::<MessageProtocol<MailAddr, Box<[u8]>>>()
            .expect("the real Source endpoint allocates");
        let target = scope.external::<MessageProtocol<MailAddr, ()>>()
            .expect("the independent real return endpoint allocates");
        let source_address = source.address();
        let target_address = target.address();
        let source_endpoint = source.recipient().interpret(&mut ExtractLocalEndpoint);
        let target_endpoint = target.recipient().interpret(&mut ExtractLocalEndpoint);
        let acknowledgment = Box::<[u8]>::from([41, 43, 47]);
        let acknowledgment_address = acknowledgment.as_ptr();
        let foreign = Box::<[u8]>::from([53, 59]);
        let foreign_address = foreign.as_ptr();
        let admitted = target.send(&source.recipient(), acknowledgment).await;
        let foreign_admitted = source.send(&source.recipient(), foreign).await;

        let (publication, joined) = oneshot::channel();
        let observer = Arc::new(SourceJoin {
            publication: Mutex::new(Some(publication)),
            returned: Mutex::new(None),
            wakes: AtomicUsize::new(0),
        });
        let wake = Waker::from(Arc::clone(&observer));
        let mut context = Context::from_waker(&wake);
        let mut available = pin!(source_endpoint.wait_for_capacity());
        let initially_available = available.as_mut().poll(&mut context);

        let selected = RefCell::new(Vec::new());
        let authority = RefCell::new(target_address);
        let provider_calls = RefCell::new(Vec::new());
        let maximum = NonZeroUsize::new(4).unwrap();
        let limits = json::Limits {
            input_bytes: NonZeroUsize::new(8192).unwrap(),
            proof_bytes: NonZeroUsize::new(64).unwrap(),
            token_bytes: NonZeroUsize::new(128).unwrap(),
            nesting: NonZeroUsize::new(4).unwrap(),
            parser_states: NonZeroUsize::new(16).unwrap(),
        };
        let mut client = Client::new(
            source,
            "bombay/outbound/source-prefix",
            ClientLimits {
                concurrent_requests: maximum,
                retained_requests: maximum,
                retained_responses: maximum,
            },
            limits,
            Duration::from_secs(20),
            |original: &User<MailAddr, Box<[u8]>>, runtime: &str, target| {
                selected.borrow_mut().push((original.from, original.message.as_ptr(),
                    original.message.to_vec(), runtime.to_owned(), target));
                Ok::<_, &'static str>(None::<Box<[u8]>>)
            },
            async |_: &[u8], _: &User<MailAddr, Box<[u8]>>| {
                provider_calls.borrow_mut().push("protection");
                Err::<Box<[u8]>, _>("this acknowledgment has no outgoing request")
            },
            async |_: &[u8], _: &[u8]| {
                provider_calls.borrow_mut().push("verification");
                Err::<(), _>("no native response was requested")
            },
            |(): &(), _: &[u8], _: &User<MailAddr, Box<[u8]>>| {
                Err::<(), _>("no response delivery was requested")
            },
            |original: &User<MailAddr, Box<[u8]>>, runtime: &str, target| {
                let authority = authority.borrow();
                if original.from == *authority && target == *authority
                    && runtime == "source-prefix" {
                    Ok(authority)
                } else {
                    Err("the declared route is not this acknowledgment's target")
                }
            },
            |(): &(), _: &User<MailAddr, Box<[u8]>>, _: &str, _: MailAddr| {
                Err::<(), _>("no reply permission is granted for a no-Get acknowledgment")
            },
        );
        let declared = <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
            MessageProtocol<MailAddr, ()>, Never, (),
            EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
        >>::declare(&mut client, &session).await;
        <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
            MessageProtocol<MailAddr, ()>, Never, (),
            EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
        >>::serve(&mut client, (target_endpoint, target_address),
            "source-prefix".to_owned(), joined.shared()).await;
        <Client<_, _, _, _, _, _, _, _, _, _> as Exports<
            MessageProtocol<MailAddr, ()>, Never, (),
            EstablishedRecipient<MessageProtocol<MailAddr, ()>>,
        >>::withdraw(&mut client).await;
        let closed = session.close().await;
        let (source, _inputs, startup, withdrawal, joined, eof, rows, _routes) = client.into_parts();
        // The actual user-lane EOF is one-shot and already retained by Client.
        // Re-polling its Source would wait on the still-live control lane.
        let returned_source = source.address();
        let publication = observer.returned.lock()
            .unwrap_or_else(PoisonError::into_inner).take();
        let [first, foreign]: [_; 2] = rows.try_into()
            .unwrap_or_else(|_| panic!("the two accepted originals remain in order"));
        let (original, refusal, encoded, protected, permission, capacity, query, callback,
            replies, callback_eof, protection_panics, operation_panics, completed) = first;
        let (foreign, foreign_refusal, foreign_encoded, foreign_protected, foreign_permission,
            foreign_capacity, foreign_query, foreign_callback, foreign_replies, foreign_eof,
            foreign_protection_panics, foreign_operation_panics, foreign_completed) = foreign;
        let selected = selected.borrow();

        assert!(admitted.is_ok() && foreign_admitted.is_ok());
        assert!(declared.is_ok() && closed.is_ok());
        assert!(matches!(startup, Some(Ok(()))) && matches!(withdrawal, Some(Ok(()))));
        assert!(matches!(joined, Some(Ok(()))) && eof.is_some());
        assert_eq!(returned_source, source_address);
        assert!(initially_available.is_pending());
        assert_eq!(observer.wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(publication, Some(Ok(()))));
        assert_eq!((original.from, original.message.as_ptr()), (target_address, acknowledgment_address));
        assert_eq!(original.message.as_ref(), [41, 43, 47]);
        assert_eq!((foreign.from, foreign.message.as_ptr()), (source_address, foreign_address));
        assert_eq!(foreign.message.as_ref(), [53, 59]);
        assert!(matches!(foreign_refusal, Some(ClientError::Origin { expected }) if expected == target_address));
        assert!(foreign_encoded.is_none() && matches!(foreign_query, Attempt::Unattempted));
        assert!(foreign_protected.is_none() && foreign_permission.is_none() && foreign_capacity.is_none());
        assert!(foreign_callback.is_none() && foreign_replies.is_empty() && foreign_eof.is_none());
        assert!(matches!(foreign_protection_panics, (None, None, None))
            && matches!(foreign_operation_panics, (None, None, None)) && foreign_completed.is_none());
        assert!(refusal.is_none() && protected.is_none() && permission.is_none() && capacity.is_none());
        assert!(matches!(query, Attempt::Unattempted) && callback.is_none() && replies.is_empty());
        assert!(callback_eof.is_none() && completed.is_none());
        assert!(matches!(protection_panics, (None, None, None))
            && matches!(operation_panics, (None, None, None)));
        assert!(provider_calls.borrow().is_empty());
        // Primary old-source failure after all original and native cleanup facts.
        assert_eq!(selected.len(), 1);
        assert_eq!((selected[0].0, selected[0].1), (target_address, acknowledgment_address));
        assert_eq!(selected[0].2.as_slice(), [41, 43, 47]);
        assert_eq!((selected[0].3.as_str(), selected[0].4), ("source-prefix", target_address));
        assert!(matches!(encoded, Some(Ok(None))));
    }).await.expect("the complete native custody trace finishes before native expiry");
}
