//! Constructor unwind must preserve prepared native inputs and the original callable.
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, ClassifySettlement, EstablishedDelivery,
    EstablishedRecipient, InterpreterRequests, ItemSettlement, MessageProtocol, Never, NoSends,
    SettledItem, SettlementStatus,
};
use bombay::prelude::{ActorRetirement, Completion, Exit};
use bombay::{ApplicationCleanupError, ApplicationScope, MailAddr, WorkOutcome};
use core::future::{Future, poll_fn};
use core::task::Poll;
use std::fs::File;
use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

#[path = "application_services/requests.rs"]
mod requests;
use requests::{FileAccess, OpenFile, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = Services;
}

type DocumentReady = MessageProtocol<MailAddr, u8>;

fn file_request() -> OpenFile {
    OpenFile {
        path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        access: FileAccess::Ready,
    }
}

fn inspect_file(mut file: File) {
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the original acquired File remains usable");
    assert_eq!(contents, include_str!("../Cargo.toml"));
}

struct Notice {
    original: Box<[u8]>,
    file: Option<OpenFile>,
    ready: EstablishedRecipient<DocumentReady>,
}

#[bombay::actor(
    message = Never,
    sends = {
        files: InterpreterRequests<OpenFile>,
        ready: Vec<EstablishedDelivery<DocumentReady>>,
    },
)]
impl Notice {
    fn init(&mut self) -> BehaviorActed<Self> {
        let file = self.file.take().expect("the original notice file request");
        Ok(Actions::cont()
            .send_files(file)
            .send_ready(EstablishedDelivery::new(
                self.ready.clone(),
                self.original[0],
            )))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn constructor_unwind_keeps_original_actor_configuration_scope_and_work_for_retry() {
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the original early endpoint");
    let early_address = replies.address();
    let original = Box::<[u8]>::from([17, 19]);
    let actor_pointer = original.as_ptr();
    let work = Box::<[u8]>::from([23, 29]);
    let work_pointer = work.as_ptr();
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let invocations = Arc::new(AtomicUsize::new(0));
    let observed_invocations = Arc::clone(&invocations);
    let cause = Box::new([31_u8, 37]);
    let cause_pointer = ptr::from_ref(cause.as_ref()).cast::<()>();
    let constructor_failure = Mutex::new(Some(cause));
    let application = scope
        .with_root(
            Notice {
                original,
                file: Some(file_request()),
                ready: replies.recipient(),
            }
            .stop_on_shutdown(),
        )
        .with_interpreter(move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            let cause = constructor_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(cause) = cause {
                resume_unwind(cause);
            }
            Services
        });
    let (execution, receiving) = application
        .execute_with(move |application| {
            invocations.fetch_add(1, Ordering::SeqCst);
            async move {
                let report = replies
                    .receive()
                    .await
                    .expect("the original early endpoint survives retry");
                assert_eq!(report.message, 17);
                assert_eq!(report.from, application.root().address());
                let shutdown = application.request_shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                work
            }
        })
        .unwrap_or_else(|_| panic!("the actual host is entered before construction"));
    let mut execution = Box::pin(execution);
    let unwound = poll_fn(|context| {
        Poll::Ready(catch_unwind(AssertUnwindSafe(|| {
            execution.as_mut().poll(context)
        })))
    })
    .await;
    drop(execution);
    let Err(cause) = unwound else {
        panic!("the original constructor cause must unwind through the native execution port");
    };
    assert_eq!(ptr::from_ref(cause.as_ref()).cast::<()>(), cause_pointer);
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 0);
    let (work, native, notifications, ()) = receiving.await.into_parts();
    assert!(native.is_err());
    assert!(notifications.is_err());
    let WorkOutcome::Prepared {
        inputs: (root, spaces, scope),
        work,
        cleanup,
    } = work
    else {
        panic!("constructor unwind must preserve the original prepared actor and configuration");
    };
    assert!(matches!(
        cleanup,
        Err(ApplicationCleanupError::PublicationClosed(_))
    ));
    assert_eq!(root.base().original.as_ptr(), actor_pointer);
    assert_eq!(&*root.base().original, &[17, 19]);
    assert!(root.base().file.is_some());
    let additional = scope
        .external::<DocumentReady>()
        .expect("the retained original allocation source can establish another endpoint");
    assert_ne!(additional.address(), early_address);
    assert_ne!(additional.address(), MailAddr::APPLICATION_ROOT);
    let (work, native, notifications, ()) = scope
        .with_topology(root, spaces)
        .run_with(work)
        .await
        .unwrap_or_else(|_| panic!("the same original inputs retry in the same host"))
        .into_parts();
    let (work, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the retained original callable actually runs on retry"));
    assert_eq!(work.as_ptr(), work_pointer);
    assert_eq!(&*work, &[23, 29]);
    cleanup.expect("the actual native cleanup completes");
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 1);
    let notifications = notifications.expect("independent notification receiving");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the retried actor remains independently owned");
    let ActorRetirement::Completed {
        custody,
        completion,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("service construction is not behavior failure");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.behavior.base().original.as_ptr(), actor_pointer);
    assert!(custody.behavior.base().file.is_none());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.descendants.is_empty());
    assert!(custody.child_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    let mut files = 0;
    for settled in settlements {
        assert_eq!(settled.sends.owned, NoSends);
        assert!(settled.sends.inner.ready.is_empty());
        assert!(settled.creations.is_empty());
        for file in settled.sends.inner.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("the actual file is acquired exactly once on successful retry");
            };
            inspect_file(file);
            files += 1;
        }
    }
    assert_eq!(files, 1);
}
