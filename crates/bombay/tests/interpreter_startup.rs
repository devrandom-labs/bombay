//! Guarded service construction preserves the actor, original actions and callable.
use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, ClassifySettlement, EstablishedDelivery,
    EstablishedRecipient, InterpretationProgress, InterpreterRequests, ItemSettlement,
    MessageProtocol, Never, NoSends, SettledItem, SettlementStatus, Step,
};
use bombay::prelude::{ActorRetirement, Completion, Exit};
use bombay::{Application, ApplicationScope, MailAddr, WorkOutcome};
use std::fs::File;
use std::io::Read;
use std::mem;
use std::panic::resume_unwind;
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
    startup: NoticeStartup,
}

enum NoticeStartup {
    Ready,
    Rejected(NoticeError),
    Panicking(Box<[u8; 2]>),
}

#[derive(Debug, PartialEq)]
enum NoticeError {
    Restricted(Box<[u8]>),
}

#[bombay::actor(
    message = Never,
    error = NoticeError,
    sends = {
        files: InterpreterRequests<OpenFile>,
        ready: Vec<EstablishedDelivery<DocumentReady>>,
    },
)]
impl Notice {
    fn init(&mut self) -> BehaviorActed<Self> {
        match mem::replace(&mut self.startup, NoticeStartup::Ready) {
            NoticeStartup::Ready => {}
            NoticeStartup::Rejected(error) => return Err(error),
            NoticeStartup::Panicking(cause) => resume_unwind(cause),
        }
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
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
async fn constructor_unwind_preserves_actor_original_actions_cause_and_uninvoked_work() {
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the original early endpoint");
    let original = Box::<[u8]>::from([17, 19]);
    let actor_pointer = original.as_ptr();
    let work = Box::<[u8]>::from([23, 29]);
    let work_pointer = work.as_ptr();
    let request = file_request();
    let path_pointer = request.path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_path = request.path.clone();
    let expected_ready = replies.recipient();
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
                file: Some(request),
                ready: replies.recipient(),
                startup: NoticeStartup::Ready,
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
                    .expect("the original reply recipient survives recovery");
                assert_eq!(report.message, 17);
                assert_eq!(report.from, application.root().address());
                let shutdown = application.request_shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                work
            }
        })
        .unwrap_or_else(|_| panic!("the actual host is entered"));
    let ((), outcome) = tokio::join!(execution, receiving);
    let (work, native, notifications, ()) = outcome.into_parts();
    let WorkOutcome::NotInvoked {
        work,
        startup_error,
        cleanup,
    } = work
    else {
        panic!("the original callable survives the failed private actor startup");
    };
    assert!(startup_error.is_some());
    cleanup.expect("native retirement completes despite the original constructor panic");
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 0);
    let notifications = notifications.expect("native notification results remain independent");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (origin, retirement) = native.expect("the failed native actor is independently received");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::HostCommitPanicked {
        custody,
        initialization: Some(InterpretationProgress::Original(initialization)),
        payload,
    } = retirement
    else {
        panic!("constructor failure preserves untouched initialization before host commitment");
    };
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), cause_pointer);
    assert_eq!(custody.behavior.base().original.as_ptr(), actor_pointer);
    assert_eq!(custody.behavior.base().ready, expected_ready);
    assert_eq!(&*custody.behavior.base().original, &[17, 19]);
    assert!(
        custody.behavior.base().file.is_none(),
        "pure initialization already emitted the original request"
    );
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(initialization.creates.len(), 0);
    assert!(matches!(initialization.become_, Step::Continue));
    assert_eq!(initialization.sends.owned, NoSends);
    let mut files = initialization.sends.inner.files.into_iter();
    let request = files
        .next()
        .expect("the exact original request was never interpreted");
    assert!(files.next().is_none());
    assert_eq!(
        request.path.as_os_str().as_encoded_bytes().as_ptr(),
        path_pointer
    );
    assert_eq!(request.path, expected_path);
    assert!(matches!(request.access, FileAccess::Ready));
    let mut ready = initialization.sends.inner.ready.into_iter();
    let ready_delivery = ready
        .next()
        .expect("the independent ready lane also stays untouched");
    assert!(ready.next().is_none());
    assert_eq!(ready_delivery.message, 17);
    assert_eq!(ready_delivery.to, expected_ready);
    let mut actor = custody.behavior.into_inner();
    actor.file = Some(request);
    actor.ready = ready_delivery.to;
    let (work, native, notifications, ()) = Application::new(actor.stop_on_shutdown())
        .with_interpreter(|| Services)
        .run_with(work)
        .await
        .unwrap_or_else(|_| {
            panic!("the original actor and callable recover under a fresh runtime configuration")
        })
        .into_parts();
    let (work, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the retained original callable runs during recovery"));
    assert_eq!(work.as_ptr(), work_pointer);
    assert_eq!(&*work, &[23, 29]);
    cleanup.expect("the recovered native cleanup completes");
    assert_eq!(
        observed_attempts.load(Ordering::SeqCst),
        1,
        "retirement must not retry service construction"
    );
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 1);
    let notifications = notifications.expect("independent recovered notification receiving");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the recovered actor remains independently owned");
    let ActorRetirement::Completed {
        custody,
        completion,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("recovered service construction permits normal actor completion");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.behavior.base().original.as_ptr(), actor_pointer);
    assert_eq!(custody.behavior.base().ready, expected_ready);
    assert!(custody.behavior.base().file.is_none());
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
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
        assert_eq!(settled.sends.inner.ready.len(), 0);
        assert_eq!(settled.creations.len(), 0);
        for file in settled.sends.inner.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("the actual file is acquired exactly once during successful recovery");
            };
            inspect_file(file);
            files += 1;
        }
    }
    assert_eq!(files, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn pure_initialization_refusal_never_constructs_services_even_during_retirement() {
    let scope = ApplicationScope::new();
    let replies = scope
        .external::<DocumentReady>()
        .expect("a real early reply endpoint");
    let original = Box::<[u8]>::from([41, 43]);
    let actor_pointer = original.as_ptr();
    let restriction = Box::<[u8]>::from([47, 53]);
    let restriction_pointer = restriction.as_ptr();
    let request = file_request();
    let path_pointer = request.path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_path = request.path.clone();
    let expected_ready = replies.recipient();
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let (work, native, notifications, ()) = scope
        .with_root(
            Notice {
                original,
                file: Some(request),
                ready: replies.recipient(),
                startup: NoticeStartup::Rejected(NoticeError::Restricted(restriction)),
            }
            .stop_on_shutdown(),
        )
        .with_interpreter(move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Services
        })
        .run()
        .await
        .unwrap_or_else(|_| panic!("the native host accepts the application declaration"))
        .into_parts();
    let WorkOutcome::NotInvoked {
        work,
        startup_error,
        cleanup,
    } = work
    else {
        panic!("refused initialization cannot grant application startup");
    };
    assert!(work.is_none());
    assert!(startup_error.is_some());
    cleanup.expect("native retirement works without constructing any service");
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 0);
    let notifications = notifications.expect("both independent native notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the original refused actor is independently received");
    let ActorRetirement::InitializationRejected { custody, error } = retirement else {
        panic!("the original pure refusal must not become a constructor failure");
    };
    let NoticeError::Restricted(restriction) = error;
    assert_eq!(restriction.as_ptr(), restriction_pointer);
    assert_eq!(&*restriction, &[47, 53]);
    let actor = custody.behavior.base();
    assert_eq!(actor.original.as_ptr(), actor_pointer);
    assert_eq!(actor.ready, expected_ready);
    assert_eq!(&*actor.original, &[41, 43]);
    assert!(matches!(actor.startup, NoticeStartup::Ready));
    let request = actor
        .file
        .as_ref()
        .expect("pure refusal retains its un-emitted file request");
    assert_eq!(
        request.path.as_os_str().as_encoded_bytes().as_ptr(),
        path_pointer
    );
    assert_eq!(request.path, expected_path);
    assert!(matches!(request.access, FileAccess::Ready));
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn pure_initialization_panic_retains_the_original_actor_request_and_cause_without_constructing_services()
 {
    let scope = ApplicationScope::new();
    let replies = scope
        .external::<DocumentReady>()
        .expect("the original early reply endpoint");
    let original = Box::<[u8]>::from([103, 107]);
    let actor_pointer = original.as_ptr();
    let request = file_request();
    let path_pointer = request.path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_path = request.path.clone();
    let expected_ready = replies.recipient();
    let cause = Box::new([109, 113]);
    let cause_pointer = ptr::from_ref(cause.as_ref()).cast::<()>();
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let (work, native, notifications, ()) = scope
        .with_root(
            Notice {
                original,
                file: Some(request),
                ready: replies.recipient(),
                startup: NoticeStartup::Panicking(cause),
            }
            .stop_on_shutdown(),
        )
        .with_interpreter(move || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Services
        })
        .run()
        .await
        .unwrap_or_else(|_| panic!("the actual native host is entered"))
        .into_parts();
    let WorkOutcome::NotInvoked {
        work: None,
        startup_error: Some(_),
        cleanup,
    } = work
    else {
        panic!("a pure initialization panic cannot publish application readiness");
    };
    cleanup.expect("retirement never initializes the missing service");
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 0);
    let notifications = notifications.expect("independent original notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the original actor is independently received");
    let ActorRetirement::InitializationPanicked { custody, payload } = retirement else {
        panic!("the original pure cause must not become a service-constructor failure");
    };
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), cause_pointer);
    let actor = custody.behavior.base();
    assert_eq!(actor.original.as_ptr(), actor_pointer);
    assert_eq!(actor.ready, expected_ready);
    assert_eq!(&*actor.original, &[103, 107]);
    assert!(matches!(actor.startup, NoticeStartup::Ready));
    let file = actor
        .file
        .as_ref()
        .expect("the original request was never emitted");
    assert_eq!(
        file.path.as_os_str().as_encoded_bytes().as_ptr(),
        path_pointer
    );
    assert_eq!(file.path, expected_path);
    assert!(matches!(file.access, FileAccess::Ready));
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one missing-host retry checks original application, constructor, callable and every native retirement lane"
)]
fn missing_host_returns_the_original_application_constructor_and_work_for_retry() {
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the original early recipient");
    let expected_ready = replies.recipient();
    let original = Box::<[u8]>::from([127, 131]);
    let actor_pointer = original.as_ptr();
    let work = Box::<[u8]>::from([137, 139]);
    let work_pointer = work.as_ptr();
    let label = Box::<[u8]>::from([149, 151]);
    let label_pointer = label.as_ptr() as usize;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let invocations = Arc::new(AtomicUsize::new(0));
    let observed_invocations = Arc::clone(&invocations);
    let application = scope
        .with_root(
            Notice {
                original,
                file: Some(file_request()),
                ready: replies.recipient(),
                startup: NoticeStartup::Ready,
            }
            .stop_on_shutdown(),
        )
        .with_interpreter(move || {
            assert_eq!(label.as_ptr() as usize, label_pointer);
            assert_eq!(&*label, &[149, 151]);
            attempts.fetch_add(1, Ordering::SeqCst);
            Services
        });
    let Err((application, work, host_error)) = application.execute_with(move |application| {
        invocations.fetch_add(1, Ordering::SeqCst);
        async move {
            let report = replies
                .receive()
                .await
                .expect("the original recipient accepts actual file readiness");
            assert_eq!(report.message, 127);
            assert_eq!(report.from, application.root().address());
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            work
        }
    }) else {
        panic!("a missing host returns both exact owned inputs before startup");
    };
    assert!(host_error.is_missing_context());
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 0);
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("enter the actual native host only after recovering the owned inputs");
    let (work, native, notifications, ()) = host.block_on(async move {
        application
            .run_with(work)
            .await
            .unwrap_or_else(|_| panic!("the original application retries under the entered host"))
            .into_parts()
    });
    let (work, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original callable runs after the real readiness grant"));
    assert_eq!(work.as_ptr(), work_pointer);
    assert_eq!(&*work, &[137, 139]);
    cleanup.expect("native cleanup remains independent of work");
    assert_eq!(observed_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(observed_invocations.load(Ordering::SeqCst), 1);
    let notifications = notifications.expect("independent native notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (origin, retirement) = native.expect("the original actor remains independently owned");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody,
        completion,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("retry reaches the actual normal actor stop");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let actor = custody.behavior.base();
    assert_eq!(actor.original.as_ptr(), actor_pointer);
    assert_eq!(&*actor.original, &[127, 131]);
    assert_eq!(actor.ready, expected_ready);
    assert!(actor.file.is_none());
    assert!(matches!(actor.startup, NoticeStartup::Ready));
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert_eq!(custody.retirement_failures.len(), 0);
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
        assert_eq!(settled.sends.inner.ready.len(), 0);
        assert_eq!(settled.creations.len(), 0);
        for file in settled.sends.inner.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("retry acquires the actual original file request");
            };
            inspect_file(file);
            files += 1;
        }
    }
    assert_eq!(files, 1);
}
