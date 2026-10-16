//! Independent interpreter owners must not unwind through one another.
use bombay::behavior::{
    ActionItem, Actions, BehaviorActed, InterpretItem, InterpreterRequests, ItemSettlement, Never,
    SettledItem, Step,
};
use bombay::{ActorRetirement, Application, InterpreterFactory};
use core::future::Future;
use core::ptr;
use std::env;
use std::io::Read;
use std::panic::resume_unwind;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, PoisonError};

#[path = "application_services/requests.rs"]
mod requests;
use requests::{FileAccess, OpenFile, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = Files;
}

struct Config {
    cause: Option<Box<[u8; 2]>>,
    service_cause: Mutex<Option<Box<[u8; 2]>>>,
}

impl InterpreterFactory for Config {
    type Interpreter = Files;
    fn create(&self) -> Files {
        let cause = self
            .service_cause
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Files {
            cause,
            service: Services,
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if let Some(cause) = self.cause.take() {
            resume_unwind(cause);
        }
    }
}

struct Files {
    cause: Option<Box<[u8; 2]>>,
    service: Services,
}

impl<Event, Path> InterpretItem<OpenFile, Event, Path> for Files {
    fn interpret_item<'a>(
        &'a mut self,
        input: <OpenFile as ActionItem>::Input<'a>,
        received: &'a mut Option<<OpenFile as ActionItem>::Reply>,
    ) -> impl Future<Output = ()> + Send + 'a
    where
        OpenFile: 'a,
    {
        <Services as InterpretItem<OpenFile, Event, Path>>::interpret_item(
            &mut self.service,
            input,
            received,
        )
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        if let Some(cause) = self.cause.take() {
            resume_unwind(cause);
        }
    }
}

struct Document {
    file: Option<OpenFile>,
    original: Box<[u8; 2]>,
}

#[bombay::actor(message = Never, sends = { files: InterpreterRequests<OpenFile> })]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor contract returns typed Actions through its success or error carrier"
)]
impl Document {
    fn init(&mut self) -> BehaviorActed<Self> {
        let file = self.file.take().expect("the original real file request");
        Ok(Actions::stop().with_send(file))
    }
}

#[test]
fn independent_service_and_constructor_destructors_preserve_both_causes() {
    let executable = env::current_exe().expect("the actual native test executable");
    let child = Command::new(executable)
        .args([
            "--exact",
            "service_and_constructor_retirement_child",
            "--ignored",
            "--nocapture",
        ])
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("the isolated native retirement witness starts");
    assert!(
        child.status.success(),
        "independent retirement owners must not abort or lose causes: status={}\nstdout={}\nstderr={}",
        child.status,
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
    let stdout = String::from_utf8(child.stdout).expect("native test output is UTF-8");
    assert_eq!(
        stdout
            .lines()
            .filter(|line| *line == "test service_and_constructor_retirement_child ... ok")
            .count(),
        1,
        "the named ignored child must actually execute exactly once: {stdout}"
    );
    assert!(
        stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"),
        "a successful process with zero selected tests is not evidence: {stdout}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "invoked by the parent so a double-panic defect cannot abort the test harness"]
async fn service_and_constructor_retirement_child() {
    let constructor_cause = Box::new([67, 71]);
    let service_cause = Box::new([73, 79]);
    let constructor_pointer = ptr::from_ref(constructor_cause.as_ref()).cast::<()>();
    let service_pointer = ptr::from_ref(service_cause.as_ref()).cast::<()>();
    let original = Box::new([83, 89]);
    let actor_pointer = original.as_ptr();
    let (work, native, notifications, ()) = Application::new(Document {
        file: Some(OpenFile {
            path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
            access: FileAccess::Ready,
        }),
        original,
    })
    .with_interpreter(Config {
        cause: Some(constructor_cause),
        service_cause: Mutex::new(Some(service_cause)),
    })
    .run()
    .await
    .unwrap_or_else(|_| panic!("the native host is entered"))
    .into_parts();
    let bombay::WorkOutcome::NotInvoked {
        work: None,
        startup_error: Some(_),
        cleanup,
    } = work
    else {
        panic!("an actor stopped during initialization cannot publish application readiness");
    };
    cleanup.expect("both native drop causes coexist with joined cleanup");
    let notifications = notifications.expect("independent actual notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the original actor is independently received");
    let ActorRetirement::Completed {
        custody,
        completion,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("disposal failures do not rewrite the actual completed actor transition");
    };
    assert_eq!(completion, bombay::Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.behavior.original.as_ptr(), actor_pointer);
    assert_eq!(&*custody.behavior.original, &[83, 89]);
    assert!(custody.behavior.file.is_none());
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
    assert_eq!(custody.child_failures.len(), 0);
    assert_eq!(custody.capability_failures.len(), 0);
    assert_eq!(custody.additional_failures.len(), 0);
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(custody.retirement_failures.len(), 2);
    let pointers = custody
        .retirement_failures
        .iter()
        .map(|cause| ptr::from_ref(cause.as_ref()).cast::<()>())
        .collect::<Vec<_>>();
    assert_eq!(pointers, [service_pointer, constructor_pointer]);
    let mut settlements = settlements.into_iter();
    let settled = settlements
        .next()
        .expect("the actual initialized file receipt");
    assert!(settlements.next().is_none());
    assert_eq!(settled.creations.len(), 0);
    assert!(matches!(settled.become_, Step::Stop(_)));
    let mut files = settled.sends.files.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(mut file))) = files.next() else {
        panic!("the acquired original file survives both native disposal panics");
    };
    assert!(files.next().is_none());
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the retained actual file is still usable");
    assert_eq!(contents, include_str!("../Cargo.toml"));
}
