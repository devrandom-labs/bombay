//! Child constructor refusal keeps pure initialization and independent native causes.
use super::{
    Config, Document, DocumentChildrenNext, DocumentReady, FileAccess, Files, ManagedDocument,
    Root, RootChildrenDocument, file_request, inspect_file,
};
use bombay::actors::ActorExt;
use bombay::behavior::{
    BehaviorBase, ChildCreationOutcome, ClassifySettlement, CreationKind, CreationRejection,
    CreationSettlement, ItemSettlement, NoSends, SettledItem, SettlementStatus, Step,
};
use bombay::prelude::Exit;
use bombay::{
    ActorFailureAssessment, ActorRetirement, ApplicationScope, ChildFailure, Completion,
    InterpreterFactory, MailAddr, RetirementAssessment,
};
use bombay_engine::DriverError;
use std::collections::{BTreeMap, VecDeque};
use std::panic::resume_unwind;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

struct DocumentStartup {
    files: Config,
    attempts: Arc<AtomicUsize>,
    constructors: Mutex<VecDeque<Result<(), Box<[u8; 2]>>>>,
}

impl InterpreterFactory for DocumentStartup {
    type Interpreter = Files;

    fn create(&self) -> Files {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let constructor = self
            .constructors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .expect("only the explicit root and refused child may request construction");
        match constructor {
            Ok(()) => self.files.create(),
            Err(cause) => resume_unwind(cause),
        }
    }
}

bombay::topology! {
    library {
        interpreter: DocumentStartup;
        root: Root { Document(RootChildrenDocument) => document; }
        document: ManagedDocument { Next(DocumentChildrenNext) => document; }
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one refused native child preserves its original actions, unopened grandchild and independent failure lanes"
)]
async fn child_constructor_panic_keeps_original_creation_actions_and_independent_root_effects() {
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the original early reply recipient");
    let ready = replies.recipient();
    let root_bytes = Box::<[u8]>::from([157]);
    let child_bytes = Box::<[u8]>::from([163]);
    let grandchild_bytes = Box::<[u8]>::from([167]);
    let root_pointer = root_bytes.as_ptr();
    let child_pointer = child_bytes.as_ptr();
    let grandchild_pointer = grandchild_bytes.as_ptr();
    let child_file = file_request();
    let child_path_pointer = child_file.path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_child_path = child_file.path.clone();
    let grandchild_file = file_request();
    let grandchild_path_pointer = grandchild_file.path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_grandchild_path = grandchild_file.path.clone();
    let cause = Box::new([173, 179]);
    let cause_pointer = ptr::from_ref(cause.as_ref()).cast::<()>();
    let attempts = Arc::new(AtomicUsize::new(0));
    let constructions = Arc::new(AtomicUsize::new(0));
    let visits = Arc::new(Mutex::new(BTreeMap::new()));
    let application = scope.with_topology(
        Root {
            original: root_bytes,
            file: Some(file_request()),
            document: Some(
                Document {
                    original: child_bytes,
                    file: Some(child_file),
                    next: Some(Box::new(Document {
                        original: grandchild_bytes,
                        file: Some(grandchild_file),
                        next: None,
                        ready: ready.clone(),
                    })),
                    ready: ready.clone(),
                }
                .stop_on_shutdown(),
            ),
            ready: ready.clone(),
        }
        .stop_on_shutdown(),
        library::Topology::new(DocumentStartup {
            files: Config {
                label: Box::from(*b"documents"),
                constructions: Arc::clone(&constructions),
                visits: Arc::clone(&visits),
            },
            attempts: Arc::clone(&attempts),
            constructors: Mutex::new(VecDeque::from([Ok(()), Err(cause)])),
        }),
    );
    let (work, native, notifications, ()) = application
        .run_with(|application| async move {
            let report = tokio::time::timeout(Duration::from_secs(5), replies.receive())
                .await
                .expect("root IO and readiness remain independent of its child's refused startup")
                .expect("the root's real readiness request is delivered");
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            report
        })
        .await
        .unwrap_or_else(|_| panic!("the native host enters the original application"))
        .into_parts();
    let (report, cleanup) = work.try_into_completed().unwrap_or_else(|_| {
        panic!("the creator receives the handled creation outcome and can stop normally")
    });
    cleanup.expect("the native owner completes all independent retirement");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(constructions.load(Ordering::SeqCst), 1);
    assert_eq!(
        *visits.lock().unwrap_or_else(PoisonError::into_inner),
        BTreeMap::from([(0, 1)])
    );
    let notifications = notifications.expect("independent root notifications");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (origin, retirement) = native.expect("the original root is independently received");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(report.message, 157);
    assert_eq!(report.from, origin.address());
    let ActorRetirement::Completed {
        custody,
        completion,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!(
            "the creator retains the unsuccessful creation outcome and handles its own shutdown"
        );
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    let root = custody.behavior.base();
    assert_eq!(root.original.as_ptr(), root_pointer);
    assert_eq!(&*root.original, &[157]);
    assert_eq!(root.ready, ready);
    assert!(root.file.is_none());
    assert!(root.document.is_none());
    assert_eq!(custody.control.len(), 0);
    assert_eq!(custody.user.len(), 0);
    assert_eq!(custody.descendants.len(), 0);
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
    let mut rows = settlements.into_iter();
    let stopped = rows
        .next()
        .expect("the root's final shutdown row remains complete");
    let row = rows
        .next()
        .expect("the complete original initialization row");
    assert!(rows.next().is_none());
    assert!(matches!(stopped.become_, Step::Stop(_)));
    assert_eq!(stopped.sends.owned, NoSends);
    assert_eq!(stopped.sends.inner.files.len(), 0);
    assert_eq!(stopped.sends.inner.ready.len(), 0);
    let CreationSettlement::Settled(stopped_children) = stopped.creations.into_settlement() else {
        panic!("the complete empty shutdown creation lane remains settled");
    };
    assert_eq!(stopped_children.len(), 0);
    assert!(matches!(row.become_, Step::Continue));
    assert_eq!(row.sends.owned, NoSends);
    assert_eq!(row.sends.inner.ready.len(), 0);
    let mut files = row.sends.inner.files.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(file))) = files.next() else {
        panic!("root IO is independently attempted after the refused child creation");
    };
    assert!(files.next().is_none());
    inspect_file(file);
    let CreationSettlement::Settled(children) = row.creations.into_settlement() else {
        panic!("the original creation has its complete typed settlement");
    };
    let mut children = children.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(
        ChildCreationOutcome::HostRejected {
            creation,
            initialization,
            reason,
        },
    ))) = children.next()
    else {
        panic!("the child did not cross binding ACK and never acquired an established receipt");
    };
    assert!(children.next().is_none());
    assert_eq!(reason, CreationRejection::EnvironmentFailed);
    let (creation, route) = creation.into_parts();
    let (id, child, kind) = creation.into_parts();
    assert_eq!(kind, CreationKind::Birth);
    let child = child.base();
    assert_eq!(child.original.as_ptr(), child_pointer);
    assert_eq!(&*child.original, &[163]);
    assert_eq!(child.ready, ready);
    assert!(child.file.is_none());
    assert!(child.next.is_none());
    assert!(matches!(initialization.become_, Step::Continue));
    assert_eq!(initialization.sends.owned, NoSends);
    let mut files = initialization.sends.inner.files.into_iter();
    let child_file = files
        .next()
        .expect("the original child request was never interpreted");
    assert!(files.next().is_none());
    assert_eq!(child_file.path, expected_child_path);
    assert_eq!(
        child_file.path.as_os_str().as_encoded_bytes().as_ptr(),
        child_path_pointer
    );
    assert!(matches!(child_file.access, FileAccess::Ready));
    let mut notices = initialization.sends.inner.ready.into_iter();
    let notice = notices.next().expect("the child's untouched ready request");
    assert!(notices.next().is_none());
    assert_eq!(notice.to, ready);
    assert_eq!(notice.message, 163);
    let mut unborn = initialization.creates.into_iter();
    let grandchild = unborn
        .next()
        .expect("the pure recursive creation stays original");
    assert!(unborn.next().is_none());
    let (_, grandchild, grandchild_kind) = grandchild.into_parts();
    assert_eq!(grandchild_kind, CreationKind::Birth);
    let grandchild = grandchild.base();
    assert_eq!(grandchild.original.as_ptr(), grandchild_pointer);
    assert_eq!(&*grandchild.original, &[167]);
    assert_eq!(grandchild.ready, ready);
    assert!(grandchild.next.is_none());
    let grandchild_file = grandchild
        .file
        .as_ref()
        .expect("the unopened grandchild request remains with its actor");
    assert_eq!(grandchild_file.path, expected_grandchild_path);
    assert_eq!(
        grandchild_file.path.as_os_str().as_encoded_bytes().as_ptr(),
        grandchild_path_pointer
    );
    assert!(matches!(grandchild_file.access, FileAccess::Ready));
    let mut failures = custody.child_failures.into_iter();
    let Some(ChildFailure::StartupRetirementFailed {
        origin,
        primary_failure,
        additional_failures,
        terminal_report,
        retirement_failures,
        termination_notification,
        retirement_report,
    }) = failures.next()
    else {
        panic!("the independent original constructor cause survives child retirement");
    };
    assert!(failures.next().is_none());
    assert_eq!(origin.creation_id(), id);
    assert_eq!(origin.creation_kind(), kind);
    assert_eq!(origin.nonce(), route);
    let Some(DriverError::ActivationPanicked(payload)) = primary_failure else {
        panic!("the original constructor panic keeps its native phase and cause");
    };
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), cause_pointer);
    assert_eq!(additional_failures.len(), 0);
    assert!(terminal_report.is_none());
    assert_eq!(retirement_failures.len(), 0);
    termination_notification.expect("the actual child termination publication");
    assert_eq!(
        retirement_report.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        retirement_report.failures(),
        ActorFailureAssessment::FailuresFound
    );
}
