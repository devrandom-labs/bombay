use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, Interpretation, InterpretationProgress,
    InterpreterRequests, ItemSettlement, NoSends, SettledItem, Step,
};
use bombay::prelude::TimerId;
use bombay::{ActorNotificationReceipts, ActorRetirement, Application, MailAddr};
use core::time::Duration;
use std::env::VarError;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

#[path = "application_services/requests.rs"]
mod requests;

use requests::{FileAccess, OpenFile, ReadEnvironment, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = Services;
}

impl bombay::ExternalRequest for ReadEnvironment {
    type Interpreter = Services;
}

struct Documents {
    files: Vec<OpenFile>,
    settings: Vec<ReadEnvironment>,
}

struct DocumentRequest {
    files: Vec<OpenFile>,
    settings: Vec<ReadEnvironment>,
}

#[bombay::actor(sends = {
    files: InterpreterRequests<OpenFile>,
    settings: InterpreterRequests<ReadEnvironment>,
})]
impl Documents {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut actions = Actions::cont();
        for request in self.files.drain(..self.files.len().min(1)) {
            actions = actions.send_files(request);
        }
        for request in self.settings.drain(..self.settings.len().min(1)) {
            actions = actions.send_settings(request);
        }
        Ok(actions)
    }

    fn receive(&mut self, _: MailAddr, request: DocumentRequest) -> BehaviorActed<Self> {
        let mut actions = Actions::cont();
        for file in request.files {
            actions = actions.send_files(file);
        }
        for setting in request.settings {
            actions = actions.send_settings(setting);
        }
        Ok(actions)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn file_and_environment_services_compose_beside_native_timers() {
    let rejected_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml/missing");
    let rejected_path_allocation = rejected_path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_rejected_path = rejected_path.clone();
    let rejected_name = String::from("=");
    let rejected_name_allocation = rejected_name.as_ptr();
    let documents = Documents {
        files: vec![
            OpenFile {
                path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
                access: FileAccess::Ready,
            },
            OpenFile {
                path: rejected_path,
                access: FileAccess::Ready,
            },
        ],
        settings: vec![
            ReadEnvironment {
                name: String::from("CARGO_MANIFEST_DIR"),
            },
            ReadEnvironment {
                name: rejected_name,
            },
        ],
    };
    let root = documents
        .with_one_shot(TimerId(1), Duration::ZERO, |documents| {
            let mut actions = Actions::stop();
            for request in documents.files.drain(..) {
                actions = actions.with_send(request);
            }
            for request in documents.settings.drain(..) {
                actions = actions.with_send(request);
            }
            actions
        })
        .stop_on_shutdown();
    let (work, native, notifications, ()) = Application::new(root)
        .with_interpreter(|| Services)
        .run_with(|application| async move { application.termination().await })
        .await
        .unwrap_or_else(|_| panic!("the native host is entered"))
        .into_parts();
    let (termination, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("both initialization lanes finish before the real timer"));
    assert_eq!(termination, Ok(bombay::prelude::Exit::Normal));
    cleanup.expect("application cleanup joins the original actor");
    let ActorNotificationReceipts {
        termination,
        retirement,
    } = notifications.expect("both independent notification results remain available");
    termination.expect("termination publication succeeds");
    retirement.expect("retirement publication succeeds");
    let (_, retirement) = native.expect("the complete native retirement remains available");
    let ActorRetirement::Completed {
        custody,
        settlements,
        completion,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the native timer stops the actor after both lanes execute");
    };
    assert_eq!(completion, bombay::prelude::Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.behavior.base().files.is_empty());
    assert!(custody.behavior.base().settings.is_empty());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.descendants.is_empty());
    assert!(custody.child_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(settlements.len(), 2);
    let mut settlements = settlements.into_iter();
    let stopped = settlements
        .next()
        .expect("the terminal turn precedes older retained turns");
    assert!(matches!(stopped.become_, Step::Stop(_)));
    assert!(stopped.creations.is_empty());
    assert_eq!(stopped.sends.owned, NoSends);
    assert!(stopped.sends.inner.owned.is_empty());
    let mut files = stopped.sends.inner.inner.files.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) = files.next()
    else {
        panic!("the original rejected path and OS cause survive the stopping turn");
    };
    assert!(files.next().is_none());
    assert_eq!(
        item.path.as_os_str().as_encoded_bytes().as_ptr(),
        rejected_path_allocation
    );
    assert_eq!(item.path, expected_rejected_path);
    assert!(matches!(item.access, FileAccess::Ready));
    assert_eq!(reason.kind(), io::ErrorKind::NotADirectory);
    let mut settings = stopped.sends.inner.inner.settings.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) = settings.next()
    else {
        panic!("file rejection cannot suppress the independent environment lane");
    };
    assert!(settings.next().is_none());
    assert_eq!(item.name.as_ptr(), rejected_name_allocation);
    assert_eq!(item.name, "=");
    assert_eq!(reason, VarError::NotPresent);
    let initialized = settlements
        .next()
        .expect("the earlier retained initialization stays complete");
    assert!(matches!(initialized.become_, Step::Continue));
    assert!(initialized.creations.is_empty());
    assert_eq!(initialized.sends.owned, NoSends);
    assert!(initialized.sends.inner.owned.is_empty());
    let mut files = initialized.sends.inner.inner.files.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(mut file))) = files.next() else {
        panic!("the acquired file survives native interpreter retirement");
    };
    assert!(files.next().is_none());
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the actual retained file is still usable");
    assert_eq!(contents, include_str!("../Cargo.toml"));
    let mut settings = initialized.sends.inner.inner.settings.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(value))) = settings.next() else {
        panic!("the environment receipt survives alongside the actual file");
    };
    assert!(settings.next().is_none());
    assert_eq!(value, env!("CARGO_MANIFEST_DIR"));
}

#[tokio::test(flavor = "current_thread")]
async fn native_cancellation_retains_acquired_file_pending_request_and_both_untouched_suffixes() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let expected_path = path.clone();
    let waiting_path = path.clone();
    let waiting_allocation = waiting_path.as_os_str().as_encoded_bytes().as_ptr();
    let suffix_path = path.clone();
    let suffix_allocation = suffix_path.as_os_str().as_encoded_bytes().as_ptr();
    let setting = String::from("CARGO_MANIFEST_DIR");
    let setting_allocation = setting.as_ptr();
    let entered = Arc::new(Notify::new());
    let expected_entered = Arc::clone(&entered);
    let (release, permission) = oneshot::channel();
    let request = DocumentRequest {
        files: vec![
            OpenFile {
                path,
                access: FileAccess::Ready,
            },
            OpenFile {
                path: waiting_path,
                access: FileAccess::Waiting {
                    entered: Arc::clone(&entered),
                    permission,
                },
            },
            OpenFile {
                path: suffix_path,
                access: FileAccess::Ready,
            },
        ],
        settings: vec![ReadEnvironment { name: setting }],
    };
    let (work, native, notifications, ()) = Application::new(Documents {
        files: vec![],
        settings: vec![],
    })
    .with_interpreter(|| Services)
    .run_with(|application| async move {
        let sender = application
            .sender()
            .expect("the existing native delivery owner establishes a sender");
        sender
            .send(request)
            .await
            .unwrap_or_else(|_| panic!("the exact concrete command is admitted"));
        entered.notified().await;
        // Returning application work requests existing owner cancellation.
        // The file permission stays held outside the whole Application run.
    })
    .await
    .unwrap_or_else(|_| panic!("the native host is entered"))
    .into_parts();
    let ((), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work returns after the interpreter really suspends"));
    cleanup.expect("the existing owner cancellation joins complete retirement");
    let notifications = notifications.expect("both original notification results remain available");
    notifications
        .termination
        .expect("termination publication succeeds");
    notifications
        .retirement
        .expect("retirement publication succeeds");
    let (_, retirement) = native.expect("the cancelled actor is independently acquired");
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("cancellation cannot become normal completion or an effects rejection");
    };
    assert!(settlements.is_empty());
    assert!(source.is_none());
    assert!(custody.behavior.files.is_empty());
    assert!(custody.behavior.settings.is_empty());
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.descendants.is_empty());
    assert!(custody.child_failures.is_empty());
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.retirement_failures.is_empty());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    let Some(InterpretationProgress::Interpreting(((creations, sends), become_))) = interpretation
    else {
        panic!("the exact pending action remains in interpretation custody");
    };
    assert!(matches!(become_, Step::Continue));
    let Some(InterpretationProgress::Completed(Interpretation::Complete(creations))) = creations
    else {
        panic!("the original empty creation lane completed before either send lane");
    };
    assert!(creations.is_empty());
    let Some(InterpretationProgress::Interpreting((files, settings))) = sends else {
        panic!("both named lanes remain in the original product");
    };
    let Some(InterpretationProgress::Original(settings)) = settings else {
        panic!("the later independent lane is untouched");
    };
    assert_eq!(settings.len(), 1);
    assert_eq!(settings[0].name.as_ptr(), setting_allocation);
    assert_eq!(settings[0].name, "CARGO_MANIFEST_DIR");
    let Some(InterpretationProgress::Interpreting(files)) = files else {
        panic!("the file lane retains its accepted prefix and pending suffix");
    };
    assert_eq!(files.len(), 3);
    let mut files = files.into_iter();
    let Some(Some(InterpretationProgress::Completed(Interpretation::Complete(
        ItemSettlement::Accepted(mut file),
    )))) = files.next()
    else {
        panic!("the acquired file survives the native capability's disposal");
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the retained original file is usable");
    assert_eq!(contents, include_str!("../Cargo.toml"));
    let Some(Some(InterpretationProgress::Interpreting((Some(waiting), None)))) = files.next()
    else {
        panic!("the pending request has not been consumed or reconstructed");
    };
    assert_eq!(
        waiting.path.as_os_str().as_encoded_bytes().as_ptr(),
        waiting_allocation
    );
    assert_eq!(waiting.path, expected_path);
    let FileAccess::Waiting {
        entered: returned,
        permission: _,
    } = &waiting.access
    else {
        panic!("the retired pending request keeps its permission capability");
    };
    assert!(Arc::ptr_eq(&expected_entered, returned));
    let Some(Some(InterpretationProgress::Original(suffix))) = files.next() else {
        panic!("the later file remains unattempted");
    };
    assert_eq!(
        suffix.path.as_os_str().as_encoded_bytes().as_ptr(),
        suffix_allocation
    );
    assert_eq!(suffix.path, expected_path);
    assert!(matches!(suffix.access, FileAccess::Ready));
    let released = release.send(());
    released.expect("the retired request still owns its original permission receiver");
}
