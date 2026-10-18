use bombay::actors::ActorExt;
use bombay::behavior::{
    Actions, BehaviorActed, BehaviorBase, Interpretation, InterpretationProgress,
    InterpreterRequests, ItemSettlement, NoSends, SettledItem, Step,
};
use bombay::{ActorNotificationReceipts, ActorRetirement, Application, MailAddr};
use core::future::pending;
use core::pin::pin;
use core::ptr;
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
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor contract returns typed Actions through its success or error carrier"
)]
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
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
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
        .with_one_shot(Duration::ZERO, |documents| {
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
    let (work, _root_report, native, notifications, ()) = Application::new(root)
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
    assert_eq!(custody.behavior.base().files.len(), 0);
    assert_eq!(custody.behavior.base().settings.len(), 0);
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
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(settlements.len(), 2);
    let mut settlements = settlements.into_iter();
    let stopped = settlements
        .next()
        .expect("the terminal turn precedes older retained turns");
    assert!(matches!(stopped.become_, Step::Stop(_)));
    assert_eq!(stopped.creations.len(), 0);
    assert_eq!(stopped.sends.owned, NoSends);
    assert_eq!(stopped.sends.inner.owned.len(), 0);
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
    assert_eq!(initialized.creations.len(), 0);
    assert_eq!(initialized.sends.owned, NoSends);
    assert_eq!(initialized.sends.inner.owned.len(), 0);
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
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
async fn native_interpreter_panic_retains_acquired_file_pending_request_and_both_untouched_suffixes()
 {
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
    let cause = Box::new([59, 61]);
    let cause_pointer = ptr::from_ref(cause.as_ref()).cast::<()>();
    let request = DocumentRequest {
        files: vec![
            OpenFile {
                path,
                access: FileAccess::Ready,
            },
            OpenFile {
                path: waiting_path,
                access: FileAccess::Panicking {
                    entered: Arc::clone(&entered),
                    cause: permission,
                },
            },
            OpenFile {
                path: suffix_path,
                access: FileAccess::Ready,
            },
        ],
        settings: vec![ReadEnvironment { name: setting }],
    };
    let (work, _root_report, native, notifications, ()) = Application::new(Documents {
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
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("the real pending file operation is entered");
        let released = release.send(cause);
        released.expect("the original pending provider receives the actual cause");
        let termination = tokio::time::timeout(Duration::from_secs(5), application.termination())
            .await
            .expect("the guarded interpreter panic terminates the actor");
        assert_eq!(termination, Err(bombay::prelude::Crash::Panicked));
    })
    .await
    .unwrap_or_else(|_| panic!("the native host is entered"))
    .into_parts();
    let ((), cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("application work observes the actual host panic"));
    cleanup.expect("native host panic joins complete retirement");
    let notifications = notifications.expect("both original notification results remain available");
    notifications
        .termination
        .expect("termination publication succeeds");
    notifications
        .retirement
        .expect("retirement publication succeeds");
    let (_, retirement) = native.expect("the failed actor is independently acquired");
    let ActorRetirement::HostExecutionPanicked {
        payload,
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the guarded interpreter panic must preserve its complete partial action");
    };
    assert_eq!(ptr::from_ref(payload.as_ref()).cast::<()>(), cause_pointer);
    assert_eq!(settlements.len(), 0);
    assert!(source.is_none());
    assert_eq!(custody.behavior.files.len(), 0);
    assert_eq!(custody.behavior.settings.len(), 0);
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
    assert_eq!(custody.retirement_failures.len(), 0);
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
    assert_eq!(creations.len(), 0);
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
    let FileAccess::Panicking {
        entered: returned,
        cause: _,
    } = &waiting.access
    else {
        panic!("the failed pending request keeps its exact provider state");
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
    assert!(files.next().is_none());
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
async fn owner_cancellation_waits_for_the_inflight_effect_and_keeps_its_complete_settlement() {
    let entered = Arc::new(Notify::new());
    let (release, permission) = oneshot::channel();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let request = DocumentRequest {
        files: vec![
            OpenFile {
                path: path.clone(),
                access: FileAccess::Ready,
            },
            OpenFile {
                path: path.clone(),
                access: FileAccess::Waiting {
                    entered: Arc::clone(&entered),
                    permission,
                },
            },
            OpenFile {
                path,
                access: FileAccess::Ready,
            },
        ],
        settings: vec![ReadEnvironment {
            name: String::from("CARGO_MANIFEST_DIR"),
        }],
    };
    let (execution, result) = Application::new(Documents {
        files: vec![],
        settings: vec![],
    })
    .with_interpreter(|| Services)
    .execute_with(|application| async move {
        let sender = application.sender().expect("the ordinary bound sender");
        sender
            .send(request)
            .await
            .unwrap_or_else(|_| panic!("the original batch is admitted"));
        pending::<()>().await;
    })
    .unwrap_or_else(|_| panic!("the native host is entered"));
    {
        let mut execution = pin!(execution);
        tokio::select! {
            () = &mut execution => panic!("the held effect cannot complete"),
            () = entered.notified() => {},
            () = tokio::time::sleep(Duration::from_secs(5)) => panic!("the file provider must enter"),
        }
    }
    let mut result = pin!(result);
    let premature = tokio::time::timeout(Duration::from_millis(100), &mut result).await;
    assert!(
        premature.is_err(),
        "cancellation cannot preempt the in-flight effect"
    );
    assert!(
        !release.is_closed(),
        "the pending original provider remains owned"
    );
    let released = release.send(());
    released.expect("the accepted effect receives its original permission");
    let (work, _root_report, native, notifications, ()) =
        tokio::time::timeout(Duration::from_secs(5), result)
            .await
            .expect("cancellation joins after the in-flight batch settles")
            .into_parts();
    let bombay::WorkOutcome::Interrupted { cleanup } = work else {
        panic!("the original application work is interrupted");
    };
    cleanup.expect("the actual cancellation cleanup joins");
    let notifications = notifications.expect("both independent notifications are retained");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (_, retirement) = native.expect("the joined actor is independently acquired");
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the queued owner request is observed after effect settlement");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(custody.behavior.files.len(), 0);
    assert_eq!(custody.behavior.settings.len(), 0);
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
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    let mut settlements = settlements.into_iter();
    let batch = settlements
        .next()
        .expect("the complete accepted batch precedes cancellation");
    assert!(settlements.next().is_none());
    assert_eq!(batch.creations.len(), 0);
    assert!(matches!(batch.become_, Step::Continue));
    assert_eq!(batch.sends.files.len(), 3);
    for file in batch.sends.files {
        let SettledItem::Attempted(ItemSettlement::Accepted(mut file)) = file else {
            panic!("each original file effect completes before cancellation");
        };
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("the independently retained file is usable");
        assert_eq!(contents, include_str!("../Cargo.toml"));
    }
    let mut settings = batch.sends.settings.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(value))) = settings.next() else {
        panic!("the independent environment lane completes in the same batch");
    };
    assert!(settings.next().is_none());
    assert_eq!(value, env!("CARGO_MANIFEST_DIR"));
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
async fn timer_outside_shutdown_keeps_io_receipts_and_unemitted_requests() {
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
        .stop_on_shutdown()
        .with_one_shot(Duration::ZERO, |_| Actions::stop());
    let (work, _root_report, native, notifications, ()) = Application::new(root)
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
    let documents = custody.behavior.base();
    assert_eq!(documents.files.len(), 1);
    assert_eq!(documents.settings.len(), 1);
    let file = &documents.files[0];
    assert_eq!(
        file.path.as_os_str().as_encoded_bytes().as_ptr(),
        rejected_path_allocation
    );
    assert_eq!(file.path, expected_rejected_path);
    assert!(matches!(file.access, FileAccess::Ready));
    let setting = &documents.settings[0];
    assert_eq!(setting.name.as_ptr(), rejected_name_allocation);
    assert_eq!(setting.name, "=");
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
    assert_eq!(custody.retirement_failures.len(), 0);
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(settlements.len(), 2);
    let mut settlements = settlements.into_iter();
    let stopped = settlements
        .next()
        .expect("the terminal turn precedes older retained turns");
    assert!(matches!(stopped.become_, Step::Stop(_)));
    assert_eq!(stopped.creations.len(), 0);
    assert_eq!(stopped.sends.inner.owned, NoSends);
    assert_eq!(stopped.sends.owned.len(), 0);
    assert_eq!(stopped.sends.inner.inner.files.len(), 0);
    assert_eq!(stopped.sends.inner.inner.settings.len(), 0);
    let initialized = settlements
        .next()
        .expect("the earlier retained initialization stays complete");
    assert!(matches!(initialized.become_, Step::Continue));
    assert_eq!(initialized.creations.len(), 0);
    assert_eq!(initialized.sends.inner.owned, NoSends);
    assert_eq!(initialized.sends.owned.len(), 0);
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
