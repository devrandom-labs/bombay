use bombay::behavior::{
    Here, InterpretSends, Interpretation, InterpretationProgress, InterpreterRequests,
    ItemSettlement, Never, SendSettlements, SettledItem,
};
use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};
use std::env::VarError;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Notify, oneshot};

#[path = "application_services/requests.rs"]
mod requests;

use requests::{FileAccess, OpenFile, ReadEnvironment, Services};

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one owned action lane proves suspension, producer disposal, exact pending custody, resumption and receipt-preserving replay together"
)]
async fn suspended_file_keeps_accepted_prefix_original_request_and_unattempted_suffix() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let expected_path = path.clone();
    let waiting_path = path.clone();
    let waiting_allocation = waiting_path.as_os_str().as_encoded_bytes().as_ptr();
    let rejected_path = path.join("missing");
    let rejected_allocation = rejected_path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_rejected_path = rejected_path.clone();
    let entered = Arc::new(Notify::new());
    let (release, permission) = oneshot::channel();
    let mut progress = Some(InterpretationProgress::Original(InterpreterRequests::new(
        vec![
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
                path: rejected_path,
                access: FileAccess::Ready,
            },
        ],
    )));
    let mut services = Services;
    {
        let mut operation = pin!(<InterpreterRequests<OpenFile> as InterpretSends<
            Services,
            Never,
            Here,
        >>::interpret(&mut progress, &mut services,));
        let mut context = Context::from_waker(Waker::noop());
        let pending = operation.as_mut().poll(&mut context);
        assert!(matches!(pending, Poll::Pending));
    }
    let Some(InterpretationProgress::Interpreting(rows)) = &progress else {
        panic!("disposing the suspended producer preserves its caller-owned progress");
    };
    assert_eq!(rows.len(), 3);
    let Some(InterpretationProgress::Completed(Interpretation::Complete(
        ItemSettlement::Accepted(file),
    ))) = &rows[0]
    else {
        panic!("the prefix file is already acquired");
    };
    let mut file = file;
    let mut prefix = [0_u8];
    file.read_exact(&mut prefix)
        .expect("advance the original acquired file before resuming");
    assert_eq!(prefix, [include_bytes!("../Cargo.toml")[0]]);
    let Some(InterpretationProgress::Interpreting((Some(waiting), None))) = &rows[1] else {
        panic!("the exact request remains owned while permission is pending");
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
        panic!("the pending request retains its actual permission capability");
    };
    assert!(Arc::ptr_eq(&entered, returned));
    let Some(InterpretationProgress::Original(suffix)) = &rows[2] else {
        panic!("the later request cannot be attempted before the pending request finishes");
    };
    assert_eq!(
        suffix.path.as_os_str().as_encoded_bytes().as_ptr(),
        rejected_allocation
    );
    assert_eq!(suffix.path, expected_rejected_path);
    assert!(matches!(suffix.access, FileAccess::Ready));
    let notified = entered.notified();
    let mut notified = pin!(notified);
    let mut context = Context::from_waker(Waker::noop());
    let started = notified.as_mut().poll(&mut context);
    assert!(matches!(started, Poll::Ready(())));
    let released = release.send(());
    released.expect("the original request still owns its permission receiver");
    <InterpreterRequests<OpenFile> as InterpretSends<Services, Never, Here>>::interpret(
        &mut progress,
        &mut services,
    )
    .await;
    <InterpreterRequests<OpenFile> as SendSettlements>::finish_interpretation(&mut progress);
    let Some(InterpretationProgress::Completed(Interpretation::Complete(items))) = &mut progress
    else {
        panic!("resumption completes the original file lane");
    };
    let SettledItem::Attempted(ItemSettlement::Accepted(file)) = &mut items[1] else {
        panic!("the late file was actually acquired");
    };
    let mut prefix = [0_u8];
    file.read_exact(&mut prefix)
        .expect("advance the late file before completed replay");
    assert_eq!(prefix, [include_bytes!("../Cargo.toml")[0]]);
    // Both advanced file offsets must survive the completed replay unchanged.
    <InterpreterRequests<OpenFile> as InterpretSends<Services, Never, Here>>::interpret(
        &mut progress,
        &mut services,
    )
    .await;
    let Some(InterpretationProgress::Completed(Interpretation::Complete(items))) = progress else {
        panic!("the resumed loan returns the complete original product");
    };
    assert_eq!(items.len(), 3);
    let mut items = items.into_iter();
    for _ in 0..2 {
        let Some(SettledItem::Attempted(ItemSettlement::Accepted(mut file))) = items.next() else {
            panic!("both the prefix and late file remain acquired");
        };
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("the actual retained file remains usable");
        assert_eq!(contents, &include_str!("../Cargo.toml")[1..]);
    }
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) = items.next()
    else {
        panic!("the later request is attempted after the late completion");
    };
    assert_eq!(
        item.path.as_os_str().as_encoded_bytes().as_ptr(),
        rejected_allocation
    );
    assert_eq!(item.path, expected_rejected_path);
    assert!(matches!(item.access, FileAccess::Ready));
    assert_eq!(reason.kind(), io::ErrorKind::NotADirectory);
}

#[tokio::test(flavor = "current_thread")]
async fn environment_receipts_and_original_rejections_share_one_complete_lane() {
    let missing = String::from("=");
    let allocation = missing.as_ptr();
    let mut progress = Some(InterpretationProgress::Original(InterpreterRequests::new(
        vec![
            ReadEnvironment {
                name: String::from("CARGO_MANIFEST_DIR"),
            },
            ReadEnvironment { name: missing },
        ],
    )));
    let mut services = Services;
    <InterpreterRequests<ReadEnvironment> as InterpretSends<Services, Never, Here>>::interpret(
        &mut progress,
        &mut services,
    )
    .await;
    <InterpreterRequests<ReadEnvironment> as SendSettlements>::finish_interpretation(&mut progress);
    let Some(InterpretationProgress::Completed(Interpretation::Complete(items))) = progress else {
        panic!("both environment settlements remain in their original product");
    };
    assert_eq!(items.len(), 2);
    let mut items = items.into_iter();
    let Some(SettledItem::Attempted(ItemSettlement::Accepted(value))) = items.next() else {
        panic!("the actual environment value is acquired");
    };
    assert_eq!(value, env!("CARGO_MANIFEST_DIR"));
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) = items.next()
    else {
        panic!("the complete original missing variable is returned");
    };
    assert_eq!(item.name.as_ptr(), allocation);
    assert_eq!(item.name, "=");
    assert_eq!(reason, VarError::NotPresent);
}

#[tokio::test(flavor = "current_thread")]
async fn closed_permission_rejects_the_complete_original_file_request() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let allocation = path.as_os_str().as_encoded_bytes().as_ptr();
    let expected_path = path.clone();
    let entered = Arc::new(Notify::new());
    let (release, permission) = oneshot::channel();
    let mut progress = Some(InterpretationProgress::Original(InterpreterRequests::one(
        OpenFile {
            path,
            access: FileAccess::Waiting {
                entered: Arc::clone(&entered),
                permission,
            },
        },
    )));
    drop(release);
    let mut services = Services;
    <InterpreterRequests<OpenFile> as InterpretSends<Services, Never, Here>>::interpret(
        &mut progress,
        &mut services,
    )
    .await;
    <InterpreterRequests<OpenFile> as SendSettlements>::finish_interpretation(&mut progress);
    let Some(InterpretationProgress::Completed(Interpretation::Complete(items))) = progress else {
        panic!("closed permission is an ordinary complete rejection");
    };
    assert_eq!(items.len(), 1);
    let Some(SettledItem::Attempted(ItemSettlement::Rejected { item, reason })) =
        items.into_iter().next()
    else {
        panic!("failure cannot become a file receipt or discard its original request");
    };
    assert_eq!(
        item.path.as_os_str().as_encoded_bytes().as_ptr(),
        allocation
    );
    assert_eq!(item.path, expected_path);
    assert_eq!(reason.kind(), io::ErrorKind::Other);
    let FileAccess::Waiting {
        entered: returned,
        permission: _,
    } = item.access
    else {
        panic!("rejection preserves the request's permission capability");
    };
    assert!(Arc::ptr_eq(&entered, &returned));
}
