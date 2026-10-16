//! One-way senders own identities and targets without allocating reply storage.

use bombay::actors::ActorExt;
use bombay::behavior::{ClassifySettlement, MessageProtocol, SettlementStatus};
use bombay::prelude::{ActorRetirement, Application, Completion, MailAddr};
use communication::TrySendError;
use dhat::{HeapStats, Profiler};

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use counter::{Counter, CounterMessage};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

type Documents = MessageProtocol<MailAddr, Box<[u8]>>;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one measured public journey retains sender origins, original commands and complete joined retirement"
)]
async fn one_way_senders_need_no_reply_storage_and_keep_complete_delivery() {
    let _profiler = Profiler::builder().testing().build();
    let first = Box::<[u8]>::from([107, 109]);
    let second = Box::<[u8]>::from([113, 127]);
    let closed = Box::<[u8]>::from([131, 137]);
    let pointers = [first.as_ptr(), second.as_ptr(), closed.as_ptr()];
    let report = Application::new(Counter::new().stop_on_shutdown())
        .run_with(|application| async move {
            let mut peer = application
                .external::<Documents>()
                .expect("document endpoint");
            let interface = application.interface(peer.recipient());
            let before = HeapStats::get();
            let root_sender = application.sender().expect("root sender identity");
            let root = HeapStats::get();
            let peer_sender = application
                .sender_to(peer.recipient())
                .unwrap_or_else(|_| panic!("exact document sender identity"));
            let selected = HeapStats::get();
            let exported_sender = interface
                .sender_to(interface.api().clone())
                .unwrap_or_else(|_| panic!("exported document sender identity"));
            let exported = HeapStats::get();
            let sizes = [
                core::mem::size_of_val(&root_sender),
                core::mem::size_of_val(&peer_sender),
                core::mem::size_of_val(&exported_sender),
            ];
            let allocations = [
                (
                    root.total_blocks - before.total_blocks,
                    root.curr_bytes - before.curr_bytes,
                ),
                (
                    selected.total_blocks - root.total_blocks,
                    selected.curr_bytes - root.curr_bytes,
                ),
                (
                    exported.total_blocks - selected.total_blocks,
                    exported.curr_bytes - selected.curr_bytes,
                ),
            ];
            let root_address = application.root().address();
            assert_ne!(root_sender.address(), root_address);
            assert_ne!(peer_sender.address(), root_address);
            assert_ne!(exported_sender.address(), root_address);
            assert_ne!(root_sender.address(), peer.address());
            assert_ne!(peer_sender.address(), peer.address());
            assert_ne!(exported_sender.address(), peer.address());
            assert_ne!(root_sender.address(), peer_sender.address());
            assert_ne!(root_sender.address(), exported_sender.address());
            assert_ne!(peer_sender.address(), exported_sender.address());
            root_sender
                .send(CounterMessage::Increment)
                .await
                .unwrap_or_else(|_| panic!("increment admitted"));
            drop(root_sender);
            exported_sender
                .try_send(second)
                .expect("second document admitted");
            let exported_address = exported_sender.address();
            drop(exported_sender);
            peer_sender
                .send(first)
                .await
                .expect("the target still admits after another sender is dropped");
            let second = peer
                .receive()
                .await
                .expect("exported document received first");
            let first = peer
                .receive()
                .await
                .expect("selected document received next");
            assert_eq!(second.from, exported_address);
            assert_eq!(first.from, peer_sender.address());
            peer.close_admission();
            let Err(TrySendError::Closed(closed)) = peer_sender.try_send(closed) else {
                panic!("closed admission returns the original document");
            };
            let exhausted = peer.receive().await;
            assert!(exhausted.is_none());
            let mut client = application.client().expect("counter reply client");
            client
                .send(CounterMessage::Read(client.recipient()))
                .await
                .unwrap_or_else(|_| panic!("read admitted"));
            let reply = client.receive().await.expect("actual counter reply");
            assert_eq!(reply.from, root_address);
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            (
                allocations,
                sizes,
                [first.message, second.message, closed],
                reply.message,
            )
        })
        .await
        .unwrap_or_else(|_| panic!("application starts"));
    let ((allocations, sizes, documents, count), retirement) = report
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original work and joined retirement return"));
    assert_eq!(count, 1);
    for (document, original) in documents.iter().zip(pointers) {
        assert_eq!(document.as_ptr(), original);
    }
    assert_eq!(&*documents[0], &[107, 109]);
    assert_eq!(&*documents[1], &[113, 127]);
    assert_eq!(&*documents[2], &[131, 137]);
    assert!(retirement.cleanup().is_ok());
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("both notifications");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (origin, root) = retirement
        .retirement()
        .as_ref()
        .expect("native root returns");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed {
        custody,
        interpretation,
        source,
        settlements,
        ..
    } = root
    else {
        panic!("the original counter stops normally");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
    assert_eq!(custody.descendants, []);
    assert!(custody.control.is_empty());
    assert!(custody.user.is_empty());
    assert!(custody.received_interpretation.is_none());
    assert!(custody.received_source.is_none());
    assert!(custody.source_index.is_none());
    assert!(custody.acquired_ingress.is_none());
    assert!(custody.terminal_report.is_none());
    assert!(custody.unread_owner_cancellation.is_none());
    assert_eq!(custody.child_failures, []);
    assert!(custody.capability_failures.is_empty());
    assert!(custody.additional_failures.is_empty());
    assert!(custody.retirement_failures.is_empty());
    eprintln!("sender allocations/retained bytes: {allocations:?}; sizes: {sizes:?}");
    assert_eq!(
        allocations,
        [(0, 0); 3],
        "one-way senders need no reply storage"
    );
}
