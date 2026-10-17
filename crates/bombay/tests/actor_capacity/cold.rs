use super::{Archive, ArchiveReply, DocumentCommand, Library, Receipt};
use bombay::behavior::BehaviorBase;
use bombay::prelude::*;
use bombay::{ApplicationScope, MailboxCapacity};
use communication::TrySendError;
use std::ptr;

struct Entrance;

#[bombay::actor(message = Never)]
impl Entrance {}

struct Branch;

bombay::topology! {
    branches {
        root: Entrance {}
        members { Library(Branch) => library; }
        library { Archive => archive; }
        archive {}
        hosts { receipts: Receipt; archives: ArchiveReply; }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete cold creation, rejection, accepted prefix and independent retirement trace"
)]
#[tokio::test(flavor = "current_thread")]
async fn cold_member_passes_actor_capacity_to_its_fresh_child_and_returns_complete_custody() {
    let scope =
        ApplicationScope::new().mailbox_capacity(MailboxCapacity::new(2).expect("two actor slots"));
    let mut archives = scope
        .external::<ArchiveReply>()
        .expect("the actual birth reply endpoint starts");
    let reply = archives.recipient();
    let application = scope
        .with_topology(Entrance.stop_on_shutdown(), branches::Topology::new())
        .child(
            Branch,
            Library {
                visitors: Vec::new(),
                archive: Some(
                    Archive {
                        documents: Vec::new(),
                    }
                    .stop_on_shutdown(),
                ),
                reply,
                settlements: Vec::new(),
            }
            .stop_on_shutdown(),
        );
    let outcome = application.run_with(|application| async move {
        let archive = archives.receive().await.expect("the cold member commits its fresh child").message;
        let mut caller = application.external::<Receipt>().expect("the actual child caller starts");
        let mut identities = Vec::new();
        for value in 0..2 {
            let document = vec![value].into_boxed_slice();
            identities.push(document.as_ptr());
            let admission = caller.try_send(&archive, DocumentCommand::Store(document));
            assert!(admission.is_ok());
        }
        let document = vec![2].into_boxed_slice();
        let identity = document.as_ptr();
        let admission = caller.try_send(&archive, DocumentCommand::Store(document));
        let Err(TrySendError::Full(DocumentCommand::Store(document))) = admission else {
            panic!("the cold member must pass its original configured actor limit to fresh children");
        };
        assert!(ptr::eq(document.as_ptr(), identity));
        caller.send(&archive, DocumentCommand::Count(caller.recipient())).await.unwrap_or_else(|_| panic!("the accepted child prefix precedes its count"));
        let receipt = caller.receive().await.expect("both original documents were processed");
        assert_eq!(receipt.message, 2);
        let shutdown = application.shutdown();
        assert_eq!(shutdown, Ok(()));
        (identities, document)
    }).await.unwrap_or_else(|_| panic!("the cold-member application starts"));
    let ((identities, document), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the root joins the whole cold-member subtree"));
    assert_eq!(&*document, &[2]);
    assert!(retirement.cleanup().is_ok());
    let notifications = retirement
        .notifications()
        .as_ref()
        .expect("both notifications return");
    assert!(notifications.termination.is_ok());
    assert!(notifications.retirement.is_ok());
    let (_, root) = retirement
        .retirement()
        .as_ref()
        .expect("the original root remains owned");
    assert_eq!(root.completion(), Some(&Completion::Stopped));
    let ActorRetirement::Completed { custody: root, .. } = root else {
        panic!("the root stops normally");
    };
    assert_eq!(root.child_failures.len(), 0);
    assert_eq!(root.capability_failures.len(), 0);
    assert_eq!(root.additional_failures.len(), 0);
    assert_eq!(root.retirement_failures.len(), 0);
    assert_eq!(root.descendants.len(), 1);
    let branches::RootRetirements {
        retirement: library,
        ..
    } = &root.descendants[0];
    let ActorRetirement::OwnerCancelled {
        custody: library, ..
    } = library.as_ref()
    else {
        panic!("parent retirement cancels its original cold child");
    };
    assert_eq!(library.child_failures.len(), 0);
    assert_eq!(library.capability_failures.len(), 0);
    assert_eq!(library.additional_failures.len(), 0);
    assert_eq!(library.retirement_failures.len(), 0);
    assert_eq!(library.descendants.len(), 1);
    let branches::LibraryRetirements {
        retirement: archive,
        ..
    } = &library.descendants[0];
    let ActorRetirement::OwnerCancelled {
        custody: archive, ..
    } = archive.as_ref()
    else {
        panic!("parent retirement cancels its original fresh child");
    };
    assert_eq!(archive.child_failures.len(), 0);
    assert_eq!(archive.capability_failures.len(), 0);
    assert_eq!(archive.additional_failures.len(), 0);
    assert_eq!(archive.retirement_failures.len(), 0);
    let documents = &archive.behavior.base().documents;
    assert_eq!(documents.len(), identities.len());
    for (document, identity) in documents.iter().zip(identities) {
        assert!(ptr::eq(document.as_ptr(), identity));
    }
}
