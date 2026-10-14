use bombay::behavior::{
    Behavior, BehaviorBase, ChildCreationOutcome, CreateChild, CreationSequence,
    CreationSettlement, Creations, CreationsSettled, ItemSettlement, MessageProtocol, SettledItem,
};
use bombay::prelude::*;
use bombay::{ApplicationScope, MailboxCapacity};
use communication::TrySendError;
use std::ptr;
use tokio::runtime::Builder;

#[path = "actor_capacity/entity.rs"]
mod entity;

type Receipt = MessageProtocol<MailAddr, usize>;

enum DocumentCommand {
    Store(Box<[u8]>),
    Count(EstablishedRecipient<Receipt>),
}

struct Archive {
    documents: Vec<Box<[u8]>>,
}

type ManagedArchive = StopOnShutdown<Archive>;
type ArchiveReply =
    MessageProtocol<MailAddr, EstablishedRecipient<<Archive as Behavior>::Protocol>>;

enum LibraryCommand {
    Visit(Box<[u8]>, EstablishedRecipient<Receipt>),
}

struct Library {
    visitors: Vec<Box<[u8]>>,
    archive: Option<ManagedArchive>,
    reply: EstablishedRecipient<ArchiveReply>,
    settlements: Vec<CreationsSettled<MailAddr, ManagedArchive>>,
}

#[bombay::actor(
    births = { archive: ManagedArchive },
    creation_settlements = return_to_creator,
    sends = { archives: Vec<EstablishedDelivery<ArchiveReply>>, receipts: Vec<EstablishedDelivery<Receipt>> },
)]
impl Library {
    fn receive(&mut self, command: LibraryCommand) -> BehaviorActed<Self> {
        let LibraryCommand::Visit(visitor, reply) = command;
        self.visitors.push(visitor);
        Ok(Actions::cont().with_send(EstablishedDelivery::new(reply, self.visitors.len())))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the owning actor algebra returns complete typed Actions through its existing error carrier"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one fresh archive request");
        let archive = self
            .archive
            .take()
            .expect("the original archive starts once");
        Ok(Actions::create(Creations::one(CreateChild::birth(
            id, archive,
        ))))
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "the owning actor algebra returns complete typed Actions through its existing error carrier"
    )]
    fn creations_settled(
        &mut self,
        settled: CreationsSettled<MailAddr, LibraryChildren>,
    ) -> BehaviorActed<Self> {
        let settlement = settled.into_settlement();
        let mut actions = Actions::cont();
        if let CreationSettlement::Settled(creations) = &settlement {
            for creation in creations {
                if let SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(archive),
                )) = creation
                {
                    actions = actions.with_send(EstablishedDelivery::new(
                        self.reply.clone(),
                        archive.actor().recipient(),
                    ));
                }
            }
        }
        self.settlements.push(CreationsSettled::new(settlement));
        Ok(actions)
    }
}

bombay::topology! {
    library {
        root { Archive => archive; }
        archive {}
        hosts { receipts: Receipt; archives: ArchiveReply; }
    }
}

#[bombay::actor(sends = { receipts: Vec<EstablishedDelivery<Receipt>> })]
impl Archive {
    fn receive(&mut self, command: DocumentCommand) -> BehaviorActed<Self> {
        match command {
            DocumentCommand::Store(document) => {
                self.documents.push(document);
                Ok(Actions::cont())
            }
            DocumentCommand::Count(recipient) => Ok(Actions::cont()
                .with_send(EstablishedDelivery::new(recipient, self.documents.len()))),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn root_limit_preserves_rejection_retry_prefix_and_complete_retirement() {
    let documents: Vec<_> = (0..5).map(|value| vec![value].into_boxed_slice()).collect();
    let identities: Vec<_> = documents.iter().map(|value| value.as_ptr()).collect();
    let outcome = Application::new(
        Archive {
            documents: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .mailbox_capacity(MailboxCapacity::new(3).expect("four actor slots"))
    .run_with(|application| async move {
        let mut caller = application
            .external::<Receipt>()
            .expect("an actual receipt endpoint");
        let recipient = application.root().established_recipient();
        let mut documents = documents.into_iter();
        for _ in 0..4 {
            let document = documents.next().expect("the original accepted prefix");
            let admission = caller.try_send(&recipient, DocumentCommand::Store(document));
            assert!(admission.is_ok());
        }
        let document = documents.next().expect("the original fifth document");
        let identity = document.as_ptr();
        let admission = caller.try_send(&recipient, DocumentCommand::Store(document));
        let Err(TrySendError::Full(DocumentCommand::Store(document))) = admission else {
            panic!("the fifth document must return intact on saturation");
        };
        assert!(ptr::eq(identity, document.as_ptr()));
        caller
            .send(&recipient, DocumentCommand::Count(caller.recipient()))
            .await
            .unwrap_or_else(|_| panic!("count admitted"));
        let receipt = caller
            .receive()
            .await
            .expect("the accepted prefix is processed");
        assert_eq!(receipt.message, 4);
        caller
            .send(&recipient, DocumentCommand::Store(document))
            .await
            .unwrap_or_else(|_| panic!("the original rejection is retryable"));
        caller
            .send(&recipient, DocumentCommand::Count(caller.recipient()))
            .await
            .unwrap_or_else(|_| panic!("the final count is admitted"));
        let receipt = caller
            .receive()
            .await
            .expect("all accepted documents precede the reply");
        assert_eq!(receipt.message, 5);
        let shutdown = application.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        let report = application.retirement().await;
        assert_eq!(
            report.retirement(),
            bombay::RetirementAssessment::Established
        );
        receipt.message
    })
    .await
    .unwrap_or_else(|_| panic!("the native caller owns the application host"));
    let (count, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("work and retirement complete"));
    assert_eq!(count, 5);
    assert!(retirement.cleanup().is_ok());
    let (_, archive) = retirement
        .retirement()
        .as_ref()
        .expect("the actual actor remains owned");
    assert_eq!(archive.completion(), Some(&Completion::Stopped));
    let archive = archive
        .behavior()
        .expect("all documents remain in the original actor")
        .base();
    assert_eq!(archive.documents.len(), identities.len());
    for (index, (document, identity)) in archive.documents.iter().zip(identities).enumerate() {
        assert!(ptr::eq(document.as_ptr(), identity));
        assert_eq!(
            &**document,
            &[u8::try_from(index).expect("five documents fit in a byte")]
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn early_scope_limit_survives_root_assembly_and_shutdown_with_full_admission() {
    let scope =
        ApplicationScope::new().mailbox_capacity(MailboxCapacity::new(2).expect("two actor slots"));
    let outcome = scope
        .with_root(
            Archive {
                documents: Vec::new(),
            }
            .stop_on_shutdown(),
        )
        .run_with(|application| async move {
            let caller = application
                .external::<Receipt>()
                .expect("the real sending identity starts");
            let recipient = application.root().established_recipient();
            for value in 0..2 {
                let admission = caller.try_send(
                    &recipient,
                    DocumentCommand::Store(vec![value].into_boxed_slice()),
                );
                assert!(admission.is_ok());
            }
            let document = vec![9].into_boxed_slice();
            let identity = document.as_ptr();
            let admission = caller.try_send(&recipient, DocumentCommand::Store(document));
            let Err(TrySendError::Full(DocumentCommand::Store(document))) = admission else {
                panic!("scope selection must apply to the actual root mailbox");
            };
            assert!(ptr::eq(identity, document.as_ptr()));
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let report = application.retirement().await;
            assert_eq!(
                report.retirement(),
                bombay::RetirementAssessment::Established
            );
            document
        })
        .await
        .unwrap_or_else(|_| panic!("the scoped application starts"));
    let (document, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the saturated actor still retires"));
    assert_eq!(&*document, &[9]);
    assert!(retirement.cleanup().is_ok());
}

#[test]
fn missing_host_returns_actor_configuration_and_original_work_for_retry() {
    let application = Application::new(
        Archive {
            documents: Vec::new(),
        }
        .stop_on_shutdown(),
    )
    .mailbox_capacity(MailboxCapacity::new(2).expect("two actor slots"));
    let original = vec![29].into_boxed_slice();
    let identity = original.as_ptr();
    let Err((application, work, _error)) = application.execute_with(|application| async move {
        let mut caller = application
            .external::<Receipt>()
            .expect("the original work establishes its receipt endpoint");
        let recipient = application.root().established_recipient();
        for value in 0..2 {
            let admitted = caller.try_send(
                &recipient,
                DocumentCommand::Store(vec![value].into_boxed_slice()),
            );
            assert!(admitted.is_ok());
        }
        let admission = caller.try_send(&recipient, DocumentCommand::Store(original));
        let Err(TrySendError::Full(DocumentCommand::Store(original))) = admission else {
            panic!("rejected startup must retain the original actor capacity");
        };
        assert!(ptr::eq(identity, original.as_ptr()));
        caller
            .send(&recipient, DocumentCommand::Count(caller.recipient()))
            .await
            .unwrap_or_else(|_| panic!("the accepted prefix precedes its count"));
        let receipt = caller
            .receive()
            .await
            .expect("the original work receives its actual reply");
        assert_eq!(receipt.message, 2);
        let shutdown = application.request_shutdown();
        assert_eq!(shutdown, Ok(()));
        original
    }) else {
        panic!("missing-host rejection must return the actual application and original work");
    };
    let host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the native retry host starts");
    let outcome = host
        .block_on(application.run_with(work))
        .unwrap_or_else(|_| panic!("the same returned inputs retry"));
    let (original, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("original work and joined retirement complete"));
    assert!(ptr::eq(identity, original.as_ptr()));
    assert!(retirement.cleanup().is_ok());
    let (_, archive) = retirement
        .retirement()
        .as_ref()
        .expect("the complete retried actor remains owned");
    assert_eq!(
        archive
            .behavior()
            .expect("the actual accepted prefix survives")
            .base()
            .documents
            .len(),
        2
    );
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one fresh-child trace preserves complete birth, rejection, retry and joined terminal custody"
)]
async fn fresh_child_inherits_the_limit_and_returns_original_documents_at_retirement() {
    let scope =
        ApplicationScope::new().mailbox_capacity(MailboxCapacity::new(2).expect("two actor slots"));
    let mut archives = scope
        .external::<ArchiveReply>()
        .expect("the birth reply endpoint starts");
    let reply = archives.recipient();
    let outcome = scope
        .with_topology(
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
            library::Topology::new(),
        )
        .run_with(|application| async move {
            let archive = archives
                .receive()
                .await
                .expect("the interpreter committed a fresh archive")
                .message;
            let mut caller = application
                .external::<Receipt>()
                .expect("the archive receipt endpoint starts");
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
                panic!("a fresh child must inherit the creator's actor limit");
            };
            assert!(ptr::eq(identity, document.as_ptr()));
            caller
                .send(&archive, DocumentCommand::Count(caller.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the accepted child prefix precedes its count"));
            let receipt = caller
                .receive()
                .await
                .expect("the child processed both originals");
            assert_eq!(receipt.message, 2);
            caller
                .send(&archive, DocumentCommand::Store(document))
                .await
                .unwrap_or_else(|_| panic!("the original child rejection is retryable"));
            caller
                .send(&archive, DocumentCommand::Count(caller.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the child final count is admitted"));
            let receipt = caller
                .receive()
                .await
                .expect("all three original child inputs are processed");
            assert_eq!(receipt.message, 3);
            identities.push(identity);
            let shutdown = application.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            let report = application.retirement().await;
            assert_eq!(
                report.retirement(),
                bombay::RetirementAssessment::Established
            );
            identities
        })
        .await
        .unwrap_or_else(|_| panic!("the library application starts on the native host"));
    let (identities, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the creator joins its complete child retirement"));
    assert!(retirement.cleanup().is_ok());
    let (_, root) = retirement
        .retirement()
        .as_ref()
        .expect("the creator remains owned");
    let custody = root
        .custody()
        .expect("the complete creator custody remains");
    assert_eq!(custody.descendants.len(), 1);
    assert_eq!(custody.behavior.base().settlements.len(), 1);
    let library::RootRetirements {
        retirement: archive,
        ..
    } = &custody.descendants[0];
    let documents = &archive
        .behavior()
        .expect("the actual child retains its documents")
        .base()
        .documents;
    assert_eq!(documents.len(), identities.len());
    for (document, identity) in documents.iter().zip(identities) {
        assert!(ptr::eq(document.as_ptr(), identity));
    }
}
