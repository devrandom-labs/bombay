use super::{Archive, ArchiveReply, DocumentCommand, Library, LibraryCommand, Receipt};
use bombay::behavior::{BehaviorBase, Never};
use bombay::entity::{
    ActivationId, AdmissionFailure, DirectoryConfig, DrainFailure, EntityActivationError,
    EntityCapacity, EntityDefinition, EntityId,
};
use bombay::prelude::*;
use bombay::{ActorRetirementReport, ApplicationScope, MailboxCapacity, RetirementSchema};
use communication::TrySendError;
use core::num::NonZeroUsize;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::task::JoinError;

struct Entrance;

#[bombay::actor(message = Never)]
impl Entrance {}

#[derive(Clone, Copy)]
struct Libraries;

type LibraryRetirement = ActorRetirement<
    StopOnShutdown<Library>,
    catalogue::LibraryRetirements,
    Vec<<catalogue::LibraryRetirements as RetirementSchema>::Failure>,
>;
type LibraryStartup = EntityActivationError<
    Never,
    StopOnShutdown<Library>,
    catalogue::LibraryRetirements,
    Vec<<catalogue::LibraryRetirements as RetirementSchema>::Failure>,
>;

#[derive(Default)]
struct Reports {
    startups: Vec<(EntityId<u64>, ActivationId, LibraryStartup)>,
    admissions: Vec<(EntityId<u64>, AdmissionFailure<LibraryCommand>)>,
    drains: Vec<(EntityId<u64>, ActivationId, DrainFailure)>,
    #[expect(
        clippy::type_complexity,
        reason = "each original retirement row retains identity, attempt, complete native custody and its independent assessment"
    )]
    retirements: Vec<(
        EntityId<u64>,
        ActivationId,
        Result<LibraryRetirement, JoinError>,
        ActorRetirementReport,
    )>,
}

struct Catalogue {
    reply: EstablishedRecipient<ArchiveReply>,
    reports: Arc<Mutex<Reports>>,
}

impl EntityDefinition for Catalogue {
    type Id = u64;
    type Behavior = StopOnShutdown<Library>;
    type Hosts = catalogue::Topology;
    type HydrationError = Never;
    type Terminal = catalogue::LibraryRetirements;
    type ChildFailures = Vec<<Self::Terminal as RetirementSchema>::Failure>;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "hydrate only when the native family polls its owned work"
    )]
    async fn hydrate(&self, _: EntityId<u64>) -> Result<Self::Behavior, Never> {
        Ok(Library {
            visitors: Vec::new(),
            archive: Some(
                Archive {
                    documents: Vec::new(),
                }
                .stop_on_shutdown(),
            ),
            reply: self.reply.clone(),
            settlements: Vec::new(),
        }
        .stop_on_shutdown())
    }

    fn activation_failed(&self, id: EntityId<u64>, attempt: ActivationId, failure: LibraryStartup) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .startups
            .push((id, attempt, failure));
    }

    fn admission_refused(&self, id: EntityId<u64>, failure: AdmissionFailure<LibraryCommand>) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .admissions
            .push((id, failure));
    }

    fn forced_retirement(&self, id: &EntityId<u64>, attempt: ActivationId, failure: DrainFailure) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drains
            .push((*id, attempt, failure));
    }

    fn retired(
        &self,
        id: &EntityId<u64>,
        attempt: ActivationId,
        retirement: Result<LibraryRetirement, JoinError>,
        report: ActorRetirementReport,
    ) {
        self.reports
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retirements
            .push((*id, attempt, retirement, report));
    }
}

bombay::topology! {
    catalogue {
        root: Entrance {}
        hosts { libraries: Library; archives: Archive; receipts: Receipt; registry: ArchiveReply; }
        families { library: Catalogue; }
        library { Archive => archive; }
        archive {}
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one real family activation and child saturation trace preserves every original input and all callback and retirement custody"
)]
async fn installed_entity_family_carries_actor_limit_into_fresh_descendants_and_retirement() {
    let scope =
        ApplicationScope::new().mailbox_capacity(MailboxCapacity::new(2).expect("two actor slots"));
    let mut archives = scope
        .external::<ArchiveReply>()
        .expect("the child reply endpoint starts");
    let reports = Arc::new(Mutex::new(Reports::default()));
    let application = scope
        .with_topology(Entrance.stop_on_shutdown(), catalogue::Topology::new())
        .entity_family(
            Libraries,
            Catalogue {
                reply: archives.recipient(),
                reports: Arc::clone(&reports),
            },
            DirectoryConfig::default(),
            EntityCapacity::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
        );
    let outcome = application
        .run_with_entities(|application| async move {
            let libraries = application.entities(Libraries);
            let library = libraries.entity(7);
            let mut caller = application
                .external::<Receipt>()
                .expect("the actual document caller starts");
            let visitor = vec![7].into_boxed_slice();
            let visitor_identity = visitor.as_ptr();
            caller
                .send(&library, LibraryCommand::Visit(visitor, caller.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the actual visit activates its library"));
            let visit = caller
                .receive()
                .await
                .expect("the actual library processed its original visitor");
            assert_eq!(visit.message, 1);
            let archive = archives
                .receive()
                .await
                .expect("a family activation commits its fresh child")
                .message;
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
                panic!("Entity family startup must carry the selected limit into ordinary births");
            };
            assert!(ptr::eq(identity, document.as_ptr()));
            caller
                .send(&archive, DocumentCommand::Count(caller.recipient()))
                .await
                .unwrap_or_else(|_| panic!("the accepted prefix precedes its count"));
            let receipt = caller
                .receive()
                .await
                .expect("the Entity child processes the accepted prefix");
            assert_eq!(receipt.message, 2);
            let retired = application.retire_entity(Libraries, &7).await;
            assert!(retired.is_ok());
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
            (identities, document, visitor_identity)
        })
        .await
        .unwrap_or_else(|_| panic!("the actual Entity application starts"));
    let ((identities, document, visitor_identity), retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the original work and complete family cleanup survive"));
    assert_eq!(&*document, &[2]);
    assert!(retirement.cleanup().is_ok());
    let reports = reports.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(reports.startups.is_empty());
    assert!(reports.admissions.is_empty());
    assert_eq!(reports.drains.len(), 0);
    assert_eq!(reports.retirements.len(), 1);
    let (id, _, library, report) = &reports.retirements[0];
    assert_eq!(*id.get(), 7);
    assert_eq!(
        report.retirement(),
        bombay::RetirementAssessment::Established
    );
    let custody = library
        .as_ref()
        .unwrap_or_else(|_| panic!("the exact family actor is retained"))
        .custody()
        .expect("the original creator custody survives");
    assert_eq!(custody.descendants.len(), 1);
    assert_eq!(custody.behavior.base().visitors.len(), 1);
    assert!(ptr::eq(
        custody.behavior.base().visitors[0].as_ptr(),
        visitor_identity
    ));
    let catalogue::LibraryRetirements {
        retirement: archive,
        ..
    } = &custody.descendants[0];
    let documents = &archive
        .behavior()
        .expect("the actual Entity child survives retirement")
        .base()
        .documents;
    assert_eq!(documents.len(), identities.len());
    for (document, identity) in documents.iter().zip(identities) {
        assert!(ptr::eq(document.as_ptr(), identity));
    }
}
