//! A configured native host must survive cold and recursive child startup.
use bombay::actors::ActorExt;
use bombay::behavior::{
    ActionItem, Actions, BehaviorActed, BehaviorBase, ChildChoice, ChildCreationOutcome,
    ClassifySettlement, CreateChild, CreationKind, CreationSequence, CreationSettlement, Creations,
    EstablishedDelivery, EstablishedRecipient, InterpretItem, InterpreterRequests, ItemSettlement,
    MessageProtocol, Never, NoSends, SettledItem, SettlementStatus,
};
use bombay::prelude::{ActorRetirement, Completion, Exit, StopOnShutdown};
use bombay::{ApplicationScope, MailAddr, RetirementSchema};
use core::future::Future;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

#[path = "application_services/requests.rs"]
mod requests;

use requests::{FileAccess, OpenFile, Services};

impl bombay::ExternalRequest for OpenFile {
    type Interpreter = Files;
}

struct Files {
    identity: usize,
    calls: Cell<usize>,
    visits: Arc<Mutex<BTreeMap<usize, usize>>>,
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
        let previous = self.calls.replace(self.calls.get() + 1);
        assert_eq!(
            previous, 0,
            "each separately owned service interprets its one file"
        );
        let previous = self
            .visits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self.identity, self.calls.get());
        assert!(
            previous.is_none(),
            "no service instance is shared between actors"
        );
        <Services as InterpretItem<OpenFile, Event, Path>>::interpret_item(
            &mut self.service,
            input,
            received,
        )
    }
}

struct Config {
    label: Box<[u8]>,
    constructions: Arc<AtomicUsize>,
    visits: Arc<Mutex<BTreeMap<usize, usize>>>,
}

impl bombay::InterpreterFactory for Config {
    type Interpreter = Files;

    fn create(&self) -> Files {
        assert_eq!(&*self.label, b"documents");
        let identity = self.constructions.fetch_add(1, Ordering::SeqCst);
        Files {
            identity,
            calls: Cell::new(0),
            visits: Arc::clone(&self.visits),
            service: Services,
        }
    }
}

struct Root {
    original: Box<[u8]>,
    file: Option<OpenFile>,
    document: Option<ManagedDocument>,
    ready: EstablishedRecipient<DocumentReady>,
}

type DocumentReady = MessageProtocol<MailAddr, u8>;

#[bombay::actor(
    message = Never,
    sends = {
        files: InterpreterRequests<OpenFile>,
        ready: Vec<EstablishedDelivery<DocumentReady>>,
    },
    births = { document: ManagedDocument },
    creation_settlements = retain_for_retirement,
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor contract returns typed Actions through its success or error carrier"
)]
impl Root {
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut sequence = CreationSequence::new();
        let id = sequence.issue().expect("one root child fits");
        let document = self.document.take().expect("the original root child");
        let file = self.file.take().expect("the original root file request");
        Ok(
            Actions::create(Creations::one(CreateChild::birth(id, document)))
                .with_send(file)
                .with_send(EstablishedDelivery::new(
                    self.ready.clone(),
                    self.original[0],
                )),
        )
    }
}

struct Document {
    original: Box<[u8]>,
    file: Option<OpenFile>,
    next: Option<Box<Document>>,
    ready: EstablishedRecipient<DocumentReady>,
}

type ManagedDocument = StopOnShutdown<Document>;

#[bombay::actor(
    message = Never,
    sends = {
        files: InterpreterRequests<OpenFile>,
        ready: Vec<EstablishedDelivery<DocumentReady>>,
    },
    births = { next: ManagedDocument },
    creation_settlements = retain_for_retirement,
)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the actor contract returns typed Actions through its success or error carrier"
)]
impl Document {
    fn init(&mut self) -> BehaviorActed<Self> {
        let file = self
            .file
            .take()
            .expect("the original descendant file request");
        let actions = match self.next.take() {
            Some(next) => {
                let mut sequence = CreationSequence::new();
                let id = sequence.issue().expect("one recursive child fits");
                Actions::create(Creations::one(CreateChild::birth(
                    id,
                    (*next).stop_on_shutdown(),
                )))
            }
            None => Actions::cont(),
        };
        Ok(actions.with_send(file).with_send(EstablishedDelivery::new(
            self.ready.clone(),
            self.original[0],
        )))
    }
}

struct Background;

bombay::topology! {
    archive {
        interpreter: Config;
        root: Root { Document(RootChildrenDocument) => document; }
        members { Background(Background) => document; }
        document: ManagedDocument { Next(DocumentChildrenNext) => document; }
    }
}

fn file_request() -> OpenFile {
    OpenFile {
        path: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
        access: FileAccess::Ready,
    }
}

fn inspect_file(mut file: File) {
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("the actual acquired file survives complete native retirement");
    assert_eq!(contents, include_str!("../Cargo.toml"));
}

type DocumentRetirement = ActorRetirement<
    ManagedDocument,
    archive::DocumentRetirements,
    Vec<<archive::DocumentRetirements as RetirementSchema>::Failure>,
>;

fn collect_documents(
    mut retirement: DocumentRetirement,
    expected: &[(usize, u8)],
    reports: &BTreeMap<u8, MailAddr>,
    originals: &mut BTreeMap<usize, Vec<u8>>,
    addresses: &mut BTreeSet<MailAddr>,
) -> usize {
    let mut count = 0;
    let mut remaining = expected.iter();
    loop {
        let (custody, settlements) = match retirement {
            ActorRetirement::Completed {
                custody,
                completion,
                settlements,
                interpretation,
                source,
            } => {
                assert_eq!(completion, Completion::Stopped);
                assert!(interpretation.is_none());
                assert!(source.is_none());
                (custody, settlements)
            }
            ActorRetirement::OwnerCancelled {
                custody,
                settlements,
                interpretation,
                source,
            } => {
                assert!(interpretation.is_none());
                assert!(source.is_none());
                (custody, settlements)
            }
            _ => panic!("every descendant must return its complete ordinary retirement"),
        };
        assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
        let mut files = 0;
        let mut births = Vec::new();
        for settled in settlements {
            assert_eq!(settled.sends.owned, NoSends);
            assert_eq!(settled.sends.inner.ready.len(), 0);
            for file in settled.sends.inner.files {
                let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                    panic!("the descendant's actual acquired file survives retirement");
                };
                inspect_file(file);
                files += 1;
            }
            let CreationSettlement::Settled(children) = settled.creations.into_settlement() else {
                panic!("every descendant birth lane was interpreted completely");
            };
            for child in children {
                let SettledItem::Attempted(ItemSettlement::Accepted(
                    ChildCreationOutcome::Established(child),
                )) = child
                else {
                    panic!("every actual descendant creation committed successfully");
                };
                assert_eq!(child.kind(), CreationKind::Birth);
                births.push((child.id(), child.kind()));
            }
        }
        assert_eq!(files, 1);
        count += files;
        let actor = custody.behavior.base();
        let (pointer, value) = remaining
            .next()
            .expect("no extra actor in this declared role");
        assert_eq!(actor.original.as_ptr() as usize, *pointer);
        assert_eq!(&*actor.original, &[*value]);
        assert!(actor.file.is_none());
        assert!(actor.next.is_none());
        let previous = originals.insert(actor.original.as_ptr() as usize, actor.original.to_vec());
        assert!(previous.is_none());
        assert_eq!(custody.control.len(), 0);
        assert_eq!(custody.user.len(), 0);
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
        let mut children = custody.descendants.into_iter();
        let Some(archive::DocumentRetirements {
            origin,
            retirement: next,
        }) = children.next()
        else {
            assert_eq!(births.len(), 0);
            assert!(remaining.next().is_none());
            return count;
        };
        assert!(children.next().is_none());
        assert_eq!(births, [(origin.creation_id(), origin.creation_kind())]);
        let (_, next_value) = remaining
            .as_slice()
            .first()
            .expect("the exact next domain actor");
        assert_eq!(reports[next_value], origin.address());
        let fresh = addresses.insert(origin.address());
        assert!(fresh);
        retirement = *next;
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native ownership journey checks every independent original result and retirement lane"
)]
async fn configured_services_reach_fresh_grandchildren_and_cold_members() {
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    for constructions in [&first, &second] {
        let visits = Arc::new(Mutex::new(BTreeMap::new()));
        let scope = ApplicationScope::new();
        let mut replies = scope
            .external::<DocumentReady>()
            .expect("a real reply endpoint exists before pure actor construction");
        let root_bytes = Box::<[u8]>::from([3]);
        let child_bytes = Box::<[u8]>::from([5]);
        let grandchild_bytes = Box::<[u8]>::from([7]);
        let cold_bytes = Box::<[u8]>::from([11]);
        let born = [
            (child_bytes.as_ptr() as usize, 5),
            (grandchild_bytes.as_ptr() as usize, 7),
        ];
        let cold = [(cold_bytes.as_ptr() as usize, 11)];
        let expected = BTreeMap::from([
            (root_bytes.as_ptr() as usize, root_bytes.to_vec()),
            (child_bytes.as_ptr() as usize, child_bytes.to_vec()),
            (
                grandchild_bytes.as_ptr() as usize,
                grandchild_bytes.to_vec(),
            ),
            (cold_bytes.as_ptr() as usize, cold_bytes.to_vec()),
        ]);
        let grandchild = Document {
            original: grandchild_bytes,
            file: Some(file_request()),
            next: None,
            ready: replies.recipient(),
        };
        let application = scope
            .with_topology(
                Root {
                    original: root_bytes,
                    file: Some(file_request()),
                    document: Some(
                        Document {
                            original: child_bytes,
                            file: Some(file_request()),
                            next: Some(Box::new(grandchild)),
                            ready: replies.recipient(),
                        }
                        .stop_on_shutdown(),
                    ),
                    ready: replies.recipient(),
                }
                .stop_on_shutdown(),
                archive::Topology::new(Config {
                    label: Box::from(*b"documents"),
                    constructions: Arc::clone(constructions),
                    visits: Arc::clone(&visits),
                }),
            )
            .child(
                Background,
                Document {
                    original: cold_bytes,
                    file: Some(file_request()),
                    next: None,
                    ready: replies.recipient(),
                }
                .stop_on_shutdown(),
            );
        let (work, native, notifications, ()) = application
            .run_with(|application| async move {
                let mut ready = BTreeMap::new();
                for _ in 0..4 {
                    let report = replies
                        .receive()
                        .await
                        .expect("each initialized actor replies");
                    let previous = ready.insert(report.message, report.from);
                    assert!(previous.is_none());
                }
                assert_eq!(
                    ready.keys().copied().collect::<BTreeSet<_>>(),
                    BTreeSet::from([3, 5, 7, 11])
                );
                assert_eq!(constructions.load(Ordering::SeqCst), 4);
                let shutdown = application.shutdown();
                assert_eq!(shutdown, Ok(()));
                let termination = application.termination().await;
                assert_eq!(termination, Ok(Exit::Normal));
                ready
            })
            .await
            .unwrap_or_else(|_| panic!("the configured native application enters its host"))
            .into_parts();
        let (reports, cleanup) = work
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the complete work phase remains available"));
        cleanup.expect("the original native owner joins all four actors");
        let visits = visits.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(*visits, BTreeMap::from([(0, 1), (1, 1), (2, 1), (3, 1)]));
        let notifications = notifications.expect("independent root notification custody");
        notifications.termination.expect("termination publication");
        notifications.retirement.expect("retirement publication");
        let (origin, retirement) = native.expect("independent root retirement custody");
        assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
        assert_eq!(reports[&3], origin.address());
        let ActorRetirement::Completed {
            custody,
            completion,
            settlements,
            interpretation,
            source,
        } = retirement
        else {
            panic!("the original root handles its actual shutdown command");
        };
        assert_eq!(completion, Completion::Stopped);
        assert!(interpretation.is_none());
        assert!(source.is_none());
        assert_eq!(settlements.settlement_status(), SettlementStatus::Accepted);
        let mut files = 0;
        let mut births = BTreeMap::new();
        for settled in settlements {
            assert_eq!(settled.sends.owned, NoSends);
            assert_eq!(settled.sends.inner.ready.len(), 0);
            for file in settled.sends.inner.files {
                let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                    panic!("the root's real acquired file survives alongside all children");
                };
                inspect_file(file);
                files += 1;
            }
            let CreationSettlement::Settled(children) = settled.creations.into_settlement() else {
                panic!("every root birth lane was interpreted completely");
            };
            for child in children {
                let SettledItem::Attempted(ItemSettlement::Accepted(child)) = child else {
                    panic!("every root birth has its complete settlement");
                };
                let (role, id, kind) = match child {
                    ChildChoice::Head(ChildCreationOutcome::Established(child)) => {
                        ("document", child.id(), child.kind())
                    }
                    ChildChoice::Tail(ChildChoice::Head(ChildCreationOutcome::Established(
                        child,
                    ))) => ("background", child.id(), child.kind()),
                    _ => panic!("both actual root births committed successfully"),
                };
                assert_eq!(kind, CreationKind::Birth);
                let previous = births.insert(role, (id, kind));
                assert!(previous.is_none());
            }
        }
        assert_eq!(files, 1);
        let actor = custody.behavior.base();
        assert!(actor.file.is_none());
        assert!(actor.document.is_none());
        let mut originals =
            BTreeMap::from([(actor.original.as_ptr() as usize, actor.original.to_vec())]);
        assert_eq!(custody.control.len(), 0);
        assert_eq!(custody.user.len(), 0);
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
        let mut addresses = BTreeSet::from([origin.address()]);
        let mut roles = BTreeSet::new();
        for child in custody.descendants {
            let (role, origin, retirement, expected_chain) = match child {
                archive::RootRetirements::Document { origin, retirement } => {
                    assert_eq!(
                        births.remove("document"),
                        Some((origin.creation_id(), origin.creation_kind()))
                    );
                    assert_eq!(reports[&5], origin.address());
                    ("document", origin.address(), retirement, born.as_slice())
                }
                archive::RootRetirements::Background { origin, retirement } => {
                    assert_eq!(
                        births.remove("background"),
                        Some((origin.creation_id(), origin.creation_kind()))
                    );
                    assert_eq!(reports[&11], origin.address());
                    ("background", origin.address(), retirement, cold.as_slice())
                }
            };
            let fresh = addresses.insert(origin);
            assert!(fresh);
            let distinct = roles.insert(role);
            assert!(distinct);
            files += collect_documents(
                *retirement,
                expected_chain,
                &reports,
                &mut originals,
                &mut addresses,
            );
        }
        assert_eq!(births.len(), 0);
        assert_eq!(roles, BTreeSet::from(["document", "background"]));
        assert_eq!(addresses.len(), 4);
        assert_eq!(originals, expected);
        assert_eq!(files, 4);
    }
    assert_eq!(first.load(Ordering::SeqCst), 4);
    assert_eq!(second.load(Ordering::SeqCst), 4);
}

#[path = "configured_descendants/manual.rs"]
mod manual;

#[path = "configured_descendants/startup.rs"]
mod startup;

#[path = "configured_descendants/entity.rs"]
mod entity;
