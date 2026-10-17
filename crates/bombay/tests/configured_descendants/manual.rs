//! A local manual namespace retains generic child ports and actual construction.
use super::{
    Config, Document, DocumentReady, ManagedDocument, Root, archive, collect_documents,
    file_request, inspect_file,
};
use bombay::actors::ActorExt;
use bombay::behavior::{
    Behavior, BehaviorBase, ChildCreationOutcome, ChildHead, CreationId, CreationKind,
    CreationRejection, CreationSettlement, ItemSettlement, Never, NoSends, RoutedCreation,
    SettledItem,
};
use bombay::prelude::{Completion, Exit};
use bombay::{
    ActorRetirement, ActorSpace, ApplicationScope, ApplicationTopology, EstablishNativeChild,
    ExternalActor, MailAddr, NativeChild, NativeHost, PendingChild, ProjectTerminal,
    RootRegistration, Runtime, WorkOutcome,
};
use core::future::Future;
use core::pin::pin;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::oneshot;

struct Library {
    root: ActorSpace<<Root as Behavior>::Protocol>,
    runtime: Runtime<Config>,
}

impl NativeHost for Library {
    type Runtime = Runtime<Config>;
    fn runtime(&self) -> &Self::Runtime {
        &self.runtime
    }
}

impl RootRegistration<<Root as Behavior>::Protocol> for Library {
    fn root_space(&self) -> &ActorSpace<<Root as Behavior>::Protocol> {
        &self.root
    }
}

impl<C> ApplicationTopology<C, ()> for Library
where
    C: BehaviorBase<Base = Root>
        + Behavior<Protocol = <Root as Behavior>::Protocol, Birth = <Root as Behavior>::Birth>,
{
    type Descendants = archive::RootRetirements<C>;
}

impl<C>
    NativeChild<
        C,
        ChildHead,
        ManagedDocument,
        archive::RootRetirements<C>,
        archive::DocumentRetirements,
    > for Library
where
    C: BehaviorBase<Base = Root>
        + Behavior<Protocol = <Root as Behavior>::Protocol, Birth = <Root as Behavior>::Birth>,
    Self: EstablishNativeChild<C, ManagedDocument, archive::DocumentRetirements>,
{
    fn registration(&self) -> ActorSpace<<ManagedDocument as Behavior>::Protocol> {
        ActorSpace::new()
    }

    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            C,
            Self,
            ChildHead,
            ManagedDocument,
            archive::RootRetirements<C>,
            archive::DocumentRetirements,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedDocument>,
            ChildCreationOutcome<ManagedDocument, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        archive::RootRetirements<C>: ProjectTerminal<Origin, ManagedDocument, Descendants = archive::DocumentRetirements>
            + Send
            + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<C, ManagedDocument, archive::DocumentRetirements>>::establish_native_child::<ChildHead, archive::RootRetirements<C>, Origin, OriginFn>(pending)
    }
}

impl
    NativeChild<
        ManagedDocument,
        ChildHead,
        ManagedDocument,
        archive::DocumentRetirements,
        archive::DocumentRetirements,
    > for Library
{
    fn registration(&self) -> ActorSpace<<ManagedDocument as Behavior>::Protocol> {
        ActorSpace::new()
    }

    fn establish<Origin, OriginFn>(
        pending: PendingChild<
            '_,
            ManagedDocument,
            Self,
            ChildHead,
            ManagedDocument,
            archive::DocumentRetirements,
            archive::DocumentRetirements,
            OriginFn,
        >,
    ) -> impl Future<
        Output = ItemSettlement<
            RoutedCreation<MailAddr, ManagedDocument>,
            ChildCreationOutcome<ManagedDocument, ChildHead>,
            CreationRejection,
            Never,
        >,
    > + Send
    where
        archive::DocumentRetirements: ProjectTerminal<Origin, ManagedDocument, Descendants = archive::DocumentRetirements>
            + Send
            + 'static,
        Origin: Send + 'static,
        OriginFn: Fn(MailAddr, u64, CreationId, CreationKind) -> Origin + Send,
    {
        <Self as EstablishNativeChild<
            ManagedDocument,
            ManagedDocument,
            archive::DocumentRetirements,
        >>::establish_native_child::<ChildHead, archive::DocumentRetirements, Origin, OriginFn>(
            pending,
        )
    }
}

async fn receive_documents(replies: &mut ExternalActor<DocumentReady>) -> BTreeMap<u8, MailAddr> {
    let mut reports = BTreeMap::new();
    for _ in 0..3 {
        let report = tokio::time::timeout(Duration::from_secs(5), replies.receive())
            .await
            .expect("all three actual native initializations complete")
            .expect("each actor sends its typed readiness after acquiring its File");
        let previous = reports.insert(report.message, report.from);
        assert!(previous.is_none());
    }
    assert_eq!(
        reports.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([3, 5, 7])
    );
    reports
}

#[expect(
    clippy::type_complexity,
    reason = "return the actor with its independent original-allocation oracles without another domain wrapper"
)]
fn documents(
    replies: &ExternalActor<DocumentReady>,
) -> (Root, BTreeMap<usize, Vec<u8>>, [(usize, u8); 2]) {
    let root = Box::<[u8]>::from([3]);
    let child = Box::<[u8]>::from([5]);
    let grandchild = Box::<[u8]>::from([7]);
    let born = [
        (child.as_ptr() as usize, 5),
        (grandchild.as_ptr() as usize, 7),
    ];
    let expected = BTreeMap::from([
        (root.as_ptr() as usize, root.to_vec()),
        (child.as_ptr() as usize, child.to_vec()),
        (grandchild.as_ptr() as usize, grandchild.to_vec()),
    ]);
    (
        Root {
            original: root,
            file: Some(file_request()),
            document: Some(
                Document {
                    original: child,
                    file: Some(file_request()),
                    next: Some(Box::new(Document {
                        original: grandchild,
                        file: Some(file_request()),
                        next: None,
                        ready: replies.recipient(),
                    })),
                    ready: replies.recipient(),
                }
                .stop_on_shutdown(),
            ),
            ready: replies.recipient(),
        },
        expected,
        born,
    )
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native manual-host journey checks complete root and recursive descendant custody"
)]
async fn manual_namespace_keeps_configured_services_through_generic_root_and_recursive_child_ports()
{
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the actual early reply endpoint");
    let (root, expected, born) = documents(&replies);
    let constructions = Arc::new(AtomicUsize::new(0));
    let visits = Arc::new(Mutex::new(BTreeMap::new()));
    let hosts = Library {
        root: ActorSpace::new(),
        runtime: Runtime::with_interpreter(Config {
            label: Box::from(*b"documents"),
            constructions: Arc::clone(&constructions),
            visits: Arc::clone(&visits),
        }),
    };
    let (work, _root_report, native, notifications, ()) = scope
        .with_topology(root.stop_on_shutdown(), hosts)
        .run_with(|application| async move {
            let reports = receive_documents(&mut replies).await;
            let shutdown = application.shutdown();
            assert_eq!(shutdown, Ok(()));
            let termination = application.termination().await;
            assert_eq!(termination, Ok(Exit::Normal));
            reports
        })
        .await
        .unwrap_or_else(|_| panic!("the actual native host is entered"))
        .into_parts();
    let (reports, cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("actual application work completes"));
    cleanup.expect("the complete manual native graph joins");
    let notifications = notifications.expect("both independent notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (origin, retirement) = native.expect("the original root is independently received");
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
        panic!("the root handles its actual typed shutdown request");
    };
    assert_eq!(completion, Completion::Stopped);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.behavior.base().file.is_none());
    assert!(custody.behavior.base().document.is_none());
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
    let mut files = 0;
    let mut births = Vec::new();
    for settled in settlements {
        assert_eq!(settled.sends.owned, NoSends);
        assert_eq!(settled.sends.inner.ready.len(), 0);
        for file in settled.sends.inner.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("the original root File is retained");
            };
            inspect_file(file);
            files += 1;
        }
        let CreationSettlement::Settled(children) = settled.creations.into_settlement() else {
            panic!("the original creation lane settled");
        };
        for child in children {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::Established(child),
            )) = child
            else {
                panic!("the actual child birth committed");
            };
            births.push((child.id(), child.kind()));
        }
    }
    let mut originals = BTreeMap::from([(
        custody.behavior.base().original.as_ptr() as usize,
        custody.behavior.base().original.to_vec(),
    )]);
    let mut addresses = BTreeSet::from([origin.address()]);
    let mut descendants = custody.descendants.into_iter();
    let Some(archive::RootRetirements::Document { origin, retirement }) = descendants.next() else {
        panic!("the sole declared child remains independently owned");
    };
    assert!(descendants.next().is_none());
    assert_eq!(births, [(origin.creation_id(), CreationKind::Birth)]);
    assert_eq!(origin.creation_kind(), CreationKind::Birth);
    assert_eq!(reports[&5], origin.address());
    let fresh = addresses.insert(origin.address());
    assert!(fresh);
    files += collect_documents(*retirement, &born, &reports, &mut originals, &mut addresses);
    assert_eq!(files, 3);
    assert_eq!(originals, expected);
    assert_eq!(addresses.len(), 3);
    assert_eq!(constructions.load(Ordering::SeqCst), 3);
    assert_eq!(
        *visits.lock().unwrap_or_else(PoisonError::into_inner),
        BTreeMap::from([(0, 1), (1, 1), (2, 1)])
    );
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one native manual-host journey checks complete root and recursive descendant custody"
)]
async fn manual_namespace_preserves_bare_creator_services_and_owner_cancellation() {
    let scope = ApplicationScope::new();
    let mut replies = scope
        .external::<DocumentReady>()
        .expect("the actual early reply endpoint");
    let (root, expected, born) = documents(&replies);
    let constructions = Arc::new(AtomicUsize::new(0));
    let visits = Arc::new(Mutex::new(BTreeMap::new()));
    let hosts = Library {
        root: ActorSpace::new(),
        runtime: Runtime::with_interpreter(Config {
            label: Box::from(*b"documents"),
            constructions: Arc::clone(&constructions),
            visits: Arc::clone(&visits),
        }),
    };
    let (started, observing) = oneshot::channel();
    let (execution, result) = scope
        .with_topology(root, hosts)
        .execute_with(|application| async move {
            started
                .send(())
                .expect("the outside owner observes actual work entry");
            application.termination().await
        })
        .unwrap_or_else(|_| panic!("the actual native host is entered"));
    let reports = {
        let mut execution = pin!(execution);
        let reports = tokio::select! {
            () = &mut execution => panic!("the bare root remains live until owner cancellation"),
            reports = receive_documents(&mut replies) => reports,
        };
        tokio::select! {
            () = &mut execution => panic!("the entered caller still awaits actual actor termination"),
            started = tokio::time::timeout(Duration::from_secs(5), observing) => {
                started.expect("work entry must complete before owner cancellation")
                    .expect("work must begin before testing its interruption");
            },
        }
        reports
    };
    let (work, _root_report, native, notifications, ()) =
        tokio::time::timeout(Duration::from_secs(5), result)
            .await
            .expect("the dropped execution owner joins the complete graph")
            .into_parts();
    let WorkOutcome::Interrupted { cleanup } = work else {
        panic!("dropping execution preserves its actual interrupted work phase");
    };
    cleanup.expect("the complete manual native graph joins after owner cancellation");
    let notifications = notifications.expect("both independent notification results");
    notifications
        .termination
        .expect("actual termination publication");
    notifications
        .retirement
        .expect("actual retirement publication");
    let (origin, retirement) = native.expect("the original root is independently received");
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(reports[&3], origin.address());
    let ActorRetirement::OwnerCancelled {
        custody,
        settlements,
        interpretation,
        source,
    } = retirement
    else {
        panic!("the bare root retains its exact owner-cancelled retirement");
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(custody.behavior.base().file.is_none());
    assert!(custody.behavior.base().document.is_none());
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
    let mut files = 0;
    let mut births = Vec::new();
    for settled in settlements {
        assert_eq!(settled.sends.ready.len(), 0);
        for file in settled.sends.files {
            let SettledItem::Attempted(ItemSettlement::Accepted(file)) = file else {
                panic!("the original root File is retained");
            };
            inspect_file(file);
            files += 1;
        }
        let CreationSettlement::Settled(children) = settled.creations.into_settlement() else {
            panic!("the original creation lane settled");
        };
        for child in children {
            let SettledItem::Attempted(ItemSettlement::Accepted(
                ChildCreationOutcome::Established(child),
            )) = child
            else {
                panic!("the actual child birth committed");
            };
            births.push((child.id(), child.kind()));
        }
    }
    let mut originals = BTreeMap::from([(
        custody.behavior.base().original.as_ptr() as usize,
        custody.behavior.base().original.to_vec(),
    )]);
    let mut addresses = BTreeSet::from([origin.address()]);
    let mut descendants = custody.descendants.into_iter();
    let Some(archive::RootRetirements::Document { origin, retirement }) = descendants.next() else {
        panic!("the sole declared child remains independently owned");
    };
    assert!(descendants.next().is_none());
    assert_eq!(births, [(origin.creation_id(), CreationKind::Birth)]);
    assert_eq!(origin.creation_kind(), CreationKind::Birth);
    assert_eq!(reports[&5], origin.address());
    let fresh = addresses.insert(origin.address());
    assert!(fresh);
    files += collect_documents(*retirement, &born, &reports, &mut originals, &mut addresses);
    assert_eq!(files, 3);
    assert_eq!(originals, expected);
    assert_eq!(addresses.len(), 3);
    assert_eq!(constructions.load(Ordering::SeqCst), 3);
    assert_eq!(
        *visits.lock().unwrap_or_else(PoisonError::into_inner),
        BTreeMap::from([(0, 1), (1, 1), (2, 1)])
    );
}
