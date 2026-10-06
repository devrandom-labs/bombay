//! For learning a parent-owned application topology with two heterogeneous
//! named children, same-action child delivery, and creation-dependent phased
//! shutdown. Behavior owns the closed roles and routes, Behavior Actors owns
//! the shutdown plan, and Bombay only interprets and retains the result.

use bombay::behavior::{
    BehaviorBase, BehaviorSettlements, ChildHead, ChildTail, Children, ClassifySettlement,
    CreationSequence, Never, SettlementStatus,
};
use bombay::lifecycle::shutdown_after_children;
use bombay::prelude::*;
use bombay::{ChildFailure, ProjectTerminal};

const INDEXER_NONCE: u64 = 0;
const JOURNAL_NONCE: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
enum IndexCommand {
    Index(Vec<String>),
}

#[derive(Default)]
struct Indexer {
    indexed_documents: usize,
}

#[bombay::actor]
impl Indexer {
    fn receive(&mut self, command: IndexCommand) -> BehaviorActed<Self> {
        let IndexCommand::Index(documents) = command;
        self.indexed_documents += documents.len();
        Ok(Actions::cont())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum JournalCommand {
    Record(String),
}

#[derive(Default)]
struct Journal {
    entries: Vec<String>,
}

#[bombay::actor]
impl Journal {
    fn receive(&mut self, command: JournalCommand) -> BehaviorActed<Self> {
        let JournalCommand::Record(entry) = command;
        self.entries.push(entry);
        Ok(Actions::cont())
    }
}

type ManagedIndexer = StopOnShutdown<Indexer>;
type ManagedJournal = StopOnShutdown<Journal>;

struct DocumentSystem;

#[bombay::actor(
    message = Never,
    sends = pub(crate) {
        indexing: Vec<ChildDelivery<Indexer, DocumentSystemChildrenIndexer>>,
        journaling: Vec<ChildDelivery<Journal, DocumentSystemChildrenJournal>>,
    },
    births = {
        indexer: ManagedIndexer,
        journal: ManagedJournal,
    },
    creation_settlements = retain_for_retirement,
)]
impl DocumentSystem {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let mut creations = CreationSequence::new();
        let indexer = creations
            .issue()
            .expect("the indexer creation ID is available");
        let journal = creations
            .issue()
            .expect("the journal creation ID is available");
        let children = Children::<MailAddr>::new()
            .child(indexer, Indexer::default().stop_on_shutdown())
            .child(journal, Journal::default().stop_on_shutdown())
            .into_creates();
        Ok(Actions::create(children)
            .send_indexing(ChildDelivery::after(
                indexer,
                IndexCommand::Index(vec!["contract".to_owned(), "ledger".to_owned()]),
            ))
            .send_journaling(ChildDelivery::after(
                journal,
                JournalCommand::Record("topology initialized".to_owned()),
            )))
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: RootOrigin<R>,
        #[expect(
            clippy::type_complexity,
            reason = "The root retirement retains both original typed child failure products and their distinct structural origins."
        )]
        terminal: ActorRetirement<
            R,
            Self,
            (
                Vec<ChildFailure<ChildOrigin<DocumentSystem, ChildHead>, ManagedJournal>>,
                (
                    Vec<
                        ChildFailure<
                            ChildOrigin<DocumentSystem, ChildTail<ChildHead>>,
                            ManagedIndexer,
                        >,
                    >,
                    (),
                ),
            ),
        >,
    },
    #[declared_child(DocumentSystem, DocumentSystemChildrenIndexer, ManagedIndexer)]
    Indexer {
        origin: ChildOrigin<DocumentSystem, DocumentSystemChildrenIndexer>,
        terminal: ActorRetirement<ManagedIndexer, Self, ()>,
    },
    #[declared_child(DocumentSystem, DocumentSystemChildrenJournal, ManagedJournal)]
    Journal {
        origin: ChildOrigin<DocumentSystem, DocumentSystemChildrenJournal>,
        terminal: ActorRetirement<ManagedJournal, Self, ()>,
    },
}

fn main() {
    let application = shutdown_after_children(DocumentSystem)
        .shutdown_phase(DocumentSystemChild::Indexer)
        .shutdown_phase(DocumentSystemChild::Journal)
        .finish();
    let (termination, origin, retirement) = Application::new(application)
        .run_with(|application| async move {
            let lifecycle = application.lifecycle();
            let shutdown = lifecycle.request_shutdown();
            assert_eq!(shutdown, Ok(()));
            lifecycle.termination().await
        })
        .expect("the named child topology activates and shuts down in declared phase order");

    assert_eq!(termination, Ok(Exit::Normal));
    let terminal = ApplicationTerminal::project(
        origin,
        retirement.expect("the original named child topology joined"),
    );
    assert_terminal_tree(terminal);
}

fn assert_terminal_tree<R>(terminal: ApplicationTerminal<R>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                child_failures: (journal_failures, (indexer_failures, ())),
                capability_failures,
                unread_owner_cancellation,
                settlements,
                control,
                user,
                descendants,
                completion,
                behavior,
                interpretation,
                source,
                additional_failures,
                received_interpretation,
                received_source,
                source_index,
                acquired_ingress,
                retirement_failures,
                terminal_report,
            },
    } = terminal
    else {
        panic!("phased shutdown must preserve the completed application root")
    };
    assert_eq!(journal_failures.len(), 0);
    assert_eq!(indexer_failures.len(), 0);
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);

    let mut indexer = None;
    let mut journal = None;
    for descendant in descendants {
        match descendant {
            ApplicationTerminal::Indexer { origin, terminal } => {
                let indexed_documents = assert_indexer(terminal);
                let previous_indexer = indexer.replace(indexed_documents);
                assert_eq!(previous_indexer, None);
                assert_eq!(origin.nonce(), INDEXER_NONCE);
            }
            ApplicationTerminal::Journal { origin, terminal } => {
                let journal_entry = assert_journal(terminal);
                let previous_journal = journal.replace(journal_entry);
                assert_eq!(previous_journal, None);
                assert_eq!(origin.nonce(), JOURNAL_NONCE);
            }
            ApplicationTerminal::Root { .. } => {
                panic!("a child retirement cannot project as another root")
            }
        }
    }
    assert_eq!(indexer, Some(2));
    assert_eq!(journal.as_deref(), Some("topology initialized"));
}

fn assert_indexer<R>(terminal: ActorRetirement<ManagedIndexer, ApplicationTerminal<R>, ()>) -> usize
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        behavior,
        settlements,
        control,
        user,
        descendants,
        completion,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = terminal
    else {
        panic!("the indexer must complete its explicit shutdown")
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
    behavior.base().indexed_documents
}

fn assert_journal<R>(
    terminal: ActorRetirement<ManagedJournal, ApplicationTerminal<R>, ()>,
) -> String
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        child_failures: (),
        capability_failures,
        unread_owner_cancellation,
        behavior,
        settlements,
        control,
        user,
        descendants,
        completion,
        interpretation,
        source,
        additional_failures,
        received_interpretation,
        received_source,
        source_index,
        acquired_ingress,
        retirement_failures,
        terminal_report,
    } = terminal
    else {
        panic!("the journal must complete its explicit shutdown")
    };
    assert!(interpretation.is_none());
    assert!(source.is_none());
    assert!(additional_failures.is_empty());
    assert!(received_interpretation.is_none());
    assert!(received_source.is_none());
    assert!(source_index.is_none());
    assert!(acquired_ingress.is_none());
    assert!(retirement_failures.is_empty());
    assert!(terminal_report.is_none());
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert_eq!(control.len(), 0);
    assert_eq!(user.len(), 0);
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
    behavior
        .base()
        .entries
        .first()
        .cloned()
        .expect("the journal retains the initialization entry")
}

#[cfg(test)]
mod tests {
    use bombay::behavior::Step;

    use super::*;

    #[test]
    fn parent_initialization_preserves_both_named_births_and_deliveries() {
        let initialized = DocumentSystem
            .initialize()
            .expect("document-system initialization is infallible");

        assert_eq!(initialized.actions.creates.len(), 2);
        let creation_ids = initialized
            .actions
            .creates
            .iter()
            .map(|creation| creation.id().get())
            .collect::<Vec<_>>();
        assert_eq!(creation_ids, [1, 2]);
        assert_eq!(initialized.actions.sends.indexing.len(), 1);
        assert_eq!(initialized.actions.sends.journaling.len(), 1);
        let indexing_creation = initialized.actions.sends.indexing[0].creation.get();
        let journaling_creation = initialized.actions.sends.journaling[0].creation.get();
        assert_eq!(indexing_creation, 1);
        assert_eq!(journaling_creation, 2);
        assert!(matches!(
            initialized.actions.sends.indexing[0].message,
            IndexCommand::Index(ref documents) if documents == &["contract", "ledger"]
        ));
        assert_eq!(
            initialized.actions.sends.journaling[0].message,
            JournalCommand::Record("topology initialized".to_owned())
        );
        assert_eq!(initialized.actions.become_, Step::Continue);
    }
}
