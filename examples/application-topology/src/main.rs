//! For learning a parent-owned application topology with two heterogeneous
//! named children, same-action child delivery, and creation-dependent phased
//! shutdown. Behavior owns the closed roles and routes, Behavior Actors owns
//! the shutdown plan, and Bombay only interprets and retains the result.

use bombay::behavior::{Behavior, BehaviorBase, Children, Never, shutdown_after_children};
use bombay::prelude::*;

const INDEXER_NONCE: u64 = 11;
const JOURNAL_NONCE: u64 = 12;

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
    sends = {
        indexing: Vec<ChildDelivery<Indexer, DocumentSystemChildrenIndexer>>,
        journaling: Vec<ChildDelivery<Journal, DocumentSystemChildrenJournal>>,
    },
    births = {
        indexer: ManagedIndexer,
        journal: ManagedJournal,
    },
)]
impl DocumentSystem {
    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        reason = "the generated foundational fold fixes the controlled-error boundary"
    )]
    fn init(&mut self) -> BehaviorActed<Self> {
        let routes = DocumentSystemChildrenRoutes::new(INDEXER_NONCE, JOURNAL_NONCE);
        let children = Children::new()
            .child_at(routes.indexer, Indexer::default().stop_on_shutdown())
            .child_at(routes.journal, Journal::default().stop_on_shutdown())
            .into_creates()
            .expect("the document-system child nonces are distinct");
        Ok(Actions::create(children)
            .send_indexing(ChildDelivery::at(
                routes.indexer,
                IndexCommand::Index(vec!["contract".to_owned(), "ledger".to_owned()]),
            ))
            .send_journaling(ChildDelivery::at(
                routes.journal,
                JournalCommand::Record("topology initialized".to_owned()),
            )))
    }
}

#[derive(TerminalProjection)]
enum ApplicationTerminal<R>
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    Root {
        origin: ActorOrigin<R>,
        terminal: ActorRetirement<R, Self>,
    },
    Indexer {
        origin: ActorOrigin<DocumentSystem, DocumentSystemChildrenIndexer>,
        terminal: ActorRetirement<ManagedIndexer, Self>,
    },
    Journal {
        origin: ActorOrigin<DocumentSystem, DocumentSystemChildrenJournal>,
        terminal: ActorRetirement<ManagedJournal, Self>,
    },
}

fn main() {
    let application = shutdown_after_children(DocumentSystem)
        .shutdown_phase(DocumentSystemChild::Indexer)
        .shutdown_phase(DocumentSystemChild::Journal)
        .finish();
    let (termination, terminal): (_, ApplicationTerminal<_>) = Application::new(application)
        .run_with(|application| async move {
            let lifecycle = application.lifecycle();
            assert_eq!(lifecycle.request_shutdown(), Ok(()));
            lifecycle.termination().await
        })
        .expect("the named child topology activates and shuts down in declared phase order");

    assert_eq!(termination, Ok(Exit::Normal));
    assert_terminal_tree(terminal);
}

fn assert_terminal_tree<R>(terminal: ApplicationTerminal<R>)
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ApplicationTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                control,
                user,
                descendants,
                completion,
                ..
            },
    } = terminal
    else {
        panic!("phased shutdown must preserve the completed application root")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(completion, Completion::Stopped);
    assert_eq!(descendants.len(), 2);

    let mut indexer = None;
    let mut journal = None;
    for descendant in descendants {
        match descendant {
            ApplicationTerminal::Indexer { origin, terminal } => {
                assert_eq!(indexer.replace(assert_indexer(terminal)), None);
                assert_eq!(origin.nonce(), Some(INDEXER_NONCE));
            }
            ApplicationTerminal::Journal { origin, terminal } => {
                assert_eq!(journal.replace(assert_journal(terminal)), None);
                assert_eq!(origin.nonce(), Some(JOURNAL_NONCE));
            }
            ApplicationTerminal::Root { .. } => {
                panic!("a child retirement cannot project as another root")
            }
        }
    }
    assert_eq!(indexer, Some(2));
    assert_eq!(journal.as_deref(), Some("topology initialized"));
}

fn assert_indexer<R>(terminal: ActorRetirement<ManagedIndexer, ApplicationTerminal<R>>) -> usize
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        behavior,
        control,
        user,
        descendants,
        completion,
    } = terminal
    else {
        panic!("the indexer must complete its explicit shutdown")
    };
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
    behavior.base().indexed_documents
}

fn assert_journal<R>(terminal: ActorRetirement<ManagedJournal, ApplicationTerminal<R>>) -> String
where
    R: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let ActorRetirement::Completed {
        behavior,
        control,
        user,
        descendants,
        completion,
    } = terminal
    else {
        panic!("the journal must complete its explicit shutdown")
    };
    assert!(control.is_empty());
    assert!(user.is_empty());
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
    use bombay::behavior::{Activate as _, Step};

    use super::*;

    #[test]
    fn parent_initialization_preserves_both_named_births_and_deliveries() {
        let initialized = DocumentSystem
            .initialize()
            .expect("document-system initialization is infallible");

        assert_eq!(initialized.actions.creates.len(), 2);
        assert_eq!(initialized.actions.creates[0].nonce, INDEXER_NONCE);
        assert_eq!(initialized.actions.creates[1].nonce, JOURNAL_NONCE);
        assert_eq!(initialized.actions.sends.indexing.len(), 1);
        assert_eq!(initialized.actions.sends.journaling.len(), 1);
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
