use bombay::behavior::{Children, CreationSequence};
use bombay::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum IndexCommand {
    Index(Vec<String>),
}

#[derive(Default)]
pub(super) struct Indexer {
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
pub(super) enum JournalCommand {
    Record(String),
}

#[derive(Default)]
pub(super) struct Journal {
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

pub(super) struct DocumentSystem;

#[bombay::actor(
    message = Never,
    sends = pub(crate) {
        indexing: Vec<ChildDelivery<Indexer, DocumentSystemChildrenIndexer>>,
        journaling: Vec<ChildDelivery<Journal, DocumentSystemChildrenJournal>>,
    },
    births = {
        indexer: StopOnShutdown<Indexer>,
        journal: StopOnShutdown<Journal>,
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

bombay::topology! {
    pub(super) document_application {
        root: DocumentSystem {
            Indexer(DocumentSystemChildrenIndexer) => indexer;
            Journal(DocumentSystemChildrenJournal) => journal;
        }
        indexer {}
        journal {}
    }
}
