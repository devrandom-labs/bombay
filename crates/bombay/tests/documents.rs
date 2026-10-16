//! Complete initialization and retirement laws for the actual heterogeneous document system.

mod documents {
    //! For learning a parent-owned application topology with two heterogeneous
    //! named children, same-action child delivery, and creation-dependent phased
    //! shutdown. Behavior owns the closed roles and routes, Behavior Actors owns
    //! the shutdown plan, and Bombay only interprets and retains the result.

    use bombay::WorkOutcome;
    use bombay::behavior::{
        BehaviorBase, BehaviorSettlements, ClassifySettlement, Never, SettlementStatus,
    };
    use bombay::lifecycle::shutdown_after_children;
    use bombay::{ActorCustody, ActorNotificationReceipts};
    use tokio::runtime::Builder;

    const INDEXER_NONCE: u64 = 0;
    const JOURNAL_NONCE: u64 = 1;

    include!("../../../examples/application-topology/src/documents.rs");

    #[test]
    fn named_children_keep_their_complete_retirements() {
        let application = shutdown_after_children(DocumentSystem)
            .shutdown_phase(DocumentSystemChild::Indexer)
            .shutdown_phase(DocumentSystemChild::Journal)
            .finish();
        let application_host = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("the synchronous caller owns its explicit current-thread host");
        let application_outcome = application_host
            .block_on(
                Application::with_topology(application, document_application::Topology::new())
                    .run_with(|application| async move {
                        let shutdown = application.request_shutdown();
                        assert_eq!(shutdown, Ok(()));
                        application.termination().await
                    }),
            )
            .unwrap_or_else(|failed| {
                drop(failed);
                panic!("the named child topology activates and shuts down in declared phase order");
            })
            .into_parts();
        drop(application_host);
        let (
            WorkOutcome::Completed {
                output: termination,
                cleanup: Ok(()),
            },
            Ok((origin, retirement)),
            Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }),
            (),
        ) = application_outcome
        else {
            panic!("the original completed Work and joined root remain independently owned");
        };

        assert_eq!(termination, Ok(Exit::Normal));
        let retirement = match retirement {
            ActorRetirement::ActorTaskFailed(failure) => {
                panic!("the original named child topology joined: {failure}")
            }
            retirement => retirement,
        };
        assert_terminal_tree(origin, retirement);
    }

    fn assert_terminal_tree<R>(
        origin: RootOrigin<R>,
        terminal: ActorRetirement<
            R,
            document_application::RootRetirements,
            Vec<document_application::RootChildFailure>,
        >,
    ) where
        R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
        R::Settlements: ClassifySettlement,
    {
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    control,
                    user,
                    descendants,
                    behavior,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            settlements,
            completion,
            interpretation,
            source,
        } = terminal
        else {
            panic!("phased shutdown must preserve the completed application root")
        };
        assert_eq!(child_failures.len(), 0);
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
                document_application::RootRetirements::Indexer {
                    origin,
                    retirement: terminal,
                } => {
                    let indexed_documents = assert_indexer(*terminal);
                    let previous_indexer = indexer.replace(indexed_documents);
                    assert_eq!(previous_indexer, None);
                    assert_eq!(origin.nonce(), INDEXER_NONCE);
                }
                document_application::RootRetirements::Journal {
                    origin,
                    retirement: terminal,
                } => {
                    let journal_entry = assert_journal(*terminal);
                    let previous_journal = journal.replace(journal_entry);
                    assert_eq!(previous_journal, None);
                    assert_eq!(origin.nonce(), JOURNAL_NONCE);
                }
            }
        }
        assert_eq!(indexer, Some(2));
        assert_eq!(journal.as_deref(), Some("topology initialized"));
    }

    fn assert_indexer(
        terminal: ActorRetirement<StopOnShutdown<Indexer>, Never, Vec<Never>>,
    ) -> usize {
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    behavior,
                    control,
                    user,
                    descendants,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            settlements,
            completion,
            interpretation,
            source,
        } = terminal
        else {
            panic!("the indexer must complete its explicit shutdown")
        };
        assert_eq!(child_failures.len(), 0);
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
        assert_eq!(descendants, []);
        assert_eq!(completion, Completion::Stopped);
        behavior.base().indexed_documents
    }

    fn assert_journal(
        terminal: ActorRetirement<StopOnShutdown<Journal>, Never, Vec<Never>>,
    ) -> String {
        let ActorRetirement::Completed {
            custody:
                ActorCustody {
                    child_failures,
                    capability_failures,
                    unread_owner_cancellation,
                    behavior,
                    control,
                    user,
                    descendants,
                    additional_failures,
                    received_interpretation,
                    received_source,
                    source_index,
                    acquired_ingress,
                    retirement_failures,
                    terminal_report,
                },
            settlements,
            completion,
            interpretation,
            source,
        } = terminal
        else {
            panic!("the journal must complete its explicit shutdown")
        };
        assert_eq!(child_failures.len(), 0);
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
        assert_eq!(descendants, []);
        assert_eq!(completion, Completion::Stopped);
        behavior
            .base()
            .entries
            .first()
            .cloned()
            .expect("the journal retains the initialization entry")
    }

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
}
