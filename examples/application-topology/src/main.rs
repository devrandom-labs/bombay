//! Create two heterogeneous children and shut them down in declared phases.

mod documents;

use bombay::Application;
use bombay::lifecycle::shutdown_after_children;
use documents::{DocumentSystem, DocumentSystemChild, document_application};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let root = shutdown_after_children(DocumentSystem)
        .shutdown_phase(DocumentSystemChild::Indexer)
        .shutdown_phase(DocumentSystemChild::Journal)
        .finish();
    let outcome = Application::with_topology(root, document_application::Topology::new())
        .run_with(|application| async move {
            application.shutdown().expect("phased shutdown admitted");
            application.retirement().await
        })
        .await
        .unwrap_or_else(|_| panic!("the document application starts"));
    let (report, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the document work completes"));
    println!("actor retirement: {report:?}");
    // Execution already performed cleanup; these reports are optional inspection.
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
}
