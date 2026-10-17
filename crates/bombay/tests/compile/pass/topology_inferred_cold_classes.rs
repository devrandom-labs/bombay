use bombay::actors::ActorExt;
use bombay::behavior::Never;

struct Root;
#[bombay::actor(message = Never)]
impl Root {}
struct Background;
struct Worker {
    original: Box<[u8]>,
}
#[bombay::actor(message = Never)]
impl Worker {}

bombay::topology! {
    application {
        root: Root {}
        members { Background(Background) => worker; }
        worker {}
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let original: Box<[u8]> = Box::from([7, 11]);
    let allocation = original.as_ptr();
    let (outcome, native, notifications, ()) = bombay::Application::with_topology(Root.stop_on_shutdown(), application::Topology::new())
        .child(Background, Worker { original }.stop_on_shutdown())
        .run_with(|application| async move {
            application.shutdown().expect("shutdown admitted");
        }).await.unwrap_or_else(|original| { drop(original); panic!("host entered"); }) .into_parts();
    let ((), cleanup) = outcome.try_into_completed().unwrap_or_else(|original| { drop(original); panic!("work returned"); });
    cleanup.expect("the actual cleanup producer completed independently");
    let notifications = notifications.expect("both original notification receipts survive");
    notifications.termination.expect("actual ordinary termination notification");
    notifications.retirement.expect("actual joined retirement notification");
    let (_, retirement) = native.expect("root acquired");
    let bombay::ActorRetirement::Completed {
        custody: bombay::ActorCustody { descendants, child_failures, .. }, ..
    } = retirement else { panic!("root completed"); };
    assert!(child_failures.is_empty());
    let [application::RootRetirements { retirement, .. }] = <[_; 1]>::try_from(descendants).unwrap_or_else(|original| { drop(original); panic!("original child"); });
    let bombay::ActorRetirement::OwnerCancelled {
        custody: bombay::ActorCustody { behavior, descendants, child_failures, .. }, ..
    } = *retirement else { panic!("child cancellation custody"); };
    assert!(descendants.is_empty());
    assert!(child_failures.is_empty());
    assert_eq!(behavior.into_inner().original.as_ptr(), allocation);
}
