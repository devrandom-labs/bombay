//! The caller names domain roles and owns every retirement without a terminal enum.

use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Never};
use bombay::{ActorRetirement, Application};

#[path = "../../../../../examples/counter/src/counter.rs"]
mod counter;

use counter::{Counter, CounterMessage, CounterValue};

struct Ledger {
    entries: Box<[u8]>,
}

#[bombay::actor(message = Never)]
impl Ledger {}

struct Primary;
struct Replica;
struct Audit;

bombay::topology! {
    counter_application {
        root: Counter {}
        members {
            Primary(Primary) => counter;
            Replica(Replica) => counter;
            Audit(Audit) => ledger;
        }
        counter: bombay::lifecycle::StopOnShutdown<Counter> {}
        ledger: bombay::lifecycle::StopOnShutdown<Ledger> {}
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let entries: Box<[u8]> = Box::from([3, 7]);
    let allocation = entries.as_ptr();
    let outcome = Application::with_topology(
        Counter::new().stop_on_shutdown(),
        counter_application::Topology::new(),
    )
    .child(Primary, Counter::new().stop_on_shutdown())
    .child(Replica, Counter::new().stop_on_shutdown())
    .child(Audit, Ledger { entries }.stop_on_shutdown())
    .run_with(|application| async move {
        let lifecycle = application.lifecycle();
        let interface = application.interface(application.root().established_recipient());
        let mut customer = interface
            .external::<CounterValue>()
            .expect("real reply endpoint");
        customer
            .send(interface.api(), CounterMessage::Increment)
            .await
            .expect("increment admitted");
        customer
            .send(interface.api(), CounterMessage::Read(customer.recipient()))
            .await
            .expect("read admitted");
        let reply = customer.receive().await.expect("business reply");
        lifecycle.request_shutdown().expect("shutdown is admitted");
        reply.message
    })
    .await
    .unwrap_or_else(|original| {
        drop(original);
        panic!("the caller entered a host");
    });
    let (value, cleanup) = outcome.try_into_completed().unwrap_or_else(|original| {
        drop(original);
        panic!("work must return with independent cleanup");
    });
    assert_eq!(value, 1);
    let (origin, retirement) = cleanup.expect("joined cleanup");
    assert_eq!(origin.address(), bombay::MailAddr::APPLICATION_ROOT);
    let ActorRetirement::Completed {
        custody: ActorCustody { descendants, .. },
        ..
    } = retirement
    else {
        panic!("root completed after its real counter reply");
    };
    assert_eq!(descendants.len(), 3);
    let mut audit_addresses = Vec::new();
    let mut primary_addresses = Vec::new();
    let mut replica_addresses = Vec::new();
    for report in descendants {
        let audit_before = match &report {
            counter_application::RootRetirements::Audit { origin, retirement } => {
                Some((*origin, core::ptr::from_ref(retirement.as_ref())))
            }
            _ => None,
        };
        let primary_before = match &report {
            counter_application::RootRetirements::Primary { origin, retirement } => {
                Some((*origin, core::ptr::from_ref(retirement.as_ref())))
            }
            _ => None,
        };
        let replica_before = match &report {
            counter_application::RootRetirements::Replica { origin, retirement } => {
                Some((*origin, core::ptr::from_ref(retirement.as_ref())))
            }
            _ => None,
        };
        if let counter_application::RootRetirements::Audit {
            retirement: audit_retirement,
            ..
        } = &report
        {
            let ActorRetirement::OwnerCancelled {
                custody: ActorCustody { behavior, .. },
                ..
            } = audit_retirement.as_ref()
            else {
                panic!("root retirement cancelled the audit child");
            };
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(&*behavior.base().entries, &[3, 7]);
        }
        let report = match report {
            counter_application::RootRetirements::Audit {
                origin: audit_origin,
                retirement: audit_retirement,
            } => {
                assert_eq!(
                    audit_before,
                    Some((audit_origin, core::ptr::from_ref(&*audit_retirement)))
                );
                assert!(primary_before.is_none());
                assert!(replica_before.is_none());
                audit_addresses.push(audit_origin.address());
                let ActorRetirement::OwnerCancelled {
                    custody: ActorCustody { behavior, .. },
                    ..
                } = *audit_retirement
                else {
                    panic!("audit outcome is preserved by consuming selection");
                };
                let entries = behavior.into_inner().entries;
                assert_eq!(entries.as_ptr(), allocation);
                assert_eq!(&*entries, &[3, 7]);
                continue;
            }
            original => {
                assert!(audit_before.is_none());
                assert_eq!(
                    primary_before,
                    (match &original {
                        counter_application::RootRetirements::Primary { origin, retirement } =>
                            Some((*origin, core::ptr::from_ref(retirement.as_ref()))),
                        _ => None,
                    })
                );
                assert_eq!(
                    replica_before,
                    (match &original {
                        counter_application::RootRetirements::Replica { origin, retirement } =>
                            Some((*origin, core::ptr::from_ref(retirement.as_ref()))),
                        _ => None,
                    })
                );
                original
            }
        };
        let report = match report {
            counter_application::RootRetirements::Primary {
                origin: primary_origin,
                retirement: primary_retirement,
            } => {
                assert_eq!(
                    primary_before,
                    Some((primary_origin, core::ptr::from_ref(&*primary_retirement)))
                );
                assert!(replica_before.is_none());
                primary_addresses.push(primary_origin.address());
                let ActorRetirement::OwnerCancelled { .. } = *primary_retirement else {
                    panic!("primary cancellation remains exact");
                };
                continue;
            }
            original => original,
        };
        let (replica_origin, replica_retirement) = match report {
            counter_application::RootRetirements::Replica { origin, retirement } => {
                (origin, retirement)
            }
            original => {
                drop(original);
                panic!("the unmatched report is the replica");
            }
        };
        assert!(primary_before.is_none());
        assert_eq!(
            replica_before,
            Some((replica_origin, core::ptr::from_ref(&*replica_retirement)))
        );
        replica_addresses.push(replica_origin.address());
        let ActorRetirement::OwnerCancelled { .. } = *replica_retirement else {
            panic!("replica cancellation remains exact");
        };
    }
    assert_eq!(audit_addresses.len(), 1);
    assert_eq!(primary_addresses.len(), 1);
    assert_eq!(replica_addresses.len(), 1);
    assert_ne!(audit_addresses[0], primary_addresses[0]);
    assert_ne!(audit_addresses[0], replica_addresses[0]);
    assert_ne!(primary_addresses[0], replica_addresses[0]);
}
