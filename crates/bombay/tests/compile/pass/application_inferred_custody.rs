//! The caller names domain roles and owns every retirement without a terminal enum.


use bombay::ActorCustody;
use bombay::actors::ActorExt;
use bombay::behavior::{BehaviorBase, Never};
use bombay::{ActorRetirement, Application, ChildRetirementSelection};

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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let entries: Box<[u8]> = Box::from([3, 7]);
    let allocation = entries.as_ptr();
    let outcome = Application::new(Counter::new().stop_on_shutdown())
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
custody: ActorCustody {
descendants,
..
},
..
} = retirement else {
        panic!("root completed after its real counter reply");
    };
    assert_eq!(descendants.len(), 3);
    let mut audit_addresses = Vec::new();
    let mut primary_addresses = Vec::new();
    let mut replica_addresses = Vec::new();
    for report in descendants {
        let audit_before = report
            .child_retirement(&Audit)
            .map(|audit| (audit.origin, core::ptr::from_ref(&*audit.retirement)));
        let primary_before = report
            .child_retirement(&Primary)
            .map(|primary| (primary.origin, core::ptr::from_ref(&*primary.retirement)));
        let replica_before = report
            .child_retirement(&Replica)
            .map(|replica| (replica.origin, core::ptr::from_ref(&*replica.retirement)));
        if let Some(audit) = report.child_retirement(&Audit) {
            let ActorRetirement::OwnerCancelled {
custody: ActorCustody {
behavior,
..
},
..
} = &*audit.retirement else {
                panic!("root retirement cancelled the audit child");
            };
            assert_eq!(behavior.base().entries.as_ptr(), allocation);
            assert_eq!(&*behavior.base().entries, &[3, 7]);
        }
        let report = match report.try_into_child_retirement(&Audit) {
            Ok(audit) => {
                assert_eq!(
                    audit_before,
                    Some((audit.origin, core::ptr::from_ref(&*audit.retirement)))
                );
                assert!(primary_before.is_none());
                assert!(replica_before.is_none());
                audit_addresses.push(audit.origin.address());
                let ActorRetirement::OwnerCancelled {
custody: ActorCustody {
behavior,
..
},
..
} = *audit.retirement else {
                    panic!("audit outcome is preserved by consuming selection");
                };
                let entries = behavior.into_inner().entries;
                assert_eq!(entries.as_ptr(), allocation);
                assert_eq!(&*entries, &[3, 7]);
                continue;
            }
            Err(original) => {
                assert!(audit_before.is_none());
                assert_eq!(
                    primary_before,
                    original.child_retirement(&Primary).map(|primary| {
                        (primary.origin, core::ptr::from_ref(&*primary.retirement))
                    })
                );
                assert_eq!(
                    replica_before,
                    original.child_retirement(&Replica).map(|replica| {
                        (replica.origin, core::ptr::from_ref(&*replica.retirement))
                    })
                );
                original
            }
        };
        let report = match report.try_into_child_retirement(&Primary) {
            Ok(primary) => {
                assert_eq!(
                    primary_before,
                    Some((primary.origin, core::ptr::from_ref(&*primary.retirement)))
                );
                assert!(replica_before.is_none());
                primary_addresses.push(primary.origin.address());
                let ActorRetirement::OwnerCancelled { .. } = *primary.retirement else {
                    panic!("primary cancellation remains exact");
                };
                continue;
            }
            Err(original) => original,
        };
        let replica = report
            .try_into_child_retirement(&Replica)
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the unmatched report is the replica");
            });
        assert!(primary_before.is_none());
        assert_eq!(
            replica_before,
            Some((replica.origin, core::ptr::from_ref(&*replica.retirement)))
        );
        replica_addresses.push(replica.origin.address());
        let ActorRetirement::OwnerCancelled { .. } = *replica.retirement else {
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
