//! Runtime text and checked actor addresses come from their existing owners.
use super::custody::completed_actor;
use super::protocol::{EndpointRole, configuration, reserved_address};
use super::{Counter, counter::CounterValue};
use crate::actors::ActorExt;
use crate::remote::ExportError;
use crate::{Application, RetirementAssessment};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_identity_matches_its_owner_before_and_after_close() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let address = reserved_address();
        let mut first = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("actual first Node opens"));
        let address = reserved_address();
        let mut second = Application::new(Counter::new().stop_on_shutdown())
            .open_node(configuration(&address, &EndpointRole::Receiver))
            .await
            .unwrap_or_else(|_| panic!("actual second Node opens"));
        let first_owner = first.session.zid().to_string();
        let second_owner = second.session.zid().to_string();
        let first_id = first.runtime_id();
        let second_id = second.runtime_id();
        let first_closed = first.close().await;
        let second_closed = second.close().await;
        let first_after_close = first.runtime_id();
        let second_after_close = second.runtime_id();
        first_closed.expect("actual first connection close");
        second_closed.expect("actual second connection close");
        assert_eq!(first_id, first_owner);
        assert_eq!(second_id, second_owner);
        assert_ne!(first_id, second_id);
        assert_eq!(first_after_close, first_owner);
        assert_eq!(second_after_close, second_owner);
    })
    .await
    .expect("finite identity projection and both native closes");
}

#[tokio::test(flavor = "current_thread")]
async fn checked_actor_address_refuses_numeric_collision_and_reply_endpoint() {
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        Application::new(Counter::new().stop_on_shutdown())
            .run_with(|first| async move {
                let foreign = first.root().established_recipient();
                let first_address = first.root().address();
                let second = Application::new(Counter::new().stop_on_shutdown())
                    .run_with(|second| async move {
                        let target = second.root().established_recipient();
                        let address = second.root().address();
                        let external = second
                            .external::<CounterValue>()
                            .expect("actual reply endpoint in the same allocation namespace");
                        let own = second.actor_address(&target);
                        let foreign = second.actor_address(&foreign);
                        let reply = second.actor_address(&external.recipient());
                        let shutdown = second.request_shutdown();
                        let retirement = second.retirement().await;
                        let after_retirement = second.actor_address(&target);
                        (
                            address,
                            own,
                            after_retirement,
                            foreign,
                            reply,
                            shutdown,
                            retirement,
                        )
                    })
                    .await
                    .unwrap_or_else(|_| panic!("the second actual host accepts execution"));
                let shutdown = first.request_shutdown();
                let retirement = first.retirement().await;
                (first_address, second, shutdown, retirement)
            })
            .await
            .unwrap_or_else(|_| panic!("the first actual host accepts execution"))
    })
    .await
    .expect("both actual applications join without a timing delay");
    let (work, first_native, first_notifications, ()) = outcome.into_parts();
    let ((first_address, second, first_shutdown, first_retirement), first_cleanup) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("first actual Work completes"));
    let (work, second_native, second_notifications, ()) = second.into_parts();
    let (
        (second_address, own, after_retirement, foreign, reply, second_shutdown, second_retirement),
        second_cleanup,
    ) = work
        .try_into_completed()
        .unwrap_or_else(|_| panic!("second actual Work completes"));
    let (first_origin, first_native) = first_native.expect("first original root retirement");
    let (second_origin, second_native) = second_native.expect("second original root retirement");
    let first_notifications = first_notifications.expect("first notification originals");
    let second_notifications = second_notifications.expect("second notification originals");
    first_cleanup.expect("first ordinary cleanup");
    second_cleanup.expect("second ordinary cleanup");
    first_notifications
        .termination
        .expect("first termination publication");
    first_notifications
        .retirement
        .expect("first joined retirement publication");
    second_notifications
        .termination
        .expect("second termination publication");
    second_notifications
        .retirement
        .expect("second joined retirement publication");
    let actual_first = first_origin.address();
    let actual_second = second_origin.address();
    let _first = completed_actor(first_origin, first_native);
    let _second = completed_actor(second_origin, second_native);
    assert_eq!(
        first_retirement.retirement(),
        RetirementAssessment::Established
    );
    assert_eq!(
        second_retirement.retirement(),
        RetirementAssessment::Established
    );
    assert!(first_shutdown.is_ok() && second_shutdown.is_ok());
    assert_eq!(first_address, actual_first);
    assert_eq!(second_address, actual_second);
    assert_eq!(actual_first, actual_second);
    let own = own.unwrap_or_else(|_| panic!("actual associated root address"));
    let after_retirement =
        after_retirement.unwrap_or_else(|_| panic!("association survives actual actor retirement"));
    assert_eq!(own, actual_second);
    assert_eq!(after_retirement, actual_second);
    assert!(matches!(foreign, Err(ExportError::UnownedTarget)));
    assert!(matches!(reply, Err(ExportError::UnownedTarget)));
}
