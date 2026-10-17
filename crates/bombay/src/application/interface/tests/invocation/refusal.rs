//! An explicit startup refusal retains Work before owner cancellation and join.
use super::ledger_custody;
use crate::actors::ActorExt;
use crate::application::execution::ApplicationWorkPresence;
use crate::application::work::invoke;
use crate::behavior::{Behavior, BehaviorBase, Never};
use crate::{ActorRetirement, Application, ApplicationHandle, ShutdownLedger, WorkOutcome};
use behavior_actors::StopOnShutdown;
use core::future::{Future, Ready, poll_fn, ready};
use core::pin::pin;
use core::task::Poll;
use std::cell::Cell;
use std::ptr;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one refusal returns exact Work and export presence with complete joined outcomes"
)]
async fn refused_binding_returns_original_work_after_joined_cancellation() {
    let calls = Rc::new(Cell::new(0));
    let used = Rc::clone(&calls);
    let original = Box::new(71_u64);
    let output_allocation = ptr::from_ref(original.as_ref());
    let work = Box::new(
        move |_application: ApplicationHandle<
            <ShutdownLedger as Behavior>::Protocol,
            <StopOnShutdown<ShutdownLedger> as Behavior>::Event,
        >| {
            used.set(used.get() + 1);
            ready(original)
        },
    );
    let work_allocation = ptr::from_ref(work.as_ref()).cast::<()>();
    let bindings = Cell::new(0);
    let mut handoff = None;
    let observations = timeout(Duration::from_secs(10), async {
        let (publication, receiving) = oneshot::channel();
        let published = &mut handoff;
        let bound = &bindings;
        let application = Application::new(
            ShutdownLedger {
                entries: vec![13],
                received: Vec::new(),
            }
            .stop_on_shutdown(),
        );
        let (execution, (received, received_families)) = application
            .execute_with_entities_invocation(
                ApplicationWorkPresence::<_, Never>::Supplied((work, invoke)),
                Some(
                    move |application: &ApplicationHandle<
                        <ShutdownLedger as Behavior>::Protocol,
                        <StopOnShutdown<ShutdownLedger> as Behavior>::Event,
                    >| {
                        bound.set(bound.get() + 1);
                        *published = Some(publication.send(application.clone()));
                        ready(Err::<Option<Ready<()>>, ()>(()))
                    },
                ),
            )
            .unwrap_or_else(|_| panic!("the entered host issues the actual application"));
        let mut execution = pin!(execution);
        let refused = poll_fn(|context| {
            let observed = execution.as_mut().poll(context);
            if bindings.get() == 1 {
                Poll::Ready(observed)
            } else {
                match observed {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => panic!("execution cannot finish before its invocation"),
                }
            }
        })
        .await;
        let application = receiving.await;
        let fallback = application
            .as_ref()
            .ok()
            .map(ApplicationHandle::request_shutdown);
        match refused {
            Poll::Pending => execution.await,
            Poll::Ready(()) => {}
        }
        let (work, root, notifications) = received.await;
        let work = work.into_supplied_with_entities();
        let families = received_families.await;
        (
            work,
            root,
            notifications,
            families,
            application,
            fallback,
            refused,
        )
    })
    .await
    .expect("startup, owning cancellation and joined cleanup watchdog; never an inverse oracle");

    // Acquire every independent result before any semantic assertion.
    let (outcome, root, notifications, (), application, fallback, refused) = observations;
    let (origin, native) = root.expect("the actual joined root remains available");
    let notifications = notifications.expect("both actual notification receipts remain available");
    let application = application.expect("the original live root capability was transferred");
    assert_eq!(origin.address(), application.root().address());
    assert!(notifications.termination.is_ok() && notifications.retirement.is_ok());
    assert!(matches!(&native, ActorRetirement::OwnerCancelled { .. }));
    assert!(refused.is_pending());
    assert!(matches!(fallback, Some(Ok(()))));
    let custody = ledger_custody(&native);
    assert_eq!(custody.behavior.base().entries, [13]);
    assert!(custody.behavior.base().received.is_empty() && custody.user.is_empty());
    assert_eq!(bindings.get(), 1);
    assert_eq!(calls.get(), 0);
    assert!(matches!(&handoff, Some(Ok(()))));
    assert!(
        matches!(
            &outcome,
            WorkOutcome::NotInvoked {
                startup_error: None,
                cleanup: Ok(()),
                ..
            }
        ),
        "a refused binding returns untouched Work after actual joined cleanup"
    );
    let WorkOutcome::NotInvoked { work, .. } = outcome else {
        unreachable!("the complete typed outcome was inspected above");
    };
    let allocation = ptr::from_ref(work.as_ref()).cast::<()>();
    assert_eq!(allocation, work_allocation);
    let callable = *work;
    let output = callable(application).await;
    let allocation = ptr::from_ref(output.as_ref());
    assert_eq!(allocation, output_allocation);
    assert_eq!(*output, 71);
    assert_eq!(calls.get(), 1);
}
