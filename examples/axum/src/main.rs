//! For integrating one typed Bombay root with Axum while keeping HTTP and I/O
//! outside the Level-1 Behavior. Axum owns extraction and responses; Bombay
//! owns local activation, delivery, exact rejection recovery, and termination.

use bombay::{ActorCustody, ActorNotificationReceipts, ApplicationOutcome};
use tokio::runtime::Builder;
mod domain;
mod http;
mod order_book;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
    let application_host = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the synchronous HTTP caller owns its enabled current-thread host");
    let application_outcome = application_host
        .block_on(
            Application::new(order_book::OrderBook::default().stop_on_shutdown()).run_axum(
                address,
                |application| {
                    let interface =
                        application.interface(application.root().established_recipient());
                    http::router(interface, application.lifecycle())
                },
            ),
        )
        .unwrap_or_else(|failed| {
            drop(failed);
            panic!("the explicit HTTP host is entered");
        });
    drop(application_host);
    if let (
        ApplicationOutcome::Completed {
            output: _,
            cleanup: Ok(()),
        },
        Ok((_, ActorRetirement::ActorTaskFailed(_))),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = &application_outcome
    {
        panic!("the exact serving result and independently joined root remain complete");
    }
    let (
        ApplicationOutcome::Completed {
            output: Ok(()),
            cleanup: Ok(()),
        },
        Ok((origin, retirement)),
        Ok(ActorNotificationReceipts {
            termination: Ok(()),
            retirement: Ok(()),
        }),
    ) = application_outcome
    else {
        panic!("the exact serving result and independently joined root remain complete");
    };
    let terminal = (origin, retirement);
    assert_application_stopped(terminal);
}

pub(crate) fn assert_application_stopped<R>(
    terminal: (RootOrigin<R>, ActorRetirement<R, Never, Vec<Never>>),
) where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
    R::Settlements: ClassifySettlement,
{
    let (
        origin,
        ActorRetirement::Completed {
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
        },
    ) = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(child_failures, []);
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
    assert_eq!(descendants, []);
    assert_eq!(completion, Completion::Stopped);
}
