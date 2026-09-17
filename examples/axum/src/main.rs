//! For integrating one typed Bombay root with Axum while keeping HTTP and I/O
//! outside the Level-1 Behavior. Axum owns extraction and responses; Bombay
//! owns local activation, delivery, exact rejection recovery, and termination.

mod domain;
mod http;
mod order_book;

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bombay::behavior::{ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

type OrderBookRoot = StopOnShutdown<order_book::OrderBook>;

#[derive(TerminalProjection)]
pub(crate) enum OrderBookTerminal {
    Root {
        origin: ActorOrigin<OrderBookRoot>,
        terminal: ActorRetirement<OrderBookRoot, Self>,
    },
}

impl fmt::Debug for OrderBookTerminal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OrderBookTerminal")
    }
}

#[allow(
    clippy::result_large_err,
    reason = "the exact Axum error retains application terminal custody without boxing"
)]
fn main() -> Result<(), AxumRunError<OrderBookTerminal>> {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
    let terminal = Application::new(order_book::OrderBook::default().stop_on_shutdown()).run_axum(
        address,
        |application| {
            let interface = application.interface(http::OrderApi {
                orders: application.root().established_recipient(),
            });
            http::router(interface, application.lifecycle())
        },
    )?;
    assert_application_stopped(terminal);
    Ok(())
}

pub(crate) fn assert_application_stopped(terminal: OrderBookTerminal) {
    let OrderBookTerminal::Root {
        origin,
        terminal:
            ActorRetirement::Completed {
                behavior,
                settlements,
                control,
                user,
                descendants,
                completion,
            },
    } = terminal
    else {
        panic!("the application must preserve the root's completed terminal state")
    };
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    assert_eq!(origin.nonce(), None);
    drop(behavior);
    let settlement_status = settlements.settlement_status();
    assert_eq!(settlement_status, SettlementStatus::Accepted);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert!(descendants.is_empty());
    assert_eq!(completion, Completion::Stopped);
}
