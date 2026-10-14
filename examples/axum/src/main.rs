//! Serve a typed order book through Axum. Bombay owns actor execution and cleanup.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bombay::prelude::*;

mod domain;
mod http;
mod order_book;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
    println!("orders: http://{address}/orders; shutdown: DELETE /admin/shutdown");
    let outcome = Application::new(order_book::OrderBook::default().stop_on_shutdown())
        .run_axum(address, http::router)
        .await
        .unwrap_or_else(|_| panic!("the HTTP application starts"));
    let (serving, retirement) = outcome
        .try_into_completed()
        .unwrap_or_else(|_| panic!("the HTTP application reaches serving"));
    match serving {
        Ok(()) => println!("server stopped"),
        Err(error) => eprintln!("server failed: {error}"),
    }
    // Bombay has already joined cleanup; these reports are optional inspection.
    println!(
        "cleanup: {:?}; notifications: {:?}",
        retirement.cleanup(),
        retirement.notifications()
    );
}
