use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, post},
};
use bombay::behavior::Behavior;
use bombay::prelude::*;

use crate::domain::PlaceOrder;
use crate::order_book::{OrderBook, OrderMessage};

type OrderApplication =
    ApplicationHandle<OrderBook, <StopOnShutdown<OrderBook> as Behavior>::Event>;

pub(crate) fn router(application: OrderApplication) -> Router {
    Router::new()
        .route("/orders", post(place_order))
        .route("/admin/shutdown", delete(shutdown))
        .with_state(application)
}

async fn place(application: &OrderApplication, order: PlaceOrder) -> Result<(), PlaceOrder> {
    let sender = match application.sender() {
        Ok(sender) => sender,
        Err(_allocation) => return Err(order),
    };
    sender
        .send(OrderMessage::Place(order))
        .await
        .map_err(|rejected| {
            let OrderMessage::Place(order) = rejected.into_message();
            order
        })
}

async fn place_order(
    State(application): State<OrderApplication>,
    Json(order): Json<PlaceOrder>,
) -> Result<(StatusCode, Json<PlaceOrder>), (StatusCode, Json<PlaceOrder>)> {
    place(&application, order.clone())
        .await
        .map(|()| (StatusCode::ACCEPTED, Json(order)))
        .map_err(|rejected| (StatusCode::SERVICE_UNAVAILABLE, Json(rejected)))
}

async fn shutdown(State(application): State<OrderApplication>) -> StatusCode {
    match application.request_shutdown() {
        Ok(()) => StatusCode::ACCEPTED,
        Err(_rejection) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
