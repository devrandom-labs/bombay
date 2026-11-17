use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, post},
};
use bombay::prelude::*;

use crate::domain::PlaceOrder;
use crate::order_book::{OrderBook, OrderMessage};

type OrderInterface = ActorInterface<EstablishedRecipient<OrderBook>>;

pub(crate) fn router<Shutdown>(orders: OrderInterface, shutdown: Shutdown) -> Router
where
    Shutdown: Fn() -> Result<(), ShutdownRejection> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/orders", post(place_order).with_state(orders))
        .route(
            "/admin/shutdown",
            delete(move || {
                let shutdown = shutdown.clone();
                async move {
                    match shutdown() {
                        Ok(()) => StatusCode::ACCEPTED,
                        Err(_rejection) => StatusCode::SERVICE_UNAVAILABLE,
                    }
                }
            }),
        )
}

async fn place(orders: &OrderInterface, order: PlaceOrder) -> Result<(), PlaceOrder> {
    let sender = match orders.sender_to(orders.api().clone()) {
        Ok(sender) => sender,
        Err((_target, _allocation)) => return Err(order),
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
    State(orders): State<OrderInterface>,
    Json(order): Json<PlaceOrder>,
) -> Result<(StatusCode, Json<PlaceOrder>), (StatusCode, Json<PlaceOrder>)> {
    place(&orders, order.clone())
        .await
        .map(|()| (StatusCode::ACCEPTED, Json(order)))
        .map_err(|rejected| (StatusCode::SERVICE_UNAVAILABLE, Json(rejected)))
}
