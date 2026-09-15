use std::future::Future;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, post},
};
use bombay::prelude::*;

use crate::domain::PlaceOrder;
use crate::order_book::{OrderBook, OrderMessage};

struct HttpReplies;

impl Protocol for HttpReplies {
    type Addr = MailAddr;
    type Msg = Never;
}

#[derive(Clone)]
pub(crate) struct OrderApi {
    pub(crate) orders: EstablishedRecipient<OrderBook>,
}

trait OrderCommands: Clone + Send + Sync + 'static {
    fn place(&self, order: PlaceOrder) -> impl Future<Output = Result<(), PlaceOrder>> + Send;

    fn shutdown(&self) -> impl Future<Output = Result<(), ()>> + Send;
}

#[derive(Clone)]
struct LiveOrderCommands {
    interface: ActorInterface<OrderApi>,
    lifecycle: ApplicationLifecycle<OrderBook>,
}

impl OrderCommands for LiveOrderCommands {
    async fn place(&self, order: PlaceOrder) -> Result<(), PlaceOrder> {
        let caller = self
            .interface
            .external::<HttpReplies>()
            .map_err(|_| order.clone())?;
        caller
            .send(&self.interface.api().orders, OrderMessage::Place(order))
            .await
            .map_err(|rejected| {
                let OrderMessage::Place(order) = rejected.into_message();
                order
            })
    }

    async fn shutdown(&self) -> Result<(), ()> {
        self.lifecycle.request_shutdown().map_err(|_| ())
    }
}

#[derive(Clone)]
struct ApiState<C> {
    commands: C,
}

pub(crate) fn router(
    interface: ActorInterface<OrderApi>,
    lifecycle: ApplicationLifecycle<OrderBook>,
) -> Router {
    router_with(LiveOrderCommands {
        interface,
        lifecycle,
    })
}

fn router_with<C>(commands: C) -> Router
where
    C: OrderCommands,
{
    Router::new()
        .route("/orders", post(place_order::<C>))
        .route("/admin/shutdown", delete(shutdown::<C>))
        .with_state(ApiState { commands })
}

async fn place_order<C>(
    State(state): State<ApiState<C>>,
    Json(order): Json<PlaceOrder>,
) -> Result<(StatusCode, Json<PlaceOrder>), (StatusCode, Json<PlaceOrder>)>
where
    C: OrderCommands,
{
    state
        .commands
        .place(order.clone())
        .await
        .map(|()| (StatusCode::ACCEPTED, Json(order)))
        .map_err(|rejected| (StatusCode::SERVICE_UNAVAILABLE, Json(rejected)))
}

async fn shutdown<C>(State(state): State<ApiState<C>>) -> StatusCode
where
    C: OrderCommands,
{
    match state.commands.shutdown().await {
        Ok(()) => StatusCode::ACCEPTED,
        Err(()) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    use axum::{
        body::Body,
        http::{Request, header},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::assert_application_stopped;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingCommands {
        orders: Arc<Mutex<Vec<PlaceOrder>>>,
    }

    impl OrderCommands for RecordingCommands {
        async fn place(&self, order: PlaceOrder) -> Result<(), PlaceOrder> {
            self.orders
                .lock()
                .expect("recording command lock is not poisoned")
                .push(order);
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), ()> {
            Ok(())
        }
    }

    #[test]
    fn order_route_admits_one_domain_command() {
        let commands = RecordingCommands::default();
        let recorded = commands.orders.clone();
        let request = PlaceOrder {
            order_id: 41,
            customer_id: 7,
            sku: "COFFEE-1KG".into(),
            quantity: 2,
        };
        let body = serde_json::to_vec(&request).expect("request serializes");

        let response = tokio_test(
            router_with(commands).oneshot(
                Request::post("/orders")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request builds"),
            ),
        )
        .expect("router is infallible");

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            *recorded.lock().expect("recording lock is not poisoned"),
            vec![request]
        );
        let response_body = tokio_test(response.into_body().collect())
            .expect("response body collects")
            .to_bytes();
        assert!(!response_body.is_empty());
    }

    #[derive(Clone)]
    struct RejectingCommands;

    impl OrderCommands for RejectingCommands {
        async fn place(&self, order: PlaceOrder) -> Result<(), PlaceOrder> {
            Err(order)
        }

        async fn shutdown(&self) -> Result<(), ()> {
            Err(())
        }
    }

    #[test]
    fn rejected_admission_returns_the_exact_order() {
        let request = PlaceOrder {
            order_id: 73,
            customer_id: 12,
            sku: "TEA-EARL-GREY".into(),
            quantity: 1,
        };
        let body = serde_json::to_vec(&request).expect("request serializes");
        let response = tokio_test(
            router_with(RejectingCommands).oneshot(
                Request::post("/orders")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .expect("request builds"),
            ),
        )
        .expect("router is infallible");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response_body = tokio_test(response.into_body().collect())
            .expect("response body collects")
            .to_bytes();
        let rejected: PlaceOrder =
            serde_json::from_slice(&response_body).expect("rejected order deserializes");
        assert_eq!(rejected, request);
    }

    #[test]
    #[allow(
        clippy::result_large_err,
        reason = "the exact Axum error retains application terminal custody without boxing"
    )]
    fn live_http_flow_reaches_the_root_and_shutdowns_it() {
        let address = TcpListener::bind("127.0.0.1:0")
            .expect("a free test port is available")
            .local_addr()
            .expect("the test port has an address");
        let (ready, started) = mpsc::channel();
        let server = thread::spawn(move || {
            Application::new(OrderBook::default().stop_on_shutdown()).run_axum(
                address,
                move |application| {
                    ready
                        .send(())
                        .expect("the test server receiver remains live");
                    let interface = application.interface(OrderApi {
                        orders: application.root().established_recipient(),
                    });
                    router(interface, application.lifecycle())
                },
            )
        });

        started
            .recv_timeout(Duration::from_secs(2))
            .expect("the live actor and Axum router start");

        let order = PlaceOrder {
            order_id: 101,
            customer_id: 8,
            sku: "COFFEE-FLOW".into(),
            quantity: 1,
        };
        let body = serde_json::to_string(&order).expect("order serializes");
        let accepted = raw_http(address, "POST", "/orders", &body);
        assert!(accepted.starts_with("HTTP/1.1 202 Accepted"));

        let stopped = raw_http(address, "DELETE", "/admin/shutdown", "");
        assert!(stopped.starts_with("HTTP/1.1 202 Accepted"));

        let result = server.join().expect("the server thread joins");
        let terminal = result.expect("the live application must exit normally");
        assert_application_stopped(terminal);
    }

    fn raw_http(address: SocketAddr, method: &str, path: &str, body: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("the live Axum listener accepts");
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("the response timeout is configured");
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .expect("the HTTP request writes");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("the HTTP response reads");
        String::from_utf8(response).expect("the HTTP response is UTF-8")
    }

    fn tokio_test<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime builds")
            .block_on(future)
    }
}
