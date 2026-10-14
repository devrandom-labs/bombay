#![cfg(feature = "axum")]

use bombay::ActorCustody;
use bombay::behavior::{BehaviorSettlements, ClassifySettlement, SettlementStatus};
use bombay::prelude::*;

#[path = "../../../examples/axum/src/domain.rs"]
mod domain;

mod order_book {
    include!("../../../examples/axum/src/order_book.rs");

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{assert_application_stopped, http::router};
        use axum::{
            body::Body,
            http::{Request, StatusCode, header},
        };
        use bombay::behavior::BehaviorBase;
        use bombay::testing::InfallibleResultExt;
        use http_body_util::BodyExt;
        use tower::ServiceExt;
        #[test]
        fn order_route_admits_one_domain_command() {
            let request = PlaceOrder {
                order_id: 41,
                customer_id: 7,
                sku: "COFFEE-1KG".into(),
                quantity: 2,
            };
            let body = serde_json::to_vec(&request).expect("request serializes");
            let host = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the caller owns its HTTP host");
            let outcome = host
                .block_on(
                    Application::new(OrderBook::default().stop_on_shutdown()).run_with(
                        |application| async move {
                            let response = router(application.clone())
                                .oneshot(
                                    Request::post("/orders")
                                        .header(header::CONTENT_TYPE, "application/json")
                                        .body(Body::from(body))
                                        .expect("request builds"),
                                )
                                .await
                                .expect("router is infallible");
                            assert_eq!(response.status(), StatusCode::ACCEPTED);
                            let response_body = response
                                .into_body()
                                .collect()
                                .await
                                .expect("response body collects")
                                .to_bytes();
                            assert!(!response_body.is_empty());
                            let admitted: PlaceOrder = serde_json::from_slice(&response_body)
                                .expect("admitted order deserializes");
                            assert_eq!(admitted, request);
                            // Give the actual actor its turn before requesting shutdown.
                            tokio::task::yield_now().await;
                            let shutdown = application.request_shutdown();
                            assert_eq!(shutdown, Ok(()));
                            request
                        },
                    ),
                )
                .unwrap_or_else(|_| panic!("the actual application starts"));
            let (request, retirement) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("HTTP admission completes"));
            assert!(retirement.cleanup().is_ok());
            let (_, root) = retirement
                .retirement()
                .as_ref()
                .expect("the original root is acquired");
            let orders = &root
                .behavior()
                .expect("the actual order book is retained")
                .base()
                .orders;
            assert_eq!(orders, &BTreeMap::from([(request.order_id, request)]));
            let (_, native, _, ()) = retirement.into_parts();
            assert_application_stopped(native.expect("the original root joins"));
        }

        #[test]
        fn placing_an_order_changes_only_that_order() {
            let mut orders = OrderBook::default().initialize().infallible().behavior;
            let order = PlaceOrder {
                order_id: 41,
                customer_id: 7,
                sku: "COFFEE-1KG".into(),
                quantity: 2,
            };

            let actions = orders
                .receive(MailAddr(1), OrderMessage::Place(order.clone()))
                .infallible();

            assert_eq!(orders.orders, BTreeMap::from([(41, order)]));
            assert!(matches!(actions.become_, Step::Continue));
        }
    }
}
mod http {
    include!("../../../examples/axum/src/http.rs");

    #[cfg(test)]
    mod tests {
        use bombay::{ActorNotificationReceipts, WorkOutcome};
        use std::future::Future;
        use std::io::{Read, Write};
        use std::net::{SocketAddr, TcpListener, TcpStream};
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;
        use tokio::runtime::Builder;

        use axum::{
            body::Body,
            http::{Request, header},
        };
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        use crate::assert_application_stopped;

        use super::*;

        #[test]
        fn rejected_admission_returns_the_exact_order() {
            let request = PlaceOrder {
                order_id: 73,
                customer_id: 12,
                sku: "TEA-EARL-GREY".into(),
                quantity: 1,
            };
            let body = serde_json::to_vec(&request).expect("request serializes");
            let outcome = tokio_test(
                Application::new(OrderBook::default().stop_on_shutdown()).run_with(
                    |application| async move {
                        let stopped = application.request_shutdown();
                        assert_eq!(stopped, Ok(()));
                        let termination = application.termination().await;
                        assert_eq!(termination, Ok(Exit::Normal));
                        let routes = router(application);
                        let response = routes
                            .clone()
                            .oneshot(
                                Request::post("/orders")
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .body(Body::from(body))
                                    .expect("request builds"),
                            )
                            .await
                            .expect("router is infallible");
                        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                        let response_body = response
                            .into_body()
                            .collect()
                            .await
                            .expect("response body collects")
                            .to_bytes();
                        let rejected: PlaceOrder = serde_json::from_slice(&response_body)
                            .expect("rejected order deserializes");
                        assert_eq!(rejected, request);
                        let response = routes
                            .oneshot(
                                Request::delete("/admin/shutdown")
                                    .body(Body::empty())
                                    .expect("shutdown request builds"),
                            )
                            .await
                            .expect("router is infallible");
                        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                    },
                ),
            )
            .unwrap_or_else(|_| panic!("the actual application starts"));
            let ((), retirement) = outcome
                .try_into_completed()
                .unwrap_or_else(|_| panic!("HTTP rejection completes"));
            assert!(retirement.cleanup().is_ok());
            let (_, native, _, ()) = retirement.into_parts();
            assert_application_stopped(native.expect("the original root joins"));
        }

        #[test]
        fn closed_actor_admission_returns_the_original_order_allocation() {
            let rejected = tokio_test(
                Application::new(OrderBook::default().stop_on_shutdown()).run_with(
                    |application| async move {
                        let stopped = application.request_shutdown();
                        assert_eq!(stopped, Ok(()));
                        let termination = application.termination().await;
                        assert_eq!(termination, Ok(Exit::Normal));
                        let order = PlaceOrder {
                            order_id: 127,
                            customer_id: 19,
                            sku: "ORIGINAL-CLOSED-ORDER".into(),
                            quantity: 3,
                        };
                        let allocation = order.sku.as_ptr();
                        let rejected = place(&application, order).await;
                        let Err(original) = rejected else {
                            panic!("a closed actor must return the complete original order");
                        };
                        assert_eq!(original.sku.as_ptr(), allocation);
                        assert_eq!(original.order_id, 127);
                        assert_eq!(original.customer_id, 19);
                        assert_eq!(original.sku, "ORIGINAL-CLOSED-ORDER");
                        assert_eq!(original.quantity, 3);
                        original
                    },
                ),
            )
            .unwrap_or_else(|original| {
                drop(original);
                panic!("the caller's actual runtime is entered");
            })
            .into_parts();
            let (rejected, native, notifications, ()) = rejected;
            let (original, cleanup) = rejected.try_into_completed().unwrap_or_else(|original| {
                drop(original);
                panic!("the completed rejection preserves independent actor cleanup");
            });
            cleanup.expect("the application's cleanup producer completes");
            let Ok(ActorNotificationReceipts {
                termination: Ok(()),
                retirement: Ok(()),
            }) = notifications
            else {
                panic!("both closed-root notification results remain independently acquired");
            };
            assert_application_stopped(native.expect("the actual closed root joins"));
            assert_eq!(original.order_id, 127);
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
                let application_host = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("the server thread owns its enabled caller host");
                let result = application_host.block_on(
                    Application::new(OrderBook::default().stop_on_shutdown()).run_axum(
                        address,
                        move |application| {
                            ready
                                .send(())
                                .expect("the test server receiver remains live");
                            router(application)
                        },
                    ),
                );
                drop(application_host);
                result
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
            let outcome = result.unwrap_or_else(|failed| {
                drop(failed);
                panic!("the live caller host is entered");
            });
            let outcome = outcome.into_parts();
            if let (
                WorkOutcome::Completed {
                    output: _,
                    cleanup: Ok(()),
                },
                Ok((_, ActorRetirement::ActorTaskFailed(_))),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = &outcome
            {
                panic!("the original serving result and joined root must both remain acquired");
            }
            let (
                WorkOutcome::Completed {
                    output: Ok(()),
                    cleanup: Ok(()),
                },
                Ok((origin, retirement)),
                Ok(ActorNotificationReceipts {
                    termination: Ok(()),
                    retirement: Ok(()),
                }),
                (),
            ) = outcome
            else {
                panic!("the original serving result and joined root must both remain acquired");
            };
            let terminal = (origin, retirement);
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
