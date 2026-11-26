---
name: axum
description: Build or review Rust Axum HTTP services, extractors, Tower middleware, error responses, shutdown and router tests. Use for client APIs and private worker HTTP interfaces.
license: MIT
metadata:
  upstream: bobmatnyc/claude-mpm-skills
---

# Axum

Read [ADR 0001](../../../docs/adr/0001-use-loco.md) and
[ADR 0007](../../../docs/adr/0007-enforce-openapi.md). Check the workspace's
actual Axum, Tower, tower-http and Utoipa versions before using an example.

## Request boundaries

- Build routers around existing application operations. Keep domain rules out
  of handlers and HTTP types out of core.
- Use `State`, `Path`, `Query` and `Json` for transport parsing. Put an extractor
  that consumes the body last. Parsing does not replace business validation.
- Current Axum uses `/users/{id}` path captures. Do not copy the older `:id`
  syntax into an incompatible version.
- Reuse cheap-to-clone pools and clients in state. Do not put the whole
  application behind one mutex. Use `Arc` where shared ownership is needed.
- Map typed application failures to documented statuses and response bodies.
  Preserve the error source internally; do not expose raw database errors.
- Keep Utoipa schemas, implemented routes and response behavior aligned. Use
  the `utoipa` skill for that contract.

## Middleware and runtime

Use supported Tower layers for tracing, body limits and timeouts. Check layer
order, extractor rejections, fallback routes and middleware-generated errors.
These responses also need the agreed HTTP error shape. Select CORS origins and
request deadlines from the actual interface; examples do not establish defaults.

Do not block Tokio executor threads with synchronous media or inference work.
Use the worker's existing bounded execution path for that work. A timeout does
not establish that a database write or external operation was rolled back.

Cloud Run binds to `0.0.0.0` on its configured `PORT`. The process handles
SIGTERM, stops accepting work and drains within the platform termination window.
Use the version-matched Axum graceful-shutdown example; propagate signal setup
errors. Do not copy a fixed shutdown duration from another supervisor.

## Verification

Use `tower::ServiceExt::oneshot` for router behavior. Use a real listener for
socket, streaming, cancellation and shutdown behavior. Check status, headers,
body and side effects, including middleware failures. Follow the existing Rust
test workflow; do not introduce a second framework or package layout.

## Sources

- [Axum API and examples](https://docs.rs/axum/latest/axum/)
- [Axum testing example](https://github.com/tokio-rs/axum/blob/main/examples/testing/src/main.rs)
- [Axum graceful shutdown](https://github.com/tokio-rs/axum/tree/main/examples/graceful-shutdown)
- [Tower HTTP](https://docs.rs/tower-http/latest/tower_http/)

Adapted from Claude MPM's Axum skill. See [license](../LICENSE.mpm) and
[source record](../sources.json).
