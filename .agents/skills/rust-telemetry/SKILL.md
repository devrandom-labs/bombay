---
name: rust-telemetry
description: Instrument Rust services and workers with tracing and OpenTelemetry. Use for spans, async context, OTLP exporters, metrics, latency measurements and telemetry shutdown.
metadata:
  origin: labcast-local
---

# Rust telemetry

Read [ADR 0006](../../../docs/adr/0006-use-opentelemetry.md) and the affected
pipeline document. Check compatibility across tracing, tracing-subscriber,
tracing-opentelemetry, opentelemetry, SDK and exporter crates in Cargo.lock.

Configure providers and subscribers at the application entry point. Libraries
emit telemetry through narrow existing APIs; they do not install global defaults.
Use supported async instrumentation. Do not keep an entered span guard across
an await and accidentally attribute another task's work to that span.
Record each span field at most once per path; the locked bridge and SDK append
repeated keys, and JSON logs can hide them, so use collector attributes to
verify uniqueness.

Propagate trace context at HTTP and durable-work boundaries. Keep event, source,
frame and operation identities distinct from trace identity. Record proposed,
accepted, applied, expired and unknown outcomes without treating a successful
transport response as proof that a media change appeared on air.

Measure capture, receipt, inference, decision, composition and output separately.
For cross-device timing use the architecture's clock mapping and uncertainty.
A local monotonic duration cannot alone measure the complete distributed path.

Bound exporter queues and cardinality. Do not make live production wait for a
telemetry backend. Keep tokens, signed URLs and raw private media out of logs.
Flush within the process shutdown budget. Define sampling so the measurements
used for product claims are not silently biased by trace selection.

Traces, metrics and logs have distinct integration paths. Do not assume adding
the tracing bridge also exports all logs. Check the selected exporter/collector
protocol and signal support, and test propagation across asynchronous boundaries.

## Sources

- [tracing](https://docs.rs/tracing/latest/tracing/)
- [Async Instrument](https://docs.rs/tracing/latest/tracing/trait.Instrument.html)
- [tracing-opentelemetry](https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/)
- [Rust OpenTelemetry](https://opentelemetry.io/docs/languages/rust/)
- [Collector](https://opentelemetry.io/docs/collector/)
