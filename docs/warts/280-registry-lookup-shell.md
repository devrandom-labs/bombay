# Wart (card #280): registry lookup leaks the Shell adapter

`Registry::lookup::<A>` is keyed on the RUNTIME actor type, so resolving a
caps actor requires `lookup::<caps::Shell<Dispatcher>>(name)` — the internal
adapter leaks into user code (see `examples/job_queue/main.rs`). A caps-aware
lookup (or a `Handle`-returning alias) belongs to the actor-builder /
ergonomics arc after ADR-0026 lands.
