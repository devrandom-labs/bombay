# Bombay Rust & Distributed Systems Code Rules

Root [AGENTS.md](../../../../AGENTS.md) owns workflow, lanes, review tiers, and merge rules. This document owns how Rust and distributed actor code are designed and written across `bombay`, `mnesis-bombay`, and `selo`, grounded directly in Joel's verified engineering book library.

## Reuse and dependencies

- Search existing `bombay-behavior`, `bombay-behavior-actors`, `bombay-engine`, `bombay-address`, `bombay-communication`, `bombay-timers`, `bombay-observe`, and the Rust standard library before writing new machinery.
- Any step adding a new module or more than 150 `src` lines requires a `Reuse: <crate/primitive used, or concrete gap>` commit trailer.
- Add dependencies only via `nix develop -c cargo add` in a dedicated `Shared` step after checking locked-version docs, `deny.toml` license/advisory gates, and layer boundary rules ([ADR 0001](../../../../docs/adr/0001-layer-bombay-crates.md)). Never work around a missing workspace crate dependency by erasing types into generics or passing closure callbacks across functions.

## Signature & Module Integrity

- **Holistic Cohesive Types Over Combinator Soup (`architecture/step.sh` `signature-design` — mandatory, zero escape hatch)**:
  - **No tuple-packed parameters (`tuple-parameter-packing`)**: Never destructure tuple parameters `(a, b): (T1, T2)` in function or trait signatures (outside single-argument `From`/`TryFrom` conversions) to evade `clippy.toml` (`too-many-arguments-threshold = 5`). Group cohesive inputs into a named domain struct or move the method onto its owning receiver.
  - **No callback soup (`callback-soup`)**: Never pass 3 or more `FnOnce`/`FnMut`/`Fn`/`AsyncFn*` parameters into a function or trait method. Multi-closure sequencing combinators invert module boundaries and destroy readability; pass concrete domain values, methods on cohesive structs, or narrow ADR 0001 Port traits instead.
  - **No single-letter generic soup (`single-letter-generics`)**: Never declare 3 or more single-letter type parameters (`<H, T, S, R, E>`) on a function, struct, enum, or trait. Use concrete Layer 0/1/2a domain types or full role-revealing type parameter names (`<Protocol, TransportError>`).
  - **No unnamed public `Result` tuples (`pub-result-tuple`)**: Never return `Result<(A, B), E>` from a `pub fn`; return a named struct with intention-revealing fields.
- **Module-Scope Imports Only (`AGENTS.md`)**: Keep every `use` declaration at module scope. Never write an inline `use` inside a function, method, block, closure, macro arm, or test body (`architecture/rules/no-inline-use.yml`).
- **Ubiquitous Domain Vocabulary (Evans, *Domain-Driven Design* (2003), ch. 10, pp. 243–266; `bombay-behavior` `AGENTS.md`)**: Never use `Manager`, `Handler`, `Helper`, `Utils`, `Thing`, `Data`, `Process`, `Fold`, `Fact`, `Boundary`, `Installation`, or `Incarnation` in module, type, trait, or variable names (`architecture/rules/bombay-banned-names.yml`).
- **Standard Traits, Slices & Cost Contracts (Matthews, *Idiomatic Rust* (2024), ch. 2 pp. 19, 27; ch. 4.4 pp. 80–81; ch. 5.7 pp. 124–127; ch. 7.6–7.7 pp. 161–173; ch. 10 pp. 205–224)**:
  - Reuse `From`, `TryFrom`, `AsRef`, `Borrow`, and `IntoIterator` before inventing local traits.
  - Accept `&[T]` and `&str` over `&Vec<T>`, `&String`, or `&Box<T>` (`slice-aversion`), and expose borrowed view structs (`FooRef<'a>`) rather than cloning or opening invariant-bearing `pub` fields (`pub-field-domain`).
  - Follow `as_` (free borrow), `to_` (allocates/copies), and `into_` (consumes `self`) naming strictly.
  - Use value-consuming typestates (`fn next(self) -> Result<Next, (Self, Error)>`) so rejected transitions return affine inputs intact.
  - Ban `Deref` struct inheritance (`deref-polymorphism`), redundant `Vec<Box<T>>` / unshared `Arc<T>`, borrow-appeasing `.clone()`, and production `.unwrap()`.
- **Constrained Public Interfaces & Async Footprints (Gjengset, *Rust for Rustaceans* (2021), ch. 3 pp. 37–56; ch. 8 pp. 117–129)**:
  - Never encode semantic state, policy, authority, or provenance in a `bool`; use closed domain enums (pp. 48–49).
  - Mark pure decision, action, and transition return types `#[must_use]` (p. 50) and check `Send + Sync + Unpin` on public boundary types (p. 55).
  - Expose explicit consuming `async fn close(self) -> Result<...>` teardown on live `Node`, session, and listener handles alongside best-effort `Drop` (pp. 46–47).
  - Seal internal extension traits and never leak foreign SDK types (`zenoh*`, `axum`, `mnesis*`) across Layer 2 signatures (pp. 52–54).
  - Keep large buffers out of `.await` scopes so generated `Future` state machines remain compact, and never poll unfused futures after `Poll::Ready` (pp. 121–126).

## Runtime and SDKs

- **Pure `Behavior` Fold & `sans-IO` Domain Crates (Evans ch. 10; Sharvit, *Data-Oriented Programming* (2022), ch. 4 p. 72, ch. 6 pp. 111–120)**:
  - `Behavior` methods (`init`, `receive`, `transition`) and all `bombay-domain-*` crates compute pure values and return typed `Actions` / decisions; they never perform I/O, read clocks, spawn tasks, or touch channels/locks (`domain-is-pure.yml` + `clippy.toml`).
- **Atomics & Lock Lifetime Hygiene (Bos, *Rust Atomics and Locks* (2023), ch. 1 pp. 20–22; ch. 2–3 pp. 37, 72–73; ch. 5 pp. 94–100)**:
  - Bind `.lock()` extractions in a standalone `let` statement before `if let`, `match`, or `while let` so temporary `MutexGuard` lifetimes never extend across the block (`temp-lock-guard`).
  - Treat `Ordering::SeqCst` (`seqcst-ordering`) as requiring an explicit happens-before proof; prefer `Mutex`/`OnceLock`, `Relaxed` counters, or paired `Release`/`Acquire`, and guard `fetch_add`/`fetch_sub` wraparound.
  - Prefer move-once `!Copy` capability handles over runtime `AtomicBool` `in_use` flags.
- **Executor vs. Reactor Discipline (Samson, *Asynchronous Programming in Rust* (2024), ch. 8)**:
  - Never block Tokio worker threads with synchronous I/O, `thread::sleep`, or `std::sync::Mutex` held across `.await` (`sync-lock-in-async`). Bound every queue and ingress channel explicitly (`unbounded-channel`).
- **Parse, Don't Validate, Security & Epoch Fencing (Palmieri, *Zero To Production In Rust* (2022), ch. 4 §4.5.13, ch. 6 §§6.1–6.4, ch. 8 §§8.1–8.4; Madden, *API Security in Action* (2020), ch. 2, 6, 8; Kleppmann, *Designing Data-Intensive Applications* (2017), ch. 7–8)**:
  - Parse untrusted wire metadata, AIDs, key expressions, and limits once at the boundary into validated private-field types before allocation (`WIRE-08`).
  - Never discard error causes with `map_err(|_| ...)` (`keep-error-sources`), and avoid "Ball of Mud" `thiserror` enums (`>= 6 #[from]` variants, `ball-of-mud-error`) when callers only branch on semantic failure classes.
  - Keep auth denials free of key/decryption oracle details (`AUTH-07`), redact credentials in `Debug` / diagnostics (`OBS-05`), preserve disconnect/timeout uncertainty explicitly (`NET-05`, `DUR-07`), and enforce monotonic hosting-epoch fencing tokens at admission (`PLACE-06`) and durable commits (`DUR-13`, `PLACE-07`).

## Tests

- **Stateful Port Fakes Over `unreachable!()` Callback Mocks (`architecture/step.sh` `test-unreachable` — zero rise allowed)**:
  - `architecture/step.sh` rejects any increase in `unreachable!()` macro calls across changed test files (`crates/**/tests/**/*.rs`, `apps/**/tests/**/*.rs`).
  - Never test production logic by passing dummy `|_| unreachable!("not called")` interaction closures; drive pure state machines directly with domain values or deterministic stateful Port fakes (`SimTransport`, `SimDurableStore`, `SimPlacementDirectory`), and assert expected enum variants directly (`assert_eq!`, `assert!(matches!(...))`, or `let Variant::Expected(v) = actual else { panic!(...) }`).
- **Observable Oracles, Defect Inversions & FSM Properties (Hebert, *Property-Based Testing* (2019), ch. 11)**:
  - Never place mutations, ownership transfers, or required calls inside `assert!` / `debug_assert!`.
  - Prove every new guard or regression test fails when the guarded defect is simulated, and test stateful protocols with state-conditioned FSM `proptest` suites and `cargo mutants`.
