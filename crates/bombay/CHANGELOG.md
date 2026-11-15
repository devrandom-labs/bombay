# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/devrandom-labs/bombay/compare/bombay-rs-v0.1.3...bombay-rs-v0.2.0) - 2026-11-15

### Changed

- **Breaking:** use Behavior/Actors 0.30, Engine 0.3 and macros 0.2 together.
- **Breaking:** joined application results retain the root report between work
  and native retirement. Update destructuring to
  `(work, root_report, native, notifications, families)`.
- **Breaking:** Node work callbacks receive typed actor capabilities; native
  sessions, queries and SDK configuration stay inside the runtime.
- **Breaking:** runner methods infer the actor and its staging and child failure
  types. Remove those redundant generic arguments; only actual work, futures and
  delivery routers remain caller type inputs.
- Compose supervisors and pools through owning Config values and inferred startup.
- Use wrapper-owned timer targets, `stash.take()` and explicit Stash/Machine
  holding policies with complete refused-input custody.
- Name application shutdown `shutdown()` while retaining typed admission and
  independent joined retirement.

### Added

- Use one Application for named actor topologies, configured interpreters,
  clients, HTTP and Entity families with complete recursive retirement custody.
- Compose typed remote requests and replies through Node Client/Export interfaces
  while retaining isolated actor requests and verified provider results.

### Fixed

- Publish Entity retirement completion only after queued routing removal, so
  reactivation cannot consume the preceding incarnation's retirement.

- Reject unsupported native mailbox capacities before allocation.
- Preserve complete reply, refusal, effect and retirement custody through the
  selected policy compositions; borrow the primary outcome before consuming it.
- Bound remote declaration diagnostics.

## [0.1.3](https://github.com/devrandom-labs/bombay/compare/bombay-rs-v0.1.2...bombay-rs-v0.1.3) - 2026-10-15

### Added

- *(remote)* certify application actor clients ([#370](https://github.com/devrandom-labs/bombay/pull/370))
- *(remote)* check protected JSON before typed admission ([#368](https://github.com/devrandom-labs/bombay/pull/368))

## [0.1.2](https://github.com/devrandom-labs/bombay/compare/bombay-rs-v0.1.1...bombay-rs-v0.1.2) - 2026-10-15

### Fixed

- *(local)* bound event service without reordering child results ([#366](https://github.com/devrandom-labs/bombay/pull/366))

### Other

- prove hosted child capability isolation ([#365](https://github.com/devrandom-labs/bombay/pull/365))

## [0.1.1](https://github.com/devrandom-labs/bombay/compare/bombay-rs-v0.1.0...bombay-rs-v0.1.1) - 2026-09-15

### Other

- adopt atomic behavior composition

## [0.1.0](https://github.com/devrandom-labs/bombay/releases/tag/bombay-rs-v0.1.0) - 2026-08-13

### Other

- establish Bombay actor runtime
