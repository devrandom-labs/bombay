# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/devrandom-labs/bombay/compare/bombay-rs-v0.1.3...bombay-rs-v0.2.0) - 2026-10-17

### Changed

- **Breaking:** use Behavior/Actors 0.29, Engine 0.3 and macros 0.2.0 together.
- Compose supervisors and pools through owning Config values and inferred startup.
- Use wrapper-owned timer targets, finite stash admission and `stash.take()`.
- Name application shutdown `shutdown()` while retaining typed admission and
  independent joined retirement.

### Fixed

- Reject unsupported native mailbox capacities before allocation.
- Preserve complete reply, refusal, effect and retirement custody through the
  selected policy compositions.

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
