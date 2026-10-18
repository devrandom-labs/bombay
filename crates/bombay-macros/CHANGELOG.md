# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/devrandom-labs/bombay/compare/bombay-macros-v0.1.3...bombay-macros-v0.2.0) - 2026-10-17

### Changed

- **Breaking:** topology generation requires the Bombay 0.2 Runtime and NativeHost
  contracts; keep older runtime consumers on macros 0.1.

### Added

- Derive runtime ownership for application topologies, including configured
  interpreters and inferred finite descendants of cold members.

## [0.1.3](https://github.com/devrandom-labs/bombay/compare/bombay-macros-v0.1.2...bombay-macros-v0.1.3) - 2026-10-14

### Other

- integrate DevX and preserve remote custody ([#359](https://github.com/devrandom-labs/bombay/pull/359))

## [0.1.2](https://github.com/devrandom-labs/bombay/compare/bombay-macros-v0.1.1...bombay-macros-v0.1.2) - 2026-10-08

### Other

- Retain actor results across cancellation and joining

## [0.1.1](https://github.com/devrandom-labs/bombay/compare/bombay-macros-v0.1.0...bombay-macros-v0.1.1) - 2026-10-02

### Other

- reconcile released main with runtime audit
- distill Bombay ownership and verification

## [0.1.0](https://github.com/devrandom-labs/bombay/releases/tag/bombay-macros-v0.1.0) - 2026-09-15

### Other

- adopt atomic behavior composition
