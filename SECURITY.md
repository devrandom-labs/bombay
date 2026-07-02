# Security Policy

`bombay` is a Zenoh-native actor framework (a hard-fork of kameo). Actors are
addressable across a dataspace and exchange messages over the network, so the
remote layer decodes bytes from untrusted peers — we take the security of this
crate and the systems that depend on it seriously, and we appreciate responsible
disclosure of vulnerabilities.

## Supported Versions

Security fixes target the latest released minor line. bombay is `0.x` and under
active development (see [`CLAUDE.md`](./CLAUDE.md)), so a fix that must change
observable behavior is handled as a coordinated release; following the SemVer
`0.x` convention a breaking fix bumps the minor version.

| Version | Supported          |
|---------|--------------------|
| 0.21.x  | :white_check_mark: |
| < 0.21  | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Report privately through GitHub's built-in advisory workflow:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (GitHub Private Vulnerability Reporting).
3. Provide a clear description, affected version(s), and reproduction steps.

A maintainer will receive your report privately, and you can collaborate on a
fix through the same private advisory.

Direct link: <https://github.com/devrandom-labs/bombay/security/advisories/new>

### What to include

- The area affected (`core` — actor/mailbox/supervision/registry, `remote` —
  the Zenoh transport, `macros`, `console`, `actors`) and the version or git
  commit.
- A description of the impact (e.g. memory unsafety, panic on an untrusted
  remote message, mailbox/supervision isolation break, resource exhaustion).
- A minimal reproduction (a failing test, an actor definition, or the bytes that
  trigger it).

## Response Expectations

- **Acknowledgement:** within 3 business days.
- **Triage & severity assessment:** within 7 business days.
- **Fix & coordinated disclosure:** timeline communicated during triage, scaled
  to severity. We will credit reporters who wish to be acknowledged.

## Scope

In scope: vulnerabilities in this crate's source — including memory safety,
panics on untrusted/malformed input (especially messages decoded by the remote
layer), actor isolation or supervision failures that let one actor corrupt
another, unbounded resource growth reachable from the network, and supply-chain
issues in declared dependencies.

Out of scope: vulnerabilities in downstream applications that merely depend on
`bombay`, issues requiring a non-default explicitly-unsafe configuration, and
denial-of-service that requires a trusted local peer already inside the
dataspace's authorization boundary.

## Supply-Chain Hygiene

Every change is gated by `nix flake check`, which runs `cargo audit`
(RUSTSEC advisory database) and `cargo deny` (advisories, license, and source
bans) on the full dependency tree. Dependabot continuously monitors and opens
update pull requests, and CodeQL scans first-party Rust source on pull requests.
