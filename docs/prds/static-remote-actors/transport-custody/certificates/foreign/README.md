# Foreign test certificates

These deliberately public test credentials come unchanged from
`bombay-zenoh-link-tls 1.10.4`, under `tests/fixtures/tls-ownership/`.
The published crate archive has SHA256
`66fdcaa60b410fdfca22d0b963bb7254049d0e31b3c6f4c0d2b815c41d4c4fe3`;
its VCS revision is `c34eb350faad3d8d103aa383091d85502b67a898`,
path `io/zenoh-links/zenoh-link-tls`.

The upstream fixtures were generated with minica. The source credits
Copyright (c) 2023 ZettaScale Technology and is licensed
`EPL-2.0 OR Apache-2.0`. These copies use
[Apache-2.0](../../../../../../LICENSE-APACHE).
The original [Eclipse zenoh notices](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/NOTICE.md)
retain the project's authorship and Eclipse Foundation trademark attribution.
No CA signing key is included. Original bytes, including absent final newlines,
are preserved. These keys are for tests and must never be deployment credentials.

| File | SHA256 |
| --- | --- |
| root-ca.pem | `ad9c002d1d9cca03305da59045b62fba9dd03e833d330832385d94de6c8f618b` |
| localhost-certificate.pem | `36c8780b6f781f7f18042989dc7d83dd234f0fd20c618f71fd875407b4bd0f58` |
| localhost-private-key.pem | `b31abac5d364db6252b34af1933af6a7ecb0cc29cda8f779675438f510ed5bbb` |

The leaf supports localhost client/server authentication and expires in 2123.
Its CA is independent of Bombay's existing test CA. The foreign-client test
presents this leaf/key while retaining Bombay's CA for server verification;
this foreign root is an input to fixture validation. The original seven
top-level PEMs and their earlier receipt maps remain unchanged.
