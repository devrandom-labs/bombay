//! Head-to-head: the #119 `Registry` (papaya, erased weak handles) vs the
//! vendored kameo fork's `ActorRegistry` (`Mutex<HashMap>` — benched through a
//! local `Mutex` instance, the same shape as kameo's global `ACTOR_REGISTRY`).
//!
//! Method: one live actor per side (spawned once in setup, kept alive for the
//! whole bench), a registered name, then three groups:
//!
//! * `registry_lookup_hit` — single-thread typed lookup of a live entry, the
//!   read-heavy hot path the papaya pick optimizes for.
//! * `registry_lookup_contended_4x1k` — 4 OS threads × 1 000 lookups per
//!   iteration on the same name: lock-free reads vs mutex convoy.
//! * `registry_register_unregister` — the write-path cycle (claim + free),
//!   one lock acquisition per op on the kameo side, as production would.
//!
//! Semantics are NOT identical, and that is the point being priced: bombay's
//! lookup pays `upgrade` + channel-open liveness + downcast (weak handles — a
//! registration never pins the actor, dead entries read as absent); kameo's
//! pays lock + downcast only (strong refs — a registered actor can never die,
//! so there is nothing to check). Both sides clone an owned `ActorRef` out per
//! hit.
//!
//! Measured 2026-07-18 (M-series laptop, criterion defaults, #186 — the
//! single-allocation `ActorRef`, ADR-0010, which this bench motivated under
//! PR #185; per-group deltas are vs that same-session pre-#186 baseline):
//!
//! | group                          | bombay-core          | kameo                | delta         |
//! |--------------------------------|----------------------|----------------------|---------------|
//! | lookup_hit                     | 12.1 ns (−57%)       | 19.3 ns              | bombay 1.59×  |
//! | lookup_contended_4x1k (1 name) | 192.2 µs (20.8 M/s)  | 255.9 µs (15.6 M/s)  | bombay 1.33×  |
//! | lookup_contended_4x1k_distinct | 42.0 µs (95.3 M/s)   | 210.6 µs (19.0 M/s)  | bombay 5.02×  |
//! | lookup_under_churn_3r1w        | 267.0 µs (11.2 M/s)  | 334.7 µs (8.96 M/s)  | bombay 1.25×  |
//! | register_unregister            | 67.4 ns (−39%)       | 47.0 ns              | kameo 1.43×   |
//!
//! Reading — ADR-0010 confirmed by measurement. Under PR #185 (the ADR-0003
//! handle shape: flume-CAS + 2 Arc RMWs per clone, three contended
//! cachelines) the same-name groups LOST to kameo's `Mutex<HashMap>` by
//! 1.4–1.8×; the single-allocation handle (1 Weak-upgrade CAS + inline id
//! per hit) flips every read group to a bombay win and lifts the same-name
//! ceiling −59% (478 → 192 µs). The distinct-names regime — the design
//! target: many actors, message-rate lookups — jumps to 5× over the global
//! mutex, which is flat by construction whatever the name.
//!
//! * **Write cycle (kameo 1.43×):** a `Box` per claim + `compute`'s atomics
//!   vs a plain locked insert — accepted, registration is passivation-rate
//!   (the #186 restructure still shaved it −39% as a side effect: the weak
//!   handle it boxes is now two words).
//!
//! Both designs sit 2–3 orders below message-rate costs (µs-scale sends);
//! the structural drivers on the card (no guard-across-`.await` deadlock
//! class, atomic register-once, weak no-pinning entries — kameo's registry
//! pins registered actors and returns refs to stopped ones) are what the
//! extra same-name nanoseconds buy. `scc::HashIndex` is the recorded
//! runner-up if the map itself ever measures as the bottleneck.

use std::{hint::black_box, num::NonZeroUsize, sync::Mutex, thread};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::{Builder, Runtime};

use bombay_core::registry::Registry;

mod core_side {
    use bombay_core::{
        actor::{Actor, ActorRef, Spawn as _},
        mailbox::{Capacity, Mailboxed},
        message::Msg,
    };

    use super::NonZeroUsize;

    pub struct Svc;
    #[derive(Debug)]
    pub struct Ping;
    impl Msg for Ping {}
    impl Mailboxed for Svc {
        type Msg = Ping;
    }
    impl Actor for Svc {
        type Args = ();
        type Error = core::convert::Infallible;
        async fn on_start((): (), _: ActorRef<Self>) -> Result<Self, Self::Error> {
            Ok(Self)
        }
        async fn handle(
            &mut self,
            _: Ping,
            _: ActorRef<Self>,
            _: &mut bool,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    pub fn spawn() -> ActorRef<Svc> {
        let cap = Capacity::new(NonZeroUsize::new(64).expect("nonzero")).expect("within max");
        Svc::spawn_with_capacity(cap, ())
    }
}

mod kameo_side {
    use bombay::prelude::*;

    #[derive(Actor, Default)]
    pub struct Svc;

    pub struct Ping;
    impl Message<Ping> for Svc {
        type Reply = ();
        async fn handle(&mut self, _: Ping, _: &mut Context<Self, Self::Reply>) {}
    }

    pub fn spawn() -> ActorRef<Svc> {
        Svc::spawn_with_mailbox(Svc, bombay::mailbox::bounded(64))
    }
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("bench runtime")
}

/// Both registries populated with one live actor under `"svc"`, both actors
/// (and the runtime driving them) kept alive by the returned tuple.
#[expect(clippy::type_complexity, reason = "bench setup bundle, used once")]
fn setup() -> (
    Runtime,
    Registry,
    bombay_core::actor::ActorRef<core_side::Svc>,
    Mutex<bombay::registry::ActorRegistry>,
    bombay::actor::ActorRef<kameo_side::Svc>,
) {
    let rt = runtime();
    let registry = Registry::new();
    let core_ref = rt.block_on(async { core_side::spawn() });
    registry.register("svc", &core_ref).expect("fresh name");

    let kameo_registry = Mutex::new(bombay::registry::ActorRegistry::new());
    let kameo_ref = rt.block_on(async { kameo_side::spawn() });
    assert!(
        kameo_registry
            .lock()
            .expect("unpoisoned")
            .insert("svc", kameo_ref.clone()),
        "fresh name",
    );
    (rt, registry, core_ref, kameo_registry, kameo_ref)
}

fn lookup_hit(c: &mut Criterion) {
    let (_rt, registry, _core_ref, kameo_registry, _kameo_ref) = setup();
    let mut group = c.benchmark_group("registry_lookup_hit");
    group.throughput(Throughput::Elements(1));

    group.bench_function("bombay_core", |b| {
        b.iter(|| {
            black_box(
                registry
                    .lookup::<core_side::Svc>("svc")
                    .expect("typed")
                    .expect("live"),
            )
        });
    });

    group.bench_function("kameo", |b| {
        b.iter(|| {
            black_box(
                kameo_registry
                    .lock()
                    .expect("unpoisoned")
                    .get::<kameo_side::Svc, _>("svc")
                    .expect("typed")
                    .expect("present"),
            )
        });
    });

    group.finish();
}

const READERS: usize = 4;
const LOOKUPS_PER_READER: u64 = 1_000;

fn lookup_contended(c: &mut Criterion) {
    let (_rt, registry, _core_ref, kameo_registry, _kameo_ref) = setup();
    let mut group = c.benchmark_group("registry_lookup_contended_4x1k");
    group.throughput(Throughput::Elements(READERS as u64 * LOOKUPS_PER_READER));

    group.bench_function("bombay_core", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..READERS {
                    s.spawn(|| {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                registry
                                    .lookup::<core_side::Svc>("svc")
                                    .expect("typed")
                                    .expect("live"),
                            );
                        }
                    });
                }
            });
        });
    });

    group.bench_function("kameo", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..READERS {
                    s.spawn(|| {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                kameo_registry
                                    .lock()
                                    .expect("unpoisoned")
                                    .get::<kameo_side::Svc, _>("svc")
                                    .expect("typed")
                                    .expect("present"),
                            );
                        }
                    });
                }
            });
        });
    });

    group.finish();
}

const NAMES: [&str; READERS] = ["svc0", "svc1", "svc2", "svc3"];

/// The regime the card's "read-heavy" claim is actually about: concurrent
/// lookups of DIFFERENT names (different actors). Same-name contention (the
/// group above) is dominated by the one actor's shared handle cachelines,
/// which every design pays; here each reader touches its own actor's
/// handles, so what remains is the map discipline itself — kameo's single
/// global mutex serializes all names, papaya's buckets are independent.
fn lookup_contended_distinct(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("registry_lookup_contended_4x1k_distinct");
    group.throughput(Throughput::Elements(READERS as u64 * LOOKUPS_PER_READER));

    let registry = Registry::new();
    let core_refs: Vec<_> = (0..READERS)
        .map(|_| rt.block_on(async { core_side::spawn() }))
        .collect();
    for (name, actor_ref) in NAMES.iter().zip(&core_refs) {
        registry.register(*name, actor_ref).expect("fresh name");
    }
    group.bench_function("bombay_core", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for name in NAMES {
                    let registry = &registry;
                    s.spawn(move || {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                registry
                                    .lookup::<core_side::Svc>(name)
                                    .expect("typed")
                                    .expect("live"),
                            );
                        }
                    });
                }
            });
        });
    });

    let kameo_registry = Mutex::new(bombay::registry::ActorRegistry::new());
    let kameo_refs: Vec<_> = (0..READERS)
        .map(|_| rt.block_on(async { kameo_side::spawn() }))
        .collect();
    for (name, actor_ref) in NAMES.iter().zip(&kameo_refs) {
        assert!(
            kameo_registry
                .lock()
                .expect("unpoisoned")
                .insert(*name, actor_ref.clone()),
            "fresh name",
        );
    }
    group.bench_function("kameo", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for name in NAMES {
                    let kameo_registry = &kameo_registry;
                    s.spawn(move || {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                kameo_registry
                                    .lock()
                                    .expect("unpoisoned")
                                    .get::<kameo_side::Svc, _>(name)
                                    .expect("typed")
                                    .expect("present"),
                            );
                        }
                    });
                }
            });
        });
    });

    group.finish();
}

/// The workload the #119 design actually targets: message-rate lookups
/// *concurrent with* passivation-rate register/unregister churn. 3 reader
/// threads hammer the stable `"svc"` entry while 1 writer thread cycles a
/// different name — on the mutex side every reader queues behind every
/// writer; on the papaya side reads never block.
fn lookup_under_churn(c: &mut Criterion) {
    const CHURN_READERS: usize = 3;
    let (_rt, registry, core_ref, kameo_registry, kameo_ref) = setup();
    let mut group = c.benchmark_group("registry_lookup_under_churn_3r1w");
    group.throughput(Throughput::Elements(
        CHURN_READERS as u64 * LOOKUPS_PER_READER,
    ));

    group.bench_function("bombay_core", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..CHURN_READERS {
                    s.spawn(|| {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                registry
                                    .lookup::<core_side::Svc>("svc")
                                    .expect("typed")
                                    .expect("live"),
                            );
                        }
                    });
                }
                s.spawn(|| {
                    for _ in 0..LOOKUPS_PER_READER {
                        registry.register("churn", &core_ref).expect("fresh name");
                        assert!(registry.unregister("churn"), "own entry removable");
                    }
                });
            });
        });
    });

    group.bench_function("kameo", |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..CHURN_READERS {
                    s.spawn(|| {
                        for _ in 0..LOOKUPS_PER_READER {
                            black_box(
                                kameo_registry
                                    .lock()
                                    .expect("unpoisoned")
                                    .get::<kameo_side::Svc, _>("svc")
                                    .expect("typed")
                                    .expect("present"),
                            );
                        }
                    });
                }
                s.spawn(|| {
                    for _ in 0..LOOKUPS_PER_READER {
                        assert!(
                            kameo_registry
                                .lock()
                                .expect("unpoisoned")
                                .insert("churn", kameo_ref.clone()),
                            "fresh name",
                        );
                        assert!(
                            kameo_registry.lock().expect("unpoisoned").remove("churn"),
                            "own entry removable",
                        );
                    }
                });
            });
        });
    });

    group.finish();
}

fn register_unregister(c: &mut Criterion) {
    let (_rt, registry, core_ref, kameo_registry, kameo_ref) = setup();
    let mut group = c.benchmark_group("registry_register_unregister");
    group.throughput(Throughput::Elements(1));

    group.bench_function("bombay_core", |b| {
        b.iter(|| {
            registry.register("cycle", &core_ref).expect("fresh name");
            assert!(registry.unregister("cycle"), "own entry removable");
        });
    });

    group.bench_function("kameo", |b| {
        b.iter(|| {
            assert!(
                kameo_registry
                    .lock()
                    .expect("unpoisoned")
                    .insert("cycle", kameo_ref.clone()),
                "fresh name",
            );
            assert!(
                kameo_registry.lock().expect("unpoisoned").remove("cycle"),
                "own entry removable",
            );
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    lookup_hit,
    lookup_contended,
    lookup_contended_distinct,
    lookup_under_churn,
    register_unregister
);
criterion_main!(benches);
