//! Cucumber runner for core/registry.feature — the example scenarios for the
//! bombay core LOCAL actor registry (`src/registry.rs`): the `ActorRegistry`
//! behind the process-global `ACTOR_REGISTRY` Mutex — insert (no overwrite),
//! get with type-safe downcast (BadActorType on mismatch), remove, remove_by_id,
//! contains_name, len/is_empty/names/clear — driven against REAL SPAWNED ACTORS.
//!
//! Shares the `RegistryWorld` + step definitions with
//! `core_registry_props_bdd.rs` (the @property/@model laws). Standard
//! `#[tokio::test(flavor = "multi_thread")]` libtest function (no
//! `harness = false`) so nextest's `--list` enumerates it; built only with the
//! `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: this module exercises the same
//! `Mutex<ActorRegistry>` shape as the process-global static and asserts ABSOLUTE
//! counts (`len() == 3`, `is_empty()`, `len() == 32`). Each scenario holds its
//! OWN fresh `Arc<Mutex<ActorRegistry>>` (Background `Given an empty local actor
//! registry`), so scenarios cannot see each other's registrations; serializing
//! scenarios keeps the per-scenario real-concurrency (Barrier + tokio::spawn)
//! from overlapping ACROSS scenarios. registry.feature has NO @bug scenarios; the
//! filter predicate is the standard `bug*`-tag drop kept identical to the other
//! core runners.

#[path = "core_steps/registry.rs"]
mod registry;

use cucumber::World;
use registry::RegistryWorld;

#[tokio::test(flavor = "multi_thread")]
async fn registry_features() {
    RegistryWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/registry.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
