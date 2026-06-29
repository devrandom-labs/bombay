//! Cucumber runner for core/registry.properties.feature — the @property/@model
//! laws over the kameo core LOCAL actor registry (`src/registry.rs`):
//! wrong-type-get is ALWAYS BadActorType (∀ name/type), the registry refines an
//! insert-NO-overwrite map under any op sequence, and a same-name concurrent
//! insert elects exactly one winner.
//!
//! Shares the `RegistryWorld` + step definitions with `core_registry_bdd.rs`
//! (the example feature). Standard `#[tokio::test(flavor = "multi_thread")]`
//! libtest function (no `harness = false`) so nextest's `--list` enumerates it;
//! built only with the `testing` feature (see `required-features` in Cargo.toml).
//!
//! `.max_concurrent_scenarios(1)`: same process-global discipline as the example
//! runner — each scenario holds its OWN fresh `Arc<Mutex<ActorRegistry>>`, and
//! the @model @linearizability law's real concurrency is WITHIN the scenario
//! (Barrier + tokio::spawn). registry.properties.feature has NO @bug scenarios;
//! the filter predicate is kept identical to the other core runners.

#[path = "core_steps/registry.rs"]
mod registry;

use cucumber::World;
use registry::RegistryWorld;

#[tokio::test(flavor = "multi_thread")]
async fn registry_props_features() {
    RegistryWorld::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .with_default_cli()
        .filter_run_and_exit(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/features/core/registry.properties.feature"
            ),
            |_, _, sc| !sc.tags.iter().any(|t| t.starts_with("bug")),
        )
        .await;
}
