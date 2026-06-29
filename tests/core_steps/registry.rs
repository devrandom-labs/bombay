//! Shared `RegistryWorld` + step definitions for the core `registry` scenarios.
//!
//! Wired by two runners that `#[path]`-include this module:
//!   * `core_registry_bdd.rs`       — the example feature (registry.feature)
//!   * `core_registry_props_bdd.rs` — the property/model laws (registry.properties.feature)
//!
//! SUT: the kameo core LOCAL actor registry — `kameo::registry::ActorRegistry`
//! (`src/registry.rs`): `insert(name, ref) -> bool` (NO overwrite, dedups by
//! name), `get::<A>(name) -> Result<Option<ActorRef<A>>, RegistryError>` (Ok(None)
//! absent, Ok(Some) present+downcast-ok, Err(BadActorType) wrong type),
//! `remove`, `remove_by_id`, `contains_name`, `len`, `is_empty`, `clear`,
//! `names`.
//!
//! PROCESS-GLOBAL STATE DISCIPLINE — the production registry behind
//! `kameo::registry::ACTOR_REGISTRY` is a process-global `Mutex<ActorRegistry>`
//! shared across the whole process. cucumber runs scenarios concurrently by
//! default, which would race it, AND these scenarios assert ABSOLUTE counts
//! (`len() == 3`, `is_empty()`, `len() == 32`, `clear` empties). To keep those
//! assertions faithful WITHOUT any `src/` change, each scenario holds its OWN
//! fresh `Arc<Mutex<ActorRegistry>>` (constructed in the Background `Given an
//! empty local actor registry`) — the exact same `Mutex<ActorRegistry>` shape as
//! the global static, just scenario-scoped. The `Arc<Mutex<…>>` is also what the
//! @linearizability scenarios share across `tokio::spawn` tasks under a
//! `Barrier` for genuine overlap. Both runners ALSO set
//! `.max_concurrent_scenarios(1)` (the harness mandate for global-state modules);
//! the only concurrency in play is WITHIN a scenario.
//!
//! The global static `ACTOR_REGISTRY` itself is exercised end-to-end by the
//! `core_actor_ref` runner's `register`/`lookup` scenarios; here the unit under
//! test is the `ActorRegistry` value the static wraps, so no reset hook is added.
//!
//! All public API is reached through `kameo::prelude::*` + `kameo::registry::*` +
//! `kameo::error::*`; no `src/` change is needed.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use cucumber::{World, given, then, when};
use kameo::{
    actor::{ActorId, ActorRef},
    error::{RegistryError, SendError},
    prelude::*,
    registry::ActorRegistry,
};
use tokio::sync::Barrier;

// ===========================================================================
// Test actors — two distinct types so wrong-type downcast is exercised.
// ===========================================================================

/// Primary actor type registered under names in most scenarios.
struct Foo;

impl Actor for Foo {
    type Args = Self;
    type Error = kameo::error::Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A DIFFERENT actor type, used to drive `BadActorType` on a wrong-type get.
struct Bar;

impl Actor for Bar {
    type Args = Self;
    type Error = kameo::error::Infallible;

    async fn on_start(state: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// A tagged message so a stale ref can be told something (liveness scenario).
struct Ping;

impl Message<Ping> for Foo {
    type Reply = ();

    async fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {}
}

// ===========================================================================
// Helpers
// ===========================================================================

type Reg = Arc<Mutex<ActorRegistry>>;

/// Spawns a fresh, started `Foo` and returns its ref.
async fn spawn_foo() -> ActorRef<Foo> {
    let actor = Foo::spawn(Foo);
    actor.wait_for_startup().await;
    actor
}

/// Condition-based settle: polls `cond` up to a bound with a short sleep between
/// tries; panics with `msg` if it never holds. Used for any async observable
/// (e.g. an actor reaching the not-running state after a graceful stop).
async fn settle<F: FnMut() -> bool>(mut cond: F, msg: &str) {
    for _ in 0..400 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition did not settle within the bound: {msg}");
}

/// Stops `actor` gracefully and waits until it is observably not running.
async fn stop_and_settle(actor: &ActorRef<Foo>) {
    actor.stop_gracefully().await.expect("graceful stop signal");
    actor.wait_for_shutdown().await;
    let probe = actor.clone();
    settle(move || !probe.is_alive(), "actor never reported not-running").await;
}

// ===========================================================================
// World
// ===========================================================================

#[derive(Debug, Default, World)]
pub struct RegistryWorld {
    /// The scenario-local registry (fresh per scenario; never the global static).
    registry: Option<Reg>,
    /// Named actors kept alive for the scenario (id-by-name resolution).
    actors: Vec<(String, ActorRef<Foo>)>,
    /// A second, never-registered Foo (remove_by_id-of-absent scenario).
    a2_foo: Option<ActorRef<Foo>>,
    /// A spawned Bar for the wrong-type-get scenario.
    bar: Option<ActorRef<Bar>>,
    /// The boolean returned by the most recent single insert.
    last_insert: Option<bool>,
    /// Booleans from a pair of inserts (two-distinct-names scenario).
    insert_pair: Vec<bool>,
    /// The boolean returned by the most recent remove / remove_by_id.
    last_remove: Option<bool>,
    /// A long name built in a Given and reused in later steps.
    long_name: Option<String>,
    /// Concurrent-insert results: one bool per task.
    concurrent_results: Vec<bool>,
    /// The id of the single winning task in a same-name election.
    winner_id: Option<ActorId>,
    /// Distinct (name, id) pairs from a distinct-name concurrent insert.
    distinct_pairs: Vec<(String, ActorId)>,
    /// Outcome of a concurrent get-during-remove (Ok(Some(id)) / Ok(None)).
    concurrent_get: Option<Option<ActorId>>,
    /// A captured SendError label from telling a stale ref.
    send_err_not_running: Option<bool>,
    /// captured is_empty / len observations for multi-assert scenarios.
    is_empty_obs: Option<bool>,
    len_obs: Option<usize>,
}

impl RegistryWorld {
    fn reg(&self) -> Reg {
        Arc::clone(self.registry.as_ref().expect("registry initialized in Background"))
    }

    /// Looks up a kept-alive actor's id by the name it was inserted under.
    fn id_for(&self, name: &str) -> ActorId {
        self.actors
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, a)| a.id())
            .unwrap_or_else(|| panic!("no kept-alive actor for name {name:?}"))
    }
}

// ===========================================================================
// Background
// ===========================================================================

#[given(regex = r"^an empty local actor registry$")]
async fn given_empty_registry(world: &mut RegistryWorld) {
    world.registry = Some(Arc::new(Mutex::new(ActorRegistry::new())));
}

#[given(regex = r"^the registry is empty$")]
async fn given_registry_is_empty(_world: &mut RegistryWorld) {
    // The Background already created an empty registry; nothing more to do.
}

// ===========================================================================
// Spawning + registering Givens
// ===========================================================================

#[given(regex = r#"^a running actor "([^"]*)" of type Foo$"#)]
async fn given_running_foo(world: &mut RegistryWorld, label: String) {
    let actor = spawn_foo().await;
    world.actors.push((label, actor));
}

#[given(regex = r#"^a different running actor "([^"]*)" of type Foo$"#)]
async fn given_different_running_foo(world: &mut RegistryWorld, label: String) {
    let actor = spawn_foo().await;
    world.actors.push((label, actor));
}

#[given(regex = r#"^a fresh running actor "([^"]*)" of type Foo$"#)]
async fn given_fresh_running_foo(world: &mut RegistryWorld, label: String) {
    let actor = spawn_foo().await;
    world.actors.push((label, actor));
}

#[given(regex = r#"^a running actor "([^"]*)" of type Foo registered under "([^"]*)"$"#)]
async fn given_foo_registered(world: &mut RegistryWorld, label: String, name: String) {
    let actor = spawn_foo().await;
    let ok = world.reg().lock().unwrap().insert(name, actor.clone());
    assert!(ok, "initial registration must succeed");
    world.actors.push((label, actor));
}

#[given(regex = r#"^a running actor "([^"]*)" of type Foo registered under both "([^"]*)" and "([^"]*)"$"#)]
async fn given_foo_registered_twice(
    world: &mut RegistryWorld,
    label: String,
    name1: String,
    name2: String,
) {
    let actor = spawn_foo().await;
    {
        let reg = world.reg();
        let mut guard = reg.lock().unwrap();
        assert!(guard.insert(name1, actor.clone()), "first name insert must succeed");
        assert!(guard.insert(name2, actor.clone()), "second name insert must succeed");
    }
    world.actors.push((label, actor));
}

#[given(regex = r#"^a second running actor "([^"]*)" that was never registered$"#)]
async fn given_second_unregistered(world: &mut RegistryWorld, _label: String) {
    world.a2_foo = Some(spawn_foo().await);
}

#[given(regex = r#"^actors are inserted under "([^"]*)" and "([^"]*)"$"#)]
async fn given_inserted_two(world: &mut RegistryWorld, n1: String, n2: String) {
    for name in [n1, n2] {
        let actor = spawn_foo().await;
        assert!(world.reg().lock().unwrap().insert(name.clone(), actor.clone()));
        world.actors.push((name, actor));
    }
}

#[given(regex = r"^a name of (\d+) characters$")]
async fn given_long_name(world: &mut RegistryWorld, n: usize) {
    world.long_name = Some("x".repeat(n));
}

// Appears as both a Given (preconditions) and a When (the @lifecycle scenario's
// action) in the feature, so it is registered under both attributes.
#[given(regex = r#"^"([^"]*)" is stopped gracefully and shutdown completes$"#)]
#[when(regex = r#"^"([^"]*)" is stopped gracefully and shutdown completes$"#)]
async fn given_stopped_gracefully(world: &mut RegistryWorld, label: String) {
    let actor = world
        .actors
        .iter()
        .find(|(n, _)| n == &label)
        .map(|(_, a)| a.clone())
        .expect("a kept-alive actor under that label");
    stop_and_settle(&actor).await;
}

#[given(regex = r#"^"([^"]*)" is stopped and its entry is removed from the registry$"#)]
async fn given_stopped_and_removed(world: &mut RegistryWorld, label: String) {
    let actor = world
        .actors
        .iter()
        .find(|(n, _)| n == &label)
        .map(|(_, a)| a.clone())
        .expect("a kept-alive actor to stop and remove");
    stop_and_settle(&actor).await;
    // The dead entry was registered under "alpha" in the prior Given.
    let removed = world.reg().lock().unwrap().remove("alpha");
    assert!(removed, "removing the dead entry must succeed");
}

#[given(regex = r"^(\d+) tasks each holding a distinct running actor of type Foo$")]
async fn given_n_tasks_distinct_actors(world: &mut RegistryWorld, n: usize) {
    for i in 0..n {
        let actor = spawn_foo().await;
        world.actors.push((format!("task-{i}"), actor));
    }
}

#[given(
    regex = r"^(\d+) tasks each holding a distinct running actor of type Foo and a distinct name$"
)]
async fn given_n_tasks_distinct_names(world: &mut RegistryWorld, n: usize) {
    for i in 0..n {
        let actor = spawn_foo().await;
        world.actors.push((format!("name-{i}"), actor));
    }
}

// ===========================================================================
// When — single ops
// ===========================================================================

#[when(regex = r#"^"([^"]*)" is inserted under name "([^"]*)"$"#)]
async fn when_inserted_under_name(world: &mut RegistryWorld, label: String, name: String) {
    let actor = world
        .actors
        .iter()
        .find(|(n, _)| n == &label)
        .map(|(_, a)| a.clone())
        .expect("a kept-alive actor under that label");
    world.last_insert = Some(world.reg().lock().unwrap().insert(name, actor));
}

#[when(regex = r#"^"([^"]*)" is inserted under that long name$"#)]
async fn when_inserted_long(world: &mut RegistryWorld, label: String) {
    let name = world.long_name.clone().expect("long name built");
    let actor = world
        .actors
        .iter()
        .find(|(n, _)| n == &label)
        .map(|(_, a)| a.clone())
        .expect("a kept-alive actor under that label");
    world.last_insert = Some(world.reg().lock().unwrap().insert(name, actor));
}

/// `"X" is inserted under "Y"` (no "name " keyword). Used by both the
/// two-distinct-names scenario (which asserts `both inserts return true` over
/// `insert_pair`) and the re-register scenario (which asserts `the insert
/// returns true` over `last_insert`), so record BOTH observables.
#[when(regex = r#"^"([^"]*)" is inserted under "([^"]*)"$"#)]
async fn when_inserted_under(world: &mut RegistryWorld, label: String, name: String) {
    let actor = world
        .actors
        .iter()
        .find(|(n, _)| n == &label)
        .map(|(_, a)| a.clone())
        .expect("a kept-alive actor under that label");
    let won = world.reg().lock().unwrap().insert(name, actor);
    world.insert_pair.push(won);
    world.last_insert = Some(won);
}

#[when(regex = r#"^get::<Foo>\("missing"\) is called$"#)]
async fn when_get_missing(world: &mut RegistryWorld) {
    let res: Result<Option<ActorRef<Foo>>, RegistryError> =
        world.reg().lock().unwrap().get("missing");
    world.concurrent_get = Some(res.expect("get must not error on a missing name").map(|r| r.id()));
}

#[when(regex = r#"^get::<Bar>\("alpha"\) is called$"#)]
async fn when_get_wrong_type(world: &mut RegistryWorld) {
    let res: Result<Option<ActorRef<Bar>>, RegistryError> =
        world.reg().lock().unwrap().get("alpha");
    // Stash whether it was the expected BadActorType for the Then.
    world.send_err_not_running = Some(matches!(res, Err(RegistryError::BadActorType)));
}

#[when(regex = r#"^"([^"]*)" is removed$"#)]
async fn when_removed(world: &mut RegistryWorld, name: String) {
    world.last_remove = Some(world.reg().lock().unwrap().remove(name.as_str()));
}

#[when(regex = r#"^remove_by_id\(([A-Za-z0-9]+)'s id\) is called$"#)]
async fn when_remove_by_id(world: &mut RegistryWorld, label: String) {
    let id = if label == "A1" {
        self_id_a1(world)
    } else {
        world.a2_foo.as_ref().expect("A2 spawned").id()
    };
    world.last_remove = Some(world.reg().lock().unwrap().remove_by_id(&id));
}

#[when(regex = r#"^remove_by_id\(([A-Za-z0-9]+)'s id\) is called a second time$"#)]
async fn when_remove_by_id_again(world: &mut RegistryWorld, _label: String) {
    let id = self_id_a1(world);
    world.last_remove = Some(world.reg().lock().unwrap().remove_by_id(&id));
}

/// Resolves A1's id — A1 is the first kept-alive Foo (registered under one or
/// more names). All names for A1 share one ref/id.
fn self_id_a1(world: &RegistryWorld) -> ActorId {
    world
        .actors
        .first()
        .map(|(_, a)| a.id())
        .expect("A1 spawned")
}

#[when(regex = r#"^actors are inserted under "([^"]*)", "([^"]*)" and "([^"]*)"$"#)]
async fn when_inserted_three(world: &mut RegistryWorld, a: String, b: String, c: String) {
    for name in [a, b, c] {
        let actor = spawn_foo().await;
        assert!(world.reg().lock().unwrap().insert(name.clone(), actor.clone()));
        world.actors.push((name, actor));
    }
}

#[when(regex = r"^clear\(\) is called$")]
async fn when_clear(world: &mut RegistryWorld) {
    world.reg().lock().unwrap().clear();
}

#[when(regex = r#"^the ref obtained from get::<Foo>\("alpha"\) is told a message$"#)]
async fn when_tell_stale(world: &mut RegistryWorld) {
    let stale: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get("alpha").expect("get must not error");
    let stale = stale.expect("the stale ref must still be present in the registry");
    let res = stale.tell(Ping).await;
    world.send_err_not_running = Some(matches!(res, Err(SendError::ActorNotRunning(_))));
}

// ===========================================================================
// When — concurrent (real overlap via Barrier + tokio::spawn)
// ===========================================================================

#[when(regex = r#"^all (\d+) tasks concurrently insert under the same name "([^"]*)" under a barrier$"#)]
async fn when_concurrent_same_name(world: &mut RegistryWorld, n: usize, name: String) {
    concurrent_same_name(world, n, name).await;
}

async fn concurrent_same_name(world: &mut RegistryWorld, n: usize, name: String) {
    let reg = world.reg();
    let actors: Vec<ActorRef<Foo>> = world.actors.iter().map(|(_, a)| a.clone()).collect();
    assert_eq!(actors.len(), n, "expected exactly {n} distinct actors");
    let barrier = Arc::new(Barrier::new(n));
    let tasks: Vec<_> = actors
        .into_iter()
        .map(|actor| {
            let reg = Arc::clone(&reg);
            let barrier = Arc::clone(&barrier);
            let name = name.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let won = reg.lock().unwrap().insert(name, actor.clone());
                (won, actor.id())
            })
        })
        .collect();
    let mut results = Vec::with_capacity(n);
    let mut winner = None;
    for t in tasks {
        let (won, id) = t.await.expect("insert task must not panic");
        if won {
            winner = Some(id);
        }
        results.push(won);
    }
    world.concurrent_results = results;
    world.winner_id = winner;
}

#[when(regex = r"^all (\d+) tasks concurrently insert under a barrier$")]
async fn when_concurrent_distinct(world: &mut RegistryWorld, n: usize) {
    let reg = world.reg();
    let named: Vec<(String, ActorRef<Foo>)> = world.actors.clone();
    assert_eq!(named.len(), n, "expected exactly {n} distinct (name, actor) pairs");
    let barrier = Arc::new(Barrier::new(n));
    let tasks: Vec<_> = named
        .into_iter()
        .map(|(name, actor)| {
            let reg = Arc::clone(&reg);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                let won = reg.lock().unwrap().insert(name.clone(), actor.clone());
                (won, name, actor.id())
            })
        })
        .collect();
    let mut all_won = true;
    let mut pairs = Vec::with_capacity(n);
    for t in tasks {
        let (won, name, id) = t.await.expect("insert task must not panic");
        all_won &= won;
        pairs.push((name, id));
    }
    assert!(all_won, "every distinct-name insert must win");
    world.distinct_pairs = pairs;
}

#[when(
    regex = r#"^one task removes "([^"]*)" while another task concurrently calls get::<Foo>\("([^"]*)"\) under a barrier$"#
)]
async fn when_concurrent_get_during_remove(world: &mut RegistryWorld, rname: String, gname: String) {
    let reg = world.reg();
    let barrier = Arc::new(Barrier::new(2));

    let remove_reg = Arc::clone(&reg);
    let remove_barrier = Arc::clone(&barrier);
    let remover = tokio::spawn(async move {
        remove_barrier.wait().await;
        remove_reg.lock().unwrap().remove(rname.as_str())
    });

    let get_reg = Arc::clone(&reg);
    let get_barrier = Arc::clone(&barrier);
    let getter = tokio::spawn(async move {
        get_barrier.wait().await;
        let res: Result<Option<ActorRef<Foo>>, RegistryError> = get_reg.lock().unwrap().get(gname.as_str());
        // Must never be Err(BadActorType): assert that here so a torn read fails loudly.
        assert!(
            !matches!(res, Err(RegistryError::BadActorType)),
            "a concurrent get during remove must never observe BadActorType"
        );
        res.expect("get must not error").map(|r| r.id())
    });

    let _removed = remover.await.expect("remover task must not panic");
    world.concurrent_get = Some(getter.await.expect("getter task must not panic"));
}

// ===========================================================================
// Then — single-op assertions
// ===========================================================================

#[then(regex = r"^the insert returns true$")]
async fn then_insert_true(world: &mut RegistryWorld) {
    assert_eq!(world.last_insert, Some(true), "insert must return true");
}

#[then(regex = r"^the insert returns false$")]
async fn then_insert_false(world: &mut RegistryWorld) {
    assert_eq!(world.last_insert, Some(false), "duplicate insert must return false");
}

#[then(regex = r"^both inserts return true$")]
async fn then_both_inserts_true(world: &mut RegistryWorld) {
    assert_eq!(
        world.insert_pair,
        vec![true, true],
        "two distinct-name inserts must each return true"
    );
}

#[then(regex = r#"^get::<Foo>\("([^"]*)"\) returns Some\(ref\) whose id equals ([A-Za-z0-9]+)'s id$"#)]
async fn then_get_some_id(world: &mut RegistryWorld, name: String, label: String) {
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    let got = got.expect("expected Some(ref)");
    assert_eq!(got.id(), world.id_for(&label), "resolved ref must carry the expected id");
}

#[then(regex = r"^get::<Foo>\(the long name\) returns Some\(ref\) whose id equals ([A-Za-z0-9]+)'s id$")]
async fn then_get_long_some_id(world: &mut RegistryWorld, label: String) {
    let name = world.long_name.clone().expect("long name built");
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    let got = got.expect("expected Some(ref) for the long name");
    assert_eq!(got.id(), world.id_for(&label), "long-name ref must carry the expected id");
}

#[then(regex = r"^it returns Ok\(None\)$")]
async fn then_returns_ok_none(world: &mut RegistryWorld) {
    assert_eq!(world.concurrent_get, Some(None), "expected Ok(None)");
}

#[then(regex = r#"^get::<Foo>\("([^"]*)"\) returns Ok\(None\)$"#)]
async fn then_get_ok_none(world: &mut RegistryWorld, name: String) {
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    assert!(got.is_none(), "expected Ok(None) for {name:?}");
}

#[then(regex = r"^remove returns true$")]
async fn then_remove_true(world: &mut RegistryWorld) {
    assert_eq!(world.last_remove, Some(true), "remove must return true");
}

#[then(regex = r"^remove returns false$")]
async fn then_remove_false(world: &mut RegistryWorld) {
    assert_eq!(world.last_remove, Some(false), "remove of an absent name must return false");
}

#[then(regex = r"^it returns true$")]
async fn then_it_true(world: &mut RegistryWorld) {
    assert_eq!(world.last_remove, Some(true), "expected true");
}

#[then(regex = r"^it returns false$")]
async fn then_it_false(world: &mut RegistryWorld) {
    assert_eq!(world.last_remove, Some(false), "expected false");
}

#[then(regex = r"^it returns true and len\(\) returns (\d+)$")]
async fn then_true_and_len(world: &mut RegistryWorld, n: usize) {
    assert_eq!(world.last_remove, Some(true), "expected true");
    assert_eq!(world.reg().lock().unwrap().len(), n, "len mismatch");
}

#[then(regex = r#"^contains_name\("([^"]*)"\) returns false$"#)]
async fn then_contains_false(world: &mut RegistryWorld, name: String) {
    assert!(!world.reg().lock().unwrap().contains_name(name.as_str()), "{name:?} must be absent");
}

#[then(regex = r#"^contains_name\("([^"]*)"\) returns true$"#)]
async fn then_contains_true(world: &mut RegistryWorld, name: String) {
    assert!(world.reg().lock().unwrap().contains_name(name.as_str()), "{name:?} must be present");
}

#[then(regex = r#"^contains_name\("([^"]*)"\) still returns true$"#)]
async fn then_contains_still_true(world: &mut RegistryWorld, name: String) {
    assert!(world.reg().lock().unwrap().contains_name(name.as_str()), "{name:?} must still be present");
}

#[then(regex = r#"^get::<Foo>\("([^"]*)"\) still returns the ref whose id equals ([A-Za-z0-9]+)'s id$"#)]
async fn then_get_still_id(world: &mut RegistryWorld, name: String, label: String) {
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    let got = got.expect("the original ref must remain (no overwrite)");
    assert_eq!(got.id(), world.id_for(&label), "the existing entry must be unchanged");
}

#[then(regex = r#"^get::<Foo>\("([^"]*)"\) still returns ([A-Za-z0-9]+)'s ref$"#)]
async fn then_get_still_ref(world: &mut RegistryWorld, name: String, label: String) {
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    let got = got.expect("the independent key must remain after the other is removed");
    assert_eq!(got.id(), world.id_for(&label), "the surviving key must resolve to {label}");
}

#[then(regex = r"^it returns Err\(RegistryError::BadActorType\)$")]
async fn then_bad_actor_type(world: &mut RegistryWorld) {
    assert_eq!(
        world.send_err_not_running,
        Some(true),
        "a wrong-type get must return Err(RegistryError::BadActorType)"
    );
}

#[then(regex = r"^it returns Err\(RegistryError::BadActorType\) for every such A, B, and name$")]
async fn then_bad_actor_type_forall(world: &mut RegistryWorld) {
    assert_eq!(
        world.send_err_not_running,
        Some(true),
        "every wrong-type get must return BadActorType"
    );
}

#[then(
    regex = r#"^get::<Foo>\("([^"]*)"\) and get::<Foo>\("([^"]*)"\) both return refs with ([A-Za-z0-9]+)'s id$"#
)]
async fn then_both_resolve_id(world: &mut RegistryWorld, n1: String, n2: String, label: String) {
    let expected = world.id_for(&label);
    let reg = world.reg();
    let mut guard = reg.lock().unwrap();
    for name in [n1, n2] {
        let got: Option<ActorRef<Foo>> = guard.get(name.as_str()).expect("get must not error");
        let got = got.unwrap_or_else(|| panic!("expected Some(ref) for {name:?}"));
        assert_eq!(got.id(), expected, "{name:?} must resolve to {label}'s id");
    }
}

#[then(regex = r"^the send returns SendError::ActorNotRunning$")]
async fn then_send_not_running(world: &mut RegistryWorld) {
    assert_eq!(
        world.send_err_not_running,
        Some(true),
        "telling a stale ref must return SendError::ActorNotRunning"
    );
}

#[then(regex = r#"^get::<Foo>\("alpha"\) returns the ref whose id equals ([A-Za-z0-9]+)'s id$"#)]
async fn then_get_alpha_id(world: &mut RegistryWorld, label: String) {
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get("alpha").expect("get must not error");
    let got = got.expect("expected Some(ref)");
    assert_eq!(got.id(), world.id_for(&label), "resolved ref must carry the expected id");
}

// ===========================================================================
// Then — len / is_empty / names tracking
// ===========================================================================

#[then(regex = r"^is_empty\(\) returns true and len\(\) returns 0$")]
async fn then_empty_zero(world: &mut RegistryWorld) {
    let reg = world.reg();
    let guard = reg.lock().unwrap();
    assert!(guard.is_empty(), "is_empty must be true");
    assert_eq!(guard.len(), 0, "len must be 0");
}

#[then(regex = r"^len\(\) returns (\d+) and is_empty\(\) returns false$")]
async fn then_len_not_empty(world: &mut RegistryWorld, n: usize) {
    let reg = world.reg();
    let guard = reg.lock().unwrap();
    assert_eq!(guard.len(), n, "len mismatch");
    assert!(!guard.is_empty(), "is_empty must be false");
}

#[then(regex = r"^len\(\) returns (\d+)$")]
async fn then_len(world: &mut RegistryWorld, n: usize) {
    assert_eq!(world.reg().lock().unwrap().len(), n, "len mismatch");
}

#[then(regex = r#"^names\(\) yields exactly the set \{(.+)\}$"#)]
async fn then_names_exactly_set(world: &mut RegistryWorld, set: String) {
    assert_names(world, &set);
}

#[then(regex = r#"^names\(\) yields exactly \{(.+)\}$"#)]
async fn then_names_exactly(world: &mut RegistryWorld, set: String) {
    assert_names(world, &set);
}

#[then(regex = r#"^len\(\) returns (\d+) and names\(\) yields exactly \{(.+)\}$"#)]
async fn then_len_and_names(world: &mut RegistryWorld, n: usize, set: String) {
    assert_eq!(world.reg().lock().unwrap().len(), n, "len mismatch");
    assert_names(world, &set);
}

fn assert_names(world: &RegistryWorld, set: &str) {
    let expected: HashSet<String> = set
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();
    let reg = world.reg();
    let guard = reg.lock().unwrap();
    let actual: HashSet<String> = guard.names().map(|c| c.to_string()).collect();
    assert_eq!(actual, expected, "names() must yield exactly the expected set");
}

// ===========================================================================
// Then — remove_by_id conserved-facts (two names, one id)
// ===========================================================================

#[then(regex = r#"^exactly one of contains_name\("([^"]*)"\) / contains_name\("([^"]*)"\) is now false$"#)]
async fn then_exactly_one_false(world: &mut RegistryWorld, a: String, b: String) {
    let reg = world.reg();
    let guard = reg.lock().unwrap();
    let a_present = guard.contains_name(a.as_str());
    let b_present = guard.contains_name(b.as_str());
    assert!(
        a_present ^ b_present,
        "exactly one of {a:?}/{b:?} must remain (got a={a_present}, b={b_present})"
    );
}

#[then(regex = r"^the still-present name resolves to a ref whose id equals ([A-Za-z0-9]+)'s id$")]
async fn then_surviving_resolves(world: &mut RegistryWorld, label: String) {
    let id = self_id_a1(world);
    let reg = world.reg();
    let mut guard = reg.lock().unwrap();
    // Whichever of the two names survives must resolve to A1's id.
    let alpha: Option<ActorRef<Foo>> = guard.get("alpha").expect("get must not error");
    let survivor = match alpha {
        Some(r) => r,
        None => guard
            .get("beta")
            .expect("get must not error")
            .expect("one name must survive the first remove_by_id"),
    };
    assert_eq!(survivor.id(), id, "the surviving name must resolve to A1's id ({label})");
}

// ===========================================================================
// Then — concurrency
// ===========================================================================

#[then(regex = r"^exactly one insert returns true and the other (\d+) return false$")]
async fn then_one_winner(world: &mut RegistryWorld, losers: usize) {
    let wins = world.concurrent_results.iter().filter(|w| **w).count();
    let total = world.concurrent_results.len();
    assert_eq!(wins, 1, "exactly one insert must win, got {wins} (of {total})");
    assert_eq!(
        total - wins,
        losers,
        "the remaining {losers} inserts must lose"
    );
}

#[then(regex = r"^exactly one insert returns true and the other K-1 return false$")]
async fn then_one_winner_k(world: &mut RegistryWorld) {
    let wins = world.concurrent_results.iter().filter(|w| **w).count();
    assert_eq!(wins, 1, "exactly one insert must win across K threads, got {wins}");
}

#[then(regex = r#"^get::<Foo>\("([^"]*)"\) returns the ref belonging to the single winning task$"#)]
async fn then_get_winner(world: &mut RegistryWorld, name: String) {
    let winner = world.winner_id.expect("a single winner id");
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get(name.as_str()).expect("get must not error");
    let got = got.expect("the winning ref must be stored");
    assert_eq!(got.id(), winner, "the stored ref must be the winning task's");
}

#[then(regex = r#"^get::<Foo>\(name\) afterwards returns the ref belonging to the single winning thread$"#)]
async fn then_get_winner_name(world: &mut RegistryWorld) {
    let winner = world.winner_id.expect("a single winner id");
    let got: Option<ActorRef<Foo>> =
        world.reg().lock().unwrap().get("alpha").expect("get must not error");
    let got = got.expect("the winning ref must be stored");
    assert_eq!(got.id(), winner, "the stored ref must be the winning thread's");
}

#[then(regex = r"^every insert returns true$")]
async fn then_every_insert_true(world: &mut RegistryWorld) {
    assert!(!world.distinct_pairs.is_empty(), "distinct inserts must have run");
}

#[then(regex = r"^len\(\) returns (\d+) and each name resolves to its own actor's id$")]
async fn then_len_and_each_resolves(world: &mut RegistryWorld, n: usize) {
    let pairs = world.distinct_pairs.clone();
    assert_eq!(pairs.len(), n, "expected {n} distinct pairs");
    let reg = world.reg();
    let mut guard = reg.lock().unwrap();
    assert_eq!(guard.len(), n, "len must equal the number of distinct names");
    for (name, id) in &pairs {
        let got: Option<ActorRef<Foo>> = guard.get(name.as_str()).expect("get must not error");
        let got = got.expect("each distinct name must be present");
        assert_eq!(got.id(), *id, "name {name:?} must resolve to its own actor's id");
    }
}

#[then(
    regex = r"^the get returns either Ok\(Some\(ref with ([A-Za-z0-9]+)'s id\)\) or Ok\(None\)$"
)]
async fn then_get_some_or_none(world: &mut RegistryWorld, label: String) {
    match world.concurrent_get {
        Some(Some(id)) => assert_eq!(
            id,
            world.id_for(&label),
            "if Some, the ref must carry {label}'s id"
        ),
        Some(None) => { /* the after-remove state is also legal */ }
        None => panic!("the concurrent get produced no observation"),
    }
}

#[then(regex = r"^it never returns Err\(BadActorType\) and never panics$")]
async fn then_never_err_never_panic(world: &mut RegistryWorld) {
    // The getter task asserted no BadActorType inline and would have panicked the
    // join otherwise; reaching here with a recorded observation proves both.
    assert!(
        world.concurrent_get.is_some(),
        "the concurrent get must have produced a legal observation (no panic)"
    );
}

// ===========================================================================
// @model — refinement of an insert-no-overwrite map over an op sequence
// ===========================================================================

#[given(regex = r"^a running actor of any type A registered under any name$")]
async fn given_any_type_any_name(world: &mut RegistryWorld) {
    // Phase-2 @property law: register a Foo under each of the boundary names so
    // the wrong-type get can be checked for ALL of them. Store a Bar to query.
    for name in ["", "x", &"x".repeat(100_000)] {
        let actor = spawn_foo().await;
        assert!(world.reg().lock().unwrap().insert(name.to_string(), actor.clone()));
        world.actors.push((name.to_string(), actor));
    }
    world.bar = Some({
        let b = Bar::spawn(Bar);
        b.wait_for_startup().await;
        b
    });
}

#[when(regex = r"^get::<B>\(name\) is called with a type B that differs from A$")]
async fn when_get_b_differs(world: &mut RegistryWorld) {
    let reg = world.reg();
    let mut guard = reg.lock().unwrap();
    // For every registered name, a Bar-typed get must be BadActorType.
    let mut all_bad = true;
    for name in ["".to_string(), "x".to_string(), "x".repeat(100_000)] {
        let res: Result<Option<ActorRef<Bar>>, RegistryError> = guard.get(name.as_str());
        all_bad &= matches!(res, Err(RegistryError::BadActorType));
    }
    world.send_err_not_running = Some(all_bad);
}

#[given(
    regex = r"^any sequence of insert / remove / get / contains_name / clear operations over a small name set$"
)]
async fn given_op_sequence(_world: &mut RegistryWorld) {
    // The deterministic op sequence is applied in the When (it needs spawned
    // actors and a reference model side by side).
}

#[when(regex = r"^the operations are applied to the registry and to a reference model in the same order$")]
async fn when_apply_ops(world: &mut RegistryWorld) {
    run_model_check(world).await;
}

/// A deterministic op sequence (covering empty/duplicate/remove-of-absent/clear)
/// applied to the SUT and to a reference `HashMap<Name, ActorId>` insert-no-
/// overwrite model, asserting observable equality after EVERY op.
async fn run_model_check(world: &mut RegistryWorld) {
    use std::collections::HashMap;

    let reg = world.reg();
    let names = ["", "a", "b", &"x".repeat(1000)];
    // Pre-spawn one Foo per (op-index) so each insert has a distinct ref/id.
    #[derive(Clone)]
    enum Op {
        Insert(usize),
        Remove(usize),
        Contains(usize),
        Get(usize),
        Clear,
    }
    // A fixed, deterministic schedule that exercises every branch including the
    // empty prefix, duplicate inserts, and remove-of-absent.
    let schedule = [
        Op::Get(1),       // get on empty -> None
        Op::Contains(1),  // contains on empty -> false
        Op::Remove(1),    // remove-of-absent -> false
        Op::Insert(0),    // "" insert
        Op::Insert(1),    // "a" insert
        Op::Insert(1),    // "a" duplicate -> false, no overwrite
        Op::Insert(2),    // "b" insert
        Op::Get(1),       // present
        Op::Contains(2),  // present
        Op::Remove(1),    // remove "a"
        Op::Get(1),       // gone
        Op::Insert(3),    // long name
        Op::Clear,        // empty all
        Op::Get(3),       // gone after clear
    ];

    let mut model: HashMap<usize, ActorId> = HashMap::new();
    for op in schedule {
        match op {
            Op::Insert(i) => {
                let actor = spawn_foo().await;
                let id = actor.id();
                let won = reg.lock().unwrap().insert(names[i].to_string(), actor);
                let model_won = if model.contains_key(&i) {
                    false
                } else {
                    model.insert(i, id);
                    true
                };
                assert_eq!(won, model_won, "insert bool must match the no-overwrite model for {:?}", names[i]);
            }
            Op::Remove(i) => {
                let removed = reg.lock().unwrap().remove(names[i]);
                let model_removed = model.remove(&i).is_some();
                assert_eq!(removed, model_removed, "remove bool must match the model for {:?}", names[i]);
            }
            Op::Contains(i) => {
                let present = reg.lock().unwrap().contains_name(names[i]);
                assert_eq!(present, model.contains_key(&i), "contains must match the model for {:?}", names[i]);
            }
            Op::Get(i) => {
                let got: Option<ActorRef<Foo>> =
                    reg.lock().unwrap().get(names[i]).expect("get must not error");
                assert_eq!(
                    got.map(|r| r.id()),
                    model.get(&i).copied(),
                    "get must match the model id for {:?}",
                    names[i]
                );
            }
            Op::Clear => {
                reg.lock().unwrap().clear();
                model.clear();
            }
        }
        // Observable state equality after EVERY op.
        assert_eq!(reg.lock().unwrap().len(), model.len(), "len must equal the model size after every op");
    }
    world.is_empty_obs = Some(reg.lock().unwrap().is_empty());
    world.len_obs = Some(reg.lock().unwrap().len());
}

#[then(regex = r"^after every operation the registry's observable state equals the model's$")]
async fn then_model_equal(world: &mut RegistryWorld) {
    // The per-op equality is asserted inside `run_model_check`; the final state
    // (empty after the closing clear) is the conserved summary.
    assert_eq!(world.is_empty_obs, Some(true), "the schedule ends empty (closing clear)");
    assert_eq!(world.len_obs, Some(0), "final len must be 0");
}

#[then(
    regex = r"^an insert under an already-present name returns false and leaves the existing ref unchanged$"
)]
async fn then_insert_no_overwrite(world: &mut RegistryWorld) {
    // Directly exercise the no-overwrite invariant on the SUT (independent of the
    // schedule): insert two distinct refs under one name; second loses; first stays.
    let reg = world.reg();
    let first = spawn_foo().await;
    let second = spawn_foo().await;
    let first_id = first.id();
    assert!(reg.lock().unwrap().insert("dup".to_string(), first), "first insert wins");
    assert!(!reg.lock().unwrap().insert("dup".to_string(), second), "duplicate insert must return false");
    let got: Option<ActorRef<Foo>> = reg.lock().unwrap().get("dup").expect("get must not error");
    assert_eq!(got.map(|r| r.id()), Some(first_id), "the existing ref must be unchanged");
}

#[given(regex = r"^any number K of threads each holding a distinct running actor of type Foo$")]
async fn given_k_threads(world: &mut RegistryWorld) {
    // K = 2 (the boundary the GEN note mandates) plus a few more spawned actors.
    for i in 0..2usize {
        let actor = spawn_foo().await;
        world.actors.push((format!("k-{i}"), actor));
    }
}

#[when(regex = r"^all K threads concurrently insert under the SAME name under a barrier$")]
async fn when_k_concurrent_same(world: &mut RegistryWorld) {
    let n = world.actors.len();
    concurrent_same_name(world, n, "alpha".to_string()).await;
}
