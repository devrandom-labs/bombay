//! The ONE spawn door: [`RunKind`] discharges the compile-time-selected
//! loop shape onto the [`PreparedActor`] floor paths; [`spawn`] and
//! [`spawn_with`] are the two ergonomic entries.

use crate::actor::{PreparedActor, SpawnConfig};

use super::{
    Actor, Handle, HasSupervising, HasWatching, LinkedRun, PlainRun, SelectRunner, Shell,
    SupervisedRun,
};

/// Runs a selected loop shape (ADR-0026 stage 3, spike-280).
///
/// Each marker spawns the [`Shell`] onto its [`PreparedActor`] floor path.
/// The obligation `SelectRunner::Runner: RunKind<A>` is discharged at the
/// one [`spawn`] — monomorphized; the "branch" is trait resolution, not
/// code.
pub trait RunKind<A: Actor> {
    /// Spawns the actor onto this loop shape.
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A>;
}

impl<A: Actor> RunKind<A> for PlainRun {
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let prepared = PreparedActor::<Shell<A>>::new(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn(args);
        handle
    }
}

impl<A: Actor> RunKind<A> for LinkedRun
where
    A::Caps: HasWatching<A>,
{
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let (prepared, link_rx) = PreparedActor::<Shell<A>>::new_linked(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn_linked_task(args, link_rx);
        handle
    }
}

impl<A: Actor> RunKind<A> for SupervisedRun
where
    A::Caps: HasSupervising<A>,
{
    fn spawn_with(config: SpawnConfig, args: A::Args) -> Handle<A> {
        let (prepared, link_rx) = PreparedActor::<Shell<A>>::new_linked(config);
        let handle = prepared.actor_ref().clone();
        let _join = prepared.spawn_supervised_task(args, link_rx);
        handle
    }
}

/// Spawns a `capability` actor with the default [`SpawnConfig`] — the ONE
/// ergonomic entry.
///
/// The loop shape is selected from [`Actor::Caps`] at compile time
/// (monomorphized, no runtime branch): plain sets run the one-arm loop,
/// [`Watching`](super::Watching) sets the linked loop, [`Supervising`](super::Supervising) sets the supervised
/// loop (ADR-0026 stage 3).
///
/// A `Supervising` set without `Watching` does not compile — the
/// composition law rides the [`HasSupervising`] supertrait (and the
/// derive rejects it with a readable error):
///
/// ```compile_fail
/// #[derive(bombay_macros::Provide)]
/// struct RogueCaps {
///     supervising: bombay::capability::Supervising<bombay::capability::OneForOne>,
/// }
/// ```
#[must_use]
pub fn spawn<A: Actor>(args: A::Args) -> Handle<A>
where
    <A::Caps as SelectRunner<A>>::Runner: RunKind<A>,
{
    spawn_with(SpawnConfig::default(), args)
}

/// Spawns with an explicit [`SpawnConfig`] (mailbox capacity + stop
/// grace); loop shape selected exactly as [`spawn`].
#[must_use]
pub fn spawn_with<A: Actor>(config: SpawnConfig, args: A::Args) -> Handle<A>
where
    <A::Caps as SelectRunner<A>>::Runner: RunKind<A>,
{
    <<A::Caps as SelectRunner<A>>::Runner as RunKind<A>>::spawn_with(config, args)
}
