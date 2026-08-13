//! Actor terminal classifications.

/// The non-failing ways an actor process finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunExit<D> {
    /// The behavior explicitly stopped.
    Stopped(D),
    /// The environment closed before the behavior stopped.
    EnvironmentClosed,
}

/// A failure on either side of the behavior/environment boundary, or a
/// poisoned machine executor.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RunError<B, E> {
    /// The behavior transition failed.
    #[error("behavior transition failed: {0:?}")]
    Behavior(B),
    /// The environment rejected a successful transition's effects.
    #[error("environment rejected a successful transition's effects: {0:?}")]
    Environment(E),
    /// The machine executor was poisoned by a panicking transition and the
    /// driver was reused. The actor is terminal. The driver detects poison
    /// before polling the environment again, so it does not consume another
    /// input while reporting this marker.
    #[error("machine executor poisoned by a panicking transition")]
    Poisoned,
}
