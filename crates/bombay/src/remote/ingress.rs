use core::fmt;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc::{Sender, error::TrySendError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// A refused ingress transfer, retaining its original and actual native cause.
///
/// Native callback originals are static; diagnostics omit their payloads and
/// expose the actual native cause through `Error::source`.
pub enum IngressError<Original: 'static> {
    /// No count charge was acquired; the original remains unqueued.
    Capacity {
        original: Original,
        cause: TryAcquireError,
    },
    /// The channel refused the original and its acquired count charge.
    Queue {
        cause: TrySendError<(Original, OwnedSemaphorePermit)>,
    },
}

impl<Original: 'static> fmt::Display for IngressError<Original> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { cause, .. } => write!(formatter, "ingress count unavailable: {cause}"),
            Self::Queue { cause } => write!(formatter, "ingress channel refused transfer: {cause}"),
        }
    }
}

impl<Original: 'static> Error for IngressError<Original> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Capacity { cause, .. } => Some(cause),
            Self::Queue { cause } => Some(cause),
        }
    }
}

impl<Original: 'static> fmt::Debug for IngressError<Original> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity { cause, .. } => formatter
                .debug_struct("Capacity")
                .field("cause", cause)
                .finish_non_exhaustive(),
            Self::Queue { cause } => formatter
                .debug_struct("Queue")
                .field("cause", cause)
                .finish(),
        }
    }
}

/// Transfer one original and its count charge without waiting or copying it.
///
/// `capacity` counts queued, active and acquired-but-unread originals across
/// the channels sharing it. Keep each received `(original, permit)` intact
/// until that original is disposed; tuple destruction disposes it first.
/// Dequeue and provider completion do not themselves release the charge.
/// A channel refusal retains the charge beside the refused original.
///
/// This is ingress retention, not authentication or actor-mailbox admission.
/// The caller selects the capacity and owns both existing Tokio primitives.
/// Unpacking early or cloning an original can defeat this count discipline.
/// Native allocations, bytes, producer tasks and verification are not bounded
/// by this operation. It introduces no defaults, waits or background work.
///
/// # Errors
/// Returns the exact original with its native capacity failure, or the native
/// channel failure containing that original and its still-owned count charge.
pub fn try_retain<Original: 'static>(
    ingress: &Sender<(Original, OwnedSemaphorePermit)>,
    capacity: &Arc<Semaphore>,
    original: Original,
) -> Result<(), IngressError<Original>> {
    let permit = match Arc::clone(capacity).try_acquire_owned() {
        Ok(permit) => permit,
        Err(cause) => return Err(IngressError::Capacity { original, cause }),
    };
    ingress
        .try_send((original, permit))
        .map_err(|cause| IngressError::Queue { cause })
}
