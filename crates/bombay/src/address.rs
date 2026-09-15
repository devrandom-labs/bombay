//! Bombay's concrete local actor-address domain.

use core::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use behavior::{Address, AllocationRejection, EndpointAddress, Protocol};

use crate::local::ActorRef;

/// One logical address in Bombay's standard local runtime.
///
/// The value is a runtime identity at the ordinary application layer.
/// Creator-local child nonces are correlation values and are never converted
/// into addresses. Bombay allocates every local actor from one application
/// source and Address independently commits the resulting claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MailAddr(pub u64);

impl MailAddr {
    /// Stable logical address reserved for the one local application root.
    ///
    /// This is useful only when an owning actor template deliberately routes
    /// internal messages back to the application root by logical name.
    pub const APPLICATION_ROOT: Self = Self(0);
}

impl From<u64> for MailAddr {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<MailAddr> for u64 {
    fn from(value: MailAddr) -> Self {
        value.0
    }
}

impl Address for MailAddr {
    type Nonce = u64;
}

impl EndpointAddress for MailAddr {
    type Established<P>
        = ActorRef<P>
    where
        P: Protocol<Addr = Self>;
}

/// One never-wrapping address source shared by a complete local application.
#[derive(Clone)]
pub(crate) struct ApplicationAddresses {
    next: Arc<AtomicU64>,
}

impl ApplicationAddresses {
    pub(crate) fn new() -> Self {
        // Zero is reserved for the application root.
        Self::from_next(1)
    }

    pub(crate) fn from_next(next: u64) -> Self {
        Self {
            next: Arc::new(AtomicU64::new(next)),
        }
    }

    pub(crate) fn allocate(&self) -> Result<MailAddr, AllocationRejection> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |address| {
                address.checked_add(1)
            })
            .map(MailAddr)
            .map_err(|_| AllocationRejection::Exhausted)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn allocation_is_monotonic_and_never_uses_the_root() {
        let addresses = ApplicationAddresses::new();

        assert_eq!(MailAddr::APPLICATION_ROOT, MailAddr(0));
        assert_eq!(addresses.allocate(), Ok(MailAddr(1)));
        assert_eq!(addresses.allocate(), Ok(MailAddr(2)));
    }

    #[test]
    fn allocation_never_wraps() {
        let addresses = ApplicationAddresses::from_next(u64::MAX - 1);

        assert_eq!(addresses.allocate(), Ok(MailAddr(u64::MAX - 1)));
        assert_eq!(addresses.allocate(), Err(AllocationRejection::Exhausted));
        assert_eq!(addresses.allocate(), Err(AllocationRejection::Exhausted));
    }

    #[test]
    fn cloned_sources_allocate_one_unique_application_sequence() {
        const TASKS: usize = 8;
        const ALLOCATIONS_PER_TASK: usize = 512;

        let addresses = ApplicationAddresses::new();
        let mut tasks = Vec::with_capacity(TASKS);

        for _ in 0..TASKS {
            let addresses = addresses.clone();
            tasks.push(thread::spawn(move || {
                (0..ALLOCATIONS_PER_TASK)
                    .map(|_| {
                        addresses
                            .allocate()
                            .expect("the bounded test allocation cannot exhaust u64")
                    })
                    .collect::<Vec<_>>()
            }));
        }

        let mut allocated = tasks
            .into_iter()
            .flat_map(|task| {
                task.join()
                    .expect("the allocation worker must complete normally")
            })
            .collect::<Vec<_>>();
        allocated.sort_unstable();

        let expected = (1..=(TASKS * ALLOCATIONS_PER_TASK) as u64)
            .map(MailAddr)
            .collect::<Vec<_>>();
        assert_eq!(allocated, expected);
    }
}
