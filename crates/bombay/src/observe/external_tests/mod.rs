#[cfg(not(loom))]
use std::thread::{self, ThreadId};
#[cfg(not(loom))]
use std::time::{Duration, Instant};

#[cfg(not(loom))]
use super::{Observation, Waiter, lock};

#[cfg(not(loom))]
pub(super) fn wait_for_thread_registrations<O>(
    observation: &Observation<O>,
    expected_thread_order: &[ThreadId],
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let registrations = lock(observation.slot.waiters());
        let observed_thread_order =
            registrations
                .iter()
                .filter_map(|registration| match registration {
                    Waiter::Thread(thread) => Some(thread.id()),
                    Waiter::Waker { .. } => None,
                });
        if observed_thread_order.eq(expected_thread_order.iter().copied()) {
            return;
        }
        drop(registrations);
        assert!(
            Instant::now() < deadline,
            "thread registration order was not observed"
        );
        thread::yield_now();
    }
}

#[cfg(not(loom))]
mod affine;
#[cfg(not(loom))]
mod contract;
#[cfg(not(loom))]
mod exhaustive;
#[cfg(not(loom))]
mod future_cancel;
#[cfg(loom)]
mod loom_external;
#[cfg(not(loom))]
mod model;
#[cfg(not(loom))]
mod pair;
#[cfg(not(loom))]
mod panic_safety;
#[cfg(not(loom))]
mod pool;
#[cfg(not(loom))]
mod stress;
