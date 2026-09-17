//! Generation-safe completion publication and observation.

use core::num::NonZeroUsize;
#[cfg(loom)]
use loom::sync::atomic::AtomicUsize;
use std::future::{Future, IntoFuture};
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
#[cfg(loom)]
use std::sync::PoisonError;
#[cfg(not(loom))]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
#[cfg(not(loom))]
use std::time::Instant;

#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::Mutex;
#[cfg(loom)]
use loom::thread::{Thread, current, park};
#[cfg(not(loom))]
use parking_lot::Mutex;
#[cfg(not(loom))]
use std::cell::UnsafeCell;
#[cfg(not(loom))]
use std::thread::{Thread, current, park, park_timeout};
#[cfg(not(loom))]
use triomphe::Arc;

/// Acquire a mutex, unwrapping poisoning. std's `Mutex` poisons (and, on
/// macOS, lazily heap-allocates its pthread mutex on first lock);
/// `parking_lot`'s is inline and does not poison. The loom build models the
/// same lock protocol with loom's scheduler.
#[cfg(loom)]
fn lock<T>(mutex: &loom::sync::Mutex<T>) -> loom::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(recover)
}

#[cfg(not(loom))]
fn lock<T>(mutex: &parking_lot::Mutex<T>) -> parking_lot::MutexGuard<'_, T> {
    mutex.lock()
}

#[cfg(loom)]
fn recover<T>(error: PoisonError<T>) -> T {
    error.into_inner()
}

/// Park until woken or the deadline passes; returns whether the deadline has
/// not yet passed. Under loom, park without a timeout (the model has no
/// clock, and the registration protocol is what matters).
#[cfg(not(loom))]
fn park_until(deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    park_timeout(deadline - now);
    true
}

/// Park until woken (loom variant; no clock).
#[cfg(loom)]
fn park_until() -> bool {
    park();
    true
}

/// Completion-state bits for [`Slot`].
const COMPLETED: usize = 1 << 0;
const HAS_WAITER: usize = 1 << 1;
/// Set exactly while the outcome cell holds a live value. Rides the state
/// word (set by `complete`'s RMW, cleared by
/// an ownership-transferring take) so the outcome cell needs no separate tag:
/// the validity gate is `COMPLETED` for readers, `OUTCOME_VALID` for droppers.
const OUTCOME_VALID: usize = 1 << 2;

/// A registered waiter: either a blocked thread (sync [`Observation::wait`])
/// or an async waker ([`Observation::register_waker`]).
enum Waiter {
    Thread(Thread),
    Waker {
        waker: Waker,
        ownership: WakerOwnership,
    },
}

enum WakerOwnership {
    Futures(NonZeroUsize),
    Persistent { future_owners: usize },
}

/// Inline-first waiter storage.
///
/// The overwhelmingly common async shape has one task waiting for one
/// publication. Keeping that first waiter inside the slot means polling an
/// affine observation performs no allocation beyond the slot's existing
/// `Arc`; shared fan-out promotes to a `Vec` only when a second distinct
/// waiter is installed.
#[derive(Default)]
enum Waiters {
    #[default]
    Empty,
    One(Waiter),
    Many(Vec<Waiter>),
}

impl Waiters {
    fn push(&mut self, waiter: Waiter) {
        match mem::take(self) {
            Self::Empty => *self = Self::One(waiter),
            Self::One(first) => *self = Self::Many(vec![first, waiter]),
            Self::Many(mut waiters) => {
                waiters.push(waiter);
                *self = Self::Many(waiters);
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&Waiter) -> bool) {
        match mem::take(self) {
            Self::Empty => {}
            Self::One(waiter) => {
                if keep(&waiter) {
                    *self = Self::One(waiter);
                }
            }
            Self::Many(mut waiters) => {
                waiters.retain(&mut keep);
                *self = match waiters.len() {
                    0 => Self::Empty,
                    1 => Self::One(waiters.pop().expect("one waiter remains")),
                    _ => Self::Many(waiters),
                };
            }
        }
    }

    fn retain_mut(&mut self, mut keep: impl FnMut(&mut Waiter) -> bool) {
        match mem::take(self) {
            Self::Empty => {}
            Self::One(mut waiter) => {
                if keep(&mut waiter) {
                    *self = Self::One(waiter);
                }
            }
            Self::Many(mut waiters) => {
                waiters.retain_mut(&mut keep);
                *self = match waiters.len() {
                    0 => Self::Empty,
                    1 => Self::One(waiters.pop().expect("one waiter remains")),
                    _ => Self::Many(waiters),
                };
            }
        }
    }

    fn for_each(self, mut visit: impl FnMut(Waiter)) {
        match self {
            Self::Empty => {}
            Self::One(waiter) => visit(waiter),
            Self::Many(waiters) => waiters.into_iter().for_each(visit),
        }
    }
}

impl Deref for Waiters {
    type Target = [Waiter];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[],
            Self::One(waiter) => std::slice::from_ref(waiter),
            Self::Many(waiters) => waiters,
        }
    }
}

impl DerefMut for Waiters {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Empty => &mut [],
            Self::One(waiter) => std::slice::from_mut(waiter),
            Self::Many(waiters) => waiters,
        }
    }
}

/// One completion cell: a lock-free outcome publication point plus a
/// mutex-protected waiter registry.
///
/// Summary of the safety invariants:
/// - The outcome cell is written exactly once, by `complete`, and the write
///   happens-before the `COMPLETED|OUTCOME_VALID` bits are set by the Release
///   `fetch_or`.
/// - The outcome cell is read only after observing COMPLETED via an
///   Acquire-or-stronger load of `state`, which synchronizes-with that
///   Release RMW.
/// - The outcome cell is dropped exactly once: by the slot's final drop, or
///   transferred by a unique take (which clears `OUTCOME_VALID`, so the
///   slot's final drop skips it).
/// - The `HAS_WAITER` bit and the `COMPLETED` bit share one word, so the two
///   RMWs are totally ordered by the modification order: a waiter that
///   completes its registration never parks without either seeing `COMPLETED`
///   on its post-push recheck or receiving an unpark token from the drain.
/// - Reclamation is entirely `Arc`-based; no raw pointer outlives the slot.
struct Slot<O> {
    state: AtomicUsize,
    // O-sized (no Option tag): the OUTCOME_VALID bit in `state` is the
    // liveness marker, and COMPLETED gates every read.
    outcome: UnsafeCell<MaybeUninit<O>>,
    // One waiter is retained inline in the slot allocation. Fan-out promotes
    // to a Vec only for a second distinct waiter.
    waiters: Mutex<Waiters>,
}

// SAFETY: the outcome cell is written exactly once, before the COMPLETED bit
// is published by the Release RMW, and read only after COMPLETED is observed
// (Acquire or stronger); while shared it is immutable. `O: Send + Sync`
// bounds the shared access to the stored value and its clones.
unsafe impl<O: Send + Sync> Sync for Slot<O> {}

impl<O> Slot<O> {
    /// Construct one fresh pending completion slot.
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            outcome: UnsafeCell::new(MaybeUninit::uninit()),
            waiters: Mutex::new(Waiters::Empty),
        }
    }

    /// Borrow the outcome cell.
    ///
    /// # Safety
    /// The caller must have observed the COMPLETED bit (Acquire or stronger);
    /// the single write happens-before that bit is set (Release RMW), so the
    /// cell holds a valid outcome.
    #[cfg(not(loom))]
    unsafe fn outcome_ref(&self) -> &O {
        // SAFETY: gated by the caller via the COMPLETED bit (invariant 2).
        unsafe { (*self.outcome.get()).assume_init_ref() }
    }

    /// Borrow the outcome cell (loom-checked access).
    ///
    /// # Safety
    /// Same gating as the non-loom branch; loom additionally verifies the
    /// access against its scheduling model.
    #[cfg(loom)]
    unsafe fn outcome_ref(&self) -> &O {
        self.outcome.with(|ptr| {
            // SAFETY: same gating as the non-loom branch; loom additionally
            // verifies the access against its scheduling model.
            unsafe { (*ptr).assume_init_ref() }
        })
    }

    /// Write the outcome cell.
    ///
    /// # Safety
    /// Called exactly once, before the COMPLETED bit is set.
    #[cfg(not(loom))]
    unsafe fn set_outcome(&self, outcome: O) {
        // SAFETY: single writer (invariant 1); the write happens-before the
        // COMPLETED|OUTCOME_VALID Release RMW.
        unsafe { (*self.outcome.get()).write(outcome) };
    }

    /// Write the outcome cell (loom-checked access).
    ///
    /// # Safety
    /// Called exactly once, before the COMPLETED bit is set.
    #[cfg(loom)]
    unsafe fn set_outcome(&self, outcome: O) {
        self.outcome.with_mut(|ptr| {
            // SAFETY: single writer (invariant 1).
            unsafe { (*ptr).write(outcome) };
        });
    }

    /// Drop the published outcome, if the cell holds one.
    ///
    /// # Safety
    /// No reader can observe the cell (a pooled slot has no observers; the
    /// subsequent store of `state` publishes the drop), and the caller
    /// guarantees `OUTCOME_VALID` is set (so the drop happens exactly once).
    #[cfg(not(loom))]
    unsafe fn drop_outcome(&self) {
        // SAFETY: no concurrent readers (pooled-slot invariant).
        unsafe { (*self.outcome.get()).assume_init_drop() };
    }

    /// Drop the published outcome (loom-checked access).
    ///
    /// # Safety
    /// Same as the non-loom branch.
    #[cfg(loom)]
    unsafe fn drop_outcome(&self) {
        self.outcome.with_mut(|ptr| {
            // SAFETY: same as the non-loom branch.
            unsafe { (*ptr).assume_init_drop() };
        });
    }

    /// Move the published outcome out of this slot.
    ///
    /// Returns `None` while publication is pending. Once publication is
    /// visible, `OUTCOME_VALID` is cleared before the value is moved so the
    /// slot destructor cannot drop it again.
    ///
    /// # Safety
    /// The caller must own the only authority that can read or take the
    /// outcome. This is true after `Arc::try_unwrap`, and for an affine pair's
    /// sole observation. Calling this while a shared observation can read the
    /// outcome would invalidate its reference.
    unsafe fn try_take_outcome(&self) -> Option<O> {
        let state = self.state.load(Ordering::Acquire);
        if state & COMPLETED == 0 {
            return None;
        }
        let previous = self.state.fetch_and(!OUTCOME_VALID, Ordering::Relaxed);
        assert_ne!(
            previous & OUTCOME_VALID,
            0,
            "completed observation polled after yielding its outcome"
        );
        #[cfg(not(loom))]
        {
            // SAFETY: the caller guarantees unique outcome access, COMPLETED
            // was observed with Acquire, and this RMW claimed OUTCOME_VALID.
            Some(unsafe { self.outcome.get().read().assume_init() })
        }
        #[cfg(loom)]
        {
            Some(self.outcome.with(|ptr| {
                // SAFETY: same contract as the non-loom branch; loom checks
                // the cell access against modeled interleavings.
                unsafe { (*ptr).assume_init_read() }
            }))
        }
    }

    fn waiters(&self) -> &Mutex<Waiters> {
        &self.waiters
    }

    /// Install or migrate one future's logical waker registration.
    ///
    /// `previous` is the future's currently installed waker, if any. A
    /// migration first installs the replacement and then removes one logical
    /// owner from the previous physical entry under the same lock. Shared
    /// task wakers remain deduplicated and sibling ownership remains counted,
    /// while the migrating future leaves no stale registration behind.
    fn update_future_waker(&self, previous: Option<&Waker>, next: &Waker) -> bool {
        if self.state.load(Ordering::Acquire) & COMPLETED != 0 {
            return true;
        }
        self.state.fetch_or(HAS_WAITER, Ordering::SeqCst);
        let mut waiters = lock(self.waiters());
        if self.state.load(Ordering::SeqCst) & COMPLETED != 0 {
            return true;
        }
        if previous.is_some_and(|waker| waker.will_wake(next)) {
            return false;
        }

        let mut joined_existing = false;
        for waiter in waiters.iter_mut() {
            if let Waiter::Waker { waker, ownership } = waiter
                && waker.will_wake(next)
            {
                match ownership {
                    WakerOwnership::Futures(future_owners) => {
                        *future_owners = future_owners
                            .checked_add(1)
                            .expect("waker registration count overflowed");
                    }
                    WakerOwnership::Persistent { future_owners } => {
                        *future_owners = future_owners
                            .checked_add(1)
                            .expect("waker registration count overflowed");
                    }
                }
                joined_existing = true;
                break;
            }
        }
        if !joined_existing {
            waiters.push(Waiter::Waker {
                waker: next.clone(),
                ownership: WakerOwnership::Futures(NonZeroUsize::MIN),
            });
        }

        if let Some(previous) = previous {
            Self::remove_future_waker_owner(&mut waiters, previous);
        }
        false
    }

    /// Remove one future's logical ownership of its current waker.
    fn deregister_future_waker(&self, waker: &Waker) {
        let mut waiters = lock(self.waiters());
        Self::remove_future_waker_owner(&mut waiters, waker);
    }

    fn remove_future_waker_owner(waiters: &mut Waiters, target: &Waker) {
        let mut removed = false;
        waiters.retain_mut(|waiter| {
            let Waiter::Waker { waker, ownership } = waiter else {
                return true;
            };
            if removed || !waker.will_wake(target) {
                return true;
            }
            removed = true;
            match ownership {
                WakerOwnership::Futures(future_owners) => {
                    if let Some(remaining) = future_owners
                        .get()
                        .checked_sub(1)
                        .and_then(NonZeroUsize::new)
                    {
                        *future_owners = remaining;
                        true
                    } else {
                        false
                    }
                }
                WakerOwnership::Persistent { future_owners } => {
                    *future_owners = future_owners
                        .checked_sub(1)
                        .expect("future waker ownership was not registered");
                    true
                }
            }
        });
    }

    /// Publish the terminal outcome and notify every registered waiter.
    ///
    /// The caller owns the slot's unique publication authority and therefore
    /// invokes this operation at most once.
    fn complete(&self, outcome: O) {
        assert_eq!(
            self.state.load(Ordering::Acquire) & COMPLETED,
            0,
            "slot completed twice"
        );
        // SAFETY: the caller owns the unique publication authority; the
        // Release RMW below publishes the write to readers that observe
        // COMPLETED.
        unsafe { self.set_outcome(outcome) };
        let previous = self
            .state
            .fetch_or(COMPLETED | OUTCOME_VALID, Ordering::Release);
        if previous & HAS_WAITER != 0 {
            let waiters = {
                let mut waiters = lock(self.waiters());
                mem::take(&mut *waiters)
            };
            let mut first_panic = None;
            waiters.for_each(|waiter| {
                match waiter {
                    Waiter::Thread(thread) => thread.unpark(),
                    Waiter::Waker { waker, .. } => {
                        // A user-provided Wake implementation may panic. Keep
                        // draining so one faulty waiter cannot strand the
                        // rest, then propagate the first panic after every
                        // notification has had its chance to run.
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            waker.wake();
                        }));
                        if first_panic.is_none()
                            && let Err(panic) = result
                        {
                            first_panic = Some(panic);
                        }
                    }
                }
            });
            if let Some(panic) = first_panic {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl<O> Drop for Slot<O> {
    fn drop(&mut self) {
        // SAFETY: the last Arc reference is being dropped (reclamation is
        // entirely Arc-based), so no other thread can access the slot.
        // OUTCOME_VALID is set exactly while the cell holds a live value:
        // written by complete, cleared by a unique take,
        // so the drop fires exactly once.
        if self.state.load(Ordering::Relaxed) & OUTCOME_VALID != 0 {
            // SAFETY: see above.
            unsafe { self.drop_outcome() };
        }
    }
}

/// The unique publication authority for one unkeyed observation pair.
///
/// A publisher is created by [`pair`], is deliberately not cloneable, and is
/// consumed by [`Publisher::complete`]. Dropping it before completion does not
/// synthesize an outcome; captured observations remain pending until dropped.
///
/// ```ignore
/// let (publisher, _) = observe::pair::<u64>();
/// let duplicate = publisher.clone();
/// ```
///
/// ```ignore
/// let (publisher, _) = observe::pair::<u64>();
/// publisher.complete(1);
/// publisher.complete(2);
/// ```
#[must_use = "dropping an incomplete publisher leaves its observations pending"]
pub struct Publisher<O> {
    slot: Arc<Slot<O>>,
}

// SAFETY: a publisher is unique, exposes only a consuming `complete`, and
// never reads the outcome. Moving publication authority and `O` to another
// thread therefore requires `O: Send`, but not `O: Sync`; cloneable
// observations retain their stricter auto-trait bounds through `Slot<O>`.
unsafe impl<O: Send> Send for Publisher<O> {}

impl<O> Publisher<O> {
    /// Publish the terminal outcome exactly once.
    ///
    /// This consumes the pair's only publication authority. Every observation
    /// captured from the pair can retrieve the retained outcome.
    pub fn complete(self, outcome: O) {
        self.slot.complete(outcome);
    }
}

/// Construct one unkeyed, one-publication observation pair.
///
/// The publisher is the pair's unique completion authority. The observation
/// is cloneable and every clone remains attached to this exact pair.
pub fn pair<O>() -> (Publisher<O>, Observation<O>) {
    let slot = Arc::new(Slot::new());
    (Publisher { slot: slot.clone() }, Observation { slot })
}

/// Construct an unkeyed pair whose sole observation moves out the outcome.
///
/// The publisher is the pair's unique completion authority and remains
/// consuming. The affine observation is deliberately not cloneable and can
/// be awaited without requiring `O: Clone`; awaiting it yields the one
/// published `O` by value.
///
/// Dropping the publisher before completion does not synthesize an outcome.
/// The observation remains pending until it is cancelled, matching [`pair`]'s
/// incomplete-publication semantics.
///
/// ```ignore
/// # async fn example() {
/// struct MoveOnly(&'static str);
/// let (publisher, observation) = observe::affine_pair();
/// publisher.complete(MoveOnly("owned"));
/// assert_eq!(observation.await.0, "owned");
/// # }
/// ```
///
/// ```ignore
/// let (_, observation) = observe::affine_pair::<u64>();
/// let duplicate = observation.clone();
/// ```
pub fn affine_pair<O>() -> (Publisher<O>, AffineObservation<O>) {
    let slot = Arc::new(Slot::new());
    (
        Publisher { slot: slot.clone() },
        AffineObservation {
            registration: FutureRegistration::new(slot),
        },
    )
}

/// A cancellable observation of one completion slot.
pub struct Observation<O> {
    slot: Arc<Slot<O>>,
}

impl<O> Clone for Observation<O> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
        }
    }
}

impl<O: Clone> Observation<O> {
    /// Return the outcome without blocking when already complete.
    ///
    /// # Panics
    /// Panics only if the completed-bit protocol is violated (a programmer
    /// bug); a slot that reports completion always holds an outcome.
    #[must_use]
    pub fn try_get(&self) -> Option<O> {
        if self.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
            // SAFETY: COMPLETED observed with Acquire (invariant 2).
            Some(unsafe { self.slot.outcome_ref() }.clone())
        } else {
            None
        }
    }

    /// Block the current thread until completion.
    ///
    /// The waiter registers under the waiters mutex, re-checks completion,
    /// and only then parks. A completion that raced the registration either
    /// publishes before the post-push recheck (seen without parking) or sees
    /// the `HAS_WAITER` bit and drains the registration, so the park consumes
    /// the unpark token and returns immediately. Nothing that could itself
    /// park runs between registration and [`park`], so the token cannot be
    /// consumed elsewhere.
    ///
    /// # Panics
    /// Panics only if the completed-bit protocol is violated (a programmer
    /// bug); a slot that reports completion always holds an outcome.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "exercised via the observe-tests mirror and the cfg(test) external harness"
        )
    )]
    pub fn wait(&self) -> O {
        loop {
            if self.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
                // SAFETY: COMPLETED observed with Acquire (invariant 2), so
                // the outcome is present.
                return unsafe { self.slot.outcome_ref() }.clone();
            }
            self.slot.state.fetch_or(HAS_WAITER, Ordering::SeqCst);
            let mut waiters = lock(self.slot.waiters());
            let thread_id = current().id();
            if !matches!(waiters.last(), Some(Waiter::Thread(t)) if t.id() == thread_id) {
                waiters.push(Waiter::Thread(current()));
            }
            if self.slot.state.load(Ordering::SeqCst) & COMPLETED != 0 {
                // SAFETY: COMPLETED observed with SeqCst (invariant 2), so the
                // outcome is present. Deregister: we are in the Vec, and a
                // stale registration must not survive into a pooled slot.
                waiters
                    .retain(|waiter| !matches!(waiter, Waiter::Thread(t) if t.id() == thread_id));
                return unsafe { self.slot.outcome_ref() }.clone();
            }
            drop(waiters);
            park();
        }
    }

    /// Block the current thread until completion or `timeout` elapses.
    ///
    /// Returns `None` when the timeout elapses before the outcome is
    /// published. A completion that races the deadline is still observed:
    /// the loop re-checks after every wake, before the deadline test. A
    /// timed-out waiter is deregistered before returning, so a later
    /// completion cannot wake it.
    ///
    /// # Panics
    /// Panics if the completed-bit protocol is violated (a programmer bug; a
    /// slot that reports completion always holds an outcome), or if `timeout`
    /// overflows the [`Instant`] deadline.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "exercised via the observe-tests mirror and the cfg(test) external harness"
        )
    )]
    pub fn wait_timeout(&self, timeout: Duration) -> Option<O> {
        #[cfg(not(loom))]
        let deadline = Instant::now()
            .checked_add(timeout)
            .expect("wait timeout overflows Instant");
        #[cfg(loom)]
        let _ = timeout;
        loop {
            if self.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
                // SAFETY: COMPLETED observed with Acquire (invariant 2), so
                // the outcome is present.
                return Some(unsafe { self.slot.outcome_ref() }.clone());
            }
            self.slot.state.fetch_or(HAS_WAITER, Ordering::SeqCst);
            let mut waiters = lock(self.slot.waiters());
            let thread_id = current().id();
            if !matches!(waiters.last(), Some(Waiter::Thread(t)) if t.id() == thread_id) {
                waiters.push(Waiter::Thread(current()));
            }
            if self.slot.state.load(Ordering::SeqCst) & COMPLETED != 0 {
                // SAFETY: COMPLETED observed with SeqCst (invariant 2), so
                // the outcome is present. Deregister: we are in the Vec, and
                // a stale registration must not survive into a pooled slot.
                waiters
                    .retain(|waiter| !matches!(waiter, Waiter::Thread(t) if t.id() == thread_id));
                return Some(unsafe { self.slot.outcome_ref() }.clone());
            }
            drop(waiters);
            #[cfg(not(loom))]
            {
                if !park_until(deadline) {
                    // Deregister: the deadline passed while we were
                    // registered.
                    let mut waiters = lock(self.slot.waiters());
                    waiters.retain(
                        |waiter| !matches!(waiter, Waiter::Thread(t) if t.id() == thread_id),
                    );
                    return None;
                }
            }
            #[cfg(loom)]
            {
                park_until();
            }
        }
    }
}

impl<O> Observation<O> {
    /// Consume this observation and return the outcome by value, if the
    /// outcome is published and this handle is the last reference to the
    /// slot (no other observation, waiter, or the publisher still holds it).
    ///
    /// Unlike [`Observation::try_get`], this supports outcomes that are not
    /// `Clone`: the value moves out of the slot. It returns `None` while the
    /// slot is still shared or the outcome is not yet published.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "exercised via the observe-tests mirror and the cfg(test) external harness"
        )
    )]
    pub fn into_outcome(self) -> Option<O> {
        let slot = Arc::try_unwrap(self.slot).ok()?;
        // Exclusive ownership via the move; no access can race it.
        // SAFETY: `Arc::try_unwrap` proved this handle owns the slot and its
        // outcome exclusively.
        unsafe { slot.try_take_outcome() }
    }

    /// Register a waker to be woken when the outcome is published, for
    /// async adapters. Returns `true` when the outcome is already published
    /// (in which case nothing is registered and the caller can read the
    /// outcome directly).
    ///
    /// The waker may be woken spuriously and more than once; callers must
    /// re-read the outcome (via [`Observation::try_get`] or
    /// [`Observation::into_outcome`]) after a wake.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "exercised via the observe-tests mirror and the cfg(test) external harness"
        )
    )]
    pub fn register_waker(&self, waker: &Waker) -> bool {
        if self.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
            return true;
        }
        self.slot.state.fetch_or(HAS_WAITER, Ordering::SeqCst);
        let mut waiters = lock(self.slot.waiters());
        if self.slot.state.load(Ordering::SeqCst) & COMPLETED != 0 {
            // We may still be registered; a later drain produces only a
            // spurious wake, which callers must tolerate.
            return true;
        }
        // Registering a waker that will_wake an already-registered one is
        // idempotent (the std-endorsed pattern behind `Waker::clone_from`):
        // repeated polling of the same task never accumulates duplicates,
        // and the re-registration path performs no clone and no allocation.
        for waiter in waiters.iter_mut() {
            if let Waiter::Waker {
                waker: existing,
                ownership,
            } = waiter
                && existing.will_wake(waker)
            {
                if let WakerOwnership::Futures(future_owners) = ownership {
                    *ownership = WakerOwnership::Persistent {
                        future_owners: future_owners.get(),
                    };
                }
                return false;
            }
        }
        waiters.push(Waiter::Waker {
            waker: waker.clone(),
            ownership: WakerOwnership::Persistent { future_owners: 0 },
        });
        false
    }
}

/// One future's current registration in a completion slot.
///
/// Both observation flavors use this lifecycle: repeated polls are
/// idempotent, migration replaces the previous waker under the waiter lock,
/// and drop removes the one current logical owner.
struct FutureRegistration<O> {
    slot: Arc<Slot<O>>,
    waker: Option<Waker>,
}

impl<O> FutureRegistration<O> {
    fn new(slot: Arc<Slot<O>>) -> Self {
        Self { slot, waker: None }
    }

    fn update(&mut self, cx: &Context<'_>) {
        if self
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            return;
        }
        // Clone before mutating the registry. If a user-provided RawWaker
        // clone panics, the previous registration and local bookkeeping stay
        // unchanged.
        let next = cx.waker().clone();
        if self.slot.update_future_waker(self.waker.as_ref(), &next) {
            return;
        }
        let previous = self.waker.replace(next);
        drop(previous);
    }
}

impl<O> Drop for FutureRegistration<O> {
    fn drop(&mut self) {
        if let Some(waker) = &self.waker {
            self.slot.deregister_future_waker(waker);
        }
    }
}

/// A future that resolves to a clone of a shared outcome.
///
/// Created via [`Observation`]'s [`IntoFuture`] impl. A task has exactly one
/// current waker registration: polling after migration replaces the old
/// registration, and cancellation removes the replacement.
pub struct ObservationFuture<O> {
    registration: FutureRegistration<O>,
}

// The outcome itself remains in the heap-stable `Slot`; moving this handle
// moves only an `Arc` and optional `Waker`. Neither field points into the
// future, so polling never establishes a self-reference.
impl<O> Unpin for ObservationFuture<O> {}

impl<O: Clone> Future for ObservationFuture<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        if this.registration.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
            // SAFETY: COMPLETED observed with Acquire.
            let outcome = unsafe { this.registration.slot.outcome_ref() }.clone();
            return Poll::Ready(outcome);
        }
        this.registration.update(cx);
        if this.registration.slot.state.load(Ordering::Acquire) & COMPLETED != 0 {
            // SAFETY: COMPLETED observed with Acquire.
            let outcome = unsafe { this.registration.slot.outcome_ref() }.clone();
            return Poll::Ready(outcome);
        }
        Poll::Pending
    }
}

impl<O: Clone> IntoFuture for Observation<O> {
    type Output = O;
    type IntoFuture = ObservationFuture<O>;

    fn into_future(self) -> Self::IntoFuture {
        ObservationFuture {
            registration: FutureRegistration::new(self.slot),
        }
    }
}

/// A unique, cancellable future that moves out one published outcome.
///
/// Created by [`affine_pair`]. This type is deliberately not cloneable.
/// Awaiting it requires no `O: Clone` bound and transfers the exact value
/// stored in the observation slot. Its first and only waker is stored inline
/// in that slot; task migration replaces the old registration rather than
/// accumulating stale wakers.
///
/// ```ignore
/// let (_, observation) = observe::affine_pair::<String>();
/// let duplicate = observation.clone();
/// ```
#[must_use = "dropping an affine observation cancels its wait"]
pub struct AffineObservation<O> {
    registration: FutureRegistration<O>,
}

// As with `ObservationFuture`, the possibly `!Unpin` outcome never leaves its
// heap-stable slot while this handle is pending. Moving the unique handle does
// not move the outcome or invalidate its waker registration.
impl<O> Unpin for AffineObservation<O> {}

// SAFETY: an affine observation is the only handle allowed to read or move
// its outcome. Moving it between threads transfers that unique authority, so
// `O: Send` is sufficient even though cloneable observations require `Sync`.
unsafe impl<O: Send> Send for AffineObservation<O> {}

impl<O> Future for AffineObservation<O> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        let this = self.get_mut();
        // SAFETY: `AffineObservation` is the pair's sole outcome-reading
        // authority and cannot be cloned.
        if let Some(outcome) = unsafe { this.registration.slot.try_take_outcome() } {
            return Poll::Ready(outcome);
        }
        this.registration.update(cx);
        // SAFETY: same unique affine authority. The second check closes the
        // publication-versus-registration race.
        if let Some(outcome) = unsafe { this.registration.slot.try_take_outcome() } {
            return Poll::Ready(outcome);
        }
        Poll::Pending
    }
}

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::doc_markdown,
    clippy::duration_suboptimal_units,
    clippy::items_after_statements,
    clippy::manual_assert,
    clippy::manual_let_else,
    clippy::match_wild_err_arm,
    clippy::option_option,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::used_underscore_binding,
    reason = "preserve the exact imported adversarial Observe corpus while its source repository catches up with the pinned Clippy"
)]
mod external_tests;
#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(all(test, not(loom)))]
mod test_support;
#[cfg(all(test, not(loom)))]
mod tests;
