//! Optional, statically dispatched lifecycle facts.

use bombay_address::{Lease, RegistrationId};

/// A completed runtime lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTransition {
    /// All incarnation resources were prepared and its address was claimed.
    Prepared,
    /// The actor task began execution.
    Started,
    /// A typed shutdown request was accepted by the actor mailbox.
    ShutdownRequested,
    /// A Behavior-marked replacement incarnation was installed and launched.
    Restarted,
    /// The exact address registration was released.
    Retired,
    /// Terminal completion was published.
    Completed,
}

/// One truthful lifecycle fact for an exact local actor incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent<A, I = RegistrationId> {
    /// Logical local actor address.
    pub address: A,
    /// Exact process-local registration identity.
    pub incarnation: I,
    /// Handler transition that has completed.
    pub transition: LifecycleTransition,
}

/// Synchronous consumer of lifecycle facts.
///
/// Implementations own storage, filtering, formatting, and export. Calls run
/// inline after the corresponding runtime state edge and should return
/// promptly. A panic is contained at the instrumentation boundary.
pub trait LifecycleSink<A, I>: Clone + Send + Sync {
    /// Record one completed lifecycle transition.
    fn record(&self, event: LifecycleEvent<A, I>);
}

/// Disabled lifecycle instrumentation.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoLifecycle;

/// Static lifecycle instrumentation configuration produced by
/// [`crate::System::with_lifecycle`].
#[doc(hidden)]
#[derive(Clone)]
pub struct Lifecycle<S>(pub(crate) S);

/// Extracts the exact identity carried by a registry ownership token.
///
/// Custom [`crate::EndpointRegistry`] implementations must implement this
/// contract for their registration type when used with
/// [`crate::System::with_lifecycle`].
pub trait RegistrationIdentity {
    /// Exact, process-local incarnation identity.
    type Identity: Clone + Send + Sync + 'static;

    /// Return the identity of this exact live registration.
    fn registration_identity(&self) -> Self::Identity;
}

impl<A, E> RegistrationIdentity for Lease<A, E>
where
    A: Eq + core::hash::Hash,
{
    type Identity = RegistrationId;

    fn registration_identity(&self) -> Self::Identity {
        self.registration_id()
    }
}

#[doc(hidden)]
pub trait IncarnationReporter: Clone + Send + Sync + 'static {
    fn emit(&self, transition: LifecycleTransition);
}

impl IncarnationReporter for NoLifecycle {
    fn emit(&self, _transition: LifecycleTransition) {}
}

#[derive(Clone)]
#[doc(hidden)]
pub struct Reporting<A, I, S> {
    address: A,
    incarnation: I,
    sink: S,
}

impl<A, I, S> IncarnationReporter for Reporting<A, I, S>
where
    A: Clone + Send + Sync + 'static,
    I: Clone + Send + Sync + 'static,
    S: LifecycleSink<A, I> + 'static,
{
    fn emit(&self, transition: LifecycleTransition) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.sink.record(LifecycleEvent {
                address: self.address.clone(),
                incarnation: self.incarnation.clone(),
                transition,
            });
        }));
    }
}

#[doc(hidden)]
pub trait LifecycleFactory<A, R>: Clone {
    type Reporter: IncarnationReporter;

    fn reporter(&self, address: A, registration: &R) -> Self::Reporter;
}

impl<A, R> LifecycleFactory<A, R> for NoLifecycle {
    type Reporter = NoLifecycle;

    fn reporter(&self, _address: A, _registration: &R) -> Self::Reporter {
        NoLifecycle
    }
}

impl<A, R, S> LifecycleFactory<A, R> for Lifecycle<S>
where
    A: Clone + Send + Sync + 'static,
    R: RegistrationIdentity,
    S: LifecycleSink<A, R::Identity> + 'static,
{
    type Reporter = Reporting<A, R::Identity, S>;

    fn reporter(&self, address: A, registration: &R) -> Self::Reporter {
        Reporting {
            address,
            incarnation: registration.registration_identity(),
            sink: self.0.clone(),
        }
    }
}
