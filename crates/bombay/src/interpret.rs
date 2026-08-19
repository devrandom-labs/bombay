//! Ordered, statically dispatched interpretation of complete actor actions.

use core::future::Future;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use behavior::{
    Address, Behavior, BehaviorAddr, BirthMode, Create, CreationResolved, DispatchBirth,
    InstallBirth, Never, Protocol,
};
use bombay_engine::ActionsOf;

use crate::local::CommitActions;

pub(crate) struct CreationResults<A>
where
    A: Address,
    A::Nonce: Eq + core::hash::Hash,
{
    results: Arc<Mutex<HashMap<A::Nonce, CreationResolved<A>>>>,
}

impl<A> Clone for CreationResults<A>
where
    A: Address,
    A::Nonce: Eq + core::hash::Hash,
{
    fn clone(&self) -> Self {
        Self {
            results: self.results.clone(),
        }
    }
}

impl<A> CreationResults<A>
where
    A: Address,
    A::Nonce: Copy + Eq + core::hash::Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn begin(&self) {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub(crate) fn record(&self, result: CreationResolved<A>) {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(result.nonce, result);
    }

    pub(crate) fn resolve(&self, nonce: &A::Nonce) -> Option<CreationResolved<A>> {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(nonce)
            .copied()
    }
}

/// Common installation error selected by one actor-local capability product.
pub(crate) trait BirthInstaller<A: Address> {
    type Error;
}

/// Launches one concrete child at an already-derived address.
pub(crate) trait SpawnChild<A: Address, C: Behavior<Protocol: Protocol<Addr = A>>, E> {
    fn spawn_child(
        &mut self,
        address: A,
        creation: Create<A, C>,
    ) -> impl Future<Output = Result<(), E>> + Send;
}

/// Affine retirement of actor-local runtime capability tasks.
pub(crate) trait RetireCapabilities {
    fn retire(self) -> impl Future<Output = ()> + Send;
}

pub(crate) trait CreationTransaction<N> {
    fn begin_creations(&mut self);
}

/// Exact action leg whose ordered interpretation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InterpretationError<C, S> {
    #[error("actor creation effects could not be interpreted")]
    Creations(#[source] C),
    #[error("actor send effects could not be interpreted")]
    Sends(#[source] S),
}

/// Actor-local capability product paired with the emitting actor's address.
pub(crate) struct ActionInterpreter<A, I> {
    address: A,
    capabilities: I,
}

impl<A, I> ActionInterpreter<A, I> {
    pub(crate) const fn new(address: A, capabilities: I) -> Self {
        Self {
            address,
            capabilities,
        }
    }
}

impl<A, C, I> InstallBirth<A, C, (), <I as BirthInstaller<A>>::Error> for ActionInterpreter<A, I>
where
    A: Address + Send,
    A::Nonce: Send,
    C: Behavior<Protocol: Protocol<Addr = A>> + Send,
    I: BirthInstaller<A> + SpawnChild<A, C, <I as BirthInstaller<A>>::Error> + Send,
{
    async fn install_birth(
        &mut self,
        creation: Create<A, C>,
    ) -> Result<(), <I as BirthInstaller<A>>::Error> {
        let address = self.address.birth(creation.nonce);
        self.capabilities.spawn_child(address, creation).await
    }
}

impl<B, I> CommitActions<B> for ActionInterpreter<BehaviorAddr<B>, I>
where
    B: Behavior<Ph = Never>,
    BehaviorAddr<B>: Send,
    <BehaviorAddr<B> as Address>::Nonce: Send,
    B::Sends: Send,
    <B::Birth as BirthMode>::Child: Send,
    I: BirthInstaller<BehaviorAddr<B>>
        + behavior::SendInterpreter
        + RetireCapabilities
        + CreationTransaction<<BehaviorAddr<B> as Address>::Nonce>
        + Send,
    <B::Birth as BirthMode>::Child: DispatchBirth<
            BehaviorAddr<B>,
            ActionInterpreter<BehaviorAddr<B>, I>,
            (),
            <I as BirthInstaller<BehaviorAddr<B>>>::Error,
        >,
    B::Sends: behavior::InterpretSends<I, B::Event, behavior::Here>,
{
    type Error = InterpretationError<
        <I as BirthInstaller<BehaviorAddr<B>>>::Error,
        <I as behavior::SendInterpreter>::Error,
    >;

    async fn commit(&mut self, actions: ActionsOf<B>) -> Result<(), Self::Error> {
        self.capabilities.begin_creations();
        for creation in actions.creates {
            creation
                .child
                .dispatch_birth(creation.nonce, creation.kind, self)
                .await
                .map_err(InterpretationError::Creations)?;
        }
        <B::Sends as behavior::InterpretSends<I, B::Event, behavior::Here>>::interpret(
            actions.sends,
            &mut self.capabilities,
        )
        .await
        .map_err(InterpretationError::Sends)
    }

    async fn retire(self) {
        self.capabilities.retire().await;
    }
}
