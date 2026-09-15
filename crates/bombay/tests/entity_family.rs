use core::convert::Infallible;
use core::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use bombay::entity::{
    Activated, ActivationId, AdmissionFailure, DirectoryConfig, EntityId, EntityRuntime,
    FenceFailure, LocalEntityRuntime, Refusal, RetirementMode,
};

#[derive(Clone, Default)]
struct RecordingRuntime {
    state: Arc<RecordingState>,
}

#[derive(Default)]
struct RecordingState {
    activations: AtomicUsize,
    deliveries: Mutex<Vec<u64>>,
    fences: AtomicUsize,
    retirements: AtomicUsize,
}

#[derive(Debug, PartialEq, Eq)]
struct RuntimeTrace {
    activations: usize,
    deliveries: Vec<u64>,
    fences: usize,
    retirements: usize,
}

impl RecordingRuntime {
    fn trace(&self) -> RuntimeTrace {
        RuntimeTrace {
            activations: self.state.activations.load(Ordering::Acquire),
            deliveries: self.state.deliveries.lock().unwrap().clone(),
            fences: self.state.fences.load(Ordering::Acquire),
            retirements: self.state.retirements.load(Ordering::Acquire),
        }
    }
}

impl LocalEntityRuntime<u64, u64> for RecordingRuntime {
    type Origin = ();
    type Endpoint = u64;
    type Lease = u64;
    type ActivationError = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        drop(tokio::spawn(task));
    }

    async fn activate(
        &self,
        _: EntityId<u64>,
        activation_id: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        self.state.activations.fetch_add(1, Ordering::Release);
        let incarnation = activation_id.get().get();
        Ok(Activated {
            endpoint: incarnation,
            lease: incarnation,
        })
    }

    fn activation_failed(&self, _: EntityId<u64>, _: ActivationId, error: Self::ActivationError) {
        match error {}
    }

    async fn deliver(&self, _: Self::Endpoint, (): Self::Origin, command: u64) -> Result<(), u64> {
        self.state.deliveries.lock().unwrap().push(command);
        Ok(())
    }

    async fn fence(&self, _: Self::Endpoint) -> Result<(), FenceFailure> {
        self.state.fences.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn retire(&self, _: EntityId<u64>, _: ActivationId, _: Self::Lease, _: RetirementMode) {
        self.state.retirements.fetch_add(1, Ordering::Release);
    }
}

#[tokio::test]
async fn family_shutdown_closes_admission_drains_every_slot_and_joins_tasks() {
    let runtime = RecordingRuntime::default();
    let observations = runtime.clone();
    let entities = EntityRuntime::new(DirectoryConfig::default(), runtime).unwrap();

    entities.admit((), EntityId::new(41), 7).await.unwrap();
    entities.admit((), EntityId::new(73), 11).await.unwrap();

    let shutdown = entities.shutdown().await;

    assert_eq!(shutdown.represented, 2);
    assert_eq!(
        observations.trace(),
        RuntimeTrace {
            activations: 2,
            deliveries: vec![7, 11],
            fences: 2,
            retirements: 2,
        }
    );
    assert!(matches!(
        entities.admit((), EntityId::new(89), 13).await,
        Err(AdmissionFailure::Refused {
            command: 13,
            reason: Refusal::Shutdown,
        })
    ));
}

#[derive(Clone)]
struct GatedRuntime {
    inner: RecordingRuntime,
    activation_started: Arc<Notify>,
    release_activation: Arc<Notify>,
}

impl LocalEntityRuntime<u64, u64> for GatedRuntime {
    type Origin = ();
    type Endpoint = u64;
    type Lease = u64;
    type ActivationError = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        drop(tokio::spawn(task));
    }

    async fn activate(
        &self,
        _: EntityId<u64>,
        activation_id: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        self.activation_started.notify_one();
        self.release_activation.notified().await;
        self.inner.state.activations.fetch_add(1, Ordering::Release);
        let incarnation = activation_id.get().get();
        Ok(Activated {
            endpoint: incarnation,
            lease: incarnation,
        })
    }

    fn activation_failed(&self, _: EntityId<u64>, _: ActivationId, error: Self::ActivationError) {
        match error {}
    }

    async fn deliver(&self, _: Self::Endpoint, (): Self::Origin, command: u64) -> Result<(), u64> {
        self.inner.state.deliveries.lock().unwrap().push(command);
        Ok(())
    }

    async fn fence(&self, _: Self::Endpoint) -> Result<(), FenceFailure> {
        self.inner.state.fences.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn retire(&self, _: EntityId<u64>, _: ActivationId, _: Self::Lease, _: RetirementMode) {
        self.inner.state.retirements.fetch_add(1, Ordering::Release);
    }
}

#[tokio::test]
async fn shutdown_settles_an_installed_activation_before_draining_and_joining() {
    let observations = RecordingRuntime::default();
    let activation_started = Arc::new(Notify::new());
    let release_activation = Arc::new(Notify::new());
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: observations.clone(),
            activation_started: Arc::clone(&activation_started),
            release_activation: Arc::clone(&release_activation),
        },
    )
    .unwrap();

    let admission = tokio::spawn({
        let entities = entities.clone();
        async move { entities.admit((), EntityId::new(41), 7).await }
    });
    activation_started.notified().await;

    let shutdown = tokio::spawn({
        let entities = entities.clone();
        async move { entities.shutdown().await }
    });
    tokio::task::yield_now().await;

    assert!(matches!(
        entities.admit((), EntityId::new(73), 11).await,
        Err(AdmissionFailure::Refused {
            command: 11,
            reason: Refusal::Shutdown,
        })
    ));
    assert!(!shutdown.is_finished());

    release_activation.notify_one();
    admission.await.unwrap().unwrap();
    assert_eq!(shutdown.await.unwrap().represented, 1);
    assert_eq!(
        observations.trace(),
        RuntimeTrace {
            activations: 1,
            deliveries: vec![7],
            fences: 1,
            retirements: 1,
        }
    );
}

struct MoveOnlyCommand {
    value: String,
    drops: Arc<AtomicUsize>,
}

impl Drop for MoveOnlyCommand {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct RejectingRuntime;

impl LocalEntityRuntime<u64, MoveOnlyCommand> for RejectingRuntime {
    type Origin = ();
    type Endpoint = ();
    type Lease = ();
    type ActivationError = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        drop(tokio::spawn(task));
    }

    async fn activate(
        &self,
        _: EntityId<u64>,
        _: ActivationId,
    ) -> Result<Activated<Self::Endpoint, Self::Lease>, Self::ActivationError> {
        Ok(Activated {
            endpoint: (),
            lease: (),
        })
    }

    fn activation_failed(&self, _: EntityId<u64>, _: ActivationId, error: Self::ActivationError) {
        match error {}
    }

    async fn deliver(
        &self,
        (): Self::Endpoint,
        (): Self::Origin,
        command: MoveOnlyCommand,
    ) -> Result<(), MoveOnlyCommand> {
        Err(command)
    }

    async fn fence(&self, (): Self::Endpoint) -> Result<(), FenceFailure> {
        Ok(())
    }

    async fn retire(&self, _: EntityId<u64>, _: ActivationId, (): Self::Lease, _: RetirementMode) {}
}

#[tokio::test]
async fn runtime_admission_returns_the_exact_move_only_command() {
    let drops = Arc::new(AtomicUsize::new(0));
    let command = MoveOnlyCommand {
        value: String::from("owned command"),
        drops: Arc::clone(&drops),
    };
    let allocation = command.value.as_ptr();
    let entities = EntityRuntime::new(DirectoryConfig::default(), RejectingRuntime)
        .unwrap_or_else(|_| panic!("valid directory configuration must construct"));

    let failure = entities
        .admit((), EntityId::new(9), command)
        .await
        .unwrap_err();
    let AdmissionFailure::Refused { command, reason } = failure else {
        panic!("delivery must preserve the rejected command");
    };

    assert_eq!(reason, Refusal::Unavailable);
    assert_eq!(command.value, "owned command");
    assert_eq!(command.value.as_ptr(), allocation);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(command);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(entities.shutdown().await.represented, 1);
}
