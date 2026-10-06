use core::convert::Infallible;
use core::future::{Future, poll_fn};
use core::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use bombay::entity::{
    Activated, ActivationId, AdmissionFailure, DirectoryConfig, EntityId, EntityRuntime,
    EntityShutdown, FenceFailure, LocalEntityRuntime, Passivation, Refusal, RetirementMode,
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
    type Task = tokio::task::JoinHandle<()>;
    type TaskFailure = tokio::task::JoinError;
    type RetirementFailure = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        tokio::spawn(task)
    }

    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        task.await
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
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

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn deliver(&self, _: Self::Endpoint, (): Self::Origin, command: u64) -> Result<(), u64> {
        self.state.deliveries.lock().unwrap().push(command);
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn fence(&self, _: Self::Endpoint) -> Result<(), FenceFailure> {
        self.state.fences.fetch_add(1, Ordering::Release);
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Retirement recording must occur when the consuming trait-port future is polled."
    )]
    async fn retire(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        _: Self::Lease,
        _: RetirementMode,
    ) -> Result<(), Self::RetirementFailure> {
        self.state.retirements.fetch_add(1, Ordering::Release);
        Ok(())
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

    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = shutdown
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 2);
    assert_eq!(
        returned_entities,
        vec![EntityId::new(41), EntityId::new(73)]
    );
    assert_eq!(
        observations.trace(),
        RuntimeTrace {
            activations: 2,
            deliveries: vec![7, 11],
            fences: 2,
            retirements: 2,
        }
    );
    let shutdown_refusal = entities.admit((), EntityId::new(89), 13).await;
    assert!(matches!(
        shutdown_refusal,
        Err(AdmissionFailure::Refused {
            command: 13,
            reason: Refusal::Shutdown,
        })
    ));
}

#[tokio::test]
async fn family_shutdown_has_one_result_owner() {
    let entities =
        EntityRuntime::new(DirectoryConfig::default(), RecordingRuntime::default()).unwrap();
    let first = entities.shutdown().await;
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = first
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 0);
    assert_eq!(returned_entities, vec![]);

    let repeated = tokio::spawn(async move { entities.shutdown().await }).await;
    let repeated = repeated.expect("a second caller receives a disposition");
    assert!(matches!(repeated, EntityShutdown::AlreadyClaimed));
}

#[tokio::test]
async fn cancelled_shutdown_returns_task_custody_to_the_family() {
    let observations = RecordingRuntime::default();
    let activation_started = Arc::new(Notify::new());
    let release_activation = Arc::new(Notify::new());
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: observations.clone(),
            activation_started: Arc::clone(&activation_started),
            release_activation: Arc::clone(&release_activation),
            delivery_gate: DeliveryGate::Immediate,
            retirement_disposition: RetirementDisposition::Complete,
            join_gate: JoinGate::Immediate,
        },
    )
    .unwrap();
    let admission = tokio::spawn({
        let entities = entities.clone();
        async move { entities.admit((), EntityId::new(41), 7).await }
    });
    activation_started.notified().await;

    let (closure, closed) = tokio::sync::oneshot::channel();
    let shutdown = tokio::spawn({
        let entities = entities.clone();
        async move {
            let mut transaction = pin!(entities.shutdown());
            let mut closure = Some(closure);
            poll_fn(|context| {
                let outcome = transaction.as_mut().poll(context);
                if let Some(closure) = closure.take() {
                    closure
                        .send(())
                        .expect("the shutdown observer remains present");
                }
                outcome
            })
            .await
        }
    });
    closed.await.unwrap();
    shutdown.abort();
    let cancellation = shutdown.await.expect_err("the first shutdown was canceled");
    assert!(cancellation.is_cancelled());

    release_activation.notify_one();
    admission.await.unwrap().unwrap();
    let retry = entities.shutdown().await;
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = retry
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![EntityId::new(41)]);
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

#[derive(Clone)]
struct GatedRuntime {
    inner: RecordingRuntime,
    activation_started: Arc<Notify>,
    release_activation: Arc<Notify>,
    delivery_gate: DeliveryGate,
    retirement_disposition: RetirementDisposition,
    join_gate: JoinGate,
}

struct GatedTask {
    handle: tokio::task::JoinHandle<()>,
    join_gate: JoinGate,
}

#[derive(Clone)]
enum JoinGate {
    Immediate,
    BlockOnce {
        entered: Arc<Notify>,
        phase: Arc<Mutex<JoinPhase>>,
    },
}

enum JoinPhase {
    Pending,
    Entered,
}

#[derive(Clone)]
enum DeliveryGate {
    Immediate,
    Blocked {
        started: Arc<Notify>,
        release: Arc<Notify>,
    },
}

#[derive(Clone)]
enum RetirementDisposition {
    Complete,
    Panic,
    Cancel {
        started: Arc<Notify>,
        task: Arc<Mutex<Option<tokio::task::AbortHandle>>>,
    },
}

impl LocalEntityRuntime<u64, u64> for GatedRuntime {
    type Origin = ();
    type Endpoint = u64;
    type Lease = u64;
    type ActivationError = Infallible;
    type Task = GatedTask;
    type TaskFailure = tokio::task::JoinError;
    type RetirementFailure = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        let scheduled = tokio::spawn(task);
        if let RetirementDisposition::Cancel { task, .. } = &self.retirement_disposition {
            *task.lock().unwrap() = Some(scheduled.abort_handle());
        }
        GatedTask {
            handle: scheduled,
            join_gate: self.join_gate.clone(),
        }
    }

    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        let pause = match &task.join_gate {
            JoinGate::Immediate => None,
            JoinGate::BlockOnce { entered, phase } => {
                let mut phase = phase.lock().unwrap();
                match *phase {
                    JoinPhase::Pending => {
                        *phase = JoinPhase::Entered;
                        Some(Arc::clone(entered))
                    }
                    JoinPhase::Entered => None,
                }
            }
        };
        if let Some(entered) = pause {
            entered.notify_one();
            core::future::pending::<()>().await;
        }
        (&mut task.handle).await
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
        if let DeliveryGate::Blocked { started, release } = &self.delivery_gate {
            started.notify_one();
            release.notified().await;
        }
        self.inner.state.deliveries.lock().unwrap().push(command);
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn fence(&self, _: Self::Endpoint) -> Result<(), FenceFailure> {
        self.inner.state.fences.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn retire(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        _: Self::Lease,
        _: RetirementMode,
    ) -> Result<(), Self::RetirementFailure> {
        self.inner.state.retirements.fetch_add(1, Ordering::Release);
        match &self.retirement_disposition {
            RetirementDisposition::Complete => {}
            RetirementDisposition::Panic => panic!("retirement task failed"),
            RetirementDisposition::Cancel { started, .. } => {
                started.notify_one();
                core::future::pending::<()>().await;
            }
        }
        Ok(())
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
            delivery_gate: DeliveryGate::Immediate,
            retirement_disposition: RetirementDisposition::Complete,
            join_gate: JoinGate::Immediate,
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

    let shutdown_refusal = entities.admit((), EntityId::new(73), 11).await;
    assert!(matches!(
        shutdown_refusal,
        Err(AdmissionFailure::Refused {
            command: 11,
            reason: Refusal::Shutdown,
        })
    ));
    assert!(!shutdown.is_finished());
    let repeated = entities.shutdown().await;
    assert!(matches!(repeated, EntityShutdown::AlreadyClaimed));

    release_activation.notify_one();
    admission.await.unwrap().unwrap();
    let shutdown = shutdown.await.unwrap();
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = shutdown
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![EntityId::new(41)]);
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

#[tokio::test]
async fn shutdown_claims_passivation_before_pending_delivery_settles() {
    let observations = RecordingRuntime::default();
    let activation_started = Arc::new(Notify::new());
    let release_activation = Arc::new(Notify::new());
    let delivery_started = Arc::new(Notify::new());
    let release_delivery = Arc::new(Notify::new());
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: observations.clone(),
            activation_started: Arc::clone(&activation_started),
            release_activation: Arc::clone(&release_activation),
            delivery_gate: DeliveryGate::Blocked {
                started: Arc::clone(&delivery_started),
                release: Arc::clone(&release_delivery),
            },
            retirement_disposition: RetirementDisposition::Complete,
            join_gate: JoinGate::Immediate,
        },
    )
    .unwrap();
    let entity_id = EntityId::new(41);
    let admission = tokio::spawn({
        let entities = entities.clone();
        async move { entities.admit((), entity_id, 7).await }
    });
    activation_started.notified().await;
    release_activation.notify_one();
    delivery_started.notified().await;

    let (closure, closed) = tokio::sync::oneshot::channel();
    let shutdown = tokio::spawn({
        let entities = entities.clone();
        async move {
            let mut transaction = pin!(entities.shutdown());
            let mut closure = Some(closure);
            poll_fn(|context| {
                let outcome = transaction.as_mut().poll(context);
                if let Some(closure) = closure.take() {
                    closure
                        .send(())
                        .expect("the shutdown observer remains present");
                }
                outcome
            })
            .await
        }
    });
    closed.await.unwrap();

    let passivation = entities.passivate(&entity_id);
    assert_eq!(passivation, Passivation::ShuttingDown);

    release_delivery.notify_one();
    admission.await.unwrap().unwrap();
    let shutdown = shutdown.await.unwrap();
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = shutdown
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![EntityId::new(41)]);
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

#[tokio::test]
async fn family_shutdown_preserves_retirement_task_failure() {
    let observations = RecordingRuntime::default();
    let release_activation = Arc::new(Notify::new());
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: observations,
            activation_started: Arc::new(Notify::new()),
            release_activation: Arc::clone(&release_activation),
            delivery_gate: DeliveryGate::Immediate,
            retirement_disposition: RetirementDisposition::Panic,
            join_gate: JoinGate::Immediate,
        },
    )
    .unwrap();
    release_activation.notify_one();
    entities.admit((), EntityId::new(41), 7).await.unwrap();

    let shutdown = tokio::spawn(async move { entities.shutdown().await });
    let outcome = shutdown
        .await
        .expect("shutdown returns the owned task failure");
    let EntityShutdown::Unsettled {
        represented,
        entities: returned_entities,
        mut failures,
    } = outcome
    else {
        panic!("retirement task panic must be returned");
    };
    assert_eq!(represented, 1);
    assert_eq!(returned_entities.len(), 1);
    let (id, disposition, receipts) = &returned_entities[0];
    assert_eq!(id, &EntityId::new(41));
    assert!(matches!(disposition, Err(Some(_))));
    assert_eq!(receipts.len(), 0);
    assert_eq!(failures.len(), 1);
    let failure = failures.pop().expect("one failed retirement task");
    let panic = failure
        .try_into_panic()
        .expect("the retirement task panicked");
    let message = panic
        .downcast::<&'static str>()
        .expect("the original panic message is retained");
    assert_eq!(*message, "retirement task failed");
}

#[tokio::test]
async fn family_shutdown_preserves_cancelled_retirement_task() {
    let retirement_started = Arc::new(Notify::new());
    let retirement_task = Arc::new(Mutex::new(None));
    let release_activation = Arc::new(Notify::new());
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: RecordingRuntime::default(),
            activation_started: Arc::new(Notify::new()),
            release_activation: Arc::clone(&release_activation),
            delivery_gate: DeliveryGate::Immediate,
            retirement_disposition: RetirementDisposition::Cancel {
                started: Arc::clone(&retirement_started),
                task: Arc::clone(&retirement_task),
            },
            join_gate: JoinGate::Immediate,
        },
    )
    .unwrap();
    release_activation.notify_one();
    entities.admit((), EntityId::new(41), 7).await.unwrap();

    let shutdown = tokio::spawn(async move { entities.shutdown().await });
    retirement_started.notified().await;
    let task = retirement_task
        .lock()
        .unwrap()
        .take()
        .expect("the retirement task was scheduled");
    task.abort();
    let outcome = shutdown.await.expect("shutdown retains cancellation");
    let EntityShutdown::Unsettled {
        represented,
        entities: returned_entities,
        mut failures,
    } = outcome
    else {
        panic!("canceled retirement task must be returned");
    };
    assert_eq!(represented, 1);
    assert_eq!(returned_entities.len(), 1);
    let (id, disposition, receipts) = &returned_entities[0];
    assert_eq!(id, &EntityId::new(41));
    assert!(matches!(disposition, Err(Some(_))));
    assert_eq!(receipts.len(), 0);
    assert_eq!(failures.len(), 1);
    let failure = failures.pop().expect("one canceled retirement task");
    assert!(failure.is_cancelled());
}

#[tokio::test]
async fn cancelled_join_preserves_the_original_family_drain_count() {
    let join_entered = Arc::new(Notify::new());
    let release_activation = Arc::new(Notify::new());
    let observations = RecordingRuntime::default();
    let entities = EntityRuntime::new(
        DirectoryConfig::default(),
        GatedRuntime {
            inner: observations.clone(),
            activation_started: Arc::new(Notify::new()),
            release_activation: Arc::clone(&release_activation),
            delivery_gate: DeliveryGate::Immediate,
            retirement_disposition: RetirementDisposition::Complete,
            join_gate: JoinGate::BlockOnce {
                entered: Arc::clone(&join_entered),
                phase: Arc::new(Mutex::new(JoinPhase::Pending)),
            },
        },
    )
    .unwrap();
    release_activation.notify_one();
    entities.admit((), EntityId::new(41), 7).await.unwrap();

    let shutdown = tokio::spawn({
        let entities = entities.clone();
        async move { entities.shutdown().await }
    });
    join_entered.notified().await;
    shutdown.abort();
    let cancellation = shutdown.await.expect_err("the first join was canceled");
    assert!(cancellation.is_cancelled());

    let resumed = entities.shutdown().await;
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = resumed
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![EntityId::new(41)]);
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
    type Task = tokio::task::JoinHandle<()>;
    type TaskFailure = tokio::task::JoinError;
    type RetirementFailure = Infallible;

    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> Self::Task {
        tokio::spawn(task)
    }

    async fn join(task: &mut Self::Task) -> Result<(), Self::TaskFailure> {
        task.await
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
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

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn deliver(
        &self,
        (): Self::Endpoint,
        (): Self::Origin,
        command: MoveOnlyCommand,
    ) -> Result<(), MoveOnlyCommand> {
        Err(command)
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Defer trait-port work and owned inputs until the future is polled."
    )]
    async fn fence(&self, (): Self::Endpoint) -> Result<(), FenceFailure> {
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "Retirement completion remains a cold trait-port future."
    )]
    async fn retire(
        &self,
        _: &EntityId<u64>,
        _: ActivationId,
        (): Self::Lease,
        _: RetirementMode,
    ) -> Result<(), Self::RetirementFailure> {
        Ok(())
    }
}

#[tokio::test]
async fn runtime_admission_returns_the_exact_move_only_command() {
    let drops = Arc::new(AtomicUsize::new(0));
    let command = MoveOnlyCommand {
        value: String::from("owned command"),
        drops: Arc::clone(&drops),
    };
    let allocation = command.value.as_ptr();
    let entity_id = EntityId::new(9);
    let entities = EntityRuntime::new(DirectoryConfig::default(), RejectingRuntime)
        .unwrap_or_else(|_| panic!("valid directory configuration must construct"));

    let failure = entities.admit((), entity_id, command).await.unwrap_err();
    let AdmissionFailure::Refused { command, reason } = failure else {
        panic!("delivery must preserve the rejected command");
    };

    assert_eq!(reason, Refusal::Unavailable);
    assert_eq!(command.value, "owned command");
    assert_eq!(command.value.as_ptr(), allocation);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(command);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let shutdown = entities.shutdown().await;
    let EntityShutdown::Settled {
        represented,
        entities: mut returned_entities,
    } = shutdown
    else {
        panic!("family must settle with original keys");
    };
    returned_entities.sort_by_key(|id| *id.get());
    assert_eq!(represented, 1);
    assert_eq!(returned_entities, vec![entity_id]);
}
