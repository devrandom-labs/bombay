use crate::address::MailAddr;
use crate::application::composition::ComposeApplication;
use crate::application::execution::{
    ApplicationWorkCustody, ApplicationWorkPresence, ApplicationWorkPublication, RootInterpreter,
    execute_application_with,
};
use crate::application::{App, Application, ApplicationHandle, ApplicationOutcome};
use crate::launch::ActorSpace;
use crate::local::children::{
    ChildBindings, RetireChildTasks, RuntimeChildBindings, StructuralOrigins,
};
use crate::local::effects::CommitActions;
use crate::terminal::{ActorRetirement, RootOrigin};
use crate::topology::Hosts;
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Here, InjectEvent, Never, Protocol,
};
use behavior_actors::{ShutdownRejection, ShutdownRequested};
use core::future::Future;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::runtime::{Handle, TryCurrentError};

#[allow(
    private_bounds,
    reason = "the launch proof is private static runtime composition"
)]
impl<Root, Spaces> App<Root, Spaces>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
{
    #[cfg(feature = "axum")]
    /// Pair asynchronous prebind and serving with the original application result receiver.
    /// Cold inputs remain published during bind; invocation owns the bare router and listener.
    /// Primary root projection is caller-owned after acquiring the raw joined outcome.
    ///
    /// # Errors
    /// Returns the untouched application, router and address with the actual entered-host error.
    /// Bind refusal is an exact `StagingRejected` product before actor handoff.
    ///
    /// # Panics
    /// Setup or router invocation can unwind; the original cause remains with the caller.
    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "retain original router/listener and all raw root result boundaries"
    )]
    pub fn execute_axum<Terminal, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<Output = ApplicationOutcome<
                (Self, Router, SocketAddr),
                (Router, TcpListener),
                Result<(), io::Error>,
                (RootOrigin<Root>, ActorRetirement<Root, Terminal, ChildFailures>),
                (Self, Router, SocketAddr, io::Error),
                (Root, Spaces),
                (),
            >>,
        ),
        ((Self, Router, SocketAddr), TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Router: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> axum::Router,
{
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err(((self, router, address), error)),
        };
        let (execution, receiving) = execute_application_with::<
            _,
            Root,
            Root,
            Spaces,
            StructuralOrigins<Root::Base>,
            Terminal,
            (),
            (Self, Router, SocketAddr, io::Error),
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            Never,
        >(
            executor,
            (self, router, address),
            (),
            async |(_, _, address): &(Self, Router, SocketAddr)| TcpListener::bind(*address).await,
            |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
             startup_inputs,
             _allocations| {
                let (application, cold_work, publication) = original_work.into_unstarted();
                let () = cold_work;
                let (application, router, address) = application;
                let listener = match startup_inputs {
                    Ok(listener) => listener,
                    Err(error) => {
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected((
                                    application,
                                    router,
                                    address,
                                    error,
                                )),
                                publication,
                            )),
                        });
                    }
                };
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Supplied((
                                (router, listener),
                                async |(router, listener): (Router, TcpListener),
                                       application: ApplicationHandle<
                                    Root::Protocol,
                                    Root::Event,
                                >| {
                                    let lifecycle = application.lifecycle();
                                    let server_shutdown = lifecycle.termination();
                                    let serve = axum::serve(listener, router(application.clone()))
                                        .with_graceful_shutdown(async move {
                                            match server_shutdown.await {
                                                Ok(_) | Err(_) => {}
                                            }
                                        })
                                        .await;
                                    if serve.is_err() {
                                        match lifecycle.request_shutdown() {
                                            Ok(())
                                            | Err(
                                                ShutdownRejection::AlreadyStopping
                                                | ShutdownRejection::AlreadyStopped,
                                            ) => {}
                                        }
                                    }
                                    serve
                                },
                            )),
                            None,
                        ),
                        publication,
                    )),
                };
                let App {
                    root,
                    spaces,
                    families: (),
                } = application;
                let () = ();
                let installed_families = ();
                let (work, publication) = original_work.into_not_invoked();
                Ok((
                    ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces), work),
                            publication,
                        )),
                    },
                    installed_families,
                ))
            },
            ((), |_executor, (), ()| None),
            |root| root,
        );
        let receiving = async move { receiving.await.into_http() };
        Ok((execution, receiving))
    }

    #[cfg(feature = "axum")]
    /// Await asynchronous prebind, caller-local serving and the complete raw root result.
    /// Use `execute_axum` when a separately retained result receiver is required.
    ///
    /// # Errors
    /// Returns original cold inputs and the native entered-host error.
    /// Actual bind refusal remains an explicit `StagingRejected` product.
    ///
    /// # Panics
    /// Setup or serving can unwind; the original native cause remains with the caller.
    pub async fn run_axum<Terminal, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        ApplicationOutcome<
            (Self, Router, SocketAddr),
            (Router, TcpListener),
            Result<(), io::Error>,
            (RootOrigin<Root>, ActorRetirement<Root, Terminal, ChildFailures>),
            (Self, Router, SocketAddr, io::Error),
            (Root, Spaces),
            (),
        >,
        ((Self, Router, SocketAddr), TryCurrentError),
    >
where
    Root: BehaviorBase + BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + Send + 'static,
    Root::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
    BehaviorMessage<Root>: Send + 'static,
    Root::Sends: Send + 'static,
    Root::Error: Send + 'static,
    Root::InterpretationCustody: Send + 'static,
    Root::SourceCustody: Send + 'static,
    <Root::Birth as BirthMode>::Child:
        ChildOccurrenceProduct<RuntimeChildBindings<Terminal, StructuralOrigins<Root::Base>>> + Send + 'static,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
    ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>>:
        Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
    RootInterpreter<Root, Spaces, Terminal, StructuralOrigins<Root::Base>>: CommitActions<
            Root,
            Retired = (
                Vec<Terminal>,
                <ChildBindings<Root, Terminal, StructuralOrigins<Root::Base>> as RetireChildTasks>::Failures,
            ),
        > + Send
        + 'static,
    <Root as BehaviorSettlements>::Settlements: ClassifySettlement + Send,
    Terminal: Send + 'static,
    ChildFailures: Send + 'static,
    Router: FnOnce(ApplicationHandle<Root::Protocol, Root::Event>) -> axum::Router,
{
        let (execution, result) =
            self.execute_axum::<Terminal, ChildFailures, Router>(address, router)?;
        execution.await;
        Ok(result.await)
    }
}

#[allow(
    private_bounds,
    reason = "the running application and its launch proof remain private"
)]
impl<Root, Members> Application<Root, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
{
    #[cfg(feature = "axum")]
    /// Pair prebinding and caller-local HTTP serving with the same application owner.
    /// Bind refusal retains the untouched declaration/router/address/error before
    /// composition. Staging refusal retains actual remaining root/declarations,
    /// router and acquired listener. Each phase is explicit in the existing result.
    /// No root terminal conversion runs inside serving or the execution kernel.
    ///
    /// # Errors
    /// Missing host returns untouched cold inputs. `StagingRejected` inputs contain
    /// Ok(original bind-refusal inputs) or Err(actual partial composition inputs).
    /// Serving returns its exact `io::Error` beside the independently joined root.
    ///
    /// # Panics
    /// Native consuming composition or router causes remain caller-owned. The
    /// separately retained result receiver preserves every still-owned fact.
    #[expect(
        clippy::type_complexity,
        clippy::too_many_lines,
        reason = "retain original prebind and partial composition inputs beside exact HTTP/root results"
    )]
    pub fn execute_axum<Terminal, Actor, StagingFailure, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        (
            impl Future<Output = ()>,
            impl Future<
                Output = ApplicationOutcome<
                    (Self, Router, SocketAddr),
                    (Router, TcpListener),
                    Result<(), io::Error>,
                    (
                        RootOrigin<Root>,
                        ActorRetirement<Actor, Terminal, ChildFailures>,
                    ),
                    Result<
                        (Self, Router, SocketAddr, io::Error),
                        (Root, Router, StagingFailure, TcpListener),
                    >,
                    (Actor, ActorSpace<Root::Protocol>),
                    (),
                >,
            >,
        ),
        ((Self, Router, SocketAddr), TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Router: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> axum::Router,
    {
        let executor = match Handle::try_current() {
            Ok(executor) => executor,
            Err(error) => return Err(((self, router, address), error)),
        };
        let (execution, receiving) = execute_application_with::<
            _,
            Root,
            Actor,
            ActorSpace<Root::Protocol>,
            Members::Origins,
            Terminal,
            (),
            Result<
                (Self, Router, SocketAddr, io::Error),
                (Root, Router, StagingFailure, TcpListener),
            >,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            Never,
        >(
            executor,
            (self, router, address),
            (),
            async |(_, _, address): &(Self, Router, SocketAddr)| TcpListener::bind(*address).await,
            |original_work: ApplicationWorkPublication<_, _, _, _, _, _>,
             startup_inputs,
             _allocations| {
                let (application, cold_work, publication) = original_work.into_unstarted();
                let () = cold_work;
                let (application, router, address) = application;
                let listener = match startup_inputs {
                    Ok(listener) => listener,
                    Err(error) => {
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected(Ok((
                                    application,
                                    router,
                                    address,
                                    error,
                                ))),
                                publication,
                            )),
                        });
                    }
                };
                let original_work = ApplicationWorkPublication {
                    publication: Some((
                        ApplicationWorkCustody::NotInvoked(
                            ApplicationWorkPresence::Supplied((
                                (router, listener),
                                async |(router, listener): (Router, TcpListener),
                                       application: ApplicationHandle<
                                    Root::Protocol,
                                    Actor::Event,
                                >| {
                                    let lifecycle = application.lifecycle();
                                    let server_shutdown = lifecycle.termination();
                                    let serve = axum::serve(listener, router(application.clone()))
                                        .with_graceful_shutdown(async move {
                                            match server_shutdown.await {
                                                Ok(_) | Err(_) => {}
                                            }
                                        })
                                        .await;
                                    if serve.is_err() {
                                        match lifecycle.request_shutdown() {
                                            Ok(())
                                            | Err(
                                                ShutdownRejection::AlreadyStopping
                                                | ShutdownRejection::AlreadyStopped,
                                            ) => {}
                                        }
                                    }
                                    serve
                                },
                            )),
                            None,
                        ),
                        publication,
                    )),
                };
                let (root, members) = application.into_parts();
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        let ((router, listener), invoke) = work.into_supplied();
                        let _ = invoke;
                        let inputs = Err((root, router, failure, listener));
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected(inputs),
                                publication,
                            )),
                        });
                    }
                };
                let spaces = ActorSpace::new();
                let installed_families = ();
                let (work, publication) = original_work.into_not_invoked();
                Ok((
                    ApplicationWorkPublication {
                        publication: Some((
                            ApplicationWorkCustody::Prepared((root, spaces), work),
                            publication,
                        )),
                    },
                    installed_families,
                ))
            },
            ((), |_executor, (), ()| None),
            |root| root,
        );
        let receiving = async move { receiving.await.into_http() };
        Ok((execution, receiving))
    }

    #[cfg(feature = "axum")]
    /// Await the same prebind/serving/root pair. Use `execute_axum` to retain receiving separately.
    ///
    /// # Errors
    /// Missing host returns cold originals; all actual bind/staging/serving/root
    /// outcomes remain in their explicit independent result fields.
    ///
    /// # Panics
    /// Native composition/serving causes propagate; dropping this combined future
    /// surrenders both owning execution and receiving, unlike a separately retained pair.
    pub async fn run_axum<Terminal, Actor, StagingFailure, ChildFailures, Router>(
        self,
        address: SocketAddr,
        router: Router,
    ) -> Result<
        ApplicationOutcome<
            (Self, Router, SocketAddr),
            (Router, TcpListener),
            Result<(), io::Error>,
            (
                RootOrigin<Root>,
                ActorRetirement<Actor, Terminal, ChildFailures>,
            ),
            Result<
                (Self, Router, SocketAddr, io::Error),
                (Root, Router, StagingFailure, TcpListener),
            >,
            (Actor, ActorSpace<Root::Protocol>),
            (),
        >,
        ((Self, Router, SocketAddr), TryCurrentError),
    >
    where
        Members: ComposeApplication<Root, Actor = Actor, StagingFailure = StagingFailure>,
        Actor: BehaviorBase
            + BehaviorSettlements<Protocol = Root::Protocol, Ph = Never>
            + Send
            + 'static,
        Actor::Event: InjectEvent<ShutdownRequested, Here> + Send + 'static,
        BehaviorMessage<Actor>: Send + 'static,
        Actor::Sends: Send + 'static,
        Actor::Error: Send + 'static,
        Actor::InterpretationCustody: Send + 'static,
        Actor::SourceCustody: Send + 'static,
        <Actor::Birth as BirthMode>::Child: ChildOccurrenceProduct<RuntimeChildBindings<Terminal, Members::Origins>>
            + Send
            + 'static,
        ChildBindings<Actor, Terminal, Members::Origins>:
            Default + RetireChildTasks<Root = Terminal, Failures = ChildFailures> + Send + 'static,
        RootInterpreter<Actor, ActorSpace<Root::Protocol>, Terminal, Members::Origins>:
            CommitActions<Actor, Retired = (Vec<Terminal>, ChildFailures)> + Send + 'static,
        Actor::Settlements: ClassifySettlement + Send,
        RootOrigin<Root>: Send + 'static,
        Terminal: Send + 'static,
        ChildFailures: Send + 'static,
        Router: FnOnce(ApplicationHandle<Root::Protocol, Actor::Event>) -> axum::Router,
    {
        let (execution, receiving) = self
            .execute_axum::<Terminal, Actor, StagingFailure, ChildFailures, Router>(
                address, router,
            )?;
        execution.await;
        Ok(receiving.await)
    }
}
