use crate::address::MailAddr;
use crate::application::composition::ComposeApplication;
use crate::application::execution::{
    ApplicationWorkCustody, ApplicationWorkPresence, ApplicationWorkPublication, RootInterpreter,
    execute_application_with,
};
use crate::application::{Application, ApplicationHandle, ApplicationOutcome};
use crate::local::children::{ChildBindings, RetireChildTasks, RuntimeChildBindings};
use crate::local::effects::CommitActions;
use crate::terminal::{ActorRetirement, RootOrigin};
use crate::topology::Hosts;
use behavior::{
    Behavior, BehaviorBase, BehaviorMessage, BehaviorSettlements, BirthMode,
    ChildOccurrenceProduct, ClassifySettlement, Never, Protocol,
};
use core::future::Future;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::runtime::{Handle, TryCurrentError};

#[allow(
    private_bounds,
    reason = "the running application and its launch proof remain private"
)]
impl<Root, Spaces, Members> Application<Root, Spaces, Members>
where
    Root: Behavior<Protocol: Protocol<Addr = MailAddr>, Ph = Never> + BehaviorBase,
    Members: ComposeApplication<Root>,
    Spaces: Hosts<Root::Protocol> + Send + Sync + 'static,
{
    #[cfg(feature = "axum")]
    /// Pair prebinding and caller-local HTTP serving with the same application owner.
    /// Bind refusal retains the untouched declaration/router/address/error before
    /// composition. Staging refusal retains actual remaining root/declarations,
    /// router and acquired listener. Each phase is explicit in the existing result.
    /// No root terminal conversion runs inside serving or the execution kernel.
    /// Pinned Axum graceful serving finishes after root termination and has no
    /// serving-error transition; execution needs no root shutdown capability.
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
                        (Root, Spaces, Router, StagingFailure, TcpListener),
                    >,
                    (Actor, Spaces),
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
        Actor::Event: Send + 'static,
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
        RootInterpreter<Actor, Spaces, Terminal, Members::Origins>:
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
            Spaces,
            Members::Origins,
            Terminal,
            (),
            Result<
                (Self, Router, SocketAddr, io::Error),
                (Root, Spaces, Router, StagingFailure, TcpListener),
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
                                    axum::serve(listener, router(application.clone()))
                                        .with_graceful_shutdown(async move {
                                            match server_shutdown.await {
                                                Ok(_) | Err(_) => {}
                                            }
                                        })
                                        .await
                                },
                            )),
                            None,
                        ),
                        publication,
                    )),
                };
                let (root, spaces, members, ()) = application.into_parts();
                let root = match members.compose(root) {
                    Ok(actor) => actor,
                    Err((root, failure)) => {
                        let (work, publication) = original_work.into_not_invoked();
                        let ((router, listener), invoke) = work.into_supplied();
                        let _ = invoke;
                        let inputs = Err((root, spaces, router, failure, listener));
                        return Err(ApplicationWorkPublication {
                            publication: Some((
                                ApplicationWorkCustody::StagingRejected(inputs),
                                publication,
                            )),
                        });
                    }
                };
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
                (Root, Spaces, Router, StagingFailure, TcpListener),
            >,
            (Actor, Spaces),
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
        Actor::Event: Send + 'static,
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
        RootInterpreter<Actor, Spaces, Terminal, Members::Origins>:
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
