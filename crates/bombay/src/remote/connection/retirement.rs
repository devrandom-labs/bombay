//! Independent connection cleanup through the existing Application owner.

use super::Network;
use crate::remote::startup::NodeStartupRefusal;
use core::future::{Future, IntoFuture, poll_fn};
use core::pin::pin;
use core::task::Poll;
use std::error::Error;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::{JoinError, JoinHandle};
use zenoh::Session;

/// Keep the live session and its sole cleanup signal in the same owner.
pub(crate) struct Connection {
    session: Session,
    retirement: oneshot::Sender<()>,
}

impl Connection {
    /// Create acquisition and independent startup/close receiving before polling.
    /// The actual publication return retains a report refused by its receiver.
    /// An absent Connection never substitutes for the separately owned cause.
    #[expect(
        clippy::type_complexity,
        reason = "preserve the actual startup publication, unique connection and independent native close receipts without another owner"
    )]
    pub(crate) fn startup() -> (
        impl AsyncFnOnce(
            &Network,
        ) -> (
            Option<Self>,
            Result<
                (),
                Result<
                    (
                        JoinHandle<()>,
                        oneshot::Receiver<Result<(), Box<dyn Error + Send + Sync>>>,
                    ),
                    NodeStartupRefusal,
                >,
            >,
        ),
        impl Future<
            Output = Result<
                Result<
                    (
                        Result<(), JoinError>,
                        Result<Result<(), Box<dyn Error + Send + Sync>>, RecvError>,
                    ),
                    NodeStartupRefusal,
                >,
                RecvError,
            >,
        > + use<>,
    ) {
        let (startup_publication, reported) = oneshot::channel();
        let acquire = async move |network: &Network| {
            let configuration = match network.configuration() {
                Ok(configuration) => configuration,
                Err(cause) => return (None, startup_publication.send(Err(cause))),
            };
            let executor = match Handle::try_current() {
                Ok(executor) => executor,
                Err(cause) => {
                    return (
                        None,
                        startup_publication.send(Err(NodeStartupRefusal::Host { cause })),
                    );
                }
            };
            let flavor = executor.runtime_flavor();
            if flavor != RuntimeFlavor::MultiThread {
                return (
                    None,
                    startup_publication.send(Err(NodeStartupRefusal::HostFlavor { flavor })),
                );
            }
            let mut opening = pin!(zenoh::open(configuration).into_future());
            let mut startup_publication = Some(startup_publication);
            poll_fn(|context| match opening.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(original) => {
                    let (connection, report) = match original {
                        Ok(session) => {
                            let (retirement, requested) = oneshot::channel();
                            let (publication, received) = oneshot::channel();
                            let connection = Self {
                                session: session.clone(),
                                retirement,
                            };
                            let cleanup = executor.spawn(async move {
                                match requested.await {
                                    Ok(()) | Err(_) => {}
                                }
                                let result = session.close().await;
                                // This receiver is read only after its producer joins.
                                let refused = publication.send(result).err();
                                drop(session);
                                drop(refused);
                            });
                            (Some(connection), Ok((cleanup, received)))
                        }
                        Err(cause) => (None, Err(NodeStartupRefusal::Open { cause })),
                    };
                    let publication = startup_publication
                        .take()
                        .expect("one actual open result consumes its startup publication");
                    // Transfer before either the SDK operation or this producer drops.
                    Poll::Ready((connection, publication.send(report)))
                }
            })
            .await
        };
        let receiving = async move {
            match reported.await {
                Ok(Ok((cleanup, received))) => {
                    let joined = cleanup.await;
                    let result = received.await;
                    Ok(Ok((joined, result)))
                }
                Ok(Err(cause)) => Ok(Err(cause)),
                Err(cause) => Err(cause),
            }
        };
        (acquire, receiving)
    }

    pub(crate) fn session(&self) -> &Session {
        &self.session
    }

    pub(crate) fn retire(self) {
        let Self {
            session,
            retirement,
        } = self;
        drop(session);
        drop(retirement);
    }
}
