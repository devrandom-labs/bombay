//! Independent connection cleanup through the existing Application owner.

use super::Network;
use crate::remote::startup::NodeStartupRefusal;
use core::future::Future;
use std::error::Error;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;
use zenoh::Session;

/// Keep the live session and its sole cleanup signal in the same owner.
pub(crate) struct Connection {
    session: Session,
    retirement: oneshot::Sender<()>,
}

impl Connection {
    pub(crate) async fn open(
        network: &Network,
    ) -> Result<
        (
            Self,
            impl Future<
                Output = (
                    Result<(), JoinError>,
                    Result<Result<(), Box<dyn Error + Send + Sync>>, RecvError>,
                ),
            > + use<>,
        ),
        NodeStartupRefusal,
    > {
        let configuration = network.configuration()?;
        let executor = Handle::try_current().map_err(|cause| NodeStartupRefusal::Host { cause })?;
        let flavor = executor.runtime_flavor();
        if flavor != RuntimeFlavor::MultiThread {
            return Err(NodeStartupRefusal::HostFlavor { flavor });
        }
        let session = zenoh::open(configuration)
            .await
            .map_err(|cause| NodeStartupRefusal::Open { cause })?;
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
            // The receiving channel is polled only after this task joins.
            // Preserve the actual close result before the strong Session drops.
            let refused = publication.send(result).err();
            drop(session);
            drop(refused);
        });
        let receiving = async move {
            let joined = cleanup.await;
            let result = received.await;
            (joined, result)
        };
        Ok((connection, receiving))
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
