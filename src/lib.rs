#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(unused_must_use)]

pub mod actor;
#[cfg(feature = "console")]
pub mod console;
pub mod error;
// `links` is an internal module. Under `testing` it is exposed publicly ONLY so
// the cucumber wiring can reach `links::testing` (and the `Links` /
// `LinkDiedParts` types it returns); the production build keeps it `pub(crate)`.
// The pre-existing internal items it exposes are not part of the public API, so
// `missing_docs` is allowed on the test-only public surface.
#[cfg(any(test, feature = "testing"))]
#[allow(missing_docs)]
pub mod links;
#[cfg(not(any(test, feature = "testing")))]
pub(crate) mod links;
pub mod mailbox;
pub mod message;
#[cfg(not(feature = "remote"))]
pub mod registry;
#[cfg(feature = "remote")]
pub mod remote;
pub mod reply;
pub mod request;
pub mod supervision;

pub use actor::Actor;
#[cfg(feature = "macros")]
pub use bombay_macros::{Actor, RemoteActor, Reply, messages, remote_message};
pub use reply::Reply;

#[cfg(all(feature = "otel", not(feature = "tracing")))]
compile_error!("the `otel` feature requires the `tracing` feature to be enabled");

/// Commonly used types and functions that can be imported with a single use statement.
///
/// ```
/// use bombay::prelude::*;
/// ```
///
/// This module includes the most essential actor components, messaging types,
/// and traits needed for typical actor system usage.
pub mod prelude {
    #[cfg(feature = "macros")]
    pub use bombay_macros::{Actor, RemoteActor, Reply, messages, remote_message};

    #[cfg(feature = "remote")]
    pub use crate::actor::RemoteActorRef;
    pub use crate::actor::{
        Actor, ActorId, ActorRef, PreparedActor, Recipient, ReplyRecipient, Spawn, WeakActorRef,
        WeakRecipient, WeakReplyRecipient,
    };
    #[cfg(feature = "remote")]
    pub use crate::error::RemoteSendError;
    pub use crate::error::{ActorStopReason, PanicError, PanicReason, SendError};
    pub use crate::mailbox::{self, MailboxReceiver, MailboxSender};
    pub use crate::message::{Context, Message};
    #[cfg(feature = "remote")]
    pub use crate::remote::{self, RemoteActor, RemoteMessage};
    pub use crate::reply::{DelegatedReply, ForwardedReply, Reply, ReplyError, ReplySender};
}
