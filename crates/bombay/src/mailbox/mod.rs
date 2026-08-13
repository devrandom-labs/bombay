//! Actor mailbox ownership and Bombay Communication integration.

mod communication;
mod protocol;

pub use communication::{MailboxAnchor, MailboxConfig, MailboxReceiver, MailboxSender};
pub use protocol::{EventSender, EventSource};
