//! Scratch: measure post-#225 slot sizes for the tripwire tests. Deleted after use.

use std::mem::size_of;

use bombay::SendContext;
use bombay::mailbox::{ControlSignal, MailboxSender, Mailboxed, Recv, Signal};

struct Probe;
impl Mailboxed for Probe {
    type Msg = u64;
}

enum SmallMsg {
    Ping,
    Pong(u64),
}
struct Small;
impl Mailboxed for Small {
    type Msg = SmallMsg;
}

#[expect(
    clippy::print_stdout,
    dead_code,
    reason = "a measurement scratch binary prints its results"
)]
fn main() {
    println!("flume::Sender<()>     = {}", size_of::<flume::Sender<()>>());
    println!("flume::Receiver<()>   = {}", size_of::<flume::Receiver<()>>());
    println!("MailboxSender<Probe>  = {}", size_of::<MailboxSender<Probe>>());
    println!("SendContext           = {}", size_of::<SendContext>());
    println!("Signal<Probe>         = {}", size_of::<Signal<Probe>>());
    println!("Signal<Small>         = {}", size_of::<Signal<Small>>());
    println!("ControlSignal         = {}", size_of::<ControlSignal>());
    println!("Recv<Probe>           = {}", size_of::<Recv<Probe>>());
    println!("ActorId               = {}", size_of::<bombay::ActorId>());
}
