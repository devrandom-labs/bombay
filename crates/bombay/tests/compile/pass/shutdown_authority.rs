use bombay::behavior::{Here, InjectEvent, Protocol};
use behavior_actors::ShutdownRequested;
use bombay::prelude::{ActorRef, ApplicationLifecycle};

fn ordinary_delivery<P: Protocol>(recipient: ActorRef<P>, from: P::Addr, message: P::Msg) {
    let another_recipient = recipient.clone();
    let _delivery = recipient.send_from(from, message);
    let _termination = another_recipient.termination();
}

fn typed_shutdown<P: Protocol, Event>(lifecycle: ApplicationLifecycle<P, Event>)
where
    Event: InjectEvent<ShutdownRequested, Here>,
{
    let _request = lifecycle.request_shutdown();
    let _termination = lifecycle.termination();
}

fn main() {}
