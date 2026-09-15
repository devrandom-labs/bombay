use bombay::prelude::*;

fn admit<P>(actor: &ActorRef<P>, from: P::Addr, message: P::Msg)
where
    P: Protocol,
{
    let _ = actor.send(from, message);
}

fn main() {}
