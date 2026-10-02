use bombay::prelude::*;

fn foundational<I, P, RootEvent, Path>()
where
    P: Protocol,
    P::Addr: Send,
    P::Msg: Send,
    I: InterpretItem<bombay::behavior::Delivery<P>, RootEvent, Path>,
{
}

fn main() {}
