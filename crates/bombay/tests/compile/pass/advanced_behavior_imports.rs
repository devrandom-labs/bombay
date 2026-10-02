use bombay::behavior::{
    Behavior, Delivery, InterpretItem, LogicalHostRequirements, SendInput,
};
use bombay::prelude::Protocol;
use bombay::MailAddr;

fn logical_host_requirements_are_deliberate<T: LogicalHostRequirements>() {}

fn behavior_implementation_is_deliberate<T: Behavior>() {}

fn item_interpreter_is_deliberate<I, P, RootEvent, Path>()
where
    P: Protocol,
    P::Addr: Send,
    P::Msg: Send,
    I: InterpretItem<Delivery<P>, RootEvent, Path>,
{
}

fn send_input_is_deliberate<T, Input, Path>()
where
    T: SendInput<Input, Path>,
{
}

struct ObservationProtocol;

impl Protocol for ObservationProtocol {
    type Addr = MailAddr;
    type Msg = ();
}

fn main() {}
