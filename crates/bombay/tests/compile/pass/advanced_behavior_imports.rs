use bombay::behavior::{
    Behavior, CancelObservation, EndpointAddress, EstablishedObservation, InterpretDelivery,
    LogicalHostRequirements, ObservationId, ObserveEstablished, ObserveEstablishedCreation,
    SendInput,
};
use bombay::prelude::Protocol;
use bombay::MailAddr;

fn logical_host_requirements_are_deliberate<T: LogicalHostRequirements>() {}

fn behavior_implementation_is_deliberate<T: Behavior>() {}

fn delivery_interpreter_is_deliberate<I, P>()
where
    P: Protocol,
    P::Addr: Send,
    P::Msg: Send,
    I: InterpretDelivery<P>,
{
}

fn send_input_is_deliberate<T, Input, Path>()
where
    T: SendInput<Input, Path>,
{
}

fn unsettled_observation_types_are_deliberate<P, Occurrence>()
where
    P: Protocol,
    P::Addr: EndpointAddress,
{
    let _: Option<CancelObservation<P>> = None;
    let _: Option<EstablishedObservation<P>> = None;
    let _: Option<ObserveEstablished<P>> = None;
    let _: Option<ObserveEstablishedCreation<P, Occurrence>> = None;
    let _correlation = ObservationId(0);
}

struct ObservationProtocol;

impl Protocol for ObservationProtocol {
    type Addr = MailAddr;
    type Msg = ();
}

fn main() {
    unsettled_observation_types_are_deliberate::<ObservationProtocol, ()>();
}
