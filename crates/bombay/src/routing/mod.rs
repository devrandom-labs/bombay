//! Typed actor references, address registration, and delivery routing.

mod actor_ref;
mod delivery;

pub use actor_ref::{ActorRef, MailboxDeliveryClosed, ShutdownRequestError};
pub(crate) use delivery::PeerOutcome;
pub use delivery::{
    AddressInUse, AddressRouter, DeliveryEndpoint, DeliveryRouter, EndpointRegistry,
    IncarnationEndpoint, ObservesCreations, PeerObservationError, PeerObserver, RejectedDelivery,
    RouteSends, RoutingError,
};
#[cfg(test)]
pub(crate) use delivery::{ReceiveTimeoutSendsError, WatchSendsError};
