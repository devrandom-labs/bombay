use bombay::ActorCustody;

use bombay::behavior::{BehaviorSettlements, Never, Protocol};
use bombay::prelude::{ActorRetirement, Completion, MailAddr, RootOrigin};
#[cfg(feature = "remote")]
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
#[cfg(feature = "remote")]
use std::num::NonZeroUsize;
#[cfg(feature = "remote")]
use std::path::PathBuf;

#[cfg(feature = "remote")]
#[allow(
    dead_code,
    reason = "integration tests compile their shared support independently"
)]
pub(crate) fn listening_network(address: String) -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    Network {
        layout: NetworkLayout::Peer {
            connect: None,
            listen: Some(ConnectionEndpoints {
                addresses: vec![address],
                certificate: CredentialSource::File(certificates.join("recipient-certificate.pem")),
                private_key: CredentialSource::File(certificates.join("recipient-private-key.pem")),
            }),
        },
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}

pub(crate) type RootTerminal<R> = (RootOrigin<R>, ActorRetirement<R, Never, Vec<Never>>);

#[allow(
    dead_code,
    reason = "integration tests compile their shared support independently"
)]
pub(crate) fn assert_completed<R>(terminal: RootTerminal<R>, expected_terminal_report: Option<()>)
where
    R: BehaviorSettlements<Protocol: Protocol<Addr = MailAddr>, Ph = Never>,
{
    let (
        origin,
        ActorRetirement::Completed {
            custody:
                ActorCustody {
                    capability_failures,
                    unread_owner_cancellation,
                    behavior,
                    control,
                    user,
                    descendants,
                    child_failures,
                    additional_failures: retirement_additional_failures,
                    received_interpretation: retirement_received_interpretation,
                    received_source: retirement_received_source,
                    source_index: retirement_source_index,
                    acquired_ingress: retirement_acquired_ingress,
                    retirement_failures: retirement_native_failures,
                    terminal_report: retirement_terminal_report,
                },
            settlements,
            completion,
            interpretation: retirement_interpretation,
            source: retirement_source,
        },
    ) = terminal
    else {
        panic!("the application root must retain its completed terminal state")
    };
    assert_eq!(child_failures, []);
    assert!(retirement_interpretation.is_none());
    assert!(retirement_source.is_none());
    assert!(retirement_additional_failures.is_empty());
    assert!(retirement_received_interpretation.is_none());
    assert!(retirement_received_source.is_none());
    assert!(retirement_source_index.is_none());
    assert!(retirement_acquired_ingress.is_none());
    assert!(retirement_native_failures.is_empty());
    match (retirement_terminal_report, expected_terminal_report) {
        (None, None) | (Some(Ok(())), Some(())) => {}
        (None, Some(())) => {
            panic!("the selected terminal report must retain its acquired publication")
        }
        (Some(Ok(())), None) => panic!("this root must not select a terminal report"),
        (Some(Err(termination)), None | Some(())) => {
            panic!("the original terminal report publication was refused: {termination:?}");
        }
    }
    assert!(capability_failures.is_empty());
    assert!(unread_owner_cancellation.is_none());
    assert_eq!(origin.address(), MailAddr::APPLICATION_ROOT);
    drop(behavior);
    assert_eq!(settlements.len(), 1);
    assert!(control.is_empty());
    assert!(user.is_empty());
    assert_eq!(descendants, []);
    assert_eq!(completion, Completion::Stopped);
}
