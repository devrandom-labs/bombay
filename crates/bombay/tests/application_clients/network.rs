//! Shared concrete TLS inputs for public remote application tests.
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::PathBuf;

pub(crate) fn configuration(address: &str, role: &EndpointRole) -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    let (certificate, private_key) = match role {
        EndpointRole::Receiver => ("recipient-certificate.pem", "recipient-private-key.pem"),
        EndpointRole::Caller => ("caller-certificate.pem", "caller-private-key.pem"),
    };
    let endpoints = ConnectionEndpoints {
        addresses: vec![
            address
                .strip_prefix("tls/")
                .expect("the fixture uses TLS")
                .to_owned(),
        ],
        certificate: CredentialSource::File(certificates.join(certificate)),
        private_key: CredentialSource::File(certificates.join(private_key)),
    };
    let layout = match role {
        EndpointRole::Receiver => NetworkLayout::Peer {
            connect: None,
            listen: Some(endpoints),
        },
        EndpointRole::Caller => NetworkLayout::Peer {
            connect: Some(endpoints),
            listen: None,
        },
    };
    Network {
        layout,
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}
pub(crate) enum EndpointRole {
    Caller,
    Receiver,
}
pub(crate) fn reserved_address() -> String {
    let reservation = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    format!("tls/localhost:{}", address.port())
}
