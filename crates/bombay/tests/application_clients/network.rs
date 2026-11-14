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
fn reserve_listener() -> TcpListener {
    TcpListener::bind(("localhost", 0)).unwrap()
}

pub(crate) fn listener_address() -> String {
    let reservation = reserve_listener();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    format!("tls/localhost:{}", address.port())
}

#[cfg(test)]
mod tests {
    use super::reserve_listener;
    use std::io::ErrorKind;
    use std::net::TcpListener;

    #[tokio::test(flavor = "current_thread")]
    async fn listener_reservation_matches_the_configured_host() {
        let reservation = reserve_listener();
        let reserved = reservation.local_addr().expect("the socket is bound");
        let address = format!("localhost:{}", reserved.port());
        let selected = tokio::net::lookup_host(address.as_str())
            .await
            .expect("the configured local host resolves")
            .next()
            .expect("the TLS owner selects the first resolved address");
        let attempt = TcpListener::bind(selected);
        let refusal = match attempt {
            Err(cause) => cause,
            Ok(listener) => {
                let actual = listener.local_addr().expect("the second socket is bound");
                drop(listener);
                drop(reservation);
                panic!(
                    "the TLS address {actual} accepted a listener while {reserved} remained held"
                );
            }
        };
        drop(reservation);

        assert_eq!(refusal.kind(), ErrorKind::AddrInUse);
        assert_eq!(reserved, selected);
    }
}
