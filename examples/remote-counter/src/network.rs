//! Explicit TLS inputs; no networking implementation or discovery fallback.
use super::deployment::Role;
use bombay::remote::{ConnectionEndpoints, CredentialSource, Network, NetworkLayout};
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::path::Path;

pub(crate) fn configuration(address: String, certificates: &Path, role: Role) -> Network {
    let (certificate, private_key) = match role {
        Role::Serve => ("recipient-certificate.pem", "recipient-private-key.pem"),
        Role::Call => ("caller-certificate.pem", "caller-private-key.pem"),
    };
    let endpoints = ConnectionEndpoints {
        addresses: vec![address],
        certificate: CredentialSource::File(certificates.join(certificate)),
        private_key: CredentialSource::File(certificates.join(private_key)),
    };
    let layout = match role {
        Role::Serve => NetworkLayout::Peer {
            connect: None,
            listen: Some(endpoints),
        },
        Role::Call => NetworkLayout::Peer {
            connect: Some(endpoints),
            listen: None,
        },
    };
    Network {
        layout,
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(3).expect("three finite native operations"),
        max_sessions: NonZeroUsize::new(2).expect("two registered transports"),
        max_pending_links: NonZeroUsize::new(2).expect("two pending links per native stage"),
        batch_bytes: NonZeroU16::new(4_096).expect("a four KiB batch offer"),
        receive_buffer_bytes: NonZeroU32::new(256 * 1_024).expect("a 256 KiB receive buffer limit"),
        reassembly_bytes: NonZeroUsize::new(32_768).expect("a 32 KiB reassembly limit"),
    }
}
