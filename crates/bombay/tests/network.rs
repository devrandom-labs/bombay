#![cfg(feature = "remote")]

mod application_support;
#[path = "../../../examples/counter/src/counter.rs"]
#[expect(dead_code, reason = "the network caller exercises increment and read")]
mod counter;

use application_support::assert_completed;
use bombay::Application;
use bombay::actors::ActorExt;
use bombay::remote::{
    ConnectionEndpoints, CredentialSource, Network, NetworkLayout, NodeStartupRefusal,
};
use counter::{Counter, CounterMessage};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::RuntimeFlavor;
use tokio::time::timeout;

fn listening_network() -> Network {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/prds/static-remote-actors/transport-custody/certificates");
    Network {
        layout: NetworkLayout::Peer {
            connect: None,
            listen: Some(ConnectionEndpoints {
                addresses: vec!["localhost:0".to_owned()],
                certificate: CredentialSource::File(certificates.join("recipient-certificate.pem")),
                private_key: CredentialSource::File(certificates.join("recipient-private-key.pem")),
            }),
        },
        deployment_roots: CredentialSource::File(certificates.join("root-ca.pem")),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "inspect every original network allocation and the complete recovered application retirement"
)]
async fn host_refusal_returns_original_network_and_usable_application() {
    let completed = timeout(Duration::from_secs(10), async {
        let network = listening_network();
        let NetworkLayout::Peer {
            listen: Some(listener),
            ..
        } = &network.layout
        else {
            panic!("the supplied listening direction");
        };
        let addresses_allocation = listener.addresses.as_ptr();
        let address_allocation = listener.addresses[0].as_ptr();
        let CredentialSource::File(certificate) = &listener.certificate else {
            panic!("the supplied listening certificate file");
        };
        let certificate_allocation = certificate.as_os_str().as_encoded_bytes().as_ptr();
        let original_certificate = certificate.clone();
        let CredentialSource::File(private_key) = &listener.private_key else {
            panic!("the supplied listening private-key file");
        };
        let private_key_allocation = private_key.as_os_str().as_encoded_bytes().as_ptr();
        let original_private_key = private_key.clone();
        let CredentialSource::File(roots) = &network.deployment_roots else {
            panic!("the supplied deployment roots file");
        };
        let roots_allocation = roots.as_os_str().as_encoded_bytes().as_ptr();
        let original_roots = roots.clone();
        let refused = Application::new(Counter::new().stop_on_shutdown())
            .open_node(network)
            .await;
        let Err((application, network, cause)) = refused else {
            panic!("the native network requires a multithreaded host");
        };
        let NetworkLayout::Peer {
            connect: None,
            listen: Some(listener),
        } = &network.layout
        else {
            panic!("the original network layout must survive refusal");
        };
        let CredentialSource::File(roots) = &network.deployment_roots else {
            panic!("the original roots declaration must survive refusal");
        };
        let CredentialSource::File(certificate) = &listener.certificate else {
            panic!("the original certificate declaration must survive refusal");
        };
        let CredentialSource::File(private_key) = &listener.private_key else {
            panic!("the original private-key declaration must survive refusal");
        };
        assert_eq!(listener.addresses.as_ptr(), addresses_allocation);
        assert_eq!(listener.addresses.len(), 1);
        assert_eq!(listener.addresses[0].as_ptr(), address_allocation);
        assert_eq!(listener.addresses[0], "localhost:0");
        assert_eq!(
            roots.as_os_str().as_encoded_bytes().as_ptr(),
            roots_allocation
        );
        assert_eq!(
            certificate.as_os_str().as_encoded_bytes().as_ptr(),
            certificate_allocation
        );
        assert_eq!(
            private_key.as_os_str().as_encoded_bytes().as_ptr(),
            private_key_allocation
        );
        assert_eq!(roots, &original_roots);
        assert_eq!(certificate, &original_certificate);
        assert_eq!(private_key, &original_private_key);
        assert_eq!(network.max_retained_queries.get(), 2);
        assert!(matches!(
            cause,
            NodeStartupRefusal::HostFlavor {
                flavor: RuntimeFlavor::CurrentThread
            }
        ));
        let outcome = application
            .run_with(|application| async move {
                let mut caller = application.client().expect("original Counter client");
                caller
                    .send(CounterMessage::Read(caller.recipient()))
                    .await
                    .expect("original Counter read");
                let reply = caller.receive().await.expect("original Counter value");
                application
                    .request_shutdown()
                    .expect("original Counter stop");
                reply.message
            })
            .await
            .unwrap_or_else(|_| panic!("the returned Application remains executable"));
        let (value, retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("the original Counter work completes"));
        let (cleanup, root, notifications, ()) = retirement.into_parts();
        let notifications = notifications.expect("original notification receipts");
        assert_completed(root.expect("original native Counter retirement"), None);
        assert_eq!(value, 0);
        assert!(cleanup.is_ok());
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
    })
    .await;
    completed.expect("network startup, work and retirement finish within the watchdog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_opens_actual_node_with_local_work_and_complete_retirement() {
    let completed = timeout(Duration::from_secs(10), async {
        let original = Rc::new(41_u64);
        let acquired = Rc::clone(&original);
        let mut node = Application::new(Counter::new().stop_on_shutdown())
            .open_node(listening_network())
            .await
            .unwrap_or_else(|(_, _, cause)| panic!("actual private TLS listener: {cause:?}"));
        let outcome = node
            .run_with(|application| async move {
                let mut caller = application.client().expect("Counter client");
                caller
                    .send(CounterMessage::Increment)
                    .await
                    .expect("increment admitted");
                caller
                    .send(CounterMessage::Read(caller.recipient()))
                    .await
                    .expect("read admitted");
                let reply = caller.receive().await.expect("Counter value");
                application
                    .request_shutdown()
                    .expect("Counter stop admitted");
                (reply.message, acquired)
            })
            .await
            .unwrap_or_else(|_| panic!("the existing Node execution starts"));
        let ((value, acquired), retirement) = outcome
            .try_into_completed()
            .unwrap_or_else(|_| panic!("actual Node work completes"));
        let (cleanup, root, notifications, ()) = retirement.into_parts();
        let notifications = notifications.expect("complete notification receipts");
        let closed = node.close().await;
        assert_completed(root.expect("actual native Counter retirement"), None);
        assert_eq!(value, 1);
        assert!(Rc::ptr_eq(&acquired, &original));
        assert_eq!(*acquired, 41);
        assert!(cleanup.is_ok());
        assert!(notifications.termination.is_ok());
        assert!(notifications.retirement.is_ok());
        assert!(closed.is_ok());
    })
    .await;
    completed.expect("network startup, work and retirement finish within the watchdog");
}

#[tokio::test]
async fn endpoint_options_refuse_before_open_with_original_address() {
    let completed = timeout(Duration::from_secs(10), async {
        let mut network = listening_network();
        let NetworkLayout::Peer {
            listen: Some(listener),
            ..
        } = &mut network.layout
        else {
            panic!("the supplied listening direction");
        };
        listener.addresses[0] = "localhost:0#use_public_pki=true".to_owned();
        let allocation = listener.addresses[0].as_ptr();
        let refused = Application::new(Counter::new().stop_on_shutdown())
            .open_node(network)
            .await;
        let Err((_application, network, cause)) = refused else {
            panic!("endpoint options must not bypass the selected profile");
        };
        let NetworkLayout::Peer {
            listen: Some(listener),
            ..
        } = &network.layout
        else {
            panic!("the original listening direction");
        };
        assert_eq!(listener.addresses[0].as_ptr(), allocation);
        assert_eq!(listener.addresses[0], "localhost:0#use_public_pki=true");
        assert!(matches!(
            cause,
            NodeStartupRefusal::Unsupported {
                field: "listen/endpoints"
            }
        ));
    })
    .await;
    completed.expect("network startup, work and retirement finish within the watchdog");
}

#[test]
fn network_debug_omits_credential_files_and_values() {
    let secrets = [
        "private-deployment-roots.pem",
        "Y29ubmVjdC1zZWNyZXQ=",
        "private-connect-key.pem",
        "private-listen-certificate.pem",
        "bGlzdGVuLXNlY3JldA==",
    ];
    let connect = ConnectionEndpoints {
        addresses: vec!["router.example.test:7447".to_owned()],
        certificate: CredentialSource::Base64(secrets[1].into()),
        private_key: CredentialSource::File(secrets[2].into()),
    };
    let listen = ConnectionEndpoints {
        addresses: vec!["localhost:0".to_owned()],
        certificate: CredentialSource::File(secrets[3].into()),
        private_key: CredentialSource::Base64(secrets[4].into()),
    };
    let network = Network {
        deployment_roots: CredentialSource::File(secrets[0].into()),
        max_retained_queries: NonZeroUsize::new(2).unwrap(),
        layout: NetworkLayout::Peer {
            connect: Some(connect),
            listen: Some(listen),
        },
    };
    let NetworkLayout::Peer {
        connect: Some(connect),
        listen: Some(listen),
    } = &network.layout
    else {
        panic!("the supplied diagnostic fixture has both directions");
    };
    let diagnostics = [
        format!("{network:?}"),
        format!("{:?}", network.deployment_roots),
        format!("{connect:?}"),
        format!("{listen:?}"),
        format!("{:?}", connect.certificate),
        format!("{:?}", connect.private_key),
        format!("{:?}", listen.certificate),
        format!("{:?}", listen.private_key),
    ];
    for diagnostic in diagnostics {
        for secret in secrets {
            assert!(
                !diagnostic.contains(secret),
                "credential contents must stay out of ordinary Debug output"
            );
        }
    }
}
