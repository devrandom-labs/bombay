#![cfg(feature = "remote")]

#[path = "../../../examples/counter/src/counter.rs"]
mod counter;

use bombay::Application;
use bombay::actors::ActorExt;
use bombay::remote::NodeStartupRefusal;
use counter::Counter;
use serde::Deserialize;
use std::error::Error;
use std::io;
use std::num::NonZeroUsize;
use std::ptr;
use tokio::{runtime::RuntimeFlavor, sync::Semaphore};
use zenoh::config::{EndPoint, EndPoints, WhatAmI};
use zenoh_config::{Config, ModeDependent, ModeDependentValue, SecretValue};

fn declared_secret(encoded: &str) -> SecretValue {
    SecretValue::deserialize(serde_json::Value::String(encoded.to_owned()))
        .expect("existing native secret string deserialization")
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "compare one native configuration with all its independent credential and profile getters"
)]
fn typed_configuration_preserves_separate_credentials_without_config_serialization() {
    let mut config = Config::default();
    config
        .set_id(None)
        .expect("native fresh identity selection");
    config
        .set_mode(Some(WhatAmI::Peer))
        .expect("native peer mode");
    config
        .set_queries_max_retained(NonZeroUsize::new(7))
        .expect("explicit native query capacity");
    let connector = EndPoint::new("tls", "router.example.test:7447", "", "")
        .expect("native DNS endpoint grammar");
    let listener =
        EndPoint::new("tls", "[::1]:7448", "", "").expect("native IPv6 endpoint grammar");
    config
        .connect
        .set_endpoints(ModeDependentValue::Unique(vec![EndPoints::Single(
            connector,
        )]))
        .expect("typed outgoing endpoints");
    config
        .listen
        .set_endpoints(ModeDependentValue::Unique(vec![listener]))
        .expect("typed incoming endpoints");
    config
        .scouting
        .multicast
        .set_enabled(Some(false))
        .expect("disable multicast");
    config
        .scouting
        .multicast
        .set_listen(Some(ModeDependentValue::Unique(false)))
        .expect("disable multicast listening");
    config
        .scouting
        .gossip
        .set_enabled(Some(false))
        .expect("disable gossip");
    config
        .transport
        .link
        .set_protocols(Some(vec!["tls".into()]))
        .expect("TLS only");
    let tls = &mut config.transport.link.tls;
    tls.set_enable_mtls(Some(true)).expect("mutual TLS");
    tls.set_verify_name_on_connect(Some(true))
        .expect("verify names");
    tls.set_use_public_pki(Some(false))
        .expect("deployment roots only");
    tls.set_root_ca_certificate_base64(Some(declared_secret("cm9vdHM=")))
        .expect("roots");
    tls.set_connect_certificate_base64(Some(declared_secret("Y2xpZW50")))
        .expect("outgoing certificate");
    tls.set_connect_private_key_base64(Some(declared_secret("Y2xpZW50LWtleQ==")))
        .expect("outgoing key");
    tls.set_listen_certificate_base64(Some(declared_secret("c2VydmVy")))
        .expect("incoming certificate");
    tls.set_listen_private_key_base64(Some(declared_secret("c2VydmVyLWtleQ==")))
        .expect("incoming key");

    let tls = config.transport().link().tls();
    let credentials = [
        (tls.root_ca_certificate_base64(), "cm9vdHM="),
        (tls.connect_certificate_base64(), "Y2xpZW50"),
        (tls.connect_private_key_base64(), "Y2xpZW50LWtleQ=="),
        (tls.listen_certificate_base64(), "c2VydmVy"),
        (tls.listen_private_key_base64(), "c2VydmVyLWtleQ=="),
    ];
    for (credential, expected) in credentials {
        let actual = serde_json::to_value(credential).expect("native secret serialization");
        assert_eq!(actual.as_str(), Some(expected));
    }
    assert_eq!(config.id(), &None);
    assert_eq!(config.mode(), &Some(WhatAmI::Peer));
    assert_eq!(
        config.queries_max_retained().map(NonZeroUsize::get),
        Some(7)
    );
    let outgoing = config.connect().endpoints().get(WhatAmI::Peer).unwrap();
    let [EndPoints::Single(outgoing)] = outgoing.as_slice() else {
        panic!("the actual outgoing all-of endpoint");
    };
    let incoming = config.listen().endpoints().get(WhatAmI::Peer).unwrap();
    assert_eq!(outgoing.address().as_ref(), "router.example.test:7447");
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].address().as_ref(), "[::1]:7448");
    for endpoint in [outgoing, &incoming[0]] {
        assert_eq!(endpoint.protocol().as_ref(), "tls");
        assert!(endpoint.metadata().is_empty());
        assert!(endpoint.config().is_empty());
    }
    assert!(
        matches!(config.transport().link().protocols().as_deref(), Some([protocol]) if protocol == "tls")
    );
    assert_eq!(tls.enable_mtls(), &Some(true));
    assert_eq!(tls.verify_name_on_connect(), &Some(true));
    assert_eq!(tls.use_public_pki(), &Some(false));
    assert_eq!(config.scouting().multicast().enabled(), &Some(false));
    assert_eq!(
        config
            .scouting()
            .multicast()
            .listen()
            .as_ref()
            .and_then(|setting| setting.get(WhatAmI::Peer)),
        Some(&false)
    );
    assert_eq!(config.scouting().gossip().enabled(), &Some(false));
}

#[test]
fn native_endpoint_construction_distinguishes_plain_addresses_from_injected_options() {
    for address in ["router.example.test:7447", "[::1]:7448"] {
        let endpoint = EndPoint::new("tls", address, "", "").expect("native address grammar");
        assert_eq!(endpoint.protocol().as_ref(), "tls");
        assert_eq!(endpoint.address().as_ref(), address);
        assert!(endpoint.metadata().is_empty());
        assert!(endpoint.config().is_empty());
    }
    let metadata = EndPoint::new("tls", "router.example.test:7447?priority=1", "", "")
        .expect("native endpoint accepts metadata in address input");
    let options = EndPoint::new(
        "tls",
        "router.example.test:7447#use_public_pki=true",
        "",
        "",
    )
    .expect("native endpoint accepts options in address input");
    assert_eq!(metadata.address().as_ref(), "router.example.test:7447");
    assert!(!metadata.metadata().is_empty());
    assert_eq!(metadata.metadata().get("priority"), Some("1"));
    assert!(metadata.config().is_empty());
    assert_eq!(options.address().as_ref(), "router.example.test:7447");
    assert!(!options.config().is_empty());
    assert_eq!(options.config().get("use_public_pki"), Some("true"));
    assert!(options.metadata().is_empty());
}

#[tokio::test]
async fn configured_runtime_id_returns_original_application_and_configuration() {
    let application = Application::new(Counter::new().stop_on_shutdown());
    let mut config = Config::default();
    let configured_id = "123456789abcdef0123456789abcdef0"
        .parse()
        .expect("existing SDK full ID");
    let previous_id = config.set_id(Some(configured_id));
    assert_eq!(previous_id, Ok(None));
    let refused = application.open_node(config).await;
    let Err((application, config, cause)) = refused else {
        panic!("a caller-specified runtime ID must be refused before native open");
    };
    assert_eq!(config.id(), &Some(configured_id));
    let _retained_application = application;
    assert!(matches!(cause, NodeStartupRefusal::ConfiguredRuntimeId));
}

const BOTH_DIRECTIONS: &str = r#"{"mode":"peer","queries_max_retained":2,"connect":{"endpoints":["tls/localhost:7447"]},"listen":{"endpoints":["tls/localhost:7448"]},"scouting":{"multicast":{"enabled":false,"listen":false},"gossip":{"enabled":false}},"transport":{"link":{"protocols":["tls"],"tls":{"root_ca_certificate":"root.pem","listen_certificate":"listener.pem","listen_private_key":"listener-key.pem","connect_certificate":"connector.pem","connect_private_key":"connector-key.pem","enable_mtls":true,"verify_name_on_connect":true,"use_public_pki":false}}}}"#;

fn configured_fixture() -> Config {
    let mut original = serde_json::Deserializer::from_str(BOTH_DIRECTIONS);
    Config::from_deserializer(&mut original)
        .expect("native validated declaration; files remain unopened")
}

async fn check_refusal(config: Config, field: &'static str, expected: NodeStartupRefusal) {
    let original = config
        .get_json(field)
        .expect("actual supplied field before transfer");
    let tls = config.transport().link().tls();
    let allocations = [
        tls.root_ca_certificate(),
        tls.connect_certificate(),
        tls.connect_private_key(),
        tls.listen_certificate(),
        tls.listen_private_key(),
    ]
    .map(|value| value.as_deref().map(str::as_ptr));
    let application = Application::new(Counter::new().stop_on_shutdown());
    let refused = application.open_node(config).await;
    let Err((application, config, cause)) = refused else {
        panic!("the declared adverse profile must refuse before native open");
    };
    let retained = config
        .get_json(field)
        .expect("same original field after refusal");
    let tls = config.transport().link().tls();
    let retained_allocations = [
        tls.root_ca_certificate(),
        tls.connect_certificate(),
        tls.connect_private_key(),
        tls.listen_certificate(),
        tls.listen_private_key(),
    ]
    .map(|value| value.as_deref().map(str::as_ptr));
    let retained_application = application;
    assert_eq!(retained, original);
    assert_eq!(retained_allocations, allocations);
    match (cause, expected) {
        (
            NodeStartupRefusal::Unsupported { field: actual },
            NodeStartupRefusal::Unsupported { field: expected },
        )
        | (
            NodeStartupRefusal::CredentialRequired { field: actual },
            NodeStartupRefusal::CredentialRequired { field: expected },
        )
        | (
            NodeStartupRefusal::CredentialConflict { field: actual },
            NodeStartupRefusal::CredentialConflict { field: expected },
        ) => assert_eq!(actual, expected),
        (NodeStartupRefusal::QueryLimitRequired, NodeStartupRefusal::QueryLimitRequired)
        | (NodeStartupRefusal::QueryLimitExcessive, NodeStartupRefusal::QueryLimitExcessive) => {}
        (
            NodeStartupRefusal::HostFlavor { flavor: actual },
            NodeStartupRefusal::HostFlavor { flavor: expected },
        ) => assert_eq!(actual, expected),
        (actual, _) => panic!("unexpected startup classification: {actual:?}"),
    }
    drop(retained_application);
}

#[tokio::test(flavor = "current_thread")]
async fn weakened_tls_discovery_and_target_options_refuse_with_original_configuration() {
    for (field, value, expected_field) in [
        ("mode", "null", "mode"),
        (
            "transport/link/protocols",
            "null",
            "transport/link/protocols",
        ),
        (
            "transport/link/protocols",
            "[\"tls\",\"tcp\"]",
            "transport/link/protocols",
        ),
        ("transport/link/tls/enable_mtls", "false", "enable_mtls"),
        ("transport/link/tls/enable_mtls", "null", "enable_mtls"),
        (
            "transport/link/tls/verify_name_on_connect",
            "false",
            "verify_name_on_connect",
        ),
        (
            "transport/link/tls/verify_name_on_connect",
            "null",
            "verify_name_on_connect",
        ),
        (
            "transport/link/tls/use_public_pki",
            "true",
            "use_public_pki",
        ),
        (
            "transport/link/tls/use_public_pki",
            "null",
            "use_public_pki",
        ),
        ("scouting/multicast/enabled", "true", "scouting"),
        ("scouting/multicast/listen", "true", "scouting"),
        ("scouting/gossip/enabled", "true", "scouting"),
        (
            "connect/endpoints",
            "[\"tcp/localhost:7447\"]",
            "connect/endpoints",
        ),
        (
            "listen/endpoints",
            "[\"tcp/localhost:7448\"]",
            "listen/endpoints",
        ),
        (
            "connect/endpoints",
            "[\"tls/localhost:7447#use_public_pki=true\"]",
            "connect/endpoints",
        ),
        (
            "listen/endpoints",
            "[\"tls/localhost:7448#enable_mtls=false\"]",
            "listen/endpoints",
        ),
        (
            "connect/endpoints",
            "[{\"strategy\":\"oneOf\",\"locators\":[\"tls/localhost:7447\"]}]",
            "connect/endpoints/strategy",
        ),
    ] {
        let mut config = configured_fixture();
        let changed = config.insert_json5(field, value);
        changed.expect("actual native supported field update");
        check_refusal(
            config,
            field,
            NodeStartupRefusal::Unsupported {
                field: expected_field,
            },
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn credential_and_capacity_refusals_preserve_the_actual_supplied_values() {
    for (field, expected_field) in [
        (
            "transport/link/tls/root_ca_certificate",
            "root_ca_certificate",
        ),
        (
            "transport/link/tls/connect_certificate",
            "connect_certificate",
        ),
        (
            "transport/link/tls/connect_private_key",
            "connect_private_key",
        ),
        (
            "transport/link/tls/listen_certificate",
            "listen_certificate",
        ),
        (
            "transport/link/tls/listen_private_key",
            "listen_private_key",
        ),
    ] {
        let mut missing = configured_fixture();
        let changed = missing.insert_json5(field, "null");
        changed.expect("actual native missing declaration");
        check_refusal(
            missing,
            field,
            NodeStartupRefusal::CredentialRequired {
                field: expected_field,
            },
        )
        .await;
        let mut conflicting = configured_fixture();
        let base64 = format!("{field}_base64");
        let changed = conflicting.insert_json5(&base64, "\"AA==\"");
        changed.expect("actual simultaneous native credential declarations");
        check_refusal(
            conflicting,
            field,
            NodeStartupRefusal::CredentialConflict {
                field: expected_field,
            },
        )
        .await;
    }
    let mut omitted = configured_fixture();
    let changed = omitted.set_queries_max_retained(None);
    changed.expect("native omission retains legacy compatibility outside Node");
    check_refusal(
        omitted,
        "queries_max_retained",
        NodeStartupRefusal::QueryLimitRequired,
    )
    .await;
    let oversized = Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("native finite boundary");
    let declaration = BOTH_DIRECTIONS.replace(
        "\"queries_max_retained\":2",
        &format!("\"queries_max_retained\":{oversized}"),
    );
    // Direct public Deserialize bypasses the owning setter predicate; Node must still reject it.
    let original: Config =
        serde_json::from_str(&declaration).expect("native nonzero scalar representation");
    check_refusal(
        original,
        "queries_max_retained",
        NodeStartupRefusal::QueryLimitExcessive,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn direction_specific_profiles_pass_to_the_actual_host_guard() {
    for (field, credential_fields) in [
        (
            "connect/endpoints",
            [
                "transport/link/tls/connect_certificate",
                "transport/link/tls/connect_private_key",
            ],
        ),
        (
            "listen/endpoints",
            [
                "transport/link/tls/listen_certificate",
                "transport/link/tls/listen_private_key",
            ],
        ),
    ] {
        let mut config = configured_fixture();
        let cleared = config.insert_json5(field, "[]");
        cleared.expect("actual direction omitted");
        for credential in credential_fields {
            let cleared = config.insert_json5(credential, "null");
            cleared.expect("unused native credential declaration omitted");
        }
        check_refusal(
            config,
            field,
            NodeStartupRefusal::HostFlavor {
                flavor: RuntimeFlavor::CurrentThread,
            },
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn empty_allof_group_does_not_require_credentials_for_nonexistent_outgoing_endpoints() {
    let mut config = configured_fixture();
    for (field, value) in [
        (
            "connect/endpoints",
            "[{\"strategy\":\"allOf\",\"locators\":[]}]",
        ),
        ("transport/link/tls/connect_certificate", "null"),
        ("transport/link/tls/connect_private_key", "null"),
    ] {
        let changed = config.insert_json5(field, value);
        changed.expect("actual empty native connect group");
    }
    check_refusal(
        config,
        "connect/endpoints",
        NodeStartupRefusal::HostFlavor {
            flavor: RuntimeFlavor::CurrentThread,
        },
    )
    .await;
}

#[test]
fn startup_diagnostics_keep_native_cause_out_of_ordinary_output() {
    let secret = "private-node-configuration";
    let native = Box::new(io::Error::other(secret));
    let original_allocation = ptr::from_ref(native.as_ref());
    let cause = NodeStartupRefusal::Open { cause: native };
    let displayed = cause.to_string();
    let debugged = format!("{cause:?}");
    let source = cause.source().expect("actual original native cause");
    let native = source
        .downcast_ref::<io::Error>()
        .expect("the original concrete native error");
    let retained_allocation = ptr::from_ref(native);
    assert_eq!(retained_allocation, original_allocation);
    assert_eq!(displayed, "native Node startup failed");
    assert!(!displayed.contains(secret));
    assert!(!debugged.contains(secret));
    assert_eq!(source.to_string(), secret);
}
