#![cfg(feature = "remote")]

#[path = "../../../examples/counter/src/counter.rs"]
#[expect(
    dead_code,
    reason = "startup refusals retain the real Counter before its messages execute"
)]
mod counter;

use bombay::Application;
use bombay::actors::ActorExt;
use bombay::remote::NodeStartupRefusal;
use counter::Counter;
use std::error::Error;
use std::io;
use std::ptr;
use tokio::{runtime::RuntimeFlavor, sync::Semaphore};
use zenoh_config::Config;

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
