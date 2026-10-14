#![cfg(feature = "remote")]

use bombay::remote::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::error::Category;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RequestKind {
    Request,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Increment,
    Read,
    Stop,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LedgerRead {
    Read,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CounterRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: CounterOperation,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LedgerReadRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: LedgerRead,
}

const COUNTER_RECORDED: &str = r#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"kind":"request","operation":"read"}"#;
const LEDGER_RECORDED: &str = r#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":31,"authority_generation":19,"request_sequence":29},"kind":"request","operation":"read"}"#;
const COUNTER_SCHEDULED: &str = r#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-scheduled-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"kind":"request","operation":"read"}"#;
const LEDGER_SCHEDULED: &str = r#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-scheduled-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":31,"authority_generation":19,"request_sequence":29},"kind":"request","operation":"read"}"#;

#[test]
fn counter_and_ledger_compose_the_same_protected_metadata() {
    for (counter_bytes, ledger_bytes, profile) in [
        (COUNTER_RECORDED, LEDGER_RECORDED, "counter-record-test-v1"),
        (
            COUNTER_SCHEDULED,
            LEDGER_SCHEDULED,
            "counter-scheduled-test-v1",
        ),
    ] {
        let counter: CounterRequest = serde_json::from_str(counter_bytes).expect("Counter vector");
        let ledger: LedgerReadRequest = serde_json::from_str(ledger_bytes).expect("Ledger vector");
        let expected = Metadata {
            wire_version: 1,
            schema_version: 1,
            provider_profile: profile.into(),
            deployment: "comparison".into(),
            caller: "caller".into(),
            caller_session: "fedcba9876543210fedcba9876543210".into(),
            controller: "owner".into(),
            actor: "counter".into(),
            host: "host".into(),
            protocol: "counter".into(),
            grant_issuer: "issuer".into(),
            runtime_generation: "123456789abcdef0123456789abcdef0".into(),
            actor_generation: 17,
            authority_generation: 19,
            request_sequence: 23,
        };
        assert_eq!(counter.bombay, expected);
        let ledger_expected = Metadata {
            actor: "ledger".into(),
            protocol: "ledger".into(),
            actor_generation: 31,
            request_sequence: 29,
            ..expected
        };
        assert_eq!(ledger.bombay, ledger_expected);
        assert_eq!(counter.kind, RequestKind::Request);
        assert_eq!(counter.operation, CounterOperation::Read);
        assert_eq!(ledger.kind, RequestKind::Request);
        assert_eq!(ledger.operation, LedgerRead::Read);
        let counter_encoded = serde_json::to_vec(&counter).expect("Counter encoding");
        let ledger_encoded = serde_json::to_vec(&ledger).expect("Ledger encoding");
        assert_eq!(counter_encoded, counter_bytes.as_bytes());
        assert_eq!(ledger_encoded, ledger_bytes.as_bytes());
    }
}

#[test]
fn unknown_and_duplicate_metadata_are_refused_with_original_input_retained() {
    for (declaration, cause) in [
        ("\"unsupported\":1,", "unknown field `unsupported`"),
        ("\"wire_version\":1,", "duplicate field `wire_version`"),
    ] {
        let original = COUNTER_RECORDED
            .replacen("\"bombay\":{", &format!("\"bombay\":{{{declaration}"), 1)
            .into_bytes()
            .into_boxed_slice();
        let allocation = original.as_ptr();
        let failure = serde_json::from_slice::<CounterRequest>(&original)
            .expect_err("strict metadata refuses the declaration");
        assert_eq!(failure.classify(), Category::Data);
        assert!(failure.to_string().contains(cause));
        assert_eq!(original.as_ptr(), allocation);
        assert!(
            original
                .windows(declaration.len())
                .any(|bytes| bytes == declaration.as_bytes())
        );
    }
}

#[test]
fn every_metadata_declaration_is_required() {
    for field in [
        "wire_version",
        "schema_version",
        "provider_profile",
        "deployment",
        "caller",
        "caller_session",
        "controller",
        "actor",
        "host",
        "protocol",
        "grant_issuer",
        "runtime_generation",
        "actor_generation",
        "authority_generation",
        "request_sequence",
    ] {
        let mut request: serde_json::Value =
            serde_json::from_str(COUNTER_RECORDED).expect("independent fixed vector");
        let removed = request["bombay"]
            .as_object_mut()
            .expect("metadata object")
            .remove(field);
        assert!(removed.is_some());
        let original = serde_json::to_vec(&request).expect("one omitted field");
        let failure = serde_json::from_slice::<CounterRequest>(&original)
            .expect_err("all declarations required");
        assert_eq!(failure.classify(), Category::Data);
        assert!(
            failure
                .to_string()
                .contains(&format!("missing field `{field}`"))
        );
    }
}

#[test]
fn integer_claims_reject_fractions_negative_values_and_overflow() {
    for field in [
        "wire_version",
        "schema_version",
        "actor_generation",
        "authority_generation",
        "request_sequence",
    ] {
        let value = match field {
            "wire_version" | "schema_version" => "1",
            "actor_generation" => "17",
            "authority_generation" => "19",
            "request_sequence" => "23",
            _ => unreachable!("declared numeric field inventory"),
        };
        for invalid in ["1.5", "-1", "18446744073709551616"] {
            let original = COUNTER_RECORDED.replacen(
                &format!("\"{field}\":{value}"),
                &format!("\"{field}\":{invalid}"),
                1,
            );
            let failure = serde_json::from_str::<CounterRequest>(&original)
                .expect_err("unsigned complete integer claim");
            assert_eq!(failure.classify(), Category::Data);
        }
    }
}

#[test]
fn raw_metadata_does_not_decide_versions_or_protocol_permissions() {
    let original = COUNTER_RECORDED.replacen("\"wire_version\":1", "\"wire_version\":2", 1);
    let parsed: CounterRequest =
        serde_json::from_str(&original).expect("well-formed untrusted claims");
    assert_eq!(parsed.bombay.wire_version, 2);
    let unknown_operation =
        COUNTER_RECORDED.replacen("\"operation\":\"read\"", "\"operation\":\"append\"", 1);
    let failure = serde_json::from_str::<CounterRequest>(&unknown_operation)
        .expect_err("concrete Counter owns operations");
    assert_eq!(failure.classify(), Category::Data);
    assert!(failure.to_string().contains("unknown variant `append`"));
}

#[test]
fn integer_claims_preserve_values_above_float_precision_and_the_u64_boundary() {
    for value in [9_007_199_254_740_993_u64, u64::MAX] {
        let original = COUNTER_RECORDED.replacen(
            "\"request_sequence\":23",
            &format!("\"request_sequence\":{value}"),
            1,
        );
        let parsed: CounterRequest =
            serde_json::from_str(&original).expect("complete integer representation");
        assert_eq!(parsed.bombay.request_sequence, value);
        let encoded = serde_json::to_string(&parsed).expect("preserved integer encoding");
        assert_eq!(encoded, original);
    }
}

#[test]
fn raw_serde_products_do_not_choose_the_shipping_json_object_shape() {
    let original = r#"{"bombay":[1,1,"counter-record-test-v1","comparison","caller","fedcba9876543210fedcba9876543210","owner","counter","host","counter","issuer","123456789abcdef0123456789abcdef0",17,19,23],"kind":"request","operation":"read"}"#;
    let parsed: CounterRequest =
        serde_json::from_str(original).expect("Serde supports positional struct input");
    let object: CounterRequest =
        serde_json::from_str(COUNTER_RECORDED).expect("independent selected object vector");
    assert_eq!(parsed, object);
    assert_ne!(original, COUNTER_RECORDED);
}
