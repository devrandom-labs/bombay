#![cfg(feature = "remote")]

use bombay::remote::{
    Metadata,
    json::{self, Error, Limits},
};
use serde::Deserialize;
use serde_json::error::Category;
use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::ptr;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RequestKind {
    Request,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CounterOperation {
    Increment,
    Read,
    Stop,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LedgerRead {
    Read,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CounterRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: CounterOperation,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LedgerReadRequest {
    bombay: Metadata,
    kind: RequestKind,
    operation: LedgerRead,
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplyKind {
    ReadReply,
}
// Separate syntax-only reply: origins are raw runtime-scoped claims, NOT capabilities.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LedgerReadReply {
    bombay: Metadata,
    kind: ReplyKind,
    operation: LedgerRead,
    origins: Vec<u64>,
}

const COUNTER_OBJECT: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"kind":"request","operation":"read"}"#;
const LEDGER_OBJECT: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":31,"authority_generation":19,"request_sequence":29},"kind":"request","operation":"read"}"#;
const OUTER_ARRAY: &[u8] = br#"[{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"request","read"]"#;
const METADATA_ARRAY: &[u8] = br#"{"bombay":[1,1,"counter-record-test-v1","comparison","caller","fedcba9876543210fedcba9876543210","owner","counter","host","counter","issuer","123456789abcdef0123456789abcdef0",17,19,23],"kind":"request","operation":"read"}"#;
const ESCAPED_METADATA_ARRAY: &[u8] = br#"{"bom\u0062ay":[1,1,"counter-record-test-v1","comparison","caller","fedcba9876543210fedcba9876543210","owner","counter","host","counter","issuer","123456789abcdef0123456789abcdef0",17,19,23],"kind":"request","operation":"read"}"#;
const ESCAPED_KEY: &[u8] = br#"{"bom\u0062ay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"counter","host":"host","protocol":"counter","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":17,"authority_generation":19,"request_sequence":23},"kind":"request","operation":"read"}"#;
const LEDGER_REPLY: &[u8] = br#"{"bombay":{"wire_version":1,"schema_version":1,"provider_profile":"counter-record-test-v1","deployment":"comparison","caller":"caller","caller_session":"fedcba9876543210fedcba9876543210","controller":"owner","actor":"ledger","host":"host","protocol":"ledger","grant_issuer":"issuer","runtime_generation":"123456789abcdef0123456789abcdef0","actor_generation":31,"authority_generation":19,"request_sequence":29},"kind":"read_reply","operation":"read","origins":[17,23]}"#;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "compare the independent shape vectors, native causes and original buffers together"
)]
fn fixed_shape_vectors_preserve_originals_and_protocol_arrays() {
    // Explicit test inputs only; no production default or peak-allocation claim.
    let limits = Limits {
        input_bytes: NonZeroUsize::new(2048).unwrap(),
        proof_bytes: NonZeroUsize::new(2).unwrap(),
        token_bytes: NonZeroUsize::new(64).unwrap(),
        nesting: NonZeroUsize::new(4).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    };
    let protected = COUNTER_OBJECT.to_vec().into_boxed_slice();
    let proof = vec![9_u8, 255].into_boxed_slice();
    let protected_allocation = protected.as_ptr();
    let proof_allocation = proof.as_ptr();
    let counter_shape = json::check(&protected, &proof, &limits);
    counter_shape.expect("object Counter shape");
    let counter: CounterRequest =
        serde_json::from_slice(&protected).expect("object Counter schema");
    let ledger_shape = json::check(LEDGER_OBJECT, &proof, &limits);
    ledger_shape.expect("object Ledger shape");
    let ledger: LedgerReadRequest =
        serde_json::from_slice(LEDGER_OBJECT).expect("object Ledger schema");
    let reply_shape = json::check(LEDGER_REPLY, &proof, &limits);
    reply_shape.expect("payload array shape");
    let reply: LedgerReadReply =
        serde_json::from_slice(LEDGER_REPLY).expect("payload array schema");
    let escaped_shape = json::check(ESCAPED_KEY, &proof, &limits);
    escaped_shape.expect("decoded key shape");
    let escaped: CounterRequest = serde_json::from_slice(ESCAPED_KEY).expect("decoded key schema");
    let expected_counter = Metadata {
        wire_version: 1,
        schema_version: 1,
        provider_profile: "counter-record-test-v1".into(),
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
    let expected_ledger = Metadata {
        wire_version: 1,
        schema_version: 1,
        provider_profile: "counter-record-test-v1".into(),
        deployment: "comparison".into(),
        caller: "caller".into(),
        caller_session: "fedcba9876543210fedcba9876543210".into(),
        controller: "owner".into(),
        actor: "ledger".into(),
        host: "host".into(),
        protocol: "ledger".into(),
        grant_issuer: "issuer".into(),
        runtime_generation: "123456789abcdef0123456789abcdef0".into(),
        actor_generation: 31,
        authority_generation: 19,
        request_sequence: 29,
    };
    let serde_outer: CounterRequest = serde_json::from_slice(OUTER_ARRAY).expect("Serde sequence");
    let serde_nested: CounterRequest =
        serde_json::from_slice(METADATA_ARRAY).expect("Serde sequence");
    let outer = json::check(OUTER_ARRAY, &proof, &limits);
    let nested = json::check(METADATA_ARRAY, &proof, &limits);
    let escaped_nested = json::check(ESCAPED_METADATA_ARRAY, &proof, &limits);
    let truncated = json::check(b"{", &proof, &limits);
    let trailing = json::check(b"{}{}", &proof, &limits);
    let missing = serde_json::from_slice::<CounterRequest>(b"{}");
    let oversized_input = vec![b' '; 2049];
    let oversized_proof = [9, 255, 0];
    let input_refusal = json::check(&oversized_input, &proof, &limits);
    let proof_refusal = json::check(&protected, &oversized_proof, &limits);
    assert_eq!(counter.bombay, expected_counter);
    assert_eq!(counter.kind, RequestKind::Request);
    assert_eq!(counter.operation, CounterOperation::Read);
    assert_eq!(ledger.bombay, expected_ledger);
    assert_eq!(ledger.kind, RequestKind::Request);
    assert_eq!(ledger.operation, LedgerRead::Read);
    assert_eq!(reply.bombay, ledger.bombay);
    assert_eq!(reply.kind, ReplyKind::ReadReply);
    assert_eq!(reply.operation, LedgerRead::Read);
    assert_eq!(reply.origins, [17, 23]);
    assert_eq!(escaped, counter);
    assert_eq!(serde_outer, counter);
    assert_eq!(serde_nested, counter);
    assert!(
        matches!(&outer, Err(Error::OuterObject)),
        "outer JSON object rule: {outer:?}"
    );
    assert!(
        matches!(&nested, Err(Error::MetadataObject)),
        "metadata JSON object rule: {nested:?}"
    );
    assert!(
        matches!(&escaped_nested, Err(Error::MetadataObject)),
        "escaped metadata JSON object rule: {escaped_nested:?}"
    );
    assert!(
        matches!(&truncated, Err(Error::Syntax(cause)) if cause.message() == "Object keys must be strings")
    );
    assert!(
        matches!(&missing, Err(cause) if cause.classify() == Category::Data && cause.to_string().contains("missing field `bombay`"))
    );
    assert!(matches!(&trailing, Err(Error::Syntax(_))));
    assert!(matches!(&input_refusal, Err(Error::InputBytes)));
    assert!(matches!(&proof_refusal, Err(Error::ProofBytes)));
    assert_eq!(protected.as_ptr(), protected_allocation);
    assert_eq!(proof.as_ptr(), proof_allocation);
    assert_eq!(&*protected, COUNTER_OBJECT);
    assert_eq!(&*proof, &[9, 255]);
}

#[test]
fn input_and_proof_limits_keep_the_original_buffers() {
    let protected = Box::<[u8]>::from(b"{}".as_slice());
    let proof = Box::<[u8]>::from([9, 255]);
    let allocations = (protected.as_ptr(), proof.as_ptr());
    let limits = Limits {
        input_bytes: NonZeroUsize::new(2).unwrap(),
        proof_bytes: NonZeroUsize::new(2).unwrap(),
        token_bytes: NonZeroUsize::new(3).unwrap(),
        nesting: NonZeroUsize::new(1).unwrap(),
        parser_states: NonZeroUsize::new(1).unwrap(),
    };
    let exact = json::check(&protected, &proof, &limits);
    let input = json::check(
        &protected,
        &proof,
        &Limits {
            input_bytes: NonZeroUsize::new(1).unwrap(),
            ..limits
        },
    );
    let evidence = json::check(
        &protected,
        &proof,
        &Limits {
            proof_bytes: NonZeroUsize::new(1).unwrap(),
            ..limits
        },
    );
    assert!(exact.is_ok());
    assert!(
        matches!(&input, Err(Error::InputBytes)),
        "input limit: {input:?}"
    );
    assert!(
        matches!(&evidence, Err(Error::ProofBytes)),
        "proof limit: {evidence:?}"
    );
    assert_eq!((protected.as_ptr(), proof.as_ptr()), allocations);
    assert_eq!(
        (&*protected, &*proof),
        (b"{}".as_slice(), [9, 255].as_slice())
    );
}

#[test]
fn token_limits_count_decoded_key_string_and_number_bytes() {
    let limits = Limits {
        input_bytes: NonZeroUsize::new(128).unwrap(),
        proof_bytes: NonZeroUsize::new(2).unwrap(),
        token_bytes: NonZeroUsize::new(3).unwrap(),
        nesting: NonZeroUsize::new(2).unwrap(),
        parser_states: NonZeroUsize::new(8).unwrap(),
    };
    for protected in [
        br#"{"abc":0}"#.as_slice(),
        br#"{"x":"abc"}"#.as_slice(),
        br#"{"x":123}"#.as_slice(),
        br#"{"x":"\u0061\u0062\u0063"}"#.as_slice(),
    ] {
        let original = protected.to_vec().into_boxed_slice();
        let allocation = original.as_ptr();
        let exact = json::check(&original, &[9, 255], &limits);
        let excess = json::check(
            &original,
            &[9, 255],
            &Limits {
                token_bytes: NonZeroUsize::new(2).unwrap(),
                ..limits
            },
        );
        assert!(exact.is_ok(), "exact decoded token limit: {exact:?}");
        assert!(
            matches!(&excess, Err(Error::TokenBytes)),
            "decoded token limit: {excess:?}"
        );
        assert_eq!(original.as_ptr(), allocation);
        assert_eq!(&*original, protected);
    }
    let unicode = br#"{"x":"\uD83D\uDE00"}"#;
    let four_bytes = json::check(
        unicode,
        &[9, 255],
        &Limits {
            token_bytes: NonZeroUsize::new(4).unwrap(),
            ..limits
        },
    );
    let three_bytes = json::check(unicode, &[9, 255], &limits);
    assert!(four_bytes.is_ok());
    assert!(matches!(&three_bytes, Err(Error::TokenBytes)));
}

#[test]
fn nesting_limits_count_outer_metadata_and_protocol_containers() {
    let limits = Limits {
        input_bytes: NonZeroUsize::new(128).unwrap(),
        proof_bytes: NonZeroUsize::new(2).unwrap(),
        token_bytes: NonZeroUsize::new(8).unwrap(),
        nesting: NonZeroUsize::new(3).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
    };
    for (protected, depth) in [
        (br#"{"bombay":{}}"#.as_slice(), 2),
        (br#"{"x":[{}]}"#.as_slice(), 3),
    ] {
        let exact = json::check(
            protected,
            &[9, 255],
            &Limits {
                nesting: NonZeroUsize::new(depth).unwrap(),
                ..limits
            },
        );
        let excess = json::check(
            protected,
            &[9, 255],
            &Limits {
                nesting: NonZeroUsize::new(depth - 1).unwrap(),
                ..limits
            },
        );
        assert!(exact.is_ok(), "exact logical nesting: {exact:?}");
        assert!(
            matches!(&excess, Err(Error::Nesting)),
            "logical nesting: {excess:?}"
        );
    }
}

#[test]
fn parser_state_limits_preserve_the_actual_native_cause() {
    let limits = Limits {
        input_bytes: NonZeroUsize::new(128).unwrap(),
        proof_bytes: NonZeroUsize::new(2).unwrap(),
        token_bytes: NonZeroUsize::new(8).unwrap(),
        nesting: NonZeroUsize::new(8).unwrap(),
        parser_states: NonZeroUsize::new(2).unwrap(),
    };
    let exact = json::check(br#"{"x":[]}"#, &[9, 255], &limits);
    let excess = json::check(br#"{"x":[[]]}"#, &[9, 255], &limits);
    assert!(exact.is_ok());
    let error = excess.expect_err("native parser state limit must refuse");
    let Error::Syntax(native) = &error else {
        panic!("native parser state cause changed: {error:?}");
    };
    let source = error
        .source()
        .expect("original native error remains chained");
    let original_address = ptr::from_ref(native).addr();
    let retained_address = ptr::from_ref(source).addr();
    assert_eq!(
        native.message(),
        "Max stack size of 1 reached on an object opening"
    );
    assert_eq!(retained_address, original_address);
    assert_eq!(source.to_string(), native.to_string());
}
