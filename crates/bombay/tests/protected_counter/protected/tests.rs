use super::{DecodeError, Limits, Message, decode, encode};
use crate::CounterRequest;
use dhat::{HeapStats, Profiler};
use serde_json::error::Category;
use std::env;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::process::Command;
use std::ptr;

fn limits() -> Limits {
    Limits {
        input_bytes: NonZeroUsize::new(8192).unwrap(),
        token_bytes: NonZeroUsize::new(128).unwrap(),
        nesting: NonZeroUsize::new(8).unwrap(),
        parser_states: NonZeroUsize::new(16).unwrap(),
        identifier_bytes: NonZeroUsize::new(64).unwrap(),
        proof_bytes: NonZeroUsize::new(16).unwrap(),
    }
}

fn original(kind: &str, operation: &str, extra: &str) -> CounterRequest {
    CounterRequest {
        protected: format!(
            "{{\"wire_version\":1,\"schema_version\":1,\"provider_profile\":\"counter-record-test-v1\",\"kind\":\"{kind}\",\"deployment\":\"deployment\",\"caller\":\"caller\",\"controller\":\"controller\",\"actor\":\"counter\",\"host\":\"host\",\"protocol\":\"counter\",\"grant_issuer\":\"issuer\",\"runtime_generation\":3,\"actor_generation\":5,\"authority_generation\":7,\"request_sequence\":18446744073709551615,\"operation\":\"{operation}\"{extra}}}"
        ).into_bytes().into_boxed_slice(),
        proof: b"original-proof".to_vec().into_boxed_slice(),
    }
}

#[test]
fn seven_meanings_keep_originals_and_complete_flat_metadata() {
    for (kind, operation, extra) in [
        ("request", "increment", ""),
        ("refused", "read", ",\"reason\":\"revoked\""),
        ("mailbox_admitted", "increment", ""),
        ("increment_processed", "increment", ""),
        ("read_reply", "read", ",\"value\":18446744073709551615"),
        ("stop_accepted", "stop", ""),
        (
            "retirement_report",
            "stop",
            ",\"retirement\":\"established\",\"failures\":\"failures_found\"",
        ),
    ] {
        let input = original(kind, operation, extra);
        let pointers = (input.protected.as_ptr(), input.proof.as_ptr());
        let protected = input.protected.to_vec();
        let proof = input.proof.to_vec();
        let decoded = decode(&input, &limits()).unwrap();
        let mut destination = [0; 8192];
        let written = encode(&decoded, &mut destination).unwrap();
        assert_eq!(&destination[..written], protected);
        assert_eq!(input.protected.as_ref(), protected);
        assert_eq!(input.proof.as_ref(), proof);
        assert_eq!((input.protected.as_ptr(), input.proof.as_ptr()), pointers);
        match (kind, decoded) {
            ("request", Message::Request(body)) => assert_eq!(body.operation.as_ref(), "increment"),
            ("refused", Message::Refused(body)) => assert_eq!(body.reason.as_ref(), "revoked"),
            ("mailbox_admitted", Message::MailboxAdmitted(body)) => {
                assert_eq!(body.operation.as_ref(), "increment");
            }
            ("increment_processed", Message::IncrementProcessed(body)) => {
                assert_eq!(body.operation.as_ref(), "increment");
            }
            ("read_reply", Message::ReadReply(body)) => assert_eq!(body.value, u64::MAX),
            ("stop_accepted", Message::StopAccepted(body)) => {
                assert_eq!(body.operation.as_ref(), "stop");
            }
            ("retirement_report", Message::RetirementReport(body)) => {
                assert_eq!(body.retirement.as_ref(), "established");
                assert_eq!(body.failures.as_ref(), "failures_found");
            }
            _ => panic!("the protected meaning was substituted"),
        }
    }
}

#[test]
fn scalar_metadata_and_unit_operations_reject_crossed_shapes() {
    let crossed = original("request", "read", ",\"value\":1");
    let body: super::ReadReply = serde_json::from_slice(&crossed.protected).unwrap();
    let refused = body.checked(&limits());
    let Err(DecodeError::Kind(actual)) = refused else {
        panic!("crossed direct body kind was accepted")
    };
    assert_eq!(actual.as_ref(), "request");
    for operation in ["increment", "read", "stop"] {
        let input = original("request", operation, ",\"amount\":1");
        let refused = decode(&input, &limits());
        assert!(
            matches!(refused, Err(DecodeError::Body(_))),
            "extra amount was accepted"
        );
    }
    for (kind, operation, extra, field, spelling) in [
        ("request", "increment", "", "kind", "{\"request\":null}"),
        (
            "request",
            "increment",
            "",
            "operation",
            "{\"increment\":null}",
        ),
        (
            "refused",
            "read",
            ",\"reason\":\"revoked\"",
            "reason",
            "{\"revoked\":null}",
        ),
        (
            "retirement_report",
            "stop",
            ",\"retirement\":\"established\",\"failures\":\"incomplete\"",
            "retirement",
            "{\"established\":null}",
        ),
        (
            "retirement_report",
            "stop",
            ",\"retirement\":\"established\",\"failures\":\"incomplete\"",
            "failures",
            "{\"incomplete\":null}",
        ),
    ] {
        let mut input = original(kind, operation, extra);
        let text = String::from_utf8(input.protected.into_vec()).unwrap();
        let value = match field {
            "kind" => kind,
            "operation" => operation,
            "reason" => "revoked",
            "retirement" => "established",
            "failures" => "incomplete",
            _ => unreachable!(),
        };
        input.protected = text
            .replace(
                &format!("\"{field}\":\"{value}\""),
                &format!("\"{field}\":{spelling}"),
            )
            .into_bytes()
            .into_boxed_slice();
        let refused = decode(&input, &limits());
        assert!(
            refused.is_err(),
            "nonscalar protected metadata was accepted"
        );
    }
    for (kind, operation, extra) in [
        ("mailbox_admitted", "stop", ""),
        ("increment_processed", "read", ""),
        ("read_reply", "increment", ",\"value\":1"),
        ("stop_accepted", "read", ""),
        (
            "retirement_report",
            "increment",
            ",\"retirement\":\"established\",\"failures\":\"incomplete\"",
        ),
    ] {
        let input = original(kind, operation, extra);
        let refused = decode(&input, &limits());
        let Err(DecodeError::Operation(actual)) = refused else {
            panic!("crossed operation was accepted")
        };
        assert_eq!(actual.as_ref(), operation);
    }
}

#[test]
fn compatibility_follows_whole_input_and_required_declarations() {
    let input = original("request", "read", "");
    let text = String::from_utf8(input.protected.into_vec()).unwrap();
    for (changed, expected) in [
        (
            text.replace("\"wire_version\":1", "\"wire_version\":2")
                .replace("\"operation\":\"read\"", "\"future\":1e400"),
            "version",
        ),
        (
            text.replace("\"schema_version\":1", "\"schema_version\":2"),
            "version",
        ),
        (
            text.replace("\"kind\":\"request\"", "\"kind\":\"future\""),
            "kind",
        ),
        (
            text.replace("counter-record-test-v1", "future-profile"),
            "profile",
        ),
    ] {
        let input = CounterRequest {
            protected: changed.into_bytes().into_boxed_slice(),
            proof: Box::from(&b"proof"[..]),
        };
        let refused = decode(&input, &limits());
        match (expected, refused) {
            (
                "version",
                Err(DecodeError::Version {
                    wire_version,
                    schema_version,
                }),
            ) => assert!(wire_version == 2 || schema_version == 2),
            ("kind", Err(DecodeError::Kind(value))) => assert_eq!(value.as_ref(), "future"),
            ("profile", Err(DecodeError::Profile(value))) => {
                assert_eq!(value.as_ref(), "future-profile");
            }
            _ => panic!("unsupported metadata was reinterpreted"),
        }
    }
    for extra in [",\"wire_vers\\u0069on\":1", ",\"request_sequen\\u0063e\":3"] {
        let input = original("request", "read", extra);
        let refused = decode(&input, &limits());
        assert!(
            refused.is_err(),
            "duplicate protected metadata was accepted"
        );
    }
    for ending in ["{}", "false", "x"] {
        let mut input = original("request", "read", "");
        let mut bytes = input.protected.into_vec();
        bytes.extend_from_slice(ending.as_bytes());
        input.protected = bytes.into_boxed_slice();
        let refused = decode(&input, &limits());
        assert!(matches!(refused, Err(DecodeError::Lexical(_))));
    }
    for future in [
        br#"{"wire_version":2,"schema_version":1,"future":"\ud800"}"#.as_slice(),
        b"{\"wire_version\":2,\"schema_version\":1,\"future\":\"\xff\"}",
    ] {
        let input = CounterRequest {
            protected: future.into(),
            proof: Box::from(&b"proof"[..]),
        };
        let refused = decode(&input, &limits());
        assert!(
            matches!(refused, Err(DecodeError::Lexical(_))),
            "malformed future Unicode became incompatible"
        );
    }
}

#[test]
fn explicit_byte_token_nesting_proof_and_identifier_limits_refuse() {
    let input = original("request", "read", "");
    let mut bounds = limits();
    bounds.input_bytes = NonZeroUsize::new(input.protected.len()).unwrap();
    let exact = decode(&input, &bounds);
    let mut larger = original("request", "read", "");
    let mut bytes = larger.protected.into_vec();
    bytes.push(b' ');
    larger.protected = bytes.into_boxed_slice();
    let refused = decode(&larger, &bounds);
    assert!(exact.is_ok());
    assert!(
        matches!(refused, Err(DecodeError::InputBytes)),
        "oversized protected input was accepted"
    );
    for width in [128, 129] {
        let input = original(
            "request",
            "read",
            &format!(",\"future\":\"{}\"", "x".repeat(width)),
        );
        let refused = decode(&input, &limits());
        match width {
            128 => assert!(matches!(refused, Err(DecodeError::Body(_)))),
            129 => assert!(
                matches!(refused, Err(DecodeError::TokenBytes)),
                "oversized protected token was accepted"
            ),
            _ => unreachable!(),
        }
    }
    for depth in [8, 9] {
        let input = CounterRequest {
            protected: format!(
                "{{\"wire_version\":2,\"schema_version\":1,\"future\":{}0{}}}",
                "[".repeat(depth - 1),
                "]".repeat(depth - 1)
            )
            .into_bytes()
            .into_boxed_slice(),
            proof: Box::from(&b"proof"[..]),
        };
        let refused = decode(&input, &limits());
        match depth {
            8 => assert!(matches!(refused, Err(DecodeError::Version { .. }))),
            9 => assert!(
                matches!(refused, Err(DecodeError::Nesting)),
                "excessive protected nesting was accepted"
            ),
            _ => unreachable!(),
        }
    }
    let mut bounds = limits();
    bounds.proof_bytes = NonZeroUsize::new(input.proof.len() - 1).unwrap();
    let refused = decode(&input, &bounds);
    assert!(matches!(refused, Err(DecodeError::ProofBytes)));
    let mut bounds = limits();
    bounds.identifier_bytes = NonZeroUsize::new(5).unwrap();
    let refused = decode(&input, &bounds);
    assert!(matches!(refused, Err(DecodeError::Identifier { .. })));
}

#[test]
fn encoding_only_exposes_complete_prefix_and_preserves_original_intent() {
    let input = original("read_reply", "read", ",\"value\":18446744073709551615");
    let decoded = decode(&input, &limits()).unwrap();
    let address = ptr::from_ref(&decoded);
    let mut exact = vec![0; input.protected.len()];
    let written = encode(&decoded, &mut exact).unwrap();
    assert_eq!(written, exact.len());
    assert_eq!(exact, input.protected.as_ref());
    let mut short = vec![0; written - 1];
    let refused = encode(&decoded, &mut short);
    let cause = refused.unwrap_err();
    assert_eq!(cause.classify(), Category::Io);
    assert_eq!(cause.io_error_kind(), Some(std::io::ErrorKind::WriteZero));
    assert_eq!(ptr::from_ref(&decoded), address);
    let Message::ReadReply(body) = decoded else {
        panic!("read intent was lost")
    };
    assert_eq!(body.value, u64::MAX);
}

#[test]
fn both_profiles_require_each_metadata_field_and_preserve_unicode() {
    let fields = [
        ("wire_version", "1"),
        ("schema_version", "1"),
        ("provider_profile", "\"counter-record-test-v1\""),
        ("kind", "\"request\""),
        ("deployment", "\"deployment\""),
        ("caller", "\"caller\""),
        ("controller", "\"controller\""),
        ("actor", "\"counter\""),
        ("host", "\"host\""),
        ("protocol", "\"counter\""),
        ("grant_issuer", "\"issuer\""),
        ("runtime_generation", "3"),
        ("actor_generation", "5"),
        ("authority_generation", "7"),
        ("request_sequence", "18446744073709551615"),
        ("operation", "\"read\""),
    ];
    for (field, value) in fields {
        let mut input = original("request", "read", "");
        let text = String::from_utf8(input.protected.into_vec()).unwrap();
        input.protected = text
            .replace(&format!("\"{field}\":{value}"), "\"future\":0")
            .into_bytes()
            .into_boxed_slice();
        let refused = decode(&input, &limits());
        assert!(refused.is_err(), "missing required metadata was accepted");
    }
    for profile in ["counter-record-test-v1", "counter-scheduled-test-v1"] {
        let mut input = original("request", "read", "");
        let text = String::from_utf8(input.protected.into_vec()).unwrap();
        input.protected = format!(
            " \t{} \r\n",
            text.replace("counter-record-test-v1", profile)
                .replace("\"actor\":\"counter\"", "\"actor\":\"\\u03c0\\u96ea\"")
        )
        .into_bytes()
        .into_boxed_slice();
        let decoded = decode(&input, &limits()).unwrap();
        let Message::Request(body) = decoded else {
            panic!("unit Read changed meaning")
        };
        assert_eq!(body.actor.as_ref(), "π雪");
        assert_eq!(body.provider_profile.as_ref(), profile);
        assert_eq!(body.request_sequence, u64::MAX);
    }
    for protected in [b"[]".as_slice(), b"null", b"\xef\xbb\xbf{}", b"\x0c{}"] {
        let input = CounterRequest {
            protected: protected.into(),
            proof: Box::from(&b"proof"[..]),
        };
        let refused = decode(&input, &limits());
        assert!(refused.is_err());
    }
}

#[test]
fn refusal_and_retirement_strings_match_only_existing_assessments() {
    for reason in [
        "binding",
        "grant",
        "conflict",
        "retention_full",
        "revoked",
        "expired",
        "count",
        "individual_bytes",
        "total_bytes",
        "accounting",
        "exhausted",
    ] {
        let input = original("refused", "read", &format!(",\"reason\":\"{reason}\""));
        let decoded = decode(&input, &limits());
        assert!(matches!(decoded, Ok(Message::Refused(_))));
    }
    for reason in ["invalid", "stale", "unavailable", "future"] {
        let input = original("refused", "read", &format!(",\"reason\":\"{reason}\""));
        let refused = decode(&input, &limits());
        let Err(DecodeError::Reason(actual)) = refused else {
            panic!("local verification became trusted refusal")
        };
        assert_eq!(actual.as_ref(), reason);
    }
    for retirement in ["established", "not_established"] {
        for failures in ["failures_found", "no_failures_found", "incomplete"] {
            let input = original(
                "retirement_report",
                "stop",
                &format!(",\"retirement\":\"{retirement}\",\"failures\":\"{failures}\""),
            );
            let decoded = decode(&input, &limits());
            assert!(matches!(decoded, Ok(Message::RetirementReport(_))));
        }
    }
    for (retirement, failures) in [("normal", "incomplete"), ("established", "clean")] {
        let input = original(
            "retirement_report",
            "stop",
            &format!(",\"retirement\":\"{retirement}\",\"failures\":\"{failures}\""),
        );
        let refused = decode(&input, &limits());
        match refused {
            Err(DecodeError::Retirement(actual)) => assert_eq!(actual.as_ref(), retirement),
            Err(DecodeError::Failures(actual)) => assert_eq!(actual.as_ref(), failures),
            _ => panic!("unknown runtime assessment was accepted"),
        }
    }
}

#[test]
fn identifier_and_parser_state_caps_retain_causes_and_exact_headers() {
    for width in [64, 65] {
        let mut input = original("request", "read", "");
        let text = String::from_utf8(input.protected.into_vec()).unwrap();
        input.protected = text
            .replace(
                "\"actor\":\"counter\"",
                &format!("\"actor\":\"{}\"", "x".repeat(width)),
            )
            .into_bytes()
            .into_boxed_slice();
        let decoded = decode(&input, &limits());
        match width {
            64 => assert!(decoded.is_ok()),
            65 => {
                let Err(DecodeError::Identifier { field, bytes }) = decoded else {
                    panic!("identifier bound was lost")
                };
                assert_eq!((field, bytes), ("actor", 65));
            }
            _ => unreachable!(),
        }
    }
    let input = CounterRequest {
        protected: br#"{"wire_version":2,"schema_version":1,"future":[[]]}"#
            .as_slice()
            .into(),
        proof: Box::from(&b"proof"[..]),
    };
    let mut bounds = limits();
    bounds.parser_states = NonZeroUsize::new(2).unwrap();
    let at_capacity = CounterRequest {
        protected: br#"{"wire_version":2,"schema_version":1,"future":[]}"#
            .as_slice()
            .into(),
        proof: Box::from(&b"proof"[..]),
    };
    let compatible = decode(&at_capacity, &bounds);
    let refused = decode(&input, &bounds);
    assert!(matches!(compatible, Err(DecodeError::Version { .. })));
    let Err(DecodeError::Lexical(cause)) = refused else {
        panic!("original state-cap cause was lost")
    };
    assert!(cause.to_string().contains("Max stack size"));
    let input = original("read_reply", "read", ",\"value\":18446744073709551615");
    let decoded = decode(&input, &limits()).unwrap();
    let Message::ReadReply(body) = decoded else {
        panic!("read body changed meaning")
    };
    let names = [
        &body.provider_profile,
        &body.kind,
        &body.deployment,
        &body.caller,
        &body.controller,
        &body.actor,
        &body.host,
        &body.protocol,
        &body.grant_issuer,
        &body.operation,
    ];
    let name_bytes = names.iter().map(|name| name.len()).sum::<usize>();
    assert_eq!(name_bytes, 86);
    println!(
        "raw_header={} decoded_sum={} read_header={} name_bytes={} original_json={} original_proof={}",
        size_of::<CounterRequest>(),
        size_of::<Message>(),
        size_of::<super::ReadReply>(),
        name_bytes,
        input.protected.len(),
        input.proof.len()
    );
}

#[test]
fn actual_decoder_requested_allocations_preserve_original_results() {
    match env::var("BOMBAY_PROTECTED_DECODER_PROFILE") {
        Err(env::VarError::NotPresent) => {
            let executable = env::current_exe().unwrap();
            let execution = Command::new(executable)
                .args(["--exact", "protected::tests::actual_decoder_requested_allocations_preserve_original_results", "--nocapture"])
                .env("BOMBAY_PROTECTED_DECODER_PROFILE", "isolated")
                .output().unwrap();
            assert!(
                execution.status.success(),
                "isolated decoder profile failed: {}",
                String::from_utf8_lossy(&execution.stderr)
            );
            let trace = String::from_utf8(execution.stdout).unwrap();
            assert!(trace.contains("protected decoder allocation complete"));
            println!("{trace}");
            return;
        }
        Ok(role) if role == "isolated" => {}
        role => panic!("unsupported decoder profiling role {role:?}"),
    }
    let mut escaped = original("read_reply", "read", ",\"value\":18446744073709551615");
    let text = String::from_utf8(escaped.protected.into_vec()).unwrap();
    escaped.protected = text
        .replace(
            "\"actor\":\"counter\"",
            &format!("\"actor\":\"{}\"", "\\u03c0".repeat(32)),
        )
        .into_bytes()
        .into_boxed_slice();
    for input in [
        original("read_reply", "read", ",\"value\":18446744073709551615"),
        escaped,
    ] {
        let pointers = (input.protected.as_ptr(), input.proof.as_ptr());
        let profiler = Profiler::builder().testing().build();
        let decoded = decode(&input, &limits());
        let allocations = HeapStats::get();
        drop(profiler);
        let Message::ReadReply(body) = decoded.unwrap() else {
            panic!("profiled reply lost its original result")
        };
        assert_eq!(body.value, u64::MAX);
        assert_eq!((input.protected.as_ptr(), input.proof.as_ptr()), pointers);
        println!(
            "json_bytes={} proof_bytes={} actor_bytes={} allocations={allocations:?}",
            input.protected.len(),
            input.proof.len(),
            body.actor.len()
        );
    }
    println!("protected decoder allocation complete");
}
