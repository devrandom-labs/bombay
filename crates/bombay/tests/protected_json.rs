//! Private concrete JSON comparison; this is not a production wire schema.

use std::io::{self, Cursor, ErrorKind};

use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Error, Value, error::Category};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtectedCommand {
    sequence: u64,
    label: Box<str>,
}

fn decode_command(original: &[u8]) -> Result<ProtectedCommand, Error> {
    if !original.trim_ascii_start().starts_with(b"{") {
        return Err(Error::custom("protected command must be an object"));
    }
    serde_json::from_slice(original)
}

fn serialize_command(command: &ProtectedCommand, destination: &mut [u8]) -> Result<usize, Error> {
    let mut cursor = Cursor::new(destination);
    serde_json::to_writer(&mut cursor, command)?;
    Ok(usize::try_from(cursor.position()).expect("position is within the supplied slice"))
}

#[test]
fn original_objects_preserve_unicode_integer_and_known_fields() {
    let original = b" \t{\"sequence\":18446744073709551615,\"label\":\"\\u03c0\\u96ea\"} \r\n";
    let decoded = decode_command(original).expect("complete object");
    assert_eq!(decoded.sequence, u64::MAX);
    assert_eq!(&*decoded.label, "π雪");

    let reordered = b"\t\r\n {\"label\":\"read\",\"sequence\":0} \n\t";
    let decoded = decode_command(reordered).expect("reordered object with JSON whitespace");
    assert_eq!(decoded.sequence, 0);
    assert_eq!(&*decoded.label, "read");
}

#[test]
fn positional_commands_refuse_while_plain_derive_accepts_them() {
    let original = br#"[7,"read"]"#;
    let comparison = serde_json::from_slice::<ProtectedCommand>(original)
        .expect("derive accepts the positional representation");
    assert_eq!(comparison.sequence, 7);
    assert_eq!(&*comparison.label, "read");
    let rejected = decode_command(original);
    assert!(
        rejected.is_err(),
        "positional protected command was accepted"
    );

    for original in [b"null".as_slice(), b"7", b"\"read\"", b""] {
        let rejected = decode_command(original);
        assert!(
            rejected.is_err(),
            "nonobject protected command was accepted"
        );
    }
}

#[test]
fn duplicate_metadata_refuses_before_value_can_erase_it() {
    for original in [
        br#"{"sequence":7,"sequence":8,"label":"read"}"#.as_slice(),
        br#"{"sequence":7,"sequen\u0063e":8,"label":"read"}"#,
        br#"{"sequence":7,"label":"read","label":"write"}"#,
    ] {
        let rejected = decode_command(original);
        let cause = rejected.expect_err("duplicate protected metadata was accepted");
        assert_eq!(cause.classify(), Category::Data);
        assert!(cause.to_string().contains("duplicate field"));
    }
    let original = br#"{"sequence":7,"sequence":8,"label":"read"}"#;
    let intermediate: Value =
        serde_json::from_slice(original).expect("Value accepts duplicate keys");
    let comparison: ProtectedCommand = serde_json::from_value(intermediate)
        .expect("the intermediate has erased the earlier sequence");
    assert_eq!(comparison.sequence, 8);
    assert_eq!(&*comparison.label, "read");
}

#[test]
fn complete_original_input_rejects_trailing_and_non_json_whitespace() {
    for original in [
        br#"{"sequence":7,"label":"read"}{}"#.as_slice(),
        br#"{"sequence":7,"label":"read"}false"#,
        br#"{"sequence":7,"label":"read"}x"#,
    ] {
        let rejected = decode_command(original);
        assert!(rejected.is_err(), "trailing protected input was accepted");
        let mut deserializer = Deserializer::from_slice(original);
        let comparison = ProtectedCommand::deserialize(&mut deserializer)
            .expect("decoding one value alone does not require end of input");
        assert_eq!(comparison.sequence, 7);
        assert_eq!(&*comparison.label, "read");
        let trailing = deserializer.end();
        assert!(trailing.is_err());
    }
    let original = b"\x0c{\"sequence\":7,\"label\":\"read\"}";
    assert!(original.trim_ascii_start().starts_with(b"{"));
    let rejected = decode_command(original);
    assert!(
        rejected.is_err(),
        "form-feed was normalized into valid JSON"
    );
}

#[test]
fn unsupported_metadata_and_integer_representations_refuse() {
    for original in [
        br#"{"sequence":7,"label":"read","extra":0}"#.as_slice(),
        br#"{"label":"read"}"#,
        br#"{"sequence":18446744073709551616,"label":"read"}"#,
        br#"{"sequence":-1,"label":"read"}"#,
        br#"{"sequence":1.0,"label":"read"}"#,
        br#"{"sequence":"7","label":"read"}"#,
    ] {
        let rejected = decode_command(original);
        assert!(
            rejected.is_err(),
            "unsupported protected metadata was accepted"
        );
    }
}

#[test]
fn bounded_writes_keep_original_and_refuse_partial_output() {
    let command = ProtectedCommand {
        sequence: u64::MAX,
        label: Box::from("π雪\"\\\n"),
    };
    let original_allocation = command.label.as_ptr();
    let expected = r#"{"sequence":18446744073709551615,"label":"π雪\"\\\n"}"#.as_bytes();
    let mut exact = vec![0; expected.len()];
    let written = serialize_command(&command, &mut exact).expect("exact-fit destination");
    assert_eq!(written, expected.len());
    assert_eq!(exact.as_slice(), expected);
    let decoded = decode_command(&exact[..written]).expect("complete successful prefix");
    assert_eq!(decoded.sequence, command.sequence);
    assert_eq!(decoded.label, command.label);

    let mut larger = vec![0xa5; expected.len() + 3];
    let written = serialize_command(&command, &mut larger).expect("larger fixed destination");
    assert_eq!(&larger[..written], expected);
    assert_eq!(&larger[written..], &[0xa5; 3]);

    let mut short = vec![0; expected.len() - 1];
    let refused = serialize_command(&command, &mut short);
    let cause = refused.expect_err("partial JSON became an eligible successful prefix");
    assert_eq!(cause.classify(), Category::Io);
    assert_eq!(cause.io_error_kind(), Some(ErrorKind::WriteZero));
    assert_eq!(short.as_slice(), &expected[..expected.len() - 1]);
    let partial = decode_command(&short);
    assert!(partial.is_err());
    let native_cause: io::Error = cause.into();
    assert_eq!(native_cause.kind(), ErrorKind::WriteZero);
    assert_eq!(command.label.as_ptr(), original_allocation);
    assert_eq!(&*command.label, "π雪\"\\\n");
    assert_eq!(command.sequence, u64::MAX);
}
