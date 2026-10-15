//! Borrowed checks for the protected JSON object profile.

use core::fmt;
use json_event_parser::{JsonEvent, JsonSyntaxError, LowLevelJsonParser};
use std::error::Error as StdError;
use std::num::NonZeroUsize;

/// Explicit maxima for one protected message and its separate proof.
///
/// These inputs select acceptance policy; no default values are supplied.
/// Parser states and logical nesting are different quantities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum original protected bytes, checked before parsing.
    pub input_bytes: NonZeroUsize,
    /// Maximum original proof bytes, checked before parsing.
    pub proof_bytes: NonZeroUsize,
    /// Maximum bytes in a decoded key, string or number token.
    ///
    /// The parser may materialize an escaped token before this check.
    pub token_bytes: NonZeroUsize,
    /// Maximum simultaneous JSON objects and arrays, counting the outer object.
    pub nesting: NonZeroUsize,
    /// Maximum native parser states, including its non-container states.
    pub parser_states: NonZeroUsize,
}

/// A local syntax or limit refusal; original buffers remain with their caller.
#[derive(Debug)]
pub enum Error {
    InputBytes,
    ProofBytes,
    OuterObject,
    MetadataObject,
    TokenBytes,
    Nesting,
    NoEvent,
    Syntax(JsonSyntaxError),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputBytes => formatter.write_str("protected JSON exceeds its input byte limit"),
            Self::ProofBytes => formatter.write_str("proof exceeds its byte limit"),
            Self::OuterObject => formatter.write_str("protected JSON must be an object"),
            Self::MetadataObject => formatter.write_str("Bombay metadata must be an object"),
            Self::TokenBytes => formatter.write_str("decoded JSON token exceeds its byte limit"),
            Self::Nesting => formatter.write_str("JSON exceeds its logical nesting limit"),
            Self::NoEvent => {
                formatter.write_str("JSON parser produced no event for the complete input")
            }
            Self::Syntax(cause) => write!(formatter, "JSON syntax was refused: {cause}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Syntax(cause) => Some(cause),
            Self::InputBytes
            | Self::ProofBytes
            | Self::OuterObject
            | Self::MetadataObject
            | Self::TokenBytes
            | Self::Nesting
            | Self::NoEvent => None,
        }
    }
}

/// Check complete protected JSON and proof size without copying either buffer.
///
/// Require one outer object and an object value for each decoded top-level
/// `bombay` key. Other protocol fields may contain arrays. Escaped spellings of
/// the metadata key receive the same check. Parsing must reach the native end
/// event, so malformed or trailing input cannot pass.
///
/// Decode the concrete protocol with Serde after this check. Its required
/// `bombay: Metadata` field owns presence, uniqueness and strict metadata schema;
/// this function does not require that field or decode the protocol. Versions,
/// identity proof, permission, replay and actor admission are separate checks.
/// It does not normalize or re-encode protected text.
///
/// Input and proof limits precede parsing. Token bytes are checked after native
/// token materialization; these acceptance limits do not establish a peak-heap
/// bound or cover allocation by the caller's later decoder or provider.
/// Native parser-state exhaustion retains its original syntax error.
///
/// # Errors
/// Returns the exact size, object or nesting refusal, or the actual native
/// syntax error. An absent native event is retained as a distinct refusal.
pub fn check(protected: &[u8], proof: &[u8], limits: &Limits) -> Result<(), Error> {
    if protected.len() > limits.input_bytes.get() {
        return Err(Error::InputBytes);
    }
    if proof.len() > limits.proof_bytes.get() {
        return Err(Error::ProofBytes);
    }
    // The selected parser checks > max before pushing its next state.
    let mut parser = LowLevelJsonParser::new().with_max_stack_size(limits.parser_states.get() - 1);
    let mut remaining = protected;
    let first = next_event(&mut parser, &mut remaining, limits.token_bytes)?;
    if !matches!(first, JsonEvent::StartObject) {
        return Err(Error::OuterObject);
    }
    let mut depth = 1;
    loop {
        let event = next_event(&mut parser, &mut remaining, limits.token_bytes)?;
        match event {
            JsonEvent::ObjectKey(key) if depth == 1 && key == "bombay" => {
                let value = next_event(&mut parser, &mut remaining, limits.token_bytes)?;
                if !matches!(value, JsonEvent::StartObject) {
                    return Err(Error::MetadataObject);
                }
                depth += 1;
            }
            JsonEvent::StartObject | JsonEvent::StartArray => depth += 1,
            JsonEvent::EndObject | JsonEvent::EndArray => depth -= 1,
            JsonEvent::Eof => return Ok(()),
            _ => {}
        }
        if depth > limits.nesting.get() {
            return Err(Error::Nesting);
        }
    }
}

fn next_event<'input>(
    parser: &mut LowLevelJsonParser,
    remaining: &mut &'input [u8],
    token_bytes: NonZeroUsize,
) -> Result<JsonEvent<'input>, Error> {
    let original = *remaining;
    let next = parser.parse_next(original, true);
    *remaining = &original[next.consumed_bytes..];
    let event = next.event.ok_or(Error::NoEvent)?.map_err(Error::Syntax)?;
    match &event {
        JsonEvent::String(token) | JsonEvent::ObjectKey(token) | JsonEvent::Number(token)
            if token.len() > token_bytes.get() =>
        {
            Err(Error::TokenBytes)
        }
        _ => Ok(event),
    }
}
