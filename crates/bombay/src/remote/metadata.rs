//! Protected remote declarations; deserialization does not establish authority.

use serde::{Deserialize, Serialize};

/// Untrusted declarations shared by concrete protocol-owned protected messages.
///
/// Providers verify the exact original bytes. The receiver independently checks
/// these claims against its configured scope and actual target capability.
/// In object encodings all fields are required; unknown and duplicate fields
/// are rejected. Derived Serde decoding also accepts positional struct input.
///
/// The receiving JSON boundary must enforce the selected object shape.
/// Resource bounds, supported versions, provider profiles, SDK identity parsing,
/// replay and permission checks remain separate receiver policies. This product
/// does not validate them or retain the original protected bytes. Serialize it
/// inside the concrete protocol's `bombay` field; authenticate the original
/// transmitted bytes rather than a decoded and reserialized representation.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// Declared Bombay envelope version; decoding does not accept that version.
    pub wire_version: u64,
    /// Declared version of the concrete protocol schema.
    pub schema_version: u64,
    /// Declared identity-provider proof profile.
    pub provider_profile: Box<str>,
    /// Declared deployment scope.
    pub deployment: Box<str>,
    /// Declared original caller, preserved through forwarding.
    pub caller: Box<str>,
    /// Declared complete caller runtime ID in the selected SDK text form.
    pub caller_session: Box<str>,
    /// Declared controller identity in the configured actor scope.
    pub controller: Box<str>,
    /// Declared stable actor name; this string is not a delivery capability.
    pub actor: Box<str>,
    /// Declared stable receiving host name.
    pub host: Box<str>,
    /// Declared concrete actor protocol.
    pub protocol: Box<str>,
    /// Declared issuer of the authority being presented.
    pub grant_issuer: Box<str>,
    /// Declared complete receiving runtime ID in the selected SDK text form.
    pub runtime_generation: Box<str>,
    /// Declared actual allocated actor address within that runtime scope.
    pub actor_generation: u64,
    /// Declared authority revision to compare against current permission.
    pub authority_generation: u64,
    /// Declared request sequence within the caller session.
    pub request_sequence: u64,
}
