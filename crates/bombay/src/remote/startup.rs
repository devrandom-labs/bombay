//! Validate the explicitly supported Node connection profile before native open.
use core::fmt;
use std::error::Error;
use std::slice;
use tokio::runtime::{RuntimeFlavor, TryCurrentError};
use zenoh::Error as ZenohError;
use zenoh::config::{EndPoint, EndPoints};
use zenoh_config::{Config, ModeDependent, SecretValue};
use zenoh_protocol::core::LocatorsStrategy;

/// Startup was refused while retaining the original Application and configuration.
pub enum NodeStartupRefusal {
    /// A supplied runtime identity would defeat the fresh-runtime contract.
    ConfiguredRuntimeId,
    /// A connection setting falls outside the selected TLS profile.
    Unsupported { field: &'static str },
    /// Both file and base64 sources were supplied for one credential.
    CredentialConflict { field: &'static str },
    /// A used connection direction lacks a required credential declaration.
    CredentialRequired { field: &'static str },
    /// Native retained-query capacity was not explicitly supplied.
    QueryLimitRequired,
    /// Capacity exceeds the native semaphore representation.
    QueryLimitExcessive,
    /// The native Session could not open; original Application/config remain owned.
    Open { cause: ZenohError },
    /// No entered Tokio host is available.
    Host { cause: TryCurrentError },
    /// The native SDK requires an entered multithreaded Tokio host.
    HostFlavor { flavor: RuntimeFlavor },
}

fn inspect_endpoint(endpoint: &EndPoint, field: &'static str) -> Result<(), NodeStartupRefusal> {
    if endpoint.protocol().as_ref() != "tls" {
        return Err(NodeStartupRefusal::Unsupported { field });
    }
    if !endpoint.config().is_empty() {
        return Err(NodeStartupRefusal::Unsupported { field });
    }
    Ok(())
}

fn require_credential(
    path: Option<&str>,
    base64: Option<&SecretValue>,
    field: &'static str,
) -> Result<(), NodeStartupRefusal> {
    if path.is_none() && base64.is_none() {
        return Err(NodeStartupRefusal::CredentialRequired { field });
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one coherent configured connection profile, with native parsing retained by the SDK"
)]
fn inspect_node_configuration(config: &Config) -> Result<(), NodeStartupRefusal> {
    if config.id().is_some() {
        return Err(NodeStartupRefusal::ConfiguredRuntimeId);
    }
    let mode = config
        .mode()
        .ok_or(NodeStartupRefusal::Unsupported { field: "mode" })?;
    let link = config.transport().link();
    if !matches!(link.protocols().as_deref(), Some([protocol]) if protocol == "tls") {
        return Err(NodeStartupRefusal::Unsupported {
            field: "transport/link/protocols",
        });
    }
    let tls = link.tls();
    for (field, actual, expected) in [
        ("enable_mtls", tls.enable_mtls(), &Some(true)),
        (
            "verify_name_on_connect",
            tls.verify_name_on_connect(),
            &Some(true),
        ),
        ("use_public_pki", tls.use_public_pki(), &Some(false)),
    ] {
        if actual != expected {
            return Err(NodeStartupRefusal::Unsupported { field });
        }
    }
    let scouting = config.scouting();
    if scouting.multicast().enabled() != &Some(false)
        || scouting.gossip().enabled() != &Some(false)
        || scouting
            .multicast()
            .listen()
            .as_ref()
            .and_then(|value| value.get(mode))
            != Some(&false)
    {
        return Err(NodeStartupRefusal::Unsupported { field: "scouting" });
    }
    let connect = config
        .connect()
        .endpoints()
        .get(mode)
        .map_or(&[][..], Vec::as_slice);
    let listen = config
        .listen()
        .endpoints()
        .get(mode)
        .map_or(&[][..], Vec::as_slice);
    let mut connect_count = 0;
    for group in connect {
        let endpoints = match group {
            EndPoints::Single(endpoint) => slice::from_ref(endpoint),
            EndPoints::Locators(group) => match group.strategy {
                LocatorsStrategy::AllOf => group.locators.as_slice(),
                LocatorsStrategy::OneOf => {
                    return Err(NodeStartupRefusal::Unsupported {
                        field: "connect/endpoints/strategy",
                    });
                }
            },
        };
        connect_count += endpoints.len();
        for endpoint in endpoints {
            inspect_endpoint(endpoint, "connect/endpoints")?;
        }
    }
    for endpoint in listen {
        inspect_endpoint(endpoint, "listen/endpoints")?;
    }
    if connect_count == 0 && listen.is_empty() {
        return Err(NodeStartupRefusal::Unsupported {
            field: "connect/listen/endpoints",
        });
    }
    for (field, path, base64) in [
        (
            "root_ca_certificate",
            tls.root_ca_certificate(),
            tls.root_ca_certificate_base64(),
        ),
        (
            "connect_certificate",
            tls.connect_certificate(),
            tls.connect_certificate_base64(),
        ),
        (
            "connect_private_key",
            tls.connect_private_key(),
            tls.connect_private_key_base64(),
        ),
        (
            "listen_certificate",
            tls.listen_certificate(),
            tls.listen_certificate_base64(),
        ),
        (
            "listen_private_key",
            tls.listen_private_key(),
            tls.listen_private_key_base64(),
        ),
    ] {
        if path.is_some() && base64.is_some() {
            return Err(NodeStartupRefusal::CredentialConflict { field });
        }
    }
    require_credential(
        tls.root_ca_certificate().as_deref(),
        tls.root_ca_certificate_base64().as_ref(),
        "root_ca_certificate",
    )?;
    if connect_count > 0 {
        require_credential(
            tls.connect_certificate().as_deref(),
            tls.connect_certificate_base64().as_ref(),
            "connect_certificate",
        )?;
        require_credential(
            tls.connect_private_key().as_deref(),
            tls.connect_private_key_base64().as_ref(),
            "connect_private_key",
        )?;
    }
    if !listen.is_empty() {
        require_credential(
            tls.listen_certificate().as_deref(),
            tls.listen_certificate_base64().as_ref(),
            "listen_certificate",
        )?;
        require_credential(
            tls.listen_private_key().as_deref(),
            tls.listen_private_key_base64().as_ref(),
            "listen_private_key",
        )?;
    }
    if config.queries_max_retained().is_none() {
        return Err(NodeStartupRefusal::QueryLimitRequired);
    }
    if !Config::validate_queries_max_retained(config.queries_max_retained()) {
        return Err(NodeStartupRefusal::QueryLimitExcessive);
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "return the actual native configuration on refusal without boxing originals"
)]
pub(crate) fn check_node_configuration(
    config: Config,
) -> Result<Config, (Config, NodeStartupRefusal)> {
    match inspect_node_configuration(&config) {
        Ok(()) => Ok(config),
        Err(cause) => Err((config, cause)),
    }
}

impl fmt::Display for NodeStartupRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfiguredRuntimeId => {
                formatter.write_str("the Node requires an unset native runtime ID")
            }
            Self::Unsupported { field } => {
                write!(formatter, "unsupported Node connection setting: {field}")
            }
            Self::CredentialConflict { field } => write!(
                formatter,
                "conflicting file and base64 credentials: {field}"
            ),
            Self::CredentialRequired { field } => {
                write!(formatter, "missing Node credential: {field}")
            }
            Self::QueryLimitRequired => {
                formatter.write_str("the Node requires explicit retained query capacity")
            }
            Self::QueryLimitExcessive => {
                formatter.write_str("retained query capacity exceeds the native supported maximum")
            }
            Self::Open { .. } => formatter.write_str("native Node startup failed"),
            Self::Host { cause } => write!(
                formatter,
                "the Node requires an entered Tokio runtime: {cause}"
            ),
            Self::HostFlavor { flavor } => write!(
                formatter,
                "the Node requires a multithreaded Tokio runtime, received {flavor:?}"
            ),
        }
    }
}

impl fmt::Debug for NodeStartupRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NodeStartupRefusal")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl Error for NodeStartupRefusal {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Open { cause } => Some(cause.as_ref()),
            Self::Host { cause } => Some(cause),
            Self::ConfiguredRuntimeId
            | Self::Unsupported { .. }
            | Self::CredentialConflict { .. }
            | Self::CredentialRequired { .. }
            | Self::QueryLimitRequired
            | Self::QueryLimitExcessive
            | Self::HostFlavor { .. } => None,
        }
    }
}
