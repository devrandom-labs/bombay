//! Explicit application networking inputs and private native configuration.

use super::startup::NodeStartupRefusal;
use serde::Deserialize;
use serde_json::Value;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use zenoh_config::qos::{CongestionControlConf, QosOverwriteMessage, QosOverwrites};
use zenoh_config::{
    Config, EndPoint, EndPoints, InterceptorFlow, ModeDependentValue, QosOverwriteItemConf,
    SecretValue, TLSConf, WhatAmI,
};
use zenoh_protocol::core::{Locators, LocatorsStrategy};

/// Explicit addresses, identities and query capacity for one Application.
#[derive(Debug)]
pub struct Network {
    pub layout: NetworkLayout,
    pub deployment_roots: CredentialSource,
    pub max_retained_queries: NonZeroUsize,
}

/// The supported deployment layouts; routers are external infrastructure.
#[derive(Debug)]
pub enum NetworkLayout {
    Peer {
        connect: Option<ConnectionEndpoints>,
        listen: Option<ConnectionEndpoints>,
    },
    RouterClient {
        connect: ConnectionEndpoints,
    },
}

/// An all-of address list and the identity used for that connection direction.
#[derive(Debug)]
pub struct ConnectionEndpoints {
    pub addresses: Vec<String>,
    pub certificate: CredentialSource,
    pub private_key: CredentialSource,
}

/// A credential file or its base64 contents, without implicit source fallback.
pub enum CredentialSource {
    File(PathBuf),
    Base64(Box<str>),
}

impl Network {
    #[expect(
        clippy::too_many_lines,
        reason = "one coherent connection profile preserves every supplied direction and credential"
    )]
    pub(crate) fn to_configuration(&self) -> Result<Config, NodeStartupRefusal> {
        let (mode, connect, listen) = match &self.layout {
            NetworkLayout::Peer { connect, listen } => {
                (WhatAmI::Peer, connect.as_ref(), listen.as_ref())
            }
            NetworkLayout::RouterClient { connect } => (WhatAmI::Client, Some(connect), None),
        };
        if connect.is_none() && listen.is_none() {
            return Err(NodeStartupRefusal::Unsupported {
                field: "connect/listen/endpoints",
            });
        }
        let mut config = Config::default();
        config
            .set_id(None)
            .map_err(|_| NodeStartupRefusal::Unsupported { field: "id" })?;
        config
            .set_mode(Some(mode))
            .map_err(|_| NodeStartupRefusal::Unsupported { field: "mode" })?;
        config
            .set_queries_max_retained(Some(self.max_retained_queries))
            .map_err(|_| NodeStartupRefusal::QueryLimitExcessive)?;
        // This removes native refill waiting, not SDK locks or serialization.
        config
            .transport
            .link
            .tx
            .queue
            .congestion_control
            .drop
            .set_wait_before_drop(0)
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "transport/link/tx/queue/congestion_control/drop/wait_before_drop",
            })?;
        // Normal network replies inherit this query policy; local Get must set its own.
        config
            .qos
            .set_network(vec![QosOverwriteItemConf {
                id: None,
                zids: None,
                interfaces: None,
                link_protocols: None,
                messages: (QosOverwriteMessage::Query, Vec::new()).into(),
                key_exprs: None,
                overwrite: QosOverwrites {
                    congestion_control: Some(CongestionControlConf::Drop),
                    priority: None,
                    express: None,
                },
                flows: Some((InterceptorFlow::Ingress, Vec::new()).into()),
                qos: None,
                payload_size: None,
            }])
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "qos/network",
            })?;
        let outgoing = match connect {
            Some(direction) => vec![EndPoints::Locators(Locators {
                strategy: LocatorsStrategy::AllOf,
                locators: direction.endpoints("connect/endpoints")?,
            })],
            None => Vec::new(),
        };
        let incoming = match listen {
            Some(direction) => direction.endpoints("listen/endpoints")?,
            None => Vec::new(),
        };
        config
            .connect
            .set_endpoints(ModeDependentValue::Unique(outgoing))
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "connect/endpoints",
            })?;
        config
            .listen
            .set_endpoints(ModeDependentValue::Unique(incoming))
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "listen/endpoints",
            })?;
        config
            .scouting
            .multicast
            .set_enabled(Some(false))
            .map_err(|_| NodeStartupRefusal::Unsupported { field: "scouting" })?;
        config
            .scouting
            .multicast
            .set_listen(Some(ModeDependentValue::Unique(false)))
            .map_err(|_| NodeStartupRefusal::Unsupported { field: "scouting" })?;
        config
            .scouting
            .gossip
            .set_enabled(Some(false))
            .map_err(|_| NodeStartupRefusal::Unsupported { field: "scouting" })?;
        config
            .transport
            .link
            .set_protocols(Some(vec!["tls".into()]))
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "transport/link/protocols",
            })?;
        let tls = &mut config.transport.link.tls;
        tls.set_enable_mtls(Some(true))
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "enable_mtls",
            })?;
        tls.set_verify_name_on_connect(Some(true)).map_err(|_| {
            NodeStartupRefusal::Unsupported {
                field: "verify_name_on_connect",
            }
        })?;
        tls.set_use_public_pki(Some(false))
            .map_err(|_| NodeStartupRefusal::Unsupported {
                field: "use_public_pki",
            })?;
        self.deployment_roots.declare(
            tls,
            "root_ca_certificate",
            TLSConf::set_root_ca_certificate,
            TLSConf::set_root_ca_certificate_base64,
        )?;
        if let Some(direction) = connect {
            direction.certificate.declare(
                tls,
                "connect_certificate",
                TLSConf::set_connect_certificate,
                TLSConf::set_connect_certificate_base64,
            )?;
            direction.private_key.declare(
                tls,
                "connect_private_key",
                TLSConf::set_connect_private_key,
                TLSConf::set_connect_private_key_base64,
            )?;
        }
        if let Some(direction) = listen {
            direction.certificate.declare(
                tls,
                "listen_certificate",
                TLSConf::set_listen_certificate,
                TLSConf::set_listen_certificate_base64,
            )?;
            direction.private_key.declare(
                tls,
                "listen_private_key",
                TLSConf::set_listen_private_key,
                TLSConf::set_listen_private_key_base64,
            )?;
        }
        Ok(config)
    }
}

impl ConnectionEndpoints {
    fn endpoints(&self, field: &'static str) -> Result<Vec<EndPoint>, NodeStartupRefusal> {
        if self.addresses.is_empty() {
            return Err(NodeStartupRefusal::Unsupported { field });
        }
        self.addresses
            .iter()
            .map(|address| {
                let endpoint = EndPoint::new("tls", address.as_str(), "", "")
                    .map_err(|cause| NodeStartupRefusal::Address { field, cause })?;
                if endpoint.address().as_ref() != address
                    || !endpoint.metadata().is_empty()
                    || !endpoint.config().is_empty()
                {
                    return Err(NodeStartupRefusal::Unsupported { field });
                }
                Ok(endpoint)
            })
            .collect()
    }
}

impl CredentialSource {
    fn declare(
        &self,
        tls: &mut TLSConf,
        field: &'static str,
        file: impl FnOnce(&mut TLSConf, Option<String>) -> Result<Option<String>, Option<String>>,
        base64: impl FnOnce(
            &mut TLSConf,
            Option<SecretValue>,
        ) -> Result<Option<SecretValue>, Option<SecretValue>>,
    ) -> Result<(), NodeStartupRefusal> {
        match self {
            Self::File(path) => {
                let path = path
                    .to_str()
                    .ok_or(NodeStartupRefusal::CredentialPath { field })?;
                file(tls, Some(path.to_owned()))
                    .map_err(|_| NodeStartupRefusal::Unsupported { field })?;
            }
            Self::Base64(encoded) => {
                let secret = SecretValue::deserialize(Value::String(encoded.to_string()))
                    .map_err(|cause| NodeStartupRefusal::CredentialValue { field, cause })?;
                base64(tls, Some(secret)).map_err(|_| NodeStartupRefusal::Unsupported { field })?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for CredentialSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::File(_) => "File(..)",
            Self::Base64(_) => "Base64(..)",
        })
    }
}

#[cfg(test)]
mod tests;
