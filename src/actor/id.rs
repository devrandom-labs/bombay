// --- #61 quarantine (vendored kameo, pre-god-level-bar) -------------------
// This file predates the workspace god-level clippy bar (root Cargo.toml).
// It is held at the prior standard and is cleaned or deleted file-by-file
// under M1/M7. NEW code is NOT exempt — remove this block when the file is
// brought up to the bar or dropped. De-quarantine checklist: issue #61.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::disallowed_methods,
    clippy::clone_on_ref_ptr,
    clippy::as_conversions,
    clippy::str_to_string,
    clippy::implicit_clone,
    clippy::shadow_reuse,
    clippy::shadow_same,
    clippy::shadow_unrelated,
    clippy::allow_attributes_without_reason,
    reason = "Vendored kameo predating the #61 god-level clippy bar; held at the prior standard, cleaned or deleted file-by-file under M1/M7. New code is not exempt. See #61."
)]
use std::error;
use std::hash::Hash;
#[cfg(feature = "remote")]
use std::hash::Hasher;
use std::sync::atomic::Ordering;
use std::{fmt, sync::atomic::AtomicUsize};

use serde::{Deserialize, Serialize};

#[cfg(feature = "remote")]
use crate::remote::ActorSwarm;

static ACTOR_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// A globally unique identifier for an actor within a distributed system.
///
/// `ActorId` combines a locally sequential `sequence_id` with an optional `peer_id`
/// to uniquely identify actors across a distributed network.#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId {
    #[cfg(feature = "remote")]
    peer_id: PeerIdKind,
    sequence_id: u64,
}

impl ActorId {
    /// Creates a new `ActorId` with the given `sequence_id`, using the local actor swarm.
    ///
    /// If the local actor swarm hasn't been bootstrapped, no `peer_id` will be associated,
    /// but the actor is still considered to be running locally.
    ///
    /// # Arguments
    ///
    /// * `sequence_id` - The sequential identifier for the actor.
    ///
    /// # Returns
    ///
    /// A new `ActorId` instance.
    pub fn new(sequence_id: u64) -> Self {
        ActorId {
            sequence_id,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        }
    }

    /// Creates a new `ActorId` with a specific `sequence_id` and `peer_id`.
    ///
    /// # Arguments
    ///
    /// * `sequence_id` - The sequential identifier for the actor.
    /// * `peer_id` - The `PeerId` associated with this actor.
    ///
    /// # Returns
    ///
    /// A new `ActorId` instance.
    #[cfg(feature = "remote")]
    pub fn new_with_peer_id(sequence_id: u64, peer_id: libp2p::PeerId) -> Self {
        ActorId {
            sequence_id,
            peer_id: PeerIdKind::PeerId(peer_id),
        }
    }

    /// Generates a new `ActorId` with an automatically incremented `sequence_id`.
    ///
    /// Uses an atomic counter to ensure unique `sequence_id` values across threads.
    ///
    /// # Returns
    ///
    /// A new `ActorId` instance with the next available `sequence_id`.
    pub fn generate() -> Self {
        ActorId::new(
            ACTOR_COUNTER
                .fetch_add(1, Ordering::Relaxed)
                .try_into()
                .unwrap(),
        )
    }

    /// Returns the sequential identifier of the actor.
    ///
    /// This `sequence_id` is a unique, locally-generated `u64` assigned to each actor
    /// in the order they are spawned. The first spawned actor gets id 0, the second 1, and so on.
    ///
    /// # Returns
    ///
    /// A `u64` representing the actor's `sequence_id`.
    pub fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    /// Returns the `PeerId` associated with the `ActorId`, if any.
    ///
    /// # Returns
    ///
    /// An `Option<PeerId>`. `None` is returned if the peer ID is local and no actor swarm has been bootstrapped.
    #[cfg(feature = "remote")]
    pub fn peer_id(&self) -> Option<&libp2p::PeerId> {
        self.peer_id.peer_id()
    }

    /// Serializes the `ActorId` into a byte vector.
    ///
    /// The resulting vector contains the `sequence_id` followed by the `peer_id` (if present).
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the serialized `ActorId`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 42);
        bytes.extend(&self.sequence_id.to_le_bytes());

        #[cfg(feature = "remote")]
        {
            let peer_id_bytes = self
                .peer_id()
                .map(|peer_id| peer_id.to_bytes())
                .or_else(|| ActorSwarm::get().map(|swarm| swarm.local_peer_id().to_bytes()));

            if let Some(peer_id_bytes) = peer_id_bytes {
                bytes.extend(peer_id_bytes);
            }
        }

        bytes
    }

    /// Deserializes an `ActorId` from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `bytes` - A byte slice containing a serialized `ActorId`.
    ///
    /// # Returns
    ///
    /// A `Result` containing either the deserialized `ActorId` or an `ActorIdFromBytesError`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ActorIdFromBytesError> {
        // Extract the ID. Bounds-check the length before slicing: `bytes[0..8]`
        // on a slice shorter than 8 would panic, so a truncated (untrusted) wire
        // buffer must be rejected with `MissingSequenceID`, never a panic.
        let sequence_id = u64::from_le_bytes(
            bytes
                .get(0..8)
                .and_then(|head| head.try_into().ok())
                .ok_or(ActorIdFromBytesError::MissingSequenceID)?,
        );

        // Extract the peer id
        #[cfg(feature = "remote")]
        let peer_id = if bytes.len() > 8 {
            PeerIdKind::PeerId(libp2p::PeerId::from_bytes(&bytes[8..])?)
        } else {
            PeerIdKind::Local
        };

        Ok(ActorId {
            sequence_id,
            #[cfg(feature = "remote")]
            peer_id,
        })
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(not(feature = "remote"))]
        return write!(f, "#{}", self.sequence_id);

        #[cfg(feature = "remote")]
        match self.peer_id.peer_id() {
            Some(peer_id) => write!(f, "#{}@{peer_id}", self.sequence_id),
            None => write!(f, "#{}@local", self.sequence_id),
        }
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(feature = "remote")]
        return write!(
            f,
            "ActorId({:?}, {:?})",
            self.sequence_id,
            self.peer_id.peer_id()
        );

        #[cfg(not(feature = "remote"))]
        return write!(f, "ActorId({:?})", self.sequence_id);
    }
}

impl Serialize for ActorId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for ActorId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ActorIdVisitor;

        impl<'de> serde::de::Visitor<'de> for ActorIdVisitor {
            type Value = ActorId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bytes representing an ActorId")
            }

            fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes_len = bytes.len();
                ActorId::from_bytes(bytes).map_err(|err| match err {
                    ActorIdFromBytesError::MissingSequenceID => {
                        E::invalid_length(bytes_len, &"sequence ID")
                    }
                    #[cfg(feature = "remote")]
                    err @ ActorIdFromBytesError::ParsePeerID(_) => E::custom(err),
                })
            }

            fn visit_byte_buf<E>(self, bytes: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_bytes(&bytes)
            }
        }

        deserializer.deserialize_bytes(ActorIdVisitor)
    }
}

/// Errors that can occur when deserializing an `ActorId` from bytes.
#[derive(Debug)]
#[cfg_attr(not(feature = "remote"), derive(Clone))]
pub enum ActorIdFromBytesError {
    /// The byte slice doesn't contain enough data for the `sequence_id`.
    MissingSequenceID,
    /// An error occurred while parsing the `PeerId`.
    #[cfg(feature = "remote")]
    ParsePeerID(libp2p::identity::ParseError),
}

#[cfg(feature = "remote")]
impl From<libp2p::identity::ParseError> for ActorIdFromBytesError {
    fn from(err: libp2p::identity::ParseError) -> Self {
        ActorIdFromBytesError::ParsePeerID(err)
    }
}

impl fmt::Display for ActorIdFromBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorIdFromBytesError::MissingSequenceID => write!(f, "missing instance ID"),
            #[cfg(feature = "remote")]
            ActorIdFromBytesError::ParsePeerID(err) => err.fmt(f),
        }
    }
}

impl error::Error for ActorIdFromBytesError {}

#[cfg(feature = "remote")]
#[derive(Clone, Copy)]
enum PeerIdKind {
    Local,
    PeerId(libp2p::PeerId),
}

#[cfg(feature = "remote")]
impl PeerIdKind {
    fn peer_id(&self) -> Option<&libp2p::PeerId> {
        match self {
            PeerIdKind::Local => ActorSwarm::get().map(ActorSwarm::local_peer_id),
            PeerIdKind::PeerId(peer_id) => Some(peer_id),
        }
    }
}

#[cfg(feature = "remote")]
impl PartialEq for PeerIdKind {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id() == other.peer_id()
    }
}

#[cfg(feature = "remote")]
impl Eq for PeerIdKind {}

#[cfg(feature = "remote")]
impl PartialOrd for PeerIdKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "remote")]
impl Ord for PeerIdKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.peer_id().cmp(&other.peer_id())
    }
}

#[cfg(feature = "remote")]
impl Hash for PeerIdKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(peer_id) = self.peer_id() {
            state.write(&peer_id.to_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hasher};

    #[cfg(feature = "remote")]
    use libp2p::PeerId;

    use super::*;

    #[cfg(feature = "remote")]
    static BARRIER: std::sync::Barrier = std::sync::Barrier::new(2);

    #[cfg(feature = "remote")]
    fn local_peer_id() -> PeerId {
        PeerId::from_bytes(&[
            0, 32, 77, 249, 14, 119, 133, 11, 205, 96, 61, 232, 63, 206, 126, 234, 204, 60, 241,
            93, 2, 68, 130, 67, 3, 193, 242, 23, 80, 189, 82, 144, 152, 206,
        ])
        .unwrap()
    }

    #[test]
    fn test_actor_id_partial_eq_local() {
        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        assert_eq!(id1, id2);

        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 1,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        assert_ne!(id1, id2);
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_actor_id_partial_eq_remote() {
        // Unbootstrapped
        use tokio::sync::mpsc;

        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        assert_eq!(id1, id2);

        BARRIER.wait();

        // Bootstrapped
        let local_peer_id = local_peer_id();
        let _ = ActorSwarm::set(mpsc::unbounded_channel().0, local_peer_id);
        assert_eq!(id1.peer_id(), Some(&local_peer_id));
        assert_eq!(id2.peer_id(), Some(&local_peer_id));

        // Bootstrapped local ids should equal
        assert_eq!(id1, id2);

        // Bootstrapped local and remote id pointing to local peer id should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        assert_eq!(id1, id2);

        // Peer IDs should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        assert_eq!(id1, id2);

        // Different peer IDs should not equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(PeerId::random()),
        };
        assert_ne!(id1, id2);
    }

    fn hashes_eq(id1: &ActorId, id2: &ActorId) -> bool {
        let mut hasher = DefaultHasher::new();
        id1.hash(&mut hasher);
        let id1_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        id2.hash(&mut hasher);
        let id2_hash = hasher.finish();

        id1_hash == id2_hash
    }

    #[test]
    fn test_actor_id_hash_local() {
        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };

        assert!(hashes_eq(&id1, &id2));

        let id1 = ActorId {
            sequence_id: 0,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 1,
            #[cfg(feature = "remote")]
            peer_id: PeerIdKind::Local,
        };

        assert!(!hashes_eq(&id1, &id2));
    }

    #[test]
    #[cfg(feature = "remote")]
    fn test_actor_id_hash_remote() {
        // Unbootstrapped
        use tokio::sync::mpsc;

        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };

        assert!(hashes_eq(&id1, &id2));

        BARRIER.wait();

        // Bootstrapped
        let local_peer_id = local_peer_id();
        let _ = ActorSwarm::set(mpsc::unbounded_channel().0, local_peer_id);
        assert_eq!(id1.peer_id(), Some(&local_peer_id));
        assert_eq!(id2.peer_id(), Some(&local_peer_id));

        // Bootstrapped local ids should equal
        assert_eq!(id1, id2);

        // Bootstrapped local and remote id pointing to local peer id should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::Local,
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };

        assert!(hashes_eq(&id1, &id2));

        // Peer IDs should equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };

        assert!(hashes_eq(&id1, &id2));

        // Different peer IDs should not equal
        let id1 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(local_peer_id),
        };
        let id2 = ActorId {
            sequence_id: 0,
            peer_id: PeerIdKind::PeerId(PeerId::random()),
        };

        assert!(!hashes_eq(&id1, &id2));
    }
}
