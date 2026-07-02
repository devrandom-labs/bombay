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
//! Provides a topic-based message broker for the actor system.
//!
//! The `broker` module implements a flexible topic-based publish/subscribe mechanism that allows
//! actors to communicate based on hierarchical topics rather than direct references. It supports
//! glob pattern matching for topic subscriptions, allowing for powerful and flexible message routing.
//!
//! # Features
//!
//! - **Topic-Based Routing**: Messages are routed based on their topic rather than direct actor references.
//! - **Pattern Matching**: Subscriptions use glob patterns, supporting wildcards and hierarchical topics.
//! - **Multiple Delivery Strategies**: Configure how messages are delivered to handle different reliability needs.
//! - **Automatic Cleanup**: Dead actor references are automatically removed from subscription lists.
//!
//! # Example
//!
//! ```
//! use std::time::Duration;
//!
//! use bombay::prelude::*;
//! use bombay_actors::broker::{Broker, Subscribe, Publish};
//! use bombay_actors::DeliveryStrategy;
//! use glob::Pattern;
//!
//! #[derive(Actor, Clone)]
//! struct TemperatureUpdate(f32);
//!
//! #[derive(Actor)]
//! struct TemperatureSensor;
//!
//! #[derive(Actor)]
//! struct DisplayActor;
//!
//! # impl Message<TemperatureUpdate> for DisplayActor {
//! #     type Reply = ();
//! #     async fn handle(&mut self, msg: TemperatureUpdate, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply { }
//! # }
//!
//! # tokio_test::block_on(async {
//! // Create a broker with best effort delivery
//! let broker = Broker::<TemperatureUpdate>::new(DeliveryStrategy::BestEffort);
//! let broker_ref = Broker::spawn(broker);
//!
//! // Create a display actor and subscribe to kitchen temperature updates
//! let display = DisplayActor::spawn(DisplayActor);
//! broker_ref.tell(Subscribe {
//!     topic: Pattern::new("sensors/kitchen/*").unwrap(),
//!     recipient: display.recipient(),
//! }).await?;
//!
//! // Publish a temperature update
//! broker_ref.tell(Publish {
//!     topic: "sensors/kitchen/temperature".to_string(),
//!     message: TemperatureUpdate(22.5),
//! }).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! ```

use std::collections::HashMap;

use bombay::prelude::*;
use glob::{MatchOptions, Pattern};

use crate::DeliveryStrategy;

/// A generic topic-based message broker for the actor system.
///
/// The broker manages subscriptions to topics and delivers messages published
/// to those topics according to the specified delivery strategy.
///
/// Topics use glob pattern matching syntax, allowing for flexible subscription patterns:
/// - `sensors/*` - Any topic starting with "sensors/"
/// - `*/temperature` - Any topic ending with "/temperature"
/// - `sensors/*/humidity` - Match any topic with "sensors/" prefix and "/humidity" suffix
#[derive(Actor, Clone, Debug, Default)]
pub struct Broker<M: Send + 'static> {
    subscriptions: HashMap<Pattern, Vec<Recipient<M>>>,
    delivery_strategy: DeliveryStrategy,
}

impl<M: Send + 'static> Broker<M> {
    /// Creates a new broker with the specified delivery strategy.
    ///
    /// # Arguments
    ///
    /// * `delivery_strategy` - Determines how messages are delivered to subscribers
    ///
    /// # Returns
    ///
    /// A new `Broker` instance with the specified delivery strategy
    pub fn new(delivery_strategy: DeliveryStrategy) -> Self {
        Broker {
            subscriptions: HashMap::new(),
            delivery_strategy,
        }
    }

    fn unsubscribe(&mut self, pattern: &Pattern, actor_id: ActorId) {
        if let Some(recipients) = self.subscriptions.get_mut(pattern) {
            recipients.retain(|recipient| recipient.id() != actor_id);
            if recipients.is_empty() {
                self.subscriptions.remove(pattern);
            }
        }
    }
}

/// Message for subscribing an actor to a topic pattern.
///
/// When an actor subscribes to a topic pattern, it will receive all messages
/// published to topics that match that pattern.
#[derive(Clone, Debug)]
pub struct Subscribe<M: Send + 'static> {
    /// The pattern to subscribe to, using glob syntax
    pub topic: Pattern,
    /// The recipient that will receive messages published to matching topics
    pub recipient: Recipient<M>,
}

impl<M: Send + 'static> Message<Subscribe<M>> for Broker<M> {
    type Reply = ();

    async fn handle(
        &mut self,
        Subscribe { topic, recipient }: Subscribe<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscriptions.entry(topic).or_default().push(recipient);
    }
}

/// Message for unsubscribing an actor from topics.
///
/// Can unsubscribe from a specific topic pattern or all patterns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unsubscribe {
    /// The specific topic pattern to unsubscribe from.
    /// If None, unsubscribe from all topic patterns.
    pub topic: Option<Pattern>,
    /// The ID of the actor to unsubscribe.
    pub actor_id: ActorId,
}

impl<M: Send + 'static> Message<Unsubscribe> for Broker<M> {
    type Reply = ();

    async fn handle(
        &mut self,
        Unsubscribe { topic, actor_id }: Unsubscribe,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match topic {
            Some(topic) => {
                self.unsubscribe(&topic, actor_id);
            }
            None => {
                self.subscriptions.retain(|_, recipients| {
                    recipients.retain(|recipient| recipient.id() != actor_id);
                    !recipients.is_empty()
                });
            }
        }
    }
}

/// Message for publishing content to a specific topic.
///
/// When a message is published to a topic, it will be delivered to all actors
/// that have subscribed to matching topic patterns, according to the broker's
/// delivery strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish<M: Send + 'static> {
    /// The exact topic to publish to (not a pattern)
    pub topic: String,
    /// The message payload to deliver to subscribers
    pub message: M,
}

impl<M: Clone + Send + 'static> Message<Publish<M>> for Broker<M> {
    type Reply = ();

    async fn handle(
        &mut self,
        Publish { topic, message }: Publish<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let options = MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: false,
        };

        let mut to_remove = Vec::new();
        for (pattern, recipients) in &self.subscriptions {
            if pattern.matches_with(&topic, options) {
                for recipient in recipients {
                    match self.delivery_strategy {
                        DeliveryStrategy::Guaranteed => {
                            let res = recipient.tell(message.clone()).await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                to_remove.push((pattern.clone(), recipient.id()));
                            }
                        }
                        DeliveryStrategy::BestEffort => {
                            let res = recipient.tell(message.clone()).try_send();
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                to_remove.push((pattern.clone(), recipient.id()));
                            }
                        }
                        DeliveryStrategy::TimedDelivery(duration) => {
                            let res = recipient
                                .tell(message.clone())
                                .mailbox_timeout(duration)
                                .await;
                            if let Err(SendError::ActorNotRunning(_)) = res {
                                to_remove.push((pattern.clone(), recipient.id()));
                            }
                        }
                        DeliveryStrategy::Spawned => {
                            let pattern = pattern.clone();
                            let recipient = recipient.clone();
                            let message = message.clone();
                            let broker_ref = ctx.actor_ref().clone();
                            tokio::spawn(async move {
                                let res = recipient.tell(message).send().await;
                                if let Err(SendError::ActorNotRunning(_)) = res {
                                    let _ = broker_ref
                                        .tell(Unsubscribe {
                                            topic: Some(pattern),
                                            actor_id: recipient.id(),
                                        })
                                        .await;
                                }
                            });
                        }
                        DeliveryStrategy::SpawnedWithTimeout(duration) => {
                            let pattern = pattern.clone();
                            let recipient = recipient.clone();
                            let message = message.clone();
                            let broker_ref = ctx.actor_ref().clone();
                            tokio::spawn(async move {
                                let res = recipient
                                    .tell(message)
                                    .mailbox_timeout(duration)
                                    .send()
                                    .await;
                                if let Err(SendError::ActorNotRunning(_)) = res {
                                    let _ = broker_ref
                                        .tell(Unsubscribe {
                                            topic: Some(pattern),
                                            actor_id: recipient.id(),
                                        })
                                        .await;
                                }
                            });
                        }
                    }
                }
            }
        }

        for (pattern, actor_id) in to_remove {
            self.unsubscribe(&pattern, actor_id);
        }
    }
}

/// Test-only introspection: a query that replies with how many times `actor_id`
/// appears across the broker's subscription map, scoped either to one pattern or
/// to every pattern.
///
/// Routed through the broker mailbox like any other message, so the reply observes
/// state *after* whatever publish / unsubscribe preceded it has been handled — which
/// is exactly what the `broker.feature` `@lifecycle` / `@boundary` scenarios assert
/// ("removed from the subscription", "remains subscribed"). The count is over raw
/// registrations (the `Vec` slot), so a recipient subscribed twice under one pattern
/// counts twice — matching the no-dedupe routing contract.
///
/// Gated to test/`testing` builds so it never appears on the public release API.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug)]
pub struct CountSubscriptions {
    /// When `Some`, count only within this exact pattern key; when `None`, count
    /// the actor's registrations across every pattern.
    pub pattern: Option<Pattern>,
    /// The recipient actor whose registrations are counted.
    pub actor_id: ActorId,
}

#[cfg(any(test, feature = "testing"))]
impl<M: Send + 'static> Message<CountSubscriptions> for Broker<M> {
    type Reply = usize;

    async fn handle(
        &mut self,
        CountSubscriptions { pattern, actor_id }: CountSubscriptions,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let count_in = |recipients: &Vec<Recipient<M>>| {
            recipients
                .iter()
                .filter(|recipient| recipient.id() == actor_id)
                .count()
        };
        match pattern {
            Some(pattern) => self.subscriptions.get(&pattern).map_or(0, count_in),
            None => self.subscriptions.values().map(count_in).sum(),
        }
    }
}

/// Test-only introspection: a query that replies whether the broker currently holds
/// a subscription-map entry for the exact `pattern` key.
///
/// Used by the `broker.feature` `@lifecycle` scenario asserting that unsubscribing
/// the last recipient of a pattern drops the pattern key entirely.
#[cfg(any(test, feature = "testing"))]
#[derive(Clone, Debug)]
pub struct HasPatternKey {
    /// The exact pattern key to look up.
    pub pattern: Pattern,
}

#[cfg(any(test, feature = "testing"))]
impl<M: Send + 'static> Message<HasPatternKey> for Broker<M> {
    type Reply = bool;

    async fn handle(
        &mut self,
        HasPatternKey { pattern }: HasPatternKey,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscriptions.contains_key(&pattern)
    }
}
