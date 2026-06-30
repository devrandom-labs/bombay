//! Live-defect probe for `@bug:actors/src/message_queue.rs:707` (and its bind-side
//! companion `:591`) — the malformed-Topic-routing-key run-loop panic.
//!
//! Both `message_queue.feature` (~lines 292, 304) and
//! `message_queue.properties.feature` (~lines 85, 99) carry `@bug:...:707` /
//! `@bug:...:591` scenarios asserting the DESIRED behaviour: a malformed Topic
//! routing key is rejected (bind-time) / returns an error instead of panicking
//! (publish-time) via `AmqpError::InvalidRoutingKey`. That variant does NOT exist
//! in the enum yet (it has 9 variants; adding it is SEPARATE card #79), so those
//! scenarios are excluded from the green `message_queue_bdd` /
//! `message_queue_props_bdd` runners (their `!t.starts_with("bug")` filter) AND no
//! step definition in `steps/message_queue.rs` names the missing variant — the
//! crate therefore compiles.
//!
//! Defect: `message_queue.rs:591-642` (`QueueBind`) validates queue/exchange
//! existence, duplicate bindings, and the Headers `x-match` value, but NEVER
//! validates that a Topic `routing_key` is a compilable glob. A malformed key
//! (e.g. `"[unclosed"`) is ACCEPTED at bind and later hits
//! `Pattern::new(&binding.routing_key).unwrap()` at `:707` on publish, which
//! PANICS the actor run-loop.
//!
//! This probe asserts the DESIRED outcome (the actor SURVIVES the publish) through
//! the real SUT, WITHOUT referencing the missing variant. It FAILS TODAY (the
//! run-loop dies), so it is `#[ignore]`d to keep the default test run green; run it
//! with `--ignored` to watch it go RED. It starts PASSING the moment :591/:707 are
//! fixed (card #79) — at which point the matching `@bug` cucumber scenarios become
//! the green spec and this probe should be deleted. Do NOT weaken it.

#[path = "steps/message_queue.rs"]
mod message_queue;

use message_queue::malformed_topic_key_survives_publish;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "RED until message_queue.rs:591/:707 validate Topic keys (card #79); run with --ignored"]
async fn bug_message_queue_707_malformed_topic_key_panics_run_loop() {
    // `malformed_topic_key_survives_publish()` drives the SUT exactly like the @bug
    // scenarios: Topic exchange, declare q, bind q with "[unclosed" (accepted —
    // the :591 gap), publish "log.warn" (reaches the :707 `.unwrap()`), then probe
    // liveness. It returns `false` while the panic is live, `true` once the SUT
    // returns an error instead of panicking.
    let survived = malformed_topic_key_survives_publish().await;
    assert!(
        survived,
        "BUG:707/591 LIVE — a malformed Topic routing key (\"[unclosed\") was \
         accepted at bind and panicked the run-loop at publish. Once \
         message_queue.rs validates Topic keys (card #79: AmqpError::InvalidRoutingKey), \
         this passes; then move the @bug scenarios into the green runners and delete \
         this probe."
    );
}
