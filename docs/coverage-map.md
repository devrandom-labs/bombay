# Bombay outcome coverage map

This map is the Q2 coverage contract. Coverage percentages locate unexamined
code; named oracles prove laws. Run `nix build .#coverage -L` to produce the
reproducible HTML report at `result/html/index.html`. The 2026-08-08 baseline
is 93.33% functions, 97.43% lines, and 97.10% regions across both workspace
crates and every target.

Neighboring crates retain their own primitive coverage. Bombay tests cover
only composition across their public contracts:

| Bombay-owned law or path | Primary inversion oracle |
|---|---|
| Behavior folds sequentially; every ordinary return retires the environment | `bombay-engine::property_tests::run_to_completion::every_event_processed_once_in_order`; engine retirement and inversion oracles |
| Creation is interpreted before sends; failure prevents later effects | `runtime::environment::tests::{interprets_all_creates_before_any_send,failed_birth_is_reported_before_any_delivery,delivery_failure_is_distinct_from_child_birth_failure}` |
| Child nonces are generation-local tombstones | `runtime::environment::tests::duplicate_child_nonce_is_rejected_before_a_second_birth`; `runtime::child_scope::tests::observed_nonce_never_becomes_fresh_again` |
| Communication priority, zero-aging starvation rejection, draining, closure, backpressure, and payload recovery survive the mailbox adapter | `mailbox::communication::tests::{control_event_precedes_queued_user_events,zero_aging_drains_complete_control_backlog_before_waiting_user,queued_user_events_drain_before_lane_closure,registry_anchor_does_not_keep_mailbox_alive,blocked_producer_recovers_payload_when_receiver_retires}`; `lifecycle_oracle::blocked_public_send_recovers_exact_payload_after_incarnation_retirement` |
| Concurrent mailbox producers retain local FIFO without a false global interleaving promise | `adversarial_oracle::concurrent_producers_preserve_local_order_without_assuming_interleaving`; `mailbox-contract-check.pl` |
| Typed direct and routed delivery preserve payload, route order, and exact error leg | `routing::actor_ref::tests::moves_a_non_clone_message_directly_into_the_actor_event`; all `routing::delivery::tests` |
| Address registration is transactional, generation-exact, reusable only after retirement, and does not pin a mailbox | `lifecycle_oracle::{registration_failure_rolls_back_real_preparation_before_task_start,address_collision_rolls_back_without_disturbing_the_live_generation,reused_logical_address_gets_a_distinct_lifecycle_identity,last_edge_closure_is_not_pinned_by_the_registered_anchor}` |
| Observe publication cannot be missed or alias another incarnation | `lifecycle_oracle::{immediate_completion_is_published_once_before_it_can_be_missed,retained_completion_cannot_alias_a_replacement_incarnation,child_observation_reports_the_exact_spawned_generation,watching_receives_the_exact_peers_normalized_outcome}` |
| Task, registration, observation, and lifecycle retirement ordering is exact even on panic/cancellation | `runtime::incarnation::tests::{successful_execution_retires_actor_before_terminal_lease,terminal_retirement_releases_lease_before_publishing,panicking_observer_cannot_strand_the_other_terminal_domain}`; `lifecycle_oracle::{panic_and_cancellation_are_distinct_terminal_publications,lifecycle_facts_follow_the_exact_incarnation_edges,panicking_lifecycle_sink_cannot_disrupt_actor_retirement}` |
| Handle drop, outcome, close, and child extraction own distinct liveness/control/completion seats | all `runtime::handle::tests`; `lifecycle_oracle::dropping_handle_detaches_terminal_observation_without_cancelling_actor` |
| Transactional root activation initializes and interprets effects before registration, returns separate cloneable delivery and affine retirement seats, and leaves no endpoint on failure | all `activation_oracle` tests; `mnesis-bombay/tests/bombay_entity_contract.rs` exercises Entity's real runtime port in the integration repository |
| Parent retirement requests every child before ordered waits and retains child liveness | all `runtime::child_scope::tests`; `lifecycle_oracle::{parent_retains_created_child_handle_while_parent_is_live,root_shutdown_awaits_transitive_child_retirement}` |
| Shutdown uses the priority lane, interprets final effects, and reaches the full tree | `routing::actor_ref::tests::{publishes_shutdown_through_the_priority_lane,distinguishes_declined_construction_from_closed_delivery}`; `lifecycle_oracle::{graceful_shutdown_preempts_user_backlog_and_interprets_final_effects,root_shutdown_awaits_transitive_child_retirement}` |
| Timer schedules are interpretation-anchored, generation-replacing, non-early, independently identified, and injected as typed events | `runtime::incarnation_effects::tests::{a_new_generation_replaces_the_same_timer_identity,relative_schedule_is_anchored_when_the_effect_is_interpreted,unrepresentable_relative_deadline_is_a_typed_interpreter_error}`; `lifecycle_oracle::{typed_behavior_timer_fires_through_the_incarnation,successful_user_fold_replaces_the_live_receive_timeout_generation,nested_timers_at_the_same_deadline_keep_distinct_identities}` |
| Supervision reports the emitting nonce and retirement releases the whole tree | `runtime::incarnation_effects::tests::worker_report_is_stamped_with_the_emitting_child_nonce`; `lifecycle_oracle::supervision_escalation_retires_and_releases_the_complete_tree` |
| Lifecycle instrumentation observes truthful edges without becoming a failure source | `lifecycle_oracle::{lifecycle_facts_follow_the_exact_incarnation_edges,panicking_lifecycle_sink_cannot_disrupt_actor_retirement,failed_marked_creation_emits_no_restart_for_the_rejected_installation}` |
| Public facade applications compose routing, children, timers, observation, restart, shutdown, and no-loss retry | `local_runtime::tests::reference_application_composes_every_required_runtime_leg`; `job_queue::tests::queue_retries_failures_and_accounts_graceful_drain` |
| Generated and fuzzed concurrent send orders preserve every accepted payload exactly once and permit generation-safe reuse | `adversarial_oracle::{generated_send_orders_preserve_payload_and_generation_laws,miri_supported_payload_and_reuse_composition}`; fuzz target `runtime_operations` |
| Graceful-shutdown/abort races publish one ordered terminal sequence before a distinct replacement generation | `adversarial_oracle::abort_and_shutdown_races_publish_one_terminal_generation_before_reuse` |

## Uncovered-code classification

The baseline has no unexplained bombay-owned seam. Remaining uncovered
lines fall into these stable categories:

- `Never`-parameter implementations (`NoChildren::birth` and
  `CoordinatedChild for Never`) are statically unreachable. Constructing their
  input requires a value of Bombay Behavior's uninhabited `Never` type.
- Test-only dummy protocol and endpoint methods contain uninhabited matches or
  deliberately unused no-op implementations. They support static composition
  of another oracle and are not production paths.
- The line after `pending::<()>().await` in `IncarnationEffects::next_timer` is
  unreachable by the future contract. The empty-queue behavior is exercised
  indirectly whenever an actor without timers waits for mailbox input.
- Loop continuation after a woken but not due timer is defensive against clock
  behavior; Timers owns deadline selection and Tokio owns wake timing. The
  bombay law is the externally tested no-early-fire outcome.
- Optional Behavior event constructors may decline child, peer, worker, or
  timer facts. Their `None` branches intentionally perform no delivery; the
  algebra owns construction, while bombay's observable requirement is that
  no synthetic event is minted.
- Re-panicking after the normalized Observe subject panics is the symmetric
  half of a two-domain panic guard. The detailed-subject direction is covered;
  forcing the private dependency subject to panic independently would duplicate
  Observe's panic-safety contract.

Generated registry dependencies and Rust/compiler-generated code are excluded
from this workspace report. Q3 owns concurrency/property/fuzz/Miri expansion,
Q4 owns law-inverting mutation, Q5 owns allocation/benchmarks, and Q6 owns
doctest and panic-mode gates; their absence is not classified as Q2 coverage.
