# Driver coverage map

Coverage percentages locate unexamined code; named oracles prove laws. The
canonical per-law accounting is
[`driver-law-manifest.json`](driver-law-manifest.json). Earlier Bombay-runtime
coverage claims were removed because that adapter still targets the superseded
Engine API and cannot serve as evidence for the direct Driver.

## Current standalone evidence

| Driver-owned law or path | Primary executable evidence |
|---|---|
| Behavior activates exactly once | `driver_law::initialization_occurs_once_across_stop_closure_and_failure_boundaries` |
| One accepted event produces one exclusive fold | `driver_law::{every_accepted_event_is_folded_exactly_once,at_most_one_behavior_fold_is_active}` |
| Initialization actions precede first input | `driver_law::universal_causal_transcript_has_no_prefetch_or_reentrancy` |
| Complete actions cross once before later input | `driver_law::{every_successful_decision_is_committed_exactly_once,complete_move_only_stop_actions_cross_once_before_completion}` |
| Partial application remains factual and is not rolled back | `driver_law::{commit_failure_preserves_the_factual_committed_prefix,commitment_failure_does_not_roll_back_the_successful_fold}` |
| Self-send and capability results re-enter as later events | `driver_law::{self_send_reenters_only_as_a_later_ordinary_event,local_commitment_advances_only_through_a_later_capability_event}` |
| Stop and source exhaustion remain distinct successful facts | `driver_law::completion_preserves_stop_and_input_exhaustion_as_success` |
| Every ordinary return attempts retirement exactly once | `driver_law::every_ordinary_return_attempts_retirement_exactly_once` |
| Panic and cancellation expose no recovery or false completion | `driver_law::{initialization_and_turn_panics_consume_the_only_execution_and_cannot_poll_again,cancellation_at_every_await_drops_ownership_without_false_completion_or_retirement}` |
| Closed custom Behaviors and environments remain statically substitutable | `driver_law::{unrelated_custom_behavior_shapes_use_the_same_driver_algorithm,one_behavior_is_substitutable_across_distinct_static_environments}` plus compile fixtures |
| Deterministic replay matches the causal model | all `driver_property` tests |
| Every deliberate standalone semantic inversion is killed | all `driver_inversions` tests |
| Construction and one complete turn allocate nothing | `driver_allocation::one_complete_driver_execution_allocates_nothing` |
| One direct production path and tight authority surface | `law_manifest::{repository_has_one_direct_driver_path_and_no_obsolete_product_api,structural_oracle_kills_surface_and_authority_inversions}` |

## Core integration evidence

The real Bombay core incarnation now has direct executable evidence for exact
successful/error classification, panic, cancellation, exactly-once retirement,
Driver-resource drop before terminal handoff, structural lifecycle minimality,
and deliberate inversion killing in `core::incarnation::tests`. Transactional
local activation is proven by `core::local::tests`; exact address lease and
observation-generation ordering is proven by `core::generation::tests`. All six
`D-INC-*` rows are passing without introducing a System, mailbox, scheduler, or
application-facing lifecycle layer.

The private ML1 local slice adds seven focused tests over the real Communication
and Address crates: commit-before-publication with exact application counts,
failed-activation cleanup, wrapper-safe typed user injection, rejected-message
recovery after retirement, and continued control delivery after user-lane
closure. These prove only the one-generation environment boundary and add no
System, executor, handle, timer, observation-input, or child claim.

LR1 adds focused real-runtime tests proving post-commit reference handoff,
typed origin/message delivery, exact rejected-message recovery, failed-commit
non-publication, collision preservation, task execution, and lease release. A
public integration test shows the complete `LocalActors::spawn` plus
`ActorRef::send` user path without a System or public interpreter trait.

The canonical 50-test Behavior Actors scenario source is shared by Engine and
core rather than copied. Engine executes it through its minimal conformance
environment; core executes the same scenarios through real Communication
queues, Address claim/release, `LocalEnvironment`, complete-action commitment,
Driver, Incarnation classification, and active retirement. This includes every selected named template,
all reviewed wrapper edges in both orders, the maximal wrapper stack, and the
typed child-intent templates.

The Behavior Actors inventory is checked against its pinned revision and every
listed template has concrete direct-Driver execution. The closed wrapper family
is exercised in both meaningful orders and as a maximal stack. Unsupported
compositions remain compile-time rejections, including the open-phase Stash
fixture; the Driver does not weaken those owner-defined bounds.
