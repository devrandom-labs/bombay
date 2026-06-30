# Scope: bombay local core supervision (src/supervision.rs) — Erlang-style supervision:
#        RestartPolicy (Permanent/Transient/Never), SupervisionStrategy
#        (OneForOne/OneForAll/RestForOne), restart-intensity limits with a sliding window,
#        and the `should_restart` decision on EresedChildSpec (src/links.rs:226-265).
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature EXACTLY):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee.
#   * Facts only — grounded in src/supervision.rs + src/links.rs::should_restart as read 2026-06.
#   * No step definitions here; steps are written in the wiring phase.
#
# src/supervision.rs already has 29 in-file #[tokio::test]s. The scenarios below
# FORMALIZE the covered behaviour (policy x exit-kind matrix, the three strategies)
# AND target the GAPS those tests miss, called out inline with `# GAP:`:
#   * SupervisorRestart bypasses policy (Permanent/Transient) but NOT Never.
#   * restart_limit(0) ⇒ never restarts, not even the first time (0 >= 0).
#   * restart_window == ZERO and == MAX edges.
#   * on_start failing DURING a restart.
#   * concurrent OneForAll cascade — child2 crashing mid child1-restart.
#   * RestForOne younger-sibling ordering as an explicit ordered set.

@core @supervision
Feature: Supervision — restart policy, strategy, intensity limits, and coordinated restarts
  As a supervisor of child actors
  I want failed children restarted according to their policy, strategy, and intensity limit
  So that the system self-heals without restart storms or inconsistent partial recovery

  Background:
    Given a supervisor actor with default restart limit 5 restarts per 5 seconds

  # ---------------------------------------------------------------------------
  # @lifecycle — RestartPolicy x exit-kind matrix (panic / error / normal)
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario Outline: RestartPolicy decides restart by how the child exited
    Given a supervised child with restart policy "<policy>"
    When the child exits via "<exit_kind>"
    Then the child is restarted: <restarts>
    # src/links.rs::should_restart:226-265 + src/error.rs::is_normal:380.
    # Permanent: always. Transient: only abnormal (panic/error), not Normal.
    # Never: never (the Never break at :228 precedes everything).

    Examples:
      | policy    | exit_kind | restarts |
      | Permanent | panic     | yes      |
      | Permanent | error     | yes      |
      | Permanent | normal    | yes      |
      | Transient | panic     | yes      |
      | Transient | error     | yes      |
      | Transient | normal    | no       |
      | Never     | panic     | no       |
      | Never     | error     | no       |
      | Never     | normal    | no       |

  @lifecycle
  Scenario: A child restarted by the supervisor re-runs on_start and increments its start count
    Given a supervised child with restart policy "Permanent"
    When the child panics once
    Then on_start runs again and the child is alive afterwards
    # src/supervision.rs:701-707 — restart re-runs PreparedActor::spawn ⇒ on_start fires.

  # ---------------------------------------------------------------------------
  # @sequence — SupervisionStrategy: which siblings are restarted, in what set
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: OneForOne restarts only the failed child
    Given a "OneForOne" supervisor with children "c1" and "c2"
    When "c1" panics
    Then "c1" is restarted
    And "c2" is not restarted

  @sequence
  Scenario: OneForAll restarts every child when any one fails
    Given a "OneForAll" supervisor with children "c1", "c2" and "c3"
    When "c1" panics
    Then "c1", "c2" and "c3" are all restarted exactly once

  @sequence
  Scenario: RestForOne restarts the failed child plus all younger siblings, in spawn order
    Given a "RestForOne" supervisor with children spawned in order "c1", "c2", "c3"
    When "c2" panics
    Then "c1" is not restarted
    And "c2" and "c3" are restarted
    And the restarted set is exactly the failed child and the children spawned after it
    # src/supervision.rs strategy docs:277-297; existing tests cover first/middle/last child.

  @sequence
  Scenario: RestForOne on the first child restarts the whole set
    Given a "RestForOne" supervisor with children spawned in order "c1", "c2", "c3"
    When "c1" panics
    Then "c1", "c2" and "c3" are all restarted

  @sequence
  Scenario: RestForOne on the last child restarts only itself
    Given a "RestForOne" supervisor with children spawned in order "c1", "c2", "c3"
    When "c3" panics
    Then only "c3" is restarted

  # ---------------------------------------------------------------------------
  # @lifecycle — restart-intensity limit + sliding window
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: A child that exceeds max_restarts within the window stops being restarted
    Given a supervised child with restart limit 2 restarts per 10 seconds
    When the child panics 3 times within the window
    Then the child is restarted exactly twice and not a third time
    # src/links.rs:254-259 — restart_count >= max_restarts ⇒ Break(MaxRestartsExceeded).

  @lifecycle
  Scenario: Exceeding the limit breaks with MaxRestartsExceeded carrying the exact counts
    Given a supervised child with restart limit 2 restarts per 10 seconds
    When should_restart is consulted on the failure that exceeds the limit
    Then it returns Break(NoRestartReason::MaxRestartsExceeded) carrying restart_count 2 and max_restarts 2
    # src/links.rs:254-258 — the Break payload carries { restart_count, max_restarts } with the
    # current counts; pins the carried values, not just that a Break occurs. The existing
    # intensity scenario asserts the count of restarts but never the payload contents.

  @lifecycle
  Scenario: A within-limit restart increments restart_count and stamps last_restart
    Given a supervised child with restart limit 5 per 10 seconds and restart_count 0
    When should_restart is consulted on an abnormal exit within the window
    Then it returns Continue and the child's restart_count is now 1
    And last_restart is updated to the time of this consultation
    # src/links.rs:261-262 — on the restart path should_restart does restart_count += 1 and
    # last_restart = now (after the window-reset check at :249-250 and the limit check at :254).
    # No scenario asserts this post-state mutation directly today.

  @lifecycle @timing @unstable-clock
  Scenario: The restart count resets after the window elapses
    Given a supervised child with restart limit 2 restarts per 100 milliseconds
    When the child panics twice within the window
    And the window elapses
    And the child panics once more
    Then the final panic restarts the child because the count had reset
    # src/links.rs:248-251 — now - last_restart > restart_window ⇒ restart_count = 0.

  @lifecycle
  Scenario: The default restart limit is 5 restarts per 5 seconds
    Given a supervised child spawned without calling restart_limit
    Then its max_restarts is 5 and its restart_window is 5 seconds
    # src/supervision.rs:363-365 / :376-378 — builder defaults.

  # ---------------------------------------------------------------------------
  # @boundary — should_restart edge cases the 29 in-file tests DO NOT cover
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: SupervisorRestart bypasses the Permanent/Transient policy check entirely
    Given a supervised child with restart policy "Transient"
    And the child would normally NOT restart on a normal exit
    When the supervisor initiates a SupervisorRestart for coordination
    Then the child is restarted regardless of its normal-exit policy
    # GAP: not covered by the 29 in-file tests.
    # src/links.rs:232-235 — SupervisorRestart returns Continue BEFORE the policy match,
    # so a coordinating OneForAll/RestForOne restart is forced even for a Transient child
    # that exited normally.

  @boundary
  Scenario: Never policy is NOT bypassed by SupervisorRestart
    Given a supervised child with restart policy "Never"
    When the supervisor initiates a SupervisorRestart for coordination
    Then the child is still not restarted
    # GAP: not covered by the 29 in-file tests.
    # src/links.rs:228-230 — the Never break precedes the SupervisorRestart check at :233,
    # so Never wins even over a coordinator-initiated restart.

  @boundary
  Scenario: restart_limit(0) means the child is never restarted, not even the first time
    Given a supervised child with restart limit 0 restarts per 10 seconds
    When the child panics once
    Then the child is not restarted
    # GAP: not covered. src/links.rs:254 — restart_count(0) >= max_restarts(0) is true on
    # the very first failure ⇒ Break(MaxRestartsExceeded { 0, 0 }). Zero == disabled.

  @boundary @timing @unstable-clock
  Scenario: A restart_window of ZERO resets the count on every failure so each one counts as the first
    Given a supervised child with restart limit 1 restart per 0 seconds
    When the child panics, is restarted, then panics again
    Then the child is restarted on each failure because the window never holds the count
    # GAP: not covered. src/links.rs:249 — now - last_restart > ZERO is true for any
    # elapsed time, so restart_count resets to 0 before the >= max_restarts check on every
    # subsequent failure. NOTE @review-semantics: confirm whether a ZERO window is intended
    # to mean "unlimited" — pin at wiring time.

  @boundary @timing @unstable-clock
  Scenario: A restart_window of Duration::MAX never resets the count
    Given a supervised child with restart limit 2 restarts per Duration::MAX
    When the child panics 3 times
    Then the child is restarted exactly twice and never again
    # GAP: not covered. src/links.rs:249 — now - last_restart can never exceed MAX, so the
    # count never resets; the limit behaves as a lifetime cap.

  @boundary
  Scenario: A child whose on_start fails during a restart
    Given a supervised child with restart policy "Permanent" whose on_start fails on restart
    When the child panics and the supervisor attempts to restart it
    Then the restart's on_start failure is surfaced via the OnStart panic reason
    # GAP: not covered. src/error.rs PanicReason::OnStart:687. NOTE @review-semantics:
    # whether a failed-on_start restart re-enters should_restart (and thus consumes another
    # restart slot) is an OPEN invariant — pin against the run-loop at wiring time.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent cascades, real overlap
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: A second child crashing mid OneForAll cascade does not double-restart or lose a child
    Given a "OneForAll" supervisor with children "c1" and "c2"
    When "c1" panics, triggering a OneForAll restart of both
    And "c2" panics again while the cascade restart is still in flight
    Then every child ends up alive and restarted, with no child left dead or restarted out of band
    # GAP: not covered. src/supervision.rs OneForAll coordinates via SupervisorRestart
    # (src/links.rs:233). NOTE @review-semantics: the exact restart_count accounting under
    # an overlapping second crash is an OPEN invariant — assert liveness here, pin the
    # precise count at wiring time.

  @linearizability
  Scenario: Independent failures of two OneForOne children restart each exactly once with no cross-talk
    Given a "OneForOne" supervisor with children "c1" and "c2"
    When "c1" and "c2" panic concurrently
    Then "c1" is restarted exactly once and "c2" is restarted exactly once
    And neither restart is attributed to the other child
    # src/supervision.rs OneForOne isolation; complements the in-file sequential test with
    # real concurrency.

  @linearizability @timing @unstable-clock
  Scenario: Rapid successive restarts within the window are all counted against the same limit
    Given a supervised child with restart limit 10 restarts per 10 seconds
    When the child panics 5 times in rapid succession within the window
    Then the child is restarted 5 times and the restart count reflects all 5
    # src/links.rs:248-261 — within-window failures accumulate restart_count monotonically.
