# Phase 2: laws (∀ inputs) and model-checks over supervision, layered on supervision.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/links.rs::should_restart + src/supervision.rs as read 2026-06.
#
# Grounding (src/links.rs:226-265 should_restart, src/supervision.rs):
#   * Decision order: Never break FIRST (:228) ⇒ SupervisorRestart Continue (:233) ⇒ policy match
#     (Permanent restart, Transient breaks on is_normal else restart, :238-245) ⇒ window reset if
#     now-last_restart > restart_window (:248-251) ⇒ intensity break if restart_count >= max_restarts
#     (:254-259) ⇒ else increment and Continue.
#   * Strategies (supervision.rs:232-297): OneForOne = {failed}; OneForAll = all children;
#     RestForOne = failed child + all children spawned AFTER it (younger), in spawn order.

@core @supervision @phase2
Feature: Supervision — laws over the restart decision, intensity window, and strategy restart-set
  As a supervisor of child actors under generated policies, exit kinds, and failure bursts
  I want restart, intensity, and strategy to be exact functions of their inputs
  So that no policy/exit/burst combination over- or under-restarts a child

  Background:
    Given a supervisor actor with child actors

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @lifecycle
  Scenario: a child restarts iff the decision predicate holds for every (policy x exit-kind x reason)
    Given a supervised child with any restart policy and a fresh restart count under its limit
    When the child exits via any exit kind, with the reason being normal or abnormal or SupervisorRestart
    Then should_restart returns Continue iff the decision predicate holds, else Break, for every combination
    # GEN: policy ∈ {Permanent, Transient, Never}; reason ∈ {Normal, Killed, Panicked, LinkDied,
    #      SupervisorRestart}; include the edge pairs (Never x SupervisorRestart) and
    #      (Transient x SupervisorRestart x normal-exit) explicitly.
    # ORACLE: the predicate, in source decision order (links.rs:228-245):
    #         restart = !(policy == Never) && ( reason == SupervisorRestart
    #                                            || policy == Permanent
    #                                            || (policy == Transient && !reason.is_normal()) ).
    #         Never wins even over SupervisorRestart; SupervisorRestart bypasses Permanent/Transient.
    # Generalizes: supervision.feature "RestartPolicy decides restart by how the child exited" (the
    #              9-row Outline), "SupervisorRestart bypasses the Permanent/Transient policy check
    #              entirely", "Never policy is NOT bypassed by SupervisorRestart".

  @property @sequence
  Scenario: the restarted set is exactly the strategy's defined subset for any ordered child set
    Given a supervisor with any strategy and any ordered set of children spawned in a known order
    When any one child in the set fails
    Then the restarted set equals exactly OneForOne={failed}, OneForAll=all, RestForOne=failed + younger siblings
    # GEN: strategy ∈ {OneForOne, OneForAll, RestForOne}; child count ∈ [1, 8]; failed index ∈ [0, count-1]
    #      including the FIRST child (RestForOne ⇒ whole set) and the LAST (RestForOne ⇒ only itself).
    # ORACLE: index-set functions over the spawn order: OneForOne -> {i}; OneForAll -> {0..count};
    #         RestForOne -> {i..count} (failed plus all spawned after it), preserving spawn order.
    # Generalizes: supervision.feature "OneForOne restarts only the failed child", "OneForAll restarts
    #              every child when any one fails", "RestForOne restarts the failed child plus all
    #              younger siblings, in spawn order", "RestForOne on the first child restarts the whole
    #              set", "RestForOne on the last child restarts only itself".

  # ---------------------------------------------------------------------------
  # @model — the sliding-window intensity counter under any failure burst
  # ---------------------------------------------------------------------------

  @model @lifecycle @timing @unstable-clock
  Scenario: a child stops as soon as more than max failures fall within the window, for any burst and limit
    Given a supervised child with any restart limit max over any restart_window w
    When the child fails in any timed burst of failures at generated inter-failure delays
    Then the child is restarted on a failure iff fewer than max restarts are already counted in the current window
    And it stops being restarted exactly when restart_count would reach max within the window
    # GEN: max ∈ boundary-biased u32 {0, 1, 2, 10}; w ∈ boundary-biased Duration {ZERO, 100ms, Duration::MAX};
    #      burst size ∈ [1, 12]; inter-failure delays straddle w (some < w, some > w). Paused/auto-advance
    #      clock REQUIRED (@unstable-clock) — note at wiring. Edges: max==0 ⇒ never restarts even the first
    #      time (0 >= 0); w==ZERO ⇒ count resets every failure (now-last > ZERO); w==MAX ⇒ count never resets.
    # ORACLE: a sliding-window counter mirroring links.rs:248-261 — on each failure, if elapsed > w reset
    #         count to 0; restart iff count < max, then count += 1; else Break(MaxRestartsExceeded).
    # Generalizes: supervision.feature "A child that exceeds max_restarts within the window stops being
    #              restarted", "The restart count resets after the window elapses", "restart_limit(0)
    #              means the child is never restarted", "A restart_window of ZERO resets the count on
    #              every failure", "A restart_window of Duration::MAX never resets the count", "Rapid
    #              successive restarts within the window are all counted against the same limit".
