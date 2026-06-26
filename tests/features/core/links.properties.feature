# Phase 2: laws (∀ inputs) and model-checks over Links, layered on links.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/links.rs as read 2026-06. No step definitions.
#
# Grounding (src/links.rs):
#   * On death, notify_sibblings DRAINS the sibling map (.drain(), :135) and notifies each link
#     once with (None mailbox_rx, None siblings) (:132-142). mem::take empties siblings (:106) so a
#     re-driven pass has nothing left ⇒ once-only.
#   * parent_shutdown: set with Release (:50), loaded with Acquire (:107). Once observed true, the
#     child takes the drop-mailbox_rx branch (notify(.., None, None), :112) instead of queuing it.

@core @links @phase2
Feature: Links — laws over sibling fan-out exactness and the parent_shutdown Release/Acquire ordering
  As an actor with parent/sibling/child links
  I want each linked sibling notified exactly once and the shutdown flag to linearize the drop
  So that no link count or interleaving loses, duplicates, or deadlocks a death notification

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: on one actor's death each of its N linked siblings gets exactly one on_link_died
    Given an unsupervised actor "a" linked to any N distinct siblings
    When actor "a" dies with any stop reason
    Then each of the N siblings receives exactly one on_link_died for "a", with no mailbox_rx
    And no sibling receives zero notifications and none receives two, for any N
    # GEN: N ∈ boundary-biased usize {0, 1, 2, 16, 256}; stop reason ∈ {Killed, Panicked, LinkDied, Normal}.
    #      notify_sibblings drains the map and notifies each surviving link once (links.rs:132-142).
    # ORACLE: a per-sibling delivery counter; the histogram of on_link_died-per-sibling is all 1s
    #         (and all 0s when N == 0). Each notify carries None mailbox_rx (sibling = no restart).
    # Generalizes: links.feature "An unsupervised actor with sibling links notifies every sibling on death",
    #              "Siblings are drained on notify and are not notified a second time",
    #              "An actor with no links at all dies without notifying anyone",
    #              "Sibling fan-out delivers to all N siblings under concurrent death".

  @property @lifecycle
  Scenario: a parent shutting down fires exactly one shutdown per child and waits for exactly those mailboxes
    Given a supervisor with any K supervised children
    When the supervisor calls send_children_shutdown then wait_children_closed
    Then each of the K children's shutdown closures is invoked exactly once
    And wait_children_closed resolves exactly when all K child mailboxes are closed, and immediately when K == 0
    # GEN: K ∈ boundary-biased usize {0, 1, 2, 16}; vary which children close before vs after the wait begins.
    #      send_children_shutdown snapshots and join_alls the children's shutdown closures (links.rs:54-65);
    #      wait_children_closed snapshots and join_alls each signal_mailbox.closed() (:67-78).
    # ORACLE: a per-child {shutdown-called-count, mailbox-closed} model — the shutdown histogram is K ones
    #         (0 when K==0); the wait future is pending iff ∃ an open child mailbox, resolved once all closed.
    # Generalizes: links.feature "send_children_shutdown invokes every child's shutdown closure once",
    #              "wait_children_closed resolves only after every child mailbox has closed",
    #              "A parent with no children shuts down its (empty) child set without blocking".

  # ---------------------------------------------------------------------------
  # @model — the parent_shutdown Release/Acquire ordering under any interleaving
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: the parent_shutdown Release/Acquire law — no mailbox_rx is queued once the flag is observed true
    Given a supervisor with a supervised child "worker"
    When set_children_parent_shutdown and the child's independent exit run concurrently, under any interleaving
    Then the child either notifies the parent WITH mailbox_rx before the flag is visible, or drops it AFTER
    And once the child's Acquire load observes the Release store as true, it never queues mailbox_rx to the parent
    # GEN: interleave the store (links.rs:50, Release) against the child's load-then-notify (:107 Acquire,
    #      :112 drop branch / :115 queue branch) across all orderings — small cases exhaustively (loom),
    #      larger via randomized scheduling. Include both "store first" and "load first" boundaries.
    # ORACLE: a single AtomicBool model; the legal outcomes are exactly {queue mailbox_rx while flag==false}
    #         ∪ {drop mailbox_rx while flag==true}. The forbidden state — queue AFTER observing true —
    #         must never occur (it is the death-watch deadlock the flag exists to prevent).
    # Generalizes: links.feature "set_children_parent_shutdown racing a child's own notify never queues
    #              mailbox_rx after the flag", "A child exiting after parent_shutdown is set drops its
    #              mailbox_rx instead of queuing it to the parent", "The Release store then Acquire load
    #              is the ordering that prevents the death-watch deadlock".

  @model @linearizability
  Scenario: K supervised children dying simultaneously each notify the parent exactly once, independently
    Given a supervisor with any K supervised children, parent_shutdown false
    When all K children stop abnormally at the same time with real overlap
    Then the parent receives exactly one death notification per child, each carrying that child's own mailbox_rx
    And no child's notification is lost, duplicated, or attributed to another child, for any K
    # GEN: K ∈ [2, 16]. Real overlap via tokio::spawn + Barrier. notify_links is per-actor and each
    #      child's run-loop drives its own notify (links.rs:98-129), so the K notifications are independent.
    # ORACLE: a per-child delivery counter keyed by child id; the histogram is exactly K ones, each paired
    #         with the matching child's mailbox_rx (Some, since flag==false).
    # Generalizes: links.feature "Two supervised children dying simultaneously each notify the parent
    #              independently".
