# Scope: bombay local core `Links` (src/links.rs) — the link/notification machinery
#        underneath supervision. A `Links` is the per-actor registry of:
#          * parent        — the supervisor (if supervised), notified with mailbox_rx
#                            so it can restart us;
#          * sibblings     — peer-linked actors, notified WITHOUT mailbox_rx (no restart);
#          * children      — actors we supervise, each an `ErasedChildSpec`.
#        It also carries the `parent_shutdown` AtomicBool that prevents the death-watch
#        deadlock during the supervisor's final `shutdown_children` wait.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature EXACTLY):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If it cannot be stated
#     without reading the impl, write it as `# NOTE:` + @review-semantics, never a guess.
#   * Facts only — every Then is grounded in src/links.rs as read 2026-06.
#   * No step definitions here; steps are written in the wiring phase.
#
# Complements tests/supervision_mailbox.rs (issue #335 regression: pending mailbox
# messages survive a supervised restart) — those cover the happy restart path end to end;
# the scenarios here pin the LINK-LEVEL guarantees (who is notified, with/without
# mailbox_rx, and the parent_shutdown ordering) that that behaviour rests on.

@core @links
Feature: Links — death notification, restart hand-off, and shutdown-deadlock prevention
  As an actor with parent/sibling/child links
  I want a dying actor to notify exactly the right linked actors with exactly the right payload
  So that supervised children can be restarted, peers are observed, and a parent shutdown never deadlocks

  # ---------------------------------------------------------------------------
  # @sequence — multi-step: link → die → observe who was notified and with what
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: A supervised child notifies its parent with its mailbox_rx so the parent can restart it
    Given a supervisor actor with a supervised child "worker"
    And the child's "parent_shutdown" flag is false
    When the child stops abnormally
    Then the parent's Link is notified of the child's death
    And the notification carries the child's mailbox_rx
    And the notification carries the child's sibling links
    # src/links.rs:113-122 — supervised normal path: notify(parent, id, reason,
    # Some(mailbox_rx), Some(sibblings)); the parent uses mailbox_rx to restart.

  @sequence
  Scenario: An unsupervised actor with sibling links notifies every sibling on death
    Given an unsupervised actor "a" linked to siblings "b", "c" and "d"
    When actor "a" dies with reason Killed
    Then siblings "b", "c" and "d" each receive exactly one on_link_died for "a"
    And no sibling receives a mailbox_rx in its notification
    # src/links.rs:124-128 + notify_sibblings:132-142 — None parent ⇒ notify_sibblings,
    # which drains all siblings and calls link.notify(.., None, None) for each.

  @sequence
  Scenario: Siblings are drained on notify and are not notified a second time
    Given an unsupervised actor "a" linked to sibling "b"
    When actor "a" dies
    And the same notify_links pass is somehow re-driven
    Then sibling "b" receives the on_link_died notification exactly once
    # src/links.rs:106 mem::take(&mut self.sibblings) and :135 .drain() empty the map,
    # so a second pass has no siblings left to notify — death notification is once-only.

  @sequence
  Scenario: A parent shuts its children down in the order set-flag → send-shutdown → wait-closed
    Given a supervisor actor with supervised children "c1", "c2" and "c3"
    When the supervisor performs its final shutdown
    Then it first calls set_children_parent_shutdown so every child reads the flag true
    And it then calls send_children_shutdown, invoking each child's shutdown closure exactly once
    And it finally calls wait_children_closed, which resolves only after every child mailbox is closed
    # src/links.rs:47-78 — the three steps in order: set the Release flag (:50) so any
    # independent child exit drops mailbox_rx, fire every child's `shutdown` closure
    # (:54-65 join_all), then await every child's `signal_mailbox.closed()` (:67-78 join_all).
    # The flag-first ordering is what makes the final wait deadlock-free.

  # ---------------------------------------------------------------------------
  # @lifecycle — the parent_shutdown flag across set / load / restart-reset
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: set_children_parent_shutdown sets parent_shutdown on every supervised child
    Given a supervisor actor with supervised children "c1", "c2" and "c3"
    When the supervisor calls set_children_parent_shutdown
    Then each child's "parent_shutdown" flag reads true
    # src/links.rs:47-52 — iterates inner.children.values(), stores true (Release) on each.

  @lifecycle
  Scenario: send_children_shutdown invokes every child's shutdown closure once
    Given a supervisor actor with supervised children "c1", "c2" and "c3"
    When the supervisor calls send_children_shutdown
    Then each of the three children's shutdown closures is invoked exactly once
    # src/links.rs:54-65 — snapshots the children's `shutdown` closures under the lock, drops
    # the lock, then join_all-awaits all closures. Cloning the closures before awaiting avoids
    # holding the Links mutex across the awaits.

  @lifecycle
  Scenario: wait_children_closed resolves only after every child mailbox has closed
    Given a supervisor with supervised children "c1" and "c2", both still draining
    When the supervisor calls wait_children_closed
    Then the call does not resolve while either child mailbox is still open
    And it resolves once both "c1" and "c2" mailboxes are closed
    # src/links.rs:67-78 — snapshots each child's `signal_mailbox` under the lock, then
    # join_all-awaits `closed()` on each. Combined with the parent_shutdown flag, an
    # independently-exiting child closes its own channel (dropping mailbox_rx) so this wait
    # cannot deadlock on a message queued into the non-processing parent.

  @boundary
  Scenario: A parent with no children shuts down its (empty) child set without blocking
    Given a supervisor actor with no supervised children
    When the supervisor calls send_children_shutdown and then wait_children_closed
    Then both calls complete immediately without awaiting anything
    # src/links.rs:54-78 — both snapshot an empty children map, so join_all over an empty
    # iterator resolves immediately; the zero-children boundary never blocks.

  @lifecycle
  Scenario: A child exiting after parent_shutdown is set drops its mailbox_rx instead of queuing it to the parent
    Given a supervisor actor with a supervised child "worker"
    And the supervisor has called set_children_parent_shutdown
    When the child exits independently after the flag is set
    Then the parent is notified with no mailbox_rx and no siblings
    And the child's mailbox_rx is dropped so its channel closes
    # src/links.rs:107-112 — parent_shutdown.load(Acquire)==true ⇒ notify(.., None, None).
    # Dropping mailbox_rx lets the parent's mailbox.closed() wait in shutdown_children
    # resolve; queuing it would deadlock (parent is not processing its mailbox).

  @lifecycle
  Scenario: The Release store then Acquire load is the ordering that prevents the death-watch deadlock
    Given a supervisor about to enter shutdown_children
    When set_children_parent_shutdown stores true with Release ordering
    And a child later loads parent_shutdown with Acquire ordering before notifying
    Then the child observes true and takes the drop-mailbox_rx branch
    # src/links.rs:50 (Release) happens-before :107 (Acquire) — the store is visible to any
    # child that loads after it. This pairing is the documented deadlock-prevention invariant.

  @lifecycle
  Scenario: A restarted child instance does not inherit a stale parent_shutdown=true flag
    Given a supervised child whose parent_shutdown was previously set to true
    When the child is restarted via its spawn factory
    Then the restarted instance reads parent_shutdown as false
    And its stale children entries from the previous instance are cleared
    # src/supervision.rs:694-699 — factory clears inner.children and stores false (Release)
    # on parent_shutdown before re-spawning, so the new instance starts clean.

  # ---------------------------------------------------------------------------
  # @boundary — degenerate links, dead targets, no-ops
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Linking an actor to itself is a no-op
    Given a running actor "a"
    When actor "a" is linked to itself
    Then linking a→a is silently ignored and a's real links are unaffected
    # The guard exists: self-link returns early at src/actor/actor_ref.rs:890-892
    # (`if self.id == sibling_ref.id { return; }`).

  @boundary
  Scenario: Notifying a link whose target actor is already dead is swallowed
    Given an unsupervised actor "a" linked to sibling "b"
    And sibling "b" has already stopped
    When actor "a" dies and notifies "b"
    Then the notify completes without surfacing an error
    # src/links.rs:174-185 — Link::Local swallows SendError::ActorNotRunning silently;
    # other SendError variants are only logged (under the tracing feature), never returned.

  @boundary
  Scenario: An actor with no links at all dies without notifying anyone
    Given an unsupervised actor "a" with no parent and no siblings
    When actor "a" dies
    Then no on_link_died notification is produced
    # src/links.rs:124 None parent + empty sibblings map ⇒ notify_sibblings drains nothing.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent flag-set vs. child notify; simultaneous deaths
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: set_children_parent_shutdown racing a child's own notify never queues mailbox_rx after the flag
    Given a supervisor with a supervised child "worker"
    When set_children_parent_shutdown and the child's independent exit run concurrently
    Then the child either notifies with mailbox_rx BEFORE the flag was visible, or drops it AFTER
    And the child never queues mailbox_rx into the parent once it has observed the flag as true
    # Acquire/Release (src/links.rs:50/:107) linearizes the two: once the child's Acquire
    # load observes the Release store, it MUST take the drop branch. This is the precise
    # race the flag exists to make safe — no interleaving may deadlock the parent.

  @linearizability
  Scenario: Two supervised children dying simultaneously each notify the parent independently
    Given a supervisor with supervised children "c1" and "c2"
    When "c1" and "c2" both stop abnormally at the same time
    Then the parent receives one death notification for "c1" and one for "c2"
    And each notification carries that child's own mailbox_rx
    # src/links.rs:98-129 notify_links is per-actor; each child's run-loop drives its own
    # notify, so simultaneous deaths produce two independent, non-interfering notifications.

  @linearizability
  Scenario: Sibling fan-out delivers to all N siblings under concurrent death
    Given an unsupervised actor "hub" linked to N siblings
    When "hub" dies while the siblings are concurrently processing other messages
    Then every one of the N siblings receives exactly one on_link_died for "hub" with no loss or duplication
    # src/links.rs:132-142 — FuturesUnordered over the drained sibling map; each spawned
    # notify is independent, so concurrent sibling activity must not drop a notification.
