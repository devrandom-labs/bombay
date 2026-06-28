# Scope: kameo core ActorRef and its weak/recipient variants
#        (src/actor/actor_ref.rs) — ask/tell messaging, the alive/dead state
#        machine, strong/weak reference counting, downgrade/upgrade, is_current,
#        identity (id/eq/hash/ord), startup/shutdown waiters, Recipient and
#        ReplyRecipient type-erasure, and self link/unlink no-ops.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature):
#   * Every Scenario carries exactly ONE cross-cutting tag.
#   * Invariant-first: the Then names the observable guarantee; unverified
#     guarantees are `# NOTE:` + @review-semantics, never asserted guesses.
#   * Facts only: every Then is confirmed from the source above.

@core @actors @actor_ref
Feature: ActorRef — messaging, reference counting, identity, lifecycle waiters
  As a holder of an ActorRef
  I want messaging, refcounting, and identity to behave deterministically
  So that I can interact with an actor safely across its lifetime

  Background:
    Given a running actor spawned with a default bounded mailbox

  # ---------------------------------------------------------------------------
  # @sequence — ask/tell protocol, waiter ordering
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: ask sends a message and returns the handler's reply
    Given the actor replies with the doubled value of any number it receives
    When the caller asks the actor with 21
    Then the awaited reply is 42
    # Confirmed: ActorRef::ask builds an AskRequest awaited for the reply
    # (actor_ref.rs:810-824).

  @sequence
  Scenario: tell delivers a message without producing a reply
    Given the actor records every message it handles
    When the caller tells the actor a message and awaits the send
    Then the send resolves with Ok and the actor eventually records the message
    # Confirmed: ActorRef::tell builds a fire-and-forget TellRequest
    # (actor_ref.rs:856-867).

  @sequence
  Scenario: wait_for_startup returns only after on_start has finished
    Given an actor whose on_start blocks until released
    When wait_for_startup is awaited and then on_start is released
    Then wait_for_startup resolves only after on_start completes
    # Confirmed: wait_for_startup awaits the startup_result SetOnce, set after
    # on_start (actor_ref.rs:514-517; spawn.rs:413).

  @sequence
  Scenario: wait_for_shutdown returns only after the actor has stopped
    Given a running actor
    When the actor is stopped gracefully and wait_for_shutdown is awaited
    Then wait_for_shutdown resolves only after the mailbox has closed
    # Confirmed: wait_for_shutdown awaits mailbox_sender.closed()
    # (actor_ref.rs:619-622).

  # ---------------------------------------------------------------------------
  # @lifecycle — alive state machine, downgrade/upgrade, recipients
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: is_alive is true while running and false after the actor stops
    Given a running actor
    When is_alive is checked before stopping
    And the actor is stopped and shutdown awaited
    And is_alive is checked again
    Then the first check is true and the second is false
    # Confirmed: ActorRef::is_alive is !mailbox_sender.is_closed()
    # (actor_ref.rs:88-90).

  @lifecycle @review-semantics
  Scenario: WeakActorRef::is_alive uses a different predicate than ActorRef::is_alive
    Given a running actor with both a strong ActorRef and a WeakActorRef
    When the actor is stopping but its shutdown result has not yet been recorded
    Then ActorRef::is_alive becomes false as soon as the mailbox closes
    And WeakActorRef::is_alive stays true until the shutdown result is initialized
    And once shutdown completes both predicates report not-alive
    # NOTE (actor_ref.rs:88-90 vs :2133-2134): ActorRef::is_alive = !mailbox_sender.is_closed();
    # WeakActorRef::is_alive = !shutdown_result.initialized(). These are DIFFERENT signals, so
    # during the close→shutdown-recorded window they can disagree. @review-semantics: the exact
    # width/observability of that window needs a paused clock + an instrumented shutdown at
    # wiring; the invariant asserted is only the predicate identities and that both converge to
    # not-alive after shutdown.

  @lifecycle
  Scenario: A WeakActorRef can be upgraded while a strong ActorRef remains
    Given a WeakActorRef downgraded from a live ActorRef that is still held
    When the WeakActorRef is upgraded
    Then upgrade returns Some(ActorRef)
    # Confirmed: WeakActorRef::upgrade returns Some while a strong mailbox
    # sender exists (actor_ref.rs:2140-2149).

  @lifecycle
  Scenario: A WeakActorRef cannot be upgraded after every strong ref is dropped
    Given a WeakActorRef downgraded from an ActorRef
    When all strong ActorRefs are dropped
    Then upgrading the WeakActorRef returns None
    # Confirmed: upgrade maps on mailbox_sender.upgrade(), which is None once
    # all strong senders are gone (actor_ref.rs:2141).

  @lifecycle
  Scenario: A Recipient type-erases the actor but still delivers tells
    Given a Recipient created from the actor via recipient
    When a message is told through the Recipient
    Then the underlying actor handles the message
    And the Recipient reports the same ActorId as the source ActorRef
    # Confirmed: Recipient wraps the ActorRef as a MessageHandler and forwards
    # id()/tell (actor_ref.rs:1579-1675).

  @lifecycle
  Scenario: A ReplyRecipient supports both ask and tell and can erase its reply capability
    Given a ReplyRecipient created from the actor via reply_recipient
    When the caller asks through the ReplyRecipient
    Then a reply is returned
    And erase_reply yields a Recipient with the same ActorId
    # Confirmed: ReplyRecipient exposes ask/tell and erase_reply upcasts to a
    # Recipient preserving id (actor_ref.rs:1400-1521, 1417-1421).

  # ---------------------------------------------------------------------------
  # @boundary — send-to-dead, is_current, self link/unlink, refcounts
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: Telling a stopped actor fails with ActorNotRunning
    Given an actor that has been stopped and whose shutdown has been awaited
    When a message is told to the stopped actor
    Then the send fails with SendError::ActorNotRunning
    # Confirmed: stop_gracefully on a closed mailbox maps to
    # ActorNotRunning (actor_ref.rs:248-253); SendError variant exists at
    # error.rs:91.

  @boundary
  Scenario: is_current is false when checked from outside the actor's task
    Given a running actor
    When is_current is called from the spawning task
    Then is_current returns false
    # Confirmed: is_current compares CURRENT_ACTOR_ID task-local to self.id and
    # returns false when the task-local is unset (actor_ref.rs:236-241).

  @boundary
  Scenario: is_current is true when checked from inside the actor's own handler
    Given an actor that calls actor_ref.is_current inside a message handler
    When that handler runs
    Then is_current returns true within the handler
    # Confirmed: the run loop is entered under CURRENT_ACTOR_ID.scope(self.id)
    # (spawn.rs:149,177), so the task-local matches self.id.

  @boundary
  Scenario: Linking an actor to itself is a no-op
    Given a single running actor
    When the actor's ActorRef is linked to itself
    Then no link is recorded and no error occurs
    # Confirmed: link returns early when self.id == sibling_ref.id
    # (actor_ref.rs:890-892).

  @boundary
  Scenario: Unlinking an actor from itself is a no-op that leaves existing links intact
    Given a running actor A already linked to a different actor B
    And A's link set therefore has length 1
    When A's ActorRef is unlinked from itself
    Then A's link set still has length 1 and still contains B
    And no error occurs
    # Confirmed: unlink returns immediately when self.id == sibling_ref.id, BEFORE acquiring
    # self.links / sibling.links (actor_ref.rs:1073-1076) — so no entry is added or removed and
    # any pre-existing link is left untouched. (links is Arc<Mutex<LinksInner>>, links.rs:38.)
    # The length-unchanged assertion is the concrete observable for "nothing changes".

  @boundary
  Scenario: strong_count and weak_count track live handles
    Given a freshly spawned actor with exactly one strong ActorRef
    When the ActorRef is cloned once and then downgraded once
    Then strong_count is 2 and the weak count increased by exactly one
    # Confirmed: strong_count/weak_count delegate to the MailboxSender Arc-style
    # counters (actor_ref.rs:220-230). clone bumps strong (1 -> 2, absolute).
    # kameo's spawn machinery retains internal WeakSenders (spawn.rs:211,215), so
    # weak_count is NOT 0 at rest; downgrade adds exactly one more over that
    # at-rest baseline (actor_ref.rs:198-207, 1329-1340).

  @boundary
  Scenario: Two ActorRefs to the same actor are equal and hash equally
    Given an ActorRef and a clone of it
    When they are compared and hashed
    Then they are equal and produce the same hash
    # Confirmed: PartialEq/Hash/Ord are defined purely on id
    # (actor_ref.rs:1363-1387).

  @boundary
  Scenario: ActorRefs to different actors are not equal
    Given ActorRefs to two distinct actors
    When they are compared
    Then they are not equal
    # Confirmed: equality is id-based and ActorIds are distinct per actor
    # (actor_ref.rs:1363-1367; id.rs:72-79).

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent senders and waiters
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent tells from many tasks are all delivered
    Given an actor that counts every message it handles
    When 100 messages are told concurrently from 10 tasks that start at a barrier
    Then the actor's final count is exactly 100
    # Confirmed: every tell enqueues one Signal::Message into the shared
    # mailbox; no message is dropped while the actor is alive (mailbox is the
    # single serialization point).

  @linearizability
  Scenario: Concurrent asks each receive their own correct reply
    Given an actor that echoes back the number it is asked
    When 50 distinct numbers are asked concurrently from tasks started at a barrier
    Then each caller receives exactly the number it asked
    # Confirmed: each AskRequest carries its own reply channel, so replies are
    # not cross-delivered (actor_ref.rs:810-824; reply routed per BoxReplySender).

  @linearizability
  Scenario: Many concurrent waiters all observe a single startup completion
    Given an actor whose on_start blocks until released
    When 10 tasks concurrently await wait_for_startup
    And on_start is then released
    Then all 10 waiters resolve after startup completes
    # Confirmed: wait_for_startup awaits a shared SetOnce that fans out to all
    # waiters on a single set (actor_ref.rs:514-517).

  @linearizability
  Scenario: Many concurrent waiters all observe a single shutdown completion
    Given a running actor
    When 10 tasks concurrently await wait_for_shutdown
    And the actor is then stopped
    Then all 10 waiters resolve after the mailbox closes
    # Confirmed: wait_for_shutdown awaits the shared mailbox closed() signal,
    # observable by all waiters (actor_ref.rs:619-622).
