# Scope: bombay local core error types (src/error.rs) — the pure, transport-agnostic
#        error algebra exercised in-process:
#          * SendError<M, E>     — ActorNotRunning(M) / ActorStopped / MailboxFull(M)
#                                  / HandlerError(E) / Timeout(Option<M>), with
#                                  map_msg / map_err / boxed / msg / err / flatten and the
#                                  BoxSendError downcast / try_downcast round-trip.
#          * ActorStopReason     — is_normal() across variants.
#          * PanicReason         — is_lifecycle_hook() / is_message_processing().
#          * PanicError          — construction from &'static str vs String, downcast,
#                                  with_downcast_ref mismatch, poisoned-mutex access.
#          * RegistryError       — BadActorType vs NameAlreadyRegistered (distinct domains).
#
#  The `remote`-gated variants (RemoteSendError, libp2p RegistryError arms, PeerDisconnected)
#  are NOT in scope here — this is the local core. Anything requiring the remote feature is
#  flagged @review-semantics for the Zenoh-layer feature file.
#
# Authoring rules (mirrors tests/features/actors/message_queue.feature EXACTLY):
#   * Every Scenario carries exactly ONE cross-cutting tag.
#   * Invariant-first; facts only — grounded in src/error.rs as read 2026-06.
#   * No step definitions here.

@core @error
Feature: Error algebra — SendError, stop reasons, panic reasons, registry domains
  As a caller mapping and inspecting actor errors
  I want SendError transforms, stop/panic classifiers, and registry variants to be exact and lossless
  So that callers can match the real failure domain and never collapse distinct errors together

  # ---------------------------------------------------------------------------
  # @sequence — SendError transform chains (map_msg / map_err / boxed / downcast / flatten)
  # ---------------------------------------------------------------------------

  @sequence
  Scenario Outline: map_msg rewrites the inner message only for message-bearing variants
    Given a SendError "<variant>"
    When map_msg is applied with a message transform
    Then the transform is applied: <applied>
    And the variant tag is preserved
    # src/error.rs:104-115 — ActorNotRunning(f(msg)), MailboxFull(f(msg)),
    # Timeout(msg.map(f)); ActorStopped and HandlerError pass through untouched.

    Examples:
      | variant               | applied |
      | ActorNotRunning(m)    | yes     |
      | MailboxFull(m)        | yes     |
      | Timeout(Some(m))      | yes     |
      | Timeout(None)         | no      |
      | ActorStopped          | no      |
      | HandlerError(e)       | no      |

  @sequence
  Scenario Outline: map_err rewrites the inner error only for the HandlerError variant
    Given a SendError "<variant>"
    When map_err is applied with an error transform
    Then the error transform is applied: <applied>

    Examples:
      | variant            | applied |
      | HandlerError(e)    | yes     |
      | ActorNotRunning(m) | no      |
      | ActorStopped       | no      |
      | MailboxFull(m)     | no      |
      | Timeout(Some(m))   | no      |
    # src/error.rs:118-129 — only HandlerError(op(err)) is rewritten; all others pass through.

  @sequence
  Scenario: boxed then downcast round-trips a HandlerError back to its concrete type
    Given a SendError HandlerError carrying a concrete error of type E
    When boxed() erases it to BoxSendError
    And downcast::<M, E>() is applied
    Then the recovered value equals the original HandlerError(E)
    # src/error.rs:132-146 boxed + :217-254 try_downcast — variant tag and payload survive.

  @sequence
  Scenario: msg() and err() each extract from exactly their owning variants
    Given the five SendError variants
    Then msg() returns Some for ActorNotRunning, MailboxFull and Timeout(Some), else None
    And err() returns Some only for HandlerError, else None
    # src/error.rs:149-164.

  @sequence
  Scenario: flatten collapses a nested SendError<M, SendError<M, E>> by failure domain
    Given a nested SendError where the outer HandlerError wraps an inner SendError
    When flatten() is applied
    Then HandlerError(ActorNotRunning(m)) becomes ActorNotRunning(m)
    And HandlerError(ActorStopped) becomes ActorStopped
    And HandlerError(MailboxFull(m)) becomes MailboxFull(m)
    And HandlerError(Timeout(m)) becomes Timeout(m)
    And HandlerError(HandlerError(e)) becomes HandlerError(e)
    # src/error.rs:195-215 — each inner domain is hoisted to the matching outer variant.

  # ---------------------------------------------------------------------------
  # @boundary — downcast type mismatches, poisoned mutex, panic-payload typing
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: try_downcast to the wrong message type returns Err carrying the original BoxSendError
    Given a BoxSendError ActorNotRunning whose boxed message is actually type A
    When try_downcast::<B, E>() is applied with B != A
    Then it returns Err re-wrapping the value as the same variant
    And no panic occurs
    # src/error.rs:229-254 — downcast failure maps back via SendError::ActorNotRunning(..),
    # so the original is recoverable rather than lost.

  @boundary
  Scenario: downcast (the unwrapping form) to the wrong type panics
    Given a BoxSendError whose boxed payload is type A
    When downcast::<B, E>() is applied with B != A
    Then it panics because downcast is try_downcast().unwrap()
    # src/error.rs:219-226 — the infallible downcast unwraps; wrong type is a programmer bug.

  @boundary
  Scenario Outline: unwrap_msg returns the message only for message-bearing variants, else panics
    Given a SendError "<variant>"
    When unwrap_msg() is called
    Then the outcome is <outcome>

    Examples:
      | variant            | outcome                                                          |
      | ActorNotRunning(m) | returns m                                                        |
      | MailboxFull(m)     | returns m                                                        |
      | Timeout(Some(m))   | returns m                                                        |
      | Timeout(None)      | panics "called `SendError::unwrap_msg()` on a non message error" |
      | ActorStopped       | panics "called `SendError::unwrap_msg()` on a non message error" |
      | HandlerError(e)    | panics "called `SendError::unwrap_msg()` on a non message error" |
    # src/error.rs:171-176 — unwrap_msg delegates to msg() (Some for the three message-bearing
    # variants, None for Timeout(None)/ActorStopped/HandlerError) and panics on None. Note
    # Timeout(None) panics even though it is a Timeout — the message is the discriminator, not
    # the variant tag (CLAUDE.md rule 3: distinct failure domains).

  @boundary
  Scenario Outline: unwrap_err returns the error only for HandlerError, else panics
    Given a SendError "<variant>"
    When unwrap_err() is called
    Then the outcome is <outcome>

    Examples:
      | variant            | outcome                                                  |
      | HandlerError(e)    | returns e                                                |
      | ActorNotRunning(m) | panics "called `SendError::unwrap_err()` on a non error" |
      | MailboxFull(m)     | panics "called `SendError::unwrap_err()` on a non error" |
      | Timeout(Some(m))   | panics "called `SendError::unwrap_err()` on a non error" |
      | ActorStopped       | panics "called `SendError::unwrap_err()` on a non error" |
    # src/error.rs:183-188 — unwrap_err delegates to err() (Some only for HandlerError) and
    # panics on None. These two panics are programmer-bug assertions (rule 4: panics are for
    # bugs), not data-limit errors.

  @boundary
  Scenario: PanicError from a &'static str panic payload exposes the string via with_str
    Given a PanicError constructed from a panic carrying a &'static str
    When with_str is called
    Then it yields Some with the original string
    # src/error.rs:472-480 new_from_panic_any downcasts &'static str first; :489-499 with_str.

  @boundary
  Scenario: PanicError from a String panic payload also exposes the string via with_str
    Given a PanicError constructed from a panic carrying a String
    When with_str is called
    Then it yields Some with the original string
    # src/error.rs:475-478 falls through to the String downcast; with_str:496 reads String too.

  @boundary
  Scenario: with_downcast_ref to a type the panic payload is not returns None
    Given a PanicError whose inner payload is a String
    When with_downcast_ref::<SomeOtherType, _, _> is called
    Then it returns None
    # src/error.rs:510-519 — lock().downcast_ref() returns None on type mismatch.

  @boundary
  Scenario: PanicError reads its payload even when the inner mutex is poisoned
    Given a PanicError whose inner error mutex has been poisoned by a prior panic
    When with, with_str, or with_downcast_ref is called
    Then it still accesses the payload via the poisoned guard's get_ref
    And no second panic is raised by the access
    # src/error.rs:515-518 / :526-529 — Err(poison) branch falls back to err.get_ref(),
    # so a poisoned mutex never blocks error inspection.

  # ---------------------------------------------------------------------------
  # @lifecycle — classifiers across the full variant set
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario Outline: ActorStopReason::is_normal is true only for Normal
    Given an ActorStopReason "<reason>"
    Then is_normal() returns <normal>
    # src/error.rs:378-391 — only Normal is true; SupervisorRestart, Killed, Panicked,
    # LinkDied are all false. (PeerDisconnected is remote-gated, out of local scope.)

    Examples:
      | reason            | normal |
      | Normal            | true   |
      | SupervisorRestart | false  |
      | Killed            | false  |
      | Panicked          | false  |
      | LinkDied          | false  |

  @lifecycle
  Scenario Outline: PanicReason classifies lifecycle-hook vs message-processing failures
    Given a PanicReason "<reason>"
    Then is_lifecycle_hook() returns <hook>
    And is_message_processing() returns <msg>
    # src/error.rs:714-740 — lifecycle = {OnStart, OnPanic, OnLinkDied, OnStop};
    # message_processing = {HandlerPanic, OnMessage}; Next is NEITHER.

    Examples:
      | reason        | hook  | msg   |
      | OnStart       | true  | false |
      | OnPanic       | true  | false |
      | OnLinkDied    | true  | false |
      | OnStop        | true  | false |
      | HandlerPanic  | false | true  |
      | OnMessage     | false | true  |
      | Next          | false | false |

  @lifecycle
  Scenario: Next is the one PanicReason that is neither a lifecycle hook nor message processing
    Given the PanicReason Next
    Then is_lifecycle_hook() is false and is_message_processing() is false
    # src/error.rs:714-740 — Next (the `next` hook) is excluded from both sets; this is the
    # boundary the two classifiers leave uncovered between them.

  # ---------------------------------------------------------------------------
  # @lifecycle — PanicError serde round-trip is LOSSY (write→serialize→deserialize)
  #   Serialize emits err = self.to_string() (Display) + reason  (src/error.rs:564-573)
  #   Deserialize rebuilds PanicError::new(Box::new(err: String), reason)  (:628-655)
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: PanicError serde preserves only the Display string and reason, erasing the payload type
    Given a PanicError with reason R wrapping a payload of concrete type T (not String)
    When it is serialized and then deserialized
    Then the recovered PanicError carries the same reason R
    And its inner payload is a String (the original Display text), no longer type T
    And downcast::<T>() on the recovered PanicError returns None
    # src/error.rs:570 serializes self.to_string(); :652-655 rebuilds the inner as a boxed
    # String. The concrete payload type is erased — this is the documented, defensible wire
    # contract (a remote peer cannot reconstruct an arbitrary Rust type), NOT a faithful
    # round-trip. CLAUDE.md rule 4: silent lossiness pinned as the actual behaviour.

  @lifecycle
  Scenario: PanicError serde round-trip is NOT idempotent — the reason prefix compounds
    Given a PanicError with reason R wrapping the string payload "boom"
    When it is serialized once and deserialized to p1
    Then p1's inner string equals the Display "R: boom"
    When p1 is serialized again and deserialized to p2
    Then p2's inner string equals "R: R: boom"
    # src/error.rs:542-545 Display is "{reason}: {payload}" when with_str finds a string; since
    # deserialize stores that whole Display string AS the next payload, each round-trip re-reads
    # it and prepends another "{reason}: ". The transform is therefore lossy AND non-idempotent
    # — a defensive-boundary fact a future serialization refactor must not silently change.

  @lifecycle
  Scenario: a non-string PanicError payload loses its value entirely through serde
    Given a PanicError with reason R wrapping a payload whose Display surfaces no string (with_str → None)
    When it is serialized
    Then the err field is exactly "R" (the reason alone), the payload value is absent
    And after deserialize the inner String is "R" and the original value is unrecoverable
    # src/error.rs:544 — Display falls back to "{reason}" when with_str yields None, so a
    # value-less Display drops the payload before it ever reaches the wire (:570). The reason is
    # all that survives; the original value cannot be recovered from the deserialized form.

  # ---------------------------------------------------------------------------
  # @boundary — RegistryError distinct failure domains (local-only variants)
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: BadActorType and NameAlreadyRegistered are distinct, non-interchangeable variants
    Given a registry lookup that finds an actor of the wrong type
    And a registry registration whose name is already taken
    Then the first yields RegistryError::BadActorType
    And the second yields RegistryError::NameAlreadyRegistered
    And the two are not equal and carry different Display strings
    # src/error.rs:818-867 — BadActorType ("bad actor type") vs NameAlreadyRegistered
    # ("name already registered"): one failure domain each (CLAUDE.md rule 3).
