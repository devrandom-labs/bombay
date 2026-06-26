# Phase 2: laws (∀ inputs) and model-checks, layered on message.feature's examples.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN:    names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; a @bug-exposing property keeps its @bug:<file:line> and fails today.
#   * No step definitions — wiring is Phase 3.
#
# Scope: src/message.rs — single-writer sequential dispatch (message.rs:36-38,399-405),
#        forward routing of the original reply channel (message.rs:264-286), and
#        handle_dyn's ask-vs-tell error routing (message.rs:410-420; src/reply.rs:174-184).

@core @message @phase2
Feature: Message — laws over single-writer ordering, forward round-trips, and error routing
  As an actor processing messages one at a time with exclusive mutable state
  I want ordering, forwarding, and error routing to be exact for ALL inputs
  So that no message sequence, forwarded value, or handler error is misrouted

  Background:
    Given a spawned actor that handles a numbered command message

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: A handler processes any single-sender message sequence in exact send order
    Given any sequence of n distinct numbered commands from one sender
    When all n commands are sent in order and the actor records its handling order
    Then the recorded order equals the send order exactly
    # GEN: n ∈ boundary-biased usize {0, 1, 2, 64, 1024} (include empty and single);
    #      command tags are distinct and monotonic.
    # ORACLE: a VecDeque of tags in send order — single-writer dispatch over &mut A
    #         preserves FIFO (message.rs:36-38,399-405). SUT order == oracle order.
    # Generalizes: message.feature "Messages are handled sequentially in send order with
    #              exclusive state access".

  @property @lifecycle
  Scenario: forward on an ask round-trips any target reply back to the original caller
    Given a router actor and a live target that replies with any value v it is asked
    When the original caller asks the router, whose handler forwards to the target
    Then the original caller receives exactly v
    # GEN: v drawn from a boundary-biased value type {0, 1, MAX-1, MAX for integers;
    #      empty string, max-length string} — the forwarded value must survive verbatim.
    # ORACLE: identity through the channel — forward hands the original reply channel to
    #         the target's ask via forward(tx.cast()), so target reply == caller reply
    #         (message.rs:264-277).
    # Generalizes: message.feature "forward delivers the target actor's reply back to the
    #              original ask caller".

  @property @boundary
  Scenario: A handler Err on an ask routes that exact error to the caller as HandlerError
    Given a handler that returns Err(e) for any error value e
    When the message is sent via ask
    Then the caller's ask result is Err whose HandlerError carries exactly e
    # GEN: e drawn from a boundary-biased error type (distinct discriminants, including a
    #      zero/default value and a max-payload value).
    # ORACLE: identity through HandlerError — handle_dyn sends to_result Err via the reply
    #         channel, which ReplySender maps to BoxSendError::HandlerError(e)
    #         (message.rs:410-420; src/reply.rs:174-184).
    # Generalizes: message.feature "a handler that returns Err on an ask routes the error
    #              to the caller as HandlerError".

  @property @boundary
  Scenario: A handler Err on a tell routes to the error path, never to a caller, for any error
    Given a handler that returns Err(e) for any error value e
    When the message is sent via tell so no caller awaits
    Then handle_dyn surfaces e via into_any_err for the run loop to treat as a panic
    And no reply is delivered to any caller
    # GEN: e drawn from the same boundary-biased error type as the ask case.
    # ORACLE: a router that selects on the presence of a reply sender — None sender =>
    #         into_any_err(e) drives on_panic, Some sender => caller (message.rs:415-420;
    #         src/reply.rs:16-19). The tell branch must never reach a reply channel.
    # Generalizes: message.feature "a handler that returns Err on a tell routes the error
    #              to the panic hook, not a caller", "a successful handler reply on a tell
    #              produces no error and no hook invocation".

  # ---------------------------------------------------------------------------
  # @model — single-writer serialization under concurrency
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: Any concurrent mix of asks and tells applies each command exactly once
    Given an actor whose state is a fold over a stream of commands
    And any multiset of commands split across P concurrent tasks via ask and tell
    When all tasks send with real overlap, started at a barrier
    Then the final state equals the fold of every command applied exactly once
    And ask callers each receive the reply for their own command with no cross-talk
    # GEN: P ∈ [2, 8]; command count ∈ {1, 2, 50, 200}; each command tagged with origin
    #      and a distinct value; ask/tell split chosen per command.
    # ORACLE: a sequential fold model — because the actor is single-writer, the SUT final
    #         state must equal the fold of the same commands in SOME order, and every ask
    #         reply must match its own command (message.rs:36-38,399-405). Small cases via
    #         loom; larger via proptest + randomized scheduling.
    # Generalizes: message.feature "Concurrent asks from many tasks are each answered
    #              exactly once", "Interleaved asks and tells preserve single-writer
    #              state consistency".
