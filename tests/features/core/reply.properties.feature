# Phase 2: laws (∀ inputs) and model-checks, layered on reply.feature's examples.
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
# Scope: src/reply.rs — Reply for Result<T,E> (reply.rs:567-589), impl_infallible_reply!
#        (reply.rs:591-633), ReplySender::send mapping (reply.rs:174-184), and
#        ForwardedReply from_ok/from_err/from_result + into_value (reply.rs:423-564).

@core @reply @phase2
Feature: Reply — laws over Result conversion, infallibility, and ForwardedReply round-trips
  As the actor runtime turning a handler's return value into a wire reply
  I want Reply conversions to preserve every Ok value and Err error for ALL inputs
  So that no value or error is corrupted or lost on the way to the caller

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: Result Ok/Err round-trips through Reply conversion for any v and any e
    Given any Result<T, E> value that is either Ok(v) or Err(e)
    When to_result, into_value, and into_any_err are evaluated on a clone of it
    Then to_result returns the same Ok(v) or Err(e)
    And into_value returns the same Ok(v) or Err(e)
    And into_any_err returns None for Ok(v) and Some boxing e for Err(e)
    # GEN: v from a boundary-biased value type {0, 1, MAX-1, MAX; empty/max string};
    #      e from a boundary-biased error type (distinct discriminants); include both arms.
    # ORACLE: identity — to_result and into_value are the identity on Result, into_any_err
    #         maps only the Err arm to Some (reply.rs:576-588).
    # Generalizes: reply.feature "Result Ok converts to an Ok result and reports no error",
    #              "Result Err boxes the error for the caller and is reported by into_any_err".

  @property @sequence
  Scenario: ReplySender::send wires any handler Ok as a boxed value and any Err as HandlerError
    Given a ReplySender for a Result reply and any Result value Ok(v) or Err(e)
    When send is called with that value
    Then on Ok(v) the channel receives Ok(Box) recovering v
    And on Err(e) the channel receives Err(BoxSendError::HandlerError) recovering e
    # GEN: v and e from the same boundary-biased value/error types as above; include both
    #      arms; send is exercised once per value (the sender is single-use).
    # ORACLE: a tagged-union model — Ok -> Box::new(value), Err -> HandlerError(Box::new(e))
    #         (reply.rs:174-184). Recovery downcasts back to the original v / e.
    # Generalizes: reply.feature "ReplySender::send maps a handler Ok into a boxed value",
    #              "… maps a handler Err into a HandlerError on the wire".

  @property @sequence
  Scenario: Any impl_infallible_reply type reports no error and yields Ok(self)
    Given any value of an impl_infallible_reply type
    When to_result and into_any_err are evaluated
    Then to_result returns Ok(self) and into_any_err returns None
    And the associated Error type is Infallible
    # GEN: values drawn across several impl_infallible_reply types (e.g. (), bool, integer
    #      boundaries {0,1,MAX-1,MAX}, empty/max String) — the macro fixes Error=Infallible
    #      for every listed type (reply.rs:615-632,636-679).
    # ORACLE: the constant None / Ok(self) — an infallible type can never produce an error.
    # Generalizes: reply.feature "An infallible-reply type reports no error and has
    #              Error = Infallible".

  @property @lifecycle
  Scenario: ForwardedReply from_ok / from_err / from_result preserve any value or error
    Given any Result<T, E> value built into a ForwardedReply via from_ok/from_err/from_result
    When into_value, to_result, and into_any_err are evaluated on it
    Then a from_ok(v) / from_result(Ok(v)) yields Ok(v) and into_any_err None
    And a from_err(e) / from_result(Err(e)) yields Err(SendError::HandlerError(e))
    And the Err case's into_any_err is Some boxing that HandlerError
    # GEN: v / e from boundary-biased value/error types {0,1,MAX-1,MAX; empty/max string;
    #      distinct error discriminants}; cover all three constructors and both arms.
    # ORACLE: identity into the Direct arm — from_* store the Result directly and conversion
    #         maps the Err through SendError::HandlerError (reply.rs:423-461,497-544).
    #         Cross-checked by tests test_forwarded_reply_from_ok/from_err/from_result
    #         (reply.rs:792-970).
    # Generalizes: reply.feature "ForwardedReply::from_ok carries a direct success…",
    #              "… from_err carries a direct error…", "… from_result preserves both arms".

  # ---------------------------------------------------------------------------
  # @model — concurrent forwarded replies stay correctly typed
  # ---------------------------------------------------------------------------

  @model @linearizability
  Scenario: N concurrent forwarded asks each downcast to their own value with no cross-talk
    Given a router whose ForwardedReply forwards each ask to a target returning a distinct value
    And any number N of tasks concurrently asking the router and awaiting typed replies
    When all N asks run with real overlap, started at a barrier
    Then every task recovers its own correct value via downcast_ok
    And no reply downcast panics and no value is delivered to the wrong caller
    # GEN: N ∈ [2, 64] (include the smallest concurrent case 2 and a large fan-out);
    #      target values distinct per task so any cross-talk is observable.
    # ORACLE: a map task_id -> expected_value — downcast_ok/downcast_err recover per-reply
    #         types from each oneshot independently (reply.rs:546-564); received map must
    #         equal the expected map. Small cases via loom for interleavings.
    # Generalizes: reply.feature "Concurrent forwarded asks each downcast to their own
    #              correct reply value".
