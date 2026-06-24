# Scope: kameo core reply machinery (src/reply.rs) — the `Reply` trait (Ok/Error/Value
#        associated types, to_result/into_any_err/into_value/downcast_ok/downcast_err),
#        `Result<T,E>` and `impl_infallible_reply!` blanket impls, the `DelegatedReply`
#        marker, `ReplySender` single-use send, and `ForwardedReply`
#        (Forwarded vs Direct; from_ok/from_err/from_result; downcast paths).
#
# Authoring rules (apply to ALL feature files):
#   * Every Scenario carries exactly ONE cross-cutting tag:
#       @sequence | @lifecycle | @boundary | @linearizability
#   * Invariant-first: the `Then` names the observable guarantee. If the Then cannot be
#     stated without reading the implementation, write it as `# NOTE:` + @review-semantics.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: a Then asserts behaviour confirmed from src/reply.rs, never a guess.
#   * No step definitions here. Steps are written in the wiring phase.

@core @reply
Feature: Reply — reply conversion, single-use senders, forwarding, and downcast safety
  As the actor runtime turning a handler's return value into a wire reply
  I want Reply conversions and the ForwardedReply downcast paths to be exact
  So that Ok values, Err errors, and forwarded outcomes are delivered without panics

  # ---------------------------------------------------------------------------
  # @sequence — Reply conversion protocol on Result and infallible types
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: Result Ok converts to an Ok result and reports no error
    Given a reply value Ok(value) of type Result<T, E>
    When to_result is called
    Then it returns Ok(value)
    And into_any_err returns None
    And into_value returns the same Ok(value)
    # Confirmed: Reply for Result<T,E> — to_result is identity, into_any_err maps Err only,
    # into_value is identity (src/reply.rs:567-588).

  @sequence
  Scenario: Result Err boxes the error for the caller and is reported by into_any_err
    Given a reply value Err(e) of type Result<T, E>
    When into_any_err is called
    Then it returns Some boxing e as a dyn ReplyError
    And to_result returns Err(e)
    # Confirmed: into_any_err on Err returns Some(Box::new(err) as Box<dyn ReplyError>)
    # (:580-583).

  @sequence
  Scenario: An infallible-reply type reports no error and has Error = Infallible
    Given a reply value of an impl_infallible_reply type such as String
    When to_result is called
    Then it returns Ok(self)
    And into_any_err returns None
    And the associated Error type is Infallible
    # Confirmed: impl_infallible_reply! sets Ok=Self, Error=Infallible, Value=Self;
    # to_result == Ok(self); into_any_err == None (:609-633).

  @sequence
  Scenario: ReplySender::send maps a handler Ok into a boxed value on the wire
    Given a ReplySender for a Result reply
    When send is called with Ok(value)
    Then the channel receives Ok(Box value) — the boxed success
    # Confirmed: send maps to_result Ok -> Box::new(value) as BoxReply (:174-184).

  @sequence
  Scenario: ReplySender::send maps a handler Err into a HandlerError on the wire
    Given a ReplySender for a Result reply
    When send is called with Err(e)
    Then the channel receives Err(BoxSendError::HandlerError boxing e)
    # Confirmed: send maps to_result Err -> BoxSendError::HandlerError(Box::new(err))
    # (:178-184).

  # ---------------------------------------------------------------------------
  # @lifecycle — single-use sender; DelegatedReply marker; ForwardedReply ctors
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: ReplySender is single-use — sending consumes it so a second reply is impossible
    Given a ReplySender obtained for one received message
    When send is called once
    Then the ReplySender is consumed by value and cannot be used again
    # Confirmed: send(self, ...) takes ownership; the type is #[must_use] and documented
    # as one-time use to enforce a single-reply guarantee (:138-184).

  @lifecycle
  Scenario: ForwardedReply::from_ok carries a direct success usable as a handler return
    Given a ForwardedReply built via from_ok(value)
    When to_result is called
    Then it returns Ok(value)
    And into_any_err returns None
    # Confirmed: from_ok builds Direct(Ok(value)); to_result on Direct maps Ok through
    # unchanged and into_any_err yields None for Ok (:423-427, :513-533).
    # Cross-checked by existing test test_forwarded_reply_from_ok (:792-828).

  @lifecycle
  Scenario: ForwardedReply::from_err carries a direct error surfaced as a HandlerError
    Given a ForwardedReply built via from_err(error)
    When into_value is called
    Then it returns Err(SendError::HandlerError(error))
    And into_any_err returns Some boxing that HandlerError
    # Confirmed: from_err builds Direct(Err(error)); into_value/to_result map the Err via
    # SendError::HandlerError (:457-461, :513-544); into_any_err wraps it (:524-533).
    # Cross-checked by test_forwarded_reply_from_err (:830-898).

  @lifecycle
  Scenario: ForwardedReply::from_result preserves both Ok and Err arms
    Given two ForwardedReplies, one from from_result(Ok(v)) and one from from_result(Err(e))
    When each is converted via into_value
    Then the Ok one yields Ok(v) and the Err one yields Err(SendError::HandlerError(e))
    # Confirmed: from_result stores the Result directly into Direct (:497-501); conversion
    # is the same Direct arm. Cross-checked by test_forwarded_reply_from_result (:900-970).

  @lifecycle
  Scenario: A successfully-forwarded ForwardedReply reports no error
    Given a ForwardedReply representing a forward that succeeded (Forwarded(Ok(())))
    When into_any_err is called
    Then it returns None
    # Confirmed: Forwarded arm of into_any_err returns res.err() — None on Ok (:524-528).

  @lifecycle
  Scenario: A failed-to-forward ForwardedReply reports the forwarding SendError
    Given a ForwardedReply representing a forward that failed (Forwarded(Err(send_error)))
    When into_any_err is called
    Then it returns Some boxing that SendError
    # Confirmed: Forwarded arm returns Some(Box::new(err)) on Err (:524-528).

  # ---------------------------------------------------------------------------
  # @boundary — DelegatedReply misuse, downcast paths, wrong-type downcast
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: DelegatedReply::to_result is unimplemented — it is only a return marker
    Given a DelegatedReply marker value
    When to_result is called on it directly
    Then the call panics with an unimplemented marker message
    # Confirmed: DelegatedReply::to_result calls unimplemented!(...) (:238-240). It is a
    # marker that must only be RETURNED by a handler, never converted; the dispatcher
    # never calls to_result on it because Value resolves to the inner reply.

  @boundary
  Scenario: DelegatedReply::into_value is unimplemented — it is only a return marker
    Given a DelegatedReply marker value
    When into_value is called on it directly
    Then the call panics with an unimplemented marker message
    # Confirmed: DelegatedReply::into_value calls unimplemented!(...) (:246-248).

  @boundary
  Scenario: DelegatedReply reports no error so a tell with a delegated handler never panics on it
    Given a DelegatedReply marker value
    When into_any_err is called on it
    Then it returns None
    # Confirmed: DelegatedReply::into_any_err returns None unconditionally (:242-244) —
    # the only one of the three trait methods that is implemented rather than a marker.

  @boundary
  Scenario: ForwardedReply downcast_ok recovers the inner Ok type from a forwarded success
    Given a wire reply whose boxed value is the inner R::Ok of a successful forward
    When ForwardedReply::downcast_ok is called on that boxed value
    Then it returns the inner Ok value
    # Confirmed: downcast_ok downcasts the Box<dyn Any> to Self::Ok (= R::Ok) (:546-550).

  @boundary
  Scenario: ForwardedReply downcast_err tries the inner error type before the outer SendError
    Given a wire BoxSendError that originated from the inner R::Error of a forward
    When ForwardedReply::downcast_err is called
    Then it recovers the inner error mapped through SendError::HandlerError
    # Confirmed: downcast_err first try_downcast::<N, R::Error> mapping to HandlerError,
    # only falling back to the outer SendError on failure (:554-564).

  @boundary
  Scenario: Reply::downcast_ok with a mismatched boxed type is the documented misuse boundary
    Given a wire reply whose boxed value is NOT the expected Ok type
    When the default Reply::downcast_ok is called
    Then it panics on the failed downcast
    # Confirmed: default downcast_ok is *ok.downcast().unwrap() (:109-111). The
    # BoxReplySender doc warns "misuse of this can result in panics" (:66-68). This pins
    # the type-confusion boundary: feeding the wrong boxed type violates the upstream
    # contract and is detected by panic, not silent corruption (engineering rule: each
    # crate validates at its own boundary).

  @boundary
  Scenario: A bare ForwardedReply Forwarded(Ok) must never be converted to a value
    Given a ForwardedReply in the Forwarded(Ok(())) state
    When into_value is called
    Then the unreachable forwarded-success branch is hit and the call panics
    # NOTE @review-semantics: into_value's Forwarded(Ok) arm is `unreachable!(...)`
    # (:535-539) because the dispatcher only ever converts a forwarded reply to a value
    # when it is an error. A Forwarded(Ok) reaching into_value would indicate a dispatch
    # bug — pin whether any code path can reach it before asserting this as a guarantee.

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent forwarded replies stay correctly typed
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: Concurrent forwarded asks each downcast to their own correct reply value
    Given a router whose ForwardedReply forwards each ask to a target returning a distinct value
    When many tasks concurrently ask the router and await typed replies
    Then every task receives its own correct value with no cross-talk between replies
    And no reply downcast panics
    # Confirmed-shape: downcast_ok/downcast_err recover per-reply types from each oneshot
    # channel independently (:546-564); concurrency must be real (spawned tasks) to
    # exercise the per-channel isolation.
