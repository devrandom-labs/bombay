# Phase 2: laws (∀ inputs) over the SendError algebra, layered on error.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/error.rs as read 2026-06. No step definitions.
#
# Grounding (src/error.rs):
#   * map_msg (:104-115) rewrites the msg of ActorNotRunning/MailboxFull and the Some of Timeout;
#     it NEVER changes the variant tag. map_err (:118-129) rewrites only HandlerError's inner error.
#   * boxed (:132-146) + try_downcast (:229-254): for each variant, downcast back to the concrete
#     type round-trips. Wrong type ⇒ Err re-wrapping as the SAME variant (recoverable, :235-251).
#     NOTE: Timeout(None) carries no payload, so its downcast is trivially Ok for ANY type.
#   * flatten (:195-215) hoists HandlerError(inner) to the matching outer variant by failure domain.

@core @error @phase2
Feature: SendError algebra — laws over variant-preservation, downcast round-trip, and flatten hoisting
  As a caller transforming and inspecting SendError values across all variants
  I want map/boxed/downcast/flatten to be exact and lossless for EVERY variant and concrete type
  So that no transform silently changes a failure domain or loses a recoverable payload

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws over the variant set
  # ---------------------------------------------------------------------------

  @property @sequence
  Scenario: map_msg and map_err preserve the variant tag for every variant
    Given any SendError value drawn from all five variants
    When map_msg(f) and independently map_err(g) are applied
    Then the variant tag is unchanged in both results, for every variant
    And map_msg applies f only to ActorNotRunning/MailboxFull/Timeout(Some); map_err applies g only to HandlerError
    # GEN: variant ∈ {ActorNotRunning(m), ActorStopped, MailboxFull(m), HandlerError(e), Timeout(Some(m)),
    #      Timeout(None)} — include BOTH Timeout(Some) and Timeout(None) boundaries; f, g arbitrary pure maps.
    # ORACLE: a tag function tag(SendError) -> {ANR, Stopped, Full, Handler, Timeout}; tag(map_x(e)) == tag(e).
    #         Payload-rewrite predicate per error.rs:104-129.
    # Generalizes: error.feature "map_msg rewrites the inner message only for message-bearing variants"
    #              (Outline), "map_err rewrites the inner error only for the HandlerError variant" (Outline),
    #              "msg() and err() each extract from exactly their owning variants".

  @property @sequence
  Scenario: boxed then downcast to the correct concrete type is the identity for every variant
    Given any SendError<M, E> value over concrete types M and E
    When boxed() erases it to BoxSendError and try_downcast::<M, E>() recovers it
    Then the recovered SendError equals the original, for every variant and every concrete M, E
    # GEN: variant over all five (include Timeout(Some) and Timeout(None)); M, E arbitrary concrete
    #      'static types incl zero-sized and large payloads. boxed/try_downcast are inverses (error.rs:132-254).
    # ORACLE: try_downcast::<M, E> ∘ boxed == identity (the named inverse pair).
    # Generalizes: error.feature "boxed then downcast round-trips a HandlerError back to its concrete type".

  @property @boundary
  Scenario: a wrong-type try_downcast is a recoverable Err re-wrapping the value as the same variant
    Given a BoxSendError produced by boxing a SendError whose payload is concrete type A
    When try_downcast::<B, E>() is applied with B != A
    Then it returns Err carrying a BoxSendError of the SAME variant, with no panic, for every variant
    And a Timeout(None) downcasts Ok for ANY requested type because it carries no payload
    # GEN: source variant ∈ {ActorNotRunning(A), MailboxFull(A), HandlerError(A), Timeout(Some(A)),
    #      Timeout(None)}; B != A. error.rs:235-251 maps each downcast failure back via the same variant
    #      constructor — recoverable, never lost. Timeout(None) has nothing to downcast ⇒ Ok (boundary).
    # ORACLE: outcome predicate — payload-bearing variant with B != A ⇒ Err(same variant); Timeout(None) ⇒ Ok.
    # Generalizes: error.feature "try_downcast to the wrong message type returns Err carrying the original
    #              BoxSendError".

  @property @sequence
  Scenario: flatten hoists any inner HandlerError domain to the matching outer variant for every domain
    Given any nested SendError<M, SendError<M, E>> whose outer is HandlerError wrapping any inner variant
    When flatten() is applied
    Then each inner failure domain is hoisted to the matching outer variant, for every inner variant
    # GEN: inner variant ∈ {ActorNotRunning(m), ActorStopped, MailboxFull(m), HandlerError(e), Timeout(Some(m)),
    #      Timeout(None)}; also the non-nested outer variants (ActorNotRunning/ActorStopped/MailboxFull/Timeout)
    #      which flatten leaves in place. Include both Timeout(Some) and Timeout(None).
    # ORACLE: the hoist map (error.rs:195-215): HandlerError(ActorNotRunning(m)) -> ActorNotRunning(m);
    #         HandlerError(ActorStopped) -> ActorStopped; HandlerError(MailboxFull(m)) -> MailboxFull(m);
    #         HandlerError(Timeout(m)) -> Timeout(m); HandlerError(HandlerError(e)) -> HandlerError(e);
    #         each bare outer variant maps to itself.
    # Generalizes: error.feature "flatten collapses a nested SendError<M, SendError<M, E>> by failure domain".
