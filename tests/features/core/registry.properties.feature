# Phase 2: laws (∀ inputs) and model-checks over ActorRegistry, layered on registry.feature.
# Conventions (see docs/testing/properties.md):
#   * @property — ∀ inputs. invariant(SUT(inputs)). Wired with proptest.
#   * @model    — SUT ≡ reference model under any op sequence / interleaving.
#   * Each scenario ALSO carries one Phase-1 category tag (+ @timing if a clock is needed).
#   * # GEN: names the generator strategy AND the boundary values it must include.
#   * # ORACLE: names the reference model / inverse function.
#   * # Generalizes: the Phase-1 example scenario(s) this law subsumes.
#   * Facts only; grounded in src/registry.rs as read 2026-06. No step definitions.
#
# Grounding (src/registry.rs):
#   * insert(name, ref) -> bool: false (NO overwrite) on a present name, true on first insert
#     (contains_key guard, registry.rs:104-110).
#   * get::<A>(name): Ok(None) absent; Ok(Some) present + downcast ok; Err(BadActorType) wrong type
#     (registry.rs:77-86).
#   * remove/contains_name/len/is_empty/clear reflect the HashMap; the Mutex serialises every op.

@core @registry @phase2
Feature: ActorRegistry — laws over insert-no-overwrite, type safety, and concurrent election
  As code resolving actors by name under generated op sequences and concurrency
  I want the registry to refine an insert-no-overwrite map for ALL operation orders
  So that no sequence or race silently overwrites, loses, or mistypes an entry

  Background:
    Given an empty local actor registry

  # ---------------------------------------------------------------------------
  # @property — universally-quantified laws
  # ---------------------------------------------------------------------------

  @property @boundary
  Scenario: get with the wrong actor type is always BadActorType for any registered ref
    Given a running actor of any type A registered under any name
    When get::<B>(name) is called with a type B that differs from A
    Then it returns Err(RegistryError::BadActorType) for every such A, B, and name
    # GEN: name ∈ {"", "x", 100_000-char string}; (A, B) any distinct type pair. The stored Box<dyn Any>
    #      downcasts to A only; a B downcast fails ⇒ BadActorType (registry.rs:77-85).
    # ORACLE: the predicate "stored type == requested type"; false ⇒ BadActorType, never Ok.
    # Generalizes: registry.feature "get with the wrong actor type returns BadActorType".

  # ---------------------------------------------------------------------------
  # @model — refinement of an insert-no-overwrite map; concurrent election
  # ---------------------------------------------------------------------------

  @model @sequence
  Scenario: the registry refines a Map<Name,Ref> with insert-NO-overwrite under any op sequence
    Given any sequence of insert / remove / get / contains_name / clear operations over a small name set
    When the operations are applied to the registry and to a reference model in the same order
    Then after every operation the registry's observable state equals the model's
    And an insert under an already-present name returns false and leaves the existing ref unchanged
    # GEN: op sequence length ∈ [0, 64] (include empty sequence); names drawn from {"", "a", "b",
    #      long-string} so duplicates and removes-of-absent occur; types from {Foo, Bar} to exercise get.
    # ORACLE: a HashMap<Name, (TypeId, Id)> where insert is `entry().or_insert` style (first wins, returns
    #         true; subsequent returns false, no overwrite); get == Some iff present AND TypeId matches,
    #         else None / BadActorType; remove/clear/len/contains mirror the map.
    # Generalizes: registry.feature "insert then get…", "insert, remove, then get…",
    #              "len, is_empty and names track inserts and removals", "clear empties the registry",
    #              "a duplicate-name insert returns false and does not overwrite the existing entry",
    #              "two distinct names for the same actor ref are independent keys".

  @model @linearizability
  Scenario: for any name inserted by K concurrent threads, exactly one insert wins
    Given any number K of threads each holding a distinct running actor of type Foo
    When all K threads concurrently insert under the SAME name under a barrier
    Then exactly one insert returns true and the other K-1 return false
    And get::<Foo>(name) afterwards returns the ref belonging to the single winning thread
    # GEN: K ∈ [2, 32] (include K = 2). Real overlap via tokio::spawn/thread + Barrier (rule 8).
    #      The Mutex serialises insert and the contains_key guard forbids a second winner (registry.rs:104-110).
    # ORACLE: a single-winner counter — across the K return values exactly one is true; the stored id
    #         equals that winner's actor id. No interleaving may produce 0 or >1 winners.
    # Generalizes: registry.feature "concurrent inserts of the same name elect exactly one winner",
    #              "concurrent inserts of distinct names all succeed and are all visible".
