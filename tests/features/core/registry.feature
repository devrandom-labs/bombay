# Scope: bombay core local `ActorRegistry` (src/registry.rs) — the in-process,
#        name → ActorRef registry behind a global Mutex (ACTOR_REGISTRY). This
#        feature covers the LOCAL registry only (the libp2p/remote registry under
#        feature = "remote" is out of scope here).
#
# Authoring rules (mirror tests/features/actors/message_queue.feature exactly):
#   * Exactly ONE cross-cutting tag per Scenario: @sequence | @lifecycle |
#     @boundary | @linearizability.
#   * Invariant-first Then; unverifiable → `# NOTE:` + @review-semantics.
#   * @bug:<file:line> marks a scenario that MUST FAIL today.
#   * Facts only: every Then is grounded in src/registry.rs / src/error.rs.
#   * No step definitions here.
#
# Grounding facts (src/registry.rs):
#   * insert(name, ref) -> bool: returns FALSE on a duplicate name and does NOT
#     overwrite the existing entry; returns TRUE on first insert. (It does NOT
#     return RegistryError::NameAlreadyRegistered — that variant is the remote
#     path's. The local insert signals duplication purely via the bool.)
#   * get::<A>(name) -> Result<Option<ActorRef<A>>, RegistryError>: Ok(None) when
#     the name is absent; Ok(Some(ref)) when present AND the stored entry downcasts
#     to A; Err(RegistryError::BadActorType) when present but A is the wrong type.
#   * contains_name(name) -> bool.
#   * remove(name) -> bool: true iff an entry was removed.
#   * remove_by_id(&ActorId) -> bool: scans entries (extract_if) and removes the
#     first whose stored id matches; true iff one was removed.
#   * len / is_empty / clear / names() reflect the underlying HashMap.
#   * A RegisteredActorRef stores the ActorRef as Box<dyn Any + Send> plus the
#     captured ActorId; the registry holds a CLONE of the ActorRef and does not
#     observe the actor's liveness — a dead actor stays registered until removed.

@core @registry
Feature: ActorRegistry — local name-keyed actor lookup
  As code resolving actors by a human name
  I want insert/get/remove with type-safe downcast and no silent overwrite
  So that name collisions are surfaced and lookups return the right typed ref

  Background:
    Given an empty local actor registry

  # ---------------------------------------------------------------------------
  # @sequence — insert → get → remove protocol on the same registry
  # ---------------------------------------------------------------------------

  @sequence
  Scenario: insert then get returns the same actor ref for the matching type
    Given a running actor "A1" of type Foo
    When "A1" is inserted under name "alpha"
    Then the insert returns true
    And get::<Foo>("alpha") returns Some(ref) whose id equals A1's id

  @sequence
  Scenario: get for an unregistered name returns Ok(None)
    When get::<Foo>("missing") is called
    Then it returns Ok(None)

  @sequence
  Scenario: insert, remove, then get reflects the removal
    Given a running actor "A1" of type Foo registered under "alpha"
    When "alpha" is removed
    Then remove returns true
    And get::<Foo>("alpha") returns Ok(None)
    And contains_name("alpha") returns false

  @sequence
  Scenario: len, is_empty and names track inserts and removals
    Given the registry is empty
    Then is_empty() returns true and len() returns 0
    When actors are inserted under "a", "b" and "c"
    Then len() returns 3 and is_empty() returns false
    And names() yields exactly the set {"a","b","c"}
    When "b" is removed
    Then len() returns 2 and names() yields exactly {"a","c"}

  @sequence
  Scenario: clear empties the registry
    Given actors are inserted under "a" and "b"
    When clear() is called
    Then is_empty() returns true and len() returns 0
    And get::<Foo>("a") returns Ok(None)

  # ---------------------------------------------------------------------------
  # @boundary — duplicate name, wrong type, missing entries, name extremes
  # ---------------------------------------------------------------------------

  @boundary
  Scenario: a duplicate-name insert returns false and does not overwrite the existing entry
    Given a running actor "A1" of type Foo registered under "alpha"
    And a different running actor "A2" of type Foo
    When "A2" is inserted under name "alpha"
    Then the insert returns false
    And get::<Foo>("alpha") still returns the ref whose id equals A1's id
    # Grounded: insert checks contains_key and returns false without replacing (registry.rs:104-110).

  @boundary
  Scenario: get with the wrong actor type returns BadActorType
    Given a running actor "A1" of type Foo registered under "alpha"
    When get::<Bar>("alpha") is called
    Then it returns Err(RegistryError::BadActorType)
    # Grounded: actor_ref().cloned().ok_or(BadActorType) on downcast failure (registry.rs:77-85).

  @boundary
  Scenario: removing a name that was never registered returns false
    When "ghost" is removed
    Then remove returns false

  @boundary
  Scenario: remove_by_id removes the entry whose stored ActorId matches
    Given a running actor "A1" of type Foo registered under "alpha"
    When remove_by_id(A1's id) is called
    Then it returns true
    And contains_name("alpha") returns false

  @boundary
  Scenario: remove_by_id for an id that is not registered returns false
    Given a running actor "A1" of type Foo registered under "alpha"
    And a second running actor "A2" that was never registered
    When remove_by_id(A2's id) is called
    Then it returns false
    And contains_name("alpha") still returns true

  @boundary
  Scenario: remove_by_id removes only the FIRST matching entry when one id is registered under two names
    Given a running actor "A1" of type Foo registered under both "alpha" and "beta"
    Then len() returns 2
    When remove_by_id(A1's id) is called
    Then it returns true
    And len() returns 1
    And exactly one of contains_name("alpha") / contains_name("beta") is now false
    And the still-present name resolves to a ref whose id equals A1's id
    When remove_by_id(A1's id) is called a second time
    Then it returns true and len() returns 0
    # Grounded: remove_by_id is extract_if(...).next().is_some() (registry.rs:123-128) — `.next()`
    # pulls only the FIRST predicate match and stops, so one call removes a single entry even when
    # several share the id. WHICH of the two names is removed first is HashMap-iteration-arbitrary,
    # so the assertion pins the conserved facts (len 2→1→0, exactly one survives the first call),
    # never a specific name. insert dedups by name only, so the same id under two names is legal
    # (registry.rs:104-110; cf. "two distinct names for the same actor ref").

  @boundary
  Scenario: an empty-string name is a valid distinct key
    Given a running actor "A1" of type Foo
    When "A1" is inserted under name ""
    Then the insert returns true
    And contains_name("") returns true
    And get::<Foo>("") returns Some(ref) whose id equals A1's id
    # NOTE: the registry imposes no name validation; "" is just a HashMap key.

  @boundary
  Scenario: a very long name round-trips as a key
    Given a running actor "A1" of type Foo
    And a name of 100000 characters
    When "A1" is inserted under that long name
    Then the insert returns true
    And get::<Foo>(the long name) returns Some(ref) whose id equals A1's id

  @boundary
  Scenario: two distinct names for the same actor ref are independent keys
    Given a running actor "A1" of type Foo
    When "A1" is inserted under "alpha"
    And "A1" is inserted under "beta"
    Then both inserts return true
    And get::<Foo>("alpha") and get::<Foo>("beta") both return refs with A1's id
    When "alpha" is removed
    Then get::<Foo>("beta") still returns A1's ref

  # ---------------------------------------------------------------------------
  # @lifecycle — registration outlives the actor; dead refs stay until removed
  # ---------------------------------------------------------------------------

  @lifecycle
  Scenario: a registered ref remains present after the actor stops
    Given a running actor "A1" of type Foo registered under "alpha"
    When "A1" is stopped gracefully and shutdown completes
    Then contains_name("alpha") still returns true
    And get::<Foo>("alpha") returns Some(ref) whose id equals A1's id
    # The registry holds a clone and does not observe liveness; the entry is stale-but-present.

  @lifecycle
  Scenario: a stale registered ref reports the actor as not running when messaged
    Given a running actor "A1" of type Foo registered under "alpha"
    And "A1" is stopped gracefully and shutdown completes
    When the ref obtained from get::<Foo>("alpha") is told a message
    Then the send returns SendError::ActorNotRunning
    # Liveness is a property of the ActorRef, not the registry entry.

  @lifecycle
  Scenario: re-registering a name after removing the dead entry succeeds
    Given a running actor "A1" of type Foo registered under "alpha"
    And "A1" is stopped and its entry is removed from the registry
    And a fresh running actor "A2" of type Foo
    When "A2" is inserted under "alpha"
    Then the insert returns true
    And get::<Foo>("alpha") returns the ref whose id equals A2's id

  # ---------------------------------------------------------------------------
  # @linearizability — concurrent access to the Mutex-guarded registry
  # ---------------------------------------------------------------------------

  @linearizability
  Scenario: concurrent inserts of the same name elect exactly one winner
    Given 16 tasks each holding a distinct running actor of type Foo
    When all 16 tasks concurrently insert under the same name "alpha" under a barrier
    Then exactly one insert returns true and the other 15 return false
    And get::<Foo>("alpha") returns the ref belonging to the single winning task
    # Mutex serialises insert; the contains_key guard guarantees one winner, no overwrite.

  @linearizability
  Scenario: a concurrent get during a remove observes either the ref or its absence, never a torn entry
    Given a running actor "A1" of type Foo registered under "alpha"
    When one task removes "alpha" while another task concurrently calls get::<Foo>("alpha") under a barrier
    Then the get returns either Ok(Some(ref with A1's id)) or Ok(None)
    And it never returns Err(BadActorType) and never panics
    # Mutex makes each op atomic; the only legal outcomes are the before/after states.

  @linearizability
  Scenario: concurrent inserts of distinct names all succeed and are all visible
    Given 32 tasks each holding a distinct running actor of type Foo and a distinct name
    When all 32 tasks concurrently insert under a barrier
    Then every insert returns true
    And len() returns 32 and each name resolves to its own actor's id
