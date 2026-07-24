# Restart & supervision — design (card #196, slice 2 of the supervision epic)

**Status:** draft 2026-07-24 · epic #122 · parent #120 · follows #195 (links + death-watch).

## What this is

Slice 1 (#195) delivered *"an actor learns a peer died and may react"*. This
slice delivers *"a supervisor **rebuilds** a dead child under a policy, and knows
when to stop trying"*.

**This is not an OTP port.** The actor model proper — Hewitt 1973, Agha 1986:
`create` / `send` / `become` — contains no failure semantics at all. Verified
against Agha's text (MIT AITR-844): fault tolerance is never treated, and the
model's own invariant runs the other way — *"all actors must specify a
replacement behavior"*, with the syntactic default that an actor lacking a
`become` *"is replaced by an identically behaving actor"*, so that *"one can now
safely assert that all actors definable in an actor language like SAL specify a
replacement behavior."* In the model an actor **always** has a successor;
failure is not expressible.

Everything on this card is therefore **invented policy** — links, monitors, exit
signals and supervision trees alike. Each rule below is justified from the
property it buys, with independent lineages cited. Where Erlang/OTP is the only
source for a convention it is marked a **choice**; where bombay picks something
the surveyed systems disagree on, it is marked a **preference**. Where they
disagree across the whole range, that is treated as evidence the thing is **not
defaultable** and the caller must state it (see *Policy has no default*).

## Scope — slice 2a. Slice 2b is a separate card.

**In this card:**

- `Supervisor: Watch` supertrait + `spawn_supervised` entry point.
- The erased restart factory — the only `dyn` in the feature.
- `RestartPolicy` (`Permanent` / `Transient` / `Never`).
- Restart **spacing** (exponential backoff + jitter) and the **give-up counter**
  (consecutive failures with healthy-uptime reset, plus a lifetime rebuild
  budget), with escalation.
- `OneForOne` restart-set semantics (restart the failed child only).
- The two items deferred onto this card by #195: the `Unwatch`-racing-teardown
  invariant, and the `on_stop`-failure programmatic surface.

**Deferred to slice 2b (must be filed as a card before this one closes, per #166):**

- `OneForAll` / `RestForOne` — the coarser rungs of the escalation ladder.
- `supervisor_signals_heterogeneous_children` (sequence) — two child actor types
  driven through the erased factory edges.
- The heaviest DST burden: deterministic reproduction of the #100-class
  restart-storm and concurrent link/unlink/die races.

## Research grounding

Five lineages, four mechanisms, one shared contract.

### The contract they converge on

> **Discard the corpse, rebuild from a declared source, at the smallest
> granularity that works, and tell someone when it stops working.**

- **Rebuild, never resume.** Akka Typed: on restart *"the original Behavior that
  was given to `Behaviors.supervise` is re-installed"*. Crash-only (Candea & Fox, HotOS'03):
  *"There is only one way to stop such software—by crashing it—and only one way
  to bring it up—by initiating recovery."*
- **Cheapest recovery first, escalate by granularity.** Microreboot (Candea et
  al., OSDI'04): *"a simple recursive recovery policy based on the principle of
  trying the cheapest recovery first. If this does not help, RM reboots
  progressively larger subsets of components. Thus, RM first microreboots EJBs,
  then eBid's WAR, then the entire eBid application, then the JVM … and finally
  reboots the OS; if none of these actions cure the failure symptoms, RM
  notifies a human administrator."*
- **Act before diagnosis.** Microreboot: *"crashing every suspicious component
  could shorten the fault detection and diagnosis time—a period that sometimes
  lasts longer than repair itself"*, and it *"offers the possibility of curing
  the failure before diagnosis completes."*
- **Bound the attempts, and report.** Microreboot: *"In order to avoid endless
  cycles of rebooting, RM also notifies a human whenever it notices recurring
  failure patterns."* Kubernetes Jobs: `backoffLimit` then the Job is *marked as
  Failed*. Akka: `.withLimit(maxNrOfRetries, withinTimeRange)`. OTP: *"if more
  than MaxR restarts occur within MaxT seconds, the supervisor terminates all
  child processes and then itself"*.
- **Space the attempts.** Bronson et al., HotOS'21: a metastable failure is *a
  self-sustaining congestive collapse in which a system degrades in response to a
  transient stressor but fails to recover after the stressor is removed* —
  retry amplification is the canonical sustaining effect. Kubernetes: 10s → 20s →
  40s, capped at 6 min. OTP has **no** spacing mechanism; this is a place bombay
  deliberately exceeds it.

### Where they disagree, and what bombay picks

| Question | OTP | Akka Typed | Orleans | Kubernetes | **bombay** |
|---|---|---|---|---|---|
| Where supervision lives | parent tree | child-side `Behaviors.supervise` decorator | runtime, no tree | controller | **parent trait** (`Supervisor: Watch`) — because the failure signal already flows parent-ward on #195's link channel |
| Resume-in-place | absent | `SupervisorStrategy.resume` | n/a | n/a | **refused** — see below |
| Give-up accounting | sliding window MaxR/MaxT | `maxNrOfRetries` + `withinTimeRange` | n/a | count, reset on success | **consecutive count (reset on healthy uptime) + never-reset lifetime budget** |
| Spacing | none | backoff supervisor | n/a | exponential, capped | **exponential + jitter** |
| Restart-policy default | `permanent` | *stop* (unsupervised) | n/a | `Always` | **none — the caller must state it** |
| Lifecycle ownership | programmer | programmer | runtime (virtual actors) | controller | **programmer** (see *Not taken* below) |

### Refused: `Resume`

Akka offers `SupervisorStrategy.resume` — *"ignore the failure and process the
next message"*, keeping the instance and its (partially mutated) state. bombay
does not, and the reason is **not** that Rust makes it impossible: `catch_unwind`
+ `AssertUnwindSafe` leaves `&mut self` logically inconsistent but perfectly
memory-safe, so Resume is implementable. It is refused on two grounds:

1. **A decision already made.** #116 fixed the loop so a caught panic stops the
   actor and treats the state as poisoned. Offering Resume means re-opening that
   decision, not adding a policy on top of it.
2. **The consequence is worse here than on the JVM.** Akka's resumed actor
   serves subsequent messages from torn state; a resumed bombay actor backed by
   nexus additionally **emits events derived from that torn state into the
   append-only log**. A crash loses an in-flight message; a resume writes
   corruption that outlives the process. Same exposure, strictly worse blast
   radius.

### Not taken: virtual actors

Orleans deletes the question: *"Actors are purely logical entities that always
exist, virtually. An actor cannot be explicitly created nor destroyed, and its
virtual existence is unaffected by the failure of a server that executes it."*
Failure recovery is implicit — deactivate, re-activate on next message, state
reloaded from storage.

For a **nexus-backed aggregate addressed by identity** this may well be the
better model, and it composes with registry (#119) + identity-first `ActorId`
(#121). It is **out of scope for the bombay core**, which stays
transport- and domain-agnostic (CLAUDE.md): lazy reactivation belongs to the
`bombay-nexus` layer, built on top of this card's primitives. Recorded here so
the branch is not silently re-derived. → **ADR-0013**.

## Mechanism

### Ownership: the child table is loop-owned

```rust
/// Task-owned, exactly like #195's `Watchers`. The user's `&mut self` never
/// holds it, so a handler panic cannot tear the supervision bookkeeping.
struct Children {
    entries: SmallVec<[Child; 4]>,
}

struct Child {
    /// Rebuild edge: runs the user's spawn closure, installs the watch edge,
    /// and hands back the non-generic handle. Boxed future because #195's
    /// `ActorRef::watch` is `async` (it awaits mailbox capacity on the child).
    /// One box per *rebuild* — restart rate, never message rate.
    factory: Box<dyn FnMut() -> BoxFuture<'static, ChildHandle> + Send>,
    /// Current incarnation; `None` while a rebuild is pending its backoff.
    handle: Option<ChildHandle>,
    policy: RestartPolicy,
    /// Fast trip: burst failures. Reset to 0 once an incarnation survives
    /// `reset_after`.
    consecutive: u32,
    /// Slow trip: lifetime rebuild budget, NEVER reset. Catches the drip
    /// failure that resets `consecutive` every time (see below).
    total: u32,
    /// When the current incarnation started — drives the reset rule.
    started: Instant,
    /// Set while waiting out a backoff delay; drives the loop's timer arm.
    retry_at: Option<Instant>,
}

/// NON-generic: no `A` anywhere, so a heterogeneous child set is homogeneous here.
struct ChildHandle {
    id: ActorId,
    cancel: CancellationToken,
    abort: AbortHandle,
}
```

The `&mut self`-poisoning argument is the same one that put `Watchers` in the
loop in #195, and it is the crash-only argument applied to our own runtime:
recovery bookkeeping must not live in the state that the fault corrupted.

### The single `dyn`, and why it is only one

The user supplies a plain spawn closure; `supervise::<A, _>` — generic in `A` at
the call site, which is the only place `A` is in scope — wraps it into the erased
factory:

```rust
sup_ref.supervise(|| CounterActor::spawn(CounterArgs { start: 0 }));
sup_ref.supervise(|| {                                  // different actor type
    let db = DbActor::spawn(DbArgs { url: url.clone() });
    registry.register("db", &db)?;                      // name rebinding lives here
    db
});
```

The wrapper closes over the supervisor's `ActorRef<S>`, awaits
`sup_ref.watch(&child)` to install the death edge, copies out
`child.cancel_token()` / `child.abort_handle()` / `child.id()` (all already
`pub(crate)` on `ActorRef`), and **drops the strong `ActorRef<A>`** before
returning the handle — so the supervisor never pins the child.

**Consequence — this revises #122-#10 a second time.** Slice 1 showed the
predicted `Box<dyn SignalMailbox>` edges were an artifact of routing death
through the generic mailbox. Slice 2 shows the *stop* edges do not need erasure
either: `CancellationToken` and `AbortHandle` are already non-generic. The
feature's total erasure is **one boxed closure per supervised child**, on the
supervision path only (link rate, never message rate).

`supervise_cloned(args)` is a thin helper for `A::Args: Clone`. The general form
is the closure, so `Args` holding a receiver / connection / anything move-only is
supported — a `Clone` bound would have excluded them, and users discover that
late.

### Identity: a new incarnation is a new actor

A rebuilt child gets a **new `ActorId`**. Erlang's model exactly (new Pid, name
re-bound), and the one that keeps `ActorId` meaning *one mailbox lifetime* — the
alternative makes a stale `ActorRef` silently address a different incarnation.

- Third parties that watched the dead incarnation already received `LinkDied`
  for the **old** id. They are **not** migrated; they re-resolve by name.
- **Registry rebinding is the factory's job**, not the core's: the closure
  re-registers the new incarnation under the same name (#119). This keeps the
  supervision core free of registry coupling and is why the closure — not a
  declarative spec struct — is the right shape.

### Restart decision

```
LinkDied { id, reason, linked }
   │
   ├── id not in Children  ──► Watch::on_link_died  (peer watch/link, unchanged #195 path)
   │
   └── id is a child ───► should_restart(policy, reason)?
                            │
                            ├── no  ──► leave dead, entry retained
                            │
                            └── yes ──► consecutive += 1
                                        consecutive > max_restarts ?
                                          ├── yes ──► ESCALATE
                                          └── no  ──► retry_at = now + backoff(consecutive)
```

Children are handled by the framework and do **not** invoke the user's
`on_link_died`; that hook remains the peer-watch surface. Observability is
`tracing`, not a hook — POLA, and one fewer user-panic site.

`should_restart`:

| `ActorStopReason` | `Permanent` | `Transient` | `Never` |
|---|---|---|---|
| `Normal` | restart | leave dead | leave dead |
| `SupervisorRestart` | restart | leave dead | leave dead |
| `Killed` | restart | restart | leave dead |
| `Panicked(_)` | restart | restart | leave dead |
| `AlreadyDead` | restart | restart | leave dead |
| `LinkDied { .. }` | restart | restart | leave dead |

**`AlreadyDead` is restart-worthy but not crash-evidence** — *preference, not a
sourced finding.* #195 introduced it for *"the target was already gone when the
edge was installed, so its true reason is unknowable"*. Treating unknowable as
abnormal follows the act-before-diagnosis finding — restarting is the cheap probe
— and the give-up counters bound the cost if the child is genuinely unspawnable.
It **counts** toward both counters. No surveyed system has this case (it is an
artifact of bombay's message-installed watch edges), so the alternative —
ignore it, on the grounds that it is not evidence of a crash — is recorded here
rather than dismissed.

**Lifecycle-hook panics never restart.** `PanicReason::is_lifecycle_hook()`
(already in `error.rs`) means `on_start`/`on_stop`/`on_panic`/`on_link_died`
unwound. Restarting an actor that panicked *during startup* just re-panics it —
a guaranteed crash loop, and the one failure class where "try the cheap recovery
first" is knowably wrong. Such a death **escalates immediately**, bypassing both
backoff and the counter.

### Spacing and give-up

```rust
// No `Default` impl — see "Policy has no default; tuning does".
struct RestartConfig {
    policy: RestartPolicy,   // mandatory, never inferred
    max_restarts: u32,       // consecutive failures tolerated (fast trip)
    max_total: u32,          // lifetime rebuilds tolerated    (slow trip)
    min_backoff: Duration,
    max_backoff: Duration,
    jitter: f64,             // 0.0 ..= 1.0, fraction of the computed delay
    reset_after: Duration,   // healthy uptime that zeroes `consecutive`
}
```

**Two trip conditions, because they answer different questions.** `consecutive`
asks *"did this incarnation work?"*; `total` asks *"is this child worth having at
all?"* A child that fails every `reset_after + 1s` passes the first forever while
failing the second — dropping OTP's time window removes the burst-vs-drip
distinction the window was carrying, and `total` restores it without a clock.
Escalation fires on `consecutive > max_restarts || total > max_total`. Both
counters increment with `checked_add`; saturation is not an option in a limit
path (arithmetic-safety rule).

- `delay(n) = min(min_backoff * 2^(n-1), max_backoff)`, then jittered. Exponent
  computed with `checked_shl` / `checked_mul` — overflow saturates to
  `max_backoff` **by explicit branch**, never by `saturating_*` (arithmetic-safety
  rule: the cap here is a *semantic* ceiling, not an overflow sink).
- The reset rule replaces OTP's window. A window exists in OTP only because OTP
  has neither backoff nor a success signal; with `reset_after` we express
  *"it recovered"* directly instead of inferring it from timestamps.
- **No `governor`, no timestamp ring.** GCRA models a steady-state rate and
  answers `Err(NotUntil)` (backpressure); we need an exponential schedule and a
  terminal give-up. Neither half is a rate limiter. The counter is a `u32`. →
  **ADR-0012**.
- Delay is served by a **timer arm in the supervisor's own select**
  (`sleep_until(min(retry_at))`), never an inline `sleep` — a supervisor must
  keep handling messages while a child waits out 30 s of backoff, and never
  spawns a helper task that would hold a strong ref.

### Escalation

When `consecutive > max_restarts`, or on a lifecycle-hook panic:

1. Stop every remaining child **crash-only**: `cancel` → bounded grace →
   `abort`. This must terminate in bounded time *without the child's
   cooperation* — the crash-only power-off argument: *"entirely external to the
   component, thus not invoking any of the component's code and not relying on
   correct internal behavior of the component."*
2. Set the supervisor's own stop reason to
   `RestartLimitExceeded { child: ActorId, rebuilds: u32 }` (new
   `ActorStopReason` variant, owned by #113's type).
3. Stop. `Watchers::drop` (#195) then delivers `LinkDied` to **the supervisor's**
   watchers — so a parent supervisor rebuilding the larger unit **is** the next
   rung of *"progressively larger subsets"*, over the mechanism that already
   exists. No new escalation channel.

If nothing watches the supervisor, the failure surfaces as its death and the
outer layer (process, k8s) is the last rung — the same structure as Erlang's
node + `heart`, and as a Job reporting `Failed` to whoever launched it.

### Two carry-forwards from #195

**1. `Unwatch` racing teardown.** #195 documented that a queued `Signal::Unwatch`
cannot be honored once the target's receiver is dropping — the notice has
already been sent (matching Erlang, where `demonitor` may still be followed by a
delivered `DOWN`). Load-bearing here: a supervisor that stops supervising a
child *while that child is dying* must not restart it. Enforcement is
structural — the restart path is driven by a **lookup in `Children`**, so an
entry removed by `unsupervise` makes the late notice fall through to the
`on_link_died` peer path and then be ignored. Direct test required.

**2. `on_stop`-failure surface.** Today an `on_stop` returning `Err` is only
`eprintln!`-ed (`spawn.rs`), a deferral chain running #116 → #195 → here.
Minimum honest surface: `LinkDied` gains `cleanup_failed: bool`, set when the
dying actor's `on_stop` returned `Err`. One bit, still monomorphic, no new
channel; the original stop `reason` is preserved unchanged (a failed cleanup is
not a different death). A supervisor can then act on "it died *and* did not
clean up" — e.g. escalate rather than restart when a lock or file handle may be
stranded. Alternative considered and rejected: a distinct `ActorStopReason`
variant, which would overwrite the real reason.

## Public API

```rust
/// Authority marker, no methods: `Actor` cannot watch, `Watch` can watch but
/// not rebuild, `Supervisor` can rebuild. Restart policy is NOT a trait method —
/// it is per-child, supplied at `supervise` time.
pub trait Supervisor: Watch {}

pub trait SpawnSupervised: Supervisor {
    fn spawn_supervised(args: Self::Args) -> ActorRef<Self>;
    fn spawn_supervised_with_capacity(capacity: Capacity, args: Self::Args) -> ActorRef<Self>;
}

impl<A: Supervisor> SpawnSupervised for A {}

impl<S: Supervisor> ActorRef<S> {
    /// Registers a supervised child under an explicit restart policy. The
    /// closure spawns (and may name-register) each incarnation; the core
    /// installs the watch edge and keeps only weak handles.
    pub async fn supervise<A, F>(
        &self,
        config: impl Into<RestartConfig>,
        factory: F,
    ) -> Result<ActorId, TellError<()>>
    where A: Actor, F: FnMut() -> ActorRef<A> + Send + 'static;

    pub async fn supervise_cloned<A: Actor>(
        &self,
        config: impl Into<RestartConfig>,
        args: A::Args,
    ) -> Result<ActorId, TellError<()>>
    where A::Args: Clone;

    /// Drops the supervision edge; a later death for `id` is then ignored.
    pub async fn unsupervise(&self, id: ActorId) -> Result<(), TellError<()>>;
}
```

`Signal` gains one variant, `Supervise(Box<SuperviseReg>)` — boxed so the hot
`Message` variant keeps the #114 slot budget.

### Policy has no default; tuning does

**`RestartConfig` does not implement `Default`, and `RestartPolicy` has no
default.** Every `supervise` call names the policy. Rationale:

- The surveyed systems pick three *different* defaults spanning the whole range
  — OTP `permanent`, Kubernetes `restartPolicy: Always` (= permanent), Akka
  Typed *stop* (no supervision unless wrapped). Three mature systems
  disagreeing across the full range is evidence the choice is **not
  defaultable**, not an invitation to pick a fourth.
- Whether a dead child comes back is **semantics the caller owns**. A default
  policy is a silent `unwrap_or` on someone else's failure semantics — the
  supervisor would be acting on the user's behalf in a way they never wrote
  down.
- It matches the authority gradient the crate already uses: `Actor` cannot
  watch, `Watch` can watch but not rebuild, `Supervisor` can rebuild. Each
  step up is opt-in and explicit; the policy is the last step and gets the same
  treatment.

Numeric **tuning** keeps defaults, because magnitudes are not semantics:
`max_restarts = 5`, `max_total = 100`, `min_backoff = 100ms`,
`max_backoff = 30s`, `jitter = 0.2`, `reset_after = 60s`. These are unsourced
starting points, expected to move once slice 2b's DST work shows real restart
distributions.

```rust
sup.supervise(RestartPolicy::Transient, || Worker::spawn(args)).await?;
sup.supervise(                                    // tuning overridden, policy still explicit
    RestartConfig::new(RestartPolicy::Permanent).with_max_backoff(Duration::from_secs(5)),
    || Server::spawn(cfg.clone()),
).await?;
```

`impl Into<RestartConfig>` gives the bare-policy shorthand
(`From<RestartPolicy> for RestartConfig`) without ever letting the policy itself
be omitted.

*Enforcement note:* absence of a `Default` impl cannot be asserted by a runtime
test, and a `compile_fail` doctest would not run in the gate (#170). It is held
by review plus the clippy `derivable_impls` / manual-default lint surface, and
stated here so a future "add `Default` for ergonomics" PR reads as a spec
violation rather than a convenience.

## Invariants — TDD, each written failing first

One per bullet, per CLAUDE rule 3.

1. `restart_rebuilds_never_resumes` (@bug, lifecycle) — child panics with mutated
   state; the rebuilt incarnation's state equals a fresh `on_start(Args)`, and
   its `ActorId` differs.
2. `permanent_restarts_on_normal_exit` — `Permanent` rebuilds after a clean stop.
3. `transient_leaves_normal_exit_dead` — `Transient` does not.
4. `transient_restarts_on_panic` — `Transient` rebuilds after a handler panic.
5. `never_policy_never_restarts` — including after `SupervisorRestart`.
6. `already_dead_counts_as_restartable` — an `AlreadyDead` notice rebuilds under
   `Transient` and increments `consecutive`.
7. `lifecycle_hook_panic_escalates_without_restart` — an `on_start` panic
   escalates immediately; zero rebuild attempts.
8. `backoff_delays_grow_exponentially_and_cap` — under `start_paused`, attempt
   deadlines are `min_backoff·2^(n-1)` up to `max_backoff` (jitter disabled).
9. `healthy_uptime_resets_consecutive_counter` — a child surviving `reset_after`
   returns the counter to 0, so the next failure backs off from `min_backoff`.
10. `restart_limit_escalates_and_stops_supervisor` — `max_restarts + 1`
    consecutive failures stop the supervisor with
    `RestartLimitExceeded { child, rebuilds }`.
11. `slow_drip_failures_exhaust_lifetime_budget` — a child that fails once per
    `reset_after + ε` (so `consecutive` resets every time) still escalates after
    `max_total + 1` rebuilds. Fails if only the consecutive counter exists.
12. `escalation_delivers_link_died_to_supervisors_watcher` — the escalating
    supervisor's own death reaches *its* watcher (the ladder's next rung).
13. `escalation_stops_children_without_their_cooperation` — a child whose
    `on_stop` hangs is still terminated within the grace bound (crash-only).
14. `no_cascading_restart` — under `OneForOne` a child failure rebuilds that
    child only; siblings keep the same `ActorId` and stay alive.
15. `supervisor_keeps_serving_messages_during_backoff` — a `tell` sent while a
    child waits out backoff is handled before the retry deadline.
16. `unsupervised_child_death_does_not_restart` — `unsupervise` then kill ⇒ no
    rebuild (the #195 `Unwatch`-race carry-forward).
17. `on_stop_error_marks_cleanup_failed` — a child whose `on_stop` returns `Err`
    delivers `LinkDied { cleanup_failed: true }` with the original `reason`.
18. `supervisor_holds_no_strong_child_ref` — dropping the last user `ActorRef` to
    a supervised child still triggers ref-count-driven stop (ADR-0003; kameo
    #171). Verified with the counting allocator / weak-count assertion.

## Verification beyond unit tests

- **Mutation:** `cargo-mutants` zero new survivors on the supervision module;
  baseline entries added for every new function (per the mutants-baseline rule —
  new functions without entries report as `Unaccounted`).
- **MIRI:** the supervision tests join the existing two-leg lane; any proptest is
  named `prop_*` so the MIRI sweep skips it.
- **Timing determinism:** every backoff/reset test runs `start_paused`, never a
  settle-with-timeout.
- **DST:** the restart-storm and concurrent link/unlink/die races are slice 2b's
  burden and must be filed as a card before this one closes (#166).

## ADRs produced

- **ADR-0012** — restart accounting: consecutive counter (reset on healthy uptime) + never-reset lifetime budget;
  `governor`/GCRA and the OTP timestamp window both rejected, with the
  rate-vs-give-up argument.
- **ADR-0013** — virtual-actor (Orleans) lazy reactivation deliberately not taken
  in the core; belongs to `bombay-nexus`.

## Open questions

None blocking. Two items are explicitly bounded rather than open: slice 2b's
strategy ladder (filed before this card closes) and the nexus log-rehydrate
rebuild path (the factory closure is already the seam — a nexus child's factory
rehydrates from the event log instead of taking plain `Args`).
