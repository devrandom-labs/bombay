# Review — `registry.rs` + `mailbox.rs` (the actor-core registry & mailbox)

**Brief:** critical code review of *usage*, not *choice*. papaya / flume are settled
(ADR'd). Question: is the code *around* them idiomatic, correct in its API use, and
efficient by construction rather than by the optimizer's grace?

**Method (this review):**

1. Read module docstrings (`registry.rs:1-49`, `mailbox.rs:1-24`) — states what is settled.
2. Read `.auto/refs/primitives.md` — the papaya/flume/Std correctness reference.
3. Read the source: `registry.rs` (`register`/`lookup`/`unregister` + `ErasedEntry`),
   `mailbox.rs` (`Capacity`, `Signal`, `MailboxSender`/`Receiver`/`Weak`, `SendMessageFut`),
   and the referenced wrappers `actor/actor_ref.rs` (`ActorRef`/`WeakActorRef` liveness) and
   `error.rs` (the project's `thiserror` convention).
4. `cargo check -p bombay-core` → **clean** (grounds the "compiles" claim; it is *not* the verdict).

Verdict up front: both files are **genuinely good** — idiomatic, correct primitive use, no
"relying on the compiler" smells. One concrete rule-compliance gap (`CapacityError` bypasses
`thiserror`); one advisory deviation (`SendError`/`TrySendError` don't impl `Error`, defensible).
The ranked table + per-dimension evidence below is the audit, not a rubber-stamp.

---

## Ranked findings

| Severity | file:line | What's wrong | Idiomatic fix |
| --- | --- | --- | --- |
| **MEDIUM** | `mailbox.rs:66-88` | `CapacityError` hand-rolls `impl fmt::Display` + `impl std::error::Error` instead of `#[derive(thiserror::Error)]`. This violates the repo's error law ("all error types use `thiserror` — no manual `Display`/`Error` impls"; `~/.claude/CLAUDE.md`). Every sibling error — `NameTaken`, `WrongActorType`, `PanicError`, `ActorStopReason`, `TellError`, `AskError` — derives `thiserror::Error`. Runtime behavior is identical, so impact is *clarity/maintainability/consistency*, not correctness; but it is the one real conformance gap. | `#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]` on `CapacityError`, attach `#[error("mailbox capacity must be at least 1")]` / `#[error("mailbox capacity exceeds the maximum")]` to `Zero`/`TooLarge` (mirror the current `Display` text), then **delete** the manual `impl fmt::Display` and `impl std::error::Error`. Keep the existing `#[expect(clippy::exhaustive_enums, …)]`. |
| **LOW (advisory)** | `mailbox.rs:408-438` | `SendError<A>` / `TrySendError<A>` deliberately do **not** impl `std::error::Error` (only `Debug`). This deviates from the `thiserror` norm, but is *defensible*: they must carry the undelivered `Signal` back (rule 3: never drop the payload) and are matched concretely by callers, not propagated via `?`. | Leave as-is unless a caller needs `std::error::Error` (then derive `thiserror::Error` with `#[error("…")]` and keep the payload fields). **Human decision** — not a defect. |

No other defects found. The remaining audit dimensions below are *verified correct* (evidence,
not assumption) so the reader can see the check was done.

---

## Audit by dimension (with evidence)

### 1. Idiomatic Rust

- **Combinators over ladders:** `lookup` uses `let … else` (`registry.rs:154-156`), `?` with
  `ok_or` in `TryFrom<usize>` (`mailbox.rs:102`), `.map_err` in `try_send`/`send`
  (`mailbox.rs:239,250`), `.filter(ActorRef::is_alive)` (`registry.rs:161`), `.ok()` in `recv`
  (`mailbox.rs:345`). No imperative match ladders where a combinator reads cleaner.
- **Borrow-before-own:** every `clone()` is load-bearing, not gratuitous:
  - `register` clones the `WeakActorRef` *inside* the `compute` closure per attempt
    (`registry.rs:123`) — required because the closure may re-run under contention; documented.
  - `send_message`/`try_send_message` clone `self` into the `Signal::Message.self_sender`
    (`mailbox.rs:272,288`) — the ADR-0003 self-pin; this is a cheap `Arc` clone of the
    `flume::Sender`, not a heap alloc.
  - `ActorRef::clone` / `WeakActorRef::clone` use `Arc::clone` (`actor_ref.rs:46,188`).
- **Manual `Clone` impls are correct, not debt:** `MailboxSender`/`WeakMailboxSender`/`ActorRef`/
  `WeakActorRef` hand-write `Clone` (`mailbox.rs:220-226,327-333`; `actor_ref.rs:42-49,184-191`)
  specifically to avoid `#[derive(Clone)]`'s spurious `A: Clone` bound. This is the *right* call;
  deriving would over-constrain `A`. **Strength, not a smell.**
- **`use` hygiene:** all imports at file top (`mailbox.rs:26-35`); no inline `use`, no deep path
  qualification.

### 2. Proper primitive API use (per `primitives.md`)

- **papaya guard never held across `.await`:** every registry op is synchronous; the guard
  (`registry.rs:113,153,170`) is dropped at function exit, never across a suspension point. ✓
- **register-once = single `compute`, no get-then-insert race:** `register`
  (`registry.rs:118-128`) runs the whole claim decision (liveness check + insert) inside one
  `compute`; a `get`-then-`insert` would be a check-then-act race. Reclaim of a dead incumbent is
  `Operation::Insert` (papaya replaces on existing key → `Compute::Updated`, handled at
  `registry.rs:130`). ✓
- **flume disconnect = lifecycle, never `unwrap`'d:**
  - `recv` → `recv_async().await.ok()` (`mailbox.rs:345`): `Disconnected` (after drain) becomes
    `None` — the run-loop's "actor gone" signal. ✓
  - `try_send` maps `Disconnected` → `TrySendError::Closed` carrying the signal
    (`mailbox.rs:252`). ✓
  - `send` maps the error → `SendError(err.into_inner())` returning the **full** `Signal`
    (`mailbox.rs:239`) — payload never silently dropped (rule 3). ✓
- **`Weak` liveness uses both legs:** the single liveness rule (channel open) is enforced with
  *both* the upgrade and the channel-open check, identically on every path:
  - `ErasedEntry::is_alive` = `self.upgrade().is_some_and(|strong| strong.is_alive())`
    (`registry.rs:69`) — upgrade alone would miss the reaped-but-referenced state.
  - `lookup` = `weak.upgrade().filter(ActorRef::is_alive)` (`registry.rs:161`) — same two-leg test.
  - `WeakMailboxSender::upgrade` (`mailbox.rs:322-324`) and `ActorRef::downgrade`/`WeakActorRef::upgrade`
    (`actor_ref.rs:163-168,216-221`) follow the same non-pinning contract. ✓

### 3. "Relying on the compiler" — smells

**None found.** No `Vec`/`String` built then discarded; no `.collect()` into a throwaway; no
manual index loops; no hot-path clone beyond the necessary `self_sender` self-pin. `drain` returns
a lazy `impl Iterator` (`mailbox.rs:353`). The only "allocations" are the inescapable ones:
`Box<dyn ErasedEntry>` (the type-erasure the map requires) and `Box<LinkDied>` (cold control path,
keeps the hot `Signal::Message` slot small — large-variant discipline, `mailbox.rs:151-163`).

### 4. Rule compliance (`~/.claude/CLAUDE.md` + `./CLAUDE.md`)

- **Arithmetic safety:** no size/offset math in these files. `Capacity::MAX = usize::MAX >> 3`
  (`mailbox.rs:47`) is a const shift, no overflow; `NonZeroUsize` excludes 0. ✓
- **No `unwrap`/`expect`/panic on production paths:** confirmed none in non-test code. The only
  panics are `unreachable!` programmer-bug guards — both justified per "panics are for programmer
  bugs": `register`'s `Compute::Removed` arm (`registry.rs:135`, the closure never returns
  `Remove`) and `SendMessageFut`'s `Stop`/`LinkDied` arm (`mailbox.rs:396-398`, `send_message`
  enqueues only `Signal::Message`). These cannot be reached by caller input. ✓
- **One error variant per failure domain:** `NameTaken` / `WrongActorType` are bare structs
  (single-domain fallible ops, `error.rs:326-339`); `CapacityError` is a 2-variant enum with one
  variant per invalid reason. ✓ (modulo the `thiserror` gap above)
- **`pub(crate)` for internals:** `ErasedEntry` is module-private (`registry.rs:56`, no `pub`) —
  correct encapsulation; `Box<dyn ErasedEntry>` is the only externalized surface. Public items are
  all intended API (`Registry`, `Mailbox*`, `Signal`, `Capacity`, `SendError`/`TrySendError`). ✓

### 5. Encapsulation / invariants

- **Illegal states unrepresentable:** `Capacity` (`mailbox.rs:41-64`) excludes 0 via
  `NonZeroUsize` and the upper bound via `new`'s check, so `Mailbox::bounded` is infallible by
  construction (`mailbox.rs:208-211`) — no runtime `if cap == 0` anywhere. ✓
- **`ActorId(u64)`** (`mailbox.rs:123-132`) and `WeakActorRef`/`ActorRef` fields are private
  (`actor_ref.rs:37-40,179-182`); only `id()`/`downgrade`/`upgrade` expose controlled views. ✓
- **`Signal<A>` is exhaustively closed** (`mailbox.rs:169-173`, `#[expect]` with rationale) — the
  run-loop is a total `match`. This matches the project's "exhaustive enums (rule #3)" convention
  seen in `error.rs` (`ActorStopReason`, "Exhaustive … no `#[non_exhaustive]`, rule #3"). ✓
- **The anymap trick is done right:** `ErasedEntry::as_any` returns `self` (the concrete
  `WeakActorRef`, `registry.rs:72-74`), so `downcast_ref::<WeakActorRef<A>>()` preserves the type
  id and resolves correctly even through the `Box<dyn ErasedEntry>`. A common bug is returning
  `&dyn ErasedEntry` coerced to `Any` (wrong type id); this code avoids it. ✓
- **The self-pin cycle is broken, not leaked:** `Signal::Message` embeds a strong `MailboxSender`
  (`mailbox.rs:182-187`). Because flume's `Receiver::drop` does *not* purge its queue, that
  sender would form a `Shared → queue → Signal → Sender → Arc<Shared>` cycle on a hard kill —
  `Drop for MailboxReceiver` drains and drops the backlog (`mailbox.rs:366-368`), releasing the
  embedded senders. This is the *essential* leak fix and it is present and correct. ✓

---

## Per-file verdict

### `registry.rs` — GENUINELY GOOD

Atomic register-once via a single `compute` (no check-then-act race); the "dead reads as absent"
rule is enforced *consistently* across `lookup` and `register` through the shared two-leg
`is_alive` (upgrade + channel-open); clean combinators (`let … else`, `?`, `.filter`); correct
type-erased downcast; guard never crosses an `.await`; no optimizer-dependent code. **No debt.**
The one conformance gap lives in `mailbox.rs` (`CapacityError`), not here.

### `mailbox.rs` — GENUINELY GOOD (one real defect)

Bounded-only by construction (`Capacity` makes 0/invalid unrepresentable); flume disconnect is
treated as lifecycle on *every* path (`recv`→`None`, `try_send`→`Closed` carrying payload,
`send`→`SendError` carrying payload); the ADR-0003 self-pin cycle is correctly broken by
`Drop::drain`; `SendMessageFut` is a correctly-named, `Unpin` future (`mailbox.rs:384-401`,
re-pins soundly without projection); `MailboxSender`/`WeakMailboxSender` manual `Clone` impls are
*intentional* (avoid `A: Clone`). **No "relying on the compiler" smells, no hot-path clones beyond
the necessary self-pin.** One real defect: `CapacityError` bypasses `thiserror` (MEDIUM, above).
One advisory: `SendError`/`TrySendError` don't impl `std::error::Error` (LOW, defensible).

---

## Bottom line

Neither file leans on the optimizer or carries non-idiomatic debt. The *choice* of papaya/flume is
ADR'd and the *usage* around them is correct. The single change worth making is the `thiserror`
derivation for `CapacityError`; everything else is sound as written. (This verdict is from reading
the source, not from the clean `cargo check` or any benchmark — a green build proves nothing about
idiom.)
