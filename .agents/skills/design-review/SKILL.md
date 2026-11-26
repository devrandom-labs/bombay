---
name: design-review
description: Judge the design of one Bombay crate, of the layer/context map or of one logic step with fixed lenses - one language, hidden domain, hidden state machine, typestate and validated construction, known problem with a known solution, crate or protocol that deletes code, pattern fit, ownership and allocation, boundary leaks. Use before a new crate or context is built, at the end of a plan stage, inside the review of a logic step, or when Joel asks whether something is designed properly. Not for checking a change against its acceptance; use review for that.
---

# Design review

A step review asks whether a change is correct. A design review asks whether
the design is right. No gate and no static analysis can answer that question;
this procedure asks it the same way each time. Root AGENTS.md says when it runs
and who runs it.

## Scope

Take exactly one scope for a session.

| Scope | Read | Do not read |
| --- | --- | --- |
| Crate | One crate or module tree, its tests and the interfaces that it uses | Other crates' bodies |
| Map | The context map of the owning architecture document and the public interface of each crate | Function bodies |
| Change | The changed code of one `logic` step and the crate around it | Unrelated modules |

Read the owner documents of the scope first: its architecture document and the
ADRs that it names. A design that an accepted ADR settles is not a finding
unless you have a concrete conflict or new evidence (AGENTS.md, Authority).

## Signals

Signals show where to read. They are not findings. Run the commands below from
the repository root and pass their output to the cheap worker of the
[bindings](../../project.md#models) to format one table, or let the read-only
Codex worker run them directly. `S` is the scope's source directory, such as
`crates/bombay-runtime/src`.

Keep measurable design rules in automated fitness functions. Use coupling
and dependency direction to find unstable boundaries (Ford, Richards,
Sadalage and Dehghani, *Software Architecture: The Hard Parts* (2021),
ch. 1, 2, 4).

| Signal | Command | Points to |
| --- | --- | --- |
| State flags | `rg -c '^\s*(pub(\(crate\))? )?\w+: (bool\|AtomicBool\|Option<)' -g '*.rs' S` | A hidden state machine |
| Tuple matches | `ast-grep run -p 'match ($A, $B) { $$$ARMS }' -l rust S` | A hidden state machine |
| Hand-made machinery | `rg -c 'loop \{\|sleep\(\|from_[bl]e_bytes\|impl Iterator for\|impl Future for\|impl Drop for' -g '*.rs' S` | A known problem, a crate or a protocol |
| Allocation | `rg -c '\.clone\(\)\|\.to_owned\(\)\|\.to_string\(\)\|Box::new\|collect::<Vec' -g '*.rs' S` | Ownership |
| Shared state | `rg -c 'Arc<\|Mutex<\|RwLock<\|mpsc::\|oneshot::\|broadcast::' -g '*.rs' S` | Ownership |
| Churn | `git log --since='30 days ago' --name-only --format= -- S \| sort \| uniq -c \| sort -rn \| head -20` | A hidden domain, an unstable boundary |
| Public surface | `rg -c '^\s*pub (fn\|struct\|enum\|trait\|const\|type) ' -g '*.rs' S` | A wide interface |
| Imports from other contexts | `rg -o 'use crate::\w+' -g '*.rs' S \| sort \| uniq -c \| sort -rn` | A boundary leak, a hidden domain |

## Lenses

Apply every lens. For a lens with no finding, write "none" and what you read.

1. **One language.** Does each name mean one thing in this scope? Look for one
   word with two meanings, two words for one thing, and a type whose fields
   serve two different callers. Check the naming and signature rules in
   [Bombay code rules](../rust-best-practices/references/bombay.md#signature--module-integrity).
2. **Hidden domain.** Look for a group of types and functions that change
   together, share a vocabulary and are used through a few calls, but live
   inside another context. Name the context, its language and its owner
   document.
3. **Hidden state machine.** Look for two or more flags or options that change
   in sequence, a `match` on several flags, a method that is valid only after
   another call, and comments with "must", "before", "after" or "only once".
   Name the states and the transitions. Apply the state rules in
   [Bombay code rules](../rust-best-practices/references/bombay.md#signature--module-integrity).
   Choose an `enum` state when run-time data selects the state. Choose typestate
   when the order is known at compile time and a wrong order is a programmer error
   ([typestate](https://docs.rust-embedded.org/book/static-guarantees/typestate-programming.html)).
4. **Construction.** Is a value checked again after it exists? Look for the same
   validation in two places, a constructor that returns an unchecked value, a
   builder that checks required parts only at the end, and public fields on a
   type with an invariant. Check the construction and boundary-parsing rules of
   [Bombay code rules](../rust-best-practices/references/bombay.md#runtime-and-sdks).
   Name the parsed type or the typestate builder that removes the later checks
   ([parse, don't validate](https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/);
   [builder](https://rust-unofficial.github.io/patterns/patterns/creational/builder.html)).
5. **Known problem.** Is this code a well-known problem under a local name:
   scheduling, backpressure, retry with backoff, a cache, an interval set, a
   ring buffer, a clock mapping, a parser, a rate limit, a lease or a
   reconciliation loop? Check the queue-bound and fencing rules of
   [Bombay code rules](../rust-best-practices/references/bombay.md#runtime-and-sdks).
   Name the problem, a primary source for its solution (a paper, an RFC,
   standard-library or crate documentation) and what the known solution changes
   here.
6. **Reuse.** Does the standard library, the accepted stack or a maintained
   crate do this work? Follow the reuse rules of
   [Bombay code rules](../rust-best-practices/references/bombay.md#reuse-and-dependencies).
   Name the crate, the documentation of the locked version, the lines that it
   deletes, and the behavior that stays or is lost.
7. **Pattern fit.** Does an idiom or pattern of
   [Rust Design Patterns](https://rust-unofficial.github.io/patterns/) solve a
   problem that this code shows? Is a pattern present that solves no problem
   here? Check the module, signature and error rules of
   [Bombay code rules](../rust-best-practices/references/bombay.md#signature--module-integrity).
   Name the pattern and the problem.
8. **Protocol and wire.** Look for a hand-made message format, framing,
   handshake, identifier scheme or timing exchange. Does an existing protocol
   or a supported feature of the engines carry it? Name the specification
   section or the API.
9. **Ownership and allocation.** On hot actor-dispatch and wire paths, look
   for a clone, an owned copy, a collected `Vec` that is read once, a boxed
   trait object, and each `Arc`, lock or channel. Check the async-blocking and
   lock-guard rules of
   [Bombay code rules](../rust-best-practices/references/bombay.md#runtime-and-sdks)
   and borrowing rules of
   [Signature & Module Integrity](../rust-best-practices/references/bombay.md#signature--module-integrity).
   Can the code borrow, move, use a slice, an array or an iterator? Does each
   shared-state primitive have a sharing need? A performance claim needs a
   measurement.
10. **Boundary.** Does this scope know a type, an error, a constant or a timing
    assumption of a layer above or beside it? Does its public interface show
    more than its consumers use? Does a test need the real lower layer to check
    a rule of this layer? Check [ADR 0001](../../../docs/adr/0001-layer-bombay-crates.md)
    and the fake rules in [Tests](../rust-best-practices/references/bombay.md#tests).

## Findings

Write each finding in this form. Rank findings by the complexity that they
remove.

```text
<lens>: <claim in one line>
Where:    file:line, and the other sites
Today:    what the code does, with at most five quoted lines
Concept:  the pattern, problem, crate or protocol, with its source
Change:   what to do; the lines removed or the states that become impossible
Cost:     behavior lost, migration, tests that move
Kind:     step | decision for Joel
Evidence: EXECUTED | OBSERVED | REASONED
```

- A finding without a `file:line` and a named concept is an opinion. Leave it
  out.
- Check each crate or protocol claim in its documentation for the locked
  version. Never state an API from memory.
- A finding that moves a context boundary, changes the stack or changes a
  product rule is a decision for Joel. Every other finding is a step.
- Change no code. The lane that owns the files turns each accepted finding into
  a card or a step.

## Weekly review

The [automatic run](../../project.md#automatic-run) starts this review. The
lead of that session does the steps below with the commands of the
[bindings](../../project.md#models). It changes no code, and it ends with the
result line that the bindings give for a review.

1. **Scope from the numbers.** Each card of this review has a title of this
   exact form: `Design review <lane> <week>: <scope>: <claim>`. The lane and
   the week are the ones in the prompt of the run, such as `studio` and
   `2026-46`. First count the cards, open or closed, whose title starts with
   `Design review <lane> <week>:`; an earlier try of this review can have
   made some. When there are five, end with a review of 0 cards. Then run
   `just report <lane>` and take the first two code scopes of its last
   section. A scope `…/src/<name>` is the file `src/<name>.rs`, the folder
   `src/<name>/`, or both, as they exist on `origin/main`
   (`git ls-tree origin/main <path>`). The last two `STEP reviewed` lines of
   `.cache/run.history` name the scopes of the last two reviews: leave such a
   scope out and take the next one of the list, unless the scope is first in
   the report again. When the report has one scope, review that one. When it
   has none, end with a review of 0 cards. When it shows "n/a" for the
   scopes, end with `STEP failed` and the reason.
2. **The scope as it is on main.** The worktree can be on the branch of an
   open step. Do not change that branch and do not stash its work. Write the
   scopes as they are on `origin/main` into an empty folder:
   `rm -rf .cache/design-review-main && mkdir -p .cache/design-review-main &&
   git archive origin/main -- <scope paths> | tar -x -C .cache/design-review-main`.
   Give `git archive` only the paths that exist on `origin/main`. When the
   folder is empty afterwards, end with `STEP failed` and the reason. The
   worker and both judges read the code only in that folder.
3. **Signals and alternatives.** Run the signals of each scope on that folder
   (the churn signal reads `origin/main`) and pass their output to the cheap
   worker of the [bindings](../../project.md#models), or let the read-only
   Codex worker run them directly. Then, for each main responsibility of the
   scope (five at most), the cheap worker searches the web and lists the other
   ways to do that work: the standard library, the accepted stack, maintained
   crates, a known algorithm or a protocol. Each entry has a primary source
   (the documentation of the locked version, the repository, a paper or an RFC)
   and one line on what it would delete or cost here. The worker does not
   judge.
4. **Two judges.** Give each judge, in one session for each scope and judge:
   the folder of the scope, the signals, the alternatives, the path of this
   skill (its lenses and its finding form), and the owner documents of the
   scope. Each applies every lens and writes findings in the form above.
   Neither sees the other's findings.
5. **Agreement.** A finding counts when both judges report it, or when one
   judge reports it with evidence that the other confirms in a second, short
   session. Drop the rest. Drop a finding that lowers a quality limit of the
   media path ([AGENTS.md](../../../AGENTS.md#studio)). Leave out a finding
   whose scope and concept a card counted in step 1 already names. Keep the
   findings that
   remove the most complexity, for both scopes together, until the week has
   five cards with the ones counted in step 1. Only those kept findings are
   the accepted findings of the week.
6. **Measured claims only.** A finding that says "faster" or "less memory" is
   a hypothesis. Its card starts with a step that measures the two variants
   (`hyperfine`, or a benchmark of the crate). The change is a later step,
   and only when the measurement supports it. The acceptance of the change
   step also holds when the card records a measurement that does not support
   the change.
7. **Cards, not a document.** Each kept finding becomes a card with the title
   form of step 1: Kind Task, the Workstream of the lane and the assignee
   joeldsouzax. Take the Phase of the card that `just next <lane>` names, and
   a Sequence just above the Sequence of that card, or no Sequence when that
   card has none, so that the run finishes its open step first.
   When it names no card, take Phase 1. The body has the finding and a
   `## Steps` list that [to-spec](../to-spec/SKILL.md) and the
   [bindings](../../project.md#steps) describe. The run takes these cards
   like its other cards. A finding of the kind "decision for Joel" is
   recorded as [AGENTS.md](../../../AGENTS.md#steps) says for a decision that
   is Joel's, so the run leaves it until he answers.
8. **Quality of the merged steps.** After the cards of step 7 exist, do the
   [quality review of merged steps](#quality-review-of-merged-steps) for the
   same lane and week.

## Discovery review

A discovery review looks for three things in one scope: a concept of the
business that has no type of its own (hidden domain), a rule of the business
in the wrong place or in several places (hidden business logic), and code that
a clearer model or an existing facility deletes (concision). It is a design
review with a fixed input, a fixed procedure and a recorded result, so that a
second run on the same commit gives the same facts and can be compared.

It runs when Joel asks, and for a scope whose design facts rose since the last
review of that scope. One session reviews one scope; it changes no code.

1. **The input is the facts file, not a search of your own.** Run
   `bash architecture/design/facts.sh <scope>` on the commit that you review and read its
   output first. It counts, with every place: the same name with the same
   primitive type across signatures and fields; the same filter or condition
   written in several places; an enum that copies the variants of another
   enum; a state compared with a string; a rule of the database (a check, a
   trigger, a function) whose name no application code holds; and the files
   that the most work items changed. A finding starts from a row of that
   file. A thing that you see and that no row holds is a gap of the tool:
   name it under "Signals" in the record, so that the tool gets the signal.
2. **Read before you judge.** Open the places of a row. Report a finding only
   when the code confirms it. Check it against the accepted design
   ([AGENTS.md](../../../AGENTS.md#design)) and against the
   [decision principles](../../../docs/arch/principles.md): propose no layer,
   pattern or dependency that they forbid.
3. **The finding has the form of this skill**, and three more fields: the
   lens (hidden domain, hidden business logic, concision), the proposed form
   as Rust signatures, and what it deletes or which wrong state it makes
   impossible. A finding that changes a rule of the product, a boundary
   between contexts, the stack or a test oracle is Joel's.
4. **A finding has an identity:** the scope, the lens and the concept in one
   word, for example `server/models hidden-domain actor`. Before you keep a
   finding, search the cards, open or closed, for that identity. A finding
   that a card already holds is not made again.
5. **Check each kept finding with a command.** For each place, a search that
   prints the line. Put the commands and their output into the card. A
   finding whose places a command does not show is dropped.
6. **Cards and the record.** Each kept finding of severity High or Medium
   becomes a card, as the weekly review makes them, with its identity in the
   body. The Low findings, the findings for Joel, the words of the business
   that have no type, and the rules that live only in the database go into
   one record card of the review, with: the commit, the scope, the hash of
   the facts file, the model and its effort, and the list of the cards made.
   That card is the proof of the run and the base of the next one.
7. **A finding that returns becomes a rule.** When a review finds, a second
   time, a case of a class that an earlier card repaired, the card of the
   second case also adds the rule to the architecture gate, with its test
   ([AGENTS.md](../../../AGENTS.md#rules-about-rules)).

## Quality review of merged steps

A step that passed its review and the gate can still hold a weak design or a
test that proves nothing. This review reads a sample of the merged steps of a
lane again, some days later, and judges the removed assertions of the week. Its
counts are the measure of the value of the work; the count of merged steps is
not. It is part of the weekly review of a lane. It changes no code.

1. **The input is fixed.** The commits of `main` of the last seven days that
   changed the files of the lane. Run
   `bash architecture/quality/facts.sh --commits <first>..<last> <scopes of the lane>`
   and `bash architecture/design/facts.sh <scopes of the lane>` on the last
   commit, and read both outputs first.
2. **The sample is chosen by a rule, not by the reviewer.** Take the merged
   pull requests of the lane in those commits whose number, divided by 5, leaves
   the same remainder as the number of the week. A second reviewer gets the
   same sample.
3. **Judge each step of the sample** with the lenses of this skill for the
   design, and with the test rules of
   [AGENTS.md](../../../AGENTS.md#code-tools-and-tests) and of the
   [test skill](../rust-tests/SKILL.md) for the tests: does each test call the
   real code, can it fail, does it assert the exact result, and would it catch
   the plausible defect of the change. Remove one condition of the change in
   your head and name the test that fails; when no test fails, that is a
   finding.
4. **Judge each commit of the week that removed assertions** (the rows
   `oracle-edit`, with the `Oracle:` trailer of the commit): a move, a
   corrected expectation with its reason, or a weaker test. A weaker test is a
   High finding, and it goes to Joel on its card, because a change of a test
   oracle is his.
5. **Findings become cards** as in the weekly review, with the identity of a
   discovery finding. A class of finding that returns becomes a rule of the
   gate.
6. **The record card of the week** holds: the commit range, the hashes of the
   two facts outputs, the sample, for each step of the sample the findings by
   severity, the judgment of each commit of point 4, and the cards made. Two
   numbers stand in its first lines, in this exact form, because the measure
   reads them: `Steps reviewed: <n>` and
   `Steps with a High or Medium finding: <m>`.
