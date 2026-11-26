---
name: harness-owner
description: Keep the automatic runs of the lanes moving - measure what the lanes produced, find the largest stall, repair its cause in the harness, the cards or the host, and record each decision where every session can read it. Use when Joel starts a session as the harness owner, for a harness check, when a lane is slow, parked or only waits, when a gate or a harness test fails for a reason outside the step, or when a machine of the lanes misbehaves. Not for the work of a step; a lane's lead does that.
---

# Harness owner

The lanes do the steps. The harness owner makes the lanes fast and correct: the
supervisor, the card wiring, the lead instructions, the machines and the
landing of harness steps. [AGENTS.md](../../../AGENTS.md) holds the rules; this
skill is the procedure. It adds no rule.

## What the owner decides

Decide and act, then report with a decision number:

- a card's wiring: its board fields, its dependencies, its order, a note for the
  next lead;
- a restart of a run that is idle or ended;
- a harness step: its specification, its repair rounds and its landing;
- the instructions of a lead in the [bindings](../../project.md);
- a question that a lead put under `needs-joel` and that is a choice of
  procedure, not one of Joel's decisions below: answer it on the card, remove
  the label, and start the run again when it parked the card;
- a new card for a failure or a finding that has no card.

Joel decides, on a card with the alternatives and a recommendation
(`needs-joel`): product, scope, stack, a quality limit, a test oracle, a rule
of AGENTS.md, a new lane or worktree, a deployment, a credential, a payment,
and each change of a machine (a restart, an update, a setting). When he has
decided, record his words on the card.

Never touch another lane's worktree, branch or cache. Read them; write only
cards.

## The check

Run it when the session starts, then at the cadence that Joel set. Take the
time from the clock, never from an estimate.

The tools decide, not memory. Every tool of the owner is in one registry
(`bash architecture/owner/tools.sh list <state folder> <worktree parent>`), and
a check runs each tool of the kind `check`. Before you state a number, an
order, a quality or what a decision releases, run the tool that measures it and
give its numbers. When no tool measures it, say so and build the tool.

1. **Read the state.** The owner's record of this machine, the open harness
   steps and the decisions that are still open.
2. **Measure the outcome first, then the activity.** Outcome: cards merged and
   closed, the cards that block the most cards, the state of the last Checks
   run of `main`, cards that no run can select, the facts of the design and of
   the tests and what changed in them. Activity, for each lane from its
   `run.history`: sessions, merged steps, gates and their minutes, waits for the
   merge turn, parks and set-aside cards. Host: free disk, memory, swap, load.
   From the Nix shell, run `bash architecture/owner/check.sh [hours]`. It brings
   the checkout to `main`, runs `architecture/measure.sh` and then every check
   tool. Its last line says how many tools passed, failed and are absent;
   repair a tool that failed or is absent in the same check.
   Use a GitHub CLI login with access to the project board and repository,
   or set `GH_TOKEN` for that account.
3. **Find the largest stall.** Use this order:
   1. the machine (memory, disk, a host that slows every gate);
   2. a lane that does not run, or that runs and only waits;
   3. a parked card whose blocker is merged or gone;
   4. a card that waits for Joel, and a card that no run can select;
   5. a failed Checks run of `main` with no card that someone works;
   6. gate cost: gate minutes for each merged step, waits for the merge turn;
   7. the cards that block the most cards: is a lane on them, did they move;
   8. the owner's own open harness steps.
4. **Act** on the first stall that is yours. Bring the others to Joel in one
   line each. Close each card that the tool `merged-open` lists
   (`bash architecture/cards/close-merged.sh close`): the progress graph of
   the project counts closed issues.
5. **Report**, shortest first: the quality numbers and what changed in them,
   what moved (count plan cards, the cards with a Task ID), what you changed
   and what it should change in the next numbers, what waits for Joel. Merged
   steps do not show that the work is good.
6. **Record** the same facts where the next session finds them.
7. **Watch.** Start `bash architecture/watch-parks.sh <worktree parent> <cards>`
   in the background with the cards that block the most cards. It ends on a
   park that waits for no other card and on any park of a named card. Read
   that card at once, then start the watcher again.

A lane that works is not a stall. Do not react to an event that a run handles
by itself.

## Evidence

- Measure before you state a cause. A number comes with its source and its
  time. "Not measured" and "not known" are valid results; write them.
- A cause is a prediction that one check can refute
  ([Diagnosis](../../../AGENTS.md#diagnosis)). Name the check before you run it.
- Before you blame a step for a failure, search the retained logs and cards for
  the same failure text on `main`.
- When a later measure contradicts what you wrote, correct it in the same
  place (the card, the report) and say that it is a correction.
- A measurement must not disturb the lanes more than its answer is worth. Run
  heavy probes on the idle machine or keep them short.

## Patterns

Each pattern is: the signal, the first check, the act. Add a pattern when a new
kind of problem is solved. When the problem shows what any harness must do,
also add a row to the [lessons](references/lessons.md): they are the
requirements for the next harness. The [stack record](references/stack.md)
names each tool and mechanism of the harness and why we use it; change its row
when you change a tool.

| Signal | First check | Act |
| --- | --- | --- |
| A card that blocks many cards is parked on another card | Read the blocker: is its cause a state of the machine at that time, or a defect in code? Read the host record of the same minutes. | For a host cause: put the host record on the blocker, remove the dependency, and let the next full gate of the parked step be the check. A run that parked the card keeps the park until the blocker is Merged, so start that run again between a gate and a session. Keep the blocker open for its diagnosis. State how to reverse. |
| A lane runs and merges nothing | List its ready cards and the wait reason of each from `run.history`. | Remove the cause that the waits have in common (a red Checks run, a harness step that is open, a missing board field). |
| Every gate gets slower | Memory, swap and the largest kernel zone against the gate minutes of the day. | Ask Joel for the host act at once, with choices and their cost. Do not wait for the next check. |
| A harness test fails once and passes on the same commit | The earlier records of the same text; the lines of that case in the step's change; the load of the host. | Retain both logs on a card. State the failure in the results that the reviewer reads. Do not repeat runs until one is green. |
| A gate fails in a harness test while the step changed no harness file | The same suite on unmodified `main`, on a healthy host, in the environment of the gate (a test can pass as the ordinary user and fail in the build). | When `main` shows the same failure, it is the owner's, not the lane's: write it on the card so that the lane does not diagnose it, and tell the lane to request the gate again. When `main` passes, the step changed what the test runs in, and the lane keeps the failure. |
| A step waits for the merge turn for a long time | Which run holds the turn and what it gates. | Shorten what every gate pays (a check that does not depend on the step), before you reorder lanes. A stop of a lane is Joel's. |
| The status of a lane says "full gate" much longer than its last gates took | The gate log of the lane: a wait for the gate lock names the process that holds it, and `ps` names the command of that process. | When a measurement or a build of a lead holds the lock: write the facts on its card, and that a long build takes the lock for one piece at a time so that a waiting gate gets in. Do not stop a process of another lane. |
| Every lane fails in the same minutes with different errors: a name that does not resolve, a lost build service, a lead that cannot connect, a backlog that cannot be read | One request to each outside service from the machine, with the account and the path of a run. The first and the last error time in the logs of all lanes. | It is the network of the machine, not a step. When the services answer again, start each run that did not resume by itself. Write on each card whose gate failed in that time that the failure was the outage, so that its lead requests the gate again and does not diagnose it. |
| A lead asks Joel a question of procedure | Is it product, scope, stack, authority, a test oracle, or an act that only he can do? | When it is none of these: answer it on the card, remove the label and start the run again, because the park lasts as long as the run. When the same kind of question comes a second time, change the instructions of the lead. |
| A new model, tool or account is named | One small request with the exact name. | Change every site that names the old one: the lead command of the runs, the bindings, the owner's own jobs. A running lane takes a new lead only at a restart; restart it between a gate and its next session. |
| A job of a Checks run failed while the run still runs | The log of that job (`gh api --allow-escape-sequences repos/<repository>/actions/jobs/<id>/logs`), and each failed test against the open cards of its lane. | A test that no card names gets its card at once. The leads read only the last completed run, so a failure waits for hours without it. |
| A card that blocks many cards is parked behind a card that a lead made | The place of that card in the ready list of its lane. A card with no Sequence is the last one. | Give it a Sequence before the other cards of the lane (`architecture/cards/sequence.sh`) and say why on the card. |
| A lane sets a blocker aside again and again for a party outside the repository | Each card that depends on the blocker: does it need the open part, or only the steps that are merged? | Release a dependent that needs only the merged part: put a line of text in the place of the link in its Dependencies (`architecture/cards/release-dep.sh`), with a note that quotes the record and says how to reverse. A dependent that needs the open part keeps its link. |
| Open cards wait for each other in a loop | The members of the loop (the measure and the graph query `loops` print them), and which link names a part that is already merged. | Release that link in the same way. No run can select a card of a loop and no park reports it, so a loop stays until a check finds it. |
| The text of a step must change while a session works on the card | The last line of the lane's `run.history`. | Do not edit the step in work. Add a new step behind it, with a note that it asks for no change to the step before it. |

## A harness step

A harness step follows the step rules of AGENTS.md as any other step does.

- A tool of the harness is born in the repository: a specification, Codex in a
  clone, a test with exact output, an independent review, the gate. The state
  folder of a machine holds records, snapshots, logs and clones. A script that
  runs a second time does not stay there.
- No tool holds a path of one machine or the name of one person. The values of
  the project are in `architecture/project.env`; a second project changes that
  file and nothing else.
- A new measure, scan or query is finished when it has a line in the registry
  (`architecture/owner/tools.sh`) and, when it gives facts, a loader into the
  graph (`architecture/graph/build.sh`). A table that the graph does not hold
  is a debt.
- Give the step a card on the board, with its fields, before you start, and
  set the card to Merged when the step has landed. A lane that must wait for it
  then parks on that card and returns when it merges. A card that is not on the
  board when the lane parks is never checked again in that run, and without a
  card a lane can only wait for a fixed time.
- Work in a clone of the repository, never in a lane's worktree. Keep the clone
  where a restart of the machine does not remove it.
- Codex writes the code and the tests. The owner writes the specification and
  the rule text. One behaviour for each step.
- The specification lists what must hold, with the source of each basis. An
  accepted limit is written as the lead's decision, not hidden.
- Before the last review round, run one read-only adversarial pass by Codex
  with the same specification. Judge each point: repair it, or record why not.
- Give the reviewer the patch, the base files, the measured results as facts
  (failures included) and the list of mutations. Leave out conclusions.
- Prove a harness test in each environment that runs it: the Nix build on each
  platform and the ordinary user.
- Land under the merge turn, so that no lane merges between the gate and the
  merge. Batch rule-text changes into one gate. When a lane needs the gate for a
  card that blocks many cards, the lane goes first.
- After the merge, tell each card that waited for it, and start what it
  unblocks at once.

## Where state lives

| State | Place | Why |
| --- | --- | --- |
| A decision, with its reason and how to reverse it | A comment on the card that it changes | Every session and every machine reads GitHub |
| A decision that is Joel's | The card, label `needs-joel`, alternatives and a recommendation | He answers there or in the session; his words go on the card |
| A failure with no owner | A new card with the retained evidence | A comment on another card is not selectable |
| Rules, bindings, skills, tools | The repository | They change by a step, with review |
| The open work of the owner and how to continue it | [open.md](references/open.md), in the repository | A clone has it, so a new session on any machine continues from it |
| The design of an open piece of harness work | A file beside [open.md](references/open.md), named there | It is in the clone. It leaves the folder when the work is on `main` |
| Logs, clones, probe output, snapshots, the graph store, the running record | The state folder of the machine, outside the checkout (`bash architecture/harness.sh --where` prints it) | All of it can be made again, and a clean of the checkout does not remove a running job. Nothing that a new session needs is only there |
| What a check with no person did | `owner.history` and `owner-checks/` in the state folder | The next session on that machine reads it before it acts |

Name what is local to one machine when you write it down. A fact that the next
owner needs on another machine belongs on a card or in the repository.

## Tools

`bash architecture/owner/tools.sh list <state folder> <worktree parent>` names
every tool of the owner and what it is for. Keep no second list. The commands
that start things:

- `bash architecture/harness.sh up` starts what does not run on this machine:
  each lane that has a worktree, the two recorders and the owner's check loop.
  `bash architecture/harness.sh status` shows all of them.
- `bash architecture/lane.sh start <lane>`, `stop <lane>` and `status`. A stop
  of a lane is Joel's.
- `bash architecture/owner/session.sh talk` starts the owner's session with a
  terminal. See [Models](../../project.md#models) for the runner. `check` runs
  one check with no person. `loop start` runs checks at the set cadence. The
  tool holds the lock so that one owner acts at a time.
- `bash architecture/land.sh <clone> <branch> <title> <body file>` lands a
  harness step under the merge turn.

Also in the repository: `architecture/board.sh` (the board in one cheap read),
`architecture/backlog.jq` (modes `status`, `next`, `state`),
`bash architecture/run.sh turn <command> < /dev/null > <log> 2>&1` (run a
command under the merge turn; it refuses a terminal on any of its three
streams), `just gate` (the full gate).

From the Nix shell on macOS, run
`bash architecture/host-series.sh start <directory> <disk path>`
to take the first sample and record up to 24 hours of host state. Use a private
local directory for the logs. The disk path selects the filesystem to measure.
Use `bash architecture/host-series.sh status <directory>` to check the recorder
and `bash architecture/host-series.sh stop <directory>` to stop it. The recorder
appends to `host-series.tsv` and `host-top.tsv`. The kernel stream writes events
and any stream errors to `host-kernel.log`.

From the Nix shell on macOS, run
`bash architecture/diagnostics/kalloc-fast-probe.sh <count>`
to compare changes in the kernel zone across types of program starts. The count
sets the number of starts per phase.
Run `bash architecture/diagnostics/check-with-zone.sh <clone> <log folder>`
to report timing and zone changes for the checkout's shell self-tests and
`host-test`. Use a checkout that contains `architecture/tests/suites.sh` and
give each run its own log folder. The suite logs, zone samples and GitHub
output stay there. A question mark in a sample marks an unavailable zone reading.

The limits of an owner session are enforced by the hook
`architecture/owner/guard.sh`, for a session that `session.sh` started. Its
header names each rule with its source. The hook stops the direct forms of a
forbidden act; it is not a wall, and the rule holds when the hook does not see
an act.
