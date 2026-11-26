# Open work of the harness owner

This file is the first thing that the harness owner reads after its tools. It
names each piece of harness work that is not finished: what it is, where its
design and its code are, its state and the next act. A clone has it, so a new
session on any machine can continue. Change it in the pull request that
changes the state of a piece, and remove a piece when its acceptance holds on
`main`. It is not a log: it holds no history and no dated notes.

## In work

### The supervisor as an agent

A person clones the repository and runs `bash architecture/owner/session.sh talk`.
Antigravity CLI (`agy`) runs the harness owner with `gemini-4-carbon-high`.
The owner checks the machine, reports what needs a person and starts the workflow.
Codex and Claude Code keep their lane roles until 2026-12-05.

- See [Models](../../../project.md#models) for the runner and
  `architecture/owner/session.sh` for startup.
- Run `bash architecture/harness.sh doctor` to check what the runner needs.
  In the checkout whose lanes run, `bash architecture/harness.sh up` reports
  the running lanes and starts the recorders and the check loop.
- Next acts, in order:
  1. One harness for one board. Nothing stops a second clone, or a second
     machine, from starting the same lanes on the same board: `up` makes the
     worktrees and starts them. Give each lane a lease that every clone and
     machine reads before a start, and make `lane.sh start` refuse while
     another checkout holds it. Until then the rule under "What a new session
     must know" is the only protection.
  2. A start at login. It is a setting of a machine, so the owner of the
     machine applies it.
  3. The sandbox of the operating system for an owner session. The hook stops
     the direct forms of a forbidden act and is not a wall. The lanes must
     first be services that the session does not start as its own children.

### Telemetry of the harness

Time, tokens, cost and failures of each session and each gate, by lane, card
and step, in the OpenTelemetry form.

- Where: the design is [telemetry.md](telemetry.md). The code will be in
  `architecture/telemetry/`.
- State: designed. A probe proved the path for one Claude Code session.
- Next acts: the design review (step 0 of the design). Step 2 needs the
  packages of [#779](https://github.com/labcast/labcast-server/issues/779).

### The mutation score of changed code

- Where: [#774](https://github.com/labcast/labcast-server/issues/774), a card
  of the Nix lane (a nightly job).
- Next act: when it has merged, read its number in the measure and add it to
  the quality numbers of the report.

## Owed

- The test of the default range of 300 commits of
  `architecture/quality/facts.sh` does not fail for 299.
- The authorized change of `architecture/quality/authorized.tsv` is tied to
  its file and to the added paths, not to the variable that holds the OpenAPI
  document.
- The tests of the graph (`graph-converters`, `graph`, `graph-ready`) are not in
  `architecture/tests/suites.sh` yet. `oxigraph` is in the architecture check
  now, so add the three lines in a small step and run the build of the check
  before the merge turn.
- `architecture/backlog.jq`, `architecture/report.sh` and `architecture/run.sh`
  hold the assignee as a literal, and `architecture/board.sh` holds the
  project. They must read `architecture/project.env`, so that a second project
  changes one file. Until then an override of `HARNESS_ASSIGNEE` makes the
  graph query `ready` differ from the selection of the run.
- `architecture/owner/tools.sh`: a tool that starts a background job in its own
  process group survives the time limit of the check.
- `architecture/graph/build.sh`: a crash between the making of the lock folder
  and of its process entry leaves an empty lock that blocks every build until
  a person removes it.
- `architecture/owner/guard.sh`, for its next list revision: a push with
  `-c remote.<name>.mirror=true` passes; `git -c color.ui=never` on a lane is
  denied; in a command that the grammar can not parse, the runner word
  `claude` matches a part of a path such as `claude-501`.
- `architecture/owner/session.sh`: a session whose lock was taken over removes
  the lock of the new holder when it ends.
- `architecture/tests/owner-session-test.sh` fails when the path of its scratch
  folder is long: the history line is cut at 200 characters and the expected
  text is not.
- A discovery review of the Studio and contracts code by the procedure of the
  design-review skill. The first review ran before the procedure existed, and
  its findings have no card.
- The first weekly quality review of merged steps: read its record and put its
  two numbers into the report.
- On the first machine of the harness, the folder `.cache/harness` of the main
  checkout still holds the record of the time before this file. Move the
  record to the state folder, then remove that folder.

## What a new session must know

A decision is on the card that it changes. These hold for every card:

- Joel's decision principles: [docs/arch/principles.md](../../../../docs/arch/principles.md).
- Start the harness owner through `architecture/owner/session.sh`.
  In Claude Code, `claude --agent claude` gives an ordinary session.
- The lanes of a board run from one checkout on one machine. Do not run
  `bash architecture/harness.sh up` or `bash architecture/lane.sh start` in a
  second clone or on a second machine for a board whose lanes already run:
  nothing stops a second set of lanes yet. `doctor` and `status` only read.
- A lane that a person stopped stays stopped until a person starts it.
- The state folder of a machine holds only what can be made again. Nothing
  that a new session needs is only there, and nothing of the harness is in a
  folder that Git ignores inside the checkout.
