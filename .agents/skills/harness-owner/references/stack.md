# What the harness uses, and why

Joel builds a general harness from this one: one that develops any application,
later on several machines. Labcast is its test project (Joel, 2026-11-20). This
file names each tool and mechanism of the harness, why we use it, and what we
looked at and did not take. Change a row when we change a tool, and keep the
reason. A fact that a research agent read, and that the owner did not check,
has the mark (R). The [lessons](lessons.md) hold the requirements with their
incidents; this file holds the choices.

## In use

| Part | What we use | Why | Looked at and not taken |
| --- | --- | --- | --- |
| Supervisor of a lane | `architecture/run.sh`, one bash process for each lane, with a history file of one line for each result | It grew with the project and every act is a line that a person can read. It is the test bed; the Rust harness replaces it. | A durable workflow engine (see Watch). |
| Backlog and state | The GitHub project board and its issues; `architecture/board.sh` (one GraphQL query) and `architecture/backlog.jq` | It is the tracker that the owner of the project reads, and the state survives a machine. | A tracker of our own next to the board: a second source of truth. |
| Order and readiness | The Sequence field; a card is ready when each link in its "Dependencies" section is Merged | Simple, and a lead can change it in the card. | Typed dependencies (see Planned): links in free text made loops and silent cards. |
| Agents | Antigravity CLI (`agy`) runs the harness owner, Claude Opus design reviews and council halves, the Gemini weekly design-review judge, and read-only Gemini Hydrogen lookups and web searches (Claude Code backs up Claude reviews, and read-only Codex backs up Hydrogen lookups and web searches). Until 2026-12-05, Codex CLI writes code and tests, and fresh Codex sessions review `mechanical` and `logic` steps, run the read-only pass before Claude reviews `## Design` sections, and judge half of weekly design reviews. Claude does design and ADR work when Joel asks. See [Models](../../../project.md#models) for the model and command for each role. | The author of a change never reviews it. The cheapest model whose mistakes a gate catches does the work. | One vendor for both roles. |
| Gate | `nix flake check` through `architecture/gate.sh`, under one lock of the machine; the merge turn of `run.sh` | One definition of "passes" for every lane and for the nightly run. | Checks on each pull request in the cloud: one runner, and the gate is local. |
| Qualification | A nightly GitHub Actions run on one Linux GPU host | The checks that need the GPU and Linux. | A rented build machine (cost). |
| Measure | `architecture/measure.sh`, `architecture/host-series.sh`, the wrapper and the recorders of the owner | The owner decides from numbers of the lanes, not from the reports of the sessions. | A dashboard service. |
| Records of the owner | `STATE.md` (decisions with their numbers), the lessons file, this file | Every session can read what was decided and why. | Records in the conversation only. |
| Machine configuration | `~/nix-config` (nix-darwin and Home Manager), applied by Joel | Each tool has a pinned version and is the same on the Mac and on the Linux host. | Tools installed by hand. |

## In trial since 2026-11-20

| Part | What we use | Why | Looked at and not taken |
| --- | --- | --- | --- |
| Graph of the harness | Oxigraph 0.5.11 (RDF quads, SPARQL 1.1), from the pinned nixpkgs. Owner tools: `graph/build.sh` makes N-Quads from the board and the merged pull requests and loads a store; `graph/q.sh` has the named queries (loops, releases, blockers, ready, touched, overlap). | The questions of the owner are graph questions: what does an answer release, which cards wait for each other, what releases the most work, which changes touched this code since that run. Oxigraph is written in Rust and is also a library, so the same store can live inside the Rust harness. Its licence is Apache-2.0 or MIT. A graph of quads merges by set union and each named graph names its source, which fits a harness on several peers. The data is text (N-Quads) that git can hold. | Kuzu: archived on 2025-10-10 (R). Its fork LadybugDB and Grafeo: young, and not in nixpkgs. SurrealDB: licence BSL 1.1, not free. Neo4j: a server on the JVM, GPL. CozoDB: no release since 2023 (R). A graph database as the source of truth: see the rule below. |
| Measures of the value of the work | `architecture/quality/facts.sh`: the tests and the assertions of a scope, tests with no assertion, weak assertions, exemptions, removed and changed assertions for each commit, and the share of new lines that are still in the code later. The step gate asks for the trailer `Oracle: <reason>` when a branch removes or changes an assertion of an existing test; `architecture/quality/authorized.tsv` names the one change that a rule of AGENTS.md allows. The weekly review judges a fixed sample of the merged steps again. | The count of merged steps does not show that the work is good: a design can be weak and a test can prove nothing while every gate passes. These numbers are the same for the same commit, so a change in them is a fact. | A mutation score for each step (the time of a full run; a nightly job is planned, #774). Line coverage as the measure (a test with no assertion covers lines). |

The rule for the graph: the sources are the truth (the board, the pull requests,
later an append-only record of events), and the graph is derived. `build.sh`
removes the store and builds it again. Each source has its own named graph.

First result, 2026-11-20: the count of cards that wait for a card through other
cards. #331 has 99 and #54 has 84; the direct count of the measure said 50 and
did not name #54 at all.

Known limits: the store has no replication (the exchange between peers is our
layer); SPARQL is not a language for property graphs; a property on an edge
needs RDF 1.2 triple terms, which we have not tried.

## Planned

| Part | What we plan | Why | Source of the design |
| --- | --- | --- | --- |
| A trace for each step | The OpenTelemetry export of Claude Code and of Codex to a local collector, with each gate part as a span. The collector is not chosen: `opentelemetry-collector-contrib` 0.155.0 of the pinned nixpkgs does not build on the Mac (a Go compile error in a dependency, 2026-11-20), so it is not in `~/nix-config` | The session logs have no times, and the cost of a step in time and tokens is not visible across two vendors. | The documentation of both tools (R). Not verified: the coverage of `codex exec`. |
| One gate for several changes | A merge turn that builds one tree from the changes that wait, runs the gate one time and splits the batch only when it fails; a rerun separates a flaky test from a failure of the base | The gate lock is busy 75 to 93 of every 120 minutes. | Merge queues with batches (GitHub merge queue, Mergify) and the small tool fanout (R). It needs one coordinator, so it comes with the new supervisor. |
| Typed dependencies | A dependency is a typed edge; "what is ready" is one query; a worker claims an item in one atomic act; a loop is refused when the item is written | Links in free text made a loop of nine cards that was silent for seven days. | Beads and NEEDLE (R). `backlog.jq` shows loops since 2026-11-20. |
| The store of the Rust harness | An append-only record of events as the truth, and a derived graph: SQLite through `rusqlite` with recursive queries, `petgraph` for the analysis in the process, Oxigraph for the knowledge graph | No server, one file, the graph can be built again, and two peers merge by appending. | The research of 2026-11-20 (R). |

## Watch

- Temporal with its Rust SDK, and Restate: durable workflows give resume,
  timers and "wait for the person" as a state. They need a server and move the
  state out of git (R).
- LadybugDB and Grafeo: embedded property graphs with Cypher (R).
- A daemon on each machine that owns its own sessions, with a versioned JSON
  envelope (pohunek), for the harness on several machines (R).
- A push of a question to the phone of the owner (ntfy). It sends card text to
  an outside service, so it is Joel's choice.

## Not taken

- Spec kits, teams of persona agents and graphical control planes: none of
  them touches the gate or the dependencies (R).
- Vibe Kanban (its page says that it ends), bors-ng and homu (archived) (R).
