# Telemetry of the harness: the design

This is the design of an open piece of work of the harness owner ([open work](open.md)). It leaves this folder when its last step is on `main`; the stack record then holds what was built and why.

## Goal

The harness records what each agent session and each gate did, in one standard form, so that the harness owner sees where a run goes wrong and repairs the cause: time, tokens and cost for each session, tool calls and their failures, the gate and its parts, the waits, by lane, card and step.

Today the harness measures itself from text that it writes (`run.history`, a follower of the gate log, a host recorder). It has no number for the tokens or the cost of a step, for a tool call that failed, or for the time that a session spent in each tool.

## Measured (2026-11-20)

- The pinned nixpkgs of this repository (`flake.lock`) has these packages in the binary cache for `aarch64-darwin` and for `x86_64-linux`: `opentelemetry-collector-contrib` 0.155.0, `opentelemetry-collector` 0.155.0, `otel-cli` 0.4.5, `duckdb` 1.5.5, `victoriametrics` 1.154.0, `victorialogs` 1.53.0, `victoriatraces` 0.11.1, `vector` 0.59.0, `grafana-alloy` 1.17.1, `openobserve` 1.0.4, `otel-desktop-viewer` 0.5.0, `otel-tui` 0.7.5, `tempo` 3.1.0, `clickhouse`. `quickwit` and `greptimedb` are not in the cache for the Mac. (An earlier build failure of the collector came from the package pin of one machine configuration, not from this repository.)
- One probe: the contrib collector with an OTLP HTTP receiver on `127.0.0.1:4318` and three `file` exporters, and one session `claude -p` (Claude Code 2.1.327) with `CLAUDE_CODE_ENABLE_TELEMETRY=1`, `CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1`, `OTEL_METRICS_EXPORTER=otlp`, `OTEL_LOGS_EXPORTER=otlp`, `OTEL_TRACES_EXPORTER=otlp`, `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`, `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318` and `OTEL_RESOURCE_ATTRIBUTES=harness.lane=probe,harness.card=0`. The files held:
  - metrics `claude_code.token.usage` (tokens), `claude_code.cost.usage` (USD), `claude_code.active_time.total` (s), `claude_code.session.count`, each point with `harness.lane`, `harness.card` and `session.id`;
  - events `api_request`, `tool_decision`, `tool_result`, `user_prompt`, `assistant_response`, `hook_execution_start`, `hook_execution_complete`, and others;
  - spans `claude_code.interaction`, `claude_code.llm_request`, `claude_code.tool`, `claude_code.tool.execution`, `claude_code.tool.blocked_on_user`.
- Each metric point also carried `user.email`, `user.id`, `user.account_uuid`, `user.account_id` and `organization.id`. These are personal data and must not be kept.
- From the documentation, not yet proved here: a session `claude -p` and a Codex session join a parent trace through the environment variable `TRACEPARENT`; Codex exports through the table `[otel]` of its configuration (`exporter`, `trace_exporter`, `metrics_exporter`, `span_attributes`), with metrics such as `codex.turn.token_usage` and events such as `codex.tool_result`. Sources: the page "monitoring usage" of the Claude Code documentation; `codex-rs/otel` in the Codex repository.

## Design

Concepts, one name each:

- **Session**: one run of an agent program (Codex or Claude Code) that the harness started. It has a role: `lead`, `review`, `owner`.
- **Step trace**: one trace for one step of a card. The span of the supervisor is the parent; the session, the wait for the merge turn, the gate and the parts of the gate are its children.
- **Harness attributes**: `harness.lane`, `harness.card`, `harness.step`, `harness.role`, `harness.machine`. The supervisor sets them, because it knows them; a session never sets them. Every other name comes from the OpenTelemetry semantic conventions for generative AI (`gen_ai.*`) and for CI/CD (`cicd.pipeline.*`) where one exists. The harness invents no second name for a thing that has one.
- **Telemetry record**: the files `metrics.jsonl`, `logs.jsonl` and `traces.jsonl` in `<state folder>/telemetry/`, in the OTLP JSON form, one line for each batch.

The path:

```
agent programs, otel-cli (shell spans)
  -> OTLP HTTP, 127.0.0.1:4318
  -> the collector (opentelemetry-collector-contrib of the pinned nixpkgs)
       processor: delete user.email, user.id, user.account_uuid, user.account_id, organization.id
       exporter: file, with rotation by size and age
  -> <state folder>/telemetry/*.jsonl          the truth
  -> architecture/telemetry/q.sh <name>        DuckDB reads the files; nothing else is stored
  -> architecture/graph/build.sh               the facts by card join the graph
```

Rules and their owners:

- The record is the truth. A database that a query builds is derived and can be removed at any time. (The same rule as for the graph.)
- Telemetry never stops work. When the collector does not answer, a session and a gate run as before. A test proves it: a session with an endpoint that nobody listens on ends with its normal result.
- No personal data in the record: the collector removes the attributes above, and a test with a fixture proves that none is in the output.
- The collector listens on the loopback address only. Nothing leaves the machine.
- The harness owner reads numbers through `q.sh` and the registry (`architecture/owner/tools.sh`), never from the raw files.

What it reuses: the collector, `otel-cli` and DuckDB from nixpkgs; the export that both agent programs already have; the semantic conventions. What it removes later: the follower of the gate log (`architecture/gate-times.sh`) and the parts of `architecture/measure.sh` that read times from `run.history`, after the two give the same numbers for a week.

Looked at and not taken: a metrics, a log and a trace server (three services to run and to keep for one machine; DuckDB reads the files with no service); a viewer with a store in memory (`otel-desktop-viewer`, `otel-tui`: good to look at one trace, they keep nothing); a cloud service (the data stays on the machine).

## Steps

- 0. Review the section above under the [design review rule](../../../../AGENTS.md#design-review) (one read-only Codex pass, then Claude). An open High or Medium finding holds step 2.
- 1. The packages `opentelemetry-collector-contrib`, `otel-cli` and `duckdb` are in the development shell and in the inputs of the architecture tests ([#779](https://github.com/labcast/labcast-server/issues/779), a card of the Nix lane).
- 2. The collector and the queries. `architecture/telemetry/collector.yaml`; `architecture/telemetry.sh start|status|stop <state folder>` (one process, the form of `architecture/host-series.sh`); `architecture/telemetry/q.sh <state folder> sessions | cost-by-card | tool-failures | slow-tools`; a test with fixture files; lines in the registry; `architecture/harness.sh up` starts it. Acceptance: one session `claude -p` with the harness attributes is a row of `q.sh sessions` with its tokens and its cost, and no personal attribute is in the record.
- 3. Codex sessions export. The exact `[otel]` settings come from the Codex repository of the installed version; they are given on the command line of the lead, not in a file of one machine. Acceptance: one session `codex exec` is a row of `q.sh sessions`.
- 4. The supervisor gives each session the harness attributes and the export settings when the collector answers. One behaviour of `architecture/run.sh`, its own step and review. Acceptance: the sessions of one real step of a lane carry the lane, the card and the step.
- 5. Spans of the supervisor through `otel-cli`: the step, the wait for the merge turn, the gate and its parts; `TRACEPARENT` goes to the session. Acceptance: one trace holds one real step with its session and its gate.
- 6. The measure reads the record: tokens and cost for each merged step, sessions with failed tool calls, the slowest parts of the gates. The facts by card load into the graph. The report of the harness owner gets the cost of a merged step beside the quality numbers.
- 7. After one week in which both give the same times: remove the text recorders that the record replaces.

