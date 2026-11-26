# Bombay project bindings

Root [AGENTS.md](../AGENTS.md) holds every Bombay rule. This file binds [Patstack](skills/patstack/SKILL.md) to Bombay's tools, lanes, models, and gates.

## Steps

| Patstack step | In Bombay |
| --- | --- |
| Task | A GitHub card selected in [Bombay Project #6](https://github.com/orgs/devrandom-labs/projects/6). Link branch, PR, and checks to `devrandom-labs/bombay#<number>`. |
| Spec ([to-spec](skills/to-spec/SKILL.md)) | The card body: goal, cold-start `file:line` context, a holistic `## Design` section (when adding a domain concept, extracting or restructuring a crate or module, or running more than one `logic` step), and numbered steps marked `mechanical` or `logic` with one acceptance check per step. |
| Mode | Full Send ends at the merged PR on `main`. Crate publication and downstream `mnesis-bombay` / `selo` stages require explicit authority. |
| Implement | In the lane's worktree (`bombay-worktrees/<lane>`), inside `nix develop` using `just` recipes. |
| Lead | Started by `architecture/run.sh <lane>` (or `just step <lane>`), delegating to the [Models](#models) table below. |
| Review | Step review (R2) runs in a fresh read-only session that wrote none of the change. Security, wire/auth, placement fencing, durable commit, or public API changes require a [Council (R3)](skills/council/SKILL.md). Every R2 and R3 reviewer prompt names `AGENTS.md`, the review skill, the matched skill and owner paths in [Route by the work](#route-by-the-work), the card, Joel's decisions, and `<base>..<head>`. Each reviewer reads those paths before reviewing, and the lead records the exact paths read by author and reviewers under `Skills:` in `.cache/pr.md`. For `agy` or `claude -p` read-only reviews, write `git diff <base>..<head>` under `.cache/` first and verify `"permission_mode":"request-review"` plus at least one completed read of the diff/scope file. |
| Verify | `just architecture` (plus `just architecture-tests` when `architecture/` changed), `bash architecture/step.sh`, then `just gate` (`architecture/gate.sh`, which caches the last 50 passing tree hashes under `~/.cache/bombay/nix/gate/<tree>/`). |
| PR watch | None on pull requests: `checks.yml` qualifies `main` on push, nightly schedule, and manual `workflow_dispatch`. |
| Retro | Proportionate comment on the card when closing or splitting. |

## Models

Set read-only Antigravity permissions for all non-mutating `agy` calls:

```sh
AGY_RO='{"agentMode":"","toolPermission":"request-review","permissions":{"allow":["read_file(*)","search_web(*)","read_url(*)"],"deny":["write_file(*)","edit_file(*)","notebook_edit(*)","command(*)","unsandboxed(*)","mcp(*)","execute_url(*)"]}}'
```

| Work | Model (Until 2026-12-05) | Command | Post-2026-12-05 Ultra Switch |
| --- | --- | --- | --- |
| `logic` step implementation, tests, audits | `gpt-6.2-sol` | Codex lead directly, or `codex exec -m gpt-6.2-sol -C W "<prompt>" < /dev/null` | `agy --model gemini-pro-orchestrator-neon` |
| Fresh diagnosis session | `gpt-6.2-sol` (`max`) | `codex exec -m gpt-6.2-sol -c model_reasoning_effort=max -C W "<prompt>" < /dev/null` | `agy --model gemini-4-neon-xhigh` |
| `mechanical` step | `gpt-6-luna` | `codex exec -m gpt-6-luna -C W "<prompt>" < /dev/null` | `agy --model gemini-4-hydrogen-low` |
| Reuse search, design signals, nightly log triage, card/PR prose | `gemini-4-hydrogen-low` in `agy` | `(cd W && agy --disable-slash-commands --output-format stream-json --model gemini-4-hydrogen-low --settings "$AGY_RO" -p "<prompt>" < /dev/null)` | Same (`gemini-4-hydrogen-low`) |
| R2 review of `mechanical` step | `gpt-6.2-sol` (fresh) | `codex exec -m gpt-6.2-sol -s read-only -C W "<prompt>" < /dev/null` | `agy --model claude-opus-5-5-medium` |
| R2 review of `logic` step | `gpt-6.2-astra` (fresh) | `codex exec -m gpt-6.2-astra -s read-only -C W "<prompt>" < /dev/null` | `agy --model claude-opus-5-5-high` |
| R3 Council (auth, wire, fencing, durability, public API) | `gpt-6.2-sol` + `claude-opus-5-5-medium` in `agy` | Fresh read-only `codex exec` + `(cd W && agy --disable-slash-commands --output-format stream-json --model claude-opus-5-5-medium --settings "$AGY_RO" -p "<prompt>" < /dev/null)` | `gemini-4-carbon-high` + `claude-opus-5-5-high` in `agy` |
| `## Design`, crate, or layer-map review | `gpt-6.2-astra` pre-pass + `claude-opus-5-5-high` in `agy` | Read-only `codex exec` pre-pass, then `(cd W && agy --disable-slash-commands --output-format stream-json --model claude-opus-5-5-high --settings "$AGY_RO" -p "<prompt>" < /dev/null)` | `gemini-4-neon-high` + `claude-opus-5-5-high` in `agy` |
| Weekly layer design review judges | `gemini-4-carbon-high` in `agy` + `gpt-6.2-astra` | Read-only `agy` Carbon High + read-only `codex exec` | `gemini-4-carbon-high` + `claude-opus-5-5-high` |
| Harness owner (`session.sh` & loop) | `gemini-4-carbon-high` | `bash architecture/owner/session.sh talk` / `check` / `loop start` | Same (`gemini-4-carbon-high`) |

## Automatic run

- Lanes: `runtime` (`Runtime`, `Shared`), `domain` (`Domain`), `adapters` (`Adapters`), `nix` (`DevOps`).
- **Holistic upfront blueprint (before any code step):**
  1. Read the card and all matched paths in [Route by the work](#route-by-the-work) before designing or editing.
  2. Write a complete `## Design` section above `## Steps` whenever the card adds a domain concept, extracts or restructures a crate or module, or has more than one `logic` step. Show the full target for the whole card in concrete Rust signatures: domain structs, closed state/policy enums, narrow Port traits, error sums, and required `Cargo.toml` dependencies.
  3. Never invent per-function micro-designs or interim `FnOnce` callback / `<H, T, S, R, E>` generic-erasure wrappers to work around an unregistered workspace dependency: file a `Shared` dependency card first and block on it (`STEP blocked #<card> <reason>`).
  4. Review the complete `## Design` section once with read-only Codex (`gpt-6.2-astra`) and then with `agy` Claude Opus (`claude-opus-5-5-high`), repairing High/Medium findings on the card before any code step begins (`STEP planned`).
- Every lead session ends its final output line with one of:
  - `STEP planned` — holistic `## Design` and `## Steps` written and reviewed; ready for Step 1 in the next session.
  - `STEP gate <summary>` — requests merge-turn `just gate` and automatic squash landing using `.cache/pr.md` (`Reviewed-head: <full sha>` on line 1, PR title on line 2, blank line 3, PR body including `Skills:` below).
  - `STEP later <minutes> <reason>` — sets card aside when worktree holds no open change and takes the next ready card.
  - `STEP blocked #<card> <reason>` — parks until blocker `#<card>` reaches `Merged` or `linux-owed`.
  - `STEP waiting <minutes> <reason>` — waits on external/Joel input (`needs-joel` label).
  - `STEP failed <reason>` — retries once in a fresh session, then parks.

## Route by the work

Read every row that matches the changed paths before designing, editing, or reviewing, and record the exact paths under `Skills:` in `.cache/pr.md`:

| Work | Read before working on that boundary |
| --- | --- |
| Any Rust source (`*.rs`) | [rust-best-practices](skills/rust-best-practices/SKILL.md), [bombay.md](skills/rust-best-practices/references/bombay.md), and the resolved `bombay-behavior` `AGENTS.md` |
| Any `logic` Rust step | [modular-design-principles](skills/modular-design-principles/SKILL.md) alongside `bombay.md`; for state machines, policies, or domain models also read [domain-driven-design](skills/domain-driven-design/SKILL.md) |
| Rust tests, SIM harness, or mutation checks | [rust-tests](skills/rust-tests/SKILL.md); [property-based-testing](skills/property-based-testing/SKILL.md) for FSM/input-domain invariants; [cargo-fuzz](skills/cargo-fuzz/SKILL.md) for fuzz targets |
| Crate boundaries, ports, or Wave 1 extraction | [ADR 0001](../docs/adr/0001-layer-bombay-crates.md), [architecture.md](../docs/arch/architecture.md), and [design-review](skills/design-review/SKILL.md) |
| `crates/bombay-axum` or HTTP readiness/metrics | [axum](skills/axum/SKILL.md) and [rust-telemetry](skills/rust-telemetry/SKILL.md) |
| `crates/bombay-zenoh`, auth, wire envelopes, or AEAD | [security-threat-model](skills/security-threat-model/SKILL.md) |
| `flake.nix`, `nix/`, `justfile`, or `.github/` | [nix-builds](skills/nix-builds/SKILL.md) |
| Harness scripts, gates, or lane supervision | [harness-owner](skills/harness-owner/SKILL.md) |
