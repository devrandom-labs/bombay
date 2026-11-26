# Enter the pinned environment with `nix develop` (or `direnv allow`).
default:
    @just --list

changes:
    git diff --stat
    git ls-files --others --exclude-standard

# Run Bombay AST architecture rules and cognitive complexity gate.
architecture:
    bash architecture/check.sh rules

# Run self-tests of architecture rules and harness scripts.
architecture-tests:
    bash architecture/check.sh tests

# Run step non-regression checks against STEP_BASE (default origin/main).
step-check:
    bash architecture/step.sh

# Full local gate behind the machine lock and merge turn.
gate:
    bash architecture/gate.sh

# Automatic run board helpers (.agents/project.md).
items := "GH_TOKEN=$(gh auth token --user joeldsouzax) bash architecture/board.sh"

# Print the next ready card of a lane: runtime, domain, adapters, or nix.
next lane skip="":
    @{{items}} | jq -r --arg lane {{lane}} --arg mode next --arg skip "{{skip}}" -f architecture/backlog.jq

# Show the ready cards of a lane and external blockers.
backlog lane:
    @{{items}} | jq -r --arg lane {{lane}} --arg mode status --arg skip "" -f architecture/backlog.jq

# Show how a lane flowed over the last N days.
report lane days="7":
    @bash architecture/report.sh {{lane}} {{days}}

# Run one session for the lane's next ready card.
step lane:
    bash architecture/run.sh {{lane}} --once

# Work continuously through the lane's ready cards.
run lane:
    bash architecture/run.sh {{lane}}
