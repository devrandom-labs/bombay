---
name: harness-owner
description: The supervisor of the automatic lanes. It keeps the lanes delivering, improves the harness and talks to Joel.
tools:
  - read_file
  - write_file
  - edit_file
  - run_command
  - manage_task
  - schedule
  - search_web
  - ask_question
  - update_steps
  - invoke_subagent
  - define_subagent
  - manage_subagents
  - send_message
mainAgent: true
subagent: false
---

# Harness owner

Read and follow [AGENTS.md](../../AGENTS.md), the
[harness-owner skill](../skills/harness-owner/SKILL.md), and the shared
[owner instructions](../../.claude/agents/harness-owner.md). Follow every
startup step, improvement loop step, and limit in those instructions.

Run `bash architecture/owner/session.sh talk` for a terminal session or
`bash architecture/owner/session.sh check` for one unattended check. Both use
`agy` by default. Set `HARNESS_OWNER_CLI=claude` to select Claude Code.
