---
name: grilling
description: Sharpen a plan, design or decision by interviewing Joel - one batched, numbered list of questions, each with a recommended answer, asked only about choices that are genuinely his. Use for grill me, poke holes in my plan, have I thought this through, a Full Stop decision checkpoint, or before an approach that is costly to reverse. For model critique of finished work use review or council.
---

# Grilling

The goal is shared understanding in as few round trips as possible. Asking the wrong kind of question costs more than asking many.

**Context.** Under a Patstack task, this is how Full Stop checkpoints and genuine product decisions reach Joel. Invoked directly, it is unticketed discussion.

## Filter every draft question

1. **Has he already decided it?** Re-read his messages. Preserve his framing and vocabulary verbatim; a stated definition is the definition, not a starting position.
2. **Can a tool answer it?** Facts are your job: code, history, configs, running apps, prior tasks, a subagent. A pending lookup blocks only the questions downstream of it.
3. **Does the answer change what gets built?** If both branches lead to the same work, pick one, say which, move on.

A structured bug report (environment, area, severity, steps) is a complete spec; act on it.

## What is his call

Ask about: target environment when not established, anything user-visible or deployed (copy, behavior), money (paid tiers, new services), creating repos or infrastructure, launch timing, product rules (who may do what, what counts as correct), significant new visual direction.

Own without asking: implementation approach, an established test persona, tooling, parallelism, file layout, number of agents.

## Ask once, batched

- Directional or product questions: prose, with your recommendation stated.
- Bounded operational picks between known alternatives: two to four concrete options, recommended one first. The recommendation must be a real call, not a hedge.
- Batch the whole current frontier into one numbered list; split into a later round only when a question depends on another's answer.

```text
1. <question> - recommend: <answer, and why in one clause>
2. <question> - recommend: <...>
```

State the approach in a short paragraph, the questions that matter, and what you will do next, in one message.

## Read the answer

- Interpret an explicit reply within its clear scope. A numbered answer settles that item; clear approval of the entire described batch settles that batch. Do not re-confirm a decision already made.
- Unanswered human-gated product, permission or visual choices remain pending. Silence creates no approval. Ordinary implementation defaults may proceed under existing authority; state the assumption and keep independent work moving.
- A scope redirect is a decision, not an objection to argue with.
- If he contradicts what you believe, his environment statement outranks your inference. Verify against the live system; if you were right, show the evidence.

## When he is unavailable

Record the open question, recommendation and next action on the task as a block, and continue independent work within scope. A missing answer grants no product authority.

## Stop

The interview ends when nothing is both unsettled and his to settle. Say so and start work.

When changing approval interpretation, replay the [approval scenarios](references/approval-scenarios.md).
