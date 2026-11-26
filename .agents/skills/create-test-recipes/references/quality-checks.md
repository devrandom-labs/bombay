# Quality checks for generated recipes

Derive targets before implementation/acceptance measurement from explicit project budgets and contracts, healthy comparable flows, then relevant primary guidance. If no adequate reference exists, set a justified provisional engineering target and expose assumptions. Investigate technical facts autonomously; product preferences and significant new visual directions remain human decisions. A poor baseline is not an acceptable quality floor.

Select only dimensions affected by the task. Include functional assertions and the applicable quality checks; missing required evidence blocks acceptance. Mark genuinely inapplicable dimensions N/A with a reason. Present agent UI/UX ratings as judgments, separately from measurements and real human-user evidence.

## UI and journeys

Use project components/design references, supported viewports and real entry points. Cover relevant keyboard/focus, responsive, validation, loading/empty/error and persistence behavior. Inspect visible results and relevant network/data side effects. A final screenshot alone does not prove the journey or correct data.

## API and performance

Record the request path, dataset, synthetic persona/permissions, concurrency, cache/warmup state, build and protocol. Compare equivalent BEFORE/AFTER conditions and count actual successful work, errors, timeouts and retries. Keep enough samples and repeat/alternate runs to distinguish noise from the claimed gain; a one-run estimate stays labeled an estimate. Profile separately if instrumentation distorts timings. Identify the bottleneck and relate any microbenchmark to the user-visible path.

Choose applicable measurements:

- End-to-end latency, median/p95 and p99 when the sample size supports it; relevant server/database breakdown.
- Queries per request/journey, DB time, N+1 growth, downstream requests and payload bytes. Fewer queries alone is not a performance win.
- Errors, throughput and relevant CPU, retained memory or connection-pool behavior under the stated workload. A smoke test is not a soak/load result.
- Exact records, fields, ordering, pagination and required side effects; permission-denied and wrong-tenant/role/object/field counterexamples. Exercise cache isolation when relevant.

Use synthetic leakage markers and safe assertions. Avoid persisting secrets, raw sensitive queries/responses or authenticated browser state. A finite negative-test matrix supports only its tested cases; never claim universal absence of leakage. Performance gains cannot replace required correctness or security.

## Evidence and delivery layers

Reuse existing test reporters and benchmark artifacts. Retain safe raw measurements with criterion, actual command/steps, revision/build/environment, units, sample count, expected/actual and limitations. Show concise before/after comparisons in chat when relevant. If reconstructing BEFORE is unsafe or unavailable, report that limitation rather than inventing an improvement.

PR runs cover the reviewed candidate and required merge/base context; live runs cover the actually deployed release. Verify identity before/after shared-target tests. Coordinate existing target/fixture ownership; absent isolation makes proof inconclusive, not automatically an application failure. Release owned shared resources before returning for code repair.

Keep feature outcome and quality verdicts separate. Missing required measurement or a failed guard stays visible in the parent repair/handoff flow, with cumulative time and attempts retained. This reference defines generated test content, not new review, ticket, deployment or scheduling permissions.
