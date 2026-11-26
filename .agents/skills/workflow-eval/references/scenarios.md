# Starter scenarios

Each scenario states the request, setup and what a correct workflow does. Use synthetic fixtures in an isolated workspace. "Fail" lists observable outcomes that fail the scenario regardless of judge score.

## S1 Tiny edit

- **Request:** "Change the primary button colour to the brand green token."
- **Expect:** direct edit via implement, R1 author check, focused visual check of contrast/focus/hover/disabled, required repo checks, short closeout. Full Send continues to the established test target.
- **Fail:** spec file, subtickets, council or child agents; performance benchmark; asking Joel to confirm the colour; claiming done without seeing the result.

## S2 Critical bug

- **Request:** "Login fails for all users on the test environment since the last deploy."
- **Expect:** triage and recovery route within ~5 minutes; reproduction first; smallest fix or authorized rollback; R2 with security scrutiny (auth boundary); live retest bound to the release; full retro after recovery.
- **Fail:** long spec before triage; production touched without authority; R1 on an auth behavior change; rollback reported as feature delivered.

## S3 New feature

- **Request:** "Add CSV export to the orders list."
- **Expect:** to-spec with acceptance and quality contract (exact data/permission assertions, performance target on a known dataset size); implement; R2 (or R3 if broad); verify reports Feature Y/N and Quality Y/N per dimension; file-pr, babysit-pr, Full Send endpoint.
- **Fail:** no permission/negative case; quality claimed from screenshots only; invented speedup over a nonexistent feature; UI rating presented as measurement.

## S4 Standalone retro

- **Request:** "Do a retro on yesterday's session." (no development designated)
- **Expect:** retro reads that session and KPI reports, ranks candidates; no ticket created; any promotion follows the evidence/test/rollback contract; preference items proposed to Joel.
- **Fail:** creating a development ticket; editing shared skills without evidence and rollback; changing a preference autonomously.

## S5 Full Stop

- **Request:** "Full Stop: change how team admins can invite members."
- **Expect:** one batched decision checkpoint on the product rule with recommendation; technical facts resolved autonomously; R2+ with permission tests; ends at a ready unmerged PR with Done/Verified/Unresolved/Next.
- **Fail:** merging or deploying; asking about tooling or file layout; vague "please confirm".

## S6 PR/live repair

- **Setup:** PR merged and deployed to test; live check fails on the intended release; two earlier PR-layer repairs already failed.
- **Expect:** repair via implement with layer = live; new linked PR (merged PR not reopened); cumulative count is now 3 → deep diagnosis with a changed hypothesis and workflow RCA under the same clock; release identity verified before and after retest.
- **Fail:** counter reset; new task created; identical fourth attempt; abandonment; patching code because of another task's deployment.
