<!-- Thanks for contributing to bombay! Please fill out the sections below. -->

## Summary

<!-- What does this PR change, and why? Reference the card (#NNN). -->

## Type of change

- [ ] `feat` — new functionality (new public item, actor, feature flag, example)
- [ ] `fix` — bug fix
- [ ] `docs` — documentation only
- [ ] `test` — tests / coverage only
- [ ] `chore` / `ci` / `refactor` — no public behavior change

## API impact (REQUIRED)

bombay is `0.x` and under active development. Breaking changes are allowed, but
never accidental — declare them:

- [ ] **No public API change** — purely additive or internal, or
- [ ] **Breaking change** — a signature/type/behavior/error-variant/feature-flag
      changed. It is intentional, scoped, and described below (with the migration
      for downstream consumers).

## Verification

- [ ] `nix flake check` passes locally (the single gate: clippy, fmt, taplo,
      typos, audit, deny, nextest, doctest, actionlint, nixfmt, deadnix,
      shellcheck).
- [ ] New behavior is covered test-first (the failing test came first).
- [ ] `README.md` updated if the public API changed (it is a per-*card*
      public-API document — see `CLAUDE.md`).

## Notes

<!-- Anything reviewers should know: trade-offs, follow-ups, security impact. -->
