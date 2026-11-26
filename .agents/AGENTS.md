# Project skills

Use these skills for their stated tasks. Root AGENTS.md and user instructions
take precedence over upstream examples and defaults.

Run tools through this repository's Nix environment. Do not follow host install
commands such as `rustup install`, `cargo install`, or package-manager installs
from skill examples. Nix owns toolchains, packages and disposable services.

Use Rust examples and libraries for application code and application tests.
Database-native SQL tests follow the
[test boundaries](skills/rust-tests/SKILL.md#database-and-application-boundaries).
Other-language references do not authorize adding Python or changing the project's stack.
Follow root AGENTS.md and [Patstack](skills/patstack/SKILL.md).
Specialist skills supply domain guidance; they do not start another workflow,
grant deployment permission, or reopen accepted product decisions.

Sources, revisions, licenses and local changes are recorded in
[skills/sources.json](skills/sources.json).
