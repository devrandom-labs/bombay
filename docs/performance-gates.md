# Performance gates

Q5 measures bombay runtime composition without taking ownership of Bombay
primitive performance. Run the complete reproducible lane with:

```console
nix build path:.#performance -L
```

The artifact contains Criterion's machine-readable sample and estimate files
under `result/criterion/` plus `result/environment.txt`. The lane first runs the
process-isolated allocation-retention oracle. For a fast structural check that
does not collect timing samples, run:

```console
cargo bench -p bombay-rs --bench runtime_composition -- --test
```

## Frozen bombay workloads

The benchmark names and following outcomes are the comparison contract. A peer
result is comparable only when it performs the same observable work and waits
for the same retirement boundary.

| Workload | Frozen observable outcome | Excluded setup |
|---|---|---|
| `spawn_abort_retire` | Register one actor, start its task, abort it, classify completion, and retire the exact incarnation | Tokio runtime construction |
| `send_1024_then_stop` | Deliver 1,024 typed messages through a bounded actor mailbox; the last fold stops the actor; await complete retirement | System and actor construction |
| `stop_and_retire` | Deliver one typed stop-producing message and await complete retirement | System and actor construction |
| `arm_due_timer_and_retire` | Deliver one command that arms a zero-delay relative timer, inject its typed elapsed event, stop, and await complete retirement | System and actor construction |
| `watch_peer_and_retire` | Install one exact-incarnation peer observation, stop the peer, deliver its normalized outcome, stop the watcher, and await both retirements | System and both actor constructions |
| `restart_once_and_retire_tree` | Start one supervised worker, observe its failure, install one fresh replacement, reject the next replacement by the frozen budget, and await the full proxy/worker tree retirement | System and supervisor construction |
| `coordinated_shutdown` | Publish one priority shutdown request, perform the final fold, and await complete retirement | System and actor construction |

Setup is excluded only from Criterion timing. Each workload still constructs
and destroys its complete ownership graph once per sample; state cannot leak
between samples. The 1,024-message send case reports one batch latency, so
comparisons must divide by 1,024 before claiming per-message cost.

`boundary/tokio_sleep_until_now` is a diagnostic control, not an eighth
bombay workload. It isolates Tokio's live timer-driver registration and
wakeup floor. On the correction run, that control measured 1.2624–1.2646 ms;
the actor workload measured 1.2658–1.2688 ms before bombay consumed an
already-due queue entry directly and 1.2670–1.2775 µs afterward. The red proof
removes that due-entry check and requires the zero-relative timer's first poll
to become pending. Bombay Timers remains the owner of queue operations;
bombay owns only deciding whether a queued deadline needs Tokio to sleep.
The pinned Nix run measured the control at 4.1866–4.3452 ms and the corrected
actor path at 1.2782–1.2918 µs, confirming both the environment sensitivity of
the Tokio floor and its absence from the due-timer path.

## Allocation boundary

`allocation_oracle` is its own test binary because the counting allocator is
process-wide. It runs 64 complete public-API spawn/send/stop/retirement cycles
to establish an identically shaped warm baseline, then another 64 cycles and
requires zero positive live-allocation growth. Deliberately changing the
assertion to accept positive growth makes the inversion oracle vacuous; changing
the second phase to retain any handle makes it fail.

This is a retention gate, not a claim that actor creation allocates nothing.
Communication owns its zero-allocation steady-state send proof; Address owns
explicit table reclamation; Observe owns observation pooling; Timers owns queue
allocation; Behavior owns pure-fold cost. Their isolated benchmarks and
allocation tests remain authoritative.

## Competitive-use rules

OTP, Akka Typed, Kameo, Tokio, and historical Bombay results are admissible
only with source revision, toolchain/runtime version, machine metadata, raw
samples, and a mapping to every observable outcome above. Comparisons must
state differences in mailbox bounds, scheduling, timer semantics, observation
generation, restart policy, and retirement waiting. A partial mapping is
reported as a partial comparison and cannot become a headline ratio.

No historical number currently in the repository meets that complete mapping,
so this gate intentionally publishes bombay measurements without inventing
a competitive ratio. Adding a peer adapter is benchmark tooling, not permission
to add its API shape or policy to the bombay runtime.
