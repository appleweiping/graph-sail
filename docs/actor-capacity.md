# Local actor capacity protocol

`benchmarks/benchmark_actors.py` measures actual calls to persistent spawned
workers, separately from the planner's estimated schedules. It compares the
same round-robin keyed accumulation in direct Python and in local process
actors. It does not benchmark a distributed runtime or establish Ray parity.

```bash
uv run --frozen --extra dev python benchmarks/benchmark_actors.py \
  --requests 128 --rounds 2048 --actors 2 --window 16 --repeats 3 --warmups 1
```

Each request sums `(request_index + k) % 97` for the configured number of rounds
and adds it to that logical shard's retained total. The verifier independently
computes the arithmetic series in closed form, then hashes every ordered count
and cumulative result. Worker code does not call that oracle. Every returned
process identity must match the registered worker; all workers must exit and
be reaped before a successful trial is returned. Incorrect output or cleanup
raises instead of producing a successful timing record.

## What the timings include

- `startup_ns`: construction of every actor, including Python spawn, imports
  and the factory handshake. Direct mode constructs the same number of logical
  state holders in the parent process.
- `processing_ns`: submission, serialization/transport, actor computation,
  submission-order result waiting, ownership checks and ordered result hashing.
  Direct mode performs its computation and the same result checks/hashing.
- `shutdown_ns`: context exit, drain, exit and join. Timing ends after cleanup,
  before the final oracle digest comparison.
- `total_ns`: all three stages. Processing-only throughput must not be described
  as end-to-end throughput or used to hide process startup.

At most `window` result handles remain unconsumed by the driver. The actors have
bounded mailboxes, and the driver waits for the oldest submitted handle before
admitting another when the window is full. This intentionally measures ordered
consumption, not an optimal out-of-order scheduler. `max_pending_observed` is
the driver's outstanding-handle count, not a measurement of every OS buffer.
Each trial creates fresh actors; warmups are excluded from recorded trials.
Direct trials run before process trials. There is no affinity setting, randomized
mode ordering, host-idleness check or CPU/RSS profiling in this protocol.

The JSON retains every trial and host/Python metadata. The p95 is the empirical
nearest-rank value, so with three trials it is just their maximum, not a reliable
tail-latency estimate. Repeats, worker count, request count, arithmetic work and
pending handles are bounded. No hardware-specific timing threshold is in CI;
the test suite checks semantics, real worker ownership and lifecycle instead.

## Recorded local observations

[All measured stage timings](../benchmarks/results/actors-windows-python312.json)
come from Windows 11 / CPython 3.12.13 on a host reporting 16 logical CPUs.
Both workloads used two actors, a window of 16, one warmup and three measured
trials. Other development work was active; these are observations on that host,
not uncontended hardware capacity or a performance guarantee.

| Workload | Direct median processing | Process median processing | Process median end-to-end |
| --- | ---: | ---: | ---: |
| 128 requests × 2,048 rounds | 15.00 ms | 36.47 ms | 1,302.37 ms |
| 32 requests × 100,000 rounds | 187.36 ms | 96.00 ms | 1,480.32 ms |

The small requests did not amortize transport overhead. The heavier requests
showed lower processing time across two workers, but starting fresh workers
still made this short run slower end-to-end than direct execution. These results
motivate measuring the intended workload and actor reuse lifetime, not assuming
that more processes make every workload faster. Both modes matched the independent
output digest in every recorded trial. No reference-runtime comparison, network
transport, GPU workload, fault-rate benchmark or memory-capacity claim is made.

Verification for this increment: 11 benchmark-specific tests passed, including
real spawned workers and a fresh-process script invocation. The complete local
suite passed 488 tests with 96.73% combined branch-aware package coverage.
Ruff lint/format, strict package Mypy, Bandit, wheel/sdist build, Twine and
wheel-content checks passed. Benchmarks are tooling, not production source LOC.
