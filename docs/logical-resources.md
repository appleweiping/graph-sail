# Fractional and custom logical task resources

`ExecutionConfig(resources=LogicalResources(...))` adds one local, named resource
pool to real thread or spawned-process DAG execution. The same policy works with
`execute_graph`, `execute_process_graph`, `start_graph` and `start_process_graph`.
It is an additional admission condition, not a replacement for global workers,
per-device slots, placement compatibility or persistent-memory admission.

```python
from graph_sail import ExecutionConfig, LogicalResources

config = ExecutionConfig(
    max_workers=4,
    device_workers={"cpu": 4},
    resources=LogicalResources(
        capacities={"CPU": 1, "decoder-license": 1},
        requests={
            "decode": {"CPU": 0.5, "decoder-license": 1},
            "embed": {"CPU": 0.5},
            "aggregate": {"CPU": 1},
        },
    ),
)
```

Run `python examples/logical_resources.py` for a complete offline graph whose
fractional tasks actually overlap. Its dependent task then acquires the whole
pool and consumes their results. No simulated latency is used to manufacture
concurrency or output values.

## Exact amounts and preflight

Amounts must be built-in `int` or finite `float` values in `[0, 1_000_000]`.
One unit contains exactly 10,000 integer quanta. Float amounts are interpreted
through their shortest decimal representation, without arithmetic in the
caller's Decimal context. `0.1`, `0.0001` and `1.25` are valid; `0.1 + 0.2` is
rejected because its actual shortest representation is not quantized. There is
no silent rounding. Negative zero normalizes to zero; booleans, Decimal objects,
NaN, infinity and nonquantized nonzero quantities are rejected.

The pool may be empty. Missing node requests mean zero demand, not an inferred
CPU claim. Zero capacity and explicit zero requests are allowed. Requesting an
undeclared resource is an error even with amount zero. Every request must fit
the resource's entire configured capacity, so an inherently impossible task
fails synchronously rather than waiting forever. Request node IDs must belong
to the admitted graph; checks complete before a controller or worker starts.

Limits are 64 resource names, 10,000 request-node entries and 100,000 total
node/resource bindings. Names are exact, nonempty canonical Unicode strings of
at most 1,024 characters, without leading/trailing whitespace, control characters
or surrogate code points. Length is checked before whitespace normalization.
Policy mappings are snapshotted, including nested requests; subsequent changes
to the original dictionaries do not alter the execution. Custom Mapping methods
remain trusted caller code, not a sandbox for arbitrary Python protocols.

`capacity_units` and `request_units` are read-only integer mappings. `to_dict()`
returns a fresh integer-only configuration document, identified by
`kind="graph-sail-logical-resources"`, `schema_version=1` and
`units_per_resource=10000`. Its sorted UTF-8 JSON SHA256 is `digest`. This is
configuration identity, not a public document-import API or proof of execution.
Equivalent integer/float amounts and mapping order yield the same digest;
explicit zero bindings remain part of the configuration.

## Scheduling and lease lifetime

The shared scheduler chooses the lexicographically smallest ready node that
fits both the available device slot and *all* its named resources. It scans past
a blocked large request, so a smaller feasible task behind it can run. All
resource quantities are acquired together; a blocked task cannot hold one
resource while waiting for another. Independent branches on different logical
devices still share this one pool.

Each attempted invocation reserves before executor submission. A lease is
identified by node ID and attempt number, starts at attempt 1, and advances
sequentially up to the existing 21-attempt limit. Settled attempts release before
retry backoff. Failed or cancelled descendants that never start do not reserve.
With an enabled policy, zero-demand attempts also count as reservations/releases;
their quantities contribute zero to peaks.

Cancellation, fail-fast and timeout do not free resources belonging to a still
running callback. Thread callbacks remain cooperative and are joined before
return; a late successful value retains the existing cancelled-graph semantics.
Process drivers use the existing reply/stop/retirement contract. Idle reusable
workers do not retain an invocation's logical reservation.

Executor `submit()` can enqueue work and then raise before returning its Future.
On this uncertain-acknowledgment path, the scheduler stops new work and retains
the lease until actual executor shutdown/join returns. It cannot assert that the
task never ran. Control or infrastructure failures join remaining drivers before
bulk release. If shutdown itself fails, no completed execution result is returned
and outstanding reservations are not erased to manufacture successful cleanup.
Primary control exceptions remain primary over ordinary cleanup errors, while a
new cleanup interrupt is not hidden by an earlier ordinary error.

## Result accounting and compatibility

Successful API completion, including ordinary failed/cancelled graph outcomes,
adds `ExecutionResult.resource_usage`. The immutable `ResourceUsage` contains
the policy digest, integer capacities and observed per-resource peaks, plus
reservation/release counts. Completed usage requires equal counts and peaks
within capacities. Per-resource peaks need not have happened at the same instant.
The output is local scheduler accounting, not hardware telemetry or an
authenticated event history; manually constructed valid metadata is not proof.

For process execution, the same field is under `result.execution`, including
its existing nested JSON representation. Arbitrary callable results are still
excluded from telemetry.

Without a resource policy, the original fast heap-head scheduling path remains,
`resource_usage` is None, and the result JSON has **no new key**. Existing
positional `ExecutionConfig` and `ExecutionResult` construction remains valid
because new optional fields are appended. No TaskContext or pickled task
definition format changes are needed.

## Costs and boundaries

Policy validation is bounded by names, nodes and bindings; hashing streams JSON
chunks without concatenating a complete policy string. Very long names across
100,000 bindings can still entail substantial cumulative encoding work, and
`to_dict()` intentionally materializes its bounded output. The scheduler scans
ready candidates when resources are enabled. Reservation updates stage bounded
maps before one state swap; per-node attempt history is at most 10,000 entries,
not an ever-growing list of all 210,000 attempts. This costs copying/scan work,
not O(1) scheduling or a hard CPU/RSS bound. No fairness guarantee is made for
an infinite stream of newly submitted tasks: this API executes a finite DAG.

`CPU`, `GPU`, `decoder-license` and other names are ordinary user-defined labels.
This module does not discover hardware, change environment variables, set CPU
affinity, alter CUDA visibility, enforce physical memory or restrict threads
created by registered functions. It does not reserve resources for the separate
stateful ProcessActor API. Cross-machine placement, distributed resource leases,
autoscaling, external job submission and recovery remain separate open work.
