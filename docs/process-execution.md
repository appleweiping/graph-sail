# Local process DAG execution

`execute_process_graph` runs registered synchronous Python tasks in actual local
spawned processes. It reuses the same admission checks, ready/dependency queues,
logical device slots, fail-fast rules and application retry decisions as
`execute_graph`. A bounded set of driver threads invokes the existing
`ProcessActor` transport; there is no second scheduler or retry loop.

Run the complete offline example from the repository root:

```bash
python examples/process_graph.py
```

It creates a 2 MiB byte object, passes its small reference through a 4 KiB actor
message boundary, independently reads it in two DAG branches, and checks the
hand-derived byte count and sum in a downstream task. It needs no service,
weights, network access or optional dependency. Process startup requires the
usual `if __name__ == "__main__":` guard in executable application modules.

## API and value ownership

```python
result = execute_process_graph(
    graph,
    TaskRegistry({"decode": decode, "analyze": analyze}),
    placements,
    config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
    process_config=ProcessTaskConfig(
        max_message_bytes=1024 * 1024,
        startup_timeout_seconds=30,
        attempt_timeout_seconds=60,
        cancellation_grace_seconds=0.25,
        shutdown_timeout_seconds=5,
    ),
    cancel_event=stop_event,  # optional threading.Event owned by the caller
)
outputs = result.execution.outputs
```

Registrations are explicit trusted Python functions or callable objects, not
import paths loaded from graph documents. Every definition must be spawn
serializable; local functions, lambdas and nonserializable captured objects fail
preflight before any worker starts. All graph, placement, memory and registry
checks are shared with thread execution. Trusted serialization hooks can execute
code even during preflight; registrations and their captured objects are not an
untrusted-input boundary.

A task receives the existing `TaskContext`: node, logical device, one-based
attempt, immediate predecessor results and the read-only `CancellationSignal`
interface (`cancelled` and `raise_if_cancelled()`). Unlike the thread backend,
process argument/result values are serialized snapshots. Mutating a dependency
inside a child does not mutate the driver's retained predecessor result or
another child's separately serialized copy. Aliases within one serialized
message can still be preserved by pickle. Arbitrary output values remain caller
owned; the result mapping is read-only, not its recursively contained objects.

`ObjectRef` remains an explicit reference: the runtime does not automatically
dereference it. Register a callable holding a `LocalObjectClient` to read verified
bytes as needed. Keep the owning `LocalObjectStore` alive until all dependent
tasks finish. Readers allocate verified byte copies; this is not zero-copy,
shared-memory transport, implicit spilling or distributed reference counting.

Each driver owns one serial worker at a time and submits at most one actor call
at once. Healthy workers may run later tasks, including application retries;
module-global state can therefore persist. Each callable/argument is serialized
for each attempt, not installed as a persistent actor instance. Worker reuse is
not a task-to-task state-sharing guarantee. Workers retain the actor runtime's
daemon-process policy: tasks cannot spawn multiprocessing children themselves.

## Retry, cancellation and child ownership

Only an application exception returned by the child is classified using the
original `TaskDefinition.retry_on` classes **inside that child**. The existing
scheduler applies `max_retries` and retry delay. Remote exception classes are not
reconstructed in the driver merely to guess whether an application retry fits.

Startup failures, process death, transport/serialization failures and per-attempt
timeouts are never automatically retried, even when `max_retries > 0`. An attempt
may already have performed side effects before these failures. Independent ready
branches can continue with replacement workers; failed descendants follow the
same skip policy as the thread backend. No exactly-once side-effect claim follows
from joining a child, and no application state or durable lineage is restored.

Cancellation uses a private, one-way native pipe created during spawn bootstrap.
The child receives only the read endpoint; the driver owns the sole write
endpoint. No cancellation bytes are written, and no OS primitive is pickled over
the actor request pipe. The driver closes its writer to signal EOF; child
`cancelled` performs a zero-timeout poll and permanently latches EOF or a broken
handle. Parent disappearance also closes the writer through the OS. There is no
shared Event/condition lock that a dead child could leave held.

External cancellation, the whole-run budget and fail-fast stop admission through
the shared scheduler. Drivers observe that stop, close their channels, and allow
up to `cancellation_grace_seconds` for running callbacks to cooperate. Tasks
should periodically check the token. A response racing with an already-observed
stop cannot override it, including a task that translates cancellation into a
successful return. A per-attempt timeout is a failure (`ProcessTaskTimeout`);
external/run/fail-fast cancellation is a cancelled attempt. An explicit
application `TaskCancelled` is also a cancelled attempt.

Once a worker's channel is cancelled, that worker is permanently retired even
if its callback cooperates. Healthy responses are settled before reuse; cancelled
or broken workers are joined before replacement. No signal is reset between
attempts, so no stale cancellation can reach a later task. Noncooperating workers
are explicitly terminated and joined using the existing actor escalation logic.
No pool lock is held across native close or process join. Startup failure,
serialization failure, normal completion and cancellation close owned endpoints.

All successfully started workers are joined before a result is returned. Cleanup
failure raises rather than pretending the worker is gone; a primary invocation
exception/interrupt remains primary and records secondary cleanup notes. Forced
termination can leave files, remote calls or other application effects incomplete.
The library does not manage resources created by user task code.

## Budgets and what they do not bound

| Setting | Supported bound and interpretation |
| --- | --- |
| `ExecutionConfig.max_workers` | 1–16 local processes; logical per-device slot limits still apply |
| `max_message_bytes` | 1 KiB–16 MiB per serialized actor request or reply |
| Aggregate configured message capacity | `max_workers * max_message_bytes <= 64 MiB`; one pending call per actor |
| Startup budget | At least 1 millisecond and at most 60 seconds per spawned worker; separate from attempt budget |
| Attempt budget | Optional, more than zero and at most 86,400 seconds from driver dispatch, including serialization/transport/module reconstruction |
| Cancellation grace / shutdown budget | 0–60 seconds each; cleanup may additionally need actor escalation joins |

The shared graph limits bound node count and retry count, so diagnostic record
counts are finite (at most one process-attempt record per admitted attempt).
Workers start lazily. A pre-cancelled run starts none. A run-wide stop can be
noticed while startup is still in progress; startup waits remain governed by
their separate budget before the driver can retire that worker.
The actor startup timer bounds its greeting wait after `Process.start()`;
process launch itself and trusted serialization hooks are not preempted by it.

These are protocol/controlled-wait limits, **not a hard process-RSS cap or hard
real-time deadline**. Pickle construction/unpickling, user reconstruction hooks,
native OS operations, interpreter startup/teardown, task allocations and retained
outputs can consume more memory or time. The original caller's large Python
objects already exist before serialization checks. Graph memory reservations
remain logical declarations; they neither measure RSS nor acquire CPU/GPU
hardware. A logical device name does not set affinity or GPU visibility.

## Measurements and diagnostics

`ProcessExecutionResult` composes an `ExecutionResult` with immutable tuples of
process-attempt and worker metadata. These public dataclasses are output data
containers, not verification certificates for manually constructed instances.

- `result.execution.tasks` preserves the shared attempt/status history. For an
  actual child reply, attempt start/end timestamps measure the callback interval
  inside the child, relative to the same host's monotonic clock epoch. For a
  startup/transport/cancellation failure, they measure the driver's observed
  invocation interval instead. `result.attempts[*].timing_source` distinguishes
  these meanings; never interpret a driver interval as user CPU time.
- `round_trip_ms` is the actor's dispatch-to-response duration, including private
  transport and serialization. It is `None` if no completed exchange was measured
  and can quantize to zero for very short exchanges. It is not callback CPU time.
- Worker IDs are per execution, not stable across runs. PIDs identify actual
  spawned children. `termination_requested` means the driver explicitly called
  `terminate`; even a graceful `close` can escalate if its own budget expires.
  The recorded exit code is the process result; no force-kill inference is made
  from that boolean alone.
- Outer `elapsed_ms` includes admission, invocation and final worker shutdown.
  Nested execution elapsed time ends when the scheduler finishes its drivers,
  before healthy idle-worker shutdown. JSON telemetry excludes output objects,
  but bounded exception messages can contain application data; redact those
  before sharing if necessary.

The implementation relies on Python's documented
[shared monotonic clock across processes](https://docs.python.org/3/library/time.html#time.monotonic)
and [spawn/connection lifecycle](https://docs.python.org/3/library/multiprocessing.html).
No speedup, distributed benchmark or whole-reference repository parity is claimed.
