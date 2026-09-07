# Execute a DAG with trusted local functions

`execute_graph` invokes real Python callables and passes their return values to
dependent nodes. `GreedyPlanner`, `BeamPlanner`, `ExactPlanner` and
`simulate_plan` continue to describe estimated placements and intervals. The
executor consumes a placement map, and records measured time separately.

```python
from graph_sail import ExecutionConfig, GreedyPlanner, TaskRegistry, execute_graph

# graph is a validated GraphSpec with a, b, and join nodes.
registry = TaskRegistry(
    {
        "a": lambda context: 6,
        "b": lambda context: 7,
        "join": lambda context: context.dependencies["a"] * context.dependencies["b"],
    }
)
result = execute_graph(
    graph,
    registry,
    GreedyPlanner().plan(graph).placements,
    config=ExecutionConfig(max_workers=2, device_workers={"cpu": 2}),
)
print(result.outputs["join"])  # 42, computed by the registered functions
print(result.to_dict())  # actual attempt/status/timing telemetry
```

A complete executable example, requiring no optional dependencies, is:

```bash
python examples/execute_graph.py
```

## Registration, dependencies, and placement

Register every graph node exactly once. An entry is a synchronous callable or a
`TaskDefinition` containing one. The registry snapshots its bindings, so later
edits to the supplied dictionary do not change the run. Graph documents never
contain executable code, dotted import targets, pickled functions, or eval
expressions. Execution is a Python API because selecting trusted callables is an
application responsibility; the existing planning CLI continues to parse data.

All registry/placement coverage, graph constraints, device compatibility, option
bounds, and memory admission checks run before any callable is invoked. A
placement must honor node pins, allowlists, supported kinds and available
latency-profile device names. Persistent memory is the sum of declared
`NodeSpec.memory_mb` for **all** nodes placed on each device, independent of
whether those nodes are presently running. It must fit the device's declared
budget, using the planner's existing 1e-9 MB floating-point comparison tolerance.
Components retain this logical reservation for the whole run.

The executor admits at most `max_workers` in-flight attempts globally and at most
`device_workers[device]` on each logical device. The per-device default is one.
Increasing it allows independently ready tasks on that device to overlap. Among
currently eligible devices it selects the lexicographically first ready node;
completion timing can affect later submission order. It does not promise a
deterministic interleaving of side effects.

These are **logical admission controls**. They do not pin threads, allocate GPU
handles, isolate CUDA devices, enforce actual RAM limits, or constrain threads
that a task itself spawns. The application uses `context.device` to select its
already-configured resources. Local threads suit I/O or native code releasing
the GIL; CPU-bound Python code does not gain process-level parallelism from this
executor.

A task starts only after every immediate predecessor has succeeded. Its context
contains `node_id`, `device`, the one-based `attempt`, an immutable mapping of
predecessor node IDs to their actual return objects, and a cancellation token.
Root nodes get an empty dependency map; application inputs can be captured in
the registered callables. Edges carry full predecessor results; the graph's
payload and link bandwidth numbers remain planning estimates.

The mapping is immutable, but **its values are shared objects**. Results are not
serialized or deep-copied. Two children can observe the same parent object, and
`result.outputs` retains that object. Use immutable values or application-level
synchronization. Returned outputs and caller-created temporary buffers have no
byte cap, and their memory is not measured by the logical reservation. The
runtime retains all successful outputs until the result is released.

## Failure and retry behavior

An ordinary Python exception records a failed attempt with exception type and a
message capped at 1,024 characters. Exception messages can contain application
data, so applications decide whether to share the telemetry. If an exception's
formatter itself fails, a fixed diagnostic is retained instead. Exceptions are
not stored as dependency values. Unsuccessful nodes' descendants do not execute;
they receive `skipped` records with the responsible dependency ID. Other
branches continue by default.

```python
from graph_sail import TaskDefinition

task = TaskDefinition(
    function=fetch_from_application_cache,
    max_retries=2,
    retry_on=(TimeoutError,),
    retry_delay_seconds=0.1,
)
```

Retries are disabled by default. `max_retries=2` permits at most three attempts;
only matching ordinary exceptions retry. A delayed retry releases its device
slot, allowing unrelated work to run. Descendants wait for the final successful
attempt. Every attempt remains in telemetry; only successful values enter the
output map. Cancellation is never retried. A retry may repeat side effects, so
applications must make opted-in tasks idempotent or deduplicate their effects.
There is no crash recovery or exactly-once guarantee.

`fail_fast=True` requests cooperative cancellation after an unsuccessful task
exhausts its retry policy. Its descendants remain dependency-skipped; other
unstarted work is cancelled. Already-running tasks receive the same cooperative
token. Process-control exceptions such as `SystemExit` and `KeyboardInterrupt`
are not converted to application failures or retried: they propagate after
worker cleanup.

## Cancellation and time budgets

Pass a `threading.Event` as `cancel_event` to request cancellation externally.
Already-set events cause zero callable entries. Setting the event during a run
stops new submissions after the scheduler observes it; tasks can check
`context.cancellation.cancelled` or call
`context.cancellation.raise_if_cancelled()` to acknowledge it with
`TaskCancelled`. A task can also explicitly raise that exception to decline its
own work, which skips its dependent branch without cancelling independent work.

`ExecutionConfig(timeout_seconds=...)` measures a wall-clock budget from the
start of the scheduler. Expiry **requests cooperative cancellation**. The runner
joins already-running callables before returning. It does not terminate Python
threads or return while they silently continue changing state. A task that
ignores cancellation can therefore extend elapsed time past the budget. If it
returns normally, its successful output is retained, while the overall run still
reports the cancellation request. No later dependent task is started.

The scheduler checks requests at submission boundaries and after waits of at
most 20 ms; operating-system scheduling can add delay. There is no guaranteed
hard deadline. This behavior is tested both with cooperating tasks and with a
deliberately noncooperating task that must be released before the run returns.

## Results, timing, and work bounds

`ExecutionResult.tasks` is in stable topological order. Each node has a terminal
status, reason, and zero or more attempts. Every attempt records submission,
actual worker entry and finish times from one monotonic-clock origin. These are
observations, and will differ across executions. No estimated compute,
contention, batching, or transfer duration causes the executor to sleep.

`to_dict()` produces `graph-sail-local-execution` schema version 1. It includes
terminal statuses, bounded exception diagnostics, timings, logical memory
reservations, peak in-flight counts and successful output **node names**. It
deliberately excludes arbitrary output objects and callable representations.
The overall status is failed if any task failed; otherwise it is cancelled if
cancellation occurred, and succeeded only when all nodes returned successfully.

Repository graph limits remain 10,000 nodes, 100,000 edges and 1,024 devices.
The executor permits 1–64 threads, 1–64 slots per device, 0–20 retries per node,
up to 16 retry exception classes, 0–60 seconds of retry delay, and an optional
time budget from 1 microsecond to 24 hours. Thus at most 210,000 attempt records
can be retained for one maximum-sized graph. Ready queues and graph indexes use
`O(N + E + D)` memory, plus attempts and arbitrary application results. Dispatch
checks eligible device queues, costing `O(D)` per submitted attempt, plus heap
and dependency bookkeeping. Every run creates and joins its own thread pool.

Distributed workers, actors, object references, spilling, worker crash recovery,
cluster placement, runtime environments and serving/training services remain
open reference requirements; see [the pinned Ray gap audit](parity-runtime.md).
