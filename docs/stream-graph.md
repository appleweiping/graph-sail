# Per-yield local DAG execution

`start_stream_graph` connects the bounded native source stream to the existing
local `GraphSpec` dependency runner. Every accepted source item executes one
complete, explicitly registered fork/join DAG. The first item's graph may
finish before the source reaches EOF. See the runnable
[offline example](../examples/stream_graph.py).

The graph has exactly one root, `input_node`. The runtime provides that node's
value from each serialized source yield; **do not** include it in the task
registry. Every other node needs a registered synchronous callable, and every
node must lie on a path from the input to the selected `output_node`. Pass an
exact placement for every node. `ExecutionConfig` still governs DAG worker
slots, placements and logical resources *within* each invocation. Automatic
task retries are disallowed to avoid repeating ambiguous per-item effects.

```python
with start_stream_graph(
    source,
    graph,
    TaskRegistry({"left": left, "right": right, "join": join}),
    {"input": "cpu", "left": "cpu", "right": "cpu", "join": "cpu"},
    input_node="input",
    output_node="join",
    config=StreamMapConfig(max_pending=2, max_workers=2),
    execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3}),
) as stream:
    for item in stream:
        print(item.sequence, item.value)
    print(stream.completion().status)
```

`StreamMap` owns the source, result snapshots, accepted-item credit, ordered
consumption, cancellation and cleanup. A credit is acquired before source
advancement and released only when the ordered public result is consumed;
there is no unseen prefetch. Distinct item DAGs may run concurrently and
complete out of order; the public stream remains in source order. For
cross-instance conservative admission, stream workers times graph workers
must not exceed 64, per-device graph memory times stream workers must fit
the declared device budget, and the sum of all per-node logical resource
requests times stream workers must fit each resource capacity. This may
reject a schedulable graph. Capacities are local logical accounting, not
physical isolation or coordination with another run.

An input is snapshotted before its DAG starts, and the final result is
snapshotted before public delivery. Intermediate values are ordinary shared
Python objects among graph tasks in one invocation. Application tasks must
treat dependency values as read-only; this API does not make mutable Python
objects immutable. The snapshot byte limits do not bound arbitrary user
allocations or intermediate values. Callables and snapshot hooks are trusted.

If one item graph fails, prior ordered results can still be consumed;
`StreamGraphExecutionError` reports the item sequence, failed node IDs and
the existing `ExecutionResult` task-attempt telemetry. The graph runner does
not retain the original Python exception object; the stream error cannot
reconstruct it. There is no automatic per-item retry or external side-effect
exactly-once guarantee. Cancellation discards future publications and asks
cooperative tasks to stop. A task blocked in its own code can make
`close(timeout)` report unresolved ownership; retry the same handle after
the task exits.

This is a local threaded DAG per yield, not a process/actor graph, distributed
ObjectRef dependency, global cluster resource scheduler, durable checkpoint,
or Ray worker-failure recovery. The prior finite `execute_graph` and stream
APIs remain unchanged.
