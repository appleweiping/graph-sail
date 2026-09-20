# Spawn-process DAG for each accepted stream yield

`start_process_stream_graph` runs one complete registered fork/join DAG in
**actual local spawned processes** for each item from one local native generator.
The first selected output may be consumed before the generator reaches EOF.
The producer itself remains in an owned local thread; neither it nor its state
is moved to a subprocess. See the [offline example](../examples/process_stream_graph.py).

```python
with start_process_stream_graph(
    source,
    graph,
    TaskRegistry({"left": left, "right": right, "join": join}),
    {"input": "cpu", "left": "cpu", "right": "cpu", "join": "cpu"},
    input_node="input",
    output_node="join",
    config=StreamMapConfig(max_pending=2, max_workers=2),
    execution_config=ExecutionConfig(max_workers=3, device_workers={"cpu": 3}),
    process_config=ProcessTaskConfig(max_message_bytes=1_048_576),
) as stream:
    for item in stream:
        print(item.sequence, item.value)
```

The graph must have exactly one root (`input_node`), every node must contribute
to the selected `output_node`, and every other node must be registered with a
trusted, spawn-serializable synchronous callable. All placements are explicit.
Automatic task retries are rejected before source entry. A local lambda or
nested function cannot be sent to a spawned worker with this standard-library
pickle transport. Put process tasks in importable modules and run the caller
under `if __name__ == "__main__":`.
Admission checks pickle frames and rejects known driver-only module origins
(such as a module created solely in the parent's `sys.modules`) without
importing application modules or starting a probe child. This is not a proof
that a fresh spawned interpreter can import every task: import hooks,
namespace packages and the main module can differ at runtime. A later child
import failure is reported for that source sequence as infrastructure failure,
with driver-attempt error type/message in `process_result`; it is not retried.

The input node is supplied by the runtime and must not appear in the caller's
registry. Its per-item value is already snapshotted by `StreamMap`, then
serialized again for a private, top-level input task in a child process. The
child's return comes back to the driver; dependent tasks receive their own
serialized argument snapshots. This can duplicate transport and allocation.
There is no zero-copy or distributed ObjectRef claim. Application pickle hooks
are trusted and may run more than once; decoded heap and intermediate task
values are not bounded by the frame limit.

The returned `ProcessStreamGraph` wraps `StreamMap` and retains its ordered
consuming reader and pre-advance credit:
`accepted - published <= max_pending`, including work queued, running or
finished out of order. Credit is released on public consumption, not on child
finish. No extra source advance probes EOF at the exact yield cap. A source
or item-graph failure preserves the prior ordered successful prefix; later
items are cancelled/discarded. A failed/cancelled item raises
`ProcessStreamGraphExecutionError`, with its source sequence, failed-node IDs,
underlying `ExecutionResult` and `ProcessExecutionResult` process attempts and
joined worker observations. Process startup/death/transport/timeout failures
raise `ProcessStreamGraphInfrastructureError` with sequence and either the
original raised exception as `cause`/`__cause__` or the completed process
result containing a failed driver-side attempt with error type/message. These
are observational diagnostics,
not proof that a crashed worker performed no external effect.
Neither application nor infrastructure failure is automatically retried. A
lost reply may have followed external side effects; there is no exactly-once
or rollback guarantee.

Admission is deliberately conservative across independent per-item pools:

- At most 16 pending items, 256 graph nodes and 16 configured simultaneous
  process workers (`stream max_workers * execution max_workers`).
- At most 64 MiB configured aggregate process-message capacity (that worker
  product times `process_config.max_message_bytes`), in addition to the
  existing 64 MiB stream snapshot budget and each actual complete-frame cap.
- The per-device graph memory and per-node named-resource claims are summed
  across every possible concurrent item DAG. This can reject a feasible
  staggered schedule; it is not a global inter-run lock, GPU acquisition, CPU
  affinity or a measured RSS limit.

`next(timeout)`/`completion(timeout)` only bound the caller's wait. External
`cancel()` stops source advancement and signals running item graphs. The
process executor sends irreversible cooperative cancellation to its children,
allows its configured grace, then retires and joins them, escalating if
necessary. A child blocked in user code, trusted serialization hook, native
spawn or OS teardown may exceed a requested outer close timeout. Then the
same handle remains the owner and `closed` stays false; retry `close()` later.
If the process executor's final cleanup itself fails after the mapper settles,
the original raised exception retains a `process_graph_cleanup` capability.
The stream wrapper retains and retries that capability during its own close;
another cleanup failure leaves `closed == False`. Retry can fail again and is
not a promise of recovery from arbitrary OS failure.
Forced termination cannot certify child `finally` blocks or external effects.
`completion()` is not successful until accepted graphs and worker cleanup
finish. The result values are independently snapshotted for the public reader;
the process-execution telemetry is exposed on errors, not archived for every
successful item.

This is a local execution integration, **not** a Ray-compatible generator,
distributed object store, process/actor source, multi-source join, worker-fault
reconstruction, cluster scheduler or Ray whole-repository equivalent. See the
[frozen-reference parity matrix](parity-runtime.md) for open subsystems.
