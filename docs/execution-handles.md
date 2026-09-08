# Nonblocking execution and selected node results

`start_graph` and `start_process_graph` run the same admitted DAG and scheduler as
the blocking APIs. They return a locally owned `ExecutionHandle` after synchronous
graph, registry, placement and configuration checks. Process registrations are
also serialized before returning; actual workers start lazily in the controller.
No callable names are imported from graph documents.

```python
with start_graph(graph, registry, placements, config=config) as execution:
    first = execution.wait(("decode", "embed"), count=1, timeout=2)
    for node_id in first.ready:
        metadata = execution.node(node_id).execution()
        if metadata.status == "succeeded":
            value = execution.node(node_id).result()
    final = execution.result()  # normal failures are recorded in this result
```

The executable offline example `python examples/execution_handles.py` uses
actual event-controlled thread tasks: one branch remains blocked while another
branch and its dependent publish results. It asserts the hand-derived results
before releasing the unrelated task. No model, service, GPU or network is needed.
For processes, executable application modules need the usual
`if __name__ == "__main__":` startup guard; see [process execution](process-execution.md).

## State and result contract

- `nodes` is the admitted graph's deterministic topological tuple of IDs.
- `node(id).done()` reports a published terminal status, not success or a running
  attempt. Retries publish once, after the final attempt, with all attempt records.
- `node(id).execution(timeout)` waits for immutable `TaskExecution` metadata.
- `node(id).result(timeout)` returns a successful value, including `None`.
  A failed, skipped or cancelled node raises `TaskNotSuccessful`; its `execution`
  attribute retains the exact status, reason and attempt metadata.
- `wait(ids=None, count=1, timeout=None)` returns `WaitResult(ready, pending)` in
  requested order. It returns **all** available terminals, possibly more than
  `count`. Every terminal status counts as ready. The selection must be a unique
  tuple of IDs from this graph, not an arbitrary or unbounded iterator. `count=0`
  is a nonblocking snapshot; an empty selection requires `count=0`.
- A wait timeout returns available IDs; individual/final result timeouts raise
  `TimeoutError`. Neither form cancels or consumes work. Timeouts are finite
  seconds in `[0, 86400]`, excluding booleans; `None` waits without a time limit.
- `result(timeout)` returns the same backend result type as its blocking API,
  after owned worker cleanup. Process results include joined-worker diagnostics.
  A failed task normally makes the result's status `failed`, not an exception
  from the whole-result accessor.

Scheduler, infrastructure-cleanup or control exceptions instead terminate the
controller and are re-raised by the final accessor, like a local Future. Already
published node results remain readable. A waiter requiring an unpublished node
is awakened and raises the stored failure; no terminal success is fabricated.
`done()` means the backend and its cleanup have settled, which can include a
raised cleanup failure. It is not itself a success or complete-resource-recovery
certificate when the backend reported such a failure.

## Ownership, cancellation and closing

An execution owns one **non-daemon** controller thread and the backend's existing
bounded workers. Use a context manager or explicitly call `close()`, including
after `result()`. There is no destructor, background-job persistence or implicit
interpreter-shutdown cleanup guarantee. Waiting from a task on its own graph's
completion can deadlock and is unsupported.

`cancel()` requests a whole-graph stop. It returns `False` if already requested or
the controller has finished. It does not cancel one selected node, revoke past
side effects, or guarantee that a concurrently returning thread task will lose
its successful result. The scheduler's existing cancellation, fail-fast and
retry policies remain authoritative. Process workers retain their existing
cooperative grace and forced-retirement policy.

`close(timeout=None)` requests cancellation, waits for backend settlement, then
joins the controller. Only a successful explicit join sets `closed=True`.
A timeout does not abandon ownership: retain the handle and retry `close`.
The timeout covers settlement and join together. A trusted thread callback that
ignores cancellation can delay unbounded closing indefinitely. Close does not
raise a previously stored execution failure; use `result()` to observe it.
Context exit closes even when its body raises; control exceptions take priority
over ordinary cleanup errors, and an ordinary cleanup error does not replace a
body failure. This is local lifecycle supervision, not hard-time OS preemption.

All observation/wait/cancel/close methods can be used by multiple application
threads. Results are not consumed, so several waiters can read the same value.
Node and execution handles are local coordination objects, not portable
object-store references, serializable job identifiers or externally restorable
jobs. Get node handles through `execution.node(...)`; direct constructors are
internal plumbing and do not launch or admit a graph.

## Values and costs

Terminal metadata is immutable, but successful values are **borrowed**, not
copied or frozen. Thread task outputs are shared with live dependents and the
final result. A received process value is a parent-side object also used for
later dispatch: mutating it before a dependent's arguments are serialized can
change that dependent's input. Use immutable data or application synchronization.
Only the existing process boundary makes serialized copies; early reads do not
silently execute additional user serialization or deep-copy hooks.

The graph admission ceiling remains 10,000 nodes. The handle retains at most one
terminal record/value binding per node plus the existing final result. It does
not copy an entire graph snapshot on each completion. A selected wait scans its
bounded selection when awakened, and notification wakes current waiters; this
is not a distributed or constant-cost subscription service. Returned arbitrary
Python object sizes and the number of application-created handles/waiters are
not bounded by a new memory quota. Existing process message and worker limits
still apply; no throughput claim is inferred from correctness tests.

Cross-node scheduling, durable recovery, public per-node cancellation, task
generators/multiple returns, nested task submission and `asyncio` adapters remain
open contracts. This increment adds local asynchronous ownership and partial
results; it does not establish entire-reference parity.
