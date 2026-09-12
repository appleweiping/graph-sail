# Bounded asyncio result and selected-terminal waits

The existing local `ActorCall`, `ExecutionHandle`, and `NodeHandle` now have
explicit coroutine methods. They use event-loop notifications, not a waiting
thread per coroutine, `asyncio.to_thread`, polling, or a bridge that transfers
Future cancellation to shared work. Python 3.11 or newer is required, as before.

```python
async def observe(execution):
    partial = await execution.wait_async(("decode", "embed"), count=1, timeout=2)
    for node_id in partial.ready:
        metadata = await execution.node(node_id).execution_async()
        if metadata.status == "succeeded":
            value = await execution.node(node_id).result_async()
    return await execution.result_async()
```

Run `python examples/async_results.py` for a complete offline example. A genuine
spawn-process actor preserves state across asynchronously awaited calls; a local
thread DAG publishes a dependency while another branch is blocked. The example
checks exact expected values and demonstrates cancellation of one waiter without
stopping the graph. No model, GPU, network, dependency installation, or service
is needed. The same notification mechanism also supports
[asynchronous stream consumption](async-streams.md), with a separate single
cancellation checkpoint before destructive item reads.

## Methods and values

| Method | Returned value | On its wait deadline |
| --- | --- | --- |
| `ActorCall.result_async(timeout=None)` | The received actor return object | `TimeoutError` |
| `ExecutionHandle.result_async(timeout=None)` | Final `ExecutionResult` or `ProcessExecutionResult`, after backend cleanup | `TimeoutError` |
| `NodeHandle.execution_async(timeout=None)` | Immutable terminal `TaskExecution` | `TimeoutError` |
| `NodeHandle.result_async(timeout=None)` | Successful node value, including `None` | `TimeoutError` |
| `ExecutionHandle.wait_async(node_ids=None, *, count=1, timeout=None)` | `WaitResult(ready, pending)` | Latest available partial `WaitResult` |

Selections, counts, ordering, status meaning, and finite timeout admission reuse
the [synchronous handle contract](execution-handles.md). All ready IDs are
returned in requested order, not just the first `count`; failure, skip and
cancellation count as terminal. `count=0` is an immediate snapshot. Timeout is
`None` or finite seconds in `[0, 86400]`, excluding booleans. Validation still
runs for already-complete results. The coroutine body, including validation,
starts when the application awaits or schedules it.

An already-available result is returned without subscribing or yielding.
Successful values are borrowed objects, not frozen/copied values or portable
references. Multiple async and synchronous reads return the same object; the
existing process transport is the only implicit serialization boundary. A node
that did not succeed raises a fresh `TaskNotSuccessful` with its terminal metadata.
Normal failed tasks remain represented in the final execution result.

## Cancellation, deadlines, and ownership

Cancelling a task that is awaiting one of these methods removes only that
waiter's private subscription. The caller receives its own `CancelledError`,
including its message. A wait deadline also removes only that subscription.
Neither calls `ActorCall.cancel()`, graph `cancel()`, `close()`, nor a source
Future's cancellation method. A terminal value observed at the deadline wins
over a stale timeout snapshot; completion and cancellation are ordinary races,
not a guarantee that one wall-clock instant always determines the winner.

Explicit source cancellation retains its existing semantics. In particular,
`ActorCall.cancel()` can cancel a queued request, not a running method; graph
`cancel()` requests whole-execution stop. Cancelling an asyncio waiter neither
acknowledges these operations nor rolls back task side effects.

All execution, thread, process, and actor ownership remains with the original
owner. Awaiting the final result does not close it. Use the existing synchronous
context manager or `close()` in application cleanup. Startup/preflight and close
are still synchronous operations; this increment does not make them event-loop
nonblocking or force-stop trusted noncooperating thread callbacks. Do not await a
task's own graph completion from that task. Applications create and own their
asyncio tasks; this library creates no hidden task, executor, or per-wait thread.

## Shared source failures

Synchronous accessors are unchanged: they re-raise their stored source exception.
Async accessors deliberately use a separate contract so repeated reads do not
keep extending the shared exception's traceback:

- An ordinary source/transport/controller failure raises a fresh
  `AsyncSourceError`, whose `__cause__` is the original stored exception.
- A source control exception, such as `KeyboardInterrupt`, `SystemExit`, or
  source-side `asyncio.CancelledError`, raises a fresh
  `AsyncSourceControlError`. This derives from `BaseException`, not `Exception`.
- Explicit cancellation of a queued actor request raises
  `AsyncSourceCancelledError`, distinct from cancelling this async waiter.

The wrapper does not format the original exception or mutate its traceback.
Retained source exceptions still own their original traceback/context and any
referenced application objects; there is no new privacy or memory sandbox.
Application task failures use existing task metadata rather than these wrappers.
Already-published successful node values remain readable after controller failure.

## Bounded notification lifecycle

One execution and **all its node handles together** admit at most
`MAX_ASYNC_WAITERS == 256` live waiting subscriptions. Each actor request has its
own independent 256 ceiling. Immediate observations consume no slot. Admission
beyond the limit raises `AsyncWaitLimitError`; timeout, cancellation, success,
and failure remove a live subscription in `finally`. Repeated cancelled waits
do not append callbacks to the shared `concurrent.futures.Future`.

An execution/request also keeps at most 256 event-loop slots. One internal pump
may be pending per loop; notifications coalesce. A slot with no waiters is kept
while its pump is pending, so cancel/resubscribe churn on a stopped loop cannot
queue unbounded callbacks. This means loop-slot capacity can be occupied even
when live-waiter count is zero. Running the pump, observing a closed loop, or
observing that the loop was collected releases its slot. Loops and private
notification Futures are weakly referenced. Queued pumps weakly reference the
hub and carry an empty `contextvars.Context`, not producer application context.

State is published before notification. A snapshot/epoch check atomically arms
each subscription only if no intervening publication occurred, covering
completion between observing state and subscribing. A final notification may
overtake a node notification without losing the already-published node. The
broker/controller only posts internal pumps with `call_soon_threadsafe`; it does
not execute application callbacks or set an asyncio Future from a foreign thread.

Normal loop-close `RuntimeError` during posting abandons that closed loop's
subscriptions without cancelling the shared source or poisoning other loops.
If a loop accepts a post and then closes/discards it, pruning happens on a later
hub operation; no code can resume an abandoned task on an already-closed loop.
The application must cancel/settle its tasks before closing their event loop.
Custom loops and Python synchronization/runtime objects are trusted providers,
not hostile plugins. Control exceptions from posting are not silently swallowed.

These are per-handle bookkeeping limits, not a global application quota or a
hard-time/RSS guarantee. The existing graph ceiling is 10,000 nodes; a selected
wait scans its bounded selection on each observation. Source publication epochs
are naturally bounded by graph terminal publications plus final settlement, or
one actor terminal state. The implementation does not promise lock-free waits,
constant-cost graph observation, bounded arbitrary returned object sizes, or
distributed subscriptions. Cross-host streaming, async actor method
execution, cluster scheduling, and complete reference-repository parity remain
open.

## Independent regression evidence

Tests cover actual two-thread/two-event-loop delivery, real spawn actor and
process-DAG results/cleanup, deterministic producer barriers at snapshot,
acquire and arm, completed-state-before-notify and terminal-overtake ordering,
256 cross-node waiters and 256 pending loop slots, repeated cancellation and
deadline churn, empty notification context/weak ownership, all terminal statuses,
and fresh ordinary/control exception wrappers without source traceback growth.
The first four missing-API tests were observed failing before implementation.
Final repository gate results are recorded in the parity audit after execution;
these correctness tests do not establish throughput or distributed scale.
