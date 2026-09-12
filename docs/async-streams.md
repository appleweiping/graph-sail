# Event-driven asynchronous stream consumption

Both `TaskStream` and `ProcessTaskStream` support `async for`, `anext()` and
explicit coroutine waits. They retain the **same one consuming mailbox** as
synchronous iteration. This is a consumer API for the existing native synchronous
generators, not native async-generator execution or a second producer scheduler.

```python
async def consume(stream):
    async for item in stream:
        print(item.sequence, item.value)
    return await stream.completion_async()
```

Run `python examples/async_task_streams.py` for an offline example with a real
producer thread and spawned worker. The thread's tail is event-gated until its
first item has been consumed and the event loop has done independent work. The
process result is checked against square numbers, actual worker PID, yield-cap
status and native cleanup acknowledgement. Startup and final owner cleanup
occur outside the event loop.

## Methods

| Method | Observation | Wait timeout |
| --- | --- | --- |
| `next_async(timeout=None)` | Consume one `TaskStreamItem` | `TimeoutError` |
| `wait_ready_async(timeout=None)` | Nonconsuming readiness for an item, settled end/failure, or discard | `False` |
| `completion_async(timeout=None)` | Settled `TaskStreamResult` or `ProcessStreamResult` | `TimeoutError` |
| `__aiter__()` / `__anext__()` | The stream itself / `next_async()` without a deadline | No implicit timeout |

Timeouts retain the existing finite `[0, 86400]` seconds contract, or `None` for
no deadline. Booleans, nonfinite values and coercions are rejected even when a
result is available. Validation runs when awaited or scheduled, not merely when
a coroutine object is constructed. `next_async` performs one cancellation
checkpoint before observing the mailbox, even for immediate items and EOF. Its
timeout covers the subsequent coordination wait, not a hard total wall time.
Readiness and completion can return immediately without yielding. No immediately
ready observation acquires a subscription.

Successful, limited or cancelled iteration ends with `StopAsyncIteration`.
Queued `None` values are ordinary items. Accepted queued items precede a stored
failure during iteration, whereas `completion_async()` can observe settlement
and failure without draining. Readiness does not raise a stored producer error
and is not a success assertion.

Readiness reserves nothing: another reader can consume the item before this
reader resumes. Synchronous and asynchronous readers across different loops
compete for each item exactly once. No broadcast, fairness or per-reader order
of completion times is promised. Sequences retain the producer's accepted order.

## Cancellation, deadlines and data ownership

Cancelling an awaiting asyncio task removes only its private wait subscription.
Its `CancelledError`, including its message, belongs to that waiter. Cancellation
or timeout does not call stream `cancel()` or `close()`, request worker stopping,
acknowledge generator closure or change accepted counts. This also applies to
cancellation introduced by `asyncio.wait_for()`.

The initial checkpoint delivers an already-requested cancellation before taking
an available item. A previously caught cancellation does not poison later reads:
the implementation does not treat `Task.cancelling()` as a rejection flag.
Notifications do not reserve or dequeue. Only the running/resumed coroutine
dequeues under the mailbox lock, with no suspension between dequeue and return.
A cancelled task that has not reached that dequeue cannot take an item from
another reader. Cancellation is not rollback after a completed dequeue. An item
observed in the final deadline snapshot wins over a stale timeout observation;
these are concurrency races, not hard-time promises.

Thread values remain borrowed application objects, including later mutations by
trusted producer code. Process values retain their per-yield serialized snapshots.
Async consumption adds no copies, object store, automatic application-object
close, retry, replay or recovery.

Completion does not consume buffered items, so backpressure can prevent it until
another consumer drains the queue or the owner requests stop. The queue limit is
enforced **before** each next producer advance, including the process RPC. Reaching
the yield cap still does not perform an extra advance to guess EOF. Awaiting
completion never closes the owner.

Explicit `cancel()` preserves accepted items. Explicit `close()` discards them
and wakes item/readiness waiters, but does not claim that a live uncooperative
producer has settled. Completion waits for execution and backend cleanup.
Repeated timed-out closes do not publish repeated discard transitions. Native
cleanup flags retain the [process-stream contract](process-task-streams.md).

## Shared failures

As with [async results](async-results.md#shared-source-failures), an ordinary
stored producer/transport/cleanup failure raises a fresh `AsyncSourceError` with
the original stored exception as its `__cause__`. A stored control exception
raises fresh `AsyncSourceControlError`, which derives from `BaseException`.
Source-side cancellation is distinct from cancellation of this waiter. Repeated
async reads do not mutate the source traceback.

Remote exceptions retain the transport representation: an ordinary remote error
has a cause of type `ActorRemoteError`, not a recreated remote Python exception.
Wrappers do not authenticate or sanitize tracebacks or bound exception-held
application objects. Synchronous accessors retain their original-error contract.

## Notification and lifetime bounds

All three coroutine methods share one completion hub per stream, including the
process wrapper and underlying mailbox. At most 256 live waiting subscriptions
and 256 loop slots are admitted. Exceeding either raises `AsyncWaitLimitError`.
These are per-stream bookkeeping limits, not application-wide quotas.

Accepted append, first discard and final settlement publish state before
notifying outside the mailbox lock. Only settlement makes the hub terminal.
Snapshot/epoch subscription closes the lost-wakeup window, including when a
synchronous reader consumes an item before an async reader resumes. Epochs are
bounded by accepted yields plus at most one discard and one settlement.

Notifications coalesce to one pending internal pump per loop. An empty slot with
a pending pump remains charged, preventing unbounded stopped-loop cancellation
churn. Delivery, closed-loop observation or collected-loop observation releases
that slot. The existing hub weakly references loops and notification Futures and
posts with an empty context. It does not set values or source errors on a Future
from a foreign thread.

Applications must settle tasks before closing their loop; a closed loop cannot
resume abandoned tasks. One closed loop does not cancel shared work or poison
other readers. Loops, synchronization and callbacks are trusted Python runtime
providers, not hostile sandbox inputs. No polling, hidden asyncio tasks,
executors, `to_thread()` or extra waiter threads are introduced. The initial
single checkpoint is a cancellation boundary, not a periodic polling mechanism.
Locks, destructors, callbacks, OS operations and loop progress are not hard-time
or RSS bounded.

## Explicit remaining boundaries and verification

Startup and `close()` remain synchronous. There is no `aclose()` or async context
manager. Put potentially blocking startup/cleanup outside the loop or provide an
application-owned lifecycle strategy. A producer must not await its own stream.
Native async producers, async actor methods, actor-method streaming, per-yield
DAG scheduling, cross-host scheduling/object lifetime/failure reconstruction,
integrations and whole-reference scale remain open in the
[whole-repository assessment](parity-runtime.md).

Tests cover controlled publication/dequeue/cancellation/deadline races, two real
loops plus a synchronous reader, 256 mixed-method waiters, cancellation churn
and weak task references, closed-loop pruning, traceback stability, real child
PID/snapshot/cap/cleanup oracles and the guarded installed example. Review exposed
pending self-cancellation before an immediate read; a failing regression was
retained before adding the single checkpoint. Final full-suite, coverage and
package evidence is recorded after execution, not inferred from test design.
