# Native async-generator task streams

`start_async_task_stream()` runs a fresh native async generator on one explicitly
owned, lazy `asyncio.Task`. Its bounded mailbox contains borrowed application
objects. The source can genuinely await application Futures while other work on
the same event loop proceeds. No producer thread, executor, process, materialized
result list, or nested event loop is introduced.

```python
import asyncio
from graph_sail import TaskStreamConfig, start_async_task_stream


async def numbers(context):
    for number in range(4):
        await asyncio.sleep(0)
        context.cancellation.raise_if_cancelled()
        yield number


async def main():
    stream = start_async_task_stream(numbers, config=TaskStreamConfig(max_buffered=1))
    async with stream:
        async for item in stream:
            print(item.sequence, item.value)
        result = await stream.completion_async()
        assert result.status == "succeeded" and result.produced == 4
    assert stream.closed


asyncio.run(main())
```

Run `python examples/native_async_task_streams.py` for the offline event-gated
example. It verifies real loop interleaving, ordered borrowed values, the exact
yield cap, awaited finalization and explicit owner closure without network or
model access. The older [thread/process asynchronous consumers](async-streams.md)
remain separate APIs with unchanged lifecycle and cross-loop semantics.

## Admission, task creation and supported runtime

Call the factory inside a running owner loop. Supply an exact native Python
async-generator function, called with one `TaskStreamContext`. Bound methods,
partials, callable instances, borrowed generator objects, arbitrary async
iterators and coroutine functions are rejected before executing their hooks.
Annotations and custom signatures are not evaluated. An incompatible argument
signature is a stored source failure when the function is invoked. A changed
function code object is revalidated before invocation. Use the factory, not the
implementation constructor, to create owners.

Configuration is the existing exact `TaskStreamConfig` or `None`: `max_buffered`
defaults to 16 with range 1..1024, and `max_yields` defaults to 100,000 with range
1..10,000,000. Integers are exact, excluding booleans and coercions. Configuration
is revalidated at startup. These are item-count bounds, not byte, RSS, runtime,
exception-size or source-internal memory limits.

The runtime contract is CPython 3.11 through 3.14 with functioning, unmodified
standard-library SelectorEventLoop / ProactorEventLoop, Future, Task, clock and
deferred/FIFO scheduling primitives. Alternative event loops, altered scheduling
or Task construction, hard allocation exhaustion and loop/interpreter destruction
are outside the ownership/progress guarantee. This is not a sandbox or a claim
to identify hostile loops through their class names. Trusted source code and its
destructors may execute arbitrary Python and block the event loop.

The producer uses direct `asyncio.Task(..., loop=owner_loop, context=...)` with
default lazy construction. It deliberately bypasses custom and eager application
task factories without modifying them. Their instrumentation and eager-start
behavior do not apply to this one Task. An enclosing `TaskGroup` does not own it
automatically. The caller's context is copied before Task creation; internal
notification pumps use the existing empty-context isolation. No mutable Task or
generator is exposed as a public ownership escape hatch.

The stream and a private active-owner registry hold the producer strongly until
the actual Task settles. Dropping a handle does not cancel it. This registry is
not a global stream quota, and garbage collection is not a deterministic cleanup
strategy. Retain the handle and explicitly close it before destroying its loop.
Tasks created by source code are not transitively owned by the stream.

## Methods and loop affinity

Every public operation requires the same running owner loop, even for already
ready or closed state. Foreign-loop/thread and no-running-loop access fail before
observing or changing state. There are no blocking synchronous readers or close
methods on this class. A source may query state or request cancellation, but may
not wait on, consume, close, iterate or enter/exit the context of its own stream.
That guard uses actual Task identity. It also applies while the same source Task
is executing owned cleanup. Dependency cycles through source-created child Tasks
are not detected.

| Method | Meaning | Coordination timeout |
| --- | --- | --- |
| `next_async(timeout=None)` / `anext(stream)` | Consume one `TaskStreamItem` | `TimeoutError` |
| `wait_ready_async(timeout=None)` | Nonconsuming item, terminal or discard readiness | `False` |
| `completion_async(timeout=None)` | Actual producer settlement, without draining or closing | `TimeoutError` |
| `cancel()` | First accepted stop request, preserving the prefix | Returns `True` once, otherwise `False` |
| `aclose(timeout=None)` | Discard, request stop and acknowledge native ownership settlement | `TimeoutError`; retain owner and retry |
| `done()` | Actual Task settled and terminal record published | No wait |
| `closed` | Explicit close acknowledged settled Task and no retained native generator frame | No wait |
| `cleanup_incomplete` | Settled Task still owns a generator frame after its sole cleanup attempt | No wait |

Timeouts are `None` or exact finite numbers in `[0, 86400]` seconds; booleans,
coercions and nonfinite values are rejected. Validation runs when a coroutine is
awaited/scheduled. A timeout bounds the coordination wait, not source execution,
synchronous callbacks, destructors, loop scheduling, or total wall time.

`async for` uses the same single consuming mailbox. Competing readers get each
accepted sequence once, not a broadcast; readiness does not reserve a value.
Borrowed object identity and later mutations remain visible. The API never
copies, serializes or closes a yielded application value. Accepted prefixes are
preserved before a source failure, whereas completion may expose that failure
without draining. Readiness is not a success assertion and never raises a stored
source failure. Cancelled, successful and limited iteration end with
`StopAsyncIteration` after the prefix.

Backpressure is checked **before** each `anext(generator)`. Reaching max_yields
does not peek once more to distinguish EOF and reports `limited`. Completion
does not free mailbox credit, so a full buffer can keep completion waiting until
a consumer drains it or the owner requests stop.

## Cancellation and close acknowledgement

Consumer cancellation/timeout removes only a private notification wait. It never
awaits or cancels the producer Task, claims generator cleanup, or changes counts.
`next_async` has one cancellation checkpoint before even an immediately ready
dequeue. Cancellation already requested on that caller is delivered before any
item is removed; a previously caught cancellation with nonzero `cancelling()`
count does not poison later reads. There is no suspension or allocating result
construction between dequeue and handing the existing item to the caller.

The first `cancel()` during startup/production sets the stop token. Before the
driver's first step it does **not** call Task.cancel: the driver enters, observes
stop, skips source creation and settles with cancelled/0. Cancelling a native
Task before its first step would skip that coroutine's body and finally entirely.
After entry, an external owner may issue exactly one native cancellation request
with a private identity marker to interrupt a pending source await. Repeated
cancel/close calls never inject a second exception, even if cleanup awaits.

Self-cancel from the source only sets the cooperative stop bit; it does not queue
an exception that might first arrive in subsequent cleanup. A source which
self-cancels and then ignores the token or suppresses cancellation can remain
live. Native task cancellation cannot preempt synchronous non-yielding code.
Once production seals success/limit/cancel and enters the driver's explicit
aclose, further cancel calls return False without rewriting that result or
injecting into cleanup. A first cancellation may land in a finally that the
source entered inside a still-pending `anext`; Python does not expose that lexical
phase. The guarantee is one library request, not immunity of every source finally
to the first request.

Only an exact `CancelledError` carrying the owner's identity marker, or
`TaskCancelled` while its stop token is set, acknowledges owner cancellation.
An independent source `CancelledError` remains a source control even when a stop
bit is also set. This is cancellation classification for trusted code, not an
authentication boundary against code inspecting private state. Neither the
caller nor producer is `uncancel()`ed by this API.

`aclose()` itself explicitly requests stop and discards the mailbox. It first
checks pending caller cancellation before this mutation and allocates its
replacement deque before discarding references. After stop/discard commits,
timeout or cancellation of the **wait** cannot undo that request and sends no
additional cancellation into the producer or its finally. Discard flags are
coherent before borrowed-value destructors run; the API does not call value close
hooks. After discard, consuming readers see EOF immediately while completion can
still await cleanup. Repeated close publishes only the first discard transition.

At most 256 combined pending next/readiness/completion/close waits share the
existing bounded completion hub. Immediate observations require no lease. If
`aclose()` commits stop/discard but its wait is the 257th, `AsyncWaitLimitError`
means the wait was not admitted, not that the close request rolled back. The
owner stays available with closed=False. Retry close after timeout, caller
cancellation or waiter-budget failure. Closing a valid native owner does not
re-raise stored source failures; use completion/next to observe those separately.

`async with stream` calls `aclose()` without a timeout on exit. An ordinary body
failure cannot mask a new cleanup control; otherwise the existing primary-failure
priority applies. A previously delivered body cancellation is not mistaken for
a fresh cancellation request. A new interruption can leave closed=False and
the owner must be retained. Uncooperative cleanup can make unbounded context exit
wait forever; applications needing a bound should use explicit `aclose(timeout)`.

## Source failures and incomplete native cleanup

The same producer Task exclusively awaits its native generator's `aclose()` once
after production. It catches source and cleanup `BaseException`, including
KeyboardInterrupt/SystemExit/CancelledError, before a raw source control escapes
the Task. Each failed completion/terminal read raises a fresh `AsyncSourceError`
for an ordinary failure or `AsyncSourceControlError` (a BaseException) for a
control, with the unchanged stored failure as `__cause__`. Repeated observation
does not mutate that source traceback. Error causes may retain application data;
they are not sanitized or size-bounded wire documents.

**Task settlement is not always native generator closure.** An invalid generator
which yields during its GeneratorExit/finally cleanup can make `aclose()` fail
with `RuntimeError("async generator ignored GeneratorExit")` and leave `ag_frame`
non-None. A second native close can even raise StopAsyncIteration while that frame
still remains. This API does not retry, force-resume or claim closure based solely
on the close awaitable having returned/raised.

The owner retains that generator with done=True, cleanup_incomplete=True and
closed=False. `aclose` then raises a fresh `AsyncTaskStreamOwnershipError`, or
`AsyncTaskStreamOwnershipControlError` when the preserved failure is a control.
Both expose `.stream`, `.phase == "cleanup"` and the original cause. Repeated
close reports the same retained ownership with fresh wrappers and no further
generator advancement. There is intentionally no generic repair or force-close
operation for an illegally yielding generator. No live library Task remains once
done is true, but the retained handle/error owns that failed native frame.
GC/interpreter shutdown is not acknowledged cleanup. Conversely, a closed frame
proves only the native protocol state, not correctness of arbitrary external
resource cleanup performed by application code.

## Startup failures and publication boundaries

Owner bookkeeping, context, callbacks and the driver coroutine are prepared
before Task construction. A start-commit gate prevents source invocation before
successful lazy Task adoption and done-callback registration. If startup fails
after the owner exists, a fresh ownership error exposes `.stream` and
`.phase == "startup"`; retain `error.stream` and await its `aclose()` to acknowledge
the aborted start. Ordinary/control errors retain their separate categories.
Preflight failure before ownership creates no Task or source.

On the functioning standard loop, one FIFO abort callback lets an already
scheduled driver record itself and skip source creation, or closes a never-adopted
driver coroutine. Normal/abort settlement acknowledgement is idempotent. An
altered loop can schedule a Task and then throw, so direct Task construction is
not a universal atomicity theorem. Failure to schedule abort acknowledgement,
destroyed loops and hard OOM cannot promise settlement: ownership is retained,
never blindly closed or falsely acknowledged.

Accepted item/count state commits before consumer notification. A notification
failure cannot roll it back. Successful observations allocate before dequeue,
and any blocked capacity notification is resolved before consuming; standard
callbacks run later and the producer rechecks real credit. Terminal state commits
before terminal notification. Deterministic allocation-fault tests check these
prefix/ownership boundaries, but no notification system can promise progress
when its trusted runtime cannot allocate or schedule callbacks.

## Verification and remaining scope

The focused tests use event/future barriers, actual asyncio Tasks and counted
source advances for factory bypass, startup aborts, cancellation before entry,
single-request cancellation, awaited finally, failure wrappers, malformed cleanup,
mixed 256-waiter bounds, competing readers, loop affinity and allocation faults.
The missing-public-API RED is retained. Full repository, supported-platform,
installed-wheel and hosted acceptance must be reported separately after they run.

This implements local native async-generator execution with explicit ownership,
not Ray async actors, actor-method streams, distributed generator retries,
reference/GC reconstruction, cross-host execution or per-yield DAG scheduling.
Those remain part of the open [whole-repository objective](parity-runtime.md).
