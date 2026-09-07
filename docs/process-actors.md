# Local process actors

`ProcessActor` executes explicitly registered Python methods in a real, separate
local process. An actor's instance survives between calls; its mailbox executes
one method at a time, in admission order. Different actors can run concurrently.
This is separate from the thread-based DAG executor and from simulated planning.
There is no cluster scheduler, remote actor service or automatic state recovery.

Run the complete, executable example:

```bash
python examples/process_actor.py
```

## Registration and use

```python
from graph_sail import ActorDefinition, ActorRegistry, ProcessActor


class Counter:
    def __init__(self, value=0):
        self.value = value

    def add(self, amount):
        self.value += amount
        return self.value


if __name__ == "__main__":
    registry = ActorRegistry({"counter": ActorDefinition(Counter, ("add",))})
    with ProcessActor(registry, "counter", args=(10,)) as counter:
        first = counter.submit("add", args=(2,))
        second = counter.submit("add", kwargs={"amount": 5})
        assert first.result(timeout=5) == 12
        assert second.result(timeout=5) == 17
```

Use module-level importable factories/classes and protect process construction
with the main guard. A REPL/notebook-local class, lambda or nested function is not
supported. The factory, arguments and return objects must be compatible with
Python pickle protocol 5. This implementation explicitly selects the `spawn`
context on all supported platforms, without changing the application's global
start method. POSIX frozen executables are not supported. These restrictions
follow Python's [spawn and process programming rules](https://docs.python.org/3/library/multiprocessing.html#programming-guidelines).

`ActorRegistry` snapshots its input mapping. Each `ActorDefinition` requires an
explicit tuple of allowed public method names. Private/dunder, dotted import
paths and unspecified methods are rejected before dispatch. The factory runs in
the child; all registered bound methods are resolved and checked before startup
is acknowledged. Factories and methods must be synchronous; awaitable results
are rejected, not silently scheduled. There is no document/CLI method loader.

## Handles, order and state

`submit(method, *, args=(), kwargs=None)` serializes an argument snapshot and
returns an `ActorCall`. Admission is thread-safe. Calls from simultaneous parent
threads execute in their assigned `request_id` order, not a promised thread
priority order. A `submit` call can spend time serializing trusted Python objects;
it does not wait for the remote method to finish.

Same-actor submission or lifecycle operations reentered by an argument's pickle
hook are rejected. This prevents request-ID reuse, orphaned handles and closing
the actor halfway through admission. Lifecycle operations from transport hooks
are also rejected; hooks must not synchronously wait for work on that same actor.

Handles expose `result(timeout=None)`, `exception(timeout=None)`, `done()`,
`running()`, `cancelled()` and `cancel()`. No user callbacks run on the mailbox
thread. These are not `concurrent.futures.Future` objects and are not accepted by
`concurrent.futures.wait`. `elapsed_seconds` is set once a response is received;
it measures actual parent dispatch-to-response elapsed time, including transport
and serialization, not just method CPU time or queue wait. It is `None` for
cancelled, lost or still-pending requests.

`cancel()` succeeds only before dispatch. A cancelled queued call consumes its
mailbox slot until the broker reaches and discards it. A running call cannot be
cancelled independently without losing the actor. Completed calls leave the
actor's pending map automatically, even if the caller has not retrieved them.
Retaining many completed handles/results is the caller's memory responsibility.

Ordinary method exceptions become `ActorRemoteError` containing a bounded
phase, exception type name and message, not the remote exception instance or a
traceback. Later calls continue. A failed method may already have changed actor
state or external files; there is no rollback. An unpicklable/oversized result
produces `ActorSerializationError` while the actor remains usable. Its method
already executed, so no automatic retry is performed.

Remote exception names/messages are truncated by UTF-8 byte size so even
Unicode-heavy diagnostics fit the minimum 1 KiB message allowance.

Arguments/results cross a serialization boundary. Normal mutable collections
are copied, rather than aliasing the parent objects. Custom pickle reducers can
deliberately reconstruct external references or perform other operations; do not
interpret this boundary as universal deep-copy or immutability enforcement.

## Timeouts, failure and ownership

| Operation or event | Effect |
| --- | --- |
| `handle.result(timeout)` / `exception(timeout)` expires | Raises standard `TimeoutError` only in the waiter; the accepted method continues |
| `ActorConfig.startup_timeout_seconds` expires | Startup handshake fails with `ActorTimeoutError`; worker is terminated and joined |
| Optional `ActorConfig.method_timeout_seconds` expires | Dispatch-to-response budget fails the running and pending calls with `ActorTimeoutError`; actor is stopped, not restarted |
| Ordinary method exception | Bounded `ActorRemoteError`; later calls continue with whatever state remains |
| Worker crash, `SystemExit`, broken pipe or protocol failure | Running/pending handles fail with `ActorDiedError`; broker closes and joins the worker |
| `close(timeout=5)` | Stops admission, drains accepted work within the wait budget, then abandons unfinished calls and terminates if needed |
| `terminate()` | Stops admission and abandons running/queued work immediately; terminates and joins the worker |

Use the context manager or explicit `close()` in `finally`. Both close and
terminate are idempotent. `pending_count`, `alive`, `pid`, `exitcode` and `failure`
expose the local lifecycle; completed history is not retained. An idle worker
crash is checked even when no calls are pending. Accepted work abandoned by
explicit shutdown raises `ActorClosedError`. New calls to a closed actor raise
that error synchronously.

`alive` observes the process's exit signal without reaping its exit status, so
concurrent status readers do not compete with the broker's process cleanup.
`exitcode` becomes available when the broker records it; `alive == False` alone
does not mean the broker has finished settling pending calls. Use `close()` to
wait for that cleanup.

The [actor capacity protocol](actor-capacity.md) measures actual worker startup,
processing and shutdown separately and checks every trial against a closed-form
oracle. It includes local observations, not distributed performance guarantees.

OS failures during terminate, kill, join, pipe close or process-handle close are
collected while best-effort cleanup continues. Public close/terminate then raises
`ActorDiedError`; `failure` records that cleanup error. A worker the OS refused
to stop may still report `alive == True` and needs external cleanup. The runtime
does not report successful closure in that case. An unreadable process state is
not treated as evidence that the worker stopped.

Timeout budgets are not operating-system real-time guarantees. Startup timing
begins after `Process.start()` returns. Method timing includes response transport
but excludes parent request serialization. The broker checks waits at 20 ms
intervals; parent deserialization can run trusted Python code. Abort/deadline
checks run again after receiving bytes and after deserialization returns, so a
late reconstructed result is not accepted as success. A control exception raised
by a reconstruction hook becomes terminal transport failure, not normal closure.
Cleanup allows a
five-second join for a worker finishing after EOF or graceful shutdown, then
terminate/join and kill/join waits of up to one second each. Forced shutdown
skips the initial join. Graceful shutdown also allows a 0.5-second worker exit
wait. Close may therefore outlast its drain budget. Arbitrary serialization code, OS stalls
or application interpreter finalizers cannot be given a universal deadline.

The library neither retries lost methods nor reconstructs state. If a worker
dies after a side effect but before its response arrives, the result is unknown.
The owner must decide whether creating a fresh actor and repeating the operation
is safe. Forced termination does not run application cleanup reliably and can
leave partial external effects. Actors are daemon multiprocessing children;
creating nested multiprocessing children is unsupported. User-created external
subprocesses/resources are not supervised or cleaned up by this runtime.

## Resource and security boundaries

`ActorConfig` defaults to 32 pending calls, 1 MiB per serialized message, a
30-second startup handshake budget and no method budget. Its validated bounds:

- Pending calls: 1–1,024, including the dispatched call.
- Message bytes: 1 KiB–16 MiB; pending × message limit must not exceed 64 MiB.
- Factory/method positional or keyword arguments: at most 256 each.
- Registry entries: at most 256; allowed methods: 1–64 unique public names.
- Names: ASCII identifiers, at most 128 characters; keyword keys are bounded
  strings. Boolean, non-finite, negative or over-one-day timeout values fail.

The byte check bounds the serialized buffer while it is written, and receive
also imposes the configured limit. It is not a bound on the original object,
pickle-internal temporary objects, decoded object memory, CPU, process count
across caller-created actors or completed results retained by callers. The
runtime does not enforce RAM limits, CPU affinity, GPU allocation, network or
filesystem isolation. There is one broker thread and one child process per
actor; creating many actors is an explicit caller capacity decision.

Only trusted same-user Python code and objects are supported. Pickle can execute
code during serialization/deserialization; it is used only over a private pipe
created for that registered local worker. Do not accept untrusted serialized
bytes, factories, reducers or dependencies. Process isolation is not a security
sandbox, and an allowlist is not a defense against a malicious registered method.

## Verification and remaining scope

`tests/test_actors.py` starts actual Windows-compatible spawn workers. Tests use
file-based release barriers to verify queue order, independent-process overlap,
no next-call launch after termination, persistent state and distinct PIDs. They
also cover preflight rejection, pending limits, serialized snapshots, ordinary
exceptions followed by successful calls, unreplayable result serialization
errors, abrupt process death, constructor failure, cancellation, timeouts and
child joining. Protocol parsers have malformed-message regression cases.

Coverage uses the standard [multiprocessing instrumentation](https://coverage.readthedocs.io/en/latest/subprocess.html#multiprocessing)
and combines child data before applying the unchanged repository gate. Abruptly
killed child processes may not flush coverage; clean actor tests cover normal
worker paths. No new runtime or development dependency is required.

Actors are not yet integrated into DAG placement, remote result references or
resource admission. Async/threaded actors, actor-to-actor transport, named/shared
actors across owners, checkpointing, recovery, cluster deployment, shared-memory
object storage and distributed benchmark evidence remain open. See the
[whole-repository audit](parity-runtime.md); this increment is not Ray parity.
