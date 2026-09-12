# Local process-backed task streams

`start_process_task_stream` retains one **native Python generator in one local
spawned process**. It reuses the thread task stream's one consuming mailbox,
sequence assignment, backpressure, yield cap, and prefix-before-failure rules.
The parent does not collect a generator into a list or reconstruct it per yield.

```python
from graph_sail import ProcessStreamConfig, TaskStreamConfig, start_process_task_stream


def squares(context, count):
    for index in range(count):
        context.cancellation.raise_if_cancelled()
        yield index * index


if __name__ == "__main__":
    with start_process_task_stream(
        squares,
        args=(5,),
        config=TaskStreamConfig(max_buffered=2, max_yields=5),
        process_config=ProcessStreamConfig(max_message_bytes=4096),
    ) as stream:
        print([item.value for item in stream])
        print(stream.completion())
```

Run code from an importable module behind the main guard. The executable
[`examples/process_task_streams.py`](../examples/process_task_streams.py) is
offline, has an independent square-number oracle, records the actual child PID,
and checks the fixture's own `finally` marker.

## Admission and data ownership

Only an importable native synchronous generator **function** is admitted. Nested
functions, callable instances, borrowed generators/iterators, coroutine functions,
and async-generator functions are rejected. Optional `args` and `kwargs` use the
actor argument contract; the worker calls `function(context, *args, **kwargs)`.
Its factory and two RPC method names are fixed internally, not supplied by data.

Arguments, yielded values and any pickle reduction/reconstruction hooks are
trusted local Python code. Full startup frames are admitted before OS handle
allocation and serialized again by the actor at the actual spawn boundary. Hooks
may therefore run more than once during admission. Arbitrary hooks, mutation
during serialization, constructor imports, CPU use, and external side effects
are **not sandboxed**. No untrusted pickle files, network transport, callbacks,
remote services, credentials, or model downloads are introduced.

Each accepted value is an independently serialized snapshot, not the thread
stream's borrowed live reference. For example, repeatedly yielding and mutating
one child list produces separate parent lists. There is no zero-copy claim.
Existing `ObjectRef` values can be passed normally; reading the bytes still
requires an explicit local object client and independently owned store lifetime.

## Advancement, caps, and failures

The mailbox must have a free slot **before** the driver submits one `advance`
RPC. At most one RPC is in flight. The child calls `next()` exactly once per
advance, binding each reply to its PID and expected sequence. Only a native
`StopIteration` produces the explicit EOF frame. The generator's return value is
not another item.

`max_yields` does not probe once more to discover EOF. If a producer would have
ended on its next call, reaching the cap still reports `limited`, not `succeeded`.
Successful completion states are `succeeded`, `limited`, and `cancelled`; the
`produced` count includes only yields accepted into the parent mailbox. A result
received after cancellation may be discarded without increasing that count.

Accepted buffered items remain consumable before a producer/transport/cleanup
failure. `completion()` observes the failure immediately after the driver has
settled; it does not require draining the buffer. Explicit `close()` discards
the queue. Ordinary remote exceptions are `ActorRemoteError`, unpicklable or
oversized results are `ActorSerializationError`, and worker death/control
exceptions are `ActorDiedError`. They are not reconstructed original exception
objects. Local control exceptions are not converted into ordinary success.
There are no retries, replay, or continuation after an ambiguous response.

## Cancellation and native cleanup

`next(timeout)`, `wait_ready(timeout)` and `completion(timeout)` are wait-only.
Their timeout does not stop shared work. `cancel()` requests stopping, preserves
the accepted queue, and closes an internal one-way sender endpoint. There are
no cancellation bytes to fill a pipe and no sender in the worker. A cooperative
producer polls `context.cancellation`; EOF is irrevocably latched. An ordinary
error or success racing with an already-observed stop cannot reverse it.

The worker's generator is advanced and closed on **the same worker thread**.
There is no guardian thread concurrently calling `generator.close()`. An idle
worker closes its suspended generator when its actor request channel reaches
EOF. A running cooperative generator observes the cancellation channel's EOF
after the communication owner dies. An uncooperative callback may require
termination by the still-live parent; an abruptly dead parent cannot enforce a
deadline or guarantee reaping an uncooperative orphan.

On normal completion, cap, cancellation, or failure, the driver first requests
generator closure and then settles the actor and closes its endpoint. If an
advance exceeds its budget, it signals cancellation, allows the configured
grace, and explicitly terminates/joins if necessary. A close RPC that fails or
exceeds its budget also escalates. No timeout kills and retries a generator.

`stream.worker` / `result.worker` are frozen observational snapshots:

| Field | Exact meaning |
| --- | --- |
| `pid`, `exitcode` | Actual spawned worker identity and observed exit code (unknown until observed). |
| `cancellation_requested` | The backend requested irreversible EOF cancellation. |
| `termination_requested` | The backend explicitly called `terminate`; actor `close` can independently escalate. |
| `generator_closed` | An exact close ACK reported the native generator in `GEN_CLOSED`. Without ACK it remains false, even if a marker or process exit suggests cleanup ran. |
| `resources_closed` | The parent endpoint and actor lifecycle close both completed successfully. |

A close ACK does **not** certify application side effects: closing a
never-started generator does not enter its body or `finally`. Forced termination
does not certify that `finally` ran. Snapshot constructors/serialization are not
authenticated proof of any process history.

`done()` means the driver settled, possibly with an error. `closed` additionally
requires joining the parent mailbox thread and confirmed native resource
closure. A failed or timed-out close retains the owner for `close()` retry;
merely joining a Python thread cannot set native `resources_closed`. Stored
producer/generator failures are read via `next`/`completion`; `close` does not
re-raise an already stored task error. Actual resource cleanup errors still
raise, and sticky actor/OS cleanup failure may require operator intervention;
retry is ownership retention, not a promise every OS failure is recoverable.

If startup fails after resource acquisition, the thrown original exception has
`process_stream_cleanup`. Retain that object and call its `close()` to retry
known-resource cleanup; inspect `closed` rather than assuming throwing meant
successful cleanup. The same capability is attached to a preserved
`KeyboardInterrupt` / `SystemExit`, including an initializer that started the
broker before throwing. Cleanup attempts all admitted endpoints and the actor,
without changing the ordinary public actor factory contract. This is not a
guarantee of recovery from arbitrary interpreter corruption or hard OOM.

## Bounds and timing boundaries

`ProcessStreamConfig` defaults to 1 MiB per serialized message, 30 seconds each
for startup and each advance, 1 second cancellation grace, and 5 seconds for
generator/actor shutdown. Per-message sizes admit 1 KiB–16 MiB; the product
`max_buffered * max_message_bytes` must not exceed 64 MiB. The existing actor
transport bounds the **complete pickle frame including its envelope**, in both
directions, before accepting it. It is not just a bound on a yielded byte string.

These bounds cover the configured buffered frame estimate plus separately
bounded transport/in-flight frames. They are not exact retained heap/RSS limits:
decoding trusted object graphs or hooks can allocate more, and the child callback
can allocate arbitrary local state before yielding. Byte limits do not authorize
loading hostile pickle.

Startup is synchronous and has a separate budget **before a handle is returned**;
it cannot be cancelled through a not-yet-existing stream. `close(timeout)` bounds
the wait for the local driver; its retry/native shutdown path uses separate
configured budgets. Callback imports, serialization/reconstruction, OS spawning,
pipe operations, termination, and joins are not hard real-time interruptible.
No hard total wall-clock/process-sandbox or descendant-process guarantee is made.

## Scope and verification

Both backends support [event-driven async consumption](async-streams.md).
This is still a local standalone task stream, not actor-method streaming, native
async-generator execution, per-yield DAG scheduling, cross-host object storage/GC, fault
reconstruction, retries, or a distributed Ray-compatible runtime. Frozen
reference capability comparison remains in [the parity matrix](parity-runtime.md).

Regression tests use independent sequence/color-free numeric expectations,
real child PIDs, generated local marker files, malformed bound reply frames,
strict configuration admission, per-yield copies, full-frame serialization
failure, accepted-prefix ordering, controlled deadline/cancellation races, and
retryable resource ownership. A two-spawn test lets the **communication owner**
actually `os._exit` while the test supervisor retains both process handles for
reaping; it is not merely a fake closed flag or an unreapable orphan experiment.

The final Windows Python **3.12.13** full suite passed **1140 tests**, with three
existing symlink-privilege skips, in **1134.50 seconds**. RuntimeWarning and
ResourceWarning were errors. The 142 new tests include 124 internal/protocol and
18 real-process cases, creating 19 actor workers plus two actual communication
owners. They are not 142 separate process experiments. Dedicated absolute
coverage paths captured parent and worker data together: combined coverage is
**97.7460%** (5051/5132 statements and 1584/1656 branches), preserving the original
95% gate. This new module covers all 308 statements and 82 branches; the reused
task-stream mailbox covers all 222 statements and 52 branches.

Development failures were retained, not relabeled as passing runs: two old-API
tests failed before implementation; later regressions exposed a cancellation
response race, missing startup cleanup capabilities, and an unowned local child
pipe on partial initialization. Those defects were fixed and independently
retested before this full run. A helper import spelling error, platform-specific
pipe type annotation, and two documentation code-block blank lines were also
corrected without changing production deadlines or coverage thresholds.

The wheel-only offline example and distribution checks are recorded in the
[parity matrix](parity-runtime.md). Local tests do not claim full repository
parity, a distributed speedup, or a passing hosted check that has not run yet.
