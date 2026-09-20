# Local per-yield source-to-map edge

`start_stream_map(source, mapper, *, config=None)` connects one trusted native
synchronous generator to a bounded local thread-map pool. A mapped item can be
consumed before the source reaches EOF. This is a deliberately narrow execution
edge, not a general DAG-stream scheduler, a Ray `ObjectRef` stream, a cluster
protocol, or worker-failure recovery. See the runnable [offline example](../examples/stream_map.py).

```python
from graph_sail import StreamMapConfig, start_stream_map


def source(context):
    for value in range(3):
        context.cancellation.raise_if_cancelled()
        yield value


def mapper(context, value):
    return (context.sequence, value * 2)


with start_stream_map(source, mapper, config=StreamMapConfig(max_pending=2)) as edge:
    for item in edge:
        print(item.sequence, item.value)
    print(edge.completion().status)
```

The source must be a native generator function producing a fresh generator. The
mapper is a synchronous callable; awaitable results are rejected. Both run as
trusted local application code. The producer receives `TaskStreamContext` with
its cancellation signal. The mapper receives `StreamMapContext(sequence,
cancellation)` and a deserialized snapshot of one yield. Results are serialized
into independent snapshots before publication; neither a later source mutation
nor a later mapper mutation can silently change a published value. The existing
local snapshot profile applies, including its supported types and pickle
trust assumptions.

`max_pending` (default 8, cap 1,024) counts every accepted but unconsumed
source value: queued, running, or completed out of order. The coordinator
acquires this credit **before** calling `next(source)` and releases it only
when an ordered mapped result is consumed. There is no extra source peek or
prefetch. At the exact `max_yields` cap (default 100,000, cap 10 million),
the source is closed without attempting the next yield and completion reports
`limited`; clean EOF reports `succeeded`. `max_workers` (default 2, cap 64)
cannot exceed `max_pending`. Input/output snapshots each have a configurable
byte cap (both default 1 MiB, each cap 8 MiB), with a combined admitted
snapshot budget of at most 64 MiB. These are serialized-payload/admission
bounds, **not** hard limits on decoded heap, user code, CPU, threads it spawns,
or pickle hooks.

Mapper tasks may finish out of order, but the single consuming reader observes
source sequence order. A second concurrent reader gets an error instead of
duplicating or stealing a result. `next(timeout)` and `completion(timeout)`
use seconds; their timeout leaves the source and maps running. Completion may
require the caller to consume items first, because a full pending window stops
source advancement. One mapper/source/serialization failure retains and
publishes the successful ordered prefix, then raises the original failure;
there is no implicit retry, rollback, or exactly-once external-effect claim.

`cancel()` discards unconsumed results and requests cooperative stop. It does
not force-interrupt a mapper or a source blocked in application code. `close`
joins owned work; a timeout raises while retaining the same handle for a later
close retry, and `closed` remains false. The context manager closes the edge.
Source/mapper code is responsible for its own external effects and any workers
it creates. `done()` reports coordinator completion, not consumption or owner
closure. The result records `accepted` source items and `published` consumed
mapped items as distinct counts; cancellation can discard accepted work.

The prior task-stream, process-stream and actor-stream APIs remain separate.
This feature does not introduce a process/actor downstream mapper, streaming
fan-out, multiple consumers, durable replay, checkpointing or cross-host data
flow. These remain work toward—but are not claims of—whole frozen-reference
runtime parity.
