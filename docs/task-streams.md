# Bounded local task streams

Run `python examples/task_stream.py` for an offline event-synchronized example.
Its first item is read while the remaining computation is still waiting for an
explicit release; no timing-based sleep is used to fake concurrent progress.

`start_task_stream` runs a registered native synchronous generator function on an
owned non-daemon producer thread. Each accepted yield is available before the
generator finishes. This is a separate API: ordinary DAG task return values,
their retries, serialization and wire formats are unchanged.

## Contract

- A stream has one ordered, consuming mailbox shared by its readers, not a
  broadcast subscription. Each item has a zero-based sequence and a borrowed
  Python value. No value is copied, serialized or implicitly closed.
- `max_buffered` bounds queued items. The producer waits for a free slot **before**
  calling `next()` on the generator. It does not eagerly materialize the stream.
  Callback frames, yielded values and consumer-held objects are not an RSS limit.
- `max_yields` bounds accepted yields. Reaching it closes the generator with
  status `limited`, without an extra pull to guess whether it would have ended.
  Only observed `StopIteration` means `succeeded`; return values are discarded.
- `next(timeout=...)` consumes a queued item, raises a wait-only `TimeoutError`,
  or observes termination. Already accepted items precede a stored producer
  failure. Failed streams re-raise the original exception after their queue is
  consumed; successful, limited and cancelled streams end with `StopIteration`.
  Every failed read restores the original producer traceback instead of retaining
  earlier readers' frames. The exception itself remains a trusted shared Python
  object, not a sanitized or independently copied transport envelope.
- `wait_ready()` observes an item or settled termination without consuming it.
  `completion()` waits for generator cleanup, but does **not** drain the queue;
  a backpressured stream can require consumption or cancellation first.
- `cancel()` requests cooperative stopping without discarding accepted items.
  `close()` discards queued items, requests stop and joins. A timeout retains
  ownership for a later close. Completion is certified by the producer's final
  signal, not solely by `Thread.is_alive()` after an interrupted native join.
- Generator `finally` code runs on its producer thread. Failures there are not
  hidden: ordinary cleanup failures do not replace earlier control exceptions;
  a new cleanup control exception takes priority over an ordinary failure.

Use a context manager or explicitly close every stream. There is no destructor
or daemon-thread substitute. Callbacks are trusted Python code and can ignore
cancellation or block in `next()`/`close()`; this API cannot force-stop them.
Timeouts bound coordination waits, not arbitrary destructors or callback code.
If a generator violates Python's close protocol (for example, yielding again
while handling `GeneratorExit`), cleanup is reported as failed; settlement is
not a claim that arbitrary callback-owned resources were successfully released.
The producer must not wait on or close its own stream. Native async generators,
custom/borrowed iterators, actor methods, dynamic DAG yield
dependencies, distributed references, replay/retries and network delivery are
not implemented by this interface. A [process-backed wrapper](process-task-streams.md)
reuses this mailbox; both backends support [event-driven async consumption](async-streams.md)
without changing native synchronous-generator or explicit-close contracts.
