# Selective cancellation of one process-DAG node

`start_process_graph` returns a `ProcessExecutionHandle` with
`cancel_node(node_id) -> bool`. This requests cancellation of one admitted
node while unrelated branches continue when `fail_fast=False` (the default).
It is not available on thread execution handles or individual
`start_process_stream_graph` items.
Run the [offline example](../examples/process_node_cancellation.py) for a
spawned child PID, independent `6 * 7 = 42` result and joined-owner check.

```python
with start_process_graph(graph, registry, placements) as handle:
    requested = handle.cancel_node("decode")
    result = handle.result()
    actual = handle.node("decode").execution()
```

The boolean means the first request was admitted before terminal publication,
not that cancellation won a concurrent child result. Unknown node IDs raise
`ValidationError`; duplicate, already-terminal, globally stopped and
post-completion requests return `False`. Use `actual.status` and
`result.attempts`/`result.workers` for the observed outcome. A finished value,
application failure or external side effect can race an accepted request.
String-subclass IDs are copied to an exact built-in `str` before membership
checks or lock acquisition; their custom conversion, hash and equality hooks
are not invoked by this API. A non-string object with a spoofed `__class__`
is rejected with `ValidationError`.

A not-yet-dispatched node is marked `cancelled` without entering a child; its
transitive descendants are `skipped`. A node waiting between application
retry attempts retains earlier attempt evidence and is never retried after
the request. A running node's dedicated process cancellation channel is
closed irreversibly. The driver waits the configured cooperative grace,
retires/joins the worker and escalates for a noncooperating callback. An
independent branch may continue and may start a fresh worker after that
retirement; the old EOF signal is never reset or reused. With
`ExecutionConfig(fail_fast=True)`, the existing policy escalates a terminal
node cancellation to whole-run cancellation.

Logical resource and device slots remain occupied until a running attempt
and its child cleanup actually settle. A successful final result includes
joined-worker evidence. If OS cleanup fails, the handle retains the same
retryable `process_graph_cleanup` owner and `closed` remains false until a
later successful `close()`. A wait timeout does not abandon child ownership.
No automatic task replay, rollback, guaranteed child `finally`, hard RSS
limit or exactly-once external-effect claim is implied. Process-local node
IDs are not Ray ObjectRefs; this is not distributed or whole-Ray parity.
