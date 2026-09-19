# Stateful actor-method streams

An opt-in `ProcessActor` can execute a native synchronous generator method on
its existing instance and child thread. State persists across yields, native
generator cleanup, later streams and ordinary calls. This is real local actor
execution, not a new stateless process per stream. See the executable
[offline example](../examples/actor_method_stream.py).

Register ordinary and generator methods separately:

```python
definition = ActorDefinition(Counter, ("add", "state"), stream_methods=("totals",))
registry = ActorRegistry({"counter": definition})
with ProcessActor(registry, "counter") as actor:
    actor.submit("add", args=(2,)).result()
    with actor.stream("totals", args=((3, 5),)) as stream:
        for item in stream:
            print(item.sequence, item.value)
    print(actor.submit("state").result())
```

The bound generator signature is `method(self, context, *args, **kwargs)`, with
the existing read-only TaskStreamContext. Its cancellation signal supports
`cancelled` and `raise_if_cancelled()`. A stream method must be a native Python
bound synchronous generator on that exact instance. Custom iterators, wrapper
factories, async generators, coroutines, staticmethods and classmethods are not
admitted. Stream and ordinary allowlists are disjoint, each bounded to 64 names;
the ordinary allowlist remains nonempty. Generator return values are ignored.
There is no send/throw channel, automatic replay, restart or rollback.

## Exclusive admission and bounded production

`actor.stream(method, *, args=(), kwargs=None, config=None, stream_config=None)`
requires an idle actor: no queued/running/unsettled ordinary request and no
existing stream lease. Otherwise it raises ActorStreamBusyError immediately.
Ordinary submit and a second stream also reject while leased. No ordinary
method interleaves between yields, and no hidden request queue waits behind a
stream. Existing FIFO ordinary-call behavior is unchanged outside the lease.

The existing TaskStreamConfig defaults to 16 buffered items and 100,000 yields,
with maxima 1,024 and 10,000,000. The driver waits for buffer capacity **before**
advancing the child generator once. At the exact yield cap it closes without an
extra peek, reporting `limited` even if the next step would have reached EOF.
One complete serialized yield is an independent trusted snapshot; later actor
mutations cannot change already buffered values. This is a consuming mailbox,
not broadcasting or distributed ObjectRefs.

ActorConfig.max_message_bytes remains authoritative: default 1 MiB, admitted
1 KiB..16 MiB. Complete startup/request/yield/error/finish envelopes must fit.
The additional `max_buffered * max_message_bytes` cap is 64 MiB. Ordinary queue
and argument limits stay unchanged. These are serialized-frame/work limits,
not a bound on decoded Python heap, pickle hooks, generator state, retained
caller values, CPU, or all OS/native resources.

ActorStreamConfig defaults (seconds): open 30, advance 30, finish 5,
cancellation grace 1, shutdown wait 5. Operation timeouts must be finite in
0.001..86400; grace/shutdown accept zero. The actor's method timeout wins if
shorter. Finish has its own positive RPC deadline, not the shutdown wait budget.
Timeouts and owner-close waits do not hard-interrupt trusted pickle hooks,
OS spawn/send/join or arbitrary user code.

`pending_count` continues to count unsettled broker requests, including at most
one private stream RPC. A lease can exist while pending_count is zero. After
clean lease release, ordinary calls can resume while a caller drains old buffer
items. A subsequent stream may remain briefly busy until the previous driver
has exited and been joined; explicitly closing the previous handle joins it.
An old handle's later close/cancel never touches a newer actor lease.

## Completion, failures, cancellation and ownership

Clean EOF reports `succeeded`; exact cap reports `limited`. Ordinary producer
or result-serialization failures preserve the accepted prefix, then raise.
Native mutations before a failed serialization still happened exactly once.
The private sequence advances only after an entire bounded yield frame packs;
failed serialization reports the unchanged sequence, allowing a matching close
acknowledgement without replay. Lost transport/ACK is not recoverable certainty.

Actor reuse requires a matching native-generator closure acknowledgement and
a healthy protocol/worker at lease release. This includes ordinary source and
yield-serialization failures with successful native cleanup. Generator close
failure, illegal yield after GeneratorExit, timeout, corrupt reply, child death
or lost ACK poisons the actor and requests retirement. Child SystemExit and
KeyboardInterrupt are worker death, not pickled parent controls.

`cancel()` requests cooperative stopping and preserves accepted buffered items.
Before positively undispatched open, cancellation can release the actor without
running the generator body. After dispatch, EOF cancellation is irreversible:
even a cooperatively closed generator retires the **whole actor**, losing its
in-memory state. A noncooperating method is force-stopped after grace/deadline.
No repeated cancellation exception is injected into finally, and finally is
not guaranteed under forced process death. Reusable post-cancel actors need a
different generation-aware protocol and remain out of scope.

`close(timeout)` discards buffered values and joins the producer, with retained
ownership if its wait times out. Stored source errors are observed by next or
completion, not by close; unresolved cleanup can itself raise. A genuine parent
control keeps its identity and cleanup priority. A failure after stream-driver
adoption carries `error.actor_stream_cleanup`, the same retained handle with
`close()`/`closed`. Failed opt-in actor bootstrap can carry a retained startup
cleanup capability under the same attribute. No destructor cleanup is promised.
The actor also retains the unresolved lease owner; actor.close/terminate remain
recovery entry points. Exception attribute/OOM guarantees are limited to trusted
normal runtime operation, not arbitrary hostile hooks.

Driver start and actor shutdown share admission coordination. If shutdown wins
before the admitted driver starts, stream() raises ActorClosedError with that
same cleanup owner; it never resurrects a closed mailbox. If start wins, close
waits for the actual driver to exit. Failed-start cancellation and joins happen
outside the actor admission lock, including a real thread start that then raises.
Startup-failure cleanup uses shutdown_timeout_seconds for each of at most two
join attempts (private mailbox then retained-handle cleanup); these are separate
finite waits, not one shared absolute deadline. A failed cancel cannot skip the
independent stop/discard/join attempt or replace an earlier control. Unresolved
cleanup returns the same owner for explicit retry, without claiming closed.

`actor.close(timeout)` preserves ordinary draining when no lease is active; an
active lease is stopped without requiring the consumer to drain its buffer.
`actor.terminate()` aborts immediately. The existing broker remains the sole
pipe reader/writer and process reaper. Cleanup retry never competes with a live
broker. Only the library's direct actor child, pipes, broker and mailbox driver
are owned: user-created child processes, threads and external effects are not
transitively owned or undone.

An opt-in broker stop-notification failure cannot skip pending-call settlement
or native cleanup. Unexpected notification failures become typed transport loss
with the exact original exception/control as cause. A secondary ordinary failure
does not replace an existing failure; a new control keeps cleanup priority.
Parent cancellation endpoints serialize their native close/check under separate
per-endpoint locks shared by cancellation, broker cleanup and startup retry.
Wrappers and their locks exist before native pipe acquisition; the startup owner
retains the raw pair until both bindings succeed. Only the original receiver is
passed to the child, so these parent-only locks do not change the wire profile.

`stream.state` is an immutable ActorStreamState: actor_pid/stream_id,
generator_opened/generator_closed, lease_released, actor_reusable_at_release,
retirement_requested and actor_resources_closed. These are distinct facts.
Force-killing the worker can close OS resources without proving generator
closure. `done()` means terminal publication (including failure); `closed`
requires a joined producer and settled lease. A clean reusable stream does not
close its actor. Reusable-at-release is historical, not a guarantee that another
caller has not closed that actor since. Completion returns ActorStreamResult
with status, produced and state; failures raise instead of inventing success.

## Async consumers and trust

Iteration, next/wait_ready/completion and their explicit async counterparts
share the existing mailbox. Different stdlib event loops and synchronous
readers compete; no fairness/broadcast guarantee. Waiter cancellation/timeouts
do not cancel the producer. Existing 256 waiter/loop caps, pre-dequeue
cancellation checkpoint and allocation-before-dequeue rule remain unchanged.
Async failures use fresh AsyncSourceError/AsyncSourceControlError wrappers with
the unmodified shared cause. There is no async actor producer or async owner
close/context API in this slice. Do not block a responsive event loop with
synchronous actor creation/owner close.

Factories, methods and pickle hooks are trusted local code, not a sandbox or
network protocol. Remote type/message diagnostics are bounded but may contain
user data; PID/method/stream identifiers are explicit metadata. Legacy actors
with no stream registration keep their ordinary four-field startup/request
wire and allocate no cancellation pipe. The opt-in tagged profile does not
make Ray-compatible actor handles or change existing DAG/task/stream engines.
Cross-host ownership, actor concurrency groups, durable recovery, per-yield DAG
edges, distributed GC and the larger frozen-reference repository remain open.

The focused source run passed 529 tests without skips. The final Windows
whole-suite run passed 1,501 tests with three genuine host symlink-privilege
skips and 97.4660% combined coverage. An earlier wheel/source-distribution byte
audit and strict metadata check passed before the final documentation revision;
current-tree packages must be rebuilt and re-audited. Installed-wheel execution
and hosted exact-head checks remain separate obligations until their own results
exist; none of these local gates establishes distributed Ray parity.
