# Whole-repository reference audit: execution runtime

Audit date: 2026-09-07. Fixed reference:
[Ray `317c2888eade3c294c4fdb46eff9d9ec290b08f2`](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2).
The shared goal freezes 10,391 files in the complete recursive tree, including
5,981 files with code extensions totaling 59,579,026 bytes. Those inclusive
counts include tests/tooling and do not assert production LOC equivalence.

The target remains the entire reference repository. These increments add actual
local task execution and local process actors; they do not establish Ray parity.
The algorithms and API implementation here were authored independently.

| Reference subsystem and pinned source | Current Graph Sail evidence | Remaining contracts |
| --- | --- | --- |
| [Tasks](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/tasks.rst): asynchronous process workers, dependencies, result retrieval, wait/cancel, task events | Trusted local callable registry, real DAG dependency results, bounded concurrent threads and measured attempt events | Remote worker processes/nodes, asynchronous result handles, partial-result retrieval API, nested tasks, generators, multiple returns and dashboard event delivery |
| [Resource scheduling](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/scheduling/resources.rst): logical CPU/GPU/custom resources and capacities | Per-device logical slots, global thread limit, persistent-memory admission and node compatibility checks | Fractional/custom task resources, locality-aware placement, GPU visibility control, placement groups, cluster capacity and autoscaling; local slots do not acquire hardware |
| [Task fault tolerance](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/fault_tolerance/tasks.rst): application exception policies, worker failure retries, cancellation and object reconstruction | Application retries bounded per task, exception filtering, full attempt history, branch failure propagation, cooperative cancellation and run-budget handling | Worker-process crash detection/replacement, machine failure recovery, lost-object reconstruction, durable lineage and process termination policies |
| [Actors](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/actors.rst): stateful remote workers, handles, methods and concurrency models | Actual local spawn-process actors, explicit trusted factories/method allowlists, FIFO mailboxes, bounded pending calls, asynchronous handles, serialized objects, crash detection and joined shutdown | Multi-node actor placement, named/shared actor ownership, actor-to-actor transport, async/threaded actor policy, checkpointed recovery and integration with DAG resource admission |
| [Objects](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/objects.rst): distributed object-reference semantics | Thread DAG outputs remain explicitly shared; process actors use bounded private pipes; an actual context-owned file-backed byte store adds strict small references, verified independent readers and explicit release/capacity accounting | Distributed ownership/reference counting, shared-memory transport, implicit dereferencing, arbitrary object serialization, spilling and reconstruction |
| [Core](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/src/ray) and [Python runtime](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/python/ray): scheduler/control plane/runtime environments | Static graph validators/planners, local thread execution and single-owner process actors | Multi-node scheduling/control services, isolated environment/dependency management, distributed communication, observability/security/deployment and language bindings |
| [Ray Data](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/python/ray/data) | No dataset processing runtime | Distributed datasets, readers/writers, transformations, streaming execution and training ingestion |
| [Train](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/python/ray/train), [Tune](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/python/ray/tune), [Serve](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/python/ray/serve), [RLlib](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/rllib) | Planning/calibration can describe component estimates, but these services are absent | Training, tuning/search, online serving/batching, RL algorithms and their connectors, fault tolerance, API/CLI and benchmark surfaces |
| [Release workloads](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/release) and [documentation](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc) | Local regression CI, pure-planner benchmarks and an executable runtime example | Comparable execution throughput/latency/resource workloads, distributed failure/stress tests, subsystem integration matrices and remaining full API inventory |

Reference task/resource/fault-tolerance/actor documentation was inspected at the
pinned commit. Other subsystem directory links identify open inventory and
implementation work, not an assertion that every internal contract has already
been audited.

## Tests for this local execution increment

`tests/test_execution.py` uses an actual two-party thread barrier to prove that
independent callables overlap, then checks that the join computes 6 × 7 = 42 from
their returned values. Event-controlled tasks verify the global/device limits,
cooperative cancellation, blocked descendants and waiting for a noncooperating
task after its budget expires. Tests also cover branch-local failures, fail-fast,
retry exhaustion/filtering/delay yielding, preflight errors before side effects,
immutable registry bindings, explicit shared-output aliasing, async rejection,
exception diagnostics, and process-control exception propagation. These are
correctness tests, not distributed throughput evidence.

The cancellation tests detected and fixed a concrete ordering race: a task could
observe an external event and finish before the scheduler recorded the request.
The scheduler now records pending cancellation immediately after its completion
wait and before propagating the finished task's state.

Detailed API semantics, work limits and the cooperative time-budget boundary are
documented in [Local execution](local-execution.md). Entire-reference parity
remains open until the missing subsystems, contracts and scale evidence are
implemented and verified.

Local verification for this increment (Windows, Python 3.12.13): 381 repository
tests passed, including 47 focused execution tests. Branch-aware repository
coverage was 96.68% against the 95% gate; the execution module reached 99%.
Ruff lint/format, strict Mypy, Bandit, wheel/sdist build and Twine checks passed.
These are local results; no remote CI or distributed benchmark result is implied.

## Local process actor increment

`src/graph_sail/actors.py` and [process actor contracts](process-actors.md) add a
real local stateful worker process with an explicit trusted callable registry.
Windows-compatible spawn tests check persistent values, distinct PIDs, independent
actor progress during a blocked method, FIFO admission, bounded pending requests,
copied argument snapshots, cancellation before dispatch, ordinary exceptions with
continued state, serialization failures after execution, process death, timeout
and drain/terminate/join behavior. Private-protocol and adversarial trusted-hook
tests cover malformed messages, reentrant serialization and cancellation racing
with failure settlement. Concurrent lifecycle observations use a non-reaping
process signal so status readers cannot consume the broker's POSIX exit status.

This is not a process-backed DAG task scheduler. There is no automatic state
recovery, cluster placement, distributed object reference protocol or actor
resource admission. API breadth and repository-scale parity remain open.

Local verification for this actor increment (Windows, Python 3.12.13): all 477
repository tests passed, including 96 actor tests. Branch-aware coverage was
96.73% against the unchanged 95% repository gate; the actor module reached
97.08%. Ruff lint/format, strict Mypy, Bandit, the executable actor example,
wheel/sdist build, Twine and wheel-content checks passed. These are local
correctness/build results, not remote CI or distributed performance evidence.
The full 477-test suite also passed on WSL Ubuntu with Python 3.12.3; that Linux
run did not collect coverage.

The subsequent [actor capacity protocol](actor-capacity.md) measures real local
worker calls and separates startup/processing/shutdown, with independent
closed-form output and PID/cleanup checks. Two workloads retain every observed
trial; small-request process overhead and end-to-end startup costs are explicit.
This supplies local runtime evidence, not reference-comparable distributed,
memory, GPU, failure-rate or network-transport performance; those remain open.

## Local immutable byte object increment

`src/graph_sail/objects.py` and [local object contracts](local-object-storage.md)
add an actual local store, not just reference metadata. Real files bind each
store/object identity, byte count and SHA-256 commitment. An owner controls
bounded reservations and lifetime; read-only clients independently verify bytes.
Tests send a small reference to a spawned actor reading 2 MiB under a 4 KiB
message cap, with independent byte-count/sum checks and an explicit rejection
of directly submitting the large bytes. Additional tests cover fresh-process
JSON transport, DAG dependencies, malformed/corrupt inputs, concurrency, short
writes, failed publication/cleanup, observed filesystem changes and preservation
of unrelated files. No claim of lower latency or zero-copy follows from this.

The reference object documentation was read at the frozen commit. The local
backend deliberately leaves distributed object ownership/GC, shared memory,
spilling, fault reconstruction and cross-node transport open; it also does not
turn the thread DAG executor into a remote process scheduler. This increment
does not establish whole-repository parity.

Local object-storage verification (Windows 11, Python 3.12.13): the full
repository run passed 600 tests with three real-symlink tests skipped because
the host could not create symlinks. Branch-aware coverage was 97.03%, above the
unchanged 95% gate. Subsequent exact-type/derived-counter constructor hardening
was verified by the final focused object suite: 116 passed, the same three
host-privilege skips, and 100% statement/branch coverage of `objects.py`.
Controlled nonsymlink-mode rejection tests still ran on this host. Ruff
lint/format, strict Mypy, Bandit, lock consistency, the executable offline
example, wheel/sdist build, Twine and wheel-content checks passed. The full
repository coverage figure precedes those final constructor guards; it is not
a claim that the entire Windows repository suite was repeated after them.
A subsequent run of the final source on Ubuntu 24.04.1 LTS / Python 3.12.3,
using frozen hash-checked dependencies in a temporary virtual environment,
passed all 607 tests with zero skips, including all real-symlink cases. This
Linux run did not collect coverage. Remote CI and distributed performance
remain separate evidence.
