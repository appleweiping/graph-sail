# Whole-repository reference audit: execution runtime

Audit date: 2026-09-07. Fixed reference:
[Ray `317c2888eade3c294c4fdb46eff9d9ec290b08f2`](https://github.com/ray-project/ray/tree/317c2888eade3c294c4fdb46eff9d9ec290b08f2).
The shared goal freezes 10,391 files in the complete recursive tree, including
5,981 files with code extensions totaling 59,579,026 bytes. Those inclusive
counts include tests/tooling and do not assert production LOC equivalence.

The target remains the entire reference repository. These increments add actual
local task execution, local process actors and process-backed DAG invocation;
they do not establish Ray parity.
The algorithms and API implementation here were authored independently.

The process-DAG follow-up retains this exact frozen reference rather than moving
the target. Public task/cancellation contracts were rechecked for that lane;
standard-library spawn and pipe lifecycle documentation informed its OS boundary.

| Reference subsystem and pinned source | Current Graph Sail evidence | Remaining contracts |
| --- | --- | --- |
| [Tasks](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/tasks.rst): asynchronous process workers, dependencies, result retrieval, wait/cancel, task events | Trusted local registry, one shared DAG scheduler with real thread or spawned-process invocation, dependency snapshots/object refs, bounded logical slots and measured attempt events | Cross-node workers, public asynchronous task result handles, partial-result retrieval, nested tasks, generators, multiple returns and dashboard event delivery |
| [Resource scheduling](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/scheduling/resources.rst): logical CPU/GPU/custom resources and capacities | Per-device logical slots, global thread limit, persistent-memory admission and node compatibility checks | Fractional/custom task resources, locality-aware placement, GPU visibility control, placement groups, cluster capacity and autoscaling; local slots do not acquire hardware |
| [Task fault tolerance](https://github.com/ray-project/ray/blob/317c2888eade3c294c4fdb46eff9d9ec290b08f2/doc/source/ray-core/fault_tolerance/tasks.rst): application exception policies, worker failure retries, cancellation and object reconstruction | Bounded application retries with original child exception filtering; local crash detection, joined replacement for independent work, irreversible cooperative EOF cancellation and noncooperative termination | Automatic crash replay with explicit side-effect policy, machine failure recovery, lost-object reconstruction, durable lineage and distributed cancellation |
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

The actor-only increment did not connect process workers to DAG scheduling; the
later process-task increment below adds that link. Automatic actor state
recovery, cluster placement, distributed object ownership and general actor
resource admission remain open, along with API breadth and repository parity.

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
by itself turn the thread DAG executor into a process scheduler. This increment
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

## Local process-backed DAG increment

`src/graph_sail/process_execution.py` connects actual spawn workers to the same
`_Runner` used by thread execution. Dependency readiness, logical resource
admission, application retries and branch failure propagation are not duplicated.
The private actor bootstrap passes only a receive-side native cancellation handle;
the parent closes its sole writer to irrevocably signal EOF. Cancelled or broken
workers are joined before replacement, and infrastructure failures never trigger
automatic replay of potentially side-effecting work.

The [process execution contract](process-execution.md) distinguishes child
invocation, driver failure, transport and whole-lifecycle timing. A real two-way
task rendezvous proves overlap before computing 6 × 7 = 42. Further process tests
cover dependency copy isolation, exception-class retry filtering in the child,
worker crash/replacement, explicit 2 MiB object refs under a 4 KiB message cap,
external/global/fail-fast cancellation, translated late success, concurrent stop
races, sender disappearance, startup cancellation and attempted cleanup of all
owned resources even when a close raises a control exception. The executable
offline example exercises independent object readers and a downstream oracle.

This closes a local process-DAG integration gap, not the whole Ray task/runtime
contract. Cross-node workers, public asynchronous task handles, partial-result
retrieval, nested tasks, streaming generators, distributed resource scheduling,
durable recovery/lineage and automatic crash-replay policy remain open. No new
execution speed or distributed scale claim follows from these correctness tests.

The initial combined coverage diagnostic failed: 13 failed / 188 passed in
1,720.28 seconds. It included a test-harness bug where a marker timeout did not
set cancellation events; four precisely identified test-owned children were
terminated to release that failed run and obtain its report. That run is not
successful verification and is excluded from final coverage evidence. Other
failures involved coverage/startup latency, an invalid assumption that a task
declared cooperative must finish inside a 150 ms grace regardless of OS delay,
and a real new-config mismatch with the actor's 1 ms startup minimum.

The harness now cancels in `finally`; lightweight spawn-callable fixtures no
longer import pytest inside the measured task request. Grace tests distinguish
timely cooperation from permitted forced retirement, with a controlled timely
response test and independent native running-task EOF observation. The new
configuration rejects values below the existing actor minimum before work
admission. Production startup/attempt/cancellation budgets were not relaxed.
A frozen wheel's actor source was verified identical to published `ad81c02`;
its unchanged FIFO/drain coverage test passed with a 20.40-second call duration.
The subsequent quiet process-only run passed 60 cases in 157.74 seconds with
only the then-unfixed startup minimum test failing. After fixing that minimum,
16 configuration/admission/grace/control-cleanup cases passed in 2.53 seconds.
These targeted results are not a final full-repository gate.

A fresh Windows / Python 3.12.13 full-coverage attempt used a separate data
prefix and the frozen final source. It reached roughly 10% with failure markers
while even small unrelated shell checks were markedly delayed. The run was
interrupted once; the execution tool exited without a normal pytest failure
summary or JUnit file. Read-only process checks found no remaining Graph test
processes or descendants of the recorded launcher. Only child coverage fragments
remained, and they are excluded from final coverage evidence. This is an
interrupted diagnostic, not a completed full gate; the unreported failures'
causes are unknown. No startup/attempt budget or 95% coverage threshold was
increased to make the run pass. Fresh hosted CI on the exact published commit
must complete before this increment can claim full acceptance. The earlier
object-storage Linux run is not verification of the new process-DAG source.

A single follow-up diagnostic reran the existing actor benchmark parameter
`test_real_actor_trials_preserve_sharded_state_and_bound_inflight_requests[2-1]`
under coverage with verbose output and a separate JUnit file. It completed with
one pass in 64.44 seconds (28.75 seconds in the test call), followed by exit code
zero and no remaining Graph test processes. This isolates one successful
execution; it does not establish the causes of the interrupted run's failure
markers. No other parameter or second full suite was rerun. Final Ruff lint and
format checks, strict Mypy (22 source files), Bandit, frozen-lock consistency and
`git diff --check` passed. Full-suite coverage acceptance and final-source Linux
verification remain pending rather than inheriting earlier increments' results.
