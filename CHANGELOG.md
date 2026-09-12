# Changelog

All notable changes are recorded here. This project follows semantic versioning.

## [Unreleased]

### Added

- Local process-backed native-generator task streams on the existing bounded
  mailbox and process actor transport, with full-frame limits, irreversible EOF
  cancellation, prefix-before-failure ordering and explicit native cleanup state.
- Offline process-stream example and real spawn, backpressure, communication-owner
  death, cleanup failure and retained-startup-ownership regression tests.
- Event-driven asyncio result/selected-terminal methods for actor, execution and
  node handles, with shared waiter bounds, removable subscriptions, wait-only
  cancellation, cross-loop notifications and fresh source-exception wrappers.
- Offline async actor/DAG example and lifecycle, capacity and publication-race tests.
- Nonblocking thread/process DAG handles, selective terminal-node waits, borrowed
  result access and explicit cancellation/close ownership over the shared scheduler.
- Event-controlled partial-result example and thread/process lifecycle regressions.
- Trusted local callable execution over validated DAGs with real dependency return
  values, global/per-device concurrency admission and measured attempt telemetry.
- Bounded application retries with exception filters, independent-branch failure
  propagation, fail-fast and cooperative cancellation/time-budget policies.
- An executable local-runtime example and a pinned whole-repository Ray gap audit.
- Real local process actors with explicit trusted factories/method allowlists,
  bounded FIFO mailboxes, asynchronous result handles, process crash detection,
  serialization limits and drain/terminate/join lifecycle policies.
- An actual process-actor capacity protocol with independent closed-form output
  verification, raw per-trial lifecycle timings and honest local workload observations.
- Context-owned immutable byte storage with bounded local files, strict JSON
  references, verified cross-process reads, no-replace publication, explicit
  release and failure-retaining capacity accounting; executable actor integration.
- Actual local spawned DAG tasks on the shared execution scheduler and actor
  transport, with child-side application retry classification, explicit object-ref
  dependencies, irreversible native EOF cancellation and joined worker retirement.
- Process-specific callback/transport/lifecycle diagnostics, an executable offline
  two-reader object DAG, and failure/cancellation/cleanup regression coverage.

### Fixed

- Keep public actor status observation non-reaping, preventing POSIX readers
  from consuming an exit status concurrently with broker cleanup.

## [0.5.0] - 2026-09-07

### Added

- Added deterministic post-plan simulation with device utilization, busy-time, and peak-memory accounting.
- Added bounded Pareto planning across latency, compute, transfer, and peak-memory objectives.

## [0.4.0] - 2026-09-07

### Added

- `ExactPlanner` and `graph-sail plan --algorithm exact`, a bounded exhaustive
  placement oracle for small graphs. It enumerates every feasible assignment
  in the stable topological order and selects the minimum makespan, with an
  explicit state ceiling so the exponential cost cannot be accidental.
- The exact planner is available to sensitivity analysis as well, making it
  possible to compare estimate fragility against a globally optimal baseline.

## [0.3.0] - 2026-09-07

### Added

- `graph-sail sensitivity`: report how much a plan depends on each latency estimate. Answers two
  questions separately -- how much of an added millisecond reaches the makespan, and how far an
  estimate can move before the placement itself changes -- because they have different answers
  and different costs.
- The makespan response is measured by perturbing one estimate and re-planning, not derived from
  finish times. The gap between a node's finish and the end of the plan is not its slack: a node
  feeding the last one has successors waiting on it, and on the bundled demo that arithmetic
  reports the language model, the largest and most critical estimate, as having slack.
- Placement changes are sought on a bounded geometric grid in both directions. The first sampled
  change is bisected against the preceding unchanged sample, and every factor actually probed is
  serialized with the result. `stable` means only that the recorded probes did not change the
  placement; narrow non-monotonic changes between probes can still be missed.
- The report's short list is the intersection of influential and observed-fragile. Influential
  alone is nearly every node on a mostly serial pipeline; the observed-fragile set can include
  estimates whose first sampled-and-refined placement change is near a factor of three. The list
  is descriptive rather than an application-specific risk threshold.
- Stability is reported against the planner that produced the plan, since a beam search and a
  greedy pass can disagree about how fragile the same graph is. The planner must reproduce the
  supplied placement and sensitivity-relevant schedule/timing on the unperturbed graph before
  analysis; that call, makespan probes, placement probes, and conservative bisection allowance
  share one hard call budget.
- `graph_sail.sensitivity` as a Python API: `analyze_sensitivity`, `makespan_sensitivity`,
  `placement_stability`, and `perturb`.

- Opt-in device contention modelling: a device may declare a linear co-residency slowdown that
  rescales the caller's isolated latency estimate, changing placement when a device is loaded.
- Opt-in request batching: a node may declare a batch size, formation window, and fixed-cost
  fraction, amortising the caller's estimate under the affine batch cost model and charging the
  window as start latency.
- `latency_scale` and `batch_window_ms` on scheduled nodes, emitted in `plan.json` and the HTML
  report only when a latency model actually applied, so unmodelled plans are byte-identical.

### Changed

- Release engineering now uses a pinned Hatchling backend, explicit wheel and source-distribution
  contents, frozen dependency resolution, reproducible archive timestamps, artifact inspection,
  isolated wheel installation, checksums, and build provenance as one consistent release gate.
- `docs/architecture.md` no longer assumes node latency is independent of batching and contention;
  the schedule assumptions, both model definitions, and their limits are stated explicitly.
- Calibration results now bind every applied median to the corresponding calibrated graph cell,
  reject known cells marked as ignored, and reject duplicate or impossible run-ID provenance.
- Plan results now bind the selected candidate trace's start, finish, and transfer values to its
  scheduled node. Nested output records are revalidated and detached during construction and
  `dataclasses.replace()`, and serialization rebuilds the full result before emitting JSON.
- Programmatic graph, mapping, and nested-pair inputs are counted while iterating, so dishonest or
  unbounded iterables cannot evade resource ceilings by reporting a false length.
- Sensitivity records now validate finite scalar domains, probe density, configured ranges, and
  recorded probe factors; reports snapshot their collections and require matching, sorted cells,
  estimates, sampling configuration, and baseline makespans on construction, replacement, and
  serialization.

## [0.2.0] - 2026-09-01

### Added

- Profiler-neutral latency calibration with strict JSONL input and auditable median aggregates.
- Machine-readable greedy/beam benchmark results, deterministic plan digests, and runtime protocol.
- Performance regression coverage and research reporting documentation.
- PEP 561 type marker and complete documentation/example source-distribution manifest.
- Checked-in, host-labelled Windows/CPython reference benchmark with digest-based regression checks.

### Changed

- Public graph and plan dataclasses now defensively snapshot nested collections and validate direct
  construction and `dataclasses.replace()` operations.
- Graph, calibration, beam-search, and benchmark entry points now enforce documented file,
  collection, text, and candidate-work resource ceilings.

## [0.1.0] - 2026-08-31

### Added

- Strict JSON contract for logical nodes, devices, links, and payload-carrying edges.
- Stable topological ordering with concrete cycle diagnostics.
- Deterministic earliest-finish greedy placement.
- Bounded beam search that can avoid greedy memory dead ends.
- Persistent device-memory constraints, transfer estimates, and pinned placements.
- Auditable candidate traces for every placement decision.
- JSON, Graphviz, and self-contained HTML report output.
- Built-in multimodal assistant demo and cross-platform CLI.

### Hardened

- Duplicate JSON fields, normalized latency-key collisions, control characters, oversized integers,
  and non-finite derived schedule values now fail explicitly.
- Cycle diagnostics use an iterative traversal and support graphs beyond Python's recursion limit.
- Critical-node and critical-chain tie-breaking now agree.
- Checked-in demo text is reproduced byte-for-byte in tests on every supported platform.
