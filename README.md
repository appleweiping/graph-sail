# Graph Sail

**Deterministic placement and scheduling for heterogeneous multimodal component graphs.**

[![CI](https://github.com/appleweiping/graph-sail/actions/workflows/ci.yml/badge.svg)](https://github.com/appleweiping/graph-sail/actions/workflows/ci.yml)
[![CodeQL](https://github.com/appleweiping/graph-sail/actions/workflows/codeql.yml/badge.svg)](https://github.com/appleweiping/graph-sail/actions/workflows/codeql.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-22c55e.svg)](LICENSE)

Modern multimodal applications are not a single model call. Image decoding, vision encoding, audio
encoding, language generation, and output formatting have different memory, latency, and hardware
constraints. Graph Sail turns those explicit estimates into a reproducible placement, schedule, and
decision trace—without downloading a model or touching a cluster.

The project is useful when you need to answer:

- Can all components fit in the available device memory?
- Which cross-device edges dominate the estimate?
- Would a fast local choice block a later pinned component?
- Why was one accelerator selected and another rejected?
- What should be measured before moving from a sketch to a deployment benchmark?

## Demo

```text
$ graph-sail demo --output examples/demo-output --write-input
planned 6 nodes in 53.135 ms
  decode-audio             -> cpu
  audio-encoder            -> gpu-1
  decode-image             -> cpu
  vision-encoder           -> gpu-0
  language-core            -> gpu-0
  format-response          -> cpu
```

Open the checked-in [interactive report](examples/demo-output/report.html), inspect the
[machine-readable plan](examples/demo-output/plan.json), or regenerate both locally. The report is
self-contained and loads no remote scripts, fonts, or analytics.

![Graph Sail demo report showing device placement, schedule, and critical chain](docs/assets/demo-report.png)

## Features

- Bounded local native-generator task streams: results become consumable while
  later work is still running, with pre-pull backpressure, explicit yield limits,
  cooperative cancellation and owned thread cleanup; see [task streams](docs/task-streams.md)
  and the [runnable example](examples/task_stream.py).
- Strict, typo-resistant JSON input contract.
- Image, audio, language, decoder, post-processing, or custom node kinds.
- Per-device latency estimates, persistent memory budgets, allowlists, and pinned nodes.
- Opt-in device-contention and request-batching latency modelling from explicitly declared
  parameters.
- Directed link bandwidth and fixed-latency estimates with explicit fallbacks.
- Stable topological ordering and concrete cycle diagnostics.
- Fast deterministic greedy planner.
- Bounded beam search that can preserve memory for later constrained components.
- Bounded exhaustive planning for small graphs, providing a reproducible optimum
  reference against which greedy and beam decisions can be checked.
- Candidate-by-candidate explanations for every placement.
- Critical-chain, utilization, transfer, and memory summaries.
- JSON, Graphviz DOT, and responsive standalone HTML reports.
- Standard-library runtime: no GPU, model weights, service, or network access required.
- Profiler-neutral JSONL calibration with median aggregation and explicit provenance.
- Reproducible greedy-versus-beam benchmark JSON with plan digests and host metadata.
- Real local DAG execution through an explicit trusted callable registry, with
  dependency return values, bounded per-device concurrency and measured timings.
- Local spawn-process actors with persistent state, an explicit method allowlist,
  bounded FIFO mailboxes, asynchronous result handles and supervised shutdown.
- Context-owned local immutable byte objects with strict transferable references,
  independent read limits, content verification and explicit release/cleanup.
- Process-backed DAG tasks reuse the same scheduler with actual child dependency
  values, application retries, native cooperative cancellation and joined retirement.
- Owned nonblocking thread/process execution handles with selective node results,
  explicit whole-graph cancellation and joined lifecycle; see
  [execution handles](docs/execution-handles.md) and the runnable offline example.
- Fractional and custom [logical task resources](docs/logical-resources.md), with
  exact 0.0001-unit admission, all-or-none named demands, feasible-ready backfill
  and joined per-attempt accounting across both execution backends.

## Run registered local functions

```bash
python examples/execute_graph.py
```

`execute_graph(graph, registry, placements)` invokes the Python functions you
register, waits for their dependencies, and returns their actual values plus
per-attempt status/timing records. Resource admission follows the graph's logical
device and persistent-memory declarations. Application retries and cooperative
cancellation are explicit options. See [Local execution](docs/local-execution.md)
for the API, shared-object semantics and time-budget behavior. Planning and
`simulate_plan` continue to report estimates independently of these observations.

For isolated stateful workers, run `python examples/process_actor.py`. The
[process actor API](docs/process-actors.md) uses trusted module-level factories,
serializes arguments/results and owns the child process lifecycle. This is a
local API, not a distributed cluster runtime or security sandbox.

For large byte inputs reused by tasks or actors, run `python examples/local_objects.py`.
The [local object API](docs/local-object-storage.md) reads a generated 2 MiB object
inside a real actor while passing only its small reference through a 4 KiB
message boundary. Readers return verified byte copies; this is not zero-copy
transport or distributed reference counting.

For a complete spawned DAG, run `python examples/process_graph.py`. The
[process execution API](docs/process-execution.md) reuses logical device admission
and dependency scheduling, explicitly transfers small object references, and
separates worker callback timing from transport/lifecycle timing. Broken or
timed-out invocations are not automatically replayed.

## Installation

Graph Sail requires Python 3.11 or newer.

```bash
git clone https://github.com/appleweiping/graph-sail.git
cd graph-sail
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .venv\Scripts\activate
python -m pip install -e .
```

For development tools:

```bash
python -m pip install -e ".[dev]"
```

## Quick start

Validate a graph before planning:

```bash
graph-sail validate examples/demo-output/graph.json
```

Run the beam planner and write a report bundle:

```bash
graph-sail plan examples/demo-output/graph.json --algorithm beam --beam-width 16 --output my-plan
```

Use `--algorithm greedy` for the fastest deterministic baseline.
For a small graph, use `--algorithm exact` to enumerate every feasible
placement and obtain the minimum makespan under the declared model. The exact
planner has a hard state budget and is intended as a correctness oracle or
regression reference, not as an unbounded production scheduler.

Calibrate latency cells from measurements exported by a deployment harness, then compare the bundled
planning baselines:

```bash
graph-sail calibrate examples/demo-output/graph.json examples/measurements.jsonl --output calibrated
graph-sail benchmark calibrated/graph.json --repeats 11 --warmups 2 --output benchmark.json
```

The calibration contract, timing protocol, research reporting rules, and remaining evidence boundary
are specified in [calibration-and-benchmarks.md](docs/calibration-and-benchmarks.md).

## Input at a glance

```json
{
  "name": "image-to-text",
  "devices": [
    {"name": "cpu", "memory_mb": 16000, "kinds": ["decode"]},
    {"name": "gpu", "memory_mb": 12000, "kinds": ["vision", "language"]}
  ],
  "nodes": [
    {"id": "decode", "kind": "decode", "memory_mb": 80, "latency_ms": {"cpu": 4}},
    {"id": "vision", "kind": "vision", "memory_mb": 2900, "latency_ms": {"gpu": 11}},
    {"id": "answer", "kind": "language", "memory_mb": 7000, "latency_ms": {"gpu": 31}}
  ],
  "edges": [
    {"source": "decode", "target": "vision", "payload_mb": 18},
    {"source": "vision", "target": "answer", "payload_mb": 12}
  ]
}
```

See the complete [graph-format reference](docs/graph-format.md).

## Python API

```python
from graph_sail import BeamPlanner, load_graph
from graph_sail.report import write_report_bundle

graph = load_graph("examples/demo-output/graph.json")
plan = BeamPlanner(beam_width=16).plan(graph)

print(plan.placements)
print(f"estimated makespan: {plan.makespan_ms:.2f} ms")
write_report_bundle(graph, plan, "my-plan")
```

Public input models and result records are defensively immutable. Constructors snapshot caller-owned
collections, mapping fields expose read-only views, and direct construction plus
`dataclasses.replace()` re-run the same domain invariants as file parsing. `PlanResult.to_dict()`
returns a detached, stable JSON-ready object suitable for CI snapshots or downstream tooling.

## How planning works

### Simulation and Pareto planning

After selecting a planner, inspect operational resource use and trade-offs:

```python
from graph_sail import pareto_plans, simulate_plan

report = pareto_plans(graph, include_exact=False)
simulation = simulate_plan(graph, report.candidates[0].plan)
print(simulation.device_utilization, simulation.peak_memory_mb)
```

`simulate_plan()` checks device capacity and non-overlap, then reports deterministic
busy time, utilization, and peak persistent memory. `pareto_plans()` keeps plans that
are non-dominated across makespan, compute, transfer, and memory objectives; exact
enumeration remains explicitly opt-in because its cost is exponential.

Graph Sail first validates the document and computes a lexicographically stable topological order.
For each node, it evaluates static compatibility, remaining persistent memory, predecessor readiness,
cross-device transfer, and device availability.

The greedy planner selects the earliest-finishing candidate immediately. The beam planner keeps a
bounded set of alternatives, which lets it avoid cases where a fast early placement consumes memory
needed by a later pinned node. Both planners keep the stable topological ready-node order fixed: beam
search explores device placements, not alternative valid execution orders for independent nodes.
The exact planner enumerates all feasible device assignments in that same fixed
order and selects the `_state_rank` minimum; its explicit state ceiling makes
the cost visible. Consequently, greedy and beam plans are deterministic and
feasible under the model but are not guaranteed global optima. The full cost
model and complexity are documented in [architecture.md](docs/architecture.md).

## How much the plan depends on your estimates

Graph Sail plans from numbers you provide, and the plan arrived as a single
placement with no indication of how much of it rested on any one of them. An
estimate whose factor-of-two probe keeps the placement deserves less worry
than one whose nearby probes change it, while neither observation proves what
happens at factors that were not probed.

```console
graph-sail sensitivity graph.json --output sensitivity.json
```

```
probed 6 estimates against 53.135 ms
  weakest observed change: language-core on gpu-0 near 12%
  worth re-measuring: language-core, vision-encoder
```

Two questions are answered separately, because they have different answers.

**Which estimates reach the makespan.** `response` is the fraction of an added
millisecond that shows up in the plan: one for a node on the critical chain,
zero for one the schedule absorbs. It is measured by perturbing the estimate
and re-planning, not derived from finish times. The gap between a node's finish
and the end of the plan is *not* its slack -- a node feeding the last one has
successors waiting on it -- and on the bundled demo that mistake reports the
language model, the single largest and most critical estimate, as having
slack.

**At which sampled factors an estimate changes the placement.** A planner's
discrete decisions are not assumed to be monotonic: a placement can change and
later return. Both directions are therefore checked on a geometric grid. The
default one-sample-per-octave grid includes `0.5x` and `2x`; the configured
endpoints are always included. After the first sampled change in either
direction, Graph Sail bisects only the bracket from the preceding unchanged
sample to refine that observed boundary. `--samples-per-octave` makes the grid
denser, subject to explicit per-direction and total-work ceilings.

| node | response | first observed slower flip | observed margin |
|---|---:|---:|---:|
| language-core | 1.00 | 1.12x | **12%** |
| vision-encoder | 1.00 | 1.24x | 24% |
| audio-encoder | 0.00 | 2.73x | 173% |
| decode-audio, decode-image, format-response | 1.00 | — | no sampled flip |

`worth re-measuring` is the descriptive intersection: influential *and* an
observed placement change. It is not an application-specific risk threshold.
Influential alone is nearly every node on a mostly serial pipeline, and the
observed fragile set includes `audio-encoder`, whose first sampled-and-refined
placement change is near three times its estimate.

Stability is a property of the plan **and** the algorithm that produced it, so
the planner is passed in rather than assumed. Before perturbation it must
reproduce the supplied placement and sensitivity-relevant schedule/timing on
the unmodified graph; the report records the algorithm label emitted by that
reproduction. This baseline call, the makespan probes, the geometric probes,
and worst-case bisection work all share one conservative planner-call ceiling.

The Python result models snapshot their record collections and recheck finite values, configured
ranges, probe density and factors, cell identity, estimate equality, and report baselines on
construction and replacement. Every stability record serializes all factors actually probed.
This makes a hand-built report fail explicitly instead of serializing contradictory evidence.

`stable` means only that no placement change was observed at those recorded
probe factors. It is not a claim about the continuous interval: a narrow
non-monotonic change between probes can be missed, and factors outside the
configured range were not tested. Increase `--samples-per-octave` when that
risk matters, and retain the serialized probe list with any conclusion.

## Interpreting results responsibly

Graph Sail's planner uses numbers you provide; its output is not a measured
throughput or service-level guarantee. The separate [actor capacity protocol](docs/actor-capacity.md)
measures bounded local worker workloads and reports startup/processing/shutdown
separately; it does not discover general hardware or deployment capacity.
Current schedules assume one node at a time per device,
persistent component memory, and non-contended transfers. Kernels that overlap, power limits, and
network contention require measurement or a richer simulator.

Contention and batching are modelled only when a device or node declares them, and only as the
linear co-residency slowdown and affine batch amortisation documented in
[architecture.md](docs/architecture.md). Both models consume parameters you fitted elsewhere. Graph
Sail measures no interference of its own and has no request-arrival model, so a contended or batched
plan is an estimate built on an estimate: it is not a measurement of shared hardware, and a batched
makespan is still a latency estimate rather than a throughput figure.

The decision trace is designed to expose these assumptions instead of hiding them behind a single
score.

## Development

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m coverage run -m pytest
python -m coverage combine
python -m coverage report
```

The suite covers parsing, graph validation, cycle diagnostics, transfer estimates, memory constraints,
greedy dead ends, beam recovery, deterministic output, calibration provenance, baseline digests,
performance guards, report escaping, and CLI behavior on CPU.

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and the
[code of conduct](CODE_OF_CONDUCT.md) before contributing. Maintainer authority is documented in
[GOVERNANCE.md](GOVERNANCE.md), release verification in [docs/releases.md](docs/releases.md), and
versioned citation metadata in [CITATION.cff](CITATION.cff).

## Companion repositories

Graph Sail is one independent part of a small multimodal tooling suite. [Payload Palette](https://github.com/appleweiping/payload-palette) validates request media, [Frame Quorum](https://github.com/appleweiping/frame-quorum) selects auditable key frames, [Evidence Braid](https://github.com/appleweiping/evidence-braid) fuses evidence under explicit policies, and [Stream Quilt](https://github.com/appleweiping/stream-quilt) aligns event streams. The repositories have separate contracts and release cycles; no runtime dependency is implied.

## License

Graph Sail is released under the [MIT License](LICENSE).
