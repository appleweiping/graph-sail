# Calibration and benchmark protocol

Graph Sail separates two different measurements that should not be reported as one number:

1. **workload estimates** (`latency_ms`, memory, payload size, and link rate), which describe the
   target deployment; and
2. **planner runtime**, which measures how long Graph Sail takes to produce a plan on the current
   host.

The resulting schedule makespan is a model estimate. It is not end-to-end latency measured from a
deployed service.

## Profiler integration boundary

`graph-sail calibrate` accepts newline-delimited JSON so a model server, Python profiler, shell
harness, or telemetry query can export measurements without becoming a Graph Sail dependency:

```json
{"node":"vision-encoder","device":"gpu-0","latency_ms":12.0,"run_id":"trial-03"}
```

`node`, `device`, and a finite positive `latency_ms` are required. `run_id` is optional provenance and
the sorted unique run IDs used by each aggregate are retained in `calibration.json`.
Unknown fields, duplicate JSON keys, non-finite values, and unknown graph cells are rejected. The
calibrator uses the sample median for each node/device cell and writes:

- `graph.json`: a complete, validated graph that can be passed directly to `plan`;
- `calibration.json`: sample count, median, minimum, and maximum for each replaced cell.

The command never infers memory or network transfer from component latency. Those quantities require
separate instrumentation and remain unchanged. Declared device `contention` and node `batch`
parameters are likewise carried through untouched: they are model inputs the caller fits with its own
experiment, and no latency observation can create or revise them.

```bash
graph-sail calibrate examples/demo-output/graph.json examples/measurements.jsonl \
  --output calibrated
graph-sail plan calibrated/graph.json --output calibrated-plan
```

Use `--ignore-unknown` only for a shared profiler export. Ignored node/device pairs remain explicit in
the report.

## Reproducible planner baseline

```bash
graph-sail benchmark examples/demo-output/graph.json --repeats 11 --warmups 2 \
  --output benchmark.json
```

The command runs the greedy earliest-finish baseline and bounded beam search. It records:

- estimated makespan and its ratio to the best baseline in that run;
- median and nearest-rank p95 host-side planner runtime;
- a SHA-256 digest of the complete deterministic plan;
- graph size and canonical SHA-256 input digest, Python implementation, platform, repeats, warmups,
  and timing scope.

Only `planner.plan()` is timed: JSON parsing and result serialization are excluded. Runtime numbers
are comparable only on controlled hosts. For a paper or release, pin the commit, Python version,
machine, power mode, graph inputs, warmups, repeats, and beam width; retain the JSON files as research
artifacts. Never present the faster planner runtime as a better deployment schedule, or the estimated
makespan as hardware throughput.

`benchmarks/results/windows-python314.json` is one checked-in reference run on the Windows and
CPython environment recorded inside that file. Tests recompute its graph and plan digests, modeled
makespans, and baseline identities while deliberately ignoring elapsed-time fields. Those timings
describe only that host and run; they are neither a portable speed baseline nor a performance claim.

## Resource ceilings

The public boundary rejects work before it can grow without bound: graph and observation files are
limited to 16 MiB, observation JSONL to 100,000 records, labels to 1,024 characters, inventories to
1,024 devices and 10,000 nodes, dependency/link collections to 100,000 entries each, and latency
profiles to 1,024 cells per node. Contention models saturate at 1,024 co-tenants and batch sizes are
limited to 100,000. Beam width is capped at 4,096. Planning and benchmark commands also
estimate candidate expansions and reject requests above fixed work budgets. These are safety
ceilings, not recommended production sizes; algorithmic cost and memory may become material earlier.

## Current evidence boundary

The checked-in measurements are a format demonstration, not a published hardware result. A credible
deployment claim still requires public raw traces or a documented collection harness, device and
software versions, repeated end-to-end trials, and uncertainty reporting. This repository now makes
that experiment reproducible once those measurements exist; it does not fabricate them.
