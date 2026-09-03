# Example

`demo-output/graph.json` describes a small image-and-audio assistant. The two decoders run on the
CPU, two modality encoders can use either GPU, the language core consumes both embeddings, and the
CPU formats the final token stream.

Regenerate every checked-in artifact from the actual planner:

```bash
graph-sail demo --output examples/demo-output --write-input
```

The command writes:

- `graph.json`: validated input;
- `plan.json`: full placement, metrics, and candidate trace;
- `plan.dot`: Graphviz source grouped by selected device;
- `report.html`: standalone, local HTML timeline with no remote assets.

`measurements.jsonl` demonstrates the profiler-neutral calibration boundary. Apply its measured cells
and benchmark both bundled planners without changing the checked-in demo graph:

```bash
graph-sail calibrate examples/demo-output/graph.json examples/measurements.jsonl \
  --output calibrated
graph-sail benchmark calibrated/graph.json --repeats 7 --output calibrated/benchmark.json
```
