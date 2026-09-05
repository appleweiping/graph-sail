# Graph document format

The root JSON object accepts the following fields. Unknown fields are rejected so spelling mistakes
cannot silently change a plan. Duplicate JSON object names, text control characters, non-finite
numbers, and names that become duplicates after whitespace normalization are also rejected.

| Field | Required | Meaning |
|---|---:|---|
| `name` | no | Human-readable graph name; defaults to `graph`. |
| `devices` | yes | Non-empty device inventory. |
| `nodes` | yes | Non-empty logical component list. |
| `edges` | no | Directed data dependencies. |
| `links` | no | Directed device-link overrides. |
| `default_bandwidth_mb_s` | no | Fallback bandwidth; default `1000`. |
| `default_link_latency_ms` | no | Fallback fixed latency; default `0`. |

## Device

```json
{
  "name": "gpu-0",
  "memory_mb": 16000,
  "kinds": ["vision", "language"],
  "contention": {"slowdown_per_cotenant": 0.12, "max_cotenants": 3}
}
```

An empty or omitted `kinds` array means no kind restriction.

`contention` is optional and opts the device in to the co-residency latency model described in
[architecture.md](architecture.md). `slowdown_per_cotenant` is a number of zero or greater and
`max_cotenants` is an integer from 1 to 1024. Omit the object entirely to keep the caller's isolated
latency estimates unchanged.

## Node

```json
{
  "id": "vision-encoder",
  "kind": "vision",
  "memory_mb": 2900,
  "latency_ms": {"gpu-0": 11.0, "gpu-1": 13.0},
  "allowed_devices": ["gpu-0", "gpu-1"],
  "pinned_device": null,
  "batch": {"size": 4, "window_ms": 2.0, "fixed_fraction": 0.6}
}
```

`latency_ms` controls actual eligibility: a device must be present in this mapping. `allowed_devices`
is an optional additional restriction. `pinned_device` restricts a node to exactly one device.

`batch` is optional and opts the node in to the batch model described in
[architecture.md](architecture.md). `size` is an integer from 1 to 100000, `window_ms` is a number of
zero or greater and defaults to `0`, and `fixed_fraction` is a number from 0 to 1. Omit the object
entirely to keep the caller's isolated latency estimates unchanged.

## Edge and link

```json
{"source": "vision-encoder", "target": "language-core", "payload_mb": 12, "label": "vision tokens"}
```

```json
{"source": "gpu-0", "target": "gpu-1", "bandwidth_mb_s": 22000, "latency_ms": 0.04}
```

Links are directed because real paths may be asymmetric. Local edges always have zero transfer cost.
