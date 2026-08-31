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
{"name": "gpu-0", "memory_mb": 16000, "kinds": ["vision", "language"]}
```

An empty or omitted `kinds` array means no kind restriction.

## Node

```json
{
  "id": "vision-encoder",
  "kind": "vision",
  "memory_mb": 2900,
  "latency_ms": {"gpu-0": 11.0, "gpu-1": 13.0},
  "allowed_devices": ["gpu-0", "gpu-1"],
  "pinned_device": null
}
```

`latency_ms` controls actual eligibility: a device must be present in this mapping. `allowed_devices`
is an optional additional restriction. `pinned_device` restricts a node to exactly one device.

## Edge and link

```json
{"source": "vision-encoder", "target": "language-core", "payload_mb": 12, "label": "vision tokens"}
```

```json
{"source": "gpu-0", "target": "gpu-1", "bandwidth_mb_s": 22000, "latency_ms": 0.04}
```

Links are directed because real paths may be asymmetric. Local edges always have zero transfer cost.
