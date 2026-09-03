# Changelog

All notable changes are recorded here. This project follows semantic versioning.

## [Unreleased]

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
