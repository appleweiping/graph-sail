# Governance

Graph Sail is maintainer-led. The current release maintainer is
[@appleweiping](https://github.com/appleweiping). Design discussion, compatibility changes, and
roadmap work happen in public issues and pull requests; vulnerabilities follow the private process
in [SECURITY.md](SECURITY.md).

Changes to cost semantics, feasibility constraints, planner tie-breaking, calibration, or result
schemas require tests, a compatibility note, and an updated changelog. Performance and quality
claims must retain graph provenance, the complete machine-readable result, and the limitations in
[calibration-and-benchmarks.md](docs/calibration-and-benchmarks.md).

A release requires green CI, regenerated deterministic examples, wheel and source-distribution
checks, an isolated installation smoke test, checksums, and build provenance. Maintainer roles and
decision rules may evolve through an explicit pull request as the contributor base grows.

