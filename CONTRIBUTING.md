# Contributing

Thank you for improving Graph Sail. Small, focused changes are easiest to review.

## Development setup

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
```

Run the same gates as CI before opening a pull request:

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m coverage run -m pytest
python -m coverage report
python -m graph_sail demo --output demo-output
```

## Change expectations

- Add a regression test for behavior changes and bug fixes.
- Keep planning deterministic. A repeated run over the same graph must produce the same plan.
- Explain new cost-model assumptions in `docs/architecture.md`.
- Do not add network calls, telemetry, or model downloads to the core library.
- Do not present estimated latency as a measured benchmark.
- Update `CHANGELOG.md` for user-visible changes.

Open an issue before implementing a new scheduling model or changing the graph contract. Those
changes affect reproducibility and deserve agreement on semantics first.

By contributing, you agree that your contribution is licensed under the MIT License.
