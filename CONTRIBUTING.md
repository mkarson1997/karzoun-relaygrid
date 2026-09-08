# Contributing

1. Open an issue for behavior changes.
2. Work on a feature branch.
3. Keep queue bounds and ordering semantics explicit.
4. Add deterministic coverage for concurrency behavior.
5. Run the Release build with warnings as errors and the executable test suite.
6. Do not weaken CI or analyzers to make a change pass.
7. Do not claim durability, exactly-once delivery, distributed ordering, or measured performance without corresponding implementation and evidence.
