# Karzoun RelayGrid v0.1.0

RelayGrid v0.1.0 is the first public release of the .NET event-processing engine and its PostgreSQL durability layer.

## Packages

- `Karzoun.RelayGrid` — bounded in-memory partition runtime
- `Karzoun.RelayGrid.Postgres` — durable PostgreSQL journal, fenced leases and worker orchestration

Both packages target .NET 10. The PostgreSQL package uses Npgsql and ships the embedded versioned schema migration required by the journal.

## Proven behavior

- bounded `System.Threading.Channels` admission and per-partition FIFO processing
- concurrency across independent partitions
- deterministic capped exponential retry backoff
- process-local successful-work idempotency in the in-memory runtime
- durable PostgreSQL envelopes and monotonic database sequence numbers
- head-of-partition claim semantics that prevent delayed or leased work from being overtaken
- lease ownership with monotonically increasing fencing tokens
- expired-lease reclamation after worker loss
- fenced complete/retry/dead-letter transitions
- transactional dead-letter persistence
- durable worker execution from claim through handler outcome
- graceful worker stop that does not cancel an already claimed handler
- standard .NET `ActivitySource` and `Meter` hooks without an exporter dependency

CI evidence includes the deterministic core suite and a real PostgreSQL Testcontainers suite covering schema re-application, restart persistence, lease fencing, retry blocking, dead-letter progression, crash recovery, worker completion, retry exhaustion, same-partition worker order and graceful stop.

## Important boundaries

RelayGrid v0.1.0 does **not** claim exactly-once delivery, global/distributed ordering, consensus, durable cross-message idempotency, automatic lease renewal, or published throughput/latency numbers.

Durable worker leases are fixed-duration in v0.1.0. A handler that runs beyond its configured lease can be fenced by a newer worker after expiry, so applications should configure lease duration accordingly until renewal is implemented.

## Supply chain

The GitHub Release includes both `.nupkg` files, both `.snupkg` symbol packages and `SHA256SUMS.txt`. The primary NuGet packages are also published to GitHub Packages, and the release workflow generates GitHub build provenance attestations for the primary `.nupkg` artifacts.
