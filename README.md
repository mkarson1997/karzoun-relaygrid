# Karzoun RelayGrid

[![CI](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml)
[![CodeQL](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

RelayGrid is a C#/.NET event-processing runtime focused on bounded admission, deterministic partition routing, per-partition FIFO processing, retry semantics, successful-work idempotency, and explicit dead-letter handling.

The current v0.1 foundation is intentionally **in-memory**. PostgreSQL durability, leases/fencing, crash recovery, OpenTelemetry, Prometheus metrics, and measured benchmarks are roadmap work and are not claimed yet.

## Core semantics

- `System.Threading.Channels` bounded queue per partition with `BoundedChannelFullMode.Wait`
- stable FNV-1a UTF-8 partition routing rather than process-randomized `string.GetHashCode()`
- one sequential reader per partition, preserving FIFO for messages routed to that partition
- parallel processing across different partitions
- bounded retry count with deterministic capped exponential backoff
- explicit dead-letter sink after retry exhaustion
- idempotency coordination that suppresses duplicate work after a successful key completes
- graceful stop closes admission and drains already accepted work
- cancelling a caller's `StopAsync` wait does **not** cancel accepted handlers
- runtime snapshot counters for accepted, succeeded, duplicate-suppressed, retry, dead-letter and active-handler counts

## Important boundaries

RelayGrid v0.1 does **not** claim:

- durable delivery across process crashes
- exactly-once processing
- distributed ordering
- distributed consensus or replicated queue semantics
- persistent idempotency

A dead-letter sink is supplied explicitly by the caller. If that sink itself fails, RelayGrid faults the runtime instead of silently discarding the message.

## Build and test

Requires the .NET 10 SDK.

```bash
dotnet restore tests/RelayGrid.Tests/RelayGrid.Tests.csproj
dotnet build tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release -warnaserror
dotnet run --project tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release --no-build
```

The test project is a dependency-free deterministic executable suite. It exercises FIFO ordering, cross-partition concurrency, bounded backpressure, retries, dead-lettering, idempotency, stop-wait cancellation semantics, and a multi-partition stress run.

See [Architecture](docs/architecture.md), [Security](SECURITY.md), [Contributing](CONTRIBUTING.md), and the [Roadmap](ROADMAP.md).
