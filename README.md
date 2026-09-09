# Karzoun RelayGrid

[![CI](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml)
[![CodeQL](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml)
[![Release](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/release.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

RelayGrid is a C#/.NET 10 event-processing engine focused on bounded admission, deterministic partition routing, per-partition FIFO processing, explicit retry/dead-letter semantics, and durable PostgreSQL work leasing.

It is built as a compact systems-engineering project: concurrency, ordering, backpressure, durable state transitions, fencing, recovery and supply-chain controls are explicit rather than hidden behind a framework.

## Architecture at a glance

```mermaid
flowchart LR
    P[Publisher] --> R[Stable partition router]
    R --> Q[Bounded per-partition queues]
    Q --> H[Handler]
    P --> J[(PostgreSQL journal)]
    J --> C[Head-of-partition claim]
    C --> L[Fenced lease]
    L --> H
    H --> OK[Complete]
    H --> RETRY[Retry]
    H --> DL[Dead letter]
```

The in-memory layer uses bounded `System.Threading.Channels`; the durable layer adds PostgreSQL journaling, head-of-partition claiming and lease fencing. See the [architecture document](docs/architecture.md) for the full execution and recovery model.

## Engineering proof points

| Area | What the repository demonstrates |
| --- | --- |
| Concurrency | FIFO within a partition with concurrency across independent partitions. |
| Backpressure | Bounded queues make admission pressure explicit instead of allowing unbounded memory growth. |
| Determinism | Stable FNV-1a routing and deterministic capped retry backoff. |
| Durability | PostgreSQL journal with explicit lifecycle states and monotonic sequence numbers. |
| Distributed-worker safety | Lease owner plus monotonically increasing fencing token rejects stale worker transitions. |
| Recovery | Expired leases can be reclaimed without allowing an older worker to overwrite newer state. |
| Testing | Deterministic runtime suite plus Testcontainers integration tests against real PostgreSQL. |
| Security | CodeQL `security-extended`, least-privilege workflow permissions and immutable SHA-pinned GitHub Actions. |
| Release engineering | Versioned NuGet packages, symbols, SHA-256 manifest and build-provenance attestations. |

## Core runtime

- bounded `System.Threading.Channels` queue per partition
- stable FNV-1a UTF-8 partition routing
- FIFO processing within a partition with concurrency across partitions
- bounded retry count with deterministic capped exponential backoff
- process-local successful-work idempotency
- graceful stop that drains accepted work without forwarding stop-wait cancellation into handlers

## PostgreSQL durability

`Karzoun.RelayGrid.Postgres` adds durable storage and worker execution using Npgsql:

- versioned schema application under a PostgreSQL advisory transaction lock
- durable envelopes, payloads, partition indices and monotonic database sequence numbers
- head-of-partition claims: an active or delayed head item blocks later work in that partition
- lease owner + monotonically increasing fencing token
- expired lease reclamation after worker loss
- stale workers cannot complete, retry or dead-letter after a newer lease is issued
- retry transition persists failure metadata and future availability without allowing overtaking
- dead-letter state change + dead-letter record in one database transaction
- one worker loop per configured partition, preserving the journal's explicit head ordering
- handler success completes through the current fence token; handler failure persists retry or dead-letter state
- shutdown stops new polling but does not cancel an already claimed handler
- database and journal faults propagate instead of being swallowed

The Testcontainers integration suite exercises schema re-application, restart persistence, fencing, retry blocking, dead-letter progression, expired-lease recovery, successful worker execution, retry exhaustion, same-partition worker order, and graceful stop against real PostgreSQL.

## Telemetry hooks

The PostgreSQL worker emits standard .NET diagnostics without requiring an exporter package:

- `ActivitySource`: `Karzoun.RelayGrid.Postgres`
- `Meter`: `Karzoun.RelayGrid.Postgres`
- counters for claimed, completed, retried and dead-lettered work
- processing spans tagged with partition, attempt, fence token and outcome

Applications can attach OpenTelemetry or another `ActivityListener`/`MeterListener` externally.

## Distribution

Versioned releases publish two NuGet packages:

- `Karzoun.RelayGrid`
- `Karzoun.RelayGrid.Postgres`

The GitHub Release also contains both `.nupkg` files, both `.snupkg` symbol packages and `SHA256SUMS.txt`. Primary NuGet packages are published to GitHub Packages and receive GitHub build-provenance attestations. After configuring the repository's GitHub Packages NuGet source, applications can reference the package that matches the layer they need.

## Release and supply-chain controls

- CI builds with warnings as errors and runs deterministic + PostgreSQL Testcontainers suites
- CodeQL analyzes C# with `security-extended` queries
- third-party GitHub Actions are pinned to reviewed immutable commit SHAs
- release packaging is validated on pull requests before tag publication
- release artifacts include NuGet symbols and a SHA-256 checksum manifest
- primary packages receive GitHub build-provenance attestations

## Important boundaries

RelayGrid does **not** claim:

- exactly-once delivery
- global or distributed ordering
- consensus or replicated-queue semantics
- durable cross-message idempotency yet
- automatic lease renewal yet
- benchmark throughput or latency numbers yet

Durable worker leases are fixed-duration in this milestone. A handler that runs beyond its configured lease can be fenced by a newer worker after expiry. Choose a lease duration longer than expected handler execution until lease renewal is implemented.

## Build and test

Requires the .NET 10 SDK. PostgreSQL integration tests also require Docker.

```bash
dotnet build tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release -warnaserror
dotnet run --project tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release --no-build

dotnet restore tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj --locked-mode
dotnet build tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj -c Release --no-restore -warnaserror
dotnet run --project tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj -c Release --no-build
```

See [Architecture](docs/architecture.md), [Security](SECURITY.md), [Contributing](CONTRIBUTING.md), and the [Roadmap](ROADMAP.md).
