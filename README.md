# Karzoun RelayGrid

[![CI](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/ci.yml)
[![CodeQL](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml/badge.svg)](https://github.com/mkarson1997/karzoun-relaygrid/actions/workflows/codeql.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

RelayGrid is a C#/.NET event-processing runtime focused on bounded admission, deterministic partition routing, per-partition FIFO processing, retry semantics, explicit dead-letter handling, and durable PostgreSQL work leasing.

## Core runtime

- bounded `System.Threading.Channels` queue per partition
- stable FNV-1a UTF-8 partition routing
- FIFO processing within a partition with concurrency across partitions
- bounded retry count with deterministic capped exponential backoff
- process-local successful-work idempotency
- graceful stop that drains accepted work without forwarding stop-wait cancellation into handlers

## PostgreSQL durable journal

`Karzoun.RelayGrid.Postgres` adds a durable store using Npgsql:

- versioned schema application under a PostgreSQL advisory transaction lock
- durable envelopes, payloads, partition indices and monotonic database sequence numbers
- head-of-partition claims: an active or delayed head item blocks later work in that partition
- lease owner + monotonically increasing fencing token
- expired lease reclamation after worker loss
- stale workers cannot complete, retry or dead-letter after a newer lease is issued
- retry transition persists failure metadata and future availability without allowing overtaking
- dead-letter state change + dead-letter record in one database transaction
- completed/dead-lettered work unblocks the next partition item

The Testcontainers integration suite proves schema re-application, persistence across new Npgsql data sources, lease fencing, retry blocking, transactional dead-letter progression, and expired-lease crash recovery against real PostgreSQL.

## Important boundaries

RelayGrid does **not** claim:

- exactly-once delivery
- global or distributed ordering
- consensus or replicated-queue semantics
- durable cross-message idempotency yet
- benchmark throughput or latency numbers yet

The in-memory runtime and durable journal are deliberately separate layers today. A later worker orchestration milestone can connect them while preserving the documented lease and ordering semantics.

## Build and test

Requires the .NET 10 SDK. PostgreSQL integration tests also require Docker.

```bash
dotnet build tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release -warnaserror
dotnet run --project tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release --no-build

dotnet restore tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj --use-lock-file
dotnet build tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj -c Release --no-restore -warnaserror
dotnet run --project tests/RelayGrid.Postgres.Tests/RelayGrid.Postgres.Tests.csproj -c Release --no-build
```

See [Architecture](docs/architecture.md), [Security](SECURITY.md), [Contributing](CONTRIBUTING.md), and the [Roadmap](ROADMAP.md).
