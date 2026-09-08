# Changelog

All notable changes are documented here.

## [Unreleased]

## [0.1.0] - 2026-09-08

### Added

- .NET 10 in-memory RelayGrid runtime foundation.
- Bounded partition channels and stable partition routing.
- Per-partition FIFO with cross-partition concurrency.
- Retry, successful-work idempotency, dead-letter and graceful-drain semantics.
- PostgreSQL durable journal with versioned schema application.
- Head-of-partition durable claims, worker leases and fencing tokens.
- Retry availability, transactional dead-lettering and expired-lease recovery.
- Durable PostgreSQL worker orchestration with fenced completion, persisted retry and dead-letter execution.
- Standard .NET `ActivitySource` and `Meter` hooks for durable processing.
- Graceful durable-worker stop semantics that preserve already claimed handler execution.
- Deterministic core suite and PostgreSQL Testcontainers integration suite.
- CI and CodeQL workflows.
- NuGet package metadata, symbol packages, SHA-256 release manifest and tag-driven release automation.
