# Roadmap

## v0.1 - core runtime foundation

- bounded partitioned channels
- deterministic partition routing
- FIFO per partition
- cross-partition concurrency
- bounded retries and dead-letter handoff
- in-memory successful-work idempotency
- graceful drain semantics
- deterministic concurrency/stress tests

## v0.2 - durable journal and recovery

- PostgreSQL event journal
- atomic state transitions
- durable idempotency records
- leases with fencing tokens
- stale-worker rejection
- crash/restart recovery proven with Testcontainers

## v0.3 - operations and performance

- OpenTelemetry traces
- Prometheus-compatible metrics
- BenchmarkDotNet harness
- measured throughput and latency under documented hardware/runtime settings
- fault-injection coverage around database and worker interruption

## Not promised

RelayGrid will not claim exactly-once delivery or distributed ordering unless those properties are precisely defined and proven under the implemented failure model.
