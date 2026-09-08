# Roadmap

## v0.1 - runtime and durability foundation

- bounded partitioned channels
- deterministic partition routing
- FIFO per in-memory partition
- bounded retries and explicit dead-letter handoff
- process-local successful-work idempotency
- graceful drain semantics
- PostgreSQL durable journal
- head-of-partition FIFO claims
- lease ownership and fencing tokens
- retry availability without overtaking
- transactional dead-letter transition
- expired-lease crash recovery
- Testcontainers integration evidence

## v0.2 - worker orchestration and durable idempotency

- bridge durable claims into handler execution
- durable successful-key coordination with explicit concurrent-key semantics
- stale-worker outcome rejection end to end
- bounded polling/backoff across partitions
- shutdown handoff for durable workers
- fault injection around database interruption

## v0.3 - operations and measured performance

- OpenTelemetry traces
- Prometheus-compatible metrics
- BenchmarkDotNet harness
- measured throughput and latency under documented hardware/runtime settings
- saturation/backpressure measurements

## Not promised

RelayGrid will not claim exactly-once delivery or distributed ordering unless those properties are precisely defined and proven under the implemented failure model.
