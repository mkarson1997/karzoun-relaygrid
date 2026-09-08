# Architecture

## Admission and partitioning

Each partition owns a bounded `Channel<RelayEnvelope>`. `PublishAsync` routes a message using stable FNV-1a over the UTF-8 partition key and waits when that partition is full. `TryPublish` is the non-blocking admission path.

Backpressure is therefore local to the target partition. A hot partition can fill and block its own publishers without converting other partition queues into unbounded storage.

## Ordering

Each partition has exactly one reader task. Messages accepted into the same partition are handled sequentially in channel order. RelayGrid does not claim a global order across partitions.

## Retries and dead letters

A handler receives at most `MaxAttempts`. Between failed attempts, RelayGrid applies deterministic capped exponential backoff through an injectable `IRelayDelay`. Exhausted work is handed to the caller-provided `IDeadLetterSink`.

A dead-letter sink failure is treated as a runtime fault. Silently dropping a poison message is not an accepted fallback.

## Idempotency

The in-memory coordinator tracks successful idempotency keys and coordinates concurrent work for the same key. A duplicate waits for in-flight work. If the leader succeeds, the duplicate is suppressed. If the leader fails through to dead-letter, the key is not persisted as successful and a later duplicate may attempt processing again.

This is process-local idempotency, not durable exactly-once delivery.

## Shutdown

The first `StopAsync` transition closes all writers and waits for partition readers to drain. The cancellation token passed to `StopAsync` only cancels that caller's wait. It is deliberately not forwarded to handlers that were already accepted.

Later durability work must preserve these semantics while adding crash recovery and fencing.
