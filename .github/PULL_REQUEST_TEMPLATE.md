## What changed

Describe the behavior change and why it is needed.

## Runtime invariants

- [ ] Per-partition FIFO semantics remain explicit.
- [ ] Backpressure remains bounded.
- [ ] Retry/idempotency/DLQ behavior is covered by tests where relevant.
- [ ] No durability, exactly-once, or distributed-ordering claim was added without evidence.

## Verification

- [ ] `dotnet build tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release -warnaserror`
- [ ] `dotnet run --project tests/RelayGrid.Tests/RelayGrid.Tests.csproj -c Release`
