using Karzoun.RelayGrid;

namespace Karzoun.RelayGrid.Postgres;

public enum DurableRelayState : short
{
    Pending = 0,
    Leased = 1,
    Completed = 2,
    DeadLettered = 3,
}

public sealed record DurableRelayMessage(
    Guid Id,
    long Sequence,
    RelayEnvelope Envelope,
    int PartitionIndex,
    DurableRelayState State,
    int AttemptCount,
    DateTime AvailableAtUtc,
    string? LeaseOwner,
    long FenceToken,
    DateTime? LeaseUntilUtc,
    DateTime CreatedAtUtc,
    string? LastErrorType,
    string? LastErrorMessage);

public sealed record DurableRelayLease(
    Guid Id,
    long Sequence,
    RelayEnvelope Envelope,
    int PartitionIndex,
    int AttemptCount,
    string WorkerId,
    long FenceToken,
    DateTime LeaseUntilUtc);

public sealed class StaleRelayLeaseException : InvalidOperationException
{
    public StaleRelayLeaseException(Guid messageId, string workerId, long fenceToken)
        : base($"Lease is stale for message '{messageId}', worker '{workerId}', fence token {fenceToken}.")
    {
        MessageId = messageId;
        WorkerId = workerId;
        FenceToken = fenceToken;
    }

    public Guid MessageId { get; }

    public string WorkerId { get; }

    public long FenceToken { get; }
}
