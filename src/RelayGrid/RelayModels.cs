namespace Karzoun.RelayGrid;

public readonly record struct RelayReceipt(string MessageId, int PartitionIndex);

public enum RelayDisposition
{
    Succeeded = 1,
    DuplicateSuppressed = 2,
    DeadLettered = 3,
}

public sealed record RelayProcessingResult(
    string MessageId,
    RelayDisposition Disposition,
    int Attempts,
    string? FailureType = null,
    string? FailureMessage = null);

public sealed record DeadLetterRecord(
    RelayEnvelope Envelope,
    int Attempts,
    string FailureType,
    string FailureMessage);

public readonly record struct RelayGridSnapshot(
    long Accepted,
    long Succeeded,
    long DuplicateSuppressed,
    long Retries,
    long DeadLettered,
    long ActiveHandlers);
