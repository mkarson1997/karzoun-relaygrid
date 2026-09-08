namespace Karzoun.RelayGrid;

public sealed record RelayEnvelope
{
    public RelayEnvelope(
        string messageId,
        string idempotencyKey,
        string partitionKey,
        ReadOnlyMemory<byte> payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);

        MessageId = messageId;
        IdempotencyKey = idempotencyKey;
        PartitionKey = partitionKey;
        Payload = payload.ToArray();
    }

    public string MessageId { get; }

    public string IdempotencyKey { get; }

    public string PartitionKey { get; }

    public ReadOnlyMemory<byte> Payload { get; }
}
