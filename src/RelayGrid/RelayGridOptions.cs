namespace Karzoun.RelayGrid;

public sealed record RelayGridOptions
{
    public int PartitionCount { get; init; } = 4;

    public int PartitionCapacity { get; init; } = 256;

    public int MaxAttempts { get; init; } = 3;

    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(50);

    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(2);

    internal void Validate()
    {
        if (PartitionCount is < 1 or > 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(PartitionCount), "PartitionCount must be between 1 and 1024.");
        }

        if (PartitionCapacity is < 1 or > 1_000_000)
        {
            throw new ArgumentOutOfRangeException(nameof(PartitionCapacity), "PartitionCapacity must be between 1 and 1,000,000.");
        }

        if (MaxAttempts is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxAttempts), "MaxAttempts must be between 1 and 100.");
        }

        if (RetryBaseDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryBaseDelay));
        }

        if (RetryMaxDelay < RetryBaseDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryMaxDelay), "RetryMaxDelay must be greater than or equal to RetryBaseDelay.");
        }
    }
}
