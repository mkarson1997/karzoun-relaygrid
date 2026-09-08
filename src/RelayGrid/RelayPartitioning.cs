using System.Text;

namespace Karzoun.RelayGrid;

public static class RelayPartitioning
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    public static int GetPartition(string partitionKey, int partitionCount)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionKey);
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);

        byte[] bytes = Encoding.UTF8.GetBytes(partitionKey);
        ulong hash = FnvOffsetBasis;

        foreach (byte value in bytes)
        {
            hash ^= value;
            hash *= FnvPrime;
        }

        return (int)(hash % (uint)partitionCount);
    }
}
