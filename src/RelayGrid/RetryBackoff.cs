namespace Karzoun.RelayGrid;

public static class RetryBackoff
{
    public static TimeSpan GetDelay(int failedAttempt, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(failedAttempt, 1);

        if (baseDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(baseDelay));
        }

        if (maxDelay < baseDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDelay));
        }

        if (baseDelay == TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        int exponent = Math.Min(failedAttempt - 1, 30);
        long factor = 1L << exponent;
        long ticks = baseDelay.Ticks > maxDelay.Ticks / factor
            ? maxDelay.Ticks
            : baseDelay.Ticks * factor;

        return TimeSpan.FromTicks(Math.Min(ticks, maxDelay.Ticks));
    }
}
