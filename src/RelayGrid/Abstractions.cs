namespace Karzoun.RelayGrid;

public interface IRelayHandler
{
    ValueTask HandleAsync(RelayEnvelope envelope, CancellationToken cancellationToken);
}

public interface IDeadLetterSink
{
    ValueTask WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken);
}

public interface IRelayDelay
{
    ValueTask DelayAsync(TimeSpan delay, CancellationToken cancellationToken);
}

public sealed class SystemRelayDelay : IRelayDelay
{
    public async ValueTask DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }
}
