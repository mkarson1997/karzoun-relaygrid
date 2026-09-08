using System.Diagnostics;
using System.Diagnostics.Metrics;
using Karzoun.RelayGrid;

namespace Karzoun.RelayGrid.Postgres;

public sealed record PostgresRelayWorkerOptions
{
    public string WorkerId { get; init; } = $"relaygrid-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public IReadOnlyList<int> Partitions { get; init; } = new[] { 0 };

    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(25);

    public int MaxAttempts { get; init; } = 3;

    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(50);

    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(2);

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(WorkerId);

        if (Partitions.Count == 0)
        {
            throw new ArgumentException("At least one partition must be configured.", nameof(Partitions));
        }

        var seen = new HashSet<int>();
        foreach (int partition in Partitions)
        {
            if (partition < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(Partitions), "Partition indices cannot be negative.");
            }

            if (!seen.Add(partition))
            {
                throw new ArgumentException("Partition indices must be unique per worker instance.", nameof(Partitions));
            }
        }

        if (LeaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(LeaseDuration));
        }

        if (PollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(PollInterval));
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
            throw new ArgumentOutOfRangeException(nameof(RetryMaxDelay));
        }
    }
}

public static class RelayGridPostgresTelemetry
{
    public const string ActivitySourceName = "Karzoun.RelayGrid.Postgres";

    public const string MeterName = "Karzoun.RelayGrid.Postgres";

    internal static readonly ActivitySource Activities = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Counter<long> Claimed = Meter.CreateCounter<long>("relaygrid.durable.claimed");

    internal static readonly Counter<long> Completed = Meter.CreateCounter<long>("relaygrid.durable.completed");

    internal static readonly Counter<long> Retried = Meter.CreateCounter<long>("relaygrid.durable.retried");

    internal static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>("relaygrid.durable.dead_lettered");
}

public sealed class PostgresRelayWorker
{
    private const int MaxPersistedFailureMessageLength = 2048;

    private readonly PostgresRelayJournal _journal;
    private readonly IRelayHandler _handler;
    private readonly PostgresRelayWorkerOptions _options;

    public PostgresRelayWorker(
        PostgresRelayJournal journal,
        IRelayHandler handler,
        PostgresRelayWorkerOptions options)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        using CancellationTokenSource coordinator = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        Task[] partitionLoops = _options.Partitions
            .Select(partition => RunPartitionGuardedAsync(partition, coordinator))
            .ToArray();

        try
        {
            await Task.WhenAll(partitionLoops).ConfigureAwait(false);
        }
        finally
        {
            coordinator.Cancel();
        }
    }

    private async Task RunPartitionGuardedAsync(int partitionIndex, CancellationTokenSource coordinator)
    {
        try
        {
            await RunPartitionAsync(partitionIndex, coordinator.Token).ConfigureAwait(false);
        }
        catch
        {
            coordinator.Cancel();
            throw;
        }
    }

    private async Task RunPartitionAsync(int partitionIndex, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            DurableRelayLease? lease;
            try
            {
                lease = await _journal
                    .ClaimNextAsync(partitionIndex, _options.WorkerId, _options.LeaseDuration, stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            if (lease is null)
            {
                try
                {
                    await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

                continue;
            }

            await ProcessLeaseAsync(lease).ConfigureAwait(false);
        }
    }

    private async Task ProcessLeaseAsync(DurableRelayLease lease)
    {
        RelayGridPostgresTelemetry.Claimed.Add(1);

        using Activity? activity = RelayGridPostgresTelemetry.Activities.StartActivity(
            "relaygrid.process",
            ActivityKind.Consumer);

        activity?.SetTag("relaygrid.partition", lease.PartitionIndex);
        activity?.SetTag("relaygrid.attempt", lease.AttemptCount);
        activity?.SetTag("relaygrid.fence_token", lease.FenceToken);

        Exception? handlerFailure = null;
        try
        {
            await _handler.HandleAsync(lease.Envelope, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            handlerFailure = exception;
        }

        if (handlerFailure is null)
        {
            await _journal.CompleteAsync(lease, CancellationToken.None).ConfigureAwait(false);
            RelayGridPostgresTelemetry.Completed.Add(1);
            activity?.SetTag("relaygrid.outcome", "completed");
            activity?.SetStatus(ActivityStatusCode.Ok);
            return;
        }

        string failureType = handlerFailure.GetType().FullName ?? handlerFailure.GetType().Name;
        string failureMessage = TruncateFailureMessage(handlerFailure.Message);

        activity?.SetStatus(ActivityStatusCode.Error, failureType);

        if (lease.AttemptCount >= _options.MaxAttempts)
        {
            await _journal
                .DeadLetterAsync(lease, failureType, failureMessage, CancellationToken.None)
                .ConfigureAwait(false);
            RelayGridPostgresTelemetry.DeadLettered.Add(1);
            activity?.SetTag("relaygrid.outcome", "dead_lettered");
            return;
        }

        TimeSpan delay = RetryBackoff.GetDelay(
            lease.AttemptCount,
            _options.RetryBaseDelay,
            _options.RetryMaxDelay);

        await _journal
            .RetryAsync(lease, failureType, failureMessage, delay, CancellationToken.None)
            .ConfigureAwait(false);
        RelayGridPostgresTelemetry.Retried.Add(1);
        activity?.SetTag("relaygrid.outcome", "retried");
    }

    private static string TruncateFailureMessage(string message)
    {
        if (message.Length <= MaxPersistedFailureMessageLength)
        {
            return message;
        }

        return message[..MaxPersistedFailureMessageLength];
    }
}
