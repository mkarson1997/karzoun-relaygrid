using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace Karzoun.RelayGrid;

public sealed class RelayGridRuntime : IAsyncDisposable
{
    private const int Accepting = 0;
    private const int Stopping = 1;
    private const int Stopped = 2;
    private const int Faulted = 3;

    private readonly RelayGridOptions _options;
    private readonly IRelayHandler _handler;
    private readonly IDeadLetterSink _deadLetterSink;
    private readonly IRelayDelay _delay;
    private readonly Channel<RelayEnvelope>[] _partitions;
    private readonly Task[] _workers;
    private readonly IdempotencyCoordinator _idempotency = new();

    private ExceptionDispatchInfo? _terminalFault;
    private int _state;
    private long _accepted;
    private long _succeeded;
    private long _duplicates;
    private long _retries;
    private long _deadLettered;
    private long _activeHandlers;

    public RelayGridRuntime(
        RelayGridOptions options,
        IRelayHandler handler,
        IDeadLetterSink deadLetterSink,
        IRelayDelay? delay = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(deadLetterSink);

        options.Validate();
        _options = options;
        _handler = handler;
        _deadLetterSink = deadLetterSink;
        _delay = delay ?? new SystemRelayDelay();

        _partitions = new Channel<RelayEnvelope>[options.PartitionCount];
        _workers = new Task[options.PartitionCount];

        for (int index = 0; index < options.PartitionCount; index++)
        {
            _partitions[index] = Channel.CreateBounded<RelayEnvelope>(new BoundedChannelOptions(options.PartitionCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });

            int partitionIndex = index;
            _workers[index] = Task.Run(() => RunPartitionAsync(partitionIndex));
        }
    }

    public RelayGridSnapshot Snapshot => new(
        Interlocked.Read(ref _accepted),
        Interlocked.Read(ref _succeeded),
        Interlocked.Read(ref _duplicates),
        Interlocked.Read(ref _retries),
        Interlocked.Read(ref _deadLettered),
        Interlocked.Read(ref _activeHandlers));

    public async ValueTask<RelayReceipt> PublishAsync(
        RelayEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        EnsureAccepting();

        int partition = RelayPartitioning.GetPartition(envelope.PartitionKey, _partitions.Length);

        try
        {
            await _partitions[partition].Writer.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException exception)
        {
            throw new InvalidOperationException("RelayGrid is no longer accepting messages.", exception);
        }

        Interlocked.Increment(ref _accepted);
        return new RelayReceipt(envelope.MessageId, partition);
    }

    public bool TryPublish(RelayEnvelope envelope, out RelayReceipt receipt)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        receipt = default;

        if (Volatile.Read(ref _state) != Accepting)
        {
            return false;
        }

        int partition = RelayPartitioning.GetPartition(envelope.PartitionKey, _partitions.Length);
        if (!_partitions[partition].Writer.TryWrite(envelope))
        {
            return false;
        }

        Interlocked.Increment(ref _accepted);
        receipt = new RelayReceipt(envelope.MessageId, partition);
        return true;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        int previous = Interlocked.CompareExchange(ref _state, Stopping, Accepting);
        if (previous == Accepting)
        {
            CompleteWriters();
        }
        else if (previous == Stopped)
        {
            return;
        }

        await Task.WhenAll(_workers).WaitAsync(cancellationToken).ConfigureAwait(false);

        if (Volatile.Read(ref _state) != Faulted)
        {
            Interlocked.CompareExchange(ref _state, Stopped, Stopping);
        }

        _terminalFault?.Throw();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async Task RunPartitionAsync(int partitionIndex)
    {
        try
        {
            await foreach (RelayEnvelope envelope in _partitions[partitionIndex].Reader.ReadAllAsync().ConfigureAwait(false))
            {
                RelayProcessingResult result = await ProcessAsync(envelope).ConfigureAwait(false);
                Record(result);
            }
        }
        catch (Exception exception)
        {
            Fault(exception);
            throw;
        }
    }

    private async ValueTask<RelayProcessingResult> ProcessAsync(RelayEnvelope envelope)
    {
        IdempotencyCoordinator.IdempotencyLease? lease = await _idempotency.AcquireAsync(envelope.IdempotencyKey).ConfigureAwait(false);
        if (lease is null)
        {
            return new RelayProcessingResult(envelope.MessageId, RelayDisposition.DuplicateSuppressed, 0);
        }

        Exception? lastFailure = null;
        int attempts = 0;

        try
        {
            for (int attempt = 1; attempt <= _options.MaxAttempts; attempt++)
            {
                attempts = attempt;
                Interlocked.Increment(ref _activeHandlers);

                try
                {
                    await _handler.HandleAsync(envelope, CancellationToken.None).ConfigureAwait(false);
                    lease.Complete(succeeded: true);
                    return new RelayProcessingResult(envelope.MessageId, RelayDisposition.Succeeded, attempt);
                }
                catch (Exception exception)
                {
                    lastFailure = exception;
                }
                finally
                {
                    Interlocked.Decrement(ref _activeHandlers);
                }

                if (attempt < _options.MaxAttempts)
                {
                    Interlocked.Increment(ref _retries);
                    TimeSpan delay = RetryBackoff.GetDelay(attempt, _options.RetryBaseDelay, _options.RetryMaxDelay);
                    await _delay.DelayAsync(delay, CancellationToken.None).ConfigureAwait(false);
                }
            }

            string failureType = lastFailure?.GetType().FullName ?? "UnknownFailure";
            string failureMessage = lastFailure?.Message ?? "Handler failed without an exception payload.";

            await _deadLetterSink.WriteAsync(
                new DeadLetterRecord(envelope, attempts, failureType, failureMessage),
                CancellationToken.None).ConfigureAwait(false);

            lease.Complete(succeeded: false);
            return new RelayProcessingResult(
                envelope.MessageId,
                RelayDisposition.DeadLettered,
                attempts,
                failureType,
                failureMessage);
        }
        catch
        {
            lease.Complete(succeeded: false);
            throw;
        }
    }

    private void Record(RelayProcessingResult result)
    {
        switch (result.Disposition)
        {
            case RelayDisposition.Succeeded:
                Interlocked.Increment(ref _succeeded);
                break;
            case RelayDisposition.DuplicateSuppressed:
                Interlocked.Increment(ref _duplicates);
                break;
            case RelayDisposition.DeadLettered:
                Interlocked.Increment(ref _deadLettered);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Disposition, "Unknown relay disposition.");
        }
    }

    private void EnsureAccepting()
    {
        if (Volatile.Read(ref _state) != Accepting)
        {
            throw new InvalidOperationException("RelayGrid is no longer accepting messages.");
        }
    }

    private void CompleteWriters(Exception? exception = null)
    {
        foreach (Channel<RelayEnvelope> partition in _partitions)
        {
            partition.Writer.TryComplete(exception);
        }
    }

    private void Fault(Exception exception)
    {
        if (Interlocked.Exchange(ref _state, Faulted) != Faulted)
        {
            _terminalFault = ExceptionDispatchInfo.Capture(exception);
            CompleteWriters(exception);
        }
    }
}
