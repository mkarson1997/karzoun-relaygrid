using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Karzoun.RelayGrid;

namespace Karzoun.RelayGrid.Tests;

internal static class Program
{
    private static readonly (string Name, Func<Task> Test)[] Tests =
    [
        ("same partition preserves FIFO", SamePartitionPreservesFifoAsync),
        ("different partitions execute concurrently", DifferentPartitionsExecuteConcurrentlyAsync),
        ("bounded partition applies backpressure", BoundedPartitionAppliesBackpressureAsync),
        ("retry succeeds and exhaustion dead-letters", RetryAndDeadLetterAsync),
        ("successful idempotency key suppresses duplicates", IdempotencySuppressesSuccessfulDuplicatesAsync),
        ("cancelled stop wait does not cancel accepted work", CancelledStopWaitDoesNotCancelAcceptedWorkAsync),
        ("multi-partition stress preserves key ordering", MultiPartitionStressPreservesOrderingAsync),
        ("retry backoff is deterministic and capped", RetryBackoffIsDeterministicAsync),
    ];

    public static async Task<int> Main()
    {
        int failed = 0;
        var stopwatch = Stopwatch.StartNew();

        foreach ((string name, Func<Task> test) in Tests)
        {
            try
            {
                await test().ConfigureAwait(false);
                Console.WriteLine($"PASS  {name}");
            }
            catch (Exception exception)
            {
                failed++;
                Console.Error.WriteLine($"FAIL  {name}: {exception}");
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"RelayGrid deterministic suite: {Tests.Length - failed}/{Tests.Length} passed in {stopwatch.ElapsedMilliseconds} ms");
        return failed == 0 ? 0 : 1;
    }

    private static async Task SamePartitionPreservesFifoAsync()
    {
        var observed = new ConcurrentQueue<int>();
        var handler = new DelegateHandler((envelope, _) =>
        {
            observed.Enqueue(int.Parse(envelope.MessageId, System.Globalization.CultureInfo.InvariantCulture));
            return ValueTask.CompletedTask;
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 4, capacity: 64);
        for (int index = 0; index < 50; index++)
        {
            await runtime.PublishAsync(Message(index.ToString(System.Globalization.CultureInfo.InvariantCulture), $"id-{index}", "customer-42")).ConfigureAwait(false);
        }

        await runtime.StopAsync().ConfigureAwait(false);
        AssertEx.SequenceEqual(Enumerable.Range(0, 50), observed);
        AssertEx.Equal(50L, runtime.Snapshot.Succeeded);
    }

    private static async Task DifferentPartitionsExecuteConcurrentlyAsync()
    {
        string firstKey = "partition-a";
        int firstPartition = RelayPartitioning.GetPartition(firstKey, 4);
        string secondKey = Enumerable.Range(0, 1000)
            .Select(index => $"partition-{index}")
            .First(key => RelayPartitioning.GetPartition(key, 4) != firstPartition);

        var bothInside = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int active = 0;
        int maxActive = 0;

        var handler = new DelegateHandler(async (_, _) =>
        {
            int current = Interlocked.Increment(ref active);
            UpdateMax(ref maxActive, current);
            if (current >= 2)
            {
                bothInside.TrySetResult();
            }

            await release.Task.ConfigureAwait(false);
            Interlocked.Decrement(ref active);
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 4, capacity: 8);
        await runtime.PublishAsync(Message("a", "id-a", firstKey)).ConfigureAwait(false);
        await runtime.PublishAsync(Message("b", "id-b", secondKey)).ConfigureAwait(false);

        await bothInside.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        release.TrySetResult();
        await runtime.StopAsync().ConfigureAwait(false);

        AssertEx.True(maxActive >= 2, "Expected at least two partitions to execute concurrently.");
    }

    private static async Task BoundedPartitionAppliesBackpressureAsync()
    {
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;

        var handler = new DelegateHandler(async (_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                firstStarted.TrySetResult();
                await release.Task.ConfigureAwait(false);
            }
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 1, capacity: 1);
        await runtime.PublishAsync(Message("1", "id-1", "same")).ConfigureAwait(false);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await runtime.PublishAsync(Message("2", "id-2", "same")).ConfigureAwait(false);

        bool accepted = runtime.TryPublish(Message("3", "id-3", "same"), out _);
        AssertEx.False(accepted, "Third message should be rejected by TryPublish while the single-slot queue is full.");

        release.TrySetResult();
        await runtime.StopAsync().ConfigureAwait(false);
        AssertEx.Equal(2L, runtime.Snapshot.Accepted);
        AssertEx.Equal(2L, runtime.Snapshot.Succeeded);
    }

    private static async Task RetryAndDeadLetterAsync()
    {
        var attempts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var deadLetters = new RecordingDeadLetterSink();
        var delay = new RecordingDelay();

        var handler = new DelegateHandler((envelope, _) =>
        {
            int attempt = attempts.AddOrUpdate(envelope.MessageId, 1, static (_, current) => current + 1);
            if (envelope.MessageId == "eventual" && attempt >= 3)
            {
                return ValueTask.CompletedTask;
            }

            throw new InvalidOperationException($"planned failure {attempt}");
        });

        var options = new RelayGridOptions
        {
            PartitionCount = 1,
            PartitionCapacity = 8,
            MaxAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(10),
            RetryMaxDelay = TimeSpan.FromMilliseconds(100),
        };

        await using var runtime = new RelayGridRuntime(options, handler, deadLetters, delay);
        await runtime.PublishAsync(Message("eventual", "eventual-key", "p")).ConfigureAwait(false);
        await runtime.PublishAsync(Message("poison", "poison-key", "p")).ConfigureAwait(false);
        await runtime.StopAsync().ConfigureAwait(false);

        AssertEx.Equal(3, attempts["eventual"]);
        AssertEx.Equal(3, attempts["poison"]);
        AssertEx.Equal(4L, runtime.Snapshot.Retries);
        AssertEx.Equal(1L, runtime.Snapshot.Succeeded);
        AssertEx.Equal(1L, runtime.Snapshot.DeadLettered);
        AssertEx.Equal(4, delay.Delays.Count);
        AssertEx.Equal(1, deadLetters.Records.Count);
        AssertEx.Equal(3, deadLetters.Records.Single().Attempts);
    }

    private static async Task IdempotencySuppressesSuccessfulDuplicatesAsync()
    {
        int calls = 0;
        var handler = new DelegateHandler((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return ValueTask.CompletedTask;
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 2, capacity: 8);
        await runtime.PublishAsync(Message("first", "same-idempotency", "customer")).ConfigureAwait(false);
        await runtime.PublishAsync(Message("duplicate", "same-idempotency", "customer")).ConfigureAwait(false);
        await runtime.StopAsync().ConfigureAwait(false);

        AssertEx.Equal(1, calls);
        AssertEx.Equal(2L, runtime.Snapshot.Accepted);
        AssertEx.Equal(1L, runtime.Snapshot.Succeeded);
        AssertEx.Equal(1L, runtime.Snapshot.DuplicateSuppressed);
    }

    private static async Task CancelledStopWaitDoesNotCancelAcceptedWorkAsync()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var handler = new DelegateHandler(async (_, _) =>
        {
            started.TrySetResult();
            await release.Task.ConfigureAwait(false);
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 1, capacity: 4);
        await runtime.PublishAsync(Message("accepted", "accepted-key", "p")).ConfigureAwait(false);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await AssertEx.ThrowsAsync<OperationCanceledException>(() => runtime.StopAsync(cancellation.Token)).ConfigureAwait(false);

        release.TrySetResult();
        await runtime.StopAsync().ConfigureAwait(false);
        AssertEx.Equal(1L, runtime.Snapshot.Succeeded);

        await AssertEx.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await runtime.PublishAsync(Message("late", "late-key", "p")).ConfigureAwait(false);
        }).ConfigureAwait(false);
    }

    private static async Task MultiPartitionStressPreservesOrderingAsync()
    {
        const int logicalKeys = 16;
        const int messagesPerKey = 75;
        var observed = new ConcurrentDictionary<string, ConcurrentQueue<int>>(StringComparer.Ordinal);

        var handler = new DelegateHandler((envelope, _) =>
        {
            string[] parts = envelope.MessageId.Split(':', 2);
            observed.GetOrAdd(parts[0], static _ => new ConcurrentQueue<int>())
                .Enqueue(int.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture));
            return ValueTask.CompletedTask;
        });

        await using var runtime = CreateRuntime(handler, new RecordingDeadLetterSink(), partitionCount: 8, capacity: 256);

        for (int sequence = 0; sequence < messagesPerKey; sequence++)
        {
            for (int keyIndex = 0; keyIndex < logicalKeys; keyIndex++)
            {
                string key = $"key-{keyIndex}";
                await runtime.PublishAsync(Message($"{key}:{sequence}", $"{key}-id-{sequence}", key)).ConfigureAwait(false);
            }
        }

        await runtime.StopAsync().ConfigureAwait(false);

        for (int keyIndex = 0; keyIndex < logicalKeys; keyIndex++)
        {
            string key = $"key-{keyIndex}";
            AssertEx.SequenceEqual(Enumerable.Range(0, messagesPerKey), observed[key]);
        }

        AssertEx.Equal((long)logicalKeys * messagesPerKey, runtime.Snapshot.Succeeded);
    }

    private static Task RetryBackoffIsDeterministicAsync()
    {
        TimeSpan baseDelay = TimeSpan.FromMilliseconds(10);
        TimeSpan maxDelay = TimeSpan.FromMilliseconds(50);
        AssertEx.Equal(TimeSpan.FromMilliseconds(10), RetryBackoff.GetDelay(1, baseDelay, maxDelay));
        AssertEx.Equal(TimeSpan.FromMilliseconds(20), RetryBackoff.GetDelay(2, baseDelay, maxDelay));
        AssertEx.Equal(TimeSpan.FromMilliseconds(40), RetryBackoff.GetDelay(3, baseDelay, maxDelay));
        AssertEx.Equal(TimeSpan.FromMilliseconds(50), RetryBackoff.GetDelay(4, baseDelay, maxDelay));
        AssertEx.Equal(TimeSpan.FromMilliseconds(50), RetryBackoff.GetDelay(30, baseDelay, maxDelay));
        return Task.CompletedTask;
    }

    private static RelayGridRuntime CreateRuntime(
        IRelayHandler handler,
        IDeadLetterSink deadLetterSink,
        int partitionCount,
        int capacity)
    {
        return new RelayGridRuntime(
            new RelayGridOptions
            {
                PartitionCount = partitionCount,
                PartitionCapacity = capacity,
                MaxAttempts = 3,
                RetryBaseDelay = TimeSpan.Zero,
                RetryMaxDelay = TimeSpan.Zero,
            },
            handler,
            deadLetterSink,
            new RecordingDelay());
    }

    private static RelayEnvelope Message(string messageId, string idempotencyKey, string partitionKey)
    {
        return new RelayEnvelope(messageId, idempotencyKey, partitionKey, Encoding.UTF8.GetBytes(messageId));
    }

    private static void UpdateMax(ref int target, int value)
    {
        while (true)
        {
            int current = Volatile.Read(ref target);
            if (current >= value || Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }

    private sealed class DelegateHandler(Func<RelayEnvelope, CancellationToken, ValueTask> callback) : IRelayHandler
    {
        public ValueTask HandleAsync(RelayEnvelope envelope, CancellationToken cancellationToken) => callback(envelope, cancellationToken);
    }

    private sealed class RecordingDeadLetterSink : IDeadLetterSink
    {
        public ConcurrentQueue<DeadLetterRecord> Records { get; } = new();

        public ValueTask WriteAsync(DeadLetterRecord record, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Records.Enqueue(record);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingDelay : IRelayDelay
    {
        public ConcurrentQueue<TimeSpan> Delays { get; } = new();

        public ValueTask DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Delays.Enqueue(delay);
            return ValueTask.CompletedTask;
        }
    }
}

internal static class AssertEx
{
    public static void True(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    public static void False(bool condition, string message) => True(!condition, message);

    public static void Equal<T>(T expected, T actual)
        where T : IEquatable<T>
    {
        if (!expected.Equals(actual))
        {
            throw new InvalidOperationException($"Expected '{expected}', got '{actual}'.");
        }
    }

    public static void SequenceEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        if (!expected.SequenceEqual(actual))
        {
            throw new InvalidOperationException("Sequences differ.");
        }
    }

    public static async Task ThrowsAsync<TException>(Func<Task> action)
        where TException : Exception
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (TException)
        {
            return;
        }

        throw new InvalidOperationException($"Expected exception {typeof(TException).Name}.");
    }
}
