using System.Collections.Concurrent;
using System.Text;
using Karzoun.RelayGrid;
using Karzoun.RelayGrid.Postgres;
using Npgsql;

namespace Karzoun.RelayGrid.Postgres.Tests;

internal static class DurableWorkerScenarios
{
    public static async Task SuccessfulProcessingCompletesAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage message = await journal
            .EnqueueAsync(Message("worker-success", "worker-success-key", "p"), 0)
            .ConfigureAwait(false);

        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new DelegateRelayHandler((_, _) =>
        {
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        });

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var worker = new PostgresRelayWorker(journal, handler, Options(0));
        Task run = worker.RunAsync(stop.Token);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await WaitUntilAsync(
            async () => (await journal.GetAsync(message.Id).ConfigureAwait(false))?.State == DurableRelayState.Completed,
            TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        stop.Cancel();
        await run.ConfigureAwait(false);

        DurableRelayMessage completed = Assert.NotNull(await journal.GetAsync(message.Id).ConfigureAwait(false));
        Assert.Equal(DurableRelayState.Completed, completed.State);
        Assert.Equal(1, completed.AttemptCount);
    }

    public static async Task RetryExhaustionDeadLettersAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage message = await journal
            .EnqueueAsync(Message("worker-poison", "worker-poison-key", "p"), 1)
            .ConfigureAwait(false);

        int attempts = 0;
        var handler = new DelegateRelayHandler((_, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("planned durable handler failure");
        });

        PostgresRelayWorkerOptions options = Options(1) with
        {
            MaxAttempts = 2,
            RetryBaseDelay = TimeSpan.FromMilliseconds(10),
            RetryMaxDelay = TimeSpan.FromMilliseconds(10),
        };

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var worker = new PostgresRelayWorker(journal, handler, options);
        Task run = worker.RunAsync(stop.Token);

        await WaitUntilAsync(
            async () => (await journal.GetAsync(message.Id).ConfigureAwait(false))?.State == DurableRelayState.DeadLettered,
            TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        stop.Cancel();
        await run.ConfigureAwait(false);

        DurableRelayMessage deadLettered = Assert.NotNull(await journal.GetAsync(message.Id).ConfigureAwait(false));
        Assert.Equal(DurableRelayState.DeadLettered, deadLettered.State);
        Assert.Equal(2, deadLettered.AttemptCount);
        Assert.Equal(2, attempts);
        Assert.Equal(1L, await journal.CountDeadLettersAsync().ConfigureAwait(false));
    }

    public static async Task SamePartitionWorkerPreservesOrderAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage first = await journal.EnqueueAsync(Message("order-1", "order-1-key", "same"), 4).ConfigureAwait(false);
        DurableRelayMessage second = await journal.EnqueueAsync(Message("order-2", "order-2-key", "same"), 4).ConfigureAwait(false);
        DurableRelayMessage third = await journal.EnqueueAsync(Message("order-3", "order-3-key", "same"), 4).ConfigureAwait(false);

        var observed = new ConcurrentQueue<string>();
        var handler = new DelegateRelayHandler((envelope, _) =>
        {
            observed.Enqueue(envelope.MessageId);
            return ValueTask.CompletedTask;
        });

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var worker = new PostgresRelayWorker(journal, handler, Options(4));
        Task run = worker.RunAsync(stop.Token);

        await WaitUntilAsync(
            async () =>
                (await journal.GetAsync(first.Id).ConfigureAwait(false))?.State == DurableRelayState.Completed &&
                (await journal.GetAsync(second.Id).ConfigureAwait(false))?.State == DurableRelayState.Completed &&
                (await journal.GetAsync(third.Id).ConfigureAwait(false))?.State == DurableRelayState.Completed,
            TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        stop.Cancel();
        await run.ConfigureAwait(false);

        Assert.Equal("order-1,order-2,order-3", string.Join(',', observed));
    }

    public static async Task GracefulStopDoesNotCancelClaimedHandlerAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage message = await journal
            .EnqueueAsync(Message("graceful", "graceful-key", "p"), 2)
            .ConfigureAwait(false);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        bool handlerTokenCanBeCanceled = true;

        var handler = new DelegateRelayHandler(async (_, cancellationToken) =>
        {
            handlerTokenCanBeCanceled = cancellationToken.CanBeCanceled;
            entered.TrySetResult();
            await release.Task.ConfigureAwait(false);
        });

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var worker = new PostgresRelayWorker(journal, handler, Options(2));
        Task run = worker.RunAsync(stop.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        stop.Cancel();
        await Task.Delay(50).ConfigureAwait(false);

        Assert.True(!run.IsCompleted, "Worker stop must wait for an already claimed handler to finish.");
        Assert.True(!handlerTokenCanBeCanceled, "Stop-wait cancellation must not be forwarded into a claimed handler.");

        release.TrySetResult();
        await run.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        DurableRelayMessage completed = Assert.NotNull(await journal.GetAsync(message.Id).ConfigureAwait(false));
        Assert.Equal(DurableRelayState.Completed, completed.State);
    }

    private static PostgresRelayWorkerOptions Options(int partition)
    {
        return new PostgresRelayWorkerOptions
        {
            WorkerId = $"test-worker-{partition}-{Guid.NewGuid():N}",
            Partitions = new[] { partition },
            LeaseDuration = TimeSpan.FromSeconds(2),
            PollInterval = TimeSpan.FromMilliseconds(5),
            MaxAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(10),
            RetryMaxDelay = TimeSpan.FromMilliseconds(50),
        };
    }

    private static async Task ResetAsync(NpgsqlDataSource dataSource)
    {
        var schema = new PostgresRelaySchema(dataSource);
        await schema.ApplyAsync().ConfigureAwait(false);
        await using NpgsqlCommand command = dataSource.CreateCommand(
            "TRUNCATE TABLE relay_dead_letters, relay_messages RESTART IDENTITY CASCADE;");
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static RelayEnvelope Message(string id, string idempotencyKey, string partitionKey)
    {
        return new RelayEnvelope(id, idempotencyKey, partitionKey, Encoding.UTF8.GetBytes(id));
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(20).ConfigureAwait(false);
        }

        throw new TimeoutException($"Condition did not become true within {timeout}.");
    }

    private sealed class DelegateRelayHandler(
        Func<RelayEnvelope, CancellationToken, ValueTask> callback) : IRelayHandler
    {
        private readonly Func<RelayEnvelope, CancellationToken, ValueTask> _callback =
            callback ?? throw new ArgumentNullException(nameof(callback));

        public ValueTask HandleAsync(RelayEnvelope envelope, CancellationToken cancellationToken)
        {
            return _callback(envelope, cancellationToken);
        }
    }
}
