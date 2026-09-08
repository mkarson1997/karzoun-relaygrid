using System.Text;
using Karzoun.RelayGrid;
using Karzoun.RelayGrid.Postgres;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Karzoun.RelayGrid.Postgres.Tests;

internal static class Program
{
    public static async Task<int> Main()
    {
        await using var container = new PostgreSqlBuilder("postgres:17-alpine")
            .WithDatabase("relaygrid")
            .WithUsername("relaygrid")
            .WithPassword("relaygrid-test")
            .Build();

        await container.StartAsync().ConfigureAwait(false);
        string connectionString = container.GetConnectionString();

        var tests = new (string Name, Func<string, Task> Test)[]
        {
            ("schema application is idempotent", SchemaApplicationIsIdempotentAsync),
            ("journal survives data-source restart", JournalSurvivesRestartAsync),
            ("lease fencing preserves partition head", LeaseFencingPreservesPartitionHeadAsync),
            ("retry delay blocks partition overtaking", RetryDelayBlocksOvertakingAsync),
            ("dead-letter transaction unblocks next item", DeadLetterUnblocksNextAsync),
            ("expired lease is reclaimed after worker crash", ExpiredLeaseIsReclaimedAsync),
        };

        int failed = 0;
        foreach ((string name, Func<string, Task> test) in tests)
        {
            try
            {
                await test(connectionString).ConfigureAwait(false);
                Console.WriteLine($"PASS  {name}");
            }
            catch (Exception exception)
            {
                failed++;
                Console.Error.WriteLine($"FAIL  {name}: {exception}");
            }
        }

        Console.WriteLine($"RelayGrid PostgreSQL suite: {tests.Length - failed}/{tests.Length} passed");
        return failed == 0 ? 0 : 1;
    }

    private static async Task SchemaApplicationIsIdempotentAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        var schema = new PostgresRelaySchema(dataSource);
        await schema.ApplyAsync().ConfigureAwait(false);
        await schema.ApplyAsync().ConfigureAwait(false);

        await using NpgsqlCommand command = dataSource.CreateCommand("SELECT COUNT(*) FROM relay_schema_migrations WHERE version = 1;");
        long count = Convert.ToInt64(await command.ExecuteScalarAsync().ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(1L, count);
    }

    private static async Task JournalSurvivesRestartAsync(string connectionString)
    {
        Guid id;
        await using (NpgsqlDataSource firstDataSource = NpgsqlDataSource.Create(connectionString))
        {
            await ResetAsync(firstDataSource).ConfigureAwait(false);
            var journal = new PostgresRelayJournal(firstDataSource);
            DurableRelayMessage stored = await journal.EnqueueAsync(Message("persisted", "persist-key", "account-1"), 2).ConfigureAwait(false);
            id = stored.Id;
        }

        await using NpgsqlDataSource secondDataSource = NpgsqlDataSource.Create(connectionString);
        var restartedJournal = new PostgresRelayJournal(secondDataSource);
        DurableRelayMessage? restored = await restartedJournal.GetAsync(id).ConfigureAwait(false);

        Assert.NotNull(restored);
        Assert.Equal("persisted", restored!.Envelope.MessageId);
        Assert.Equal(DurableRelayState.Pending, restored.State);
        Assert.Equal(2, restored.PartitionIndex);
    }

    private static async Task LeaseFencingPreservesPartitionHeadAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage first = await journal.EnqueueAsync(Message("first", "first-key", "same"), 0).ConfigureAwait(false);
        DurableRelayMessage second = await journal.EnqueueAsync(Message("second", "second-key", "same"), 0).ConfigureAwait(false);

        DurableRelayLease firstLease = Assert.NotNull(await journal.ClaimNextAsync(0, "worker-a", TimeSpan.FromMilliseconds(250)).ConfigureAwait(false));
        Assert.Equal(first.Id, firstLease.Id);
        Assert.Null(await journal.ClaimNextAsync(0, "worker-b", TimeSpan.FromSeconds(1)).ConfigureAwait(false));

        await Task.Delay(400).ConfigureAwait(false);
        DurableRelayLease reclaimed = Assert.NotNull(await journal.ClaimNextAsync(0, "worker-b", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        Assert.Equal(first.Id, reclaimed.Id);
        Assert.True(reclaimed.FenceToken > firstLease.FenceToken, "Fence token must increase when an expired lease is reclaimed.");

        await Assert.ThrowsAsync<StaleRelayLeaseException>(() => journal.CompleteAsync(firstLease)).ConfigureAwait(false);
        await journal.CompleteAsync(reclaimed).ConfigureAwait(false);

        DurableRelayLease secondLease = Assert.NotNull(await journal.ClaimNextAsync(0, "worker-b", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        Assert.Equal(second.Id, secondLease.Id);
        await journal.CompleteAsync(secondLease).ConfigureAwait(false);
    }

    private static async Task RetryDelayBlocksOvertakingAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage first = await journal.EnqueueAsync(Message("retry-first", "retry-first-key", "p"), 3).ConfigureAwait(false);
        DurableRelayMessage second = await journal.EnqueueAsync(Message("retry-second", "retry-second-key", "p"), 3).ConfigureAwait(false);

        DurableRelayLease lease = Assert.NotNull(await journal.ClaimNextAsync(3, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        await journal.RetryAsync(lease, "PlannedFailure", "retry later", TimeSpan.FromMilliseconds(300)).ConfigureAwait(false);

        Assert.Null(await journal.ClaimNextAsync(3, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        await Task.Delay(450).ConfigureAwait(false);

        DurableRelayLease retried = Assert.NotNull(await journal.ClaimNextAsync(3, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        Assert.Equal(first.Id, retried.Id);
        Assert.Equal(2, retried.AttemptCount);
        await journal.CompleteAsync(retried).ConfigureAwait(false);

        DurableRelayLease next = Assert.NotNull(await journal.ClaimNextAsync(3, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        Assert.Equal(second.Id, next.Id);
        await journal.CompleteAsync(next).ConfigureAwait(false);
    }

    private static async Task DeadLetterUnblocksNextAsync(string connectionString)
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        await ResetAsync(dataSource).ConfigureAwait(false);
        var journal = new PostgresRelayJournal(dataSource);

        DurableRelayMessage poison = await journal.EnqueueAsync(Message("poison", "poison-key", "p"), 1).ConfigureAwait(false);
        DurableRelayMessage healthy = await journal.EnqueueAsync(Message("healthy", "healthy-key", "p"), 1).ConfigureAwait(false);

        DurableRelayLease poisonLease = Assert.NotNull(await journal.ClaimNextAsync(1, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        await journal.DeadLetterAsync(poisonLease, "PoisonMessage", "planned poison").ConfigureAwait(false);

        DurableRelayMessage poisonState = Assert.NotNull(await journal.GetAsync(poison.Id).ConfigureAwait(false));
        Assert.Equal(DurableRelayState.DeadLettered, poisonState.State);
        Assert.Equal(1L, await journal.CountDeadLettersAsync().ConfigureAwait(false));

        DurableRelayLease healthyLease = Assert.NotNull(await journal.ClaimNextAsync(1, "worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));
        Assert.Equal(healthy.Id, healthyLease.Id);
        await journal.CompleteAsync(healthyLease).ConfigureAwait(false);
    }

    private static async Task ExpiredLeaseIsReclaimedAsync(string connectionString)
    {
        Guid id;
        DurableRelayLease crashedLease;

        await using (NpgsqlDataSource firstDataSource = NpgsqlDataSource.Create(connectionString))
        {
            await ResetAsync(firstDataSource).ConfigureAwait(false);
            var firstJournal = new PostgresRelayJournal(firstDataSource);
            DurableRelayMessage message = await firstJournal.EnqueueAsync(Message("crash", "crash-key", "p"), 5).ConfigureAwait(false);
            id = message.Id;
            crashedLease = Assert.NotNull(await firstJournal.ClaimNextAsync(5, "crashed-worker", TimeSpan.FromMilliseconds(200)).ConfigureAwait(false));
        }

        await Task.Delay(350).ConfigureAwait(false);

        await using NpgsqlDataSource restartedDataSource = NpgsqlDataSource.Create(connectionString);
        var restartedJournal = new PostgresRelayJournal(restartedDataSource);
        DurableRelayLease recovered = Assert.NotNull(await restartedJournal.ClaimNextAsync(5, "recovery-worker", TimeSpan.FromSeconds(1)).ConfigureAwait(false));

        Assert.Equal(id, recovered.Id);
        Assert.True(recovered.FenceToken > crashedLease.FenceToken, "Crash recovery must advance the fence token.");
        await Assert.ThrowsAsync<StaleRelayLeaseException>(() => restartedJournal.RetryAsync(crashedLease, "LateWorker", "must be fenced", TimeSpan.Zero)).ConfigureAwait(false);
        await restartedJournal.CompleteAsync(recovered).ConfigureAwait(false);
    }

    private static async Task ResetAsync(NpgsqlDataSource dataSource)
    {
        var schema = new PostgresRelaySchema(dataSource);
        await schema.ApplyAsync().ConfigureAwait(false);
        await using NpgsqlCommand command = dataSource.CreateCommand("TRUNCATE TABLE relay_dead_letters, relay_messages RESTART IDENTITY CASCADE;");
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static RelayEnvelope Message(string id, string idempotencyKey, string partitionKey)
    {
        return new RelayEnvelope(id, idempotencyKey, partitionKey, Encoding.UTF8.GetBytes(id));
    }
}

internal static class Assert
{
    public static void True(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    public static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new InvalidOperationException($"Expected '{expected}', got '{actual}'.");
        }
    }

    public static T NotNull<T>(T? value)
        where T : class
    {
        return value ?? throw new InvalidOperationException("Expected a non-null value.");
    }

    public static void Null<T>(T? value)
        where T : class
    {
        if (value is not null)
        {
            throw new InvalidOperationException($"Expected null, got '{value}'.");
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
