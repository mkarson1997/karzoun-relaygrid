using System.Reflection;
using Npgsql;

namespace Karzoun.RelayGrid.Postgres;

public sealed class PostgresRelaySchema(NpgsqlDataSource dataSource)
{
    private const int CurrentVersion = 1;
    private const long AdvisoryLockKey = 7_245_472_310_001L;
    private readonly NpgsqlDataSource _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    public async Task ApplyAsync(CancellationToken cancellationToken = default)
    {
        await using NpgsqlConnection connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using (NpgsqlCommand bootstrap = connection.CreateCommand())
        {
            bootstrap.Transaction = transaction;
            bootstrap.CommandText = """
                SELECT pg_advisory_xact_lock(@lock_key);
                CREATE TABLE IF NOT EXISTS relay_schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                );
                """;
            bootstrap.Parameters.AddWithValue("lock_key", AdvisoryLockKey);
            await bootstrap.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        bool applied;
        await using (NpgsqlCommand check = connection.CreateCommand())
        {
            check.Transaction = transaction;
            check.CommandText = "SELECT EXISTS (SELECT 1 FROM relay_schema_migrations WHERE version = @version);";
            check.Parameters.AddWithValue("version", CurrentVersion);
            applied = (bool)(await check.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("Schema version query returned no value."));
        }

        if (!applied)
        {
            string migration = ReadMigration("Karzoun.RelayGrid.Postgres.Migrations.001_initial.sql");
            await using NpgsqlCommand migrate = connection.CreateCommand();
            migrate.Transaction = transaction;
            migrate.CommandText = migration;
            await migrate.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await using NpgsqlCommand markApplied = connection.CreateCommand();
            markApplied.Transaction = transaction;
            markApplied.CommandText = "INSERT INTO relay_schema_migrations(version) VALUES (@version);";
            markApplied.Parameters.AddWithValue("version", CurrentVersion);
            await markApplied.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static string ReadMigration(string resourceName)
    {
        Assembly assembly = typeof(PostgresRelaySchema).Assembly;
        using Stream stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded migration '{resourceName}' was not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
