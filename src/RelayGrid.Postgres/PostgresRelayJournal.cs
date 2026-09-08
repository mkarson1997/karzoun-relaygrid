using Karzoun.RelayGrid;
using Npgsql;

namespace Karzoun.RelayGrid.Postgres;

public sealed class PostgresRelayJournal(NpgsqlDataSource dataSource)
{
    private const string SelectColumns = """
        id, sequence, message_id, idempotency_key, partition_key, partition_index,
        payload, state, attempt_count, available_at, lease_owner, fence_token,
        lease_until, created_at, last_error_type, last_error_message
        """;

    private const string SelectMessageAliasColumns = """
        m.id, m.sequence, m.message_id, m.idempotency_key, m.partition_key, m.partition_index,
        m.payload, m.state, m.attempt_count, m.available_at, m.lease_owner, m.fence_token,
        m.lease_until, m.created_at, m.last_error_type, m.last_error_message
        """;

    private readonly NpgsqlDataSource _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));

    public async Task<DurableRelayMessage> EnqueueAsync(
        RelayEnvelope envelope,
        int partitionIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentOutOfRangeException.ThrowIfNegative(partitionIndex);

        Guid id = Guid.NewGuid();
        await using NpgsqlCommand command = _dataSource.CreateCommand($"""
            INSERT INTO relay_messages(
                id, message_id, idempotency_key, partition_key, partition_index, payload)
            VALUES (
                @id, @message_id, @idempotency_key, @partition_key, @partition_index, @payload)
            RETURNING {SelectColumns};
            """);

        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("message_id", envelope.MessageId);
        command.Parameters.AddWithValue("idempotency_key", envelope.IdempotencyKey);
        command.Parameters.AddWithValue("partition_key", envelope.PartitionKey);
        command.Parameters.AddWithValue("partition_index", partitionIndex);
        command.Parameters.AddWithValue("payload", envelope.Payload.ToArray());

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("PostgreSQL did not return the enqueued message.");
        }

        return ReadMessage(reader);
    }

    public async Task<DurableRelayMessage?> GetAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using NpgsqlCommand command = _dataSource.CreateCommand($"SELECT {SelectColumns} FROM relay_messages WHERE id = @id;");
        command.Parameters.AddWithValue("id", id);

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ? ReadMessage(reader) : null;
    }

    public async Task<DurableRelayLease?> ClaimNextAsync(
        int partitionIndex,
        string workerId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitionIndex);
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
        if (leaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        }

        await using NpgsqlCommand command = _dataSource.CreateCommand($"""
            WITH head AS (
                SELECT id
                FROM relay_messages
                WHERE partition_index = @partition_index
                  AND state IN (0, 1)
                ORDER BY sequence
                LIMIT 1
            ), candidate AS (
                SELECT m.id
                FROM relay_messages AS m
                JOIN head AS h ON h.id = m.id
                WHERE (m.state = 0 AND m.available_at <= clock_timestamp())
                   OR (m.state = 1 AND m.lease_until <= clock_timestamp())
                FOR UPDATE OF m SKIP LOCKED
            )
            UPDATE relay_messages AS m
            SET state = 1,
                attempt_count = m.attempt_count + 1,
                lease_owner = @worker_id,
                fence_token = m.fence_token + 1,
                lease_until = clock_timestamp() + @lease_duration
            FROM candidate
            WHERE m.id = candidate.id
            RETURNING {SelectMessageAliasColumns};
            """);

        command.Parameters.AddWithValue("partition_index", partitionIndex);
        command.Parameters.AddWithValue("worker_id", workerId);
        command.Parameters.AddWithValue("lease_duration", leaseDuration);

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        DurableRelayMessage message = ReadMessage(reader);
        if (message.LeaseOwner is null || message.LeaseUntilUtc is null)
        {
            throw new InvalidOperationException("Claimed row did not contain lease metadata.");
        }

        return new DurableRelayLease(
            message.Id,
            message.Sequence,
            message.Envelope,
            message.PartitionIndex,
            message.AttemptCount,
            message.LeaseOwner,
            message.FenceToken,
            message.LeaseUntilUtc.Value);
    }

    public async Task CompleteAsync(DurableRelayLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        await using NpgsqlCommand command = _dataSource.CreateCommand("""
            UPDATE relay_messages
            SET state = 2,
                lease_owner = NULL,
                lease_until = NULL,
                completed_at = clock_timestamp()
            WHERE id = @id
              AND state = 1
              AND lease_owner = @worker_id
              AND fence_token = @fence_token;
            """);

        AddLeaseIdentity(command, lease);
        await EnsureTransitionAsync(command, lease, cancellationToken).ConfigureAwait(false);
    }

    public async Task RetryAsync(
        DurableRelayLease lease,
        string failureType,
        string failureMessage,
        TimeSpan delay,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentException.ThrowIfNullOrWhiteSpace(failureType);
        ArgumentNullException.ThrowIfNull(failureMessage);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay));
        }

        await using NpgsqlCommand command = _dataSource.CreateCommand("""
            UPDATE relay_messages
            SET state = 0,
                available_at = clock_timestamp() + @delay,
                lease_owner = NULL,
                lease_until = NULL,
                last_error_type = @failure_type,
                last_error_message = @failure_message
            WHERE id = @id
              AND state = 1
              AND lease_owner = @worker_id
              AND fence_token = @fence_token;
            """);

        AddLeaseIdentity(command, lease);
        command.Parameters.AddWithValue("delay", delay);
        command.Parameters.AddWithValue("failure_type", failureType);
        command.Parameters.AddWithValue("failure_message", failureMessage);
        await EnsureTransitionAsync(command, lease, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeadLetterAsync(
        DurableRelayLease lease,
        string failureType,
        string failureMessage,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentException.ThrowIfNullOrWhiteSpace(failureType);
        ArgumentNullException.ThrowIfNull(failureMessage);

        await using NpgsqlConnection connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using (NpgsqlCommand update = connection.CreateCommand())
        {
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE relay_messages
                SET state = 3,
                    lease_owner = NULL,
                    lease_until = NULL,
                    completed_at = clock_timestamp(),
                    last_error_type = @failure_type,
                    last_error_message = @failure_message
                WHERE id = @id
                  AND state = 1
                  AND lease_owner = @worker_id
                  AND fence_token = @fence_token;
                """;
            AddLeaseIdentity(update, lease);
            update.Parameters.AddWithValue("failure_type", failureType);
            update.Parameters.AddWithValue("failure_message", failureMessage);

            int affected = await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (affected != 1)
            {
                throw new StaleRelayLeaseException(lease.Id, lease.WorkerId, lease.FenceToken);
            }
        }

        await using (NpgsqlCommand insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO relay_dead_letters(message_id, attempts, failure_type, failure_message)
                VALUES (@id, @attempts, @failure_type, @failure_message);
                """;
            insert.Parameters.AddWithValue("id", lease.Id);
            insert.Parameters.AddWithValue("attempts", lease.AttemptCount);
            insert.Parameters.AddWithValue("failure_type", failureType);
            insert.Parameters.AddWithValue("failure_message", failureMessage);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> CountDeadLettersAsync(CancellationToken cancellationToken = default)
    {
        await using NpgsqlCommand command = _dataSource.CreateCommand("SELECT COUNT(*) FROM relay_dead_letters;");
        object? value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static void AddLeaseIdentity(NpgsqlCommand command, DurableRelayLease lease)
    {
        command.Parameters.AddWithValue("id", lease.Id);
        command.Parameters.AddWithValue("worker_id", lease.WorkerId);
        command.Parameters.AddWithValue("fence_token", lease.FenceToken);
    }

    private static async Task EnsureTransitionAsync(
        NpgsqlCommand command,
        DurableRelayLease lease,
        CancellationToken cancellationToken)
    {
        int affected = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (affected != 1)
        {
            throw new StaleRelayLeaseException(lease.Id, lease.WorkerId, lease.FenceToken);
        }
    }

    private static DurableRelayMessage ReadMessage(NpgsqlDataReader reader)
    {
        Guid id = reader.GetGuid(0);
        long sequence = reader.GetInt64(1);
        string messageId = reader.GetString(2);
        string idempotencyKey = reader.GetString(3);
        string partitionKey = reader.GetString(4);
        int partitionIndex = reader.GetInt32(5);
        byte[] payload = reader.GetFieldValue<byte[]>(6);
        var state = (DurableRelayState)reader.GetInt16(7);
        int attemptCount = reader.GetInt32(8);
        DateTime availableAt = reader.GetDateTime(9);
        string? leaseOwner = reader.IsDBNull(10) ? null : reader.GetString(10);
        long fenceToken = reader.GetInt64(11);
        DateTime? leaseUntil = reader.IsDBNull(12) ? null : reader.GetDateTime(12);
        DateTime createdAt = reader.GetDateTime(13);
        string? lastErrorType = reader.IsDBNull(14) ? null : reader.GetString(14);
        string? lastErrorMessage = reader.IsDBNull(15) ? null : reader.GetString(15);

        return new DurableRelayMessage(
            id,
            sequence,
            new RelayEnvelope(messageId, idempotencyKey, partitionKey, payload),
            partitionIndex,
            state,
            attemptCount,
            availableAt,
            leaseOwner,
            fenceToken,
            leaseUntil,
            createdAt,
            lastErrorType,
            lastErrorMessage);
    }
}
