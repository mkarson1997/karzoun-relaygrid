CREATE TABLE relay_messages (
    id UUID PRIMARY KEY,
    sequence BIGSERIAL NOT NULL UNIQUE,
    message_id TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    partition_index INTEGER NOT NULL CHECK (partition_index >= 0),
    payload BYTEA NOT NULL,
    state SMALLINT NOT NULL DEFAULT 0 CHECK (state BETWEEN 0 AND 3),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT NULL,
    fence_token BIGINT NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
    lease_until TIMESTAMPTZ NULL,
    last_error_type TEXT NULL,
    last_error_message TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT relay_lease_shape CHECK (
        (state = 1 AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR
        (state <> 1 AND lease_owner IS NULL AND lease_until IS NULL)
    )
);

CREATE INDEX relay_messages_partition_head_idx
    ON relay_messages (partition_index, sequence)
    WHERE state IN (0, 1);

CREATE INDEX relay_messages_available_idx
    ON relay_messages (available_at)
    WHERE state = 0;

CREATE TABLE relay_dead_letters (
    message_id UUID PRIMARY KEY REFERENCES relay_messages(id) ON DELETE CASCADE,
    attempts INTEGER NOT NULL CHECK (attempts >= 1),
    failure_type TEXT NOT NULL,
    failure_message TEXT NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
