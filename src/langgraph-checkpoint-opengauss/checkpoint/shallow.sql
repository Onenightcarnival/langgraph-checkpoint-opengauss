BEGIN;

SET LOCAL behavior_compat_options = 'accept_empty_str';

SELECT pg_advisory_xact_lock(
    hashtext(current_schema()),
    hashtext('checkpoint_migrations')
);

DO $ddl$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'checkpoints'
        GROUP BY table_name
        HAVING bool_or(column_name = 'checkpoint_id')
    ) THEN
        RAISE EXCEPTION 'Full and shallow checkpoints require separate schemas';
    END IF;

    CREATE TABLE IF NOT EXISTS checkpoint_migrations (
        v INTEGER PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS checkpoints (
        thread_id TEXT NOT NULL,
        checkpoint_ns TEXT NOT NULL DEFAULT '',
        type TEXT,
        checkpoint JSONB NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}',
        PRIMARY KEY (thread_id, checkpoint_ns)
    );

    CREATE TABLE IF NOT EXISTS checkpoint_blobs (
        thread_id TEXT NOT NULL,
        checkpoint_ns TEXT NOT NULL DEFAULT '',
        channel TEXT NOT NULL,
        type TEXT NOT NULL,
        blob BYTEA,
        PRIMARY KEY (thread_id, checkpoint_ns, channel)
    );

    CREATE TABLE IF NOT EXISTS checkpoint_writes (
        thread_id TEXT NOT NULL,
        checkpoint_ns TEXT NOT NULL DEFAULT '',
        checkpoint_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        idx INTEGER NOT NULL,
        channel TEXT NOT NULL,
        type TEXT,
        blob BYTEA NOT NULL,
        PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
    );

    ALTER TABLE checkpoint_writes
        ADD COLUMN IF NOT EXISTS task_path TEXT NOT NULL DEFAULT '';
END;
$ddl$;

CREATE INDEX IF NOT EXISTS checkpoints_thread_id_idx
    ON checkpoints (thread_id);

CREATE INDEX IF NOT EXISTS checkpoint_blobs_thread_id_idx
    ON checkpoint_blobs (thread_id);

CREATE INDEX IF NOT EXISTS checkpoint_writes_thread_id_idx
    ON checkpoint_writes (thread_id);

INSERT INTO checkpoint_migrations (v)
VALUES (0), (1), (2), (3), (4), (5), (6), (7)
ON DUPLICATE KEY UPDATE NOTHING;

COMMIT;
