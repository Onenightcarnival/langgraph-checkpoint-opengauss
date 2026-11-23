BEGIN;

SELECT pg_advisory_xact_lock(hashtext(current_schema()), hashtext('store_migrations'));

CREATE TABLE IF NOT EXISTS store_migrations (
    v INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS store (
    prefix TEXT NOT NULL,
    key TEXT NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, key)
);

CREATE INDEX IF NOT EXISTS store_prefix_idx
    ON store USING btree (prefix text_pattern_ops);

ALTER TABLE store
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS ttl_minutes DOUBLE PRECISION;

ALTER TABLE store ALTER COLUMN ttl_minutes TYPE DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_store_expires_at
    ON store (expires_at) WHERE expires_at IS NOT NULL;

INSERT INTO store_migrations (v)
VALUES (0), (1), (2), (3), (4)
ON DUPLICATE KEY UPDATE NOTHING;

COMMIT;
