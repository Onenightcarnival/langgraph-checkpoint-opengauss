BEGIN;

SELECT pg_advisory_xact_lock(hashtext(current_schema()), hashtext('store_migrations'));

CREATE TABLE IF NOT EXISTS vector_migrations (
    v INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS store_vectors (
    prefix TEXT NOT NULL,
    key TEXT NOT NULL,
    field_name TEXT NOT NULL,
    embedding vector(1536),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prefix, key, field_name),
    FOREIGN KEY (prefix, key) REFERENCES store (prefix, key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS store_vectors_embedding_idx
    ON store_vectors USING hnsw (embedding vector_cosine_ops);

INSERT INTO vector_migrations (v)
VALUES (0), (1), (2)
ON DUPLICATE KEY UPDATE NOTHING;

COMMIT;
