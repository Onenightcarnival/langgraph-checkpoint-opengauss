from collections.abc import Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres import base
from langgraph.checkpoint.serde.types import TASKS
from psycopg import sql


class OpenGaussSQL:
    MIGRATIONS = tuple(statement.replace("INDEX CONCURRENTLY", "INDEX") for statement in base.MIGRATIONS)

    SELECT_SQL = f"""
        SELECT
            thread_id,
            checkpoint,
            checkpoint_ns,
            checkpoint_id,
            parent_checkpoint_id,
            metadata,
            (
                SELECT json_agg(ARRAY[b.channel, b.type, encode(b.blob, 'hex')])
                FROM jsonb_each_text(checkpoints.checkpoint -> 'channel_versions') v
                JOIN checkpoint_blobs b
                    ON b.channel = v.key
                   AND b.version = v.value
                WHERE b.thread_id = checkpoints.thread_id
                  AND b.checkpoint_ns = checkpoints.checkpoint_ns
            ) AS channel_values,
            (
                SELECT json_agg(ARRAY[
                    w.task_id,
                    w.channel,
                    w.type,
                    encode(w.blob, 'hex'),
                    w.task_path,
                    w.idx::text
                ])
                FROM checkpoint_writes w
                WHERE w.thread_id = checkpoints.thread_id
                  AND w.checkpoint_ns = checkpoints.checkpoint_ns
                  AND w.checkpoint_id = checkpoints.checkpoint_id
            ) AS pending_writes,
            CASE
                WHEN (checkpoint ->> 'v')::int < 4 AND parent_checkpoint_id IS NOT NULL THEN (
                    SELECT json_agg(
                        ARRAY[w.type, encode(w.blob, 'hex')]
                        ORDER BY w.task_path, w.task_id, w.idx
                    )
                    FROM checkpoint_writes w
                    WHERE w.thread_id = checkpoints.thread_id
                      AND w.checkpoint_ns = checkpoints.checkpoint_ns
                      AND w.checkpoint_id = checkpoints.parent_checkpoint_id
                      AND w.channel = '{TASKS}'
                )
            END AS sends
        FROM checkpoints
    """

    UPSERT_CHECKPOINTS_SQL = """
        INSERT INTO checkpoints (
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            parent_checkpoint_id,
            checkpoint,
            metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            checkpoint = VALUES(checkpoint),
            metadata = VALUES(metadata)
    """
    UPSERT_CHECKPOINT_BLOBS_SQL = """
        INSERT INTO checkpoint_blobs (
            thread_id,
            checkpoint_ns,
            channel,
            version,
            type,
            blob
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE NOTHING
    """
    _INSERT_WRITES = """
        INSERT INTO checkpoint_writes (
            thread_id,
            checkpoint_ns,
            checkpoint_id,
            task_id,
            task_path,
            idx,
            channel,
            type,
            blob
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    INSERT_CHECKPOINT_WRITES_SQL = _INSERT_WRITES + " ON DUPLICATE KEY UPDATE NOTHING"
    UPSERT_CHECKPOINT_WRITES_SQL = (
        _INSERT_WRITES
        + """
        ON DUPLICATE KEY UPDATE
            channel = VALUES(channel),
            type = VALUES(type),
            blob = VALUES(blob)
        """
    )

    def _select(self, config, metadata, before, limit):
        where, params = self._search_where(config, metadata, before)
        query = self.SELECT_SQL + where + " ORDER BY checkpoint_id DESC"
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))
        return query, params

    def _decode_row(self, row):
        blobs = [
            (channel.encode(), kind.encode(), bytes.fromhex(blob))
            for channel, kind, blob in row["channel_values"] or []
            if kind != "empty"
        ]
        if sends := row["sends"]:
            self._migrate_pending_sends(
                [(kind.encode(), bytes.fromhex(blob)) for kind, blob in sends],
                row["checkpoint"],
                blobs,
            )
        row["channel_values"] = blobs
        row["pending_writes"] = [
            (
                task.encode(),
                channel.encode(),
                kind.encode(),
                bytes.fromhex(blob),
                path.encode(),
                idx.encode(),
            )
            for task, channel, kind, blob, path, idx in row["pending_writes"] or []
        ]
        return row


def with_namespace(config: RunnableConfig) -> RunnableConfig:
    return {**config, "configurable": {"checkpoint_ns": "", **config["configurable"]}}


def empty_string_option(options: str):
    flags = [flag.strip() for flag in options.split(",") if flag.strip()]
    if "accept_empty_str" not in flags:
        return sql.SQL("SET LOCAL behavior_compat_options = {}").format(
            sql.Literal(",".join([*flags, "accept_empty_str"]))
        )


def migrations():
    yield (
        """
        SELECT pg_advisory_xact_lock(
            hashtext(current_schema()),
            hashtext('checkpoint_migrations')
        )
        """,
        (),
        False,
    )
    yield OpenGaussSQL.MIGRATIONS[0], (), False
    rows = yield "SELECT max(v) AS version FROM checkpoint_migrations", (), True
    latest = rows[0]["version"]
    for version, statement in enumerate(OpenGaussSQL.MIGRATIONS):
        if latest is None or version > latest:
            yield statement, (), False
            yield "INSERT INTO checkpoint_migrations (v) VALUES (%s)", (version,), False


_THREAD_TABLES = {
    "checkpoints": "checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata",
    "checkpoint_blobs": "checkpoint_ns, channel, version, type, blob",
    "checkpoint_writes": "checkpoint_ns, checkpoint_id, task_id, idx, channel, type, blob, task_path",
}
_LOCK_TABLES = """
    LOCK TABLE checkpoints, checkpoint_blobs, checkpoint_writes
    IN SHARE ROW EXCLUSIVE MODE
"""


def copy_thread(source: str, target: str):
    if source == target:
        return
    yield _LOCK_TABLES, (), False
    for table, columns in _THREAD_TABLES.items():
        statement = f"""
            INSERT INTO {table} (thread_id, {columns})
            SELECT %s, {columns}
            FROM {table}
            WHERE thread_id = %s
        """
        yield statement, (target, source), False


def delete_for_runs(run_ids: Sequence[str]):
    if not run_ids:
        return
    ids = list(run_ids)
    yield _LOCK_TABLES, (), False
    matches_blob = """c.thread_id = b.thread_id
                  AND c.checkpoint_ns = b.checkpoint_ns
                  AND c.checkpoint -> 'channel_versions' ->> b.channel = b.version"""
    yield (
        f"""
        DELETE FROM checkpoint_blobs b
        WHERE
            EXISTS (
                SELECT 1
                FROM checkpoints c
                WHERE {matches_blob}
                  AND c.metadata ->> 'run_id' = ANY(%s)
            )
            AND NOT EXISTS (
                SELECT 1
                FROM checkpoints c
                WHERE {matches_blob}
                  AND (c.metadata ->> 'run_id' = ANY(%s)) IS NOT TRUE
            )
        """,
        (ids, ids),
        False,
    )
    yield (
        """
        DELETE FROM checkpoint_writes w
        WHERE EXISTS (
            SELECT 1
            FROM checkpoints c
            WHERE c.thread_id = w.thread_id
              AND c.checkpoint_ns = w.checkpoint_ns
              AND c.checkpoint_id = w.checkpoint_id
              AND c.metadata ->> 'run_id' = ANY(%s)
        )
        """,
        (ids,),
        False,
    )
    yield "DELETE FROM checkpoints WHERE metadata ->> 'run_id' = ANY(%s)", (ids,), False


def prune_transactions(thread_ids: Sequence[str], strategy: str):
    if isinstance(thread_ids, str):
        raise TypeError("thread_ids must be a sequence of thread IDs, not a str")
    if strategy not in base.PRUNE_STRATEGIES:
        raise ValueError(f"Unknown prune strategy: {strategy!r}")
    ids = [str(thread) for thread in thread_ids]
    if not ids:
        return
    if strategy == "delete":
        yield ((statement, (ids,), False) for statement in base.PRUNE_DELETE_THREADS_SQL)
    else:
        yield base._refuse_delta_threads(ids)
        for thread in ids:
            yield base._keep_latest(thread)
