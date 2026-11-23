from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.postgres import PostgresSaver, _internal
from langgraph.checkpoint.postgres.base import _execute
from langgraph.checkpoint.serde.base import SerializerProtocol
from psycopg import Connection, Cursor
from psycopg.rows import DictRow, dict_row

from . import sql


class OpenGaussSaver(sql.OpenGaussSQL, PostgresSaver):
    @classmethod
    @contextmanager
    def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        serde: SerializerProtocol | None = None,
    ) -> Iterator["OpenGaussSaver"]:
        with Connection.connect(
            conn_string, autocommit=True, prepare_threshold=None, row_factory=dict_row
        ) as conn:
            if pipeline:
                with conn.pipeline() as pipe:
                    yield cls(conn, pipe, serde)
            else:
                yield cls(conn, serde=serde)

    @contextmanager
    def _cursor(self, *, pipeline: bool = False) -> Iterator[Cursor[DictRow]]:
        with (
            self.lock,
            _internal.get_connection(self.conn) as conn,
            conn.transaction(),
            conn.cursor(binary=True, row_factory=dict_row) as cur,
        ):
            cur.execute("SHOW behavior_compat_options")
            option = sql.empty_string_option(cur.fetchone()["behavior_compat_options"])
            if option is not None:
                cur.execute(option)
            yield cur

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        with self._cursor() as cur:
            cur.execute(*self._select(config, filter, before, limit))
            rows = cur.fetchall()
        for row in rows:
            yield self._load_checkpoint_tuple(self._decode_row(row))

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return next(self.list(sql.with_namespace(config), limit=1), None)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return super().put(
            sql.with_namespace(config), checkpoint, metadata, new_versions
        )

    def setup(self) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.migrations())

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.copy_thread(source_thread_id, target_thread_id))

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.delete_for_runs(run_ids))

    def prune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        for transaction in sql.prune_transactions(thread_ids, strategy):
            with self._cursor() as cur:
                _execute(cur, transaction)
