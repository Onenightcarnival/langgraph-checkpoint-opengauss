from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Self

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.postgres.base import _execute
from langgraph.checkpoint.serde.base import SerializerProtocol

from .._session import SyncSession, connect
from . import sql


class SaverSession(SyncSession):
    @classmethod
    @contextmanager
    def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        serde: SerializerProtocol | None = None,
    ) -> Iterator[Self]:
        with connect(conn_string, pipeline) as (conn, pipe):
            yield cls(conn, pipe, serde)

    def setup(self) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.migrations(self.MIGRATIONS, shallow=self.SHALLOW))

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
        return super().put(sql.with_namespace(config), checkpoint, metadata, new_versions)


class OpenGaussSaver(sql.OpenGaussSQL, SaverSession, PostgresSaver):
    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.copy_thread(source_thread_id, target_thread_id))

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        with self._cursor() as cur:
            _execute(cur, sql.delete_for_runs(run_ids))

    def prune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        for transaction in sql.prune_transactions(thread_ids, strategy):
            with self._cursor() as cur:
                _execute(cur, transaction)
