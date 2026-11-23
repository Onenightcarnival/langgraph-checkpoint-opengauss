import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.postgres import _ainternal
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.postgres.base import _aexecute
from langgraph.checkpoint.serde.base import SerializerProtocol
from psycopg import AsyncConnection, AsyncCursor
from psycopg.rows import DictRow, dict_row

from . import sql


class AsyncOpenGaussSaver(sql.OpenGaussSQL, AsyncPostgresSaver):
    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        serde: SerializerProtocol | None = None,
    ) -> AsyncIterator["AsyncOpenGaussSaver"]:
        async with await AsyncConnection.connect(
            conn_string, autocommit=True, prepare_threshold=None, row_factory=dict_row
        ) as conn:
            if pipeline:
                async with conn.pipeline() as pipe:
                    yield cls(conn, pipe, serde)
            else:
                yield cls(conn, serde=serde)

    @asynccontextmanager
    async def _cursor(self, *, pipeline: bool = False) -> AsyncIterator[AsyncCursor[DictRow]]:
        async with (
            self.lock,
            _ainternal.get_connection(self.conn) as conn,
            conn.transaction(),
            conn.cursor(binary=True, row_factory=dict_row) as cur,
        ):
            await cur.execute("SHOW behavior_compat_options")
            option = sql.empty_string_option((await cur.fetchone())["behavior_compat_options"])
            if option is not None:
                await cur.execute(option)
            yield cur

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        async with self._cursor() as cur:
            await cur.execute(*self._select(config, filter, before, limit))
            rows = await cur.fetchall()
        for row in rows:
            yield await self._load_checkpoint_tuple(self._decode_row(row))

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await anext(self.alist(sql.with_namespace(config), limit=1), None)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await super().aput(sql.with_namespace(config), checkpoint, metadata, new_versions)

    async def setup(self) -> None:
        async with self._cursor() as cur:
            await _aexecute(cur, sql.migrations())

    def _check_sync_thread(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop is self.loop:
            raise asyncio.InvalidStateError("Use the async method on the saver's event loop")

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        self._check_sync_thread()
        asyncio.run_coroutine_threadsafe(self.acopy_thread(source_thread_id, target_thread_id), self.loop).result()

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        self._check_sync_thread()
        asyncio.run_coroutine_threadsafe(self.adelete_for_runs(run_ids), self.loop).result()

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        async with self._cursor() as cur:
            await _aexecute(cur, sql.copy_thread(source_thread_id, target_thread_id))

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        async with self._cursor() as cur:
            await _aexecute(cur, sql.delete_for_runs(run_ids))

    async def aprune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        for transaction in sql.prune_transactions(thread_ids, strategy):
            async with self._cursor() as cur:
                await _aexecute(cur, transaction)
