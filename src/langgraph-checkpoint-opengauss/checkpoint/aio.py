import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Self

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.postgres.base import _aexecute
from langgraph.checkpoint.serde.base import SerializerProtocol

from .._session import AsyncSession, aconnect
from . import sql


class AsyncSaverSession(AsyncSession):
    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        serde: SerializerProtocol | None = None,
    ) -> AsyncIterator[Self]:
        async with aconnect(conn_string, pipeline) as (conn, pipe):
            yield cls(conn, pipe, serde)

    async def setup(self) -> None:
        async with self._cursor() as cur:
            await _aexecute(cur, sql.migrations(self.MIGRATIONS, shallow=self.SHALLOW))

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


class AsyncOpenGaussSaver(sql.OpenGaussSQL, AsyncSaverSession, AsyncPostgresSaver):
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
