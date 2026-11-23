from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Self

from langgraph.checkpoint.postgres.base import _aexecute
from langgraph.store.postgres import AsyncPostgresStore, PoolConfig
from langgraph.store.postgres.base import PostgresIndexConfig, TTLConfig

from .._session import AsyncSession, aconnect
from .sql import StoreSQL


class AsyncOpenGaussStore(StoreSQL, AsyncSession, AsyncPostgresStore):
    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        pool_config: PoolConfig | None = None,
        index: PostgresIndexConfig | None = None,
        ttl: TTLConfig | None = None,
    ) -> AsyncIterator[Self]:
        async with aconnect(conn_string, pipeline, pool_config) as (conn, pipe):
            store = cls(conn, pipe=pipe, index=index, ttl=ttl)
            try:
                yield store
            finally:
                await store.stop_ttl_sweeper()

    async def setup(self) -> None:
        async with self._cursor() as cur:
            await _aexecute(cur, self.migrations())

    async def sweep_ttl(self) -> int:
        async with self._cursor() as cur:
            await cur.execute(self.SWEEP_SQL)
            return (await cur.fetchone())["deleted"]
