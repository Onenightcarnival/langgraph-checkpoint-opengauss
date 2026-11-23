from collections.abc import Iterator
from contextlib import contextmanager
from typing import Self

from langgraph.checkpoint.postgres.base import _execute
from langgraph.store.postgres import PoolConfig, PostgresStore
from langgraph.store.postgres.base import PostgresIndexConfig, TTLConfig

from .._session import SyncSession, connect
from .sql import StoreSQL


class OpenGaussStore(StoreSQL, SyncSession, PostgresStore):
    @classmethod
    @contextmanager
    def from_conn_string(
        cls,
        conn_string: str,
        *,
        pipeline: bool = False,
        pool_config: PoolConfig | None = None,
        index: PostgresIndexConfig | None = None,
        ttl: TTLConfig | None = None,
    ) -> Iterator[Self]:
        with connect(conn_string, pipeline, pool_config) as (conn, pipe):
            store = cls(conn, pipe=pipe, index=index, ttl=ttl)
            try:
                yield store
            finally:
                store.stop_ttl_sweeper()

    def setup(self) -> None:
        with self._cursor() as cur:
            _execute(cur, self.migrations())

    def sweep_ttl(self) -> int:
        with self._cursor() as cur:
            cur.execute(self.SWEEP_SQL)
            return cur.fetchone()["deleted"]
