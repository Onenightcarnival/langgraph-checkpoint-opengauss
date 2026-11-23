import tomllib
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager, nullcontext
from importlib.resources import files

from langgraph.checkpoint.postgres import _ainternal, _internal
from psycopg import AsyncConnection, AsyncCursor, Connection, Cursor, sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool


def load_queries(package: str, resource: str = "queries.toml") -> dict:
    with files(package).joinpath(resource).open("rb") as stream:
        return tomllib.load(stream)


_SESSION = load_queries(__package__, "session.toml")
_CONNECT_OPTIONS = {"autocommit": True, "prepare_threshold": None, "row_factory": dict_row}


def _pool_options(pool_config):
    options = {"min_size": 1, **pool_config}
    options["kwargs"] = {**_CONNECT_OPTIONS, **(options.get("kwargs") or {})}
    return options


def _empty_string_option(options: str):
    flags = [flag.strip() for flag in options.split(",") if flag.strip()]
    if "accept_empty_str" not in flags:
        return sql.SQL(_SESSION["set_options"]).format(sql.Literal(",".join([*flags, "accept_empty_str"])))


@contextmanager
def connect(conn_string, pipeline=False, pool_config=None):
    if pool_config is not None:
        with ConnectionPool(conn_string, **_pool_options(pool_config)) as pool:
            yield pool, None
    else:
        with Connection.connect(conn_string, **_CONNECT_OPTIONS) as conn:
            with conn.pipeline() if pipeline else nullcontext() as pipe:
                yield conn, pipe


@asynccontextmanager
async def aconnect(conn_string, pipeline=False, pool_config=None):
    if pool_config is not None:
        async with AsyncConnectionPool(conn_string, open=False, **_pool_options(pool_config)) as pool:
            yield pool, None
    else:
        async with await AsyncConnection.connect(conn_string, **_CONNECT_OPTIONS) as conn:
            async with conn.pipeline() if pipeline else nullcontext() as pipe:
                yield conn, pipe


class SyncSession:
    @contextmanager
    def _cursor(self, *, pipeline: bool = False) -> Iterator[Cursor[DictRow]]:
        lock = nullcontext() if isinstance(self.conn, ConnectionPool) else self.lock
        with (
            lock,
            _internal.get_connection(self.conn) as conn,
            conn.transaction(),
            conn.cursor(binary=True, row_factory=dict_row) as cur,
        ):
            cur.execute(_SESSION["show_options"])
            if option := _empty_string_option(cur.fetchone()["behavior_compat_options"]):
                cur.execute(option)
            yield cur


class AsyncSession:
    @asynccontextmanager
    async def _cursor(self, *, pipeline: bool = False) -> AsyncIterator[AsyncCursor[DictRow]]:
        lock = nullcontext() if isinstance(self.conn, AsyncConnectionPool) else self.lock
        async with (
            lock,
            _ainternal.get_connection(self.conn) as conn,
            conn.transaction(),
            conn.cursor(binary=True, row_factory=dict_row) as cur,
        ):
            await cur.execute(_SESSION["show_options"])
            if option := _empty_string_option((await cur.fetchone())["behavior_compat_options"]):
                await cur.execute(option)
            yield cur
