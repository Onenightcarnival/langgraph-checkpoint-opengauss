import asyncio
import sys
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo


@pytest.fixture
def run_async():
    def run(coroutine):
        if sys.platform == "win32":
            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
                return runner.run(coroutine)
        return asyncio.run(coroutine)

    return run


@contextmanager
def isolated_schema():
    schema = f"checkpoint_test_{uuid4().hex}"
    name = sql.Identifier(schema)
    with psycopg.connect(autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(name))
        try:
            yield make_conninfo(options=f"-c search_path={schema}")
        finally:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(name))


@pytest.fixture
def schema_factory():
    return isolated_schema


@pytest.fixture
def conninfo():
    with isolated_schema() as dsn:
        yield dsn
