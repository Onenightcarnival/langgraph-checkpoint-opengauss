from importlib.resources import files

import psycopg
import pytest
from langgraph.checkpoint.conformance.test_utils import generate_checkpoint, generate_config
from langgraph.checkpoint.serde.types import TASKS
from langgraph_checkpoint_opengauss.checkpoint import AsyncShallowOpenGaussSaver, OpenGaussSaver, ShallowOpenGaussSaver


@pytest.mark.filterwarnings("ignore:.*ShallowPostgresSaver is deprecated:DeprecationWarning")
@pytest.mark.parametrize("manual", [False, True])
def test_shallow_checkpoint(conninfo, run_async, manual):
    ns = generate_config("agent")
    if manual:
        ddl = files("langgraph_checkpoint_opengauss.checkpoint").joinpath("shallow.sql").read_text("utf-8")
        with psycopg.connect(conninfo, autocommit=True) as conn:
            conn.execute(ddl)
            conn.execute(ddl)
    with ShallowOpenGaussSaver.from_conn_string(conninfo) as saver:
        if not manual:
            saver.setup()
        first = generate_checkpoint(channel_values={"data": b"abc"}, channel_versions={"data": "1"})
        stored = saver.put(ns, first, {"step": 1}, first["channel_versions"])
        saver.put_writes(stored, [(TASKS, "resume"), ("status", "old")], "task")
        assert saver.get_tuple(stored).checkpoint["channel_values"] == {"data": b"abc"}
        assert len(saver.get_tuple(stored).pending_writes) == 2

    async def resume():
        async with AsyncShallowOpenGaussSaver.from_conn_string(conninfo) as saver:
            await saver.setup()
            head = generate_checkpoint(channel_values={"extra": {"k": 1}}, channel_versions={"data": "1", "extra": "1"})
            latest = await saver.aput(stored, head, {"step": 2}, {"extra": "1"})
            await saver.aput_writes(latest, [("done", True)], "new")
            row = await saver.aget_tuple(stored)
            assert row.config == latest and row.parent_config is None
            assert row.checkpoint["channel_values"] == {"data": b"abc", "extra": {"k": 1}}
            assert row.checkpoint["pending_sends"] == ["resume"]
            assert row.pending_writes == [("new", "done", True)]
            assert [item.config async for item in saver.alist(ns, filter={"step": 2})] == [latest]
            return latest

    latest = run_async(resume())
    with ShallowOpenGaussSaver.from_conn_string(conninfo) as saver:
        assert [row.config for row in saver.list(None)] == [latest]
        assert saver.get_tuple(ns).config == latest
    with OpenGaussSaver.from_conn_string(conninfo) as full, pytest.raises(ValueError, match="separate schemas"):
        full.setup()
