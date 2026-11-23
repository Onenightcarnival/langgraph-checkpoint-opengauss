from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from langgraph.checkpoint.conformance.test_utils import (
    generate_checkpoint,
    generate_config,
    generate_metadata,
)
from langgraph.checkpoint.serde.types import TASKS, _DeltaSnapshot
from langgraph_checkpoint_opengauss import AsyncOpenGaussSaver, OpenGaussSaver


def test_pipeline_connections(conninfo, run_async):
    with OpenGaussSaver.from_conn_string(conninfo, pipeline=True) as saver:
        saver.setup()
        config = saver.put(generate_config("pipeline"), generate_checkpoint(), {}, {})
        assert saver.get(config) is not None

    async def resume():
        async with AsyncOpenGaussSaver.from_conn_string(
            conninfo, pipeline=True
        ) as saver:
            assert await saver.aget(config) is not None
            await saver.acopy_thread("pipeline", "copy")
            await saver.aprune(["copy"])
            assert await saver.aget(generate_config("copy")) is not None

    run_async(resume())


def test_concurrent_setup_and_rollback(conninfo):
    def setup(_):
        with OpenGaussSaver.from_conn_string(conninfo) as saver:
            saver.setup()

    with ThreadPoolExecutor(max_workers=3) as workers:
        list(workers.map(setup, range(3)))
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        cp = generate_checkpoint(
            channel_values={"blob": b"\x00\xff"}, channel_versions={"blob": 1}
        )
        cfg = {"configurable": {"thread_id": "source"}}
        with pytest.raises((TypeError, psycopg.Error)):
            saver.put(cfg, cp, {"invalid": object()}, {"blob": 1})
        with saver.conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM checkpoint_blobs")
            assert cur.fetchone()["n"] == 0
        stored = saver.put(cfg, cp, {}, {"blob": 1})
        assert saver.get(stored)["channel_values"] == {"blob": b"\x00\xff"}


def test_copy_delete_and_shared_blobs(conninfo):
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        saver.setup()
        values = {"共享": b"\xff\x00", "obsolete": ["old"]}
        first = generate_checkpoint(
            channel_values=values, channel_versions=dict.fromkeys(values, 1)
        )
        parent = saver.put(
            generate_config("source"),
            first,
            generate_metadata(run_id="old"),
            first["channel_versions"],
        )
        child = generate_checkpoint(channel_versions={"共享": 1})
        kept = saver.put(parent, child, generate_metadata(run_id="new"), {})
        saver.copy_thread("source", "destination")
        saver.delete_for_runs(["old"])
        assert saver.get(parent) is None
        assert saver.get(kept)["channel_values"] == {"共享": values["共享"]}
        saver.delete_thread("source")
        assert saver.get(generate_config("destination"))["channel_values"] == {
            "共享": values["共享"]
        }
        with saver.conn.cursor() as cur:
            cur.execute("SELECT channel FROM checkpoint_blobs")
            assert cur.fetchall() == [{"channel": "共享"}]


def test_legacy_sends_scoped_to_namespace_and_thread(conninfo):
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        saver.setup()
        parent = generate_checkpoint()
        child = generate_checkpoint()
        for thread, ns in [("one", ""), ("one", "child"), ("two", "")]:
            cfg = saver.put(generate_config(thread, checkpoint_ns=ns), parent, {}, {})
            saver.put_writes(cfg, [(TASKS, (thread, ns))], "task")
            saver.put(cfg, child, {}, {})
        children = [row for row in saver.list(None) if row.parent_config]
        assert len(children) == 3
        for row in children:
            keys = row.config["configurable"]
            assert row.checkpoint["channel_values"][TASKS] == [
                [keys["thread_id"], keys["checkpoint_ns"]]
            ]


def test_delta_snapshot_and_prune_guard(conninfo, run_async):
    snapshot = _DeltaSnapshot(["first"])
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        saver.setup()
        first = generate_checkpoint(
            channel_values={"events": snapshot}, channel_versions={"events": "1"}
        )
        root = saver.put(generate_config("delta"), first, {}, first["channel_versions"])
        saver.put_writes(root, [("events", "second")], "task")
        last = saver.put(
            root, generate_checkpoint(channel_versions={"events": "2"}), {}, {}
        )
        history = saver.get_delta_channel_history(
            config=last, channels=["events", "missing"]
        )
        assert history["events"]["seed"] == snapshot
        assert history["events"]["writes"] == [("task", "events", "second")]
        assert history["missing"] == {"writes": []}
        with pytest.raises(NotImplementedError, match="DeltaChannel"):
            saver.prune(["delta"])

    async def read_history():
        async with AsyncOpenGaussSaver.from_conn_string(conninfo) as saver:
            assert (
                await saver.aget_delta_channel_history(
                    config=last, channels=list(history)
                )
                == history
            )

    run_async(read_history())
