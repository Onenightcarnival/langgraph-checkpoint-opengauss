import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib.resources import files

import psycopg
import pytest
from langgraph.store.base import GetOp, ListNamespacesOp, PutOp, SearchOp
from langgraph_checkpoint_opengauss.store import AsyncOpenGaussStore, OpenGaussStore


@pytest.mark.parametrize("options", [{}, {"pipeline": True}, {"pool_config": {"min_size": 1, "max_size": 3}}])
def test_store_batch_across_sessions(conninfo, run_async, options):
    ns = ("users", "alice")
    with OpenGaussStore.from_conn_string(conninfo, **options) as store:
        store.setup()
        ops = [
            PutOp(ns, "one", {"rank": "a"}),
            PutOp(ns, "one", {"rank": "b"}),
            PutOp((*ns, "child"), "two", {"rank": "c"}),
            PutOp(("users", "bob"), "three", {"rank": "d"}),
        ]
        assert store.batch(ops) == [None] * 4
        first = store.get(ns, "one")
        assert first.value == {"rank": "b"}
        found, namespaces, missing = store.batch(
            [
                SearchOp(ns, filter={"rank": {"$gte": "b"}}),
                ListNamespacesOp(max_depth=1),
                GetOp(ns, "absent"),
            ]
        )
        assert {item.key for item in found} == {"one", "two"}
        assert namespaces == [("users",)] and missing is None
        assert store.list_namespaces(prefix=ns, suffix=("child",)) == [(*ns, "child")]
        assert store.list_namespaces(prefix=("users", "*"), max_depth=2) == [ns, ("users", "bob")]
        assert len(store.search((), limit=1, offset=1)) == 1

    async def resume():
        async with AsyncOpenGaussStore.from_conn_string(conninfo, **options) as store:
            await store.setup()
            value = await store.aget(ns, "one")
            assert value.value == first.value and value.created_at == first.created_at
            await store.aput(ns, "one", {"rank": "updated"})
            results = await store.abatch([GetOp(ns, "one"), SearchOp(ns, filter={"rank": "updated"})])
            assert results[0].value == {"rank": "updated"} and [r.key for r in results[1]] == ["one"]
            assert await store.alist_namespaces(prefix=ns, max_depth=2) == [ns]
            await store.adelete(ns, "one")
            assert await store.aget(ns, "one") is None
            assert await asyncio.to_thread(store.get, (*ns, "child"), "two") is not None

    run_async(resume())


def embed(texts):
    vectors = {"north": [1.0, 0.0, 0.0], "east": [0.0, 1.0, 0.0], "west": [0.0, -1.0, 0.0]}
    return [vectors[text] for text in texts]


@pytest.mark.parametrize("kind", ["flat", "hnsw", "ivfflat"])
@pytest.mark.parametrize("vector_type", ["vector", "halfvec"])
@pytest.mark.parametrize("distance", ["cosine", "inner_product", "l2"])
def test_vector_store(conninfo, run_async, kind, vector_type, distance):
    index = {
        "dims": 3,
        "embed": embed,
        "fields": ["title", "body"],
        "distance_type": distance,
        "ann_index_config": {"kind": kind, "vector_type": vector_type},
    }
    ns = ("places",)

    async def insert():
        async with AsyncOpenGaussStore.from_conn_string(conninfo, index=index) as store:
            await store.setup()
            await store.abatch(
                [
                    PutOp(ns, "a", {"title": "north", "body": "east", "tag": "a"}),
                    PutOp(ns, "b", {"title": "west", "tag": "b"}),
                    PutOp(ns, "ignored", {"title": "north"}, index=False),
                ]
            )
            found = await store.asearch(ns, query="north")
            assert [item.key for item in found] == ["a", "b"]
            assert found[0].score == pytest.approx(0.0 if distance == "l2" else 1.0)
            assert len(await store.asearch(ns, query="north", filter={"tag": "a"})) == 1

    run_async(insert())
    with OpenGaussStore.from_conn_string(conninfo, index=index) as store:
        store.setup()
        assert [item.key for item in store.search(ns, query="north", offset=1)] == ["b"]
        store.put(ns, "b", {"body": "north"}, index=["body"])
        assert store.search(ns, query="west")[0].score == pytest.approx(-(2**0.5) if distance == "l2" else 0, abs=1e-6)
        with pytest.raises(KeyError):
            store.put(ns, "bad", {"title": "embedding error"})
        assert store.get(ns, "bad") is None
        store.delete(ns, "a")
        assert [item.key for item in store.search(ns, query="north")] == ["b"]
    with psycopg.connect(conninfo) as db:
        rows = db.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
            "AND indexname = 'store_vectors_embedding_idx'"
        ).fetchall()
        assert len(rows) == int(kind != "flat")
        if rows:
            assert f"USING {kind}" in rows[0][0]
        assert db.execute("SELECT count(*) FROM store_vectors WHERE key='a'").fetchone() == (0,)


@pytest.mark.parametrize("pipeline", [False, True])
def test_store_ttl(conninfo, run_async, pipeline):
    ttl = {"default_ttl": 1.5, "omit_expired": True, "refresh_on_read": True}
    ns = ("ttl",)
    with (
        OpenGaussStore.from_conn_string(conninfo, ttl=ttl, pipeline=pipeline) as store,
        psycopg.connect(conninfo, autocommit=True) as admin,
    ):
        store.setup()
        store.put(ns, "live", {"x": 1})
        assert admin.execute("SELECT ttl_minutes FROM store WHERE key='live'").fetchone() == (1.5,)
        store.put(("expired",), "old", {"x": 0})
        admin.execute("UPDATE store SET expires_at = now() - interval '1 second' WHERE key='old'")
        admin.execute("UPDATE store SET expires_at = now() + interval '1 minute' WHERE key='live'")
        before = admin.execute("SELECT expires_at FROM store WHERE key='live'").fetchone()[0]
        assert store.get(ns, "live", refresh_ttl=False)
        assert admin.execute("SELECT expires_at FROM store WHERE key='live'").fetchone()[0] == before
        assert store.get(ns, "live", refresh_ttl=True)
        assert admin.execute("SELECT expires_at FROM store WHERE key='live'").fetchone()[0] > before
        assert store.get(("expired",), "old") is None
        assert store.list_namespaces() == [ns]
        assert [item.key for item in store.search(())] == ["live"]
        assert store.sweep_ttl() == 1
        store.start_ttl_sweeper()
        assert store.stop_ttl_sweeper(timeout=2)

    async def sweep():
        async with AsyncOpenGaussStore.from_conn_string(conninfo, ttl=ttl, pipeline=pipeline) as store:
            await store.aput(ns, "immediate", {"x": 0}, ttl=0)
            assert await store.aget(ns, "immediate") is None
            assert await store.sweep_ttl() == 1
            await store.start_ttl_sweeper()
            assert await store.stop_ttl_sweeper(timeout=2)

    run_async(sweep())


def test_concurrent_store_migrations(conninfo):
    def setup(_):
        with OpenGaussStore.from_conn_string(conninfo) as store:
            store.setup()

    with ThreadPoolExecutor(3) as workers:
        list(workers.map(setup, range(3)))


def test_manual_store_schema(conninfo):
    scripts = files("langgraph_checkpoint_opengauss.store")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        for _ in range(2):
            for script in ("schema.sql", "vector_schema.sql"):
                conn.execute(scripts.joinpath(script).read_text("utf-8"))
    index = {"dims": 1536, "embed": lambda texts: [[1.0] + [0.0] * 1535 for _ in texts]}
    with OpenGaussStore.from_conn_string(conninfo, index=index) as store:
        store.put(("manual",), "doc", {"tag": "manual"})
        assert store.search(("manual",), query="manual")[0].score == pytest.approx(1)
        store.setup()
        assert store.get(("manual",), "doc").value == {"tag": "manual"}


def test_vector_write_rollback(conninfo):
    def varying_dims(texts):
        return [[1.0, 0.0] if text == "invalid" else [1.0, 0.0, 0.0] for text in texts]

    index = {"dims": 3, "fields": ["text"], "embed": varying_dims}
    with OpenGaussStore.from_conn_string(conninfo, index=index) as store:
        store.setup()
        store.put(("docs",), "old", {"text": "valid"})
        with pytest.raises(psycopg.Error):
            store.batch([PutOp(("docs",), "old", {"text": "invalid"}), PutOp(("docs",), "new", {"text": "valid"})])
        assert store.get(("docs",), "old").value == {"text": "valid"}
        assert store.get(("docs",), "new") is None
        assert [item.key for item in store.search(("docs",), query="valid")] == ["old"]
        store.put(("docs",), "old", {"text": "valid"}, index=False)
        assert store.search(("docs",), query="valid") == []
