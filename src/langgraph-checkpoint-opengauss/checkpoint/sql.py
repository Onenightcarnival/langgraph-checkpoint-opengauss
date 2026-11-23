from collections.abc import Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres import base
from langgraph.checkpoint.serde.types import TASKS

from .._session import load_queries

_QUERIES = load_queries(__package__)


class OpenGaussSQL:
    MIGRATIONS = tuple(
        statement.replace(_QUERIES["migration"]["postgres_index"], _QUERIES["migration"]["opengauss_index"])
        for statement in base.MIGRATIONS
    )
    SELECT_SQL = _QUERIES["checkpoint"]["select"].format(tasks=TASKS)
    UPSERT_CHECKPOINTS_SQL = _QUERIES["checkpoint"]["upsert"]
    UPSERT_CHECKPOINT_BLOBS_SQL = _QUERIES["blob"]["insert"]
    INSERT_CHECKPOINT_WRITES_SQL = _QUERIES["write"]["insert"] + _QUERIES["write"]["ignore_duplicate"]
    UPSERT_CHECKPOINT_WRITES_SQL = _QUERIES["write"]["insert"] + _QUERIES["write"]["update_duplicate"]

    def _select(self, config, metadata, before, limit):
        where, params = self._search_where(config, metadata, before)
        query = " ".join((self.SELECT_SQL, where, _QUERIES["checkpoint"]["order"]))
        if limit is not None:
            query += " " + _QUERIES["checkpoint"]["limit"]
            params.append(int(limit))
        return query, params

    def _decode_row(self, row):
        blobs = [
            (channel.encode(), kind.encode(), bytes.fromhex(blob))
            for channel, kind, blob in row["channel_values"] or []
            if kind != "empty"
        ]
        if sends := row["sends"]:
            self._migrate_pending_sends(
                [(kind.encode(), bytes.fromhex(blob)) for kind, blob in sends],
                row["checkpoint"],
                blobs,
            )
        row["channel_values"] = blobs
        row["pending_writes"] = [
            (
                task.encode(),
                channel.encode(),
                kind.encode(),
                bytes.fromhex(blob),
                path.encode(),
                idx.encode(),
            )
            for task, channel, kind, blob, path, idx in row["pending_writes"] or []
        ]
        return row


def with_namespace(config: RunnableConfig) -> RunnableConfig:
    return {**config, "configurable": {"checkpoint_ns": "", **config["configurable"]}}


def migrations(statements):
    migration = _QUERIES["migration"]
    yield migration["lock"], (), False
    yield statements[0], (), False
    rows = yield migration["latest"], (), True
    latest = rows[0]["version"]
    for version, statement in enumerate(statements):
        if latest is None or version > latest:
            yield statement, (), False
            yield migration["record"], (version,), False


def copy_thread(source: str, target: str):
    if source == target:
        return
    thread = _QUERIES["thread"]
    yield thread["lock"], (), False
    for table, columns in thread["columns"].items():
        yield thread["copy"].format(table=table, columns=columns), (target, source), False


def delete_for_runs(run_ids: Sequence[str]):
    if not run_ids:
        return
    ids = list(run_ids)
    delete = _QUERIES["delete_runs"]
    yield _QUERIES["thread"]["lock"], (), False
    yield delete["blobs"], (ids, ids), False
    yield delete["writes"], (ids,), False
    yield delete["checkpoints"], (ids,), False


def prune_transactions(thread_ids: Sequence[str], strategy: str):
    if isinstance(thread_ids, str):
        raise TypeError("thread_ids must be a sequence of thread IDs, not a str")
    if strategy not in base.PRUNE_STRATEGIES:
        raise ValueError(f"Unknown prune strategy: {strategy!r}")
    ids = [str(thread) for thread in thread_ids]
    if not ids:
        return
    if strategy == "delete":
        yield ((statement, (ids,), False) for statement in base.PRUNE_DELETE_THREADS_SQL)
    else:
        yield base._refuse_delta_threads(ids)
        for thread in ids:
            yield base._keep_latest(thread)
