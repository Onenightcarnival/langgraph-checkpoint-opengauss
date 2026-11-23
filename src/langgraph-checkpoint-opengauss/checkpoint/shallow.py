from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.checkpoint.postgres import shallow
from langgraph.checkpoint.serde.types import TASKS

from .aio import AsyncSaverSession
from .saver import SaverSession
from .sql import _QUERIES, OpenGaussSQL


class ShallowSQL(OpenGaussSQL):
    SHALLOW = True
    MIGRATIONS = tuple(s.replace("INDEX CONCURRENTLY", "INDEX") for s in shallow.MIGRATIONS)
    SELECT_SQL = _QUERIES["shallow"]["select"].format(tasks=TASKS)
    UPSERT_CHECKPOINTS_SQL = _QUERIES["shallow"]["upsert_checkpoint"]
    UPSERT_CHECKPOINT_BLOBS_SQL = _QUERIES["shallow"]["upsert_blob"]

    def _shallow_tuple(self, row):
        return CheckpointTuple(
            config={"configurable": {key: row[key] for key in ("thread_id", "checkpoint_ns", "checkpoint_id")}},
            checkpoint={
                **row["checkpoint"],
                "channel_values": self._load_blobs(row["channel_values"]),
                "pending_sends": [
                    self.serde.loads_typed((kind, bytes.fromhex(blob))) for kind, blob in row["pending_sends"] or []
                ],
            },
            metadata=row["metadata"],
            pending_writes=self._load_writes(row["pending_writes"]),
        )


def _latest_config(config):
    keys = config["configurable"]
    return {"configurable": {"thread_id": keys["thread_id"], "checkpoint_ns": keys.get("checkpoint_ns", "")}}


class ShallowOpenGaussSaver(ShallowSQL, SaverSession, shallow.ShallowPostgresSaver):
    def _load_checkpoint_tuple(self, row):
        return self._shallow_tuple(row)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return super().get_tuple(_latest_config(config))


class AsyncShallowOpenGaussSaver(ShallowSQL, AsyncSaverSession, shallow.AsyncShallowPostgresSaver):
    async def _load_checkpoint_tuple(self, row):
        return self._shallow_tuple(row)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await super().aget_tuple(_latest_config(config))
