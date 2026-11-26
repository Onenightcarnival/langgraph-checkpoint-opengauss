# langgraph-checkpoint-opengauss

[English](https://github.com/Onenightcarnival/langgraph-checkpoint-opengauss/blob/main/README.md) | 简体中文

LangGraph 的 openGauss 持久化后端，对齐 `langgraph-checkpoint-postgres==3.2.1`。两个子包均提供同步与异步接口。

| 子包 | 入口 | 持久化范围 |
| --- | --- | --- |
| `langgraph_checkpoint_opengauss.checkpoint` | `OpenGaussSaver`、`AsyncOpenGaussSaver` | 线程内的检查点、任务写入和历史恢复 |
| `langgraph_checkpoint_opengauss.checkpoint` | `ShallowOpenGaussSaver`、`AsyncShallowOpenGaussSaver` | 每个线程、每个命名空间的最新检查点，兼容上游已弃用的 shallow 模式 |
| `langgraph_checkpoint_opengauss.store` | `OpenGaussStore`、`AsyncOpenGaussStore` | 按命名空间和键保存的跨线程 JSON 文档、TTL 和可选向量检索 |

以上类均可从包根目录导入。

## 安装与连接

```shell
uv add "langgraph-checkpoint-opengauss[binary]"
```

```shell
pip install "langgraph-checkpoint-opengauss[binary]"
```

`binary` 扩展安装 `psycopg[binary]`，即自带 libpq 的预编译驱动。不带该扩展时，运行环境需自行提供 psycopg 3 和系统 libpq。数据库需允许 PostgreSQL 客户端使用配置的认证方式连接。

连接串接受 libpq DSN，也可用空字符串读取 `PGHOST`、`PGPORT`、`PGDATABASE`、`PGUSER`、`PGPASSWORD` 和 `PGSSLMODE`。

## 初始化检查点表

**手动建表**：在目标数据库中将 `search_path` 指向应用使用的 schema，再用 SQL 客户端完整执行 [schema.sql](https://github.com/Onenightcarnival/langgraph-checkpoint-opengauss/blob/main/src/langgraph-checkpoint-opengauss/schema.sql)。脚本在一个事务中创建四张表、三个辅助索引，并登记迁移版本 0–9。相同版本可重复执行。完成后，同步和异步应用都可省略下方示例中的 `setup()`，直接编译图。

**应用建表**：首次部署时由 `setup()` 在当前 schema 中建立或迁移检查点表。

```python
from langgraph_checkpoint_opengauss import OpenGaussSaver

with OpenGaussSaver.from_conn_string("") as saver:
    saver.setup()
    graph = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "session-1"}}
    result = graph.invoke(inputs, config)
```

```python
from langgraph_checkpoint_opengauss import AsyncOpenGaussSaver

async def run(builder, inputs, config):
    async with AsyncOpenGaussSaver.from_conn_string("") as saver:
        await saver.setup()
        graph = builder.compile(checkpointer=saver)
        return await graph.ainvoke(inputs, config)
```

Windows 上的异步 psycopg 连接使用 `SelectorEventLoop`。

完整历史与 shallow 模式使用相同的表名和不同的主键，两种模式分别使用独立的 schema。shallow 可调用自己的 `setup()`，或手动执行 [shallow.sql](https://github.com/Onenightcarnival/langgraph-checkpoint-opengauss/blob/main/src/langgraph-checkpoint-opengauss/checkpoint/shallow.sql)。已有完整历史部署继续使用原表和原连接串。

## 检查点操作

| 操作 | 同步 | 异步 |
| --- | --- | --- |
| 写入检查点与任务结果 | `put`、`put_writes` | `aput`、`aput_writes` |
| 读取与筛选历史 | `get_tuple`、`list` | `aget_tuple`、`alist` |
| 读取增量通道历史 | `get_delta_channel_history` | `aget_delta_channel_history` |
| 复制、删除线程 | `copy_thread`、`delete_thread` | `acopy_thread`、`adelete_thread` |
| 按运行删除、按线程裁剪 | `delete_for_runs`、`prune` | `adelete_for_runs`、`aprune` |

`prune(strategy="keep_latest")` 保留各命名空间的最新检查点及其依赖，遇到依赖历史重放的 DeltaChannel 时拒绝裁剪。`prune(strategy="delete")` 删除指定线程的检查点、任务结果和通道数据。

## Store 与跨线程记忆

Store 可与检查点共用数据库和 schema，分别通过 `store=` 和 `checkpointer=` 传给图。命名空间由字符串元组表示；同一命名空间与键在不同线程间指向同一份文档。

```python
from langgraph_checkpoint_opengauss.checkpoint import OpenGaussSaver
from langgraph_checkpoint_opengauss.store import OpenGaussStore

with OpenGaussSaver.from_conn_string("") as saver, OpenGaussStore.from_conn_string("") as store:
    saver.setup()
    store.setup()
    store.put(("users", "alice"), "prefs", {"language": "zh"})
    preferences = store.get(("users", "alice"), "prefs").value
    graph = builder.compile(checkpointer=saver, store=store)
```

| 操作 | 同步 | 异步 |
| --- | --- | --- |
| 键值读写与删除 | `get`、`put`、`delete` | `aget`、`aput`、`adelete` |
| 条件检索、分页 | `search` | `asearch` |
| 命名空间前后缀与深度筛选 | `list_namespaces` | `alist_namespaces` |
| 混合批处理 | `batch` | `abatch` |

Store 接受 `pool_config={"min_size": 1, "max_size": 8}` 使用连接池，单连接可启用 `pipeline=True`。`ttl` 配置支持 `default_ttl`（分钟）、`refresh_on_read`、`omit_expired` 和 `sweep_interval_minutes`。调用 `sweep_ttl()` 清理过期文档，或通过 `start_ttl_sweeper()`、`stop_ttl_sweeper()` 管理后台清理；异步 Store 使用 `await`。

### 向量检索

传入与嵌入模型输出一致的维度。下例使用应用提供的 `embeddings` 实例，仅索引 `text` 字段。

```python
from langgraph_checkpoint_opengauss.store import AsyncOpenGaussStore

async def search_documents(embeddings, query):
    index = {
        "dims": 1536,
        "embed": embeddings,
        "fields": ["text"],
        "distance_type": "cosine",
        "ann_index_config": {"kind": "hnsw", "vector_type": "vector"},
    }
    async with AsyncOpenGaussStore.from_conn_string("", index=index) as store:
        await store.setup()
        await store.aput(("docs",), "intro", {"text": "openGauss 检查点", "lang": "zh"})
        return await store.asearch(("docs",), query=query, filter={"lang": "zh"}, limit=5)
```

支持 `flat`、`hnsw`、`ivfflat`，向量类型为 `vector` 或 `halfvec`，距离为 `cosine`、`l2` 或 `inner_product`。同一 Store 的读写端共用嵌入模型、维度及索引配置。本地验证环境为 openGauss-lite 7.0.0-RC3（A 兼容模式），使用数据库内置向量类型与算子。

**Store 手动建表**：执行 [store/schema.sql](https://github.com/Onenightcarnival/langgraph-checkpoint-opengauss/blob/main/src/langgraph-checkpoint-opengauss/store/schema.sql)。启用向量检索时，再执行 [store/vector_schema.sql](https://github.com/Onenightcarnival/langgraph-checkpoint-opengauss/blob/main/src/langgraph-checkpoint-opengauss/store/vector_schema.sql)。该脚本对应上例的 1536 维、cosine、HNSW 配置。其他配置可由带相应 `index` 的 Store 执行 `setup()` 生成。索引启用前已写入的文档可通过 `put` 或 `aput` 重新建立嵌入。

## 本地验证

开发环境使用 Python 3.11+ 和项目根目录的 `.venv`。测试账号需要在测试数据库中创建 schema 的权限。每组数据库测试使用独立的临时 schema。Agent 测试读取 `OPENAI_BASE_URL`、`OPENAI_API_KEY` 和 `OPENAI_MODEL`。

```shell
uv sync --locked --python 3.12
uv run ruff format --check src tests
uv run ruff check src tests
uv run --env-file .env pytest -q -s tests
```
