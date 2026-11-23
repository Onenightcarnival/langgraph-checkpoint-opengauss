# langgraph-checkpoint-opengauss

LangGraph 的 openGauss 检查点后端。同步入口为 `OpenGaussSaver`，异步入口为 `AsyncOpenGaussSaver`。两者共用表结构，支持跨进程恢复同一线程。

## 安装与连接

在使用方项目中添加本地组件包（路径指向本仓库）：

```shell
uv add --editable ../langgraph-checkpoint-opengauss
```

运行环境需要 psycopg 3 和可用的 libpq。使用预编译驱动的环境可安装 `psycopg[binary]`。数据库需允许 PostgreSQL 客户端使用配置的认证方式连接。

连接串接受 libpq DSN，也可用空字符串读取 `PGHOST`、`PGPORT`、`PGDATABASE`、`PGUSER`、`PGPASSWORD` 和 `PGSSLMODE`。

## 初始化检查点表

**手动建表**：在目标数据库中将 `search_path` 指向应用使用的 schema，再用 SQL 客户端完整执行 [schema.sql](src/langgraph-checkpoint-opengauss/schema.sql)。脚本在一个事务中创建四张表、三个辅助索引，并登记迁移版本 0–9。相同版本可重复执行。完成后，同步和异步应用都可省略下方示例中的 `setup()`，直接编译图。

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

## 检查点操作

| 操作 | 同步 | 异步 |
| --- | --- | --- |
| 写入检查点与任务结果 | `put`、`put_writes` | `aput`、`aput_writes` |
| 读取与筛选历史 | `get_tuple`、`list` | `aget_tuple`、`alist` |
| 读取增量通道历史 | `get_delta_channel_history` | `aget_delta_channel_history` |
| 复制、删除线程 | `copy_thread`、`delete_thread` | `acopy_thread`、`adelete_thread` |
| 按运行删除、按线程裁剪 | `delete_for_runs`、`prune` | `adelete_for_runs`、`aprune` |

`prune(strategy="keep_latest")` 保留各命名空间的最新检查点及其依赖，遇到依赖历史重放的 DeltaChannel 时拒绝裁剪。`prune(strategy="delete")` 删除指定线程的检查点、任务结果和通道数据。

## 本地验证

开发环境使用 Python 3.11+ 和项目根目录的 `.venv`。测试账号需要在测试数据库中创建 schema 的权限。每组数据库测试使用独立的临时 schema。Agent 测试读取 `OPENAI_BASE_URL`、`OPENAI_API_KEY` 和 `OPENAI_MODEL`。

```shell
uv sync --locked --python 3.12
uv run ruff format --check src tests
uv run ruff check src tests
uv run --env-file .env pytest -q -s tests/test_conformance.py
uv run --env-file .env pytest -q tests/test_storage.py tests/test_agent.py
```
