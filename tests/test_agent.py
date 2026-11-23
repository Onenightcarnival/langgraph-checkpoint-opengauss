import os
from uuid import uuid4

from deepagents import create_deep_agent
from deepagents.backends import StoreBackend
from langchain_openai import ChatOpenAI
from langgraph_checkpoint_opengauss import AsyncOpenGaussSaver, AsyncOpenGaussStore, OpenGaussSaver, OpenGaussStore


def build_agent(saver, store=None):
    model = ChatOpenAI(
        model=os.environ["OPENAI_MODEL"],
        base_url=os.environ["OPENAI_BASE_URL"],
        api_key=os.environ["OPENAI_API_KEY"],
        timeout=120,
        max_retries=1,
    )
    return create_deep_agent(
        model=model,
        checkpointer=saver,
        store=store,
        backend=StoreBackend(namespace=lambda _: ("agent", "files")) if store is not None else None,
        system_prompt="Use file tools when asked."
        if store is not None
        else "Answer directly in plain text. Do not call tools.",
    )


def test_agent_resumes_after_reconnect(conninfo, run_async):
    marker = f"OG-{uuid4().hex[:12]}"
    config = {"configurable": {"thread_id": str(uuid4())}}
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        saver.setup()
        result = build_agent(saver).invoke(
            {"messages": [("user", f"Remember the reference code {marker}. Reply with OK.")]},
            config,
        )
        previous = [message.id for message in result["messages"]]
        assert result["messages"][-1].type == "ai"

    async def resume():
        async with AsyncOpenGaussSaver.from_conn_string(conninfo) as saver:
            agent = build_agent(saver)
            state = await agent.aget_state(config)
            assert [message.id for message in state.values["messages"]] == previous
            result = await agent.ainvoke(
                {
                    "messages": [
                        (
                            "user",
                            "What was my reference code? Reply with the exact code.",
                        )
                    ]
                },
                config,
            )
            assert marker in result["messages"][-1].text
            return [message.id for message in result["messages"]]

    resumed = run_async(resume())
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        state = build_agent(saver).get_state(config)
        assert [message.id for message in state.values["messages"]] == resumed
        assert resumed[: len(previous)] == previous


def test_agent_shared_memory_across_threads(conninfo, run_async):
    marker = f"OG-{uuid4().hex[:12]}"
    writer = {"configurable": {"thread_id": "writer"}}
    reader = {"configurable": {"thread_id": "reader"}}
    with (
        OpenGaussStore.from_conn_string(conninfo) as store,
        OpenGaussSaver.from_conn_string(conninfo) as saver,
    ):
        store.setup()
        saver.setup()
        build_agent(saver, store).invoke(
            {"messages": [("user", f"Use write_file to save exactly {marker} in /reference.txt.")]}, writer
        )
        assert marker in store.get(("agent", "files"), "/reference.txt").value["content"]
        assert saver.get(reader) is None

    async def recall():
        async with (
            AsyncOpenGaussStore.from_conn_string(conninfo) as store,
            AsyncOpenGaussSaver.from_conn_string(conninfo) as saver,
        ):
            result = await build_agent(saver, store).ainvoke(
                {"messages": [("user", "Read /reference.txt using read_file and reply with its exact contents.")]},
                reader,
            )
            assert marker in result["messages"][-1].text

    run_async(recall())
