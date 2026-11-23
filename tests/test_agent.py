import os
from uuid import uuid4

from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI
from langgraph_checkpoint_opengauss import AsyncOpenGaussSaver, OpenGaussSaver


def build_agent(saver):
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
        system_prompt="Answer directly in plain text. Do not call tools.",
    )


def test_agent_resumes_after_reconnect(conninfo, run_async):
    marker = f"OG-{uuid4().hex[:12]}"
    config = {"configurable": {"thread_id": str(uuid4())}}
    with OpenGaussSaver.from_conn_string(conninfo) as saver:
        saver.setup()
        result = build_agent(saver).invoke(
            {
                "messages": [
                    ("user", f"Remember the reference code {marker}. Reply with OK.")
                ]
            },
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
