import asyncio
from types import SimpleNamespace
from typing import Any, cast

from app.assistant.nodes.chat_node import ChatNode


class DummyLLM:
    def __init__(self):
        self.last_prompt = None

    async def ainvoke(self, prompt: str):
        self.last_prompt = prompt
        return SimpleNamespace(content="domain response", response_metadata={"token_usage": {"total_tokens": 10}})


def _make_node_with_dummy_llm() -> tuple[ChatNode, DummyLLM]:
    node = object.__new__(ChatNode)
    llm = DummyLLM()
    node.llm = cast(Any, llm)
    return node, llm


def test_chat_node_capabilities_message_is_project_scoped():
    node, _ = _make_node_with_dummy_llm()
    state = {"messages": [SimpleNamespace(content="what can you do for me")]}

    result = asyncio.run(node.run(state))

    msg = result["messages"][0].content.lower()
    assert "tag application" in msg
    assert "querying project data" in msg
    assert result["token_usage"] == {}


def test_chat_node_out_of_scope_query_is_rejected():
    node, _ = _make_node_with_dummy_llm()
    state = {"messages": [SimpleNamespace(content="translate hello to french")]}

    result = asyncio.run(node.run(state))

    msg = result["messages"][0].content.lower()
    assert "only help with this tag project" in msg
    assert result["token_usage"] == {}


def test_chat_node_in_scope_query_calls_llm_with_domain_prompt():
    node, llm = _make_node_with_dummy_llm()
    state = {"messages": [SimpleNamespace(content="show task count")]}

    result = asyncio.run(node.run(state))

    prompt = llm.last_prompt or ""
    assert "tag backend assistant" in prompt.lower()
    assert "show task count" in prompt.lower()
    assert result["messages"][0].content == "domain response"
    assert result["token_usage"]["total_tokens"] == 10
