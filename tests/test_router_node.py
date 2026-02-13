import asyncio

from app.assistant.nodes.router_node import RouterNode


class DummyRouter:
    async def route(self, query: str) -> str:
        return "CHAT"


def test_router_node_forces_sql_when_mutation_context_present():
    node = object.__new__(RouterNode)
    node.router = DummyRouter()

    result = asyncio.run(node.run({"metadata": {"mutation_context": {"operation": "insert"}}, "messages": []}))

    assert result["route"] == "SQL"
