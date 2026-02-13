import asyncio
from types import SimpleNamespace

from app.assistant.nodes.mutation_understanding_node import MutationUnderstandingNode


class DummyService:
    def __init__(self, table: str):
        self.table = table

    def resolve_table(self, query, intent):
        return self.table


def test_node_updates_intent_table_for_mutation_ops():
    node = object.__new__(MutationUnderstandingNode)
    node.service = DummyService("scheduler_task_details")
    state = {
        "messages": [SimpleNamespace(content="schedule a task")],
        "intent": {"operation": "insert", "table": ""},
    }

    result = asyncio.run(node.run(state))
    assert result["intent"]["table"] == "scheduler_task_details"


def test_node_keeps_non_mutation_intent_unchanged():
    node = object.__new__(MutationUnderstandingNode)
    node.service = DummyService("scheduler_task_details")
    state = {
        "messages": [SimpleNamespace(content="show schedules")],
        "intent": {"operation": "select", "table": ""},
    }

    result = asyncio.run(node.run(state))
    assert result["intent"]["table"] == ""
