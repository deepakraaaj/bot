import asyncio
from typing import Any, cast

from langchain_core.messages import HumanMessage

from app.assistant.nodes.sql_builder_node import SQLBuilderNode


class FakeCatalog:
    def required_create_fields(self, table: str):
        if table == "task_transaction":
            return ["title", "status"]
        return []


class FakeBuilder:
    def __init__(self, table: str, select_sql: str = "SELECT * FROM task_transaction LIMIT 100;"):
        self._table = table
        self._select_sql = select_sql
        self.catalog = FakeCatalog()

    def resolve_table(self, query: str, intent):
        return self._table

    def parse_kv_pairs(self, query: str):
        return {}

    def build_insert(self, table: str, fields, company_id):
        return "INSERT INTO task_transaction (title) VALUES ('x');", ""

    def build_update(self, table: str, fields, company_id):
        return "", "Update requires id=<record_id>."

    async def build_select(self, query: str, table: str, company_id):
        return self._select_sql

    def mutation_form_payload(self, table: str, operation: str, required_fields):
        return {"operation": operation, "table": table, "required_fields": required_fields}


def _base_state(message: str, intent: dict):
    return {
        "messages": [HumanMessage(content=message)],
        "metadata": {"company_id": "c1"},
        "intent": intent,
    }


def test_sql_builder_node_returns_skip_when_table_missing():
    node = object.__new__(SQLBuilderNode)
    node.builder = cast(Any, FakeBuilder(table=""))

    result = asyncio.run(node.run(_base_state("how many tasks", {"operation": "select", "fields": {}})))

    assert result["sql_query"] == "SKIP"
    assert "please mention a table/entity" in result["messages"][0].content.lower()


def test_sql_builder_node_insert_requires_fields_and_returns_workflow_payload():
    node = object.__new__(SQLBuilderNode)
    node.builder = cast(Any, FakeBuilder(table="task_transaction"))

    result = asyncio.run(node.run(_base_state("create task", {"operation": "insert", "fields": {}})))

    assert result["sql_query"] == "SKIP"
    assert "missing required fields" in result["messages"][0].content.lower()
    assert result["workflow_payload"]["operation"] == "insert"


def test_sql_builder_node_update_missing_id_returns_guidance():
    node = object.__new__(SQLBuilderNode)
    node.builder = cast(Any, FakeBuilder(table="task_transaction"))

    result = asyncio.run(node.run(_base_state("update task", {"operation": "update", "fields": {"status": "Done"}})))

    assert result["sql_query"] == "SKIP"
    assert "use e.g. id=123" in result["messages"][0].content.lower()


def test_sql_builder_node_select_builds_sql():
    node = object.__new__(SQLBuilderNode)
    node.builder = cast(Any, FakeBuilder(table="task_transaction"))

    result = asyncio.run(node.run(_base_state("show tasks", {"operation": "select", "fields": {}})))

    assert result["sql_query"].startswith("SELECT")
