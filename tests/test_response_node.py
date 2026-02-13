import asyncio

from app.assistant.nodes.response_node import ResponseNode


def test_response_node_error_path():
    node = ResponseNode()
    result = asyncio.run(node.run({"error": "boom"}))
    assert "request failed safely" in result["messages"][0].content.lower()


def test_response_node_insert_message():
    node = ResponseNode()
    result = asyncio.run(node.run({"sql_query": "INSERT INTO x VALUES (1)", "row_count": 2}))
    assert "insert successful" in result["messages"][0].content.lower()


def test_response_node_update_message():
    node = ResponseNode()
    result = asyncio.run(node.run({"sql_query": "UPDATE x SET a=1", "row_count": 3}))
    assert "update successful" in result["messages"][0].content.lower()


def test_response_node_select_no_records_message():
    node = ResponseNode()
    result = asyncio.run(node.run({"sql_query": "SELECT * FROM x", "row_count": 0, "rows_preview": []}))
    assert result["messages"][0].content == "No records found."
