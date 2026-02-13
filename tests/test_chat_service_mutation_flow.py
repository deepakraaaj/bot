import asyncio

from app.schemas.chat import ChatRequest
from app.services.chat_service import ChatService


def test_active_mutation_collects_next_field_one_by_one():
    svc = ChatService()
    req = ChatRequest(session_id="s-m1", message="2026-02-14", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["date", "occurrence"],
        "collected_fields": {},
        "pending_field": "date",
        "field_descriptions": {"date": "Schedule date", "occurrence": "Repeat pattern"},
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    assert result["workflow"]["completed"] is False
    assert result["workflow"]["next_field"] == "occurrence"
    assert "occurrence" in result["message"].lower()


def test_active_mutation_completion_returns_confirmation_preview():
    svc = ChatService()
    req = ChatRequest(session_id="s-m2", message="2026-02-14", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["date"],
        "collected_fields": {},
        "pending_field": "date",
        "field_descriptions": {"date": "Schedule date"},
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    assert result["workflow"]["mode"] == "confirmation"
    assert "review before insert" in result["message"].lower()
    assert req.metadata.get("mutation_context") is None


def test_confirmation_yes_injects_mutation_context_for_execution():
    svc = ChatService()
    req = ChatRequest(session_id="s-m2b", message="yes", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["date", "occurrence"],
        "collected_fields": {"date": "2026-02-14", "occurrence": "2"},
        "pending_field": "",
        "field_descriptions": {},
        "awaiting": "confirmation",
        "page": 0,
        "page_size": 5,
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is None
    mutation_context = req.metadata.get("mutation_context", {})
    assert mutation_context.get("operation") == "insert"
    assert mutation_context.get("table") == "scheduler_details"
    assert mutation_context.get("fields", {}).get("date") == "2026-02-14"


def test_active_mutation_does_not_treat_new_command_as_field_value():
    svc = ChatService()
    req = ChatRequest(session_id="s-m3", message="create schedule", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["date", "occurrence"],
        "collected_fields": {},
        "pending_field": "date",
        "field_descriptions": {"date": "Schedule date", "occurrence": "Repeat pattern"},
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    collected = result["workflow"]["collected_data"]["collected_fields"]
    assert collected == {}
    assert result["workflow"]["next_field"] == "date"


def test_extracts_db_error_columns_for_recovery():
    svc = ChatService()
    assert svc._extract_invalid_column("Incorrect integer value: 'daily' for column 'occurrence' at row 1") == "occurrence"
    assert svc._extract_missing_required_column("Field 'scheduled_ref_no' doesn't have a default value") == "scheduled_ref_no"


def test_field_selection_number_prompts_for_value_in_next_step():
    svc = ChatService()
    req = ChatRequest(session_id="s-m4", message="1", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["occurrence", "date"],
        "collected_fields": {},
        "pending_field": "occurrence",
        "field_descriptions": {"date": "Schedule date", "occurrence": "Repeat pattern"},
        "awaiting": "field_selection",
        "page": 0,
        "page_size": 5,
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    assert result["workflow"]["mode"] == "field_value"
    assert result["workflow"]["next_field"] == "occurrence"


def test_option_label_input_is_normalized_to_numeric_value():
    svc = ChatService()
    req = ChatRequest(session_id="s-m5", message="Weekly (2)", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["occurrence", "date"],
        "collected_fields": {},
        "pending_field": "occurrence",
        "field_descriptions": {"occurrence": "Repeat pattern"},
        "awaiting": "field_value",
        "page": 0,
        "page_size": 5,
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    collected = result["workflow"]["collected_data"]["collected_fields"]
    assert collected.get("occurrence") == "2"


def test_invalid_date_value_is_rejected_and_not_collected():
    svc = ChatService()
    req = ChatRequest(session_id="s-m6", message="date - Schedule date", metadata={})
    state = {
        "workflow_id": "mutation_menu",
        "state": "collect_insert_scheduler_details",
        "operation": "insert",
        "table": "scheduler_details",
        "required_fields": ["date"],
        "collected_fields": {},
        "pending_field": "date",
        "field_descriptions": {"date": "Schedule date"},
        "awaiting": "field_value",
        "page": 0,
        "page_size": 5,
    }

    result = asyncio.run(svc._handle_active_mutation(req, state))

    assert result is not None
    assert result["workflow"]["mode"] == "field_value"
    collected = result["workflow"]["collected_data"]["collected_fields"]
    assert "date" not in collected
